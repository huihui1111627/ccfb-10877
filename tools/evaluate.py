#!/usr/bin/env python3
"""Offline evaluation of LogScope against the ground truth produced by gen_logs.

Inputs (all paths as arguments, run with -I):
  1. truth.jsonl        per-line ground-truth template ids
  2. faults.json        injected fault windows
  3. assignments.jsonl  per-line cluster assignments from `logscope bench --assignments`
  4. events.jsonl       detected events (dumped from /api/events; optional)
  5. report.json        bench throughput summary

Metrics:
  - clustering: pairwise precision/recall/F1, per-service breakdown,
    purity/inverse-purity, per-truth-template fragmentation
  - fault detection: recall, detection latency (first matching event vs fault start),
    false-positive event count outside all fault windows
  - throughput: lines/sec from the bench report
"""

import json
import math
import sys
from collections import Counter, defaultdict


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def load_faults(path):
    with open(path) as f:
        data = json.load(f)
    return data if isinstance(data, list) else load_jsonl(path)


def pairwise_metrics(records):
    """records: list of dicts with truth template and predicted cluster key."""
    n = len(records)
    same_truth = same_pred = both = 0
    truth_groups = defaultdict(list)
    pred_groups = defaultdict(list)
    for i, r in enumerate(records):
        truth_groups[r["truth"]].append(i)
        pred_groups[r["pred"]].append(i)
    for group in truth_groups.values():
        same_truth += len(group) * (len(group) - 1) // 2
    for group in pred_groups.values():
        same_pred += len(group) * (len(group) - 1) // 2
    # intersection pairs: sum over cells C(size,2) of the contingency table.
    cell = Counter((r["truth"], r["pred"]) for r in records)
    for (_, _), c in cell.items():
        both += c * (c - 1) // 2
    precision = both / same_pred if same_pred else 0.0
    recall = both / same_truth if same_truth else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    # purity / inverse purity
    purity = sum(max(Counter(r["truth"] for r in records if r["pred"] == p).values())
                 for p in pred_groups) / n
    inv_purity = sum(max(Counter(r["pred"] for r in records if r["truth"] == t).values())
                     for t in truth_groups) / n

    # fragmentation: how many predicted clusters each truth template spans.
    frag = {t: len({r["pred"] for r in records if r["truth"] == t})
            for t in truth_groups}
    # merges: how many truth templates each predicted cluster contains.
    merges = {p: len({r["truth"] for r in records if r["pred"] == p})
              for p in pred_groups}
    return {
        "n_records": n,
        "pairwise_precision": precision,
        "pairwise_recall": recall,
        "pairwise_f1": f1,
        "purity": purity,
        "inverse_purity": inv_purity,
        "truth_templates": len(truth_groups),
        "predicted_clusters": len(pred_groups),
        "fragmentation": dict(sorted(frag.items())),
        "clusters_with_merges": {p: k for p, k in merges.items() if k > 1},
    }


def fault_detection(truth, events, faults):
    # Earliest ground-truth line inside each fault window: lets us measure the
    # gap between the first anomalous log line and the first alert (the real
    # operational detection latency), not just bucket-floor distance.
    first_anomaly = {}
    for f in faults:
        lines = [t["ts_ms"] for t in truth
                 if f["start_ms"] <= t["ts_ms"] <= f["end_ms"]
                 and t["service"] in f["services"]
                 and t.get("fault") == f["id"]]
        if lines:
            first_anomaly[f["id"]] = min(lines)

    # Events belong to a fault if they land in its window and affect one of its
    # services; new_template/spike/drop all count as valid signals.
    detected = {}
    fp_events = []
    # The dataset starts cold: every service's first ERROR template fires an
    # immediate new-template alert by design. Those first-two-minute events are
    # cold-start notifications, not anomalies, and are counted separately.
    t0 = min(r["ts_ms"] for r in truth)
    cold_cutoff = t0 + 2 * 60_000
    cold_start = []
    for ev in events:
        if ev["kind"] == "NewTemplate" and ev["ts_ms"] <= cold_cutoff:
            cold_start.append(ev)
            continue
        ts = ev["ts_ms"]
        in_any = False
        for f in faults:
            if f["start_ms"] <= ts <= f["end_ms"] + 3 * 60_000 and ev["service"] in f["services"]:
                in_any = True
                # The alert timestamp is the start of the evaluated minute
                # bucket; it fires when that bucket closes, so its real emission
                # time is bucket_start + 60s. Detection latency is measured from
                # the first anomalous line to that emission.
                emit_ts = ts + 60_000
                bucket_latency = (emit_ts - f["start_ms"]) / 1000.0
                line_latency = None
                if f["id"] in first_anomaly:
                    line_latency = max(0.0, (emit_ts - first_anomaly[f["id"]]) / 1000.0)
                prev = detected.get(f["id"])
                cand = {
                    "latency_s": bucket_latency,
                    "line_to_alert_s": line_latency,
                    "event_kind": ev["kind"],
                    "service": ev["service"],
                    "template": ev["template_text"][:90],
                }
                if prev is None or bucket_latency < prev["latency_s"]:
                    detected[f["id"]] = cand
        if not in_any:
            fp_events.append(ev)
    rows = []
    for f in faults:
        d = detected.get(f["id"])
        rows.append({
            "fault": f["id"],
            "title": f["title"],
            "detected": d is not None,
            "latency_s": d["latency_s"] if d else None,
            "line_to_alert_s": d["line_to_alert_s"] if d else None,
            "first_signal": d,
        })
    latencies = [r["latency_s"] for r in rows if r["latency_s"] is not None]
    line_lat = [r["line_to_alert_s"] for r in rows if r["line_to_alert_s"] is not None]
    return {
        "per_fault": rows,
        "recall": len(latencies) / len(faults),
        "mean_latency_s": sum(latencies) / len(latencies) if latencies else None,
        "max_latency_s": max(latencies) if latencies else None,
        "mean_line_to_alert_s": sum(line_lat) / len(line_lat) if line_lat else None,
        "max_line_to_alert_s": max(line_lat) if line_lat else None,
        "cold_start_error_alerts": len(cold_start),
        "false_positive_events": len(fp_events),
        "false_positive_rate_per_hour": len(fp_events) / _span_hours(truth),
        "fp_breakdown": Counter(e["kind"] for e in fp_events),
    }


def _span_hours(truth):
    ts = [r["ts_ms"] for r in truth]
    return max((max(ts) - min(ts)) / 3_600_000.0, 1 / 60)


def main():
    truth_path, faults_path, assign_path, events_path, report_path = sys.argv[1:6]
    truth = load_jsonl(truth_path)
    faults = load_faults(faults_path)
    assigns = load_jsonl(assign_path)
    events = load_jsonl(events_path) if events_path != "-" else []
    bench = json.load(open(report_path)) if report_path != "-" else {}

    by_line = {a["line"]: f's{a["shard"]}-c{a["cluster"]}' for a in assigns}
    records = []
    missing = 0
    for t in truth:
        pred = by_line.get(t["line"])
        if pred is None:
            missing += 1
            continue
        records.append({"line": t["line"], "truth": t["template"], "pred": pred,
                        "service": t["service"], "version": t["version"]})

    overall = pairwise_metrics(records)
    overall["missing_assignments"] = missing

    per_service = {}
    for svc in sorted({r["service"] for r in records}):
        sub = [r for r in records if r["service"] == svc]
        m = pairwise_metrics(sub)
        per_service[svc] = {k: v for k, v in m.items()
                            if k in ("pairwise_precision", "pairwise_recall",
                                     "pairwise_f1", "purity", "inverse_purity",
                                     "truth_templates", "predicted_clusters",
                                     "fragmentation")}

    # Deploy (F2) specific: v8 vs v9 template fragmentation for order-gateway.
    og = [r for r in records if r["service"] == "order-gateway"]
    deploy = {}
    for ver in ("v8", "v9"):
        sub = [r for r in og if r["version"] == ver]
        deploy[ver] = pairwise_metrics(sub) if sub else {}

    detection = fault_detection(truth, events, faults)
    detection["fp_breakdown"] = dict(detection["fp_breakdown"])

    # Multiline check: every ground-truth multiline entry must share a cluster
    # with the other entries of the same truth template (stack folding works).
    ml_lines = {t["line"] for t in truth if t.get("multiline")}
    ml_records = [r for r in records if r["line"] in ml_lines]
    ml_consistency = 0.0
    if ml_records:
        # fraction of multiline records landing in the dominant cluster of
        # their truth template
        by_truth = defaultdict(list)
        for r in ml_records:
            by_truth[r["truth"]].append(r["pred"])
        ok = 0
        for preds in by_truth.values():
            dom = Counter(preds).most_common(1)[0][1]
            ok += dom
        ml_consistency = ok / len(ml_records)

    report = {
        "clustering_overall": overall,
        "clustering_per_service": per_service,
        "deploy_v8_v9": {
            "v8": {k: deploy["v8"].get(k) for k in
                   ("truth_templates", "predicted_clusters", "pairwise_f1")},
            "v9": {k: deploy["v9"].get(k) for k in
                   ("truth_templates", "predicted_clusters", "pairwise_f1")},
        },
        "fault_detection": detection,
        "multiline_entries": len(ml_records),
        "multiline_cluster_consistency": ml_consistency,
        "throughput_lines_per_sec": bench.get("throughput_lines_per_sec"),
        "templates_total": bench.get("templates"),
        "events_total": bench.get("events"),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    out = sys.argv[6] if len(sys.argv) > 6 else None
    if out:
        with open(out, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
