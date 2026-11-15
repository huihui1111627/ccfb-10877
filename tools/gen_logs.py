#!/usr/bin/env python3
"""Synthetic but realistic log generator for LogScope evaluation.

Emits five services with distinct logging styles, a v8->v9 deploy (format
merge/split), multiline Java/Python stack traces, numeric latency fields, and
five injected faults. Every logical entry is recorded in truth.jsonl with its
ground-truth template id; faults are described in faults.json.

Output directory is given as the first positional argument and is created fresh.
Run with -I (isolated mode): python3 -I tools/gen_logs.py data/logs
"""

import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone

RNG = random.Random(20261115)


def poisson(lam):
    """Poisson sample. Knuth's method for small lambda; for large lambda the
    exponential underflows so use the normal approximation there."""
    if lam < 30:
        l = math.exp(-lam)
        k = 0
        p = 1.0
        while p > l:
            k += 1
            p *= RNG.random()
        return k - 1
    # Normal approximation, rounded and floored at zero.
    val = RNG.gauss(lam, math.sqrt(lam))
    return max(0, int(round(val)))

# ---------------------------------------------------------------- templates --

class Tpl:
    def __init__(self, tid, level, weight, versions, fn, multiline=False):
        self.tid = tid
        self.level = level
        self.weight = weight
        self.versions = versions  # versions in which the template exists
        self.fn = fn
        self.multiline = multiline


def hex_blob(n=16):
    return "".join(RNG.choice("0123456789abcdef") for _ in range(n))


def ip():
    return f"10.{RNG.randrange(0, 40)}.{RNG.randrange(0, 255)}.{RNG.randrange(1, 254)}"


JAVA_STACK = [
    "\tat com.acme.payment.client.PaymentClient.invoke(PaymentClient.java:{ln})",
    "\tat com.acme.order.service.OrderService.charge(OrderService.java:{ln})",
    "\tat com.acme.order.web.OrderController.create(OrderController.java:{ln})",
    "\tat org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:{ln})",
    "\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:{ln})",
]

PY_STACK = [
    '  File "/opt/app/inventory/db.py", line {ln}, in execute',
    '    return self._conn.execute(query, params)',
    '  File "/opt/app/inventory/reservations.py", line {ln}, in create',
    '    db.execute("INSERT INTO reservations ...", values)',
]


def java_stack():
    depth = RNG.randint(3, 5)
    lines = ["java.util.concurrent.TimeoutException: Read timed out"]
    for frame in JAVA_STACK[:depth]:
        lines.append(frame.format(ln=RNG.randrange(40, 400)))
    return "\n".join(lines)


def py_stack():
    lines = ["Traceback (most recent call last):"]
    for frame in PY_STACK:
        lines.append(frame.format(ln=RNG.randrange(20, 300)))
    lines.append("psycopg2.OperationalError: FATAL: remaining connection slots are reserved")
    return "\n".join(lines)


# ---- order-gateway: Java/Spring style, deploys v8 -> v9 at minute 40 ----
def og_templates():
    return [
        Tpl("OG-T01", "INFO", 30, ("v8",),
            lambda: f"Order created orderId={RNG.randrange(10**9, 10**10)} customerId={RNG.randrange(10**6, 10**7)} amount={RNG.randrange(5, 800)}.{RNG.randrange(99):02d}USD"),
        # v9: same event, changed wording + extra latency field (split/merge pair)
        Tpl("OG-T01B", "INFO", 30, ("v9",),
            lambda: f"Order created orderId={RNG.randrange(10**9, 10**10)} customerId={RNG.randrange(10**6, 10**7)} amount={RNG.randrange(5, 800)}.{RNG.randrange(99):02d}USD latency={RNG.randrange(2, 120)}ms"),
        Tpl("OG-T02", "INFO", 26, ("v8",),
            lambda: f"Request GET /api/v1/orders/{RNG.randrange(10**6, 10**7)} completed status=200 took {RNG.randrange(5, 240)}ms"),
        Tpl("OG-T02B", "INFO", 26, ("v9",),
            lambda: f"Request GET /api/v2/orders/{RNG.randrange(10**6, 10**7)} completed status=200 took {RNG.randrange(5, 240)}ms"),
        Tpl("OG-T03", "WARN", 6, ("v8", "v9"),
            lambda: f"Slow downstream call payment-service took {RNG.randrange(500, 1500)}ms threshold=500ms"),
        Tpl("OG-T04", "ERROR", 2, ("v8",),
            lambda: f"Downstream call failed service=payment-service attempt={RNG.randrange(1, 4)} error=TimeoutException",
            multiline=True),
        Tpl("OG-T04B", "ERROR", 2, ("v9",),
            lambda: f"Downstream invocation failed target=payment-service attempt={RNG.randrange(1, 4)} type=TIMEOUT",
            multiline=True),
        Tpl("OG-T05", "INFO", 18, ("v8", "v9"),
            lambda: f"Cache hit key=order_{RNG.randrange(10**5, 10**7)} region=cn-east-{RNG.randrange(1, 5)} ttl={RNG.randrange(10, 300)}s"),
        Tpl("OG-T06", "INFO", 12, ("v8", "v9"),
            lambda: f"Message published topic=order.events partition={RNG.randrange(0, 16)} offset={RNG.randrange(10**5, 10**7)}"),
        Tpl("OG-T07", "DEBUG", 4, ("v8", "v9"),
            lambda: f"Handler dispatch path=/internal/health node=gateway-{RNG.randrange(1, 9)}.prod"),
        # v9-only brand-new template
        Tpl("OG-T10", "INFO", 8, ("v9",),
            lambda: f"Feature flag new_pricing enabled rollout={RNG.randrange(1, 100)}% cohort=uid_{RNG.randrange(10**4, 10**5)}"),
        Tpl("OG-T11", "ERROR", 1, ("v9",),
            lambda: f"Pricing rule evaluation failed ruleId={RNG.randrange(100, 999)} fallback=legacy_path",
            multiline=True),
    ]


# ---- payment-service: Go style ----
def pay_templates():
    return [
        Tpl("PAY-T01", "INFO", 34, ("v3",),
            lambda: f"payment processed transaction_id {hex_blob()} amount {RNG.randrange(1, 900)}.{RNG.randrange(99):02d}USD in {RNG.randrange(8, 220)}ms"),
        Tpl("PAY-T02", "INFO", 28, ("v3",),
            lambda: f'http handler POST /charge status=200 duration={RNG.randrange(5, 200)}ms client=10.20.{RNG.randrange(0, 40)}.{RNG.randrange(1, 254)}'),
        Tpl("PAY-T03", "ERROR", 2, ("v3",),
            lambda: f"context deadline exceeded calling bank-gateway host=10.20.{RNG.randrange(0, 40)}.{RNG.randrange(1, 254)} timeout={RNG.randrange(200, 1000)}ms"),
        Tpl("PAY-T04", "WARN", 4, ("v3",),
            lambda: f"retry attempt {RNG.randrange(1, 5)} for transaction {hex_blob()} backoff={RNG.randrange(50, 800)}ms"),
        Tpl("PAY-T05", "INFO", 16, ("v3",),
            lambda: f"connection pool stats idle={RNG.randrange(2, 20)} active={RNG.randrange(1, 30)} max=32 waiters={RNG.randrange(0, 5)}"),
        Tpl("PAY-T06", "INFO", 10, ("v3",),
            lambda: f"webhook delivered url=https://partner-{RNG.randrange(1, 20)}.example.com/hook status=200 attempt={RNG.randrange(1, 3)}"),
        Tpl("PAY-T07", "DEBUG", 6, ("v3",),
            lambda: f"gc pause {RNG.randrange(1, 30)}ms heap_inuse={RNG.randrange(200, 900)}MB"),
    ]


# ---- inventory-service: Python style ----
def inv_templates():
    return [
        Tpl("INV-T01", "INFO", 32, ("v5",),
            lambda: f"stock check sku={RNG.randrange(10**5, 10**6)} warehouse=wh-{RNG.randrange(1, 8)} qty={RNG.randrange(0, 5000)}"),
        Tpl("INV-T02", "INFO", 22, ("v5",),
            lambda: f"reservation created order_id={RNG.randrange(10**8, 10**9)} items={RNG.randrange(1, 12)} ttl={RNG.randrange(30, 300)}s"),
        Tpl("INV-T03", "ERROR", 1, ("v5",),
            lambda: "psycopg2.OperationalError: FATAL: remaining connection slots are reserved for non-replication superuser connections",
            multiline=True),
        Tpl("INV-T04", "WARN", 5, ("v5",),
            lambda: f"query took {RNG.randrange(200, 900)}ms slower than {RNG.randrange(100, 200)}ms sql=select_stock"),
        Tpl("INV-T05", "INFO", 18, ("v5",),
            lambda: f"cache invalidated sku={RNG.randrange(10**5, 10**6)} node=inventory-{RNG.randrange(1, 12)}.prod region=cn-north-{RNG.randrange(1, 4)}"),
        Tpl("INV-T06", "INFO", 12, ("v5",),
            lambda: f"scheduled reconcile batch={RNG.randrange(1000, 9999)} rows={RNG.randrange(100, 5000)} duration={RNG.randrange(100, 4000)}ms"),
        Tpl("INV-T07", "DEBUG", 4, ("v5",),
            lambda: f"replication lag replica=db-ro-{RNG.randrange(1, 4)} lag={RNG.randrange(5, 400)}ms"),
    ]


# ---- user-profile: JSON lines style ----
def up_templates():
    devices = ["iPhone15", "Pixel8", "MacBookPro", "Windows11", "LinuxX11"]
    return [
        Tpl("UP-T01", "INFO", 30, ("v2",),
            lambda: json.dumps({"event": "user_login", "uid": RNG.randrange(10**6, 10**7),
                                 "ip": ip(), "device": RNG.choice(devices)}, separators=(",", ":"))),
        Tpl("UP-T02", "INFO", 26, ("v2",),
            lambda: f"profile fetched uid={RNG.randrange(10**6, 10**7)} fields={RNG.randrange(1, 40)} cache={'MISS' if RNG.random() < 0.3 else 'HIT'}"),
        Tpl("UP-T03", "ERROR", 1, ("v2",),
            lambda: f"redis cluster unreachable endpoint=redis-{RNG.randrange(1, 6)}.prod:6379 retrying in {RNG.randrange(100, 900)}ms"),
        Tpl("UP-T04", "INFO", 20, ("v2",),
            lambda: f"token issued uid={RNG.randrange(10**6, 10**7)} ttl={RNG.randrange(600, 7200)}s scope=read:profile"),
        Tpl("UP-T05", "INFO", 12, ("v2",),
            lambda: f"avatar upload uid={RNG.randrange(10**6, 10**7)} size={RNG.randrange(20, 900)}KB bucket=avatars-{RNG.randrange(1, 4)}"),
        Tpl("UP-T06", "WARN", 5, ("v2",),
            lambda: f"rate limit near threshold uid={RNG.randrange(10**6, 10**7)} rpm={RNG.randrange(80, 120)} limit=120"),
    ]


# ---- search-api: mixed text ----
def words():
    return RNG.choice(["laptop", "phone", "shoes", "coffee", "desk", "monitor", "book", "chair", "cable", "camera"])


def sr_templates():
    return [
        Tpl("SR-T01", "INFO", 34, ("v4",),
            lambda: f"search query q={words()} results={RNG.randrange(0, 400)} latency={RNG.randrange(8, 90)}ms shard=products_v2"),
        Tpl("SR-T02", "WARN", 3, ("v4",),
            lambda: f"slow query detected latency={RNG.randrange(500, 2000)}ms query={words()}"),
        Tpl("SR-T03", "ERROR", 1, ("v4",),
            lambda: f"index shard unavailable shard={RNG.randrange(0, 12)} index=products_v2 retries={RNG.randrange(1, 4)}"),
        Tpl("SR-T04", "INFO", 20, ("v4",),
            lambda: f"suggest request prefix={words()[:4]} candidates={RNG.randrange(0, 30)} took {RNG.randrange(2, 40)}ms"),
        Tpl("SR-T05", "INFO", 14, ("v4",),
            lambda: f"index refresh completed docs={RNG.randrange(10**4, 10**6)} took {RNG.randrange(100, 900)}ms segments={RNG.randrange(5, 40)}"),
        Tpl("SR-T06", "INFO", 10, ("v4",),
            lambda: f"cache warmup phase={RNG.randrange(1, 5)} keys={RNG.randrange(1000, 9999)} hitrate={RNG.randrange(70, 99)}%"),
    ]


SERVICES = {
    "order-gateway": {"style": "text", "base_per_min": 700, "tpls": og_templates()},
    "payment-service": {"style": "text", "base_per_min": 600, "tpls": pay_templates()},
    "inventory-service": {"style": "text", "base_per_min": 500, "tpls": inv_templates()},
    "user-profile": {"style": "json", "base_per_min": 450, "tpls": up_templates()},
    "search-api": {"style": "text", "base_per_min": 550, "tpls": sr_templates()},
}

# ------------------------------------------------------------------- faults --
# Each fault: active minute window, per-service template weight multipliers,
# rate multipliers, and numeric latches (for latency regression).
FAULTS = [
    {
        "id": "F1", "title": "payment bank-gateway timeout",
        "start_min": 25, "end_min": 31,
        "weights": {
            "payment-service": {"PAY-T03": 14.0, "PAY-T04": 9.0, "PAY-T01": 0.6},
            "order-gateway": {"OG-T03": 6.0, "OG-T04": 8.0, "OG-T04B": 8.0},
        },
        "rates": {"payment-service": 1.15, "order-gateway": 1.1},
    },
    {
        "id": "F2", "title": "order-gateway deploy v8 -> v9 (format change)",
        "start_min": 40, "end_min": 42,
        "deploy": "order-gateway",
        "rates": {"order-gateway": 1.3},
    },
    {
        "id": "F3", "title": "inventory DB connection pool exhausted",
        "start_min": 60, "end_min": 65,
        "weights": {
            "inventory-service": {"INV-T03": 18.0, "INV-T04": 5.0, "INV-T01": 0.5, "INV-T02": 0.5},
            "search-api": {"SR-T03": 7.0},
            "order-gateway": {"OG-T03": 4.0},
        },
        "rates": {"inventory-service": 1.2},
    },
    {
        "id": "F4", "title": "search-api latency regression",
        "start_min": 80, "end_min": 90,
        "weights": {"search-api": {"SR-T02": 11.0, "SR-T01": 1.1}},
        "latency_regression": {"search-api": ("SR-T01", 380.0, 700.0)},
    },
    {
        "id": "F5", "title": "user-profile redis cache outage",
        "start_min": 100, "end_min": 106,
        "weights": {"user-profile": {"UP-T03": 7.0, "UP-T06": 4.0, "UP-T01": 0.18, "UP-T02": 0.4}},
    },
]


def active_faults(minute):
    return [f for f in FAULTS if f["start_min"] <= minute <= f["end_min"]]


def version_of(service, minute):
    if service == "order-gateway":
        return "v8" if minute < 40 else "v9"
    return SERVICES[service]["tpls"][0].versions[0]


def pick_template(service, minute, faults):
    svc = SERVICES[service]
    ver = version_of(service, minute)
    pool = [t for t in svc["tpls"] if ver in t.versions]
    weights = []
    for t in pool:
        w = float(t.weight)
        for f in faults:
            w *= f.get("weights", {}).get(service, {}).get(t.tid, 1.0)
        weights.append(w)
    return RNG.choices(pool, weights=weights, k=1)[0]


def render(service, tpl, minute, faults):
    # Latency regression F4: patch SR-T01 latency by regenerating with high values.
    text = tpl.fn()
    for f in faults:
        reg = f.get("latency_regression", {}).get(service)
        if reg and reg[0] == tpl.tid:
            lo, hi = reg[1], reg[2]
            import re
            text = re.sub(r"latency=\d+ms", f"latency={RNG.randrange(int(lo), int(hi))}ms", text)
    if tpl.multiline:
        if service == "order-gateway":
            text = text + "\n" + java_stack()
        elif service == "inventory-service":
            text = py_stack()
    return text


def iso(ts):
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "data/logs"
    minutes = int(os.environ.get("MINUTES", "120"))
    scale = float(os.environ.get("SCALE", "1"))
    os.makedirs(out_dir, exist_ok=True)
    for name in os.listdir(out_dir):
        os.remove(os.path.join(out_dir, name))

    start = datetime(2026, 11, 15, 0, 0, 0, tzinfo=timezone.utc)
    log_path = os.path.join(out_dir, "all.log")
    truth_path = os.path.join(out_dir, "truth.jsonl")
    n_logical = 0
    n_physical = 0

    with open(log_path, "w") as logf, open(truth_path, "w") as truthf:
        for minute in range(minutes):
            faults = active_faults(minute)
            # Entries scheduled in this minute, then globally sorted by ts so
            # services interleave realistically.
            entries = []
            for service, svc in SERVICES.items():
                rate = svc["base_per_min"] * scale
                for f in faults:
                    rate *= f.get("rates", {}).get(service, 1.0)
                count = poisson(rate)
                ver = version_of(service, minute)
                for _ in range(count):
                    tpl = pick_template(service, minute, faults)
                    sec = RNG.random() * 60.0
                    entries.append((minute * 60 + sec, service, ver, tpl))
            entries.sort(key=lambda e: e[0])
            for sec, service, ver, tpl in entries:
                ts = start + timedelta(seconds=sec)
                body = render(service, tpl, minute, faults)
                n_logical += 1
                fault_id = faults[0]["id"] if faults else None
                style = SERVICES[service]["style"]
                if style == "json":
                    payload = json.dumps({
                        "ts": iso(ts), "service": service, "version": ver,
                        "level": tpl.level, "msg": body.replace("\n", " | "),
                    }, separators=(",", ":"))
                    lines = [payload]
                else:
                    header = f"{iso(ts)} {tpl.level} [{service}/{ver}] "
                    lines = [header + body.split("\n")[0]]
                    lines.extend(body.split("\n")[1:])
                for ln in lines:
                    logf.write(ln + "\n")
                    n_physical += 1
                truthf.write(json.dumps({
                    "line": n_logical,
                    "ts_ms": int(ts.timestamp() * 1000),
                    "service": service,
                    "version": ver,
                    "level": tpl.level,
                    "template": tpl.tid,
                    "multiline": tpl.multiline,
                    "fault": fault_id,
                }, separators=(",", ":")) + "\n")

    with open(os.path.join(out_dir, "faults.json"), "w") as f:
        json.dump([{
            "id": x["id"], "title": x["title"],
            "start_ms": int((start + timedelta(minutes=x["start_min"])).timestamp() * 1000),
            "end_ms": int((start + timedelta(minutes=x["end_min"])).timestamp() * 1000),
            "start_min": x["start_min"], "end_min": x["end_min"],
            "services": sorted({s for s in x.get("weights", {})} | ({x["deploy"]} if "deploy" in x else set())),
        } for x in FAULTS], f, indent=2)

    with open(os.path.join(out_dir, "meta.json"), "w") as f:
        json.dump({
            "minutes": minutes, "scale": scale,
            "logical_lines": n_logical, "physical_lines": n_physical,
            "services": {s: {"templates": [t.tid for t in v["tpls"]],
                             "base_per_min": v["base_per_min"]}
                         for s, v in SERVICES.items()},
        }, f, indent=2)
    print(f"wrote {n_logical} logical / {n_physical} physical lines to {log_path}")


if __name__ == "__main__":
    main()
