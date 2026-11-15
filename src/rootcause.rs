//! Root-cause clues: correlate per-template changes across services around an
//! event time window and rank the likely origin.

use crate::shard::ShardState;
use crate::types::{Event, EventKind, Level};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

const WINDOW_BEFORE_MS: i64 = 2 * 60_000;
const WINDOW_AFTER_MS: i64 = 8 * 60_000;

#[derive(Debug, Serialize)]
pub struct RootCauseReport {
    pub anchor_event_id: u64,
    pub anchor_service: String,
    pub anchor_time_ms: i64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    /// Ranked suspects, highest score first.
    pub suspects: Vec<Suspect>,
    /// Per-service change counts inside the window.
    pub service_activity: Vec<ServiceActivity>,
    pub reasoning: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Suspect {
    pub service: String,
    pub template_id: String,
    pub template_text: String,
    pub score: f64,
    pub lead_ms: i64,
    pub kinds: Vec<String>,
    pub max_severity: String,
    pub explanation: String,
}

#[derive(Debug, Serialize)]
pub struct ServiceActivity {
    pub service: String,
    pub spikes: usize,
    pub drops: usize,
    pub new_templates: usize,
    pub error_events: usize,
}

pub fn analyze(states: &[Arc<Mutex<ShardState>>], anchor: &Event) -> RootCauseReport {
    let start = anchor.ts_ms - WINDOW_BEFORE_MS;
    let end = anchor.ts_ms + WINDOW_AFTER_MS;

    // Gather every event in the window across all shards (owned clones; the
    // mutex guards cannot outlive this loop).
    let window_events: Vec<Event> = states
        .iter()
        .flat_map(|st| {
            st.lock()
                .events
                .iter()
                .filter(|e| e.ts_ms >= start && e.ts_ms <= end)
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect();

    // Aggregate per (service, template).
    let mut groups: BTreeMap<(String, String), GroupAgg> = BTreeMap::new();
    let mut activity: BTreeMap<String, ServiceActivity> = BTreeMap::new();

    for e in &window_events {
        let g = groups
            .entry((e.service.clone(), e.template_id.clone()))
            .or_insert_with(|| GroupAgg {
                template_text: e.template_text.clone(),
                level: e.level.clone(),
                earliest: e.ts_ms,
                kinds: Vec::new(),
            });
        g.earliest = g.earliest.min(e.ts_ms);
        if !g.kinds.contains(&e.kind) {
            g.kinds.push(e.kind);
        }
        if severity(&e.level) > severity(&g.level) {
            g.level = e.level.clone();
        }

        let a = activity.entry(e.service.clone()).or_insert_with(|| {
            ServiceActivity {
                service: e.service.clone(),
                spikes: 0,
                drops: 0,
                new_templates: 0,
                error_events: 0,
            }
        });
        match e.kind {
            EventKind::Spike => a.spikes += 1,
            EventKind::Drop => a.drops += 1,
            EventKind::NewTemplate => a.new_templates += 1,
        }
        if matches!(e.level.as_str(), "ERROR" | "FATAL") {
            a.error_events += 1;
        }
    }

    let mut suspects: Vec<Suspect> = groups
        .into_iter()
        .map(|((service, template_id), g)| {
            let lead_ms = anchor.ts_ms - g.earliest; // positive = happened before anchor
            let score = score_group(&g, lead_ms, service == anchor.service);
            let explanation = explain(&g, lead_ms, &service, anchor);
            Suspect {
                service,
                template_id,
                template_text: g.template_text,
                score,
                lead_ms,
                kinds: g.kinds.iter().map(|k| k.as_str().to_string()).collect(),
                max_severity: g.level,
                explanation,
            }
        })
        .collect();
    suspects.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    suspects.truncate(15);

    let mut service_activity: Vec<ServiceActivity> = activity.into_values().collect();
    service_activity.sort_by(|a, b| b.error_events.cmp(&a.error_events).then(b.spikes.cmp(&a.spikes)));

    let reasoning = build_reasoning(anchor, &suspects, &service_activity);

    RootCauseReport {
        anchor_event_id: anchor.id,
        anchor_service: anchor.service.clone(),
        anchor_time_ms: anchor.ts_ms,
        window_start_ms: start,
        window_end_ms: end,
        suspects,
        service_activity,
        reasoning,
    }
}

struct GroupAgg {
    template_text: String,
    level: String,
    earliest: i64,
    kinds: Vec<EventKind>,
}

fn severity(level: &str) -> u32 {
    Level::parse(level).weight()
}

/// Scoring (documented trade-off):
/// - error/fatal weight 5, warn 2, info 1
/// - templates changing *before* the anchor get a lead bonus (likely upstream cause)
/// - new templates in the window score higher than plain spikes
/// - same-service changes get a small locality bonus
fn score_group(g: &GroupAgg, lead_ms: i64, same_service: bool) -> f64 {
    let sev = severity(&g.level) as f64;
    let kind_bonus: f64 = g
        .kinds
        .iter()
        .map(|k| match k {
            EventKind::NewTemplate => 3.0,
            EventKind::Spike => 2.0,
            EventKind::Drop => 1.5,
        })
        .sum();
    let lead_bonus = if lead_ms > 0 {
        (lead_ms as f64 / 60_000.0).min(3.0)
    } else {
        0.0
    };
    let locality = if same_service { 1.0 } else { 0.0 };
    sev + kind_bonus + lead_bonus + locality
}

fn explain(g: &GroupAgg, lead_ms: i64, service: &str, anchor: &Event) -> String {
    let when = if lead_ms > 0 {
        format!("changed {}s before the anchor event", lead_ms / 1000)
    } else {
        format!("changed {}s after the anchor event", -lead_ms / 1000)
    };
    let kinds: Vec<&str> = g.kinds.iter().map(|k| k.as_str()).collect();
    format!(
        "service `{service}` (severity {}) {} [{}]; anchor service is `{}`",
        g.level,
        when,
        kinds.join("+"),
        anchor.service
    )
}

fn build_reasoning(
    anchor: &Event,
    suspects: &[Suspect],
    activity: &[ServiceActivity],
) -> Vec<String> {
    let mut out = Vec::new();
    out.push(format!(
        "Anchor: {} on service `{}` at {} (template `{}`).",
        anchor.kind.as_str(),
        anchor.service,
        anchor.ts_ms,
        anchor.template_text
    ));
    if let Some(top) = suspects.first() {
        out.push(format!(
            "Highest-ranked suspect: `{}` / {} — {}.",
            top.service, top.template_id, top.explanation
        ));
    }
    let upstream: Vec<&ServiceActivity> = activity
        .iter()
        .filter(|a| a.service != anchor.service && a.error_events > 0)
        .collect();
    if !upstream.is_empty() {
        let names: Vec<&str> = upstream.iter().map(|a| a.service.as_str()).collect();
        out.push(format!(
            "Other services with error-level changes in the window: {}.",
            names.join(", ")
        ));
    }
    if suspects.iter().all(|s| s.service == anchor.service) {
        out.push("No cross-service changes detected in the window; the cause is likely inside this service or its dependency calls.".to_string());
    }
    out
}
