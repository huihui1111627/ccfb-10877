//! HTTP API + embedded single-page UI.

use crate::rootcause::{self, RootCauseReport};
use crate::shard::ShardState;
use crate::types::EventKind;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, Json},
    routing::get,
    Router,
};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct AppState {
    pub shards: Arc<Vec<Arc<Mutex<ShardState>>>>,
    pub started_at_ms: i64,
    pub state_dir: String,
    pub tcp_addr: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/overview", get(overview))
        .route("/api/templates", get(templates))
        .route("/api/template/{id}", get(template_detail))
        .route("/api/timeseries", get(timeseries))
        .route("/api/metrics", get(metrics))
        .route("/api/events", get(events))
        .route("/api/rootcause/{event_id}", get(root_cause))
        .route("/api/services", get(services))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

#[derive(Serialize)]
struct Overview {
    now_ms: i64,
    started_at_ms: i64,
    shards: usize,
    total_records: u64,
    total_templates: usize,
    active_services: Vec<String>,
    events_total: usize,
    events_last_hour: usize,
    state_dir: String,
    tcp_addr: Option<String>,
    ingest_rate_per_sec: f64,
}

async fn overview(State(st): State<AppState>) -> Json<Overview> {
    let now = now_ms();
    let hour_ago = now - 3_600_000;
    let mut total_records = 0u64;
    let mut total_templates = 0usize;
    let mut events_total = 0usize;
    let mut events_last_hour = 0usize;
    let mut names = std::collections::BTreeSet::new();
    for s in st.shards.iter() {
        let g = s.lock();
        total_records += g.total_records;
        total_templates += g.drain.clusters().count();
        events_total += g.events.len();
        events_last_hour += g.events.iter().filter(|e| e.ts_ms >= hour_ago).count();
        for id in &g.service_ids {
            names.insert(g.drain.resolve(*id).to_string());
        }
    }
    let elapsed = (now - st.started_at_ms).max(1) as f64 / 1000.0;
    Json(Overview {
        now_ms: now,
        started_at_ms: st.started_at_ms,
        shards: st.shards.len(),
        total_records,
        total_templates,
        active_services: names.into_iter().collect(),
        events_total,
        events_last_hour,
        state_dir: st.state_dir.clone(),
        tcp_addr: st.tcp_addr.clone(),
        ingest_rate_per_sec: total_records as f64 / elapsed,
    })
}

#[derive(serde::Deserialize)]
struct TemplatesQuery {
    service: Option<String>,
    q: Option<String>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct TemplateDto {
    template_id: String,
    service: String,
    text: String,
    level: String,
    count: u64,
    first_seen_ms: i64,
    last_seen_ms: i64,
    versions: Vec<String>,
    born_version: String,
    samples: Vec<String>,
    lineage: Vec<LineageDto>,
}

#[derive(Serialize)]
struct LineageDto {
    other_template_id: String,
    kind: String,
    similarity: f64,
}

async fn templates(
    State(st): State<AppState>,
    Query(q): Query<TemplatesQuery>,
) -> Json<Vec<TemplateDto>> {
    let limit = q.limit.unwrap_or(500).min(5000);
    let mut out = Vec::new();
    for s in st.shards.iter() {
        let g = s.lock();
        for c in g.drain.clusters() {
            let service = g.drain.resolve(c.service_id).to_string();
            if let Some(f) = &q.service {
                if &service != f {
                    continue;
                }
            }
            let text = g.drain.template_text(c);
            if let Some(needle) = &q.q {
                if !text.contains(needle.as_str()) {
                    continue;
                }
            }
            out.push(to_dto(&g, c));
        }
    }
    out.sort_by(|a, b| b.count.cmp(&a.count));
    out.truncate(limit);
    Json(out)
}

fn to_dto(g: &ShardState, c: &crate::drain::Cluster) -> TemplateDto {
    TemplateDto {
        template_id: format!("s{}-t{}", g.shard_id, c.local_id),
        service: g.drain.resolve(c.service_id).to_string(),
        text: g.drain.template_text(c),
        level: c.level.as_str().to_string(),
        count: c.count,
        first_seen_ms: c.first_seen_ms,
        last_seen_ms: c.last_seen_ms,
        versions: c
            .versions
            .keys()
            .map(|id| g.drain.resolve(*id).to_string())
            .collect(),
        born_version: g.drain.resolve(c.born_version).to_string(),
        samples: c.samples.iter().cloned().collect(),
        lineage: c
            .lineage
            .iter()
            .map(|e| LineageDto {
                other_template_id: format!("s{}-t{}", g.shard_id, e.other_local_id),
                kind: format!("{:?}", e.kind),
                similarity: e.sim as f64 / 1000.0,
            })
            .collect(),
    }
}

async fn template_detail(
    State(st): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let (sid, cid) = parse_template_id(&id).ok_or(StatusCode::BAD_REQUEST)?;
    let shard = st.shards.get(sid).ok_or(StatusCode::NOT_FOUND)?;
    let g = shard.lock();
    let c = g.drain.cluster(cid).ok_or(StatusCode::NOT_FOUND)?;
    let dto = to_dto(&g, c);
    let series = g.series.get(&cid).map(|ts| ts.points.clone()).unwrap_or_default();
    let metrics: Vec<serde_json::Value> = g
        .metrics
        .iter()
        .filter(|((cluster, _), _)| *cluster == cid)
        .map(|((_, pos), m)| {
            serde_json::json!({
                "position": pos,
                "p95": m.p95.quantile(),
                "points": m.points.iter().map(|(b, agg)| serde_json::json!({
                    "bucket_ms": b,
                    "count": agg.count,
                    "avg": agg.sum / agg.count as f64,
                    "min": agg.min,
                    "max": agg.max,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(Json(serde_json::json!({
        "template": dto,
        "timeseries": series,
        "metrics": metrics,
    })))
}

#[derive(serde::Deserialize)]
struct TsQuery {
    service: Option<String>,
    template_id: Option<String>,
}

async fn timeseries(
    State(st): State<AppState>,
    Query(q): Query<TsQuery>,
) -> Json<serde_json::Value> {
    let mut per_template: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
    let mut totals: BTreeMap<i64, u64> = BTreeMap::new();
    for s in st.shards.iter() {
        let g = s.lock();
        for (cid, ts) in &g.series {
            let c = match g.drain.cluster(*cid) {
                Some(c) => c,
                None => continue,
            };
            let service = g.drain.resolve(c.service_id).to_string();
            if let Some(f) = &q.service {
                if &service != f {
                    continue;
                }
            }
            let tid = format!("s{}-t{}", g.shard_id, cid);
            if let Some(f) = &q.template_id {
                if &tid != f {
                    continue;
                }
            }
            per_template.insert(tid, ts.points.clone());
            for (b, n) in &ts.points {
                *totals.entry(*b).or_default() += n;
            }
        }
    }
    let mut total_points: Vec<(i64, u64)> = totals.into_iter().collect();
    total_points.sort();
    Json(serde_json::json!({
        "totals": total_points,
        "templates": per_template,
    }))
}

async fn metrics(State(st): State<AppState>) -> Json<serde_json::Value> {
    let mut out = Vec::new();
    for s in st.shards.iter() {
        let g = s.lock();
        for ((cid, pos), m) in &g.metrics {
            if let Some(c) = g.drain.cluster(*cid) {
                out.push(serde_json::json!({
                    "template_id": format!("s{}-t{}", g.shard_id, cid),
                    "service": g.drain.resolve(c.service_id),
                    "position": pos,
                    "p95": m.p95.quantile(),
                    "points": m.points.iter().map(|(b, agg)| serde_json::json!({
                        "bucket_ms": b,
                        "count": agg.count,
                        "avg": agg.sum / agg.count as f64,
                        "min": agg.min,
                        "max": agg.max,
                    })).collect::<Vec<_>>(),
                }));
            }
        }
    }
    Json(serde_json::Value::Array(out))
}

#[derive(serde::Deserialize)]
struct EventsQuery {
    service: Option<String>,
    kind: Option<String>,
    limit: Option<usize>,
}

async fn events(
    State(st): State<AppState>,
    Query(q): Query<EventsQuery>,
) -> Json<Vec<crate::types::Event>> {
    let limit = q.limit.unwrap_or(200).min(2000);
    let mut all = Vec::new();
    for s in st.shards.iter() {
        let g = s.lock();
        for e in &g.events {
            if let Some(f) = &q.service {
                if &e.service != f {
                    continue;
                }
            }
            if let Some(f) = &q.kind {
                if e.kind.as_str() != f.as_str() {
                    continue;
                }
            }
            all.push(e.clone());
        }
    }
    all.sort_by(|a, b| b.ts_ms.cmp(&a.ts_ms));
    all.truncate(limit);
    Json(all)
}

async fn root_cause(
    State(st): State<AppState>,
    axum::extract::Path(event_id): axum::extract::Path<u64>,
) -> Result<Json<RootCauseReport>, (StatusCode, String)> {
    let anchor = st
        .shards
        .iter()
        .find_map(|s| s.lock().events.iter().find(|e| e.id == event_id).cloned())
        .ok_or_else(|| (StatusCode::NOT_FOUND, "event not found".to_string()))?;
    Ok(Json(rootcause::analyze(&st.shards, &anchor)))
}

async fn services(State(st): State<AppState>) -> Json<Vec<serde_json::Value>> {
    let mut map: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for s in st.shards.iter() {
        let g = s.lock();
        for id in &g.service_ids {
            let name = g.drain.resolve(*id).to_string();
            let templates = g
                .drain
                .clusters()
                .filter(|c| g.drain.resolve(c.service_id) == name)
                .count();
            let events = g
                .events
                .iter()
                .filter(|e| e.service == name)
                .count();
            map.entry(name.clone())
                .or_insert(serde_json::json!({
                    "service": name,
                    "templates": templates,
                    "events": events,
                }));
        }
    }
    Json(map.into_values().collect())
}

fn parse_template_id(id: &str) -> Option<(usize, u32)> {
    let rest = id.strip_prefix('s')?;
    let (sid, cid) = rest.split_once("-t")?;
    Some((sid.parse().ok()?, cid.parse().ok()?))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Keep the enum referenced for documentation endpoints.
#[allow(dead_code)]
fn kind_name(k: EventKind) -> &'static str {
    k.as_str()
}
