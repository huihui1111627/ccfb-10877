//! One processing shard: owns a Drain instance, per-template time series,
//! numeric metric series and the recent event ring. No sharing => no locking.

use crate::drain::Drain;
use crate::preprocess::build_record;
use crate::stats::{bucket_of, DetectorVerdict, MetricSeries, TimeSeries, BUCKET_MS};
use crate::types::{Event, EventKind, Level, LogRecord};
use crate::preprocess::RawLine;
use ahash::AHashMap;
use serde::{Deserialize, Serialize};

const MAX_EVENTS: usize = 2000;

#[derive(Debug, Clone, Copy)]
pub enum ShardMsgKind {
    Record,
    Tick(i64),
    #[allow(dead_code)]
    Shutdown,
}

pub struct ShardMsg {
    pub kind: ShardMsgKind,
    pub record: Option<LogRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardState {
    pub shard_id: usize,
    pub drain: Drain,
    /// cluster local id -> minute counts (JSON has no integer keys; store entries).
    #[serde(with = "crate::serde_util")]
    pub series: AHashMap<u32, TimeSeries>,
    /// (cluster id, numeric position) -> metric series (tuple keys need encoding).
    #[serde(with = "metric_map_serde")]
    pub metrics: AHashMap<(u32, u16), MetricSeries>,
    /// Distinct service names seen on this shard (id resolved via interner).
    pub service_ids: Vec<u32>,
    pub events: Vec<Event>,
    next_event_id: u64,
    /// Oldest data timestamp accepted (used to ignore stale replays).
    pub current_bucket: i64,
    pub total_records: u64,
    /// Evaluation mode: (source_line -> assigned cluster local id), capped.
    #[serde(skip)]
    pub assignments: Option<Vec<(u64, u32)>>,
}

/// Tuple-keyed metric map: persisted as an array of flat entries.
mod metric_map_serde {
    use ahash::AHashMap;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use crate::stats::MetricSeries;

    #[derive(Serialize, Deserialize)]
    struct Entry {
        cluster: u32,
        position: u16,
        series: MetricSeries,
    }

    pub fn serialize<S: Serializer>(
        map: &AHashMap<(u32, u16), MetricSeries>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_seq(map.iter().map(|((c, p), v)| Entry {
            cluster: *c,
            position: *p,
            series: v.clone(),
        }))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<AHashMap<(u32, u16), MetricSeries>, D::Error> {
        let entries = Vec::<Entry>::deserialize(d)?;
        Ok(entries
            .into_iter()
            .map(|e| ((e.cluster, e.position), e.series))
            .collect())
    }
}

impl ShardState {
    pub fn new(shard_id: usize) -> Self {
        let mut drain = Drain::new();
        drain.rebuild_after_load();
        Self {
            shard_id,
            drain,
            series: AHashMap::new(),
            metrics: AHashMap::new(),
            service_ids: Vec::new(),
            events: Vec::new(),
            next_event_id: 1,
            current_bucket: 0,
            total_records: 0,
            assignments: None,
        }
    }

    /// Turn on per-record assignment capture for offline evaluation.
    pub fn enable_assignments(&mut self) {
        self.assignments = Some(Vec::new());
    }

    pub fn handle(&mut self, msg: ShardMsg) -> bool {
        match msg.kind {
            ShardMsgKind::Shutdown => return false,
            ShardMsgKind::Tick(bucket) => self.evaluate_bucket(bucket),
            ShardMsgKind::Record => {
                if let Some(rec) = msg.record {
                    self.ingest(rec);
                }
            }
        }
        true
    }

    fn ingest(&mut self, mut rec: LogRecord) {
        // Staged record from a parser thread: header parsed, body not yet
        // tokenized because the interner lives on this shard.
        if rec.service == u32::MAX {
            rec = self.materialize(rec);
        }
        self.total_records += 1;
        if !self.service_ids.contains(&rec.service) {
            self.service_ids.push(rec.service);
        }
        let bucket = bucket_of(rec.ts_ms);
        if bucket < self.current_bucket {
            // Late record after a tick: still count it but don't re-alert.
        }
        self.current_bucket = self.current_bucket.max(bucket);

        let result = self.drain.add(
            &rec.tokens,
            rec.level,
            rec.service,
            rec.version,
            rec.ts_ms,
            &rec.raw,
        );
        let cid = result.local_id;

        if let Some(a) = self.assignments.as_mut() {
            if a.len() < 5_000_000 {
                a.push((rec.source_line, cid));
            }
        }

        let ts = self.series.entry(cid).or_insert_with(TimeSeries::new);
        ts.add(bucket, 1);

        // Numeric variables -> dedicated metric trends.
        for nv in &rec.numerics {
            let key = (cid, nv.pos);
            self.metrics
                .entry(key)
                .or_insert_with(MetricSeries::new)
                .observe(bucket, nv.value);
        }

        if result.is_new_cluster {
            self.maybe_emit_new_template(cid, bucket);
        }
        if !self.drain.cluster(cid).unwrap().announced {
            // Marking is separate from the first-event decision to keep ERROR
            // templates alerting immediately and others on second appearance.
            self.drain.cluster_mut(cid).unwrap().announced = true;
        }

        // Amortized same-version merge maintenance (~every 64th record call).
        let merges = self.drain.maybe_merge();
        for (keep, dropped) in merges {
            for d in dropped {
                if let Some(old) = self.series.remove(&d) {
                    let entry = self.series.entry(keep).or_insert_with(TimeSeries::new);
                    for (b, c) in old.points {
                        entry.add(b, c);
                    }
                }
                let dead: Vec<(u32, u16)> = self
                    .metrics
                    .keys()
                    .filter(|(c, _)| *c == d)
                    .copied()
                    .collect();
                for key in dead {
                    self.metrics.remove(&key);
                }
            }
        }
    }

    /// Intern the service/version names and tokenize the body with this
    /// shard's private interner.
    fn materialize(&mut self, staged: LogRecord) -> LogRecord {
        let raw = RawLine {
            ts_ms: staged.ts_ms,
            service: staged.service_name.clone(),
            version: staged.version_name.clone(),
            level: staged.level,
            body: staged.raw.clone(),
        };
        build_record(self.drain.interner_mut(), raw, &[], staged.source_line)
    }

    fn maybe_emit_new_template(&mut self, cid: u32, bucket: i64) {
        let cluster = self.drain.cluster(cid).unwrap();
        // ERROR/FATAL: alert on the very first line. Others: wait until the
        // second appearance so one-off debug noise doesn't page anyone.
        let immediate = matches!(cluster.level, Level::Error | Level::Fatal);
        if !immediate && cluster.count < 2 {
            return;
        }
        let event = self.build_event(
            cid,
            bucket,
            EventKind::NewTemplate,
            1.0,
            0.0,
            &format!(
                "new template first seen on version {}",
                self.drain.resolve(cluster.born_version)
            ),
        );
        self.push_event(event);
    }

    /// Run anomaly detection for a closed minute bucket.
    fn evaluate_bucket(&mut self, bucket: i64) {
        let cids: Vec<u32> = self.series.keys().copied().collect();
        for cid in cids {
            let verdict = self.series.get_mut(&cid).unwrap().evaluate(bucket);
            let Some(v) = verdict else { continue };
            let (kind, observed, baseline) = match v {
                DetectorVerdict::Spike { observed, baseline } => {
                    (EventKind::Spike, observed, baseline)
                }
                DetectorVerdict::Drop { observed, baseline } => {
                    // Suppress drops on a template that a newer-version template
                    // has already taken over (deploy/format change, not outage).
                    if self.is_superseded(cid) {
                        continue;
                    }
                    (EventKind::Drop, observed, baseline)
                }
            };
            let event =
                self.build_event(cid, bucket, kind, observed, baseline, "minute-bucket detector");
            self.push_event(event);
        }
        self.current_bucket = self.current_bucket.max(bucket);
    }

    /// True when another cluster born on a later version links to this one and
    /// is actively receiving traffic — i.e. the template was replaced on deploy.
    fn is_superseded(&self, cid: u32) -> bool {
        let Some(c) = self.drain.cluster(cid) else {
            return false;
        };
        for edge in &c.lineage {
            let Some(other) = self.drain.cluster(edge.other_local_id) else {
                continue;
            };
            if other.born_version != c.born_version
                && other.last_seen_ms > c.last_seen_ms
                && other.count >= 10
            {
                return true;
            }
        }
        false
    }

    fn build_event(
        &self,
        cid: u32,
        bucket: i64,
        kind: EventKind,
        observed: f64,
        baseline: f64,
        detail: &str,
    ) -> Event {
        let cluster = self.drain.cluster(cid).expect("cluster exists");
        Event {
            id: self.next_event_id,
            ts_ms: bucket,
            service: self.drain.resolve(cluster.service_id).to_string(),
            template_id: format!("s{}-t{}", self.shard_id, cid),
            kind,
            level: cluster.level.as_str().to_string(),
            detail: detail.to_string(),
            template_text: self.drain.template_text(cluster),
            observed,
            baseline,
        }
    }

    fn push_event(&mut self, mut e: Event) {
        e.id = self.next_event_id;
        self.next_event_id += 1;
        self.events.push(e);
        if self.events.len() > MAX_EVENTS {
            let cut = self.events.len() - MAX_EVENTS;
            self.events.drain(..cut);
        }
    }

    #[allow(dead_code)]
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Snapshot to JSON for persistence.
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        let tmp = path.with_extension("json.tmp");
        let data = serde_json::to_vec(self).map_err(std::io::Error::other)?;
        std::fs::write(&tmp, data)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        let data = std::fs::read(path)?;
        let mut state: ShardState =
            serde_json::from_slice(&data).map_err(std::io::Error::other)?;
        state.drain.rebuild_after_load();
        Ok(state)
    }
}

/// Message routing: deterministic shard by service name.
///
/// Uses fixed-seed FNV-1a (not ahash) so that a service lands on the *same*
/// shard after a restart — otherwise its interner and Drain state would be
/// split across shards and every template would be learned twice.
pub fn shard_for(service: &str, shards: usize) -> usize {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for b in service.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    (hash as usize) % shards
}

/// The bucket a live ticker should evaluate next.
#[allow(dead_code)]
pub fn next_bucket(now_ms: i64) -> i64 {
    bucket_of(now_ms) - BUCKET_MS
}
