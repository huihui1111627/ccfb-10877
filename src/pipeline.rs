//! The threaded processing pipeline:
//!
//! ```text
//! reader (1)  -- physical lines + multiline assembly -->
//! parsers (P) -- parse headers, intern service/version via global interner -->
//! shard workers (S) -- tokenize with shard interner, Drain, stats, detector
//! ```
//!
//! Channels are bounded `std::sync::mpsc` queues; backpressure propagates for
//! free when the shards are saturated.

use crate::preprocess::{build_record, is_entry_start, parse_line, RawLine};
use crate::shard::{shard_for, ShardMsg, ShardMsgKind, ShardState};
use crate::stats::{bucket_of, BUCKET_MS};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHAN_CAP: usize = 4096;

pub struct PipelineConfig {
    pub shards: usize,
    pub parsers: usize,
    pub state_dir: String,
    pub persist_interval: Duration,
}

/// Header parsed by a parser thread; multiline extras travel alongside.
#[allow(dead_code)]
struct ParsedEntryMarker;

enum SourceItem {
    Line(PhysicalLine),
    Eof,
}

pub struct PhysicalLine {
    pub no: u64,
    pub text: String,
}

struct PendingEntry {
    primary_no: u64,
    assembled: String,
}

pub struct Pipeline {
    pub states: Vec<Arc<Mutex<ShardState>>>,
    /// Global service/version interner; ids are passed through to shards and
    /// re-interned there (strings are few and long-lived).
    pub names: Arc<Mutex<GlobalNames>>,
    last_tick_bucket: Arc<Mutex<i64>>,
    parser_txs: Vec<SyncSender<SourceItem>>,
    #[allow(dead_code)]
    worker_handles: Vec<std::thread::JoinHandle<()>>,
    #[allow(dead_code)]
    parser_handles: Vec<std::thread::JoinHandle<()>>,
    /// Set once every parser thread has drained its input.
    parsers_done: Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>,
    parser_count: usize,
    config: PipelineConfig,
}

#[derive(Default)]
pub struct GlobalNames {
    pub services: HashSet<String>,
    pub versions: HashSet<String>,
}

impl Pipeline {
    pub fn start(config: PipelineConfig) -> std::io::Result<Self> {
        std::fs::create_dir_all(&config.state_dir)?;
        let states = (0..config.shards)
            .map(|i| {
                let path = Path::new(&config.state_dir).join(format!("shard-{i}.json"));
                let state = if path.exists() {
                    ShardState::load(&path).unwrap_or_else(|e| {
                        eprintln!("warning: failed to load {path:?}: {e}, starting fresh");
                        ShardState::new(i)
                    })
                } else {
                    ShardState::new(i)
                };
                Arc::new(Mutex::new(state))
            })
            .collect::<Vec<_>>();

        let mut shard_txs = Vec::new();
        let mut worker_handles = Vec::new();
        for (i, state) in states.iter().enumerate() {
            let (tx, rx) = sync_channel::<ShardMsg>(CHAN_CAP);
            shard_txs.push(tx);
            let state = state.clone();
            let dir = config.state_dir.clone();
            worker_handles.push(
                std::thread::Builder::new()
                    .name(format!("shard-{i}"))
                    .spawn(move || shard_loop(i, rx, state, &dir))
                    .unwrap(),
            );
        }

        let mut parser_txs = Vec::new();
        let mut parser_handles = Vec::new();
        let done = Arc::new((std::sync::Mutex::new(0usize), std::sync::Condvar::new()));
        for p in 0..config.parsers {
            let (tx, rx) = sync_channel::<SourceItem>(CHAN_CAP);
            parser_txs.push(tx);
            let shard_txs = shard_txs.clone();
            let shards = config.shards;
            let done = done.clone();
            parser_handles.push(
                std::thread::Builder::new()
                    .name(format!("parser-{p}"))
                    .spawn(move || parser_loop(rx, shard_txs, shards, done))
                    .unwrap(),
            );
        }
        let parser_count = config.parsers;

        Ok(Pipeline {
            states,
            names: Arc::new(Mutex::new(GlobalNames::default())),
            last_tick_bucket: Arc::new(Mutex::new(0)),
            parser_txs,
            worker_handles,
            parser_handles,
            parsers_done: done,
            parser_count,
            config,
        })
    }

    pub fn parser_count(&self) -> usize {
        self.parser_txs.len()
    }

    pub fn send_line(&self, idx: usize, line: PhysicalLine) {
        let _ = self.parser_txs[idx].send(SourceItem::Line(line));
    }

    fn send_eof(&self) {
        for tx in &self.parser_txs {
            let _ = tx.send(SourceItem::Eof);
        }
    }

    /// Bench mode: replay a file as fast as possible.
    pub fn ingest_file(&self, path: &Path) -> std::io::Result<IngestStats> {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(1 << 20, file);
        let start = Instant::now();
        let mut logical = 0u64;
        let mut physical = 0u64;
        let mut pending: Option<PendingEntry> = None;
        let mut next_parser = 0usize;
        let mut names = self.names.lock();

        let flush = |p: &mut Option<PendingEntry>,
                         next_parser: &mut usize,
                         txs: &[SyncSender<SourceItem>],
                         names: &mut GlobalNames| {
            if let Some(entry) = p.take() {
                if let Some(raw) = parse_line(&entry.assembled) {
                    names.services.insert(raw.service.clone());
                    names.versions.insert(raw.version.clone());
                }
                let idx = *next_parser % txs.len();
                *next_parser += 1;
                let _ = txs[idx].send(SourceItem::Line(PhysicalLine {
                    no: entry.primary_no,
                    text: entry.assembled,
                }));
            }
        };

        for line in reader.lines() {
            let line = line?;
            physical += 1;
            if is_entry_start(&line) {
                flush(&mut pending, &mut next_parser, &self.parser_txs, &mut names);
                logical += 1;
                pending = Some(PendingEntry {
                    primary_no: logical,
                    assembled: line,
                });
            } else if let Some(entry) = pending.as_mut() {
                entry.assembled.push('\n');
                entry.assembled.push_str(&line);
            }
        }
        flush(&mut pending, &mut next_parser, &self.parser_txs, &mut names);
        drop(names);
        self.send_eof();
        Ok(IngestStats {
            logical_lines: logical,
            physical_lines: physical,
            elapsed: start.elapsed(),
        })
    }

    /// Bench mode: deterministic per-minute ticks across every observed bucket.
    pub fn run_deterministic_ticks(&self, min_ts: i64, max_ts: i64) {
        let mut b = bucket_of(min_ts) + BUCKET_MS;
        let end = bucket_of(max_ts);
        while b <= end {
            for state in &self.states {
                state.lock().handle(ShardMsg {
                    kind: ShardMsgKind::Tick(b),
                    record: None,
                });
            }
            *self.last_tick_bucket.lock() = b;
            b += BUCKET_MS;
        }
    }

    /// Live mode: wall-clock ticker + periodic persistence.
    pub fn spawn_services(self: &Arc<Self>) {
        let states = self.states.clone();
        let last = self.last_tick_bucket.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(10));
            let now = now_ms();
            let bucket = bucket_of(now) - BUCKET_MS;
            if *last.lock() >= bucket {
                continue;
            }
            for state in &states {
                state.lock().handle(ShardMsg {
                    kind: ShardMsgKind::Tick(bucket),
                    record: None,
                });
            }
            *last.lock() = bucket;
        });

        let states = self.states.clone();
        let dir = self.config.state_dir.clone();
        let interval = self.config.persist_interval;
        std::thread::spawn(move || loop {
            std::thread::sleep(interval);
            for (i, state) in states.iter().enumerate() {
                let path = Path::new(&dir).join(format!("shard-{i}.json"));
                if let Err(e) = state.lock().save(&path) {
                    eprintln!("persist failed for shard {i}: {e}");
                }
            }
        });
    }

    pub fn persist_now(&self) {
        for (i, state) in self.states.iter().enumerate() {
            let path = Path::new(&self.config.state_dir).join(format!("shard-{i}.json"));
            if let Err(e) = state.lock().save(&path) {
                eprintln!("persist shard {i} to {path:?} failed: {e}");
            }
        }
    }

    /// Block until all parser threads have finished, giving shard workers time
    /// to drain their queues, then snapshot every shard.
    pub fn wait_idle_and_persist(&self, expected_records: u64) {
        let (lock, cvar) = &*self.parsers_done;
        let mut done = lock.lock().unwrap();
        while *done < self.parser_count {
            done = cvar.wait(done).unwrap();
        }
        drop(done);
        // Parsers are done; shard channels now only contain queued records.
        // Wait until the shards report the expected total (bounded poll).
        for _ in 0..600 {
            if self.total_records() >= expected_records {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        // One extra grace period for in-flight processing.
        std::thread::sleep(Duration::from_millis(100));
        self.persist_now();
    }

    pub fn total_records(&self) -> u64 {
        self.states.iter().map(|s| s.lock().total_records).sum()
    }
}

#[derive(Debug, Clone)]
pub struct IngestStats {
    pub logical_lines: u64,
    pub physical_lines: u64,
    pub elapsed: Duration,
}

fn shard_loop(
    i: usize,
    rx: Receiver<ShardMsg>,
    state: Arc<Mutex<ShardState>>,
    state_dir: &str,
) {
    while let Ok(msg) = rx.recv() {
        let mut g = state.lock();
        let keep_going = g.handle(msg);
        if !keep_going {
            break;
        }
    }
    let path = Path::new(state_dir).join(format!("shard-{i}.json"));
    let _ = state.lock().save(&path);
}

fn parser_loop(
    rx: Receiver<SourceItem>,
    shard_txs: Vec<SyncSender<ShardMsg>>,
    shards: usize,
    done: Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>,
) {
    while let Ok(item) = rx.recv() {
        let SourceItem::Line(pl) = item else { break; };
        let Some(raw) = parse_line(&pl.text) else {
            continue;
        };
        let target = shard_for(&raw.service, shards);
        let extra: Vec<String> = pl.text.split('\n').skip(1).map(str::to_string).collect();
        let msg = ShardMsg {
            kind: ShardMsgKind::Record,
            record: Some(StagedRecord {
                raw,
                extra,
                source_line: pl.no,
            }
            .into_record()),
        };
        if shard_txs[target].send(msg).is_err() {
            break;
        }
    }
    // All inputs consumed and forwarded. Shard workers keep running (live
    // sources may arrive later); bench mode snapshots explicitly afterwards.
    let (lock, cvar) = &*done;
    let mut n = lock.lock().unwrap();
    *n += 1;
    cvar.notify_all();
}

/// Staged payload: header parsed, body not yet tokenized (needs shard interner).
pub struct StagedRecord {
    pub raw: RawLine,
    pub extra: Vec<String>,
    pub source_line: u64,
}

impl StagedRecord {
    /// Encode into a LogRecord with sentinel ids; the shard interns the names
    /// and tokenizes the body in `ShardState::ingest`.
    fn into_record(self) -> crate::types::LogRecord {
        let mut full = self.raw.body;
        for e in &self.extra {
            full.push('\n');
            full.push_str(e);
        }
        crate::types::LogRecord {
            ts_ms: self.raw.ts_ms,
            service: u32::MAX, // sentinel: materialize on the shard
            version: u32::MAX,
            level: self.raw.level,
            tokens: smallvec::smallvec![],
            numerics: Default::default(),
            raw: full,
            source_line: self.source_line,
            service_name: self.raw.service,
            version_name: self.raw.version,
        }
    }
}

/// Re-export so shard.rs can build records without a cyclic module dependency.
#[allow(dead_code)]
pub fn finalize_record(
    interner: &mut crate::interner::Interner,
    service: &str,
    version: &str,
    level: crate::types::Level,
    ts_ms: i64,
    body: &str,
    source_line: u64,
) -> crate::types::LogRecord {
    let staged = RawLine {
        ts_ms,
        service: service.to_string(),
        version: version.to_string(),
        level,
        body: body.to_string(),
    };
    build_record(interner, staged, &[], source_line)
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Live TCP source: `nc host 5050 < app.log` or a fluentbit/vector forwarder.
pub fn run_tcp_source(addr: &str, pipeline: Arc<Pipeline>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("TCP log source listening on {addr}");
    for stream in listener.incoming() {
        let mut stream = stream?;
        let pipeline = pipeline.clone();
        std::thread::spawn(move || {
            let reader = BufReader::new(&mut stream);
            let mut logical = 0u64;
            let mut pending: Option<PendingEntry> = None;
            let mut next_parser = 0usize;
            let mut names = pipeline.names.lock();
            let mut flush = |p: &mut Option<PendingEntry>| {
                if let Some(entry) = p.take() {
                    if let Some(raw) = parse_line(&entry.assembled) {
                        names.services.insert(raw.service.clone());
                        names.versions.insert(raw.version.clone());
                    }
                    let idx = next_parser % pipeline.parser_count();
                    next_parser += 1;
                    pipeline.send_line(
                        idx,
                        PhysicalLine {
                            no: entry.primary_no,
                            text: entry.assembled,
                        },
                    );
                }
            };
            for line in reader.lines().flatten() {
                if is_entry_start(&line) {
                    flush(&mut pending);
                    logical += 1;
                    pending = Some(PendingEntry {
                        primary_no: logical,
                        assembled: line,
                    });
                } else if let Some(e) = pending.as_mut() {
                    e.assembled.push('\n');
                    e.assembled.push_str(&line);
                }
            }
            flush(&mut pending);
        });
    }
    Ok(())
}
