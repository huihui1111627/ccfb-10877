mod drain;
mod interner;
mod pipeline;
mod preprocess;
mod rootcause;
mod serde_util;
mod shard;
mod stats;
mod types;
mod web;

use clap::{Parser, Subcommand};
use pipeline::Pipeline;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "logscope", version, about = "Automatic log pattern analysis platform")]
struct Cli {
    /// Directory for state snapshots.
    #[arg(long, default_value = "data/state")]
    state_dir: String,
    /// Number of Drain shards (one worker thread each).
    #[arg(long, default_value_t = 8)]
    shards: usize,
    /// Number of parser threads.
    #[arg(long, default_value_t = 4)]
    parsers: usize,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the web UI plus a live TCP log source.
    Serve {
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen: String,
        #[arg(long, default_value = "0.0.0.0:5050")]
        tcp: String,
    },
    /// Bulk-replay a log file, run deterministic detection, print a summary.
    Bench {
        /// Log file (plain text or JSON lines).
        file: String,
        /// Write machine-readable summary to this path (JSON).
        #[arg(long)]
        report: Option<String>,
        /// Skip state persistence between runs (fresh templates every run).
        #[arg(long)]
        fresh: bool,
        /// Write per-record cluster assignments (JSONL) for accuracy evaluation.
        #[arg(long)]
        assignments: Option<String>,
        /// Write detected events (JSONL) for fault-detection evaluation.
        #[arg(long)]
        events_out: Option<String>,
    },
    /// Print template stats for an existing state directory and exit.
    Inspect,
}

fn main() -> anyhow_lite::Result<()> {
    let cli = Cli::parse();
    let state_dir = cli.state_dir.clone();
    // --fresh: start from an empty template library in the same state dir.
    if matches!(cli.cmd, Cmd::Bench { fresh: true, .. }) {
        let _ = std::fs::remove_dir_all(&state_dir);
    }

    let config = pipeline::PipelineConfig {
        shards: cli.shards.max(1),
        parsers: cli.parsers.max(1),
        state_dir: state_dir.clone(),
        persist_interval: Duration::from_secs(30),
    };

    match cli.cmd {
        Cmd::Serve { listen, tcp } => serve(config, listen, tcp, state_dir),
        Cmd::Bench {
            file,
            report,
            fresh: _,
            assignments,
            events_out,
        } => bench(config, file, report, state_dir, assignments, events_out),
        Cmd::Inspect => inspect(config),
    }
}

fn serve(
    config: pipeline::PipelineConfig,
    listen: String,
    tcp: String,
    state_dir: String,
) -> anyhow_lite::Result<()> {
    let pipeline = Arc::new(Pipeline::start(config)?);
    pipeline.spawn_services();

    let tcp_pipeline = pipeline.clone();
    let tcp_addr = tcp.clone();
    std::thread::spawn(move || {
        if let Err(e) = pipeline::run_tcp_source(&tcp_addr, tcp_pipeline) {
            eprintln!("TCP source stopped: {e}");
        }
    });

    let app_state = web::AppState {
        shards: Arc::new(pipeline.states.clone()),
        started_at_ms: now_ms(),
        state_dir,
        tcp_addr: Some(tcp),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let app = web::router(app_state);
        let listener = tokio::net::TcpListener::bind(&listen).await?;
        eprintln!("logscope UI on http://{listen}");
        axum::serve(listener, app).await?;
        Ok::<(), std::io::Error>(())
    })?;
    pipeline.persist_now();
    Ok(())
}
fn bench(
    config: pipeline::PipelineConfig,
    file: String,
    report: Option<String>,
    state_dir: String,
    assignments: Option<String>,
    events_out: Option<String>,
) -> anyhow_lite::Result<()> {
    let pipeline = Pipeline::start(config)?;
    if assignments.is_some() {
        for s in &pipeline.states {
            s.lock().enable_assignments();
        }
    }
    let path = Path::new(&file);
    let stats = pipeline.ingest_file(path)?;

    // Determine the data time range for deterministic ticks.
    let (min_ts, max_ts) = {
        let mut min = i64::MAX;
        let mut max = 0i64;
        for s in &pipeline.states {
            let g = s.lock();
            for c in g.drain.clusters() {
                min = min.min(c.first_seen_ms);
                max = max.max(c.last_seen_ms);
            }
        }
        (min, max)
    };
    if min_ts != i64::MAX {
        pipeline.run_deterministic_ticks(min_ts, max_ts);
    }

    let mut templates = 0usize;
    let mut events = 0usize;
    let mut all_events = Vec::new();
    for s in &pipeline.states {
        let g = s.lock();
        templates += g.drain.clusters().count();
        events += g.events.len();
        all_events.extend(g.events.iter().cloned());
    }

    let elapsed_s = stats.elapsed.as_secs_f64();
    let throughput = stats.logical_lines as f64 / elapsed_s;
    pipeline.wait_idle_and_persist(stats.logical_lines);
    let summary = serde_json::json!({
        "file": file,
        "state_dir": state_dir,
        "logical_lines": stats.logical_lines,
        "physical_lines": stats.physical_lines,
        "elapsed_seconds": elapsed_s,
        "throughput_lines_per_sec": (throughput as u64),
        "templates": templates,
        "events": events,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    if let Some(path) = report {
        std::fs::write(path, serde_json::to_vec_pretty(&summary)?)?;
    }
    if let Some(path) = events_out {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for e in &all_events {
            writeln!(f, "{}", serde_json::to_string(e)?)?;
        }
    }
    if let Some(path) = assignments {
        use std::io::Write;
        let mut f = std::fs::File::create(&path)?;
        for s in &pipeline.states {
            let g = s.lock();
            let map: Vec<serde_json::Value> = g
                .assignments
                .as_ref()
                .map(|a| {
                    a.iter()
                        .map(|(line, cid)| {
                            serde_json::json!({"line": line, "shard": g.shard_id, "cluster": cid})
                        })
                        .collect()
                })
                .unwrap_or_default();
            for v in map {
                writeln!(f, "{}", serde_json::to_string(&v)?)?;
            }
        }
    }
    Ok(())
}

fn inspect(config: pipeline::PipelineConfig) -> anyhow_lite::Result<()> {
    let pipeline = Pipeline::start(config)?;
    for s in &pipeline.states {
        let g = s.lock();
        println!("-- shard {} --", g.shard_id);
        println!("records: {}", g.total_records);
        let mut clusters: Vec<_> = g.drain.clusters().collect();
        clusters.sort_by(|a, b| b.count.cmp(&a.count));
        for c in clusters.iter().take(30) {
            println!(
                "  {:>8}  {:<6}  {}",
                c.count,
                c.level.as_str(),
                g.drain.template_text(c)
            );
        }
        println!("events: {}", g.events.len());
    }
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Tiny error alias so we don't need anyhow as a runtime dependency.
mod anyhow_lite {
    pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
}
