//! Shared types used across the pipeline.

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// One normalized log record after preprocessing and multiline assembly.
#[derive(Debug, Clone)]
pub struct LogRecord {
    /// Wall-clock unix timestamp in milliseconds (from the log line itself, not ingest time).
    pub ts_ms: i64,
    /// Interned string id of the service name.
    pub service: u32,
    /// Interned string id of the version string (e.g. "v12").
    pub version: u32,
    /// Severity level: ERROR/WARN/INFO/DEBUG/...
    pub level: Level,
    /// Tokenized message body, interned token ids. Wildcards use a dedicated id.
    pub tokens: SmallVec<[u32; 24]>,
    /// Original assembled text (header stripped), used for sample storage.
    pub raw: String,
    /// Numeric variables extracted from the body: (position, value, unit).
    pub numerics: SmallVec<[NumericVar; 4]>,
    /// 1-based index of the *primary* physical line in the input file.
    pub source_line: u64,
    /// Names carried from the parser thread; consumed when the shard interner
    /// resolves them to ids. Empty after materialization.
    pub service_name: String,
    pub version_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
    Unknown,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Trace => "TRACE",
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
            Level::Fatal => "FATAL",
            Level::Unknown => "UNKNOWN",
        }
    }

    /// Higher = more severe.
    pub fn weight(self) -> u32 {
        match self {
            Level::Trace | Level::Debug => 0,
            Level::Info => 1,
            Level::Warn => 2,
            Level::Error => 4,
            Level::Fatal => 5,
            Level::Unknown => 1,
        }
    }

    pub fn parse(s: &str) -> Level {
        let u = s.to_ascii_uppercase();
        match u.as_str() {
            "TRACE" => Level::Trace,
            "DEBUG" | "DBG" => Level::Debug,
            "INFO" | "NOTICE" => Level::Info,
            "WARN" | "WARNING" => Level::Warn,
            "ERROR" | "ERR" | "SEVERE" => Level::Error,
            "FATAL" | "CRITICAL" | "EMERG" | "ALERT" => Level::Fatal,
            _ => Level::Unknown,
        }
    }
}

/// A numeric variable spotted inside the message.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NumericVar {
    /// Index into the token slice where the wildcard replaced the number.
    pub pos: u16,
    /// Parsed value (durations with units are normalized to milliseconds, sizes to bytes).
    pub value: f64,
    pub unit: MetricUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricUnit {
    None,
    /// Time, normalized to ms.
    Ms,
    /// Size, normalized to bytes.
    Bytes,
    Percent,
}

/// An event emitted by the anomaly detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: u64,
    pub ts_ms: i64,
    pub service: String,
    pub template_id: String,
    pub kind: EventKind,
    pub level: String,
    pub detail: String,
    pub template_text: String,
    pub observed: f64,
    pub baseline: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Spike,
    Drop,
    NewTemplate,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Spike => "spike",
            EventKind::Drop => "drop",
            EventKind::NewTemplate => "new_template",
        }
    }
}
