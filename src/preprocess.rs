//! Regex-free preprocessing: line parsing, variable normalization, tokenization,
//! multiline detection and numeric metric extraction.

use crate::interner::{Interner, WILD};
use crate::types::{Level, LogRecord, MetricUnit, NumericVar};
use smallvec::SmallVec;

const MAX_TOKENS: usize = 64;

/// Header fields parsed from one physical line (plain text or JSON).
pub struct RawLine {
    pub ts_ms: i64,
    pub service: String,
    pub version: String,
    pub level: Level,
    pub body: String,
}

/// Check whether a physical line starts a new log entry (has a parseable header).
pub fn is_entry_start(line: &str) -> bool {
    parse_line(line).is_some()
}

pub fn parse_line(line: &str) -> Option<RawLine> {
    let line = line.trim_end_matches(['\r', '\n']);
    if line.is_empty() {
        return None;
    }
    if line.starts_with('{') {
        parse_json_line(line)
    } else {
        parse_text_line(line)
    }
}

/// Plain format: `2026-01-01T00:00:00.123Z ERROR [service/version] message`
fn parse_text_line(line: &str) -> Option<RawLine> {
    let bytes = line.as_bytes();
    // Timestamp: ISO8601 with optional fractional seconds / timezone, min 19 chars.
    if bytes.len() < 20 || !bytes[0].is_ascii_digit() {
        return None;
    }
    let mut end = 19;
    if end < bytes.len() && bytes[end] == b'.' {
        end += 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end < bytes.len() && (bytes[end] == b'Z' || bytes[end] == b'z') {
        end += 1;
    } else if end + 5 < bytes.len() && (bytes[end] == b'+' || bytes[end] == b'-') {
        end += 6; // +08:00
    }
    let ts_ms = parse_iso8601(&line[..end])?;
    let rest = line[end..].trim_start();
    // Level token up to whitespace.
    let sp = rest.find(' ')?;
    let level = Level::parse(&rest[..sp]);
    let rest = rest[sp..].trim_start();
    // [service/version]
    let rest = rest.strip_prefix('[')?;
    let close = rest.find(']')?;
    let header = &rest[..close];
    let (service, version) = match header.split_once('/') {
        Some((s, v)) => (s.to_string(), v.to_string()),
        None => (header.to_string(), "unknown".to_string()),
    };
    let body = rest[close + 1..].trim_start().to_string();
    Some(RawLine {
        ts_ms,
        service,
        version,
        level,
        body,
    })
}

/// JSON format: {"ts":"...","service":"...","version":"...","level":"...","msg":"..."}
fn parse_json_line(line: &str) -> Option<RawLine> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let obj = v.as_object()?;
    let ts_raw = obj
        .get("ts")
        .or_else(|| obj.get("timestamp"))
        .or_else(|| obj.get("time"))?
        .as_str()?;
    let ts_ms = parse_iso8601(ts_raw).or_else(|| {
        // Epoch millis / seconds as a number-like string.
        ts_raw.parse::<i64>().ok().map(|n| {
            if n < 10_000_000_000 {
                n * 1000
            } else {
                n
            }
        })
    })?;
    let service = obj
        .get("service")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let version = obj
        .get("version")
        .or_else(|| obj.get("ver"))
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let level = obj
        .get("level")
        .or_else(|| obj.get("severity"))
        .map(|x| Level::parse(x.as_str().unwrap_or("")))
        .unwrap_or(Level::Unknown);
    let body = obj
        .get("msg")
        .or_else(|| obj.get("message"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    Some(RawLine {
        ts_ms,
        service,
        version,
        level,
        body,
    })
}

/// Minimal ISO8601 parser (UTC or explicit offset), returns epoch millis.
pub fn parse_iso8601(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let num = |i: usize, j: usize| -> Option<i64> {
        s.get(i..j)?.parse().ok()
    };
    let year = num(0, 4)?;
    let month = num(5, 7)?;
    let day = num(8, 10)?;
    let hour = num(11, 13)?;
    let min = num(14, 16)?;
    let sec = num(17, 19)?;
    let mut frac_ms = 0i64;
    let mut pos = 19;
    if pos < b.len() && b[pos] == b'.' {
        pos += 1;
        let start = pos;
        while pos < b.len() && b[pos].is_ascii_digit() {
            pos += 1;
        }
        let digits = &s[start..pos];
        let padded = format!("{:0<3}", digits);
        frac_ms = padded.get(..3)?.parse().ok()?;
    }
    let mut offset_sec = 0i64;
    if pos < b.len() && (b[pos] == b'+' || b[pos] == b'-') {
        let sign = if b[pos] == b'-' { 1 } else { -1 };
        let oh = s.get(pos + 1..pos + 3)?.parse::<i64>().ok()?;
        let om = s.get(pos + 4..pos + 6)?.parse::<i64>().ok()?;
        offset_sec = sign * (oh * 3600 + om * 60);
    }
    let days = days_from_civil(year, month, day);
    let epoch_ms = days * 86_400_000
        + (hour * 3600 + min * 60 + sec) * 1000
        + frac_ms
        - offset_sec * 1000;
    Some(epoch_ms)
}

/// Howard Hinnant's civil-date algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Result of tokenizing one message body.
pub struct Tokenized {
    pub tokens: SmallVec<[u32; 24]>,
    pub numerics: SmallVec<[NumericVar; 4]>,
}

/// Tokenize + normalize. No regex: classification is done by character scanning.
pub fn tokenize(inter: &mut Interner, body: &str) -> Tokenized {
    let mut tokens: SmallVec<[u32; 24]> = SmallVec::new();
    let mut numerics: SmallVec<[NumericVar; 4]> = SmallVec::new();
    let mut saw_stack = false;
    for raw_line in body.split('\n') {
        if is_stack_continuation(raw_line) {
            // Collapse the whole multiline trace (any depth) into one fixed
            // token so template length is independent of stack depth.
            if !saw_stack {
                tokens.push(inter.intern("<stacktrace>"));
                saw_stack = true;
            }
            continue;
        }
        for raw_chunk in raw_line.split(|c: char| c.is_whitespace()) {
            if raw_chunk.is_empty() {
                continue;
            }
            for chunk in split_punct(raw_chunk) {
                if tokens.len() >= MAX_TOKENS {
                    break;
                }
                if chunk.is_punct {
                    // Standalone punctuation is structural noise; braces/parens
                    // are dropped, the rest kept as structural tokens.
                    if matches!(chunk.text, "{" | "}" | "(" | ")" | "[" | "]") {
                        continue;
                    }
                    tokens.push(inter.intern(chunk.text));
                    continue;
                }
                let text = chunk.text;
                if is_uuid(text) || is_hex_blob(text) || is_ip_addr(text) {
                    tokens.push(WILD);
                    continue;
                }
                if let Some((value, unit)) = numeric_with_unit(text) {
                    let pos = tokens.len() as u16;
                    tokens.push(WILD);
                    if numerics.len() < 16 {
                        numerics.push(NumericVar { pos, value, unit });
                    }
                    continue;
                }
                if is_plain_number(text) {
                    let pos = tokens.len() as u16;
                    tokens.push(WILD);
                    if numerics.len() < 16 {
                        numerics.push(NumericVar {
                            pos,
                            value: text.parse().unwrap_or(0.0),
                            unit: MetricUnit::None,
                        });
                    }
                    continue;
                }
                // Strings with embedded variable parts (order_123, /api/v2/user/42,
                // host-7.prod) get split on digit boundaries.
                if let Some(norm) = normalize_mixed(text) {
                    tokens.push(inter.intern(&norm));
                } else {
                    tokens.push(inter.intern(text));
                }
            }
        }
    }
    Tokenized { tokens, numerics }
}

/// A physical line that belongs to a multiline stack trace rather than a
/// standalone log entry.
fn is_stack_continuation(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with("\tat ")                 // JVM frames
        || t.starts_with("at ")
        || t.starts_with("Caused by:")
        || t.starts_with("... ")           // JVM "... 12 common frames omitted"
        || t.starts_with("File \"")        // Python frames
        || t.contains(".java:")
        || t.contains(".py\", line")
        || t.starts_with("Traceback (most recent call last)")
        || t.ends_with("Exception")
        || (t.contains("Error:") && t.chars().filter(|c| c.is_whitespace()).count() <= 3)
}

struct Chunk<'a> {
    text: &'a str,
    is_punct: bool,
}

/// Split a whitespace-delimited chunk on punctuation while keeping the punctuation
/// itself as separate structural tokens (colons inside times/IPs are handled later
/// by the classifiers which receive the whole chunk first).
fn split_punct(s: &str) -> SmallVec<[Chunk<'_>; 4]> {
    let mut out: SmallVec<[Chunk<'_>; 4]> = SmallVec::new();
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if c == '_' || c == '-' || c == '.' || c == '/' || c == ':' || c == ',' || c == ';'
            || c == '=' || c == '{' || c == '}' || c == '(' || c == ')' || c == '[' || c == ']'
        {
            if i > start {
                out.push(Chunk {
                    text: &s[start..i],
                    is_punct: false,
                });
            }
            out.push(Chunk {
                text: &s[i..i + c.len_utf8()],
                is_punct: true,
            });
            start = i + c.len_utf8();
        }
    }
    if start < s.len() {
        out.push(Chunk {
            text: &s[start..],
            is_punct: false,
        });
    }
    out
}

fn is_uuid(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    s.bytes().enumerate().all(|(i, b)| match i {
        8 | 13 | 18 | 23 => b == b'-',
        _ => b.is_ascii_hexdigit(),
    })
}

fn is_hex_blob(s: &str) -> bool {
    s.len() >= 12
        && s.len() <= 64
        && s.bytes().all(|b| b.is_ascii_hexdigit())
        && s.bytes().any(|b| b.is_ascii_digit())
        && s.bytes().any(|b| b.is_ascii_alphabetic())
}

fn is_ip_addr(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    parts
        .iter()
        .all(|p| p.parse::<u8>().is_ok() || (p.starts_with('*') && p.len() == 1))
}

fn is_plain_number(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    if t.is_empty() {
        return false;
    }
    let mut seen_digit = false;
    let mut dots = 0;
    for b in t.bytes() {
        if b.is_ascii_digit() {
            seen_digit = true;
        } else if b == b'.' {
            dots += 1;
            if dots > 1 {
                return false;
            }
        } else {
            return false;
        }
    }
    seen_digit
}

/// `123ms`, `2.5s`, `500us`, `4KB`, `12MB`, `99%` -> (normalized value, unit).
fn numeric_with_unit(s: &str) -> Option<(f64, MetricUnit)> {
    let bytes = s.as_bytes();
    let split_at = bytes
        .iter()
        .rposition(|b| b.is_ascii_digit() || *b == b'.')
        .map(|p| p + 1)?;
    if split_at == 0 || split_at >= s.len() {
        return None;
    }
    let num: f64 = s[..split_at].parse().ok()?;
    let unit = &s[split_at..];
    let (v, u) = match unit {
        "ms" => (num, MetricUnit::Ms),
        "s" => (num * 1000.0, MetricUnit::Ms),
        "us" | "µs" => (num / 1000.0, MetricUnit::Ms),
        "ns" => (num / 1_000_000.0, MetricUnit::Ms),
        "m" => (num * 60_000.0, MetricUnit::Ms),
        "B" => (num, MetricUnit::Bytes),
        "KB" | "kb" | "KiB" => (num * 1024.0, MetricUnit::Bytes),
        "MB" | "mb" | "MiB" => (num * 1024.0 * 1024.0, MetricUnit::Bytes),
        "GB" | "gb" | "GiB" => (num * 1024.0 * 1024.0 * 1024.0, MetricUnit::Bytes),
        "%" => (num, MetricUnit::Percent),
        _ => return None,
    };
    Some((v, u))
}

/// Normalize tokens that mix stable text with variables:
/// `order_12345` -> `order_<*>`, `node-7` -> `node-<*>`, `v2` stays (version words).
fn normalize_mixed(s: &str) -> Option<String> {
    if !s.bytes().any(|b| b.is_ascii_digit()) {
        return None;
    }
    // Pure version-ish tokens like v12, V2 are common stable markers — keep them.
    if let Some(rest) = s.strip_prefix(['v', 'V']) {
        if !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    let mut out = String::with_capacity(s.len() + 3);
    let mut in_digits = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            if !in_digits {
                out.push_str("<*>");
                in_digits = true;
            }
        } else {
            in_digits = false;
            out.push(c);
        }
    }
    if out == s {
        None
    } else {
        Some(out)
    }
}

/// Assemble a fully preprocessed record from parsed header + (possibly multiline) body.
pub fn build_record(
    inter: &mut Interner,
    raw: RawLine,
    extra_lines: &[String],
    source_line: u64,
) -> LogRecord {
    let mut full = raw.body;
    for line in extra_lines {
        full.push('\n');
        full.push_str(line);
    }
    let Tokenized { tokens, numerics } = tokenize(inter, &full);
    LogRecord {
        ts_ms: raw.ts_ms,
        service: inter.intern(&raw.service),
        version: inter.intern(&raw.version),
        level: raw.level,
        tokens,
        raw: full,
        numerics,
        source_line,
        service_name: String::new(),
        version_name: String::new(),
    }
}
