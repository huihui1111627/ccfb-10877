//! Serialization helpers for maps with non-string keys.
//!
//! serde_json only permits strings as JSON object keys, so every
//! `AHashMap<u32, _>` is persisted as an array of `[key, value]` entries.
//! Functions are named `serialize`/`deserialize` for `#[serde(with = ...)]`.

use ahash::AHashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S, V>(map: &AHashMap<u32, V>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    V: Serialize,
{
    s.collect_seq(map.iter())
}

pub fn deserialize<'de, D, V>(d: D) -> Result<AHashMap<u32, V>, D::Error>
where
    D: Deserializer<'de>,
    V: Deserialize<'de>,
{
    let entries: Vec<(u32, V)> = Vec::deserialize(d)?;
    Ok(entries.into_iter().collect())
}
