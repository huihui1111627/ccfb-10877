//! Per-shard string interner: every token/name is stored once and referenced by u32.
//! Id 0 is reserved for the wildcard token `<*>`.

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Interner {
    strings: Vec<String>,
    #[serde(skip)]
    index: Option<AHashMap<String, u32>>,
}

pub const WILD: u32 = 0;

impl Interner {
    pub fn new() -> Self {
        let mut strings = Vec::new();
        strings.push("<*>".to_string());
        let mut index = AHashMap::new();
        index.insert("<*>".to_string(), 0);
        Self {
            strings,
            index: Some(index),
        }
    }

    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(map) = self.index.as_mut() {
            if let Some(&id) = map.get(s) {
                return id;
            }
            let id = self.strings.len() as u32;
            self.strings.push(s.to_string());
            map.insert(s.to_string(), id);
            id
        } else {
            unreachable!("interner index not rebuilt")
        }
    }

    pub fn resolve(&self, id: u32) -> &str {
        &self.strings[id as usize]
    }

    /// Rebuild the lookup hashmap after deserialization.
    pub fn rebuild_index(&mut self) {
        let mut map = AHashMap::with_capacity(self.strings.len());
        for (id, s) in self.strings.iter().enumerate() {
            map.insert(s.clone(), id as u32);
        }
        self.index = Some(map);
    }
}
