//! Drain-style online log clustering with version lineage and cluster merging.
//!
//! Each shard owns one `Drain` together with its `Interner`, so there is no
//! cross-shard locking on the hot path.

use crate::interner::{Interner, WILD};
use crate::types::Level;
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

const MAX_DEPTH: usize = 4;
const MAX_CHILDREN: usize = 100;
const SIM_THRESHOLD: f64 = 0.5;
const MERGE_SIM: f64 = 0.85;
const LINEAGE_SIM: f64 = 0.6;
const TEMPLATE_UPDATE_EVERY: u64 = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    /// Stable id, local to the shard; prefixed with service at the API layer.
    pub local_id: u32,
    /// Interned id of the owning service name.
    pub service_id: u32,
    /// Current template token sequence (interned ids, 0 = wildcard).
    pub template: SmallVec<[u32; 24]>,
    pub count: u64,
    pub level: Level,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    /// version string id -> match count.
    #[serde(with = "crate::serde_util")]
    pub versions: AHashMap<u32, u64>,
    /// Up to 3 sample raw messages.
    pub samples: SmallVec<[String; 3]>,
    /// Set on the match that first creates the cluster.
    pub born_version: u32,
    /// Cluster ids that this one relates to across versions (merge/split edges).
    pub lineage: Vec<LineageEdge>,
    /// Number of times seen; ERROR clusters alert on first, others on second.
    pub announced: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LineageKind {
    /// Multiple old-version templates folded into this one.
    MergedFrom,
    /// This template is one of several new shapes split from an old one.
    SplitFrom,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LineageEdge {
    pub other_local_id: u32,
    pub kind: LineageKind,
    pub sim: u16, // similarity * 1000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Node {
    /// token id -> child node; key u32::MAX means the wildcard bucket.
    #[serde(with = "crate::serde_util")]
    children: AHashMap<u32, usize>,
    /// Leaf only: cluster ids.
    clusters: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
pub struct MatchResult {
    pub local_id: u32,
    pub is_new_cluster: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Drain {
    interner: Interner,
    nodes: Vec<Node>,
    #[serde(with = "crate::serde_util")]
    clusters: AHashMap<u32, Cluster>,
    next_cluster_id: u32,
    #[serde(skip)]
    merge_check_counter: u64,
}

impl Drain {
    pub fn new() -> Self {
        let root = Node {
            children: AHashMap::new(),
            clusters: Vec::new(),
        };
        Self {
            interner: Interner::new(),
            nodes: vec![root],
            clusters: AHashMap::new(),
            next_cluster_id: 1,
            merge_check_counter: 0,
        }
    }

    #[allow(dead_code)]
    pub fn interner(&self) -> &Interner {
        &self.interner
    }

    pub fn interner_mut(&mut self) -> &mut Interner {
        &mut self.interner
    }

    pub fn cluster(&self, id: u32) -> Option<&Cluster> {
        self.clusters.get(&id)
    }

    pub fn cluster_mut(&mut self, id: u32) -> Option<&mut Cluster> {
        self.clusters.get_mut(&id)
    }

    pub fn clusters(&self) -> impl Iterator<Item = &Cluster> {
        self.clusters.values()
    }

    /// Route one token sequence to its cluster, creating one if needed.
    pub fn add(
        &mut self,
        tokens: &SmallVec<[u32; 24]>,
        level: Level,
        service_id: u32,
        version: u32,
        ts_ms: i64,
        raw: &str,
    ) -> MatchResult {
        let leaf = self.navigate(tokens);
        let candidate_ids: Vec<u32> = self.nodes[leaf].clusters.clone();
        let mut best: Option<(u32, f64)> = None;
        for cid in candidate_ids {
            let cluster = &self.clusters[&cid];
            if cluster.template.len() != tokens.len() {
                continue;
            }
            let sim = seq_sim(&cluster.template, tokens);
            if sim >= SIM_THRESHOLD && best.map_or(true, |(_, b)| sim > b) {
                best = Some((cid, sim));
            }
        }

        match best {
            Some((cid, _sim)) => {
                self.update_cluster(cid, tokens, version, ts_ms, raw);
                MatchResult {
                    local_id: cid,
                    is_new_cluster: false,
                }
            }
            None => {
                let cid = self.next_cluster_id;
                self.next_cluster_id += 1;
                let cluster = Cluster {
                    local_id: cid,
                    service_id,
                    template: tokens.clone(),
                    count: 0,
                    level,
                    first_seen_ms: ts_ms,
                    last_seen_ms: ts_ms,
                    versions: AHashMap::new(),
                    samples: {
                        let mut s: SmallVec<[String; 3]> = SmallVec::new();
                        s.push(raw.chars().take(300).collect());
                        s
                    },
                    born_version: version,
                    lineage: Vec::new(),
                    announced: false,
                };
                self.clusters.insert(cid, cluster);
                self.nodes[leaf].clusters.push(cid);
                self.update_cluster(cid, tokens, version, ts_ms, raw);
                self.link_lineage(cid, tokens, version);
                MatchResult {
                    local_id: cid,
                    is_new_cluster: true,
                }
            }
        }
    }

    /// Walk the fixed-depth token tree, creating wildcard buckets as needed.
    fn navigate(&mut self, tokens: &SmallVec<[u32; 24]>) -> usize {
        let mut node = 0usize;
        let depth = MAX_DEPTH.min(tokens.len());
        for depth_idx in 0..depth {
            let tok = tokens[depth_idx];
            let child = if tok == WILD { u32::MAX } else { tok };
            let next = match self.nodes[node].children.get(&child).copied() {
                Some(n) => n,
                None => {
                    // Drain maxChildren rule: too many distinct first tokens means
                    // the token is variable — fall back to the wildcard bucket.
                    if depth_idx == 0 && self.nodes[node].children.len() >= MAX_CHILDREN {
                        let n = self.new_node();
                        self.nodes[node].children.insert(u32::MAX, n);
                        n
                    } else {
                        let n = self.new_node();
                        self.nodes[node].children.insert(child, n);
                        n
                    }
                }
            };
            node = next;
        }
        node
    }

    fn new_node(&mut self) -> usize {
        let id = self.nodes.len();
        self.nodes.push(Node {
            children: AHashMap::new(),
            clusters: Vec::new(),
        });
        id
    }

    fn update_cluster(
        &mut self,
        cid: u32,
        tokens: &SmallVec<[u32; 24]>,
        version: u32,
        ts_ms: i64,
        raw: &str,
    ) {
        let c = self.clusters.get_mut(&cid).unwrap();
        c.count += 1;
        c.last_seen_ms = ts_ms;
        *c.versions.entry(version).or_insert(0) += 1;
        if c.samples.len() < 3 {
            c.samples.push(raw.chars().take(300).collect());
        }
        // Periodically tighten the template to the common token shape.
        if c.count % TEMPLATE_UPDATE_EVERY == 0 {
            for (pos, tok) in tokens.iter().enumerate() {
                if pos < c.template.len() && c.template[pos] != *tok {
                    c.template[pos] = WILD;
                }
            }
        }
    }

    /// When a new cluster appears in a previously unseen version, find the closest
    /// old-version cluster(s) and record merge/split lineage. Length-tolerant:
    /// a deploy that adds/removes one field still produces an edge.
    fn link_lineage(&mut self, cid: u32, tokens: &SmallVec<[u32; 24]>, version: u32) {
        // Collect candidates first to avoid borrow conflicts.
        let candidates: Vec<(u32, SmallVec<[u32; 24]>, u32)> = self
            .clusters
            .values()
            .filter(|c| c.local_id != cid && c.born_version != version)
            .map(|c| (c.local_id, c.template.clone(), c.born_version))
            .collect();
        let mut related: Vec<(u32, f64)> = candidates
            .into_iter()
            .map(|(id, t, _)| (id, token_overlap(&t, tokens)))
            .filter(|(_, sim)| *sim >= LINEAGE_SIM)
            .collect();
        related.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        related.truncate(4);
        let edges: Vec<LineageEdge> = related
            .into_iter()
            .map(|(other, sim)| LineageEdge {
                other_local_id: other,
                kind: LineageKind::SplitFrom,
                sim: (sim * 1000.0) as u16,
            })
            .collect();
        if edges.is_empty() {
            return;
        }
        for edge in &edges {
            self.clusters.get_mut(&cid).unwrap().lineage.push(*edge);
            let back = LineageEdge {
                other_local_id: cid,
                kind: LineageKind::MergedFrom,
                sim: edge.sim,
            };
            self.clusters
                .get_mut(&edge.other_local_id)
                .unwrap()
                .lineage
                .push(back);
        }
    }

    /// Periodic maintenance: merge same-version clusters whose templates have
    /// converged above MERGE_SIM (handles format drift after upgrades).
    /// Returns (survivor_id, absorbed_ids).
    pub fn maybe_merge(&mut self) -> Vec<(u32, Vec<u32>)> {
        self.merge_check_counter += 1;
        if self.merge_check_counter % 64 != 0 {
            return Vec::new();
        }
        let ids: Vec<u32> = self.clusters.keys().copied().collect();
        let mut merges: Vec<(u32, u32, f64)> = Vec::new();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let (a, b) = (ids[i], ids[j]);
                let (ca, cb) = (&self.clusters[&a], &self.clusters[&b]);
                if ca.template.len() != cb.template.len() {
                    continue;
                }
                let sim = seq_sim(&ca.template, &cb.template);
                if sim >= MERGE_SIM {
                    merges.push((a, b, sim));
                }
            }
        }
        let mut done = Vec::new();
        for (keep, drop_id, sim) in merges {
            if !self.clusters.contains_key(&keep) || !self.clusters.contains_key(&drop_id) {
                continue;
            }
            self.absorb(keep, drop_id, sim);
            done.push((keep, drop_id));
        }
        // Group absorbed ids per survivor for the caller.
        let mut map: AHashMap<u32, Vec<u32>> = AHashMap::new();
        for (keep, drop_id) in done {
            map.entry(keep).or_default().push(drop_id);
        }
        map.into_iter().collect()
    }

    fn absorb(&mut self, keep: u32, drop_id: u32, sim: f64) {
        let dropped = self.clusters.remove(&drop_id).unwrap();
        // Remove from leaf lists.
        for node in self.nodes.iter_mut() {
            node.clusters.retain(|id| *id != drop_id);
        }
        let survivor = self.clusters.get_mut(&keep).unwrap();
        survivor.count += dropped.count;
        survivor.first_seen_ms = survivor.first_seen_ms.min(dropped.first_seen_ms);
        for (v, n) in dropped.versions {
            *survivor.versions.entry(v).or_insert(0) += n;
        }
        for (pos, tok) in dropped.template.iter().enumerate() {
            if pos < survivor.template.len() && survivor.template[pos] != *tok {
                survivor.template[pos] = WILD;
            }
        }
        for s in dropped.samples {
            if survivor.samples.len() < 3 {
                survivor.samples.push(s);
            }
        }
        survivor.lineage.push(LineageEdge {
            other_local_id: drop_id,
            kind: LineageKind::MergedFrom,
            sim: (sim * 1000.0) as u16,
        });
    }

    /// Human-readable template text with `<*>` for wildcards.
    pub fn template_text(&self, c: &Cluster) -> String {
        c.template
            .iter()
            .map(|id| self.interner.resolve(*id))
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn resolve(&self, id: u32) -> &str {
        self.interner.resolve(id)
    }

    pub fn rebuild_after_load(&mut self) {
        self.interner.rebuild_index();
    }
}

/// Drain sequence similarity: fraction of positions that agree (wildcards match anything).
pub fn seq_sim(a: &[u32], b: &[u32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let hit = a
        .iter()
        .zip(b.iter())
        .filter(|(x, y)| *x == *y || **x == WILD || **y == WILD)
        .count();
    hit as f64 / a.len() as f64
}

/// Length-tolerant similarity for version lineage: Jaccard overlap on the
/// multisets of concrete tokens (wildcards ignored), so adding/removing one
/// field across a deploy still links the old and new template.
pub fn token_overlap(a: &[u32], b: &[u32]) -> f64 {
    let concrete = |s: &[u32]| -> AHashMap<u32, u32> {
        let mut m: AHashMap<u32, u32> = AHashMap::new();
        for &t in s {
            if t != WILD {
                *m.entry(t).or_insert(0) += 1;
            }
        }
        m
    };
    let (ma, mb) = (concrete(a), concrete(b));
    let inter: u32 = ma
        .iter()
        .map(|(k, n)| (*n).min(mb.get(k).copied().unwrap_or(0)))
        .sum();
    let union: u32 = ma.values().sum::<u32>() + mb.values().sum::<u32>() - inter;
    if union == 0 {
        0.0
    } else {
        inter as f64 / union as f64
    }
}
