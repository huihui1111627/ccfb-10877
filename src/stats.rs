//! Time-series bookkeeping and the MAD-based anomaly detector.

use serde::{Deserialize, Serialize};

pub const BUCKET_MS: i64 = 60_000;
const RING_BUCKETS: usize = 240; // 4 hours of per-minute history
const BASELINE_WINDOW: usize = 20;
const MIN_BASELINE_POINTS: usize = 8;

#[inline]
pub fn bucket_of(ts_ms: i64) -> i64 {
    ts_ms / BUCKET_MS * BUCKET_MS
}

/// Fixed-width ring of per-minute counts for one template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeries {
    /// (bucket start ms, count); ascending, capped at RING_BUCKETS.
    pub points: Vec<(i64, u64)>,
    /// Minute bucket of the last drop/spike alert (dedupe one per episode).
    pub last_spike_alert: i64,
    pub last_drop_alert: i64,
    pub drop_streak: u32,
}

impl TimeSeries {
    pub fn new() -> Self {
        Self {
            points: Vec::with_capacity(64),
            last_spike_alert: 0,
            last_drop_alert: 0,
            drop_streak: 0,
        }
    }

    /// Insert a count, tolerating out-of-order and duplicate buckets (the
    /// ingest pipeline can deliver records non-monotonically across threads).
    pub fn add(&mut self, bucket: i64, n: u64) {
        match self.points.iter_mut().find(|(b, _)| *b == bucket) {
            Some((_, c)) => *c += n,
            None => {
                let pos = self
                    .points
                    .partition_point(|(b, _)| *b < bucket);
                self.points.insert(pos, (bucket, n));
                if self.points.len() > RING_BUCKETS {
                    let cut = self.points.len() - RING_BUCKETS;
                    self.points.drain(..cut);
                }
            }
        }
    }

    pub fn count_at(&self, bucket: i64) -> u64 {
        self.points
            .iter()
            .find(|(b, _)| *b == bucket)
            .map(|(_, c)| *c)
            .unwrap_or(0)
    }

    #[allow(dead_code)]
    pub fn total(&self) -> u64 {
        self.points.iter().map(|(_, c)| *c).sum()
    }

    /// Median + MAD (scaled to sigma) over the baseline window ending before
    /// `current_bucket`, skipping empty buckets only when the template has ever
    /// been active before.
    fn baseline(&self, current_bucket: i64) -> Option<Baseline> {
        let prior: Vec<f64> = self
            .points
            .iter()
            .filter(|(b, _)| *b < current_bucket)
            .rev()
            .take(BASELINE_WINDOW)
            .map(|(_, c)| *c as f64)
            .collect();
        if prior.len() < MIN_BASELINE_POINTS {
            return None;
        }
        let mut sorted = prior.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        let mut dev: Vec<f64> = prior.iter().map(|x| (x - median).abs()).collect();
        dev.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mad = dev[dev.len() / 2] * 1.4826;
        let mean = prior.iter().sum::<f64>() / prior.len() as f64;
        Some(Baseline {
            median,
            mad: mad.max(0.5),
            mean,
        })
    }

    /// Evaluate one bucket. Returns the kind of alert, if any.
    pub fn evaluate(&mut self, bucket: i64) -> Option<DetectorVerdict> {
        // Warm-up guard: the template needs at least 10 minutes of history so
        // the baseline reflects steady-state traffic, not startup ramp-up.
        if let Some((first_b, _)) = self.points.first() {
            if bucket - first_b < 10 * 60_000 {
                return None;
            }
        }
        let current = self.count_at(bucket) as f64;
        let Some(b) = self.baseline(bucket) else {
            return None;
        };
        // ---- Spike: >=4x median and outside 3.5 MAD, with an absolute floor of 8.
        // Sparse low-rate templates fluctuate naturally; the floor keeps them quiet.
        let spike_floor = (b.median * 4.0).max(b.mean + 3.5 * b.mad).max(8.0);
        if current >= spike_floor && self.last_spike_alert < bucket {
            self.last_spike_alert = bucket;
            return Some(DetectorVerdict::Spike {
                observed: current,
                baseline: b.median,
            });
        }
        // ---- Drop: median must be statistically meaningful (>=8/min), and the
        // current minute has to be essentially silent (<10% of median). Three
        // consecutive quiet minutes are required before alerting.
        if b.median >= 8.0 && current < (b.median * 0.1).max(1.0) {
            self.drop_streak += 1;
        } else {
            self.drop_streak = 0;
        }
        if self.drop_streak >= 3 && self.last_drop_alert < bucket {
            self.last_drop_alert = bucket;
            return Some(DetectorVerdict::Drop {
                observed: current,
                baseline: b.median,
            });
        }
        None
    }
}

struct Baseline {
    median: f64,
    mad: f64,
    mean: f64,
}

pub enum DetectorVerdict {
    Spike { observed: f64, baseline: f64 },
    Drop { observed: f64, baseline: f64 },
}

/// Per-minute aggregate for one extracted numeric variable.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricBucket {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSeries {
    pub points: Vec<(i64, MetricBucket)>,
    /// P² streaming p95 estimator over the whole field lifetime.
    pub p95: P2Estimator,
}

impl MetricSeries {
    pub fn new() -> Self {
        Self {
            points: Vec::with_capacity(64),
            p95: P2Estimator::new(0.95),
        }
    }

    pub fn observe(&mut self, bucket: i64, value: f64) {
        self.p95.observe(value);
        // Out-of-order/duplicate-tolerant insertion, mirroring TimeSeries::add.
        match self.points.iter_mut().find(|(b, _)| *b == bucket) {
            Some((_, agg)) => {
                agg.count += 1;
                agg.sum += value;
                agg.min = agg.min.min(value);
                agg.max = agg.max.max(value);
            }
            None => {
                let pos = self.points.partition_point(|(b, _)| *b < bucket);
                self.points.insert(
                    pos,
                    (
                        bucket,
                        MetricBucket {
                            count: 1,
                            sum: value,
                            min: value,
                            max: value,
                        },
                    ),
                );
                if self.points.len() > RING_BUCKETS {
                    let cut = self.points.len() - RING_BUCKETS;
                    self.points.drain(..cut);
                }
            }
        }
    }
}

/// Jain/Chlamtac P² quantile estimator — O(1) memory, no raw sample retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2Estimator {
    p: f64,
    n: [f64; 5],   // desired marker positions
    np: [f64; 5],  // marker heights
    dn: [f64; 5],  // desired quantile positions
    count: u64,
}

impl P2Estimator {
    pub fn new(p: f64) -> Self {
        Self {
            p,
            n: [0.0; 5],
            np: [0.0; 5],
            dn: [0.0, 1.0, p, 1.0 + p, 2.0].map(|x| x * 0.0 + x),
            count: 0,
        }
    }

    pub fn observe(&mut self, x: f64) {
        if self.count < 5 {
            // Seed phase: stash in np, sort on the 5th value.
            self.np[self.count as usize] = x;
            self.count += 1;
            if self.count == 5 {
                self.np.sort_by(|a, b| a.partial_cmp(b).unwrap());
                for i in 0..5 {
                    self.n[i] = i as f64 + 1.0;
                }
                self.dn = [0.0, 1.0, self.p, 1.0 + self.p, 2.0];
            }
            return;
        }
        self.count += 1;
        let k = if x < self.np[0] {
            self.np[0] = x;
            0
        } else if x <= self.np[1] {
            0
        } else if x <= self.np[2] {
            1
        } else if x <= self.np[3] {
            2
        } else if x <= self.np[4] {
            3
        } else {
            self.np[4] = x;
            4
        };
        for i in (k + 1)..5 {
            self.n[i] += 1.0;
        }
        for i in 0..5 {
            self.dn[i] += if i == 0 {
                0.0
            } else if i == 4 {
                1.0
            } else {
                [0.0, 0.0, self.p, 1.0 - self.p, 0.0][i]
            };
        }
        for i in 1..4 {
            let d = self.dn[i] - self.n[i];
            if (d >= 1.0 && self.n[i + 1] - self.n[i] > 1.0)
                || (d <= -1.0 && self.n[i - 1] - self.n[i] < -1.0)
            {
                let ds = d.signum();
                let q = self.quadratic(i, ds);
                if self.np[i - 1] < q && q < self.np[i + 1] {
                    self.np[i] = q;
                } else {
                    // Linear fallback.
                    let j = if ds > 0.0 { i + 1 } else { i - 1 };
                    self.np[i] += ds
                        * (self.np[j] - self.np[i])
                        / (self.n[j] - self.n[i]);
                }
                self.n[i] += ds;
            }
        }
    }

    fn quadratic(&self, i: usize, d: f64) -> f64 {
        let q = self.np;
        let n = self.n;
        let q_prev = q[i - 1];
        let q_cur = q[i];
        let q_next = q[i + 1];
        let n_prev = n[i - 1];
        let n_cur = n[i];
        let n_next = n[i + 1];
        q_cur
            + d / (n_next - n_prev)
                * ((n_cur - n_prev + d) * (q_next - q_cur) / (n_next - n_cur)
                    + (n_next - n_cur - d) * (q_cur - q_prev) / (n_cur - n_prev))
    }

    pub fn quantile(&self) -> Option<f64> {
        match self.count {
            0 => None,
            1..=4 => {
                let mut vals = self.np[..self.count as usize].to_vec();
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
                Some(vals[((self.p * (self.count - 1) as f64).round()) as usize])
            }
            _ => Some(self.np[2]),
        }
    }
}
