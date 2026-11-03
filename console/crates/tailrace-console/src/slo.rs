//! R11 T-V4: SLO history — lag p50/p95/p99 over a 1h sample ring.
//!
//! The v1.2 SLO surface was the target only (`tailrace_slo_lag_target_seconds`
//! = the lag rule's threshold, [`crate::metrics::slo_target_lines`]); this
//! module adds the achieved side. On every pass the alert evaluator hands
//! the very snapshot it evaluates to [`record_pass`] (no second sampling
//! path), which keeps one lag sample per (pipeline, statement) in
//! console-store's `slo_lag_sample` ring (migration 06). Quantiles are
//! computed from the stored rows at read time ([`window_summaries`]) and
//! surface twice: the console `/metrics`
//! (`tailrace_slo_lag_p{50,95,99}_seconds` + `tailrace_slo_lag_samples`,
//! [`crate::metrics::slo_quantile_lines`]) and `GET /api/slo`.
//!
//! Decisions:
//!
//! - **SLO identity = (pipeline, statement)**: the console-injected `node`
//!   label is provenance only (stored per sample, reported as the newest
//!   reporter) — a pipeline migrating between nodes keeps its history. Two
//!   nodes reporting the same pair in one pass (a stale node still running
//!   a migrated pipeline) record the worse (max) lag: one sample per pair
//!   per pass, the conservative reading.
//! - **Idle is not sampled** (T-V1 consistency): a lag series whose
//!   `tailrace_pipeline_source_idle` reads 1 on the same labels measures
//!   wall time over silence, not a late row — the lag rule holds on it and
//!   the SLO does not count it either (a quiet night would otherwise drag
//!   every p95 to hours). An absent or 0 idle gauge samples as usual; a
//!   series whose node was unreachable is absent from the snapshot and
//!   leaves no sample.
//! - **Nearest-rank quantiles, no interpolation**: the q-quantile of n
//!   ascending samples is the value at 1-based rank ⌈n·q⌉ clamped to
//!   [1, n] ([`nearest_rank`], integer per-mille arithmetic — no float
//!   rounding in the rank). Every reported quantile IS a stored sample,
//!   recomputable from the rows of the window.
//! - **1h ring**: [`RETENTION_SECS`]. Every pass prunes the rows at or
//!   before `now − retention` in the same transaction as its insert; the
//!   boot sweep ([`startup_prune`], `main.rs`, the backfill orphan-sweep
//!   slot) drops the tail a downtime left behind. A read window is the
//!   half-open (`now − windowSecs`, `now`], 1 ≤ windowSecs ≤ retention.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};

use console_store::{NewSloLagSample, SloLagSample, Store};

use crate::alerting::Snapshot;

/// Ring retention — also the longest read window (task card: `环形保留 1h`).
pub const RETENTION_SECS: u64 = 3600;

/// The reported quantiles in per-mille (p50 / p95 / p99).
pub const P50: usize = 500;
pub const P95: usize = 950;
pub const P99: usize = 990;

const LAG_METRIC: &str = "tailrace_pipeline_lag_seconds";
const IDLE_METRIC: &str = "tailrace_pipeline_source_idle";

/// Wall clock, epoch ms (the evaluator loop's clock domain).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The lag samples one pass contributes: one per (pipeline, statement),
/// idle series excluded, the max across nodes when several report the same
/// pair. Series without both labels, or with a non-finite / negative value,
/// are skipped (not a lag reading).
pub fn observations(snap: &Snapshot) -> Vec<NewSloLagSample> {
    let idle: HashSet<&BTreeMap<String, String>> = snap
        .samples
        .iter()
        .filter(|s| s.metric == IDLE_METRIC && s.value == 1.0)
        .map(|s| &s.labels)
        .collect();
    let mut by_pair: BTreeMap<(String, String), NewSloLagSample> = BTreeMap::new();
    for s in &snap.samples {
        if s.metric != LAG_METRIC
            || !s.value.is_finite()
            || s.value < 0.0
            || idle.contains(&s.labels)
        {
            continue;
        }
        let (Some(pipeline), Some(statement)) =
            (s.labels.get("pipeline"), s.labels.get("statement"))
        else {
            continue;
        };
        let obs = NewSloLagSample {
            pipeline: pipeline.clone(),
            statement: statement.clone(),
            node: s.labels.get("node").cloned().unwrap_or_default(),
            lag_secs: s.value,
        };
        match by_pair.entry((pipeline.clone(), statement.clone())) {
            Entry::Vacant(v) => {
                v.insert(obs);
            }
            Entry::Occupied(mut o) => {
                if obs.lag_secs > o.get().lag_secs {
                    o.insert(obs);
                }
            }
        }
    }
    by_pair.into_values().collect()
}

/// `now − retention`: the inclusive prune cutoff of a pass at `now_ms`.
fn prune_through(now_ms: u64) -> i64 {
    now_ms.saturating_sub(RETENTION_SECS * 1000) as i64
}

/// One evaluator pass: record its observations at `now_ms` and evict the
/// ring tail (same transaction). Returns the number of samples recorded.
pub fn record_pass(store: &Store, snap: &Snapshot, now_ms: u64) -> anyhow::Result<usize> {
    let rows = observations(snap);
    store.record_slo_lag_samples(now_ms as i64, &rows, prune_through(now_ms))?;
    Ok(rows.len())
}

/// Boot sweep (`main.rs`, next to the backfill orphan sweep): drop every
/// sample that fell out of the ring while the console was down. Returns
/// the number of rows removed.
pub fn startup_prune(store: &Store, now_ms: u64) -> anyhow::Result<usize> {
    store.prune_slo_lag_samples(prune_through(now_ms))
}

/// Nearest-rank quantile of ascending-sorted values: the value at 1-based
/// rank ⌈n·permille/1000⌉, clamped to [1, n]. `None` when empty.
pub fn nearest_rank(sorted: &[f64], permille: usize) -> Option<f64> {
    let n = sorted.len();
    if n == 0 {
        return None;
    }
    let rank = (n * permille).div_ceil(1000).clamp(1, n);
    Some(sorted[rank - 1])
}

/// The half-open read window (`after`, `until`] of `window_secs` ending at
/// `now_ms`, epoch ms.
pub fn window_bounds(now_ms: u64, window_secs: u64) -> (i64, i64) {
    (
        now_ms.saturating_sub(window_secs * 1000) as i64,
        now_ms as i64,
    )
}

/// One (pipeline, statement)'s quantiles over a window.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesSummary {
    pub pipeline: String,
    pub statement: String,
    /// Node of the newest sample in the window (provenance).
    pub node: String,
    /// Samples in the window — the quantiles' support.
    pub samples: usize,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    /// Oldest / newest sample time in the window, epoch ms (coverage).
    pub oldest_ms: i64,
    pub newest_ms: i64,
}

/// Group window rows per (pipeline, statement) and compute the quantiles.
/// Ordered numerically by pipeline id, then statement index.
pub fn summarize(rows: &[SloLagSample]) -> Vec<SeriesSummary> {
    let mut groups: BTreeMap<(&str, &str), Vec<&SloLagSample>> = BTreeMap::new();
    for r in rows {
        groups
            .entry((r.pipeline.as_str(), r.statement.as_str()))
            .or_default()
            .push(r);
    }
    let mut out: Vec<SeriesSummary> = groups
        .into_iter()
        .filter_map(|((pipeline, statement), rs)| {
            let mut values: Vec<f64> = rs.iter().map(|r| r.lag_secs).collect();
            values.sort_by(f64::total_cmp);
            // last of the max = the newest insertion on a timestamp tie
            let newest = rs.iter().max_by_key(|r| r.ts_ms)?;
            Some(SeriesSummary {
                pipeline: pipeline.to_string(),
                statement: statement.to_string(),
                node: newest.node.clone(),
                samples: values.len(),
                p50: nearest_rank(&values, P50)?,
                p95: nearest_rank(&values, P95)?,
                p99: nearest_rank(&values, P99)?,
                oldest_ms: rs.iter().map(|r| r.ts_ms).min()?,
                newest_ms: newest.ts_ms,
            })
        })
        .collect();
    let numeric = |v: &str| v.parse::<i64>().unwrap_or(i64::MAX);
    out.sort_by(|a, b| {
        (
            numeric(&a.pipeline),
            &a.pipeline,
            numeric(&a.statement),
            &a.statement,
        )
            .cmp(&(
                numeric(&b.pipeline),
                &b.pipeline,
                numeric(&b.statement),
                &b.statement,
            ))
    });
    out
}

/// Quantile summaries of the window of `window_secs` ending at `now_ms`,
/// optionally one pipeline and/or statement.
pub fn window_summaries(
    store: &Store,
    now_ms: u64,
    window_secs: u64,
    pipeline: Option<&str>,
    statement: Option<&str>,
) -> anyhow::Result<Vec<SeriesSummary>> {
    let (after, until) = window_bounds(now_ms, window_secs);
    Ok(summarize(
        &store.slo_lag_samples(after, until, pipeline, statement)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerting::Sample;

    fn sample(metric: &str, labels: &[(&str, &str)], value: f64) -> Sample {
        Sample {
            metric: metric.into(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value,
        }
    }

    fn row(pipeline: &str, statement: &str, node: &str, lag: f64, ts_ms: i64) -> SloLagSample {
        SloLagSample {
            pipeline: pipeline.into(),
            statement: statement.into(),
            node: node.into(),
            lag_secs: lag,
            ts_ms,
        }
    }

    /// Nearest-rank edges: rank ⌈n·q⌉ with integer arithmetic — n=1 answers
    /// its only sample for every q, n=2 splits at p50, 1..=20 and 1..=100
    /// land exactly on the textbook ranks, empty is None.
    #[test]
    fn nearest_rank_definition() {
        assert_eq!(nearest_rank(&[], P95), None);
        for q in [P50, P95, P99] {
            assert_eq!(nearest_rank(&[7.5], q), Some(7.5));
        }
        assert_eq!(nearest_rank(&[1.0, 2.0], P50), Some(1.0));
        assert_eq!(nearest_rank(&[1.0, 2.0], P95), Some(2.0));
        let twenty: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(nearest_rank(&twenty, P50), Some(10.0));
        assert_eq!(nearest_rank(&twenty, P95), Some(19.0));
        assert_eq!(nearest_rank(&twenty, P99), Some(20.0));
        let hundred: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(nearest_rank(&hundred, P50), Some(50.0));
        assert_eq!(nearest_rank(&hundred, P95), Some(95.0));
        assert_eq!(nearest_rank(&hundred, P99), Some(99.0));
        // 101 samples: ⌈95.95⌉ = 96, ⌈99.99⌉ = 100
        let more: Vec<f64> = (1..=101).map(f64::from).collect();
        assert_eq!(nearest_rank(&more, P95), Some(96.0));
        assert_eq!(nearest_rank(&more, P99), Some(100.0));
    }

    /// One pass' observations: lag series only, idle series skipped (T-V1
    /// hold twin), one sample per pipeline/statement with the max across
    /// nodes, unlabelled / non-finite / negative readings ignored.
    #[test]
    fn observations_pick_one_non_idle_lag_per_pair() {
        let p3 = |node: &'static str| [("node", node), ("pipeline", "3"), ("statement", "0")];
        let snap = Snapshot {
            samples: vec![
                sample(LAG_METRIC, &p3("edge-1"), 12.0),
                // same pair on a second node, worse lag → it wins
                sample(LAG_METRIC, &p3("edge-2"), 40.0),
                // idle series: the lag over silence is not sampled
                sample(
                    LAG_METRIC,
                    &[("node", "edge-1"), ("pipeline", "4"), ("statement", "0")],
                    900.0,
                ),
                sample(
                    IDLE_METRIC,
                    &[("node", "edge-1"), ("pipeline", "4"), ("statement", "0")],
                    1.0,
                ),
                // idle gauge 0 = backlog verdict, sampled as usual
                sample(
                    LAG_METRIC,
                    &[("node", "edge-1"), ("pipeline", "5"), ("statement", "1")],
                    3.0,
                ),
                sample(
                    IDLE_METRIC,
                    &[("node", "edge-1"), ("pipeline", "5"), ("statement", "1")],
                    0.0,
                ),
                // not a lag reading
                sample(LAG_METRIC, &[("node", "edge-1"), ("pipeline", "6")], 1.0),
                sample(
                    LAG_METRIC,
                    &[("node", "edge-1"), ("pipeline", "7"), ("statement", "0")],
                    f64::NAN,
                ),
                sample(
                    LAG_METRIC,
                    &[("node", "edge-1"), ("pipeline", "8"), ("statement", "0")],
                    -1.0,
                ),
                sample(
                    "tailrace_pipeline_state",
                    &[("node", "edge-1"), ("pipeline", "3")],
                    0.0,
                ),
            ],
            unreachable_nodes: vec![],
        };
        let obs = observations(&snap);
        assert_eq!(
            obs,
            vec![
                NewSloLagSample {
                    pipeline: "3".into(),
                    statement: "0".into(),
                    node: "edge-2".into(),
                    lag_secs: 40.0,
                },
                NewSloLagSample {
                    pipeline: "5".into(),
                    statement: "1".into(),
                    node: "edge-1".into(),
                    lag_secs: 3.0,
                },
            ]
        );
    }

    /// Summaries: per pair, quantiles from the sorted window values, the
    /// newest sample's node, coverage bounds, numeric ordering ("10" after
    /// "9"), and an empty input summarizes to nothing.
    #[test]
    fn summarize_groups_and_orders_numerically() {
        assert!(summarize(&[]).is_empty());
        let rows = vec![
            row("10", "0", "a", 4.0, 1_000),
            row("9", "0", "a", 9.0, 1_000),
            row("9", "0", "a", 1.0, 2_000),
            row("9", "0", "b", 5.0, 3_000),
        ];
        let out = summarize(&rows);
        assert_eq!(
            out.iter().map(|s| s.pipeline.as_str()).collect::<Vec<_>>(),
            vec!["9", "10"]
        );
        assert_eq!(
            out[0],
            SeriesSummary {
                pipeline: "9".into(),
                statement: "0".into(),
                node: "b".into(),
                samples: 3,
                p50: 5.0,
                p95: 9.0,
                p99: 9.0,
                oldest_ms: 1_000,
                newest_ms: 3_000,
            }
        );
        assert_eq!((out[1].samples, out[1].p50, out[1].p99), (1, 4.0, 4.0));
    }

    /// Window bounds are (now − window, now] in epoch ms.
    #[test]
    fn window_bounds_are_half_open_ms() {
        assert_eq!(
            window_bounds(1_700_000_060_000, 60),
            (1_700_000_000_000, 1_700_000_060_000)
        );
        assert_eq!(window_bounds(5, 60), (0, 5), "saturates at the epoch");
    }
}
