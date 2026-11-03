//! SLO lag sample ring (R11 T-V4, `slo_lag_sample` table): the alert
//! evaluator's per-pass lag observations behind the console's p50/p95/p99
//! surface. A pass is written as one batch — insert its rows and prune every
//! row at or before the retention cutoff in the same transaction (the ring);
//! reads hand back the raw rows of a window, so the quantiles are computed
//! from (and can be recomputed from) the samples themselves. The store is
//! policy-free: retention, window and quantile method belong to the caller
//! (`tailrace-console`'s `slo` module).

use anyhow::Result;

use super::models::{NewSloLagSample, SloLagSample};
use super::Store;

impl Store {
    /// Record one evaluator pass: insert `rows` (all stamped `ts_ms`) and
    /// drop every sample with `ts_ms <= prune_through_ms`, atomically —
    /// returns the number of pruned rows. A pass with nothing to record
    /// still prunes (time-based eviction keeps running while no lag series
    /// exist), as one autocommit DELETE outside the timed batch path.
    pub fn record_slo_lag_samples(
        &self,
        ts_ms: i64,
        rows: &[NewSloLagSample],
        prune_through_ms: i64,
    ) -> Result<usize> {
        if rows.is_empty() {
            return self.prune_slo_lag_samples(prune_through_ms);
        }
        self.with_tx(|conn| {
            let mut insert = conn.prepare(
                "INSERT INTO slo_lag_sample (pipeline, statement, node, lag_secs, ts_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for r in rows {
                insert.execute(rusqlite::params![
                    r.pipeline,
                    r.statement,
                    r.node,
                    r.lag_secs,
                    ts_ms
                ])?;
            }
            Ok(conn.execute(
                "DELETE FROM slo_lag_sample WHERE ts_ms <= ?1",
                [prune_through_ms],
            )?)
        })
    }

    /// Drop every sample with `ts_ms <= prune_through_ms`; returns the
    /// number of rows removed. The per-pass ring eviction and the console's
    /// boot-time sweep (the tail a downtime left behind) both land here.
    pub fn prune_slo_lag_samples(&self, prune_through_ms: i64) -> Result<usize> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "DELETE FROM slo_lag_sample WHERE ts_ms <= ?1",
                [prune_through_ms],
            )?)
        })
    }

    /// The samples of the half-open window (`after_ms`, `until_ms`],
    /// optionally narrowed to one pipeline and/or statement label value,
    /// ordered by (pipeline, statement, ts_ms, insertion) — consecutive rows
    /// of one series, oldest first.
    pub fn slo_lag_samples(
        &self,
        after_ms: i64,
        until_ms: i64,
        pipeline: Option<&str>,
        statement: Option<&str>,
    ) -> Result<Vec<SloLagSample>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT pipeline, statement, node, lag_secs, ts_ms FROM slo_lag_sample \
                 WHERE ts_ms > ?1 AND ts_ms <= ?2 \
                 AND (?3 IS NULL OR pipeline = ?3) AND (?4 IS NULL OR statement = ?4) \
                 ORDER BY pipeline, statement, ts_ms, id",
            )?;
            let rows = stmt.query_map(
                rusqlite::params![after_ms, until_ms, pipeline, statement],
                |r| {
                    Ok(SloLagSample {
                        pipeline: r.get(0)?,
                        statement: r.get(1)?,
                        node: r.get(2)?,
                        lag_secs: r.get(3)?,
                        ts_ms: r.get(4)?,
                    })
                },
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 1_700_000_000_000;

    fn obs(pipeline: &str, statement: &str, node: &str, lag: f64) -> NewSloLagSample {
        NewSloLagSample {
            pipeline: pipeline.into(),
            statement: statement.into(),
            node: node.into(),
            lag_secs: lag,
        }
    }

    fn count(store: &Store) -> i64 {
        store
            .with_conn(
                |c| Ok(c.query_row("SELECT COUNT(*) FROM slo_lag_sample", [], |r| r.get(0))?),
            )
            .unwrap()
    }

    /// A batch lands with the pass time; the window is half-open (after,
    /// until] — a row exactly at `after` is out, one exactly at `until` is
    /// in; rows come back grouped per series, oldest first; the optional
    /// filters narrow by exact label value.
    #[test]
    fn batches_land_and_windows_are_half_open() {
        let store = Store::open_in_memory().unwrap();
        for (i, lag) in [5.0, 7.0, 6.0].into_iter().enumerate() {
            let ts = T0 + i as i64 * 15_000;
            store
                .record_slo_lag_samples(
                    ts,
                    &[
                        obs("3", "0", "edge-1", lag),
                        obs("4", "1", "edge-2", lag * 10.0),
                    ],
                    ts - 3_600_000,
                )
                .unwrap();
        }
        assert_eq!(count(&store), 6);

        let all = store
            .slo_lag_samples(T0 - 1, T0 + 30_000, None, None)
            .unwrap();
        assert_eq!(all.len(), 6);
        assert_eq!(
            all.iter()
                .map(|s| (s.pipeline.as_str(), s.ts_ms))
                .collect::<Vec<_>>(),
            vec![
                ("3", T0),
                ("3", T0 + 15_000),
                ("3", T0 + 30_000),
                ("4", T0),
                ("4", T0 + 15_000),
                ("4", T0 + 30_000),
            ],
            "grouped per series, oldest first"
        );
        assert_eq!(all[0].node, "edge-1");
        assert_eq!(all[4].lag_secs, 70.0);

        // half-open bounds: `after` exclusive, `until` inclusive
        let w = store
            .slo_lag_samples(T0, T0 + 15_000, Some("3"), None)
            .unwrap();
        assert_eq!(
            w.iter().map(|s| s.ts_ms).collect::<Vec<_>>(),
            vec![T0 + 15_000]
        );
        // filters: statement alone, both, and a label value nobody reported
        assert_eq!(
            store
                .slo_lag_samples(T0 - 1, T0 + 30_000, None, Some("1"))
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            store
                .slo_lag_samples(T0 - 1, T0 + 30_000, Some("3"), Some("0"))
                .unwrap()
                .len(),
            3
        );
        assert!(store
            .slo_lag_samples(T0 - 1, T0 + 30_000, Some("9"), None)
            .unwrap()
            .is_empty());
    }

    /// The ring: each recorded pass prunes everything at or before its
    /// cutoff in the same batch; an empty pass still prunes; the standalone
    /// prune (boot sweep) reports what it removed and is idempotent.
    #[test]
    fn passes_prune_the_ring_and_empty_passes_still_evict() {
        let store = Store::open_in_memory().unwrap();
        store
            .record_slo_lag_samples(T0, &[obs("3", "0", "a", 1.0)], 0)
            .unwrap();
        store
            .record_slo_lag_samples(T0 + 60_000, &[obs("3", "0", "a", 2.0)], 0)
            .unwrap();
        // a pass whose cutoff sits exactly on the first row evicts it
        let pruned = store
            .record_slo_lag_samples(T0 + 120_000, &[obs("3", "0", "a", 3.0)], T0)
            .unwrap();
        assert_eq!(pruned, 1, "cutoff is inclusive");
        let left: Vec<f64> = store
            .slo_lag_samples(0, i64::MAX, None, None)
            .unwrap()
            .iter()
            .map(|s| s.lag_secs)
            .collect();
        assert_eq!(left, vec![2.0, 3.0]);

        // an empty pass inserts nothing but still evicts
        assert_eq!(
            store
                .record_slo_lag_samples(T0 + 180_000, &[], T0 + 60_000)
                .unwrap(),
            1
        );
        assert_eq!(count(&store), 1);

        // boot sweep: removes the stale tail, then finds nothing
        assert_eq!(store.prune_slo_lag_samples(T0 + 120_000).unwrap(), 1);
        assert_eq!(store.prune_slo_lag_samples(T0 + 120_000).unwrap(), 0);
        assert_eq!(count(&store), 0);
    }
}
