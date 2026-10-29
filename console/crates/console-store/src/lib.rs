//! console-store — SQLite metadata store for the tailrace console (design doc §5).
//!
//! Single SQLite file (`data/console.db`), WAL + busy_timeout + foreign_keys,
//! versioned migrations via rusqlite_migration (`migrations/*.sql`, auto
//! roll-forward on open). The store owns a `Mutex<Connection>`; call sites run
//! short synchronous statements (sub-millisecond) and never hold the lock
//! across an await point.

mod artifacts;
mod audit;
mod backfill;
mod datasources;
mod models;
mod nodes;
mod password;
mod pipelines;
mod secretbox;
mod sessions;
mod users;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};

pub use artifacts::ArtifactInsert;
pub use datasources::DatasourceUpdate;
pub use models::{
    Artifact, AuditEntry, BackfillJob, Datasource, DatasourceCredential, DeployLogRow,
    NewDeployedNode, NewNodeEvent, NewNodeMetrics, NewPipelineMetrics, Node, NodeEventRow,
    NodeMetricPoint, Pipeline, PipelineTrendPoint, PipelineVersion, TableObject, User,
};
pub use password::{hash_password, verify_password};
pub use pipelines::{PipelineFull, PipelineWrite};
pub use secretbox::{CredentialCipher, MASTER_KEY_ENV, MASTER_KEY_FILE};

/// Versioned migrations; 00_init.sql is the §5.2 DDL verbatim.
fn migrations() -> Migrations<'static> {
    Migrations::new(vec![
        M::up(include_str!("../migrations/00_init.sql")),
        // T-D1: table_object.sequence_column (dim full-table load key /
        // doris UNIQUE KEY dedup basis / jdbc cursor column).
        M::up(include_str!("../migrations/01_table_sequence_column.sql")),
        // T-A2: backfill_job — bounded-replay job rows (pending → running →
        // done|failed), the progress surface for the console-side executor.
        M::up(include_str!("../migrations/02_backfill_job.sql")),
        // T-A7: table_object.key_model (unique/duplicate) — the publish-gate
        // basis for partial-column declarations (Doris: UNIQUE MoW only).
        M::up(include_str!("../migrations/03_table_key_model.sql")),
    ])
}

/// A6 (§7.4 `tailrace_console_db_write_seconds`): SQLite write-path latency
/// statistics accumulated at the `with_tx` chokepoint — the T8 batched
/// transactional path (heartbeats, event batches, publishes). Single-
/// statement autocommit writes ride `with_conn` and are not timed (they are
/// not the T8 bottleneck surface). Read at console /metrics scrape time.
/// Durations are MICROseconds — console transactions are routinely sub-ms
/// and would all read 0 at ms granularity.
#[derive(Debug, Clone, Copy, Default)]
pub struct DbWriteStats {
    /// completed transactions (success or failure — latency is latency)
    pub count: u64,
    /// cumulative transaction wall time (µs)
    pub us_total: u64,
    /// most recent transaction wall time (µs)
    pub us_last: u64,
}

impl DbWriteStats {
    /// Mean transaction latency in µs (0 before the first write).
    pub fn avg_us(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.us_total as f64 / self.count as f64
        }
    }
}

/// Handle to the console database. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
    /// A6 write-latency accumulators, µs (Arc-shared with clones).
    tx_count: Arc<AtomicU64>,
    tx_us_total: Arc<AtomicU64>,
    tx_us_last: Arc<AtomicU64>,
}

impl Store {
    /// Open (creating if needed) the database file, apply pragmas, run
    /// migrations, then purge expired sessions and metrics older than 7 days
    /// (§5: observation series kept 7 days, purged at startup).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).context("open console.db")?;
        let store = Self::from_conn(conn)?;
        store.purge_expired()?;
        Ok(store)
    }

    /// In-memory store for tests.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("open :memory:")?;
        let store = Self::from_conn(conn)?;
        store.purge_expired()?;
        Ok(store)
    }

    fn from_conn(mut conn: Connection) -> Result<Self> {
        // §5.1 pragmas. journal_mode=WAL is a no-op (stays "memory") for :memory:.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        // SQLite's documented recommendation for WAL: NORMAL keeps the
        // database consistent across crashes (WAL is only deferred-sync at
        // commit) while heartbeats stop paying one fsync per autocommit
        // statement. T8.
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        migrations()
            .to_latest(&mut conn)
            .context("apply migrations")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            tx_count: Arc::new(AtomicU64::new(0)),
            tx_us_total: Arc::new(AtomicU64::new(0)),
            tx_us_last: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Run `f` with the raw connection (tests / maintenance).
    pub fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let conn = self.conn.lock().unwrap();
        f(&conn)
    }

    /// Run `f` inside one explicit `Connection::transaction()` (T8):
    /// multi-statement and batch writes become atomic (no half-written
    /// state on failure) and pay one commit instead of one fsync per
    /// statement. The `Mutex` guarantees at most one transaction at a time.
    /// A6: every transaction is timed (the `tailrace_console_db_write_seconds`
    /// basis, µs granularity) — counted on success and failure alike, mutex
    /// wait included (queueing behind another writer is latency a caller
    /// experiences).
    fn with_tx<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let t0 = Instant::now();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let out = match f(&tx) {
            Ok(v) => tx.commit().map_err(Into::into).map(|()| v),
            Err(e) => {
                let _ = tx.rollback();
                Err(e)
            }
        };
        drop(conn);
        let us = t0.elapsed().as_micros() as u64;
        self.tx_count.fetch_add(1, Ordering::Relaxed);
        self.tx_us_total.fetch_add(us, Ordering::Relaxed);
        self.tx_us_last.store(us, Ordering::Relaxed);
        out
    }

    /// A6: SQLite write-path latency snapshot (console /metrics).
    pub fn db_write_stats(&self) -> DbWriteStats {
        DbWriteStats {
            count: self.tx_count.load(Ordering::Relaxed),
            us_total: self.tx_us_total.load(Ordering::Relaxed),
            us_last: self.tx_us_last.load(Ordering::Relaxed),
        }
    }

    fn purge_expired(&self) -> Result<()> {
        self.with_tx(|conn| {
            conn.execute(
                "DELETE FROM pipeline_metrics WHERE ts < datetime('now', '-7 days')",
                [],
            )
            .context("purge pipeline_metrics")?;
            conn.execute(
                "DELETE FROM node_metrics WHERE ts < datetime('now', '-7 days')",
                [],
            )
            .context("purge node_metrics")?;
            conn.execute(
                "DELETE FROM session WHERE expires_at <= datetime('now')",
                [],
            )
            .context("purge sessions")?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Migration 00_init must create every §5.2 table and index.
    #[test]
    fn migration_creates_all_tables() {
        let store = Store::open_in_memory().unwrap();
        store
            .with_conn(|conn| {
                let mut names: Vec<String> = Vec::new();
                let mut stmt = conn.prepare(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','index') \
                     AND name NOT LIKE 'sqlite_%' AND name != '_rusqlite_migration' ORDER BY name",
                )?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                for row in rows {
                    names.push(row?);
                }
                for expected in [
                    "user",
                    "session",
                    "datasource",
                    "datasource_credential",
                    "table_object",
                    "node",
                    "artifact",
                    "deploy_log",
                    "idx_deploy_log_node",
                    "node_event",
                    "idx_node_event_node",
                    "pipeline",
                    "pipeline_version",
                    "pipeline_metrics",
                    "idx_pm_pipe",
                    "node_metrics",
                    "idx_nm_node",
                    "audit_log",
                    "idx_audit_entity",
                    // T-A2 (migration 02)
                    "backfill_job",
                    "idx_backfill_job_pipe",
                ] {
                    assert!(
                        names.contains(&expected.to_string()),
                        "missing {expected}: {names:?}"
                    );
                }
                let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
                assert_eq!(
                    version, 4,
                    "user_version must be 4 after 00_init + 01_sequence + 02_backfill_job + 03_key_model"
                );
                Ok(())
            })
            .unwrap();
    }

    /// A6 (`tailrace_console_db_write_seconds`): every `with_tx` lands in
    /// the write-latency accumulators, and clones share them (Arc, like the
    /// connection itself). (Single-statement autocommit writes ride
    /// `with_conn` and are NOT timed — the T8 bottleneck observation targets
    /// the batched transactional path: heartbeats, events, publishes.)
    #[test]
    fn db_write_stats_accumulate_per_transaction() {
        use models::NewNodeEvent;
        let store = Store::open_in_memory().unwrap();
        let before = store.db_write_stats();
        assert!(
            before.count >= 1,
            "open itself runs purge_expired via with_tx"
        );
        let ev = |msg: &str| {
            vec![NewNodeEvent {
                node_key: "nk-metrics".into(),
                pipeline_id: None,
                level: "INFO".into(),
                kind: "TEST".into(),
                message: msg.into(),
                ts_unix: None,
            }]
        };
        store.insert_node_events(&ev("one")).unwrap();
        let mid = store.db_write_stats();
        assert_eq!(
            mid.count,
            before.count + 1,
            "one with_tx write = one timed tx"
        );
        assert!(mid.us_last <= mid.us_total, "last ≤ cumulative");
        // clones share the accumulators (Arc-shared, like the connection)
        let clone = store.clone();
        store.insert_node_events(&ev("two")).unwrap();
        assert_eq!(
            clone.db_write_stats().count,
            store.db_write_stats().count,
            "Arc-shared stats"
        );
        assert_eq!(store.db_write_stats().count, mid.count + 1);
    }

    /// WAL on a real file, and reopening the same file skips re-migration.
    #[test]
    fn file_store_sets_wal_and_is_reopenable() {
        let dir = std::env::temp_dir().join(format!("tailrace-store-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("console.db");
        let _ = std::fs::remove_file(&db);
        {
            let store = Store::open(&db).unwrap();
            store
                .with_conn(|c| {
                    let mode: String = c.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
                    assert_eq!(mode, "wal");
                    // T8: WAL's documented companion pragma (1 = NORMAL).
                    let sync: i64 = c.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
                    assert_eq!(sync, 1, "synchronous must be NORMAL under WAL");
                    Ok(())
                })
                .unwrap();
        }
        // Reopen: migrations are a no-op, data survives.
        let store = Store::open(&db).unwrap();
        store.create_user("u1", "phc-hash", None, "admin").unwrap();
        let store2 = Store::open(&db).unwrap();
        assert!(store2.user_by_username("u1").unwrap().is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
