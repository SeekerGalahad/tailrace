//! console-store — SQLite metadata store for the tailrace console (design doc §5).
//!
//! Single SQLite file (`data/console.db`), WAL + busy_timeout + foreign_keys,
//! versioned migrations via rusqlite_migration (`migrations/*.sql`, auto
//! roll-forward on open). The store owns a `Mutex<Connection>`; call sites run
//! short synchronous statements (sub-millisecond) and never hold the lock
//! across an await point.

mod artifacts;
mod audit;
mod datasources;
mod models;
mod nodes;
mod password;
mod pipelines;
mod secretbox;
mod sessions;
mod users;

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};

pub use artifacts::ArtifactInsert;
pub use datasources::DatasourceUpdate;
pub use models::{
    Artifact, AuditEntry, Datasource, DatasourceCredential, DeployLogRow, NewDeployedNode,
    NewNodeEvent, NewNodeMetrics, NewPipelineMetrics, Node, NodeEventRow, NodeMetricPoint,
    Pipeline, PipelineTrendPoint, PipelineVersion, TableObject, User,
};
pub use password::{hash_password, verify_password};
pub use pipelines::{PipelineFull, PipelineWrite};
pub use secretbox::{CredentialCipher, MASTER_KEY_ENV, MASTER_KEY_FILE};

/// Versioned migrations; 00_init.sql is the §5.2 DDL verbatim.
fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(include_str!("../migrations/00_init.sql"))])
}

/// Handle to the console database. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
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
    fn with_tx<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        match f(&tx) {
            Ok(v) => tx.commit().map_err(Into::into).map(|()| v),
            Err(e) => {
                let _ = tx.rollback();
                Err(e)
            }
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
                ] {
                    assert!(
                        names.contains(&expected.to_string()),
                        "missing {expected}: {names:?}"
                    );
                }
                let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
                assert_eq!(version, 1, "user_version must be 1 after 00_init");
                Ok(())
            })
            .unwrap();
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
