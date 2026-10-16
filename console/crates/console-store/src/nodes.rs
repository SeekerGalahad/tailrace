//! Node repository (§5.2 `node` / `node_event` / `node_metrics` /
//! `pipeline_metrics`) — register upsert + heartbeat consumption (§4.3).

use anyhow::Result;

use super::models::{NewNodeEvent, NewNodeMetrics, NewPipelineMetrics, Node};
use super::Store;

const NODE_COLS: &str =
    "id, node_key, name, api_url, status, host, ssh_port, ssh_user, engine_port, \
                         install_dir, artifact_id, engine_version, deploy_status, register_mode, \
                         capacity_json, bootstrap_token, last_heartbeat, created_at";

fn node_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Node> {
    Ok(Node {
        id: row.get(0)?,
        node_key: row.get(1)?,
        name: row.get(2)?,
        api_url: row.get(3)?,
        status: row.get(4)?,
        host: row.get(5)?,
        ssh_port: row.get(6)?,
        ssh_user: row.get(7)?,
        engine_port: row.get(8)?,
        install_dir: row.get(9)?,
        artifact_id: row.get(10)?,
        engine_version: row.get(11)?,
        deploy_status: row.get(12)?,
        register_mode: row.get(13)?,
        capacity_json: row.get(14)?,
        bootstrap_token: row.get(15)?,
        last_heartbeat: row.get(16)?,
        created_at: row.get(17)?,
    })
}

/// Human-readable name derived from node_key ("sn-0123456789abcdef" ->
/// "sn-01234567", i.e. prefix + 8 hex), mirroring the §4.3 register example.
pub fn derive_name(node_key: &str) -> &str {
    &node_key[..node_key.len().min(11)]
}

impl Store {
    pub fn node_by_key(&self, node_key: &str) -> Result<Option<Node>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare(&format!("SELECT {NODE_COLS} FROM node WHERE node_key = ?1"))?;
            let mut rows = stmt.query([node_key])?;
            match rows.next()? {
                Some(row) => Ok(Some(node_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    /// Idempotent self-registration upsert keyed on node_key (§4.3, audit
    /// H1): new rows are created in `self` register mode; `token_hash` is the
    /// sha256 hex digest of the per-node token the handler just generated
    /// (plaintext never reaches the store) and is rotated on every successful
    /// register; existing rows also have api_url / capacity refreshed.
    /// Returns the node echo.
    pub fn upsert_self_registered(
        &self,
        node_key: &str,
        api_url: Option<&str>,
        capacity_json: Option<&str>,
        token_hash: &str,
    ) -> Result<(i64, String)> {
        self.with_conn(|conn| {
            let sql = "INSERT INTO node (node_key, name, api_url, capacity_json, bootstrap_token, register_mode) \
                       VALUES (?1, ?2, ?3, ?4, ?5, 'self') \
                       ON CONFLICT(node_key) DO UPDATE SET \
                         api_url = COALESCE(excluded.api_url, node.api_url), \
                         capacity_json = COALESCE(excluded.capacity_json, node.capacity_json), \
                         bootstrap_token = excluded.bootstrap_token \
                       RETURNING id, name";
            match conn.query_row(sql, rusqlite::params![node_key, derive_name(node_key), api_url, capacity_json, token_hash], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            }) {
                Ok(v) => Ok(v),
                // Derived name collided with another node's name -> fall back to full node_key.
                Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ErrorCode::ConstraintViolation => {
                    conn.query_row(
                        "INSERT INTO node (node_key, name, api_url, capacity_json, bootstrap_token, register_mode) \
                         VALUES (?1, ?1, ?2, ?3, ?4, 'self') \
                         ON CONFLICT(node_key) DO UPDATE SET \
                           api_url = COALESCE(excluded.api_url, node.api_url), \
                           capacity_json = COALESCE(excluded.capacity_json, node.capacity_json), \
                           bootstrap_token = excluded.bootstrap_token \
                         RETURNING id, name",
                        rusqlite::params![node_key, api_url, capacity_json, token_hash],
                        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
                    )
                    .map_err(Into::into)
                }
                Err(e) => Err(e.into()),
            }
        })
    }

    /// Apply a heartbeat (§4.3): mark ONLINE, refresh drift-correctable
    /// fields. `name` is applied only when it does not collide with another
    /// node's name. Unknown node_key is a no-op returning false (callers must
    /// reject unknown nodes before calling this).
    pub fn apply_heartbeat(
        &self,
        node_key: &str,
        name: Option<&str>,
        api_url: Option<&str>,
        engine_version: Option<&str>,
        capacity_json: Option<&str>,
    ) -> Result<bool> {
        self.with_tx(|conn| {
            let updated = conn.execute(
                "UPDATE node SET status = 'ONLINE', last_heartbeat = datetime('now'), \
                 api_url = COALESCE(?1, api_url), engine_version = COALESCE(?2, engine_version), \
                 capacity_json = COALESCE(?3, capacity_json) WHERE node_key = ?4",
                rusqlite::params![api_url, engine_version, capacity_json, node_key],
            )?;
            if updated == 0 {
                return Ok(false);
            }
            if name.is_some() {
                conn.execute(
                    "UPDATE node SET name = ?1 WHERE node_key = ?2 \
                     AND NOT EXISTS (SELECT 1 FROM node n2 WHERE n2.name = ?1 AND n2.node_key <> ?2)",
                    rusqlite::params![name, node_key],
                )?;
            }
            Ok(true)
        })
    }

    /// Persist acknowledged engine events (§4.3). Rows are pre-validated by
    /// the caller; a unix `ts` is null or garbage -> "now". One transaction
    /// per batch (T8): atomic and a single commit's fsync instead of one per
    /// row.
    pub fn insert_node_events(&self, events: &[NewNodeEvent]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        self.with_tx(|conn| {
            let mut stmt = conn.prepare(
                "INSERT INTO node_event (node_key, pipeline_id, level, kind, message, event_ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, COALESCE(datetime(?6, 'unixepoch'), datetime('now')))",
            )?;
            let mut n = 0;
            for e in events {
                n += stmt.execute(rusqlite::params![
                    e.node_key,
                    e.pipeline_id,
                    e.level,
                    e.kind,
                    e.message,
                    e.ts_unix
                ])?;
            }
            Ok(n)
        })
    }

    pub fn insert_node_metrics(&self, node_id: i64, m: &NewNodeMetrics) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO node_metrics (node_id, rss_bytes, mem_total_bytes, cpu_pct, rx_bps, tx_bps, ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))",
                rusqlite::params![node_id, m.rss_bytes, m.mem_total_bytes, m.cpu_pct, m.rx_bps, m.tx_bps],
            )?;
            Ok(())
        })
    }

    pub fn insert_pipeline_metrics(
        &self,
        node_id: i64,
        rows: &[NewPipelineMetrics],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.with_tx(|conn| {
            let mut stmt = conn.prepare(
                "INSERT INTO pipeline_metrics (pipeline_id, node_id, status, consumed, flushed, flushes, ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))",
            )?;
            let mut n = 0;
            for r in rows {
                n += stmt.execute(rusqlite::params![
                    r.pipeline_id, node_id, r.status, r.consumed, r.flushed, r.flushes
                ])?;
            }
            Ok(n)
        })
    }

    /// Revoke a node's bootstrap token (NULL = revoked, §5.2).
    pub fn revoke_bootstrap_token(&self, node_key: &str) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "UPDATE node SET bootstrap_token = NULL WHERE node_key = ?1",
                [node_key],
            )? > 0)
        })
    }

    // ---------------- admin surface (§4.3 管理端点) ----------------

    pub fn node(&self, id: i64) -> Result<Option<Node>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!("SELECT {NODE_COLS} FROM node WHERE id = ?1"))?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(node_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    pub fn list_nodes(&self) -> Result<Vec<Node>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!("SELECT {NODE_COLS} FROM node ORDER BY id"))?;
            let rows = stmt.query_map([], node_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Manual registration (§4.3 POST /api/nodes/manual, after the healthz
    /// probe succeeded). The console generates the node_key and a per-node
    /// bootstrap token (handed to the operator once); `bootstrap_token` here
    /// is the sha256 hex digest of that token — the plaintext is never stored.
    /// Name uniqueness is the caller's problem to surface as 409 — surfaced
    /// via Err on constraint.
    pub fn insert_manual_node(
        &self,
        node_key: &str,
        name: &str,
        host: &str,
        engine_port: i64,
        api_url: &str,
        bootstrap_token: &str,
    ) -> Result<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO node (node_key, name, api_url, status, host, engine_port, \
                 register_mode, bootstrap_token) \
                 VALUES (?1, ?2, ?3, 'REGISTERED', ?4, ?5, 'manual', ?6)",
                rusqlite::params![node_key, name, api_url, host, engine_port, bootstrap_token],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    /// Delete a node plus its observation rows (node_event is keyed by
    /// node_key and node_metrics by node_id — neither has an FK; deploy_log
    /// cascades via FK). Pipelines must have been re-assigned or stopped
    /// beforehand (the handler guards with `pipelines_assigned_to`).
    /// Runs in one transaction (T8): no orphaned observation rows if a
    /// later statement fails.
    pub fn delete_node_cascade(&self, id: i64, node_key: &str) -> Result<bool> {
        self.with_tx(|conn| {
            let n = conn.execute("DELETE FROM node WHERE id = ?1", [id])?;
            if n == 0 {
                return Ok(false);
            }
            conn.execute("DELETE FROM node_event WHERE node_key = ?1", [node_key])?;
            conn.execute("DELETE FROM node_metrics WHERE node_id = ?1", [id])?;
            Ok(true)
        })
    }

    /// Pipelines with `assigned_node = id` (the node delete guard, §4.3).
    pub fn pipelines_assigned_to(&self, id: i64) -> Result<Vec<i64>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare("SELECT id FROM pipeline WHERE assigned_node = ?1 ORDER BY id")?;
            let rows = stmt.query_map([id], |r| r.get::<_, i64>(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// RUNNING pipeline count per node id (auto-assignment "最少运行" input).
    pub fn running_pipeline_counts(&self) -> Result<Vec<(i64, i64)>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT assigned_node, COUNT(*) FROM pipeline \
                 WHERE assigned_node IS NOT NULL AND status = 'RUNNING' GROUP BY assigned_node",
            )?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Events feed (§4.3): by node_key (or all nodes), within `hours`,
    /// optionally filtered by level; ascending by time.
    pub fn node_events(
        &self,
        node_key: Option<&str>,
        hours: i64,
        level: Option<&str>,
    ) -> Result<Vec<super::models::NodeEventRow>> {
        self.with_conn(|conn| {
            let mut sql = String::from(
                "SELECT event_ts, node_key, pipeline_id, level, kind, message FROM node_event \
                 WHERE event_ts >= datetime('now', ?1)",
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(format!("-{hours} hours"))];
            if let Some(k) = node_key {
                sql.push_str(&format!(" AND node_key = ?{}", params.len() + 1));
                params.push(Box::new(k.to_string()));
            }
            if let Some(l) = level {
                sql.push_str(&format!(" AND level = ?{}", params.len() + 1));
                params.push(Box::new(l.to_string()));
            }
            sql.push_str(" ORDER BY event_ts, id");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(
                params
                    .iter()
                    .map(|p| p.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
                |r| {
                    Ok(super::models::NodeEventRow {
                        ts: r.get(0)?,
                        node_key: r.get(1)?,
                        pipeline_id: r.get(2)?,
                        level: r.get(3)?,
                        kind: r.get(4)?,
                        message: r.get(5)?,
                    })
                },
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Error event count per node_key in the last 24h (list `errors24h`).
    pub fn errors_24h_by_node(&self) -> Result<Vec<(String, i64)>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT node_key, COUNT(*) FROM node_event \
                 WHERE event_ts >= datetime('now', '-24 hours') AND level = 'ERROR' \
                 GROUP BY node_key",
            )?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Total events in the last 24h across all nodes (summary `events24h`).
    pub fn events_24h_total(&self) -> Result<i64> {
        self.with_conn(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM node_event WHERE event_ts >= datetime('now', '-24 hours')",
                [],
                |r| r.get(0),
            )?)
        })
    }

    /// Node resource metrics aggregated to 1-minute buckets (§4.3 metrics;
    /// AVG within the bucket, values NULL when the bucket saw no sample).
    pub fn node_metrics_series(
        &self,
        node_id: i64,
        hours: i64,
    ) -> Result<Vec<super::models::NodeMetricPoint>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT strftime('%Y-%m-%dT%H:%M', ts), AVG(rss_bytes), AVG(mem_total_bytes), \
                        AVG(cpu_pct), AVG(rx_bps), AVG(tx_bps) \
                 FROM node_metrics WHERE node_id = ?1 AND ts >= datetime('now', ?2) \
                 GROUP BY 1 ORDER BY 1",
            )?;
            let rows =
                stmt.query_map(rusqlite::params![node_id, format!("-{hours} hours")], |r| {
                    Ok(super::models::NodeMetricPoint {
                        ts: r.get(0)?,
                        rss_bytes: r.get(1)?,
                        mem_total_bytes: r.get(2)?,
                        cpu_pct: r.get(3)?,
                        rx_bps: r.get(4)?,
                        tx_bps: r.get(5)?,
                    })
                })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Deploy log tail in ascending time order (§4.4 deploy-log).
    pub fn deploy_log_items(
        &self,
        node_id: i64,
        limit: i64,
    ) -> Result<Vec<super::models::DeployLogRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT step, status, output, operator, ts FROM deploy_log WHERE node_id = ?1 \
                 ORDER BY ts DESC, id DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(rusqlite::params![node_id, limit], |r| {
                Ok(super::models::DeployLogRow {
                    step: r.get(0)?,
                    status: r.get(1)?,
                    output: r.get(2)?,
                    operator: r.get(3)?,
                    ts: r.get(4)?,
                })
            })?;
            let mut items: Vec<_> = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            items.reverse(); // ascending for the UI
            Ok(items)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_is_idempotent_upsert() {
        let store = Store::open_in_memory().unwrap();
        // Callers pass sha256 digests (opaque at this layer, audit H1).
        let (id1, name1) = store
            .upsert_self_registered(
                "sn-0123456789abcdef",
                Some("http://10.1.1.5:18100"),
                Some("{}"),
                "hash-1",
            )
            .unwrap();
        assert_eq!(name1, "sn-01234567"); // derived prefix per §4.3 example
        let node = store.node_by_key("sn-0123456789abcdef").unwrap().unwrap();
        assert_eq!(node.register_mode, "self");
        assert_eq!(node.status, "REGISTERED");
        assert_eq!(node.bootstrap_token.as_deref(), Some("hash-1"));
        // second register: same identity, refreshed api_url, rotated token hash
        let (id2, _) = store
            .upsert_self_registered(
                "sn-0123456789abcdef",
                Some("http://10.1.1.9:18100"),
                None,
                "hash-2",
            )
            .unwrap();
        assert_eq!(id1, id2);
        let node = store.node_by_key("sn-0123456789abcdef").unwrap().unwrap();
        assert_eq!(node.api_url.as_deref(), Some("http://10.1.1.9:18100"));
        assert_eq!(
            node.bootstrap_token.as_deref(),
            Some("hash-2"),
            "token digest rotates on every successful register"
        );
    }

    #[test]
    fn heartbeat_marks_online_and_persists_observations() {
        let store = Store::open_in_memory().unwrap();
        store
            .upsert_self_registered("sn-aaaaaaaaaaaaaaaa", Some("http://h:1"), None, "tok")
            .unwrap();
        assert!(store
            .apply_heartbeat(
                "sn-aaaaaaaaaaaaaaaa",
                Some("edge-01"),
                Some("http://h2:1"),
                Some("1.0.0"),
                Some("{}")
            )
            .unwrap());
        let node = store.node_by_key("sn-aaaaaaaaaaaaaaaa").unwrap().unwrap();
        assert_eq!(node.status, "ONLINE");
        assert_eq!(node.name, "edge-01");
        assert_eq!(node.engine_version.as_deref(), Some("1.0.0"));
        assert!(node.last_heartbeat.is_some());
        // unknown node -> false
        assert!(!store
            .apply_heartbeat("sn-unknown", None, None, None, None)
            .unwrap());

        // events + metrics piggyback
        let n = store
            .insert_node_events(&[NewNodeEvent {
                node_key: "sn-aaaaaaaaaaaaaaaa".into(),
                pipeline_id: Some(42),
                level: "ERROR".into(),
                kind: "SINK_WRITE_FAILED".into(),
                message: "boom".into(),
                ts_unix: Some(1760160000),
            }])
            .unwrap();
        assert_eq!(n, 1);
        store
            .insert_node_metrics(
                node.id,
                &NewNodeMetrics {
                    rss_bytes: Some(91226112),
                    cpu_pct: Some(7.2),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .insert_pipeline_metrics(
                node.id,
                &[NewPipelineMetrics {
                    pipeline_id: 42,
                    status: Some("RUNNING".into()),
                    consumed: 128000,
                    flushed: 127996,
                    flushes: 12,
                }],
            )
            .unwrap();
        store
            .with_conn(|c| {
                let ev: i64 = c.query_row("SELECT COUNT(*) FROM node_event", [], |r| r.get(0))?;
                let nm: i64 = c.query_row("SELECT COUNT(*) FROM node_metrics", [], |r| r.get(0))?;
                let pm: i64 =
                    c.query_row("SELECT COUNT(*) FROM pipeline_metrics", [], |r| r.get(0))?;
                let ts: String =
                    c.query_row("SELECT event_ts FROM node_event", [], |r| r.get(0))?;
                assert_eq!((ev, nm, pm), (1, 1, 1));
                assert_eq!(ts, "2025-10-11 05:20:00"); // 1760160000 as UTC (SQLite unixepoch)
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn revoke_clears_token() {
        let store = Store::open_in_memory().unwrap();
        store
            .upsert_self_registered("sn-b", None, None, "tok")
            .unwrap();
        assert!(store.revoke_bootstrap_token("sn-b").unwrap());
        let node = store.node_by_key("sn-b").unwrap().unwrap();
        assert!(node.bootstrap_token.is_none());
    }

    /// T8 heartbeat write-path stress (file-backed DB so the real fsync/WAL
    /// path is exercised): concurrent 500-event batches must each finish in
    /// seconds, and every row must land (conservation behind the single
    /// `Mutex<Connection>`).
    #[test]
    fn concurrent_heartbeat_500_event_batches_finish_in_seconds() {
        let dir = std::env::temp_dir().join(format!("tailrace-t8-stress-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("console.db");
        let _ = std::fs::remove_file(&db);
        let store = Store::open(&db).unwrap();
        store
            .upsert_self_registered("sn-stress0000000001", Some("http://h:1"), None, "tok")
            .unwrap();

        const THREADS: usize = 4;
        const BATCH: usize = 500;
        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let store = store.clone();
            handles.push(std::thread::spawn(move || {
                let events: Vec<NewNodeEvent> = (0..BATCH)
                    .map(|i| NewNodeEvent {
                        node_key: "sn-stress0000000001".into(),
                        pipeline_id: Some(i as i64),
                        level: "INFO".into(),
                        kind: "BENCH".into(),
                        message: "x".repeat(120),
                        ts_unix: None,
                    })
                    .collect();
                let t0 = std::time::Instant::now();
                let n = store.insert_node_events(&events).unwrap();
                assert_eq!(n, BATCH);
                t0.elapsed()
            }));
        }
        let mut worst = std::time::Duration::ZERO;
        for h in handles {
            worst = worst.max(h.join().unwrap());
        }
        let total = store.events_24h_total().unwrap();
        assert_eq!(total, (THREADS * BATCH) as i64, "every event must land");
        eprintln!(
            "t8 stress: {THREADS} concurrent {BATCH}-event batches, worst batch wall time {worst:?}"
        );
        assert!(
            worst < std::time::Duration::from_secs(5),
            "worst 500-event batch took {worst:?} (>= 5s)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
