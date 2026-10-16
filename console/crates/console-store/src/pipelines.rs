//! Pipeline + release-version repositories (§5.2 `pipeline` / `pipeline_version`
//! / `pipeline_metrics`; §4.5 business flow).

use anyhow::Result;

use super::models::{Pipeline, PipelineTrendPoint, PipelineVersion};
use super::Store;

const PIPE_COLS: &str =
    "id, name, sql_content, spec_json, ds_mapping_json, status, assigned_node, \
                         preferred_node, version, checkpoint_ms, created_at, updated_at";
const PIPE_COLS_FULL: &str = "id, name, sql_content, spec_json, ds_mapping_json, status, assigned_node, \
                              preferred_node, version, checkpoint_ms, created_at, updated_at, config_json, deploy_error, deploy_epoch";

fn pipe_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Pipeline> {
    Ok(Pipeline {
        id: row.get(0)?,
        name: row.get(1)?,
        sql_content: row.get(2)?,
        spec_json: row.get(3)?,
        ds_mapping_json: row.get(4)?,
        status: row.get(5)?,
        assigned_node: row.get(6)?,
        preferred_node: row.get(7)?,
        version: row.get(8)?,
        checkpoint_ms: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

/// Pipeline row including the advanced-config / deploy columns (detail views,
/// deploy flow).
#[derive(Debug, Clone)]
pub struct PipelineFull {
    pub base: Pipeline,
    pub config_json: Option<String>,
    pub deploy_error: Option<String>,
    pub deploy_epoch: i64,
}

fn pipe_full_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PipelineFull> {
    Ok(PipelineFull {
        base: pipe_from_row(row)?,
        config_json: row.get(12)?,
        deploy_error: row.get(13)?,
        deploy_epoch: row.get(14)?,
    })
}

/// Create/update outcome: pipeline names are UNIQUE (409 on collision).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineWrite {
    Created(i64),
    NameConflict,
}

impl Store {
    /// Create a draft pipeline (§4.5 POST /api/pipelines).
    #[allow(clippy::too_many_arguments)]
    pub fn create_pipeline(
        &self,
        name: &str,
        sql_content: &str,
        ds_mapping_json: Option<&str>,
        checkpoint_ms: i64,
        preferred_node: Option<i64>,
        config_json: Option<&str>,
    ) -> Result<PipelineWrite> {
        self.with_conn(|conn| {
            let dup: i64 = conn.query_row(
                "SELECT COUNT(*) FROM pipeline WHERE name = ?1",
                [name],
                |r| r.get(0),
            )?;
            if dup > 0 {
                return Ok(PipelineWrite::NameConflict);
            }
            conn.execute(
                "INSERT INTO pipeline (name, sql_content, ds_mapping_json, checkpoint_ms, preferred_node, config_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![name, sql_content, ds_mapping_json, checkpoint_ms, preferred_node, config_json],
            )?;
            Ok(PipelineWrite::Created(conn.last_insert_rowid()))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_pipeline(
        &self,
        id: i64,
        name: &str,
        sql_content: &str,
        ds_mapping_json: Option<&str>,
        checkpoint_ms: i64,
        preferred_node: Option<i64>,
        config_json: Option<&str>,
    ) -> Result<PipelineWrite> {
        self.with_conn(|conn| {
            let dup: i64 = conn.query_row(
                "SELECT COUNT(*) FROM pipeline WHERE name = ?1 AND id <> ?2",
                rusqlite::params![name, id],
                |r| r.get(0),
            )?;
            if dup > 0 {
                return Ok(PipelineWrite::NameConflict);
            }
            // §4.5: saving a non-DRAFT pipeline moves it to PENDING_CHANGES
            // ("已保存为待发布变更，需发布后部署才生效").
            let n = conn.execute(
                "UPDATE pipeline SET name = ?1, sql_content = ?2, ds_mapping_json = ?3, \
                 checkpoint_ms = ?4, preferred_node = ?5, config_json = ?6, updated_at = datetime('now'), \
                 status = CASE WHEN status = 'DRAFT' THEN 'DRAFT' ELSE 'PENDING_CHANGES' END \
                 WHERE id = ?7",
                rusqlite::params![name, sql_content, ds_mapping_json, checkpoint_ms, preferred_node, config_json, id],
            )?;
            if n == 0 {
                return Ok(PipelineWrite::NameConflict); // caller pre-checks existence
            }
            Ok(PipelineWrite::Created(id))
        })
    }

    pub fn pipeline(&self, id: i64) -> Result<Option<Pipeline>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare(&format!("SELECT {PIPE_COLS} FROM pipeline WHERE id = ?1"))?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(pipe_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    pub fn pipeline_full(&self, id: i64) -> Result<Option<PipelineFull>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {PIPE_COLS_FULL} FROM pipeline WHERE id = ?1"
            ))?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(pipe_full_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    /// Paged list (§4.5 GET /api/pipelines?page&pageSize) ordered by id desc
    /// (newest first), plus the total row count.
    pub fn list_pipelines_paged(&self, page: i64, page_size: i64) -> Result<(Vec<Pipeline>, i64)> {
        self.with_conn(|conn| {
            let total: i64 = conn.query_row("SELECT COUNT(*) FROM pipeline", [], |r| r.get(0))?;
            let mut stmt = conn.prepare(&format!(
                "SELECT {PIPE_COLS} FROM pipeline ORDER BY id DESC LIMIT ?1 OFFSET ?2"
            ))?;
            let rows = stmt.query_map(
                rusqlite::params![page_size, (page - 1).max(0) * page_size],
                pipe_from_row,
            )?;
            Ok((rows.collect::<rusqlite::Result<Vec<_>>>()?, total))
        })
    }

    pub fn list_pipelines(&self) -> Result<Vec<Pipeline>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare(&format!("SELECT {PIPE_COLS} FROM pipeline ORDER BY id"))?;
            let rows = stmt.query_map([], pipe_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub fn delete_pipeline(&self, id: i64) -> Result<bool> {
        self.with_conn(|conn| Ok(conn.execute("DELETE FROM pipeline WHERE id = ?1", [id])? > 0))
    }

    /// Publish (§4.5): bump version, store the materialized spec snapshot on
    /// the pipeline row and in `pipeline_version`, move to PUBLISHED.
    pub fn set_pipeline_published(
        &self,
        id: i64,
        spec_json: &str,
        sql_content: &str,
        published_by: &str,
    ) -> Result<i64> {
        self.with_conn(|conn| {
            let version: i64 = conn.query_row(
                "UPDATE pipeline SET version = version + 1, spec_json = ?2, status = 'PUBLISHED', \
                 updated_at = datetime('now') WHERE id = ?1 RETURNING version",
                rusqlite::params![id, spec_json],
                |r| r.get(0),
            )?;
            conn.execute(
                "INSERT INTO pipeline_version (pipeline_id, version, spec_json, sql_content, published_by) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![id, version, spec_json, sql_content, published_by],
            )?;
            Ok(version)
        })
    }

    /// Successful deploy: RUNNING on the assigned node, clear the error,
    /// bump the deploy epoch (heartbeat write-back staleness guard, §5.2).
    pub fn set_pipeline_deployed(&self, id: i64, node_id: i64) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE pipeline SET status = 'RUNNING', assigned_node = ?2, deploy_error = NULL, \
                 deploy_epoch = deploy_epoch + 1, updated_at = datetime('now') WHERE id = ?1",
                rusqlite::params![id, node_id],
            )?;
            Ok(())
        })
    }

    /// Engine rejected the plan / node unreachable: DEPLOY_FAILED + reason.
    pub fn set_pipeline_deploy_failed(&self, id: i64, error: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE pipeline SET status = 'DEPLOY_FAILED', deploy_error = ?2, \
                 updated_at = datetime('now') WHERE id = ?1",
                rusqlite::params![id, error],
            )?;
            Ok(())
        })
    }

    /// Post-action status write (§4.5 pause/resume/stop): stop returns the
    /// pipeline to PUBLISHED and clears the assignment ("回已发布态可再部署").
    pub fn set_pipeline_status(&self, id: i64, status: &str, clear_assignment: bool) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                if clear_assignment {
                    "UPDATE pipeline SET status = ?2, assigned_node = NULL, \
                     updated_at = datetime('now') WHERE id = ?1"
                } else {
                    "UPDATE pipeline SET status = ?2, updated_at = datetime('now') WHERE id = ?1"
                },
                rusqlite::params![id, status],
            )?;
            Ok(())
        })
    }

    /// Record a release snapshot (§5.2 pipeline_version, unique per
    /// pipeline_id+version).
    pub fn insert_pipeline_version(
        &self,
        pipeline_id: i64,
        version: i64,
        spec_json: &str,
        sql_content: &str,
        published_by: &str,
    ) -> Result<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO pipeline_version (pipeline_id, version, spec_json, sql_content, published_by) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![pipeline_id, version, spec_json, sql_content, published_by],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    pub fn pipeline_version(&self, id: i64) -> Result<Option<PipelineVersion>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, pipeline_id, version, published_by FROM pipeline_version WHERE id = ?1",
            )?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(PipelineVersion {
                    id: row.get(0)?,
                    pipeline_id: row.get(1)?,
                    version: row.get(2)?,
                    published_by: row.get(3)?,
                })),
                None => Ok(None),
            }
        })
    }

    /// 5-minute trend buckets from `pipeline_metrics` (§4.5
    /// metrics/trend): monotonic totals are MAX per bucket. Rates are
    /// derived by the caller between consecutive buckets.
    pub fn pipeline_metrics_trend(
        &self,
        pipeline_id: i64,
        hours: i64,
    ) -> Result<Vec<PipelineTrendPoint>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                // 5-minute buckets: floor the minute part (ts minus minute%5).
                "SELECT strftime('%Y-%m-%dT%H:%M', ts, '-' || (CAST(strftime('%M', ts) AS INTEGER) % 5) || ' minutes'), \
                 MAX(consumed), MAX(flushed) \
                 FROM pipeline_metrics WHERE pipeline_id = ?1 AND ts >= datetime('now', ?2) \
                 GROUP BY 1 ORDER BY 1",
            )?;
            let rows = stmt.query_map(
                rusqlite::params![pipeline_id, format!("-{hours} hours")],
                |r| {
                    Ok(PipelineTrendPoint {
                        ts: r.get(0)?,
                        consumed: r.get(1)?,
                        flushed: r.get(2)?,
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

    #[test]
    fn pipeline_and_version_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let pid = match store
            .create_pipeline("p1", "SELECT 1", None, 10000, None, None)
            .unwrap()
        {
            PipelineWrite::Created(id) => id,
            PipelineWrite::NameConflict => panic!(),
        };
        let p = store.pipeline(pid).unwrap().unwrap();
        assert_eq!(p.status, "DRAFT");
        assert_eq!(p.version, 0);
        assert_eq!(store.list_pipelines().unwrap().len(), 1);
        assert_eq!(
            store
                .create_pipeline("p1", "x", None, 10000, None, None)
                .unwrap(),
            PipelineWrite::NameConflict
        );

        // publish bumps version + snapshots
        let v = store
            .set_pipeline_published(pid, "spec", "SELECT 1", "admin")
            .unwrap();
        assert_eq!(v, 1);
        let full = store.pipeline_full(pid).unwrap().unwrap();
        assert_eq!(
            (full.base.version, full.base.status.as_str()),
            (1, "PUBLISHED")
        );
        assert_eq!(full.base.spec_json.as_deref(), Some("spec"));

        // update non-DRAFT -> PENDING_CHANGES
        store
            .update_pipeline(pid, "p1b", "SELECT 2", None, 5000, None, None)
            .unwrap();
        let p = store.pipeline(pid).unwrap().unwrap();
        assert_eq!(
            (p.name.as_str(), p.status.as_str()),
            ("p1b", "PENDING_CHANGES")
        );
        assert_eq!(p.checkpoint_ms, 5000);

        // deploy lifecycle statuses (node 7 must exist: assigned_node has an FK)
        store
            .upsert_self_registered("sn-trend-x1", None, None, "tok")
            .unwrap();
        let node_id = store.node_by_key("sn-trend-x1").unwrap().unwrap().id;
        store.set_pipeline_deployed(pid, node_id).unwrap();
        let full = store.pipeline_full(pid).unwrap().unwrap();
        assert_eq!(
            (
                full.base.status.as_str(),
                full.base.assigned_node,
                full.deploy_epoch
            ),
            ("RUNNING", Some(node_id), 1)
        );
        store.set_pipeline_status(pid, "PAUSED", false).unwrap();
        store.set_pipeline_status(pid, "PUBLISHED", true).unwrap();
        let p = store.pipeline(pid).unwrap().unwrap();
        assert_eq!((p.status.as_str(), p.assigned_node), ("PUBLISHED", None));
        store
            .set_pipeline_deploy_failed(pid, "engine rejected")
            .unwrap();
        assert_eq!(
            store
                .pipeline_full(pid)
                .unwrap()
                .unwrap()
                .deploy_error
                .as_deref(),
            Some("engine rejected")
        );

        // paged list
        let (items, total) = store.list_pipelines_paged(1, 10).unwrap();
        assert_eq!((items.len(), total), (1, 1));

        // duplicate (pipeline, version) rejected
        assert!(store
            .insert_pipeline_version(pid, 1, "s", "SELECT 1", "admin")
            .is_err());

        // version rows cascade with pipeline delete
        let vid = store
            .insert_pipeline_version(pid, 2, "s2", "SELECT 1", "admin")
            .unwrap();
        assert!(store.delete_pipeline(pid).unwrap());
        assert!(store.pipeline_version(vid).unwrap().is_none());
    }

    #[test]
    fn trend_buckets_and_rates_source() {
        let store = Store::open_in_memory().unwrap();
        // Relative-time fixture (audit H2 time bomb): anchor to the start of
        // a 5-minute bucket ~10 minutes in the past so bucket membership is
        // deterministic no matter when the test runs. 'now' appears exactly
        // once, so the anchor cannot straddle a boundary mid-computation.
        let base: String = store
            .with_conn(|c| {
                Ok(c.query_row(
                    "SELECT datetime(t, '-' || (CAST(strftime('%M', t) AS INTEGER) % 5) || ' minutes') \
                     FROM (SELECT datetime('now', '-10 minutes') AS t)",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        store
            .with_conn(|c| {
                for (offset, consumed) in [("+1 minutes", 100i64), ("+4 minutes", 300), ("+7 minutes", 700)] {
                    c.execute(
                        "INSERT INTO pipeline_metrics (pipeline_id, consumed, flushed, flushes, ts) \
                         VALUES (42, ?1, ?1, 1, datetime(?2, ?3))",
                        rusqlite::params![consumed, base, offset],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        let pts = store.pipeline_metrics_trend(42, 24).unwrap();
        assert_eq!(pts.len(), 2, "two 5-minute buckets: {pts:?}");
        assert_eq!(pts[0].consumed, 300, "MAX within the first bucket");
        assert_eq!(pts[1].consumed, 700);
    }
}
