//! Backfill-job repository (T-A2 §7.3, `backfill_job` table): job-id
//! allocation (date slug + per-day sequence), the pending → running →
//! done|failed state machine as compare-and-set updates (a terminal row is
//! immutable — a second claim or finish attempt simply reports "no row
//! moved"), and the per-pipeline listing the detail page polls.

use anyhow::Result;

use super::models::BackfillJob;
use super::Store;

const JOB_COLS: &str =
    "id, pipeline_id, statement_index, from_json, to_json, state, rows_written, error, created_at, finished_at";

fn job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BackfillJob> {
    Ok(BackfillJob {
        id: row.get(0)?,
        pipeline_id: row.get(1)?,
        statement_index: row.get(2)?,
        from_json: row.get(3)?,
        to_json: row.get(4)?,
        state: row.get(5)?,
        rows_written: row.get(6)?,
        error: row.get(7)?,
        created_at: row.get(8)?,
        finished_at: row.get(9)?,
    })
}

impl Store {
    /// Insert a pending job, allocating a `{yyyymmdd}-{seq}` id. The sequence
    /// counts today's rows + 1 and retries on a UNIQUE collision (another
    /// request slipped in between COUNT and INSERT) — SQLite is single-writer
    /// per database but the console holds one Mutex'd connection, so in
    /// practice the first attempt wins.
    pub fn create_backfill_job(
        &self,
        pipeline_id: i64,
        statement_index: i64,
        from_json: &str,
        to_json: &str,
    ) -> Result<String> {
        let today: String = self.with_conn(|conn| {
            let s: String = conn.query_row("SELECT strftime('%Y%m%d','now')", [], |r| r.get(0))?;
            Ok(s)
        })?;
        let mut seq: i64 = self.with_conn(|conn| {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM backfill_job WHERE id LIKE ?1 || '-%'",
                [&today],
                |r| r.get(0),
            )?;
            Ok(n)
        })? + 1;
        loop {
            let id = format!("{today}-{seq:02}");
            let inserted = self.with_conn(|conn| {
                Ok(conn
                    .execute(
                        "INSERT INTO backfill_job (id, pipeline_id, statement_index, from_json, to_json) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        rusqlite::params![id, pipeline_id, statement_index, from_json, to_json],
                    )
                    .is_ok())
            })?;
            if inserted {
                return Ok(id);
            }
            seq += 1;
        }
    }

    pub fn backfill_job(&self, id: &str) -> Result<Option<BackfillJob>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {JOB_COLS} FROM backfill_job WHERE id = ?1"
            ))?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(job_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    /// Jobs of one pipeline, newest first (the detail-page progress list).
    pub fn list_backfill_jobs(&self, pipeline_id: i64) -> Result<Vec<BackfillJob>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {JOB_COLS} FROM backfill_job WHERE pipeline_id = ?1 \
                 ORDER BY created_at DESC, id DESC"
            ))?;
            let rows = stmt.query_map([pipeline_id], job_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// pending → running (executor claim). Ok(false) = the row is not
    /// pending (already claimed, or already terminal).
    pub fn claim_backfill_job(&self, id: &str) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "UPDATE backfill_job SET state = 'running' WHERE id = ?1 AND state = 'pending'",
                [id],
            )? > 0)
        })
    }

    /// running → done|failed with the final row count / error text.
    /// Ok(false) = the row is not running (double finish, or a foreign
    /// claim) — the terminal state it already holds stands.
    pub fn finish_backfill_job(
        &self,
        id: &str,
        done: bool,
        rows_written: i64,
        error: Option<&str>,
    ) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "UPDATE backfill_job SET state = ?2, rows_written = ?3, error = ?4, \
                 finished_at = datetime('now') WHERE id = ?1 AND state = 'running'",
                rusqlite::params![
                    id,
                    if done { "done" } else { "failed" },
                    rows_written,
                    error
                ],
            )? > 0)
        })
    }

    /// Boot-time sweep: every `pending` / `running` row is an orphan when the
    /// console starts — its executor was a blocking thread of the previous
    /// process and died with it, and nothing re-attaches to a job across
    /// restarts. Left alone, such a row reads `running` in the jobs list
    /// forever. The rows move to `failed` with [`ORPHANED_JOB_ERROR`];
    /// terminal rows are untouched. Returns the number of rows swept.
    pub fn fail_orphaned_backfill_jobs(&self) -> Result<usize> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "UPDATE backfill_job SET state = 'failed', error = ?1, \
                 finished_at = datetime('now') WHERE state IN ('pending', 'running')",
                [ORPHANED_JOB_ERROR],
            )?)
        })
    }
}

/// Error text written by [`Store::fail_orphaned_backfill_jobs`]. Re-triggering
/// the same range is the remedy — deterministic labels make the replay
/// converge, so a partially written orphan does not double-count.
pub const ORPHANED_JOB_ERROR: &str = "console restarted while the job was pending/running; bounded replays do not resume across restarts — re-trigger the same range (deterministic labels keep the replay idempotent)";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PipelineWrite;

    fn pipeline(store: &Store) -> i64 {
        match store
            .create_pipeline("bf-jobs", "SELECT 1", None, 1000, None, None)
            .unwrap()
        {
            PipelineWrite::Created(id) => id,
            PipelineWrite::NameConflict => panic!(),
        }
    }

    /// Id allocation + the full state machine as CAS transitions, including
    /// the refusals: a terminal row is immutable, a second claim is a no-op.
    #[test]
    fn job_lifecycle_and_terminal_immutability() {
        let store = Store::open_in_memory().unwrap();
        let pid = pipeline(&store);
        let a = store
            .create_backfill_job(
                pid,
                0,
                r#"{"type":"offset","value":0}"#,
                r#"{"type":"offset","value":9}"#,
            )
            .unwrap();
        let b = store
            .create_backfill_job(
                pid,
                1,
                r#"{"type":"offset","value":9}"#,
                r#"{"type":"offset","value":99}"#,
            )
            .unwrap();
        // same-date ids share the day prefix and carry distinct sequences
        assert_eq!(a.len(), b.len());
        assert_ne!(a, b);
        assert!(
            a.starts_with("20") && a.contains('-'),
            "date-slug shape: {a}"
        );

        let job = store.backfill_job(&a).unwrap().unwrap();
        assert_eq!(
            (job.pipeline_id, job.statement_index, job.state.as_str()),
            (pid, 0, "pending")
        );
        assert_eq!(job.rows_written, 0);
        assert_eq!(job.finished_at, None);

        // pending → running → done (rows recorded, finished_at set)
        assert!(store.claim_backfill_job(&a).unwrap());
        assert!(store.finish_backfill_job(&a, true, 42, None).unwrap());
        let job = store.backfill_job(&a).unwrap().unwrap();
        assert_eq!(job.state, "done");
        assert_eq!(job.rows_written, 42);
        assert!(job.finished_at.is_some());

        // terminal rows refuse every further transition
        assert!(
            !store.claim_backfill_job(&a).unwrap(),
            "done row cannot be re-claimed"
        );
        assert!(
            !store
                .finish_backfill_job(&a, false, 0, Some("late"))
                .unwrap(),
            "done row cannot be re-finished"
        );
        assert_eq!(store.backfill_job(&a).unwrap().unwrap().state, "done");

        // failure path carries the error text verbatim
        assert!(store.claim_backfill_job(&b).unwrap());
        assert!(store
            .finish_backfill_job(&b, false, 0, Some("sink flush failed"))
            .unwrap());
        let job = store.backfill_job(&b).unwrap().unwrap();
        assert_eq!(
            (job.state.as_str(), job.error.as_deref()),
            ("failed", Some("sink flush failed"))
        );

        // unclaimed rows cannot be finished directly
        let c = store.create_backfill_job(pid, 0, "{}", "{}").unwrap();
        assert!(!store.finish_backfill_job(&c, true, 1, None).unwrap());
        assert_eq!(store.backfill_job(&c).unwrap().unwrap().state, "pending");

        // listing is scoped to the pipeline; cascade follows pipeline delete
        let (rows, _) = (store.list_backfill_jobs(pid).unwrap(), ());
        assert_eq!(rows.len(), 3);
        assert!(store.delete_pipeline(pid).unwrap());
        assert!(store.list_backfill_jobs(pid).unwrap().is_empty());
    }

    /// Boot-time orphan sweep: a `running` row (executor died with the
    /// previous process) and a `pending` row (never claimed) both become
    /// `failed` with the explanatory error; terminal rows are untouched.
    #[test]
    fn orphaned_jobs_are_failed_on_boot() {
        let store = Store::open_in_memory().unwrap();
        let pid = pipeline(&store);
        let running = store.create_backfill_job(pid, 0, "{}", "{}").unwrap();
        assert!(store.claim_backfill_job(&running).unwrap());
        let pending = store.create_backfill_job(pid, 0, "{}", "{}").unwrap();
        let done = store.create_backfill_job(pid, 0, "{}", "{}").unwrap();
        assert!(store.claim_backfill_job(&done).unwrap());
        assert!(store.finish_backfill_job(&done, true, 9, None).unwrap());
        let failed = store.create_backfill_job(pid, 0, "{}", "{}").unwrap();
        assert!(store.claim_backfill_job(&failed).unwrap());
        assert!(store
            .finish_backfill_job(&failed, false, 0, Some("sink refused"))
            .unwrap());

        assert_eq!(store.fail_orphaned_backfill_jobs().unwrap(), 2);
        for id in [&running, &pending] {
            let job = store.backfill_job(id).unwrap().unwrap();
            assert_eq!(job.state, "failed", "{id}");
            assert_eq!(job.error.as_deref(), Some(ORPHANED_JOB_ERROR));
            assert!(job.finished_at.is_some());
        }
        let job = store.backfill_job(&done).unwrap().unwrap();
        assert_eq!(
            (job.state.as_str(), job.rows_written, job.error),
            ("done", 9, None)
        );
        let job = store.backfill_job(&failed).unwrap().unwrap();
        assert_eq!(job.error.as_deref(), Some("sink refused"));
        // idempotent: a second sweep finds nothing
        assert_eq!(store.fail_orphaned_backfill_jobs().unwrap(), 0);
    }
}
