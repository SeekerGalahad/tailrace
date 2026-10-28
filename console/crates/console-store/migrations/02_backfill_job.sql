-- T-A2 backfill jobs: one row per bounded replay request (blueprint §7.3 /
-- R6 task card §三). The console executes backfills itself against a
-- bf-{jobId} ledger namespace (engine W2 run_bounded), so success/failure
-- never touches main-pipeline cursors — this table IS the job's progress
-- surface (pending → running → done|failed).
--
-- id is a date-slug TEXT PK ("20261028-01"): it names the ledger dir
-- (bf-{id}), the isolated consumer group (bf-{id}.s{n}) and appears in
-- audit detail; a plain INTEGER would leak row-order into labels.
CREATE TABLE backfill_job (
  id              TEXT PRIMARY KEY,                      -- "{yyyymmdd}-{seq}" slug
  pipeline_id     INTEGER NOT NULL REFERENCES pipeline(id) ON DELETE CASCADE,
  statement_index INTEGER NOT NULL,                      -- which statement of the pipeline to replay
  from_json       TEXT NOT NULL,                         -- the {"type":"offset","value":N} bound verbatim
  to_json         TEXT NOT NULL,
  state           TEXT NOT NULL DEFAULT 'pending',       -- pending|running|done|failed
  rows_written    INTEGER NOT NULL DEFAULT 0,
  error           TEXT,                                  -- terminal failure reason (state=failed)
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  finished_at     TEXT                                   -- set on done/failed (datetime('now'))
);
CREATE INDEX idx_backfill_job_pipe ON backfill_job(pipeline_id, created_at);
