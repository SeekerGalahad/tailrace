-- R11 T-V4 SLO history: the lag sample ring behind the achieved side of the
-- SLO (`tailrace_slo_lag_p50/p95/p99_seconds` on the console /metrics,
-- `GET /api/slo`) — the v1.3 item the R9 "no history p95" decision
-- deferred (see 04_alert.sql). The alert evaluator writes one row per
-- (pipeline, statement) on every pass (15s default) from the same
-- aggregated node /metrics snapshot it evaluates — no second sampling path.
--
-- Retention is a 1h ring: every pass prunes the rows older than the
-- retention in the same transaction as its insert, and the console startup
-- sweep drops the tail a downtime left behind — the table stays bounded
-- (~240 rows per statement at the default interval).
--
-- Quantiles are computed from these rows verbatim (nearest-rank, no
-- interpolation), so every reported quantile is one of the stored samples
-- and can be recomputed from them. The SLO identity is (pipeline,
-- statement); `node` is provenance only — a pipeline migrating between
-- nodes keeps its history.
CREATE TABLE slo_lag_sample (
  id        INTEGER PRIMARY KEY,
  pipeline  TEXT NOT NULL,                        -- `pipeline` label value (console pipeline id)
  statement TEXT NOT NULL,                        -- `statement` label value (statement index)
  node      TEXT NOT NULL,                        -- reporting node name (provenance)
  lag_secs  REAL NOT NULL,                        -- tailrace_pipeline_lag_seconds at the pass
  ts_ms     INTEGER NOT NULL                      -- evaluator pass time, epoch ms
);
CREATE INDEX idx_slo_lag_sample_key ON slo_lag_sample(pipeline, statement, ts_ms);
CREATE INDEX idx_slo_lag_sample_ts ON slo_lag_sample(ts_ms);
