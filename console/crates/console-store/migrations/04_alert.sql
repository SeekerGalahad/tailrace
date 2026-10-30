-- T-E2 alerting (R9 task card §一): rules the console-side evaluator checks
-- against its own aggregated node /metrics snapshot every tick, and the
-- alert episodes those rules produce.
--
-- alert_rule.state is the evaluator's authoritative current state for the
-- rule ('inactive' | 'firing'); the window history behind the two
-- increment rules is in-memory only (no sample store, blueprint "no history
-- p95" decision) and rebuilds after a restart.
CREATE TABLE alert_rule (
  id           INTEGER PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  rule_type    TEXT NOT NULL,                     -- lag_seconds|state_failed|heartbeat_age|dlq_window|flush_errors_window
  params_json  TEXT NOT NULL,                     -- {"threshold":N,"windowSecs":N,"pipeline":"…","statement":"…","node":"…"} (type-dependent subset)
  enabled      INTEGER NOT NULL DEFAULT 1,
  webhook_url  TEXT,                              -- NULL = SSE/API surface only
  state        TEXT NOT NULL DEFAULT 'inactive',  -- inactive|firing (evaluator-owned)
  created_at   TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- One row per alert EPISODE, written only on a state transition: inserted
-- when the rule goes inactive → firing, flipped to 'resolved' when it goes
-- back. Ticks that keep a rule firing only bump last_seen_at/sample_json on
-- the open row — never a new row (anti-flood).
CREATE TABLE alert_event (
  id                   INTEGER PRIMARY KEY,
  rule_id              INTEGER NOT NULL REFERENCES alert_rule(id) ON DELETE CASCADE,
  state                TEXT NOT NULL,             -- firing|resolved
  first_seen_at        TEXT NOT NULL DEFAULT (datetime('now')),
  last_seen_at         TEXT NOT NULL DEFAULT (datetime('now')),
  resolved_at          TEXT,                      -- set when state becomes resolved
  sample_json          TEXT NOT NULL,             -- observed values at the firing transition (updated while firing)
  resolved_sample_json TEXT                       -- observed values at the resolving transition
);
CREATE INDEX idx_alert_event_rule ON alert_event(rule_id, first_seen_at);
