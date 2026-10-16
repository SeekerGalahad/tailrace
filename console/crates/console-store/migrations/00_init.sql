-- 00_init.sql — verbatim from design doc §5.2 DDL (MySQL→SQLite dialect).
-- Users and sessions
CREATE TABLE user (
  id            INTEGER PRIMARY KEY,
  username      TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,                      -- argon2id PHC string
  display_name  TEXT,
  role          TEXT NOT NULL DEFAULT 'admin',      -- v1 single role
  disabled      INTEGER NOT NULL DEFAULT 0,
  created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE session (
  token_hash TEXT PRIMARY KEY,                      -- sha256(plaintext token), plaintext never stored
  user_id    INTEGER NOT NULL REFERENCES user(id) ON DELETE CASCADE,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Datasources (credential split: list/detail never return passwords)
CREATE TABLE datasource (
  id         INTEGER PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  type       TEXT NOT NULL,                         -- kafka|file|doris|starrocks|mysql|postgres|mqtt(P2)
  conn_json  TEXT NOT NULL,                         -- non-sensitive params only
  status     TEXT NOT NULL DEFAULT 'CREATED',      -- CREATED|CONN_OK|CONN_FAIL
  generation INTEGER NOT NULL DEFAULT 1,           -- CAS optimistic lock
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE datasource_credential (
  datasource_id INTEGER PRIMARY KEY REFERENCES datasource(id) ON DELETE CASCADE,
  nonce         TEXT NOT NULL,                      -- 12B base64
  ciphertext    TEXT NOT NULL,                      -- AES-256-GCM({"username":...,"password":...})
  updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE table_object (                          -- external table registry (pipeline compile base)
  id               INTEGER PRIMARY KEY,
  datasource_id    INTEGER NOT NULL REFERENCES datasource(id) ON DELETE CASCADE,
  db_name          TEXT NOT NULL,
  table_name       TEXT NOT NULL,
  kind             TEXT NOT NULL,                   -- kafka|file|doris|starrocks
  format           TEXT,                            -- ogg|json|jsonl|csv|parquet
  columns_json     TEXT,                            -- [["name","arrow type"],...] (required for doris sink)
  cursor_semantics TEXT NOT NULL DEFAULT 'NONE',   -- KAFKA_OFFSET|FILE_POINTER|NONE
  write_policy_json TEXT,                           -- key/partition/label prefix write policy
  source           TEXT NOT NULL DEFAULT 'manual', -- manual|discovered
  created_at       TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(datasource_id, db_name, table_name)
);

-- Engine nodes (identity triple: id row id / node_key stable identity / name human readable)
CREATE TABLE node (
  id             INTEGER PRIMARY KEY,
  node_key       TEXT NOT NULL UNIQUE,              -- sn-<16hex>, assigned authoritatively by control plane
  name           TEXT NOT NULL UNIQUE,
  api_url        TEXT,                              -- heartbeat may correct drift
  status         TEXT NOT NULL DEFAULT 'REGISTERED',-- REGISTERED|ONLINE|OFFLINE (90s offline judge)
  host           TEXT, ssh_port INTEGER, ssh_user TEXT,
  engine_port    INTEGER NOT NULL DEFAULT 18100,
  install_dir    TEXT NOT NULL DEFAULT '/opt/tailrace/engine',
  artifact_id    INTEGER REFERENCES artifact(id),
  engine_version TEXT,
  deploy_status  TEXT,                              -- eight-step state machine terminal: REGISTERED|FAILED|UNINSTALLED
  register_mode  TEXT NOT NULL,                     -- manual|deployed|self
  capacity_json  TEXT,
  bootstrap_token TEXT,                             -- revoked = set NULL
  last_heartbeat TEXT,
  created_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE artifact (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  version     TEXT NOT NULL,
  filename    TEXT NOT NULL,
  sha256      TEXT NOT NULL,                        -- deploy verification anchor
  size_bytes  INTEGER NOT NULL,
  uploaded_by TEXT NOT NULL,
  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(name, version)
);
CREATE TABLE deploy_log (
  id       INTEGER PRIMARY KEY,
  node_id  INTEGER NOT NULL REFERENCES node(id) ON DELETE CASCADE,
  action   TEXT NOT NULL,                           -- DEPLOY|START|STOP|RESTART|UPGRADE|ROLLBACK|UNINSTALL
  step     TEXT NOT NULL,                           -- PREFLIGHT|UPLOAD|UNPACK|CONFIGURE|START|HEALTH_WAIT|REGISTER|...
  status   TEXT NOT NULL,                           -- OK|FAIL
  output   TEXT,                                    -- truncated to 4KB
  operator TEXT,
  ts       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_deploy_log_node ON deploy_log(node_id, ts);
CREATE TABLE node_event (
  id          INTEGER PRIMARY KEY,
  node_key    TEXT NOT NULL,
  pipeline_id INTEGER,
  level       TEXT NOT NULL,                        -- INFO|WARN|ERROR
  kind        TEXT NOT NULL,                        -- NODE_START|PIPELINE_STARTED|SINK_WRITE_FAILED|...
  message     TEXT NOT NULL,
  event_ts    TEXT NOT NULL
);
CREATE INDEX idx_node_event_node ON node_event(node_key, event_ts);

-- Pipelines and release versions
CREATE TABLE pipeline (
  id              INTEGER PRIMARY KEY,
  name            TEXT NOT NULL UNIQUE,
  sql_content     TEXT NOT NULL,                    -- canonical SQL source (never carries credentials)
  spec_json       TEXT,                             -- publish compile output (${ds:id.db.table} materialized)
  ds_mapping_json TEXT,                             -- dbName->datasourceId (one db one source)
  status          TEXT NOT NULL DEFAULT 'DRAFT',   -- DRAFT|PUBLISHED|RUNNING|PAUSED|FAILED|DEPLOY_FAILED|PENDING_CHANGES
  assigned_node   INTEGER REFERENCES node(id),
  preferred_node  INTEGER REFERENCES node(id),
  version         INTEGER NOT NULL DEFAULT 0,
  checkpoint_ms   INTEGER NOT NULL DEFAULT 10000,
  config_json     TEXT,                             -- advanced config override (options passthrough)
  deploy_error    TEXT,
  deploy_epoch    INTEGER NOT NULL DEFAULT 0,      -- successful deploy generation (heartbeat write-back guards stale node overwrite)
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE pipeline_version (                      -- release version (traceable snapshot/rollback anchor)
  id           INTEGER PRIMARY KEY,
  pipeline_id  INTEGER NOT NULL REFERENCES pipeline(id) ON DELETE CASCADE,
  version      INTEGER NOT NULL,
  spec_json    TEXT NOT NULL,
  sql_content  TEXT NOT NULL,
  published_by TEXT NOT NULL,
  published_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(pipeline_id, version)
);

-- Observation time series (piggybacked from heartbeat, kept 7 days, purged at startup)
CREATE TABLE pipeline_metrics (
  pipeline_id INTEGER NOT NULL, node_id INTEGER, status TEXT,
  consumed INTEGER NOT NULL, flushed INTEGER NOT NULL, flushes INTEGER NOT NULL,
  rate_in_rps REAL, rate_out_rps REAL, ts TEXT NOT NULL
);
CREATE INDEX idx_pm_pipe ON pipeline_metrics(pipeline_id, ts);
CREATE TABLE node_metrics (
  node_id INTEGER NOT NULL,
  rss_bytes INTEGER, mem_total_bytes INTEGER, cpu_pct REAL, rx_bps REAL, tx_bps REAL,
  ts TEXT NOT NULL
);
CREATE INDEX idx_nm_node ON node_metrics(node_id, ts);

-- Audit
CREATE TABLE audit_log (
  id          INTEGER PRIMARY KEY,
  actor       TEXT NOT NULL,                        -- session username / "node:sn-..." / "bootstrap"
  action      TEXT NOT NULL,                        -- LOGIN|DATASOURCE_CREATE|PIPELINE_PUBLISH|NODE_DEPLOY|...
  entity_type TEXT NOT NULL, entity_id TEXT,
  detail_json TEXT,
  ts          TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_audit_entity ON audit_log(entity_type, entity_id, ts);
