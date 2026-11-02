# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-10-31

_Scope: changes since v1.1.0 — six development rounds on the main line:
the extension round (`feature/mcp-dim-join`, merge `a558231`: T-A6 /
Debezium decoder / T-E4 / T-D1), R6 (`feature/ddl-backfill`, merge
`a364b8e`: T-A1 / T-A2), R7 (`feature/sink-capabilities`, merge `aead2b5`:
T-G4 / T-A7 / T-A8), R8 (`feature/type-foundation`, merge `86c5289`:
T-G2 / T-G1 / T-A3 plus three carried items), R8.5 (`feature/docs-v1.2`,
merge `003f27e`: documentation alignment, zero code changes) and the
release line itself (`feature/v1.2-release`: R9 T-A4 / T-A5 / T-E2, R10
T-G3 / T-G6 plus the dogfooding tail-debt bundle). This entry cuts the
tree at 1.2.0 in lockstep — one workspace version inherited by all 23
members, 22 of them publishable; the v1.2.0 tag, the binary release and
the crates.io publish follow it (R5 precedent — version first, publish
second). e2e counts quoted below are the figures
recorded when each round landed (`scripts/e2e/`, against Doris 2.1.7 in
compose and an external 3.1.4); the R9 suites were re-run on the release
line on 2026-10-31 after the review-round semantics change
(`variant_vector.sh` 46/46, `alert_flow.sh` 38/38 → 50/50 with the
badRowPolicy section, dogfooding round 3 27/27 — artifacts under
`scripts/e2e/evidence/*-r10recert/`). Anchors are `path:line` in this
tree. **Heads-up for 1.3.0**: the unknown-column-type migration window
closes — see Deprecated below._

### Added

- Prometheus `/metrics` on both binaries (T-A6, extension round): a 9-metric
  surface in hand-written text format with zero exporter crates
  (`engine/crates/tailrace-api/src/metrics.rs:9` lists it). The node renders
  the page from the live pipeline registry at scrape time — no sampling
  thread, no global state (`:235` `node_metrics_text`; open route, no token,
  Prometheus convention); the console serves its own
  `tailrace_console_db_write_seconds{op}` (SQLite write latency) and proxies
  every registered node's `/metrics` behind it, relabelled `node="<name>"`
  with a 3 s timeout and an unreachable node degraded to a comment line
  (`console/crates/tailrace-console/src/metrics.rs:155` `scrape`).
  `tailrace_pipeline_bytes_total` is declared but not yet fed (sinks report
  in-flight `buffered_bytes` only — ack-side byte accounting is a follow-up).
- Debezium CDC decoding (extension round, the narrow path of the old
  "S3/CDC" item): a table object with `format = "debezium-json"` consumes
  Debezium events already on Kafka — `c` / `r` / `u` become rows from the
  `after` payload, `d` events and tombstones are skipped, the
  `schemas.enable` `{schema,payload}` wrapper is unwrapped, and an event
  without an `after` object goes to the DLQ as a Bad row rather than
  vanishing (`engine/crates/tailrace-decode/src/lib.rs:1204`
  `DebeziumJsonDecoder`; runtime dispatch
  `engine/crates/tailrace-runtime/src/lib.rs:1550`; the metadata format
  whitelist and kafka `infer-schema` accept the format). tailrace consumes
  CDC, it does not capture it — reading binlogs / WAL stays out of scope.
- MCP server (T-E4, extension round; widened in R8): `POST /mcp` on the
  console speaks JSON-RPC 2.0 in the Streamable HTTP request/response shape
  (one message per POST, batches rejected, `Mcp-Session-Id` issued on
  `initialize`, protocol `2025-06-18` with the two older versions echoed).
  14 tools — 8 read, 6 write — each a thin in-process adapter over the REST
  handler it maps to (`console/crates/tailrace-console/src/mcp.rs:90`
  `TOOLS`): list / inspect / compile / stats / logs / `ddl_draft` /
  `backfill_status` always on; `create_pipeline` / `deploy_pipeline` /
  `pause_resume_pipeline` / `trigger_backfill` / `ddl_execute` /
  `ddl_evolve` need **both** the process switch (`--mcp-allow-write` or
  `MCP_ALLOW_WRITE=1`) and `"confirm": true` in the call, else
  `WRITE_DISABLED`. Every call — success, failure, rejected, even an unknown
  tool name — lands in `audit_log` as `MCP_TOOL_CALL` with
  `actor = mcp:<client>`; `trigger_backfill` defaults to `dryRun: true`.
  Tool descriptions are generated from the OpenAPI document that serves
  `/swagger-ui` (`:748` `openapi_tool_description`), so there is no second
  hand-written copy to drift. The endpoint is session-free and rate-limited
  at a fixed 100 rps.
- Broadcast dimension join (T-D1, extension round): `… JOIN
  ${ds:dim.<alias>} d ON …` loads a `kind = dim` JDBC table object
  full-table at pipeline assembly into a static in-memory table that lives
  as long as the plan — no watermark, no state backend, no refresh while the
  pipeline runs (a restart reloads). Guardrails fail fast at compile rather
  than OOM: 500 000-row / 64 MiB caps
  (`engine/crates/tailrace-runtime/src/dim.rs:36` / `:39`, plan tuning
  `dimMaxRows` / `dimMaxBytes`), the dim table must declare a primary /
  single-column-unique `sequence_column` (the keyset uniqueness probe
  refuses otherwise), and only `INNER` / `LEFT [OUTER] JOIN` are accepted
  (`RIGHT` / `FULL` / `CROSS` rejected,
  `engine/crates/tailrace-parser/src/lib.rs:351-359`). A dim reference in
  the FROM position is rejected; statements with a dim join cannot be
  backfilled yet. This is a broadcast join — the "no streaming join" red
  line stands.
- DDL closed loop (T-A1, R6): the new `tailrace-ddl` crate is a pure
  CREATE TABLE draft generator — schema + hints in, DDL text + "this is a
  guess" annotations out, no I/O, snapshot-tested
  (`engine/crates/tailrace-ddl/src/lib.rs:180` `generate_draft`). Three
  console endpoints under
  `/api/datasources/{id}/tables/{db}/{table}/ddl/`: `draft` (key model
  recommended from CDC evidence, key column guessed from `id` / `*_id`,
  partition candidates offered **only in forms verified buildable on a real
  Doris** — the review round found VARCHAR partition columns and UNIQUE
  non-key partition columns are refused by the FE), `execute` (422
  `KEY_MODEL_REQUIRED` / `DDL_REQUIRED` / `KEY_MODEL_MISMATCH` before any
  connection — the human-confirmed key model is mandatory, there is no
  "auto-create with DUPLICATE KEY" path; the confirmed text runs verbatim
  through `tailrace-metadata::mysql_exec` and is audited in full as
  `TABLE_DDL_EXECUTE`, pass or fail) and `evolve` (registered-vs-live
  `DESC` diff, **`ADD COLUMN` only** — a retype or drop is reported, never
  run). 13 scalar vocabulary words map to Doris types (15 once T-A4 /
  T-A5 below land `variant` and `list<float32>`; `null` is the one word
  left unmapped); an unmapped word is an error, not a fold (`:438`
  `doris_type`). A three-step web wizard
  (`web/src/pages/datasources/parts/TableDdlWizard.vue`) fronts it. e2e
  `ddl_create.sh` 48/48 including the partition path and "ADD COLUMN, then
  restart, pipeline keeps running".
- Deterministic backfill (T-A2, R6): `POST /api/pipelines/{id}/backfill`
  replays a bounded range through the pipeline's published statement.
  `dryRun: true` previews `jobId` / `labelPrefix` with zero side effects;
  otherwise a `backfill_job` row (new console migration) is created and the
  console runs `PipelineRunner::run_bounded(from, to)` in-process
  (`engine/crates/tailrace-runtime/src/lib.rs:773`) against a `bf-<job>`
  ledger namespace (`:1584` `backfill_state_dir`) and `bf-` consumer
  groups — a backfill never reads or advances the resident pipeline's
  cursor. The Doris sink gains `LabelMode::Deterministic`
  (`engine/crates/tailrace-conn-doris/src/lib.rs:187`): labels are
  `<prefix>-<statement>-<sha256(from,to)[..8]>_<batch>`, so the same range
  replayed from any process reuses the same labels and Doris answers
  `label already exists` → `Dedup`. The idempotency window is Doris' label
  retention period (default 3 days); older replays fall back to the
  table's key model. Jobs left `pending` / `running` by a dead console are
  marked `failed` at the next start. Source gate today: `file` only, with
  byte-offset bounds (Kafka per-partition seeding is a follow-up); sinks:
  doris / starrocks; statements with a dim join are refused. A web panel on
  the pipeline monitor page
  (`web/src/pages/pipelines/parts/BackfillPanel.vue`, 523 lines, mounted at
  `web/src/pages/pipelines/monitor.vue:548`; landed in the same R6 web commit
  `ad0ed3d` as the DDL wizard) fronts the flow — range → dry-run preview →
  confirm — and lists the jobs. e2e
  `backfill_replay.sh` 31/31 — the core assertion is "same range replayed
  twice on a DUPLICATE KEY table, row count unchanged".
- Partial-column update (T-A7, R7): the switch is the SQL itself — `INSERT
  INTO ${ds:sink} (k, a) SELECT …` declares the subset, a bare INSERT writes
  whole rows (`engine/crates/tailrace-parser/src/lib.rs:281`
  `insert_column_list`). Four gates, each fail-fast: at publish the sink
  must be `kind = doris` with a registered `key_model = unique` and every
  listed column known; at every plan push the **live** table is checked
  with `SHOW CREATE TABLE` and must be UNIQUE KEY merge-on-write (an
  unreachable FE is 502, not a guess — carried item, R8); a table with an
  in-flight `ADD COLUMN` job refuses the deploy with 409
  `SCHEMA_CHANGE_IN_PROGRESS`, and the DDL `evolve` endpoint refuses in the
  other direction with 409 `PARTIAL_UPDATE_ACTIVE`; at assembly the sink
  must declare `partial_update` and the `sequence_column` must be inside
  the subset (`engine/crates/tailrace-conn-doris/src/lib.rs:589`
  `with_partial_columns`). Stream Load sends `partial_columns:true` **and**
  a `columns` header naming the subset — live-probed on 2.1.7: without the
  header Doris resolves the write to every table column and silently NULLs
  the unlisted ones with `Status: Success`. StarRocks sinks reject the
  mode. e2e `partial_update.sh` 38/38 (compose) + 37/37 (external 3.1.4):
  two columns written, the other three keep their old values row by row,
  row count unchanged.
- Table-object registration API (T-D1 extension round + T-A7 R7): `POST
  /api/datasources/{id}/tables` takes two new body fields — `sequenceColumn`
  (dim pagination key / jdbc keyset cursor / Doris UNIQUE KEY dedup column;
  required for `kind = dim`) and `keyModel` (`unique` / `duplicate`, doris
  and starrocks datasources only) — and `GET /api/datasources/tables` returns
  both per row (`console/crates/tailrace-console/src/api/datasources.rs:300`
  / `:307`, row DTO `:350` / `:353`). A new `PUT
  /api/datasources/tables/{id}` records the key model of a table created
  outside the console (`:319` `TableKeyModelBody`, audited as
  `TABLE_KEY_MODEL` `:1547`; route
  `console/crates/tailrace-console/src/lib.rs:57-60` — in v1.1.0 that path
  was DELETE-only), while the DDL `execute` step records it automatically.
  The plan push now fills `sequence_column` from the registration (extension
  review `c8defa5`; v1.1.0 hard-coded `"sequence_column": null` into every
  plan table), which is what made the Doris `sequence_column` Stream Load
  header, the jdbc cursor column and the dim pagination key reachable from
  the console at all.
- 16-word column type vocabulary (T-G2, R8): the type strings in
  `TableObject.columns` are now a parsed wire contract
  (`engine/crates/tailrace-ir/src/column_type.rs:30` `ColumnType`,
  re-exported as `tailrace_catalog::ColumnType`): the four legacy words
  `utf8` / `int64` / `float64` / `bool` keep their meaning byte for byte,
  plus `string` / `int8` / `int16` / `int32` / `float32` /
  `decimal(p,s)` / `timestamp(s|ms|us|ns)` / `date` / `binary` /
  `list<float32[,dim]>` / `variant` / `null`. Parametric words are parsed
  structurally and `Display` prints the canonical spelling; `parse` is
  strict (`:94`). The type surface is wired through (T-A3): the DDL
  generator maps the 13 scalar words, `infer-schema` narrows DDL column
  types to the same vocabulary instead of folding `decimal(18,2)` to
  `int64` / `utf8` (Doris `DESC` reports a `STRING` column as `text`, so
  `text` → `string` — live-probed), and the web wizard's type dropdown
  reads from it. `variant` and `list<float32>` are in the vocabulary; their
  Doris DDL mapping and end-to-end write landed in R9 (T-A4 / T-A5 below).
  e2e `type_precision.sh`: a `decimal(18,2)` column survives file → Doris
  with its scale intact.
- `sequence_col` DDL property (R8 carried item, from dogfooding round 1):
  under UNIQUE KEY a declared `sequence_column` is emitted as the
  table-level property `"function_column.sequence_col" = "<col>"`
  (`engine/crates/tailrace-ddl/src/lib.rs:367-374`). Without it Doris
  accepts the engine's `sequence_column` Stream Load header and silently
  ignores it — a low-sequence write overwrites a high-sequence row with
  `Status: Success` (probed on 2.1.7 and 3.1.4); `type_precision.sh` keeps
  the property-less table as the failure-shape control.
- Sink transaction hooks and capability declaration (T-G4, R7): the `Sink`
  trait gains `prepare` / `commit` / `abort` with no-op defaults — `flush`
  stays the only commit point and every existing sink is unchanged — plus
  `capabilities() -> SinkCapabilities { idempotent_label, transactional,
  overwrite, partial_update, partition_aware }`
  (`engine/crates/tailrace-connector/src/sink.rs:18`, hooks `:107-121`,
  `:126`). The flag is load-bearing, not decorative: the runtime refuses
  `partial_columns` on a sink whose `partial_update` is false
  (`engine/crates/tailrace-runtime/src/lib.rs:537-547`), because file /
  kafka would otherwise write the subset as a whole row. Current values:
  doris `idempotent_label = true` (`partial_update` follows the instance),
  everything else false; no production sink claims `transactional` yet —
  the exactly-once and group-commit tiers remain undelivered, this is the
  extension point they will land on. Pinned by `tests/contract`
  `sink_capabilities` (5 tests).
- VARIANT column passthrough (T-A4, R9): the `variant` vocabulary word now
  reaches Doris end to end. The DDL generator maps it to `VARIANT`
  (`engine/crates/tailrace-ddl/src/lib.rs:451`; a variant column never
  takes the key — the heuristic steps over it and an explicit pick is an
  error), the decoders pass a nested JSON value in a variant column
  through as its serialised text instead of parsing leaves (json / ogg /
  debezium-json walks; the column is marked per field with
  `VARIANT_FIELD_METADATA`, `engine/crates/tailrace-ir/src/column_type.rs:208`,
  because variant and utf8 share arrow `Utf8` — both schema-construction
  sites go through `arrow_field_or_utf8` `:221` so the micro-batch schema
  equality holds), `infer-schema` reports nested JSON samples as `variant`
  and folds Doris `DESC variant` back to the word so `evolve` stays quiet.
  The probe that made the sink a zero-change path
  (`scripts/e2e/evidence/variant-probe-2026-10-30/`, 2.1.7 and 3.1.4):
  Doris parses a JSON *string literal* into the variant structure
  server-side exactly like a bare object, so the sink's existing Utf8
  escaping is already the right wire shape — the R9 e2e round captured the
  Stream Load body through a TCP proxy to show the raw JSON on the wire.
  Version difference recorded: 3.1.4 merges a dotted key `"a.b"` into the
  `a.b` sub-column path, 2.1.7 keeps them apart — the e2e asserts via
  sub-columns only. e2e `variant.sh` 41/41 (3.1.4) + 42/42 (2.1.7);
  `variant_vector.sh` 46/46.
- Vector column (T-A5, R9): `list<float32[,dim]>` lands as `ARRAY<FLOAT>`
  (`engine/crates/tailrace-ddl/src/lib.rs:482`), a declared dimension
  riding as `COMMENT 'vector dim=N'` (`:495`) — Doris has no array
  dimension of its own, and `DESC` spells the column back as the bare
  `list<float32>` (the dim is not recoverable from DESC, so the DDL
  `evolve` diff treats the dimmed and bare spellings as one column — R9
  review fix, `console/crates/tailrace-console/src/api/datasources.rs:1753`
  `diff_columns`). The decoders build a `List(Float32)` column and
  pre-validate every row before any append (`check_vector_cells`,
  `engine/crates/tailrace-decode/src/lib.rs:899`): a length that misses
  the declared dim, a non-numeric / null / nested element or a non-array
  cell makes the whole row Bad with every builder untouched — the probe
  (`scripts/e2e/evidence/vector-probe-2026-10-30/`) found Doris silently
  degrades such vectors (`["a",2,3]` → `[null,2,3]`, `42` → `NULL`,
  `NumberFilteredRows 0`), so the engine refuses instead of NULLing. The
  dim is capped at 65 536 (`MAX_VECTOR_DIM`,
  `engine/crates/tailrace-ir/src/column_type.rs:259`), elements must be
  finite, integers widen to f32, null / absent land NULL; a list column
  never takes the key or a partition (the FE refuses both); CSV degrades a
  vector cell to NULL (v1). HNSW / IVF indexing is the user's table DDL,
  not tailrace's. e2e `variant_vector.sh` 46/46 on 3.1.4 (vector write +
  read-back, dimmed-vector evolve 200 `changed=false`).
- Console alerting, SLO exposure and ops bundles (T-E2, R9; reworked in
  the R9 review round): the rules engine lives in the console — the node
  has none. A periodic task (15 s default, `TAILRACE_ALERT_INTERVAL_SECS`)
  evaluates the same aggregated node `/metrics` snapshot the scrape serves
  against five rule kinds, `lag_seconds` / `state_failed` /
  `heartbeat_age` / `dlq_window` / `flush_errors_window`
  (`console/crates/tailrace-console/src/alerting.rs:261` `RuleKind`).
  Episodes are kept **per (rule, series)** — two pipelines under one rule
  are two independent episodes (`alert_event.series_key`, migration `05`)
  — and every series gets a three-state verdict, Firing / Clear /
  **Unknown**, where Unknown HOLDS the current state instead of resolving
  it: a window rule with no usable base sample (a restart, or a gap wider
  than `BASE_SLACK_SECS` = 60 s, `:88`) and every series of an
  unreachable node are Unknown, so a node going dark can never turn a
  firing alert into a false "resolved"; `heartbeat_age` is synthesised
  from the console's own receive clock, so a dead node does fire it.
  Thresholds are strict `>` and accept 0 (a single DLQ row can fire);
  window increments credit a counter reset as the post-reset value. An
  episode row lands **only on a transition** — inactive→firing inserts,
  firing→inactive resolves the same row — and resolved is always
  reachable: condition clears, series disappears
  (`reason=series_absent`), rule disabled (`reason=disabled`), rule
  deleted (`reason=deleted` is delivered before the row goes). Surface:
  `GET /api/alerts` (rules + current state + open episode + the 50 most
  recent), audited `POST` / `PUT` / `DELETE /api/alerts/rules`, SSE
  `GET /api/alerts/stream`; delivery is a webhook POST (JSON; a failure
  is logged, not retried, and runs off the evaluation loop) plus the SSE
  hub. SLO v1 stays deliberately small: every enabled lag rule's threshold
  is exposed on `/metrics` as `tailrace_slo_lag_target_seconds` under the
  same label set as the lag series it watches
  (`console/crates/tailrace-console/src/metrics.rs:190`) — no history, no
  p95 (there is no sample store; a v1.3 candidate). Companion files for
  users who do run a Prometheus stack: `docs/ops/grafana-dashboard.json`
  (nine panels over the `/metrics` surface plus the SLO overlay) and
  `docs/ops/prometheus-alerts.yaml` (seven equivalent rules). Contract
  suite `console/crates/tailrace-console/tests/alerts_flow.rs`; e2e
  `alert_flow.sh` 38/38 (real lag, real FAILED, the webhook receives
  exactly the transition events) and 50/50 with the R10 badRowPolicy
  section.
- Cursor openness (T-G3, R10): `Cursor` gains a `Custom { kind, payload }`
  variant (`engine/crates/tailrace-ir/src/cursor_sourcechunk_impl.rs:64`)
  — `kind` names the semantics (`pg-lsn`, `iceberg-snapshot`, …) so a
  connector can refuse a cursor it did not write, `payload` is opaque JSON
  the connector needs to resume. The four existing variants keep their
  externally-tagged encoding byte for byte, so pre-G3 ledgers load without
  migration — read old, write new
  (`engine/crates/tailrace-state/src/lib.rs:41`; both accepted legacy
  ledger shapes are pinned as fixtures under
  `engine/crates/tailrace-state/tests/fixtures/`) — and an unknown tag
  still fails loudly: openness goes through `Custom`, never through silent
  acceptance. No shipped connector writes a `Custom` cursor yet; this is
  the door for a WAL / LSN source or an Iceberg snapshot cursor, not
  either of them.
- Pipeline as code (T-G6, R10): one declarative JSON file — `datasources`
  / `tables` / `pipelines`, any subset — goes through one server-side
  pair. `POST /api/apply/dry-run` computes a structured, field-level diff
  against the store (read-only, unaudited); `POST /api/apply` performs the
  idempotent upsert; both share `plan`
  (`console/crates/tailrace-console/src/api/apply.rs:347`), so the diff a
  human reviews is the diff the machine applies, and GitOps / CI / the CLI
  hit the same entry (routes `console/crates/tailrace-console/src/lib.rs:136-137`).
  Identity is the business key (datasource name / `(datasource, db,
  table)` / pipeline name), never a numeric id, so a file written against
  one console applies unchanged to another; entry bodies **are** the REST
  create / update DTOs (serde same-source), so the file schema cannot
  drift from the endpoints. Semantics, stated plainly: an absent top-level
  key makes no statement about that kind; a present key — even an empty
  array — declares the full set, and console objects the file does not
  name are reported as **orphans, never deleted** (v1 has no prune,
  `:832`). Apply touches nothing that already matches (no generation bump,
  no `PENDING_CHANGES` flip, no audit row; a second apply of the same file
  is a zero-change no-op); whole-file validation precedes any write;
  execution order is datasources → tables → pipelines, non-transactional
  across objects, re-run converges. Credentials are compared by presence
  only and never echoed (`<redacted>` both sides). JSON only in v1 —
  `serde_yaml` is unmaintained and was not introduced; a YAML file
  converts with one `yq` line, shown in `docs/ops/pipeline-file.md` with
  the field table and an example. CLI thin shell: `tailrace-console apply
  -f <file> [--dry-run] [--server URL] [--token | --user --password]`
  (`console/crates/tailrace-console/src/apply_cli.rs`; exit 0 / 1 / 2,
  zero diff logic client-side). Contract suite
  `console/crates/tailrace-console/tests/apply_flow.rs`, 6 cases,
  including the real CLI against a served router agreeing with the API
  byte for byte.

### Changed

- **INSERT column list semantics (T-A7, R7) — the one behavioural reversal
  in this scope.** Before, a column list on the INSERT was parsed and
  silently ignored (the sink always wrote whole rows); now it is the
  partial-column-update declaration described above. A bare INSERT is
  unchanged. Because the publish gate only runs at publish time, snapshots
  published before this change are re-gated at every later plan push
  (`deploy` / `recover`, 400 with the diagnostic) and at backfill accept
  (422 `PARTIAL_DECLARATION_INVALID`) — a DUPLICATE / merge-on-read sink
  with a stale column list fails loudly instead of slipping into partial
  mode (`console/crates/tailrace-console/src/api/pipelines.rs:369`
  `partial_declarations_gate` for publish and plan push — call sites `:1096`
  / `:1301`; the backfill accept path calls the same diagnostics generator
  `partial_declaration_diags` (`:299`) directly at `:2276`). A repository
  grep found no in-tree usage of the old form; the impact is user-side
  only.
- `TableObject` is now five generic fields plus an options map (T-G1, R8):
  `datasource` / `name` / `kind` / `format` / `columns` stay typed, every
  connector-specific setting moves into `options`
  (`engine/crates/tailrace-catalog/src/lib.rs:81`, `:108`), with the
  canonical key spellings in one place (`:36` `option_keys`: `bootstrap` /
  `stream_load_url` / `auth_env` / `label_prefix` / `sequence_column` /
  `partial_columns`) and typed accessors per connector (`KafkaTableExt` /
  `DorisTableExt` / `JdbcTableExt` / `FileTableExt`). Wire compatibility:
  the six legacy flat keys are **double-written** on serialise and folded
  into `options` on read for one minor-version window (`:55-62` `LEGACY`,
  `:258-263` `fold_legacy`; explicit `options` keys win), so stored
  `table_object` rows and plan bodies keep working; a cross-window test
  (legacy field names + new vocabulary words in one object) pins the two
  windows against each other. Construction sites collapsed from 17 struct
  literals to `TableObject::new` + `with_*` builders.
- Unknown column-type words no longer fold to `utf8` silently (T-G2, R8).
  During a one-minor migration window
  (`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW = true`,
  `engine/crates/tailrace-ir/src/column_type.rs:168`) the two legacy
  `arrow_type` call sites — pipeline assembly and dim loading — log a
  `warn` naming the column and the known vocabulary, then fold exactly as
  before; **1.3.0 flips the constant and turns the fold into a hard error
  at pipeline assembly** (see Deprecated below). Review that any `columns`
  type string you rely on is one of the sixteen words before then. The
  DDL generator and `ColumnType::parse` are already strict.
- Engineering governance (R6–R8): `unsafe_code = "forbid"` for the whole
  workspace via `[workspace.lints]` (`Cargo.toml:55-56`; a grep found zero
  `unsafe` tokens, the fence is for the future); `debug =
  "line-tables-only"` for `[profile.dev.package."*"]` and `[profile.test]`
  (`:71-72`, `:84-85`) after a link-time ENOSPC incident — one build tree
  shrank to the 16 GiB range from ~30+ GiB of dev/test double fingerprints,
  backtraces keep file:line, set `CARGO_PROFILE_TEST_DEBUG=2` when you
  need full variable info; new `cargo deny` advisories are paid down as
  they land (see Security).
- Console `api/messages.rs` zh dictionary grows to 93 keys / 137 call
  sites (36 / 50 at T34; the key count is pinned by the dictionary's own
  scale test): the DDL endpoints' 400 / 404 / 422 / 409 / 502 envelopes,
  the backfill request gates, the partial-update publish / deploy / evolve
  gates, the live key-model probe, the alert-rule CRUD gates (T-E2) and
  the declarative apply's `err.apply.*` whole-file refusals (T-G6) all
  carry `message_key`. Dictionary completeness is still enforced by the
  source scan test.
- Console SQLite schema: five new migrations, auto-applied on start —
  `01` adds `table_object.sequence_column`, `02` adds the `backfill_job`
  table, `03` adds `table_object.key_model` (the T-A7 registry gate's
  ledger), `04` adds `alert_rule` / `alert_event` (T-E2) and `05` adds
  `alert_event.series_key` for per-series episodes (R9 review round; an
  open episode written under `04` is adopted by the first series the
  evaluator sees firing for its rule, `adopt_alert_episode`
  `console/crates/console-store/src/alerts.rs:237`). Downgrading
  a data directory across them is not supported.
- DLQ accounting is at-least-once — disclosed, not changed (R10 tail-debt
  bundle, from dogfooding round 3): a bad row inside the replay window is
  quarantined again on every replay (round 3 recorded 3 bad rows → 9 DLQ
  records), the side file and `tailrace_dlq_rows_total` count quarantines
  rather than distinct rows, and a consumer must dedupe on `rawB64`
  (`docs/protocol/engine-v1.md:207`; console-api ruling 13). The round-3
  open item — a `file` run ending with the cursor at EOF and
  `totals.flushed` one short — is closed on the source-evidence chain in
  `docs/dev/open-item-cursor-flush.md`: every ledger save sits after a
  successful flush except stop finalisation's `pending_rows == 0` branch,
  which can only cross a row that was decoded Bad or projected away; three
  targeted DorisSink reproductions did not recreate the shape, and the
  document says so (with a capture checklist) rather than claiming a fix.
- Repository docs: the internal process records (roadmap, review minutes,
  incident reports, lineage decision) moved out of the public tree into a
  private workflow (`1bed48d`); `docs/dev/` now holds technical decision
  records and dogfooding reports only, and `docs/dev/README.md` states the
  boundary. Links into the removed files were rewritten.

### Deprecated

- **The unknown-column-type migration window closes in 1.3.0 (T-G2).** A
  type word in `TableObject.columns` outside the sixteen-word vocabulary
  currently logs a `warn` and folds to `utf8` at pipeline assembly and dim
  loading (`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW = true`,
  `engine/crates/tailrace-ir/src/column_type.rs:168`, fold arm
  `arrow_type_or_utf8` `:174`). 1.3.0 flips the constant to `false` and
  the fold becomes a hard error: a pipeline whose table objects carry an
  unknown word will refuse to assemble — the `warn` text already says
  "hard error next minor". The DDL generator and `ColumnType::parse`
  already refuse such words and `infer-schema` only emits vocabulary
  words, so the exposure is a hand-registered table object — REST, apply
  file or MCP — carrying a word outside the list. Audit them now: compare
  every type string in `GET /api/datasources/tables` → `columnsJson`
  against the list in `ColumnType` (`:30`), and fix the registration while
  the fold still carries the row.

### Fixed

- `badRowPolicy` was silently dropped by the console (R10 tail-debt
  bundle, from dogfooding round 3): `PipelineConfig` had no field for it,
  so the key vanished between the request body and `plan["options"]` and
  the engine kept running `dlq` while the request said `fail`. The DTO now
  carries `bad_row_policy`
  (`console/crates/tailrace-console/src/api/pipelines.rs:56`) and a
  contract test pins the console's key set equal to the engine's
  `PlanOptions` key set, so the next engine-side option cannot go missing
  the same way. e2e `alert_flow.sh` section B exercises `skip` / `fail` /
  an invalid value end to end (50/50), asserting on the engine's own
  `bad_row_policy=<v>` start line rather than on the DTO.
- Doris BE memory-pressure refusals were classified as fatal (R10
  tail-debt bundle, from dogfooding round 3): a `MEM_LIMIT_EXCEEDED` /
  "Cannot alloc" / low-water-mark Stream Load answer fell through the
  generic patterns to statement error → restart loop → a poison-data
  verdict on a healthy pipeline during a 20–40 minute BE memory window.
  `is_memory_pressure` (`engine/crates/tailrace-conn-doris/src/lib.rs:401`)
  now recognises the live 3.1.4 and 2.x spellings before the generic
  patterns and returns `FlushError::retry` with the buffer kept (status
  matrix rows ⑧ / ⑧b: same buffer resent, window passes → success;
  budget exhausted → "retries exhausted", never "not retryable");
  strict-mode / quality / schema refusals stay fatal and the poison-data
  criterion itself is unchanged.
- A vector table declared with a dimension (`list<float32,N>`) answered
  409 on every DDL `evolve` (R9 review round): Doris `DESC` cannot report
  the dim, so the registered-vs-live diff saw a retype each time;
  `diff_columns` now treats the dimmed and bare spellings as the same
  column (`console/crates/tailrace-console/src/api/datasources.rs:1753`).
- Doris sink label reuse after a same-process `stop → deploy` rebuild (R6
  review round, unplanned): a rebuilt `ProcessScoped` sink restarted its
  label ordinal at `_0` and reused a label the previous instance had
  already flushed, so Doris answered `Dedup`, the batch was silently
  dropped and the ledger still advanced. A process-level floor per label
  prefix (`engine/crates/tailrace-conn-doris/src/lib.rs:219-243`
  `LABEL_SEQ_FLOORS`) now makes a new instance continue from the highest
  ordinal already flushed; the `Deterministic` mode is exempt by design
  (the k-th batch of a replay must keep ordinal k).
- File source path resolution (R6 review round): a `file` table object
  whose path was resolved against the console process's working directory
  wrote `rowsWritten = 0` and reported success. References are now
  absolutised against the registered directory; the e2e asserts the old
  shape cannot recur.
- `scripts/release-crates.sh` read a hard-coded `VERSION`: at the
  1.0.0 → 1.1.0 release it still looked for 1.0.0 and skipped every
  crate. `b19661b`'s message promised a dynamic read but its diff only
  re-pinned the literal to `1.1.0` — the same skip would have recurred at
  this release. The 1.2.0 prep commit makes the promise true: the script
  now reads `[workspace.package] version` from `Cargo.toml` and refuses
  to run if it cannot, and its publish topology gains `tailrace-ddl` (L0,
  depended on by `tailrace-console`; added in R6 but never listed), with
  the integrity check raised from 21 to 22 packages — `--list` matches the
  `cargo metadata` publishable set exactly.

### Security

- `libz-sys` 1.1.29 → 1.1.31 (RUSTSEC-2026-0418, R8 gate): the ≤1.1.30
  build script printed the full build environment to stdout under
  cc ≥ 1.6.0, leaking secrets into `target/` caches; pulled transitively
  via rdkafka → curl-sys. The same `cargo update` took `ctutils`
  0.4.6 → 0.4.7 (RUSTSEC-2026-0385), `tokio` 1.53.3 → 1.54.1
  (RUSTSEC-2026-0386) and `zstd-sys` 2.1.1 → 2.1.2 (RUSTSEC-2026-0410).
  `cargo deny check advisories bans licenses sources` is green on the
  tree. The zstd stream-context advisories RUSTSEC-2026-0391 / 0398 / 0403
  stay as documented `deny.toml` ignores (transitive via parquet →
  datafusion; tailrace uses block-level parquet compression only, never a
  reused stream context) — added in the extension round together with a
  yanked `crypto-bigint` resolution.

## [1.1.0] - 2026-10-25

_Scope: changes since v1.0.0 — roadmap cards T32 / T34 (R5-W0), T39
(R5-W0.5), T33 (R5-W1) and T35 (R5-W2). T36 (R5 lineage judge) is a
docs-only decision record (docs/dev/R5-lineage-judge-20261025.md — group
commit / 2PC / WAL all stay on the v1.x/R6 agenda with rewritten triggers);
no user-facing change, hence no entry below. The R4 cards T22 / T25 / T26 /
T30 shipped inside 1.0.0 (entries below, not repeated here) and had no
post-1.0.0 follow-ups._

### Added

- Doris / StarRocks sink macro benchmark (roadmap T32): `scripts/bench/macro.sh`
  gains `--sink doris|starrocks` plus `--pipes` / `--flush-rows` /
  `--poll-rows`; `docs/BENCHMARKS.md` §6.4 records the six-row ledger on the
  M4 machine (file→doris single pipe, default batch 33852 rows/s vs the
  same-day file→file anchor 33568 — large-batch sink overhead is not
  measurable; small batch 500 rows 11017; P=4 small batch 26781 = 2.43× the
  single small-batch pipe yet −21% vs large batch — the group-commit target
  zone; P=4 poll-floor batch 97752; starrocks via `node run` 8750, a
  different-caliber reference). These numbers are the T36 lineage-decision
  input (T18 ④T3).
- `starrocks` is a first-class console datasource type (roadmap T39 ⑥,
  closing the T32 caveat): registry member with the doris connJson shape
  (`jdbcUrl` + `streamLoadUrl` required, `fenodes` optional), probe /
  discover / infer-schema shared with doris over the MySQL-protocol FE port,
  and its own table-object kind `starrocks` (cursor semantics `NONE`) — the
  engine's conn-doris factory selects the StarRocks Stream Load flavor off
  that kind, so StarRocks targets no longer need the `node run` bypass.
- `GET /api/datasources/tables` items carry `columnsJson` (roadmap T39 ⑤,
  closing the T22 caveat): the `[name, type]` pairs registered manually or
  via infer-schema, `null` when a table was registered without columns.
  Additive wire change; the web SQL editor's `db.table.` column completion
  (CodeMirror6 schema + member sources) is fed from it end-to-end.
- SSH deploy chain, the design §4.4 eight-step flow (roadmap T35, carried
  from T19): `POST /api/nodes/deploy` drives
  PENDING→PREFLIGHT→UPLOAD→UNPACK→CONFIGURE→START→HEALTH_WAIT→REGISTER→
  REGISTERED over russh (pure-Rust transport, ring backend, public-key
  authentication only — the console's `TAILRACE_SSH_KEY` /
  `TAILRACE_SSH_KEY_PASSPHRASE` env, credentials never ride the request
  body). Two trigger modes: `hosts[]` batch onboarding (the control plane
  pre-creates the node row with register_mode=deployed and a per-node
  token) or `nodeId` re-deploy of an SSH-managed node. Per-step progress
  lands in `GET /api/nodes/{id}/deploy-log` (a failure appends a ROLLBACK
  line); any step failure rolls back — prev→current symlink restore, old
  binary restart, health re-check, deploy_status=FAILED, artifact_id
  untouched; REGISTER additionally verifies engine_version matches the
  artifact. Node actions `start/stop/restart/rollback/uninstall` execute
  synchronously and `upgrade` walks the full chain asynchronously; the
  deploy/action 501 placeholders in the console admin API are gone
  (non-SSH-managed nodes get 400, SSH-side failures 502 SSH_FAILED;
  `migrate` and `ticket` stay 501 — P2 deliverables, out of this card's
  scope). Web nodes page wired;
  in-tree tests cover the state machine (unit) plus a docker target-host
  e2e including the mid-chain-failure → rollback case.

### Changed

- i18n promise paid (roadmap T34, the T30 channel): engine `tailrace-api`
  error responses carry a `code` field at 10 formerly ad-hoc sites (8
  canonical code constants; the 11th grep hit was a data field and is left
  as such) — additive, the legacy `{ok:false,error}` shape is pinned by
  wire-compat tests and an envelope key-set gate; console user-facing
  format strings (44 sites) collapsed into the zh single-track dictionary
  `api/messages.rs` (36 keys / 50 call sites, byte-identical rendering)
  with `message_key` on 38 §4.0 envelopes, completeness enforced by a
  source scan; the 34 `ApiError` literal sites outside that caliber are
  registered as the exempted remainder and machine-pinned.
- DataFusion 48.0.1 → 55.2.0 (roadmap T33): seven-major direct jump with
  `datafusion-proto` in lockstep — arrow/parquet 59.3.1, sqlparser 0.62.0,
  prost 0.14.4; MSRV unchanged (55.2.0 declares rust-version 1.94 ≤ the
  workspace's 1.95). Six code adaptations across the 12 engine touch-point
  files (extension-codec TaskContext signatures, `TableProvider::as_any`
  removal, `TableReference` relocating to `datafusion::common`, a
  sqlparser-0.62 `ObjectNamePart::Function` rejection arm). Contract
  goldens are byte-identical across the jump (proto-plan b64 and execution
  pretty snapshots; per-diff attribution in docs/dev/
  t33-golden-attribution.md); the two behavioral drifts are attributed —
  metric rendering `8000` → `8.00 K` (human_readable_count display layer,
  tests accept both forms) and the `output_batches` metric unlocked via
  BaselineMetrics (the DF48 §4.4 limitation is lifted, memory-pool
  partial-enforcement persists). Supply-chain side effects: paste /
  lzma-sys / xz2 / thrift leave the dependency graph (pure-Rust
  libbz2-rs-sys / liblzma replace the C bindings), bzip2-1.0.6 joins the
  deny license allowlist, and the RUSTSEC-2024-0436 (paste unmaintained)
  ignore retires with the crate. M4 macro benchmark file→file: 41528
  rows/s (first DF55 sample, same machine different day vs the DF48 anchor
  33568 = +23.7% — recorded as no-regression, not a confirmed gain). The
  Doris full-stack smoke (`smoke.sh --full`) was not re-run on the DF55
  tree before this release (disk-gated window); engine coverage for the
  jump is the L1 contract suite plus the macro row-count check
  (docs/BENCHMARKS.md §6.5).
- Runtime offset-store locking: every `state.lock()` in `tailrace-runtime`
  (serve loop + standalone statement bodies, 8 sites) now recovers a
  poisoned `Mutex` with `unwrap_or_else(|e| e.into_inner())` — registry
  parity — instead of cascading a sibling statement thread's panic into
  the cursor commit path (roadmap T39 ④).
- Supply chain: `utoipa-swagger-ui` 10.0.1 → 10.0.2, whose build-dep `zip`
  moves from the yanked 8.6.0 to 9.0.5; the RUSTSEC-2026-0367/0368/0369
  advisory ignores are dropped from `deny.toml` and `yanked = "deny"` is
  restored (roadmap T39 ⑦, §八.1 closed). The same lock update had also
  silently re-resolved four windows-only `windows-sys` edges (errno /
  rustix / tempfile / winapi-util) 0.61.2 → 0.52.0 — no impact off
  Windows targets; the T39 review fix re-pins them to 0.61.2, the
  pre-T39 state (`ring` keeps its pre-existing 0.52.0 edge).

### Security

- Two advisories the T39 gate record had claimed green (roadmap T39
  review fix): `ctutils` 0.4.3 → 0.4.4 (RUSTSEC-2026-0385,
  non-constant-time mask generation — the crate rides the console
  password-hashing path via argon2 → blake2) and `tokio` 1.53.2 → 1.53.3
  (RUSTSEC-2026-0386, unsound `AsyncFd` I/O safety); `cargo deny check
  advisories bans licenses sources` is green again (cargo-deny 0.20.2,
  advisory-db 84d943a).

## [1.0.0] - 2026-10-22

### Added

- StarRocks sink (roadmap T12): shares the Doris Stream Load path inside
  `tailrace-conn-doris` with a verified difference matrix (URL/port,
  `read_json_by_line` carrier, label idempotency, buffer-flush thresholds);
  `docker compose --profile starrocks` local stack (allin1 via feproxy);
  file→starrocks e2e including restart label-idempotency (replay with the
  same label dedups, table row count does not grow).
- JDBC batch source connector (`tailrace-conn-jdbc`, roadmap T13): MySQL /
  PostgreSQL keyset incremental polling — each poll runs
  `SELECT * FROM t [WHERE cursor > ?] ORDER BY cursor LIMIT n` and emits one
  JSON object per row (`kind=jdbc` pairs with `format=json`); the cursor
  (`Cursor::JdbcKey`) commits at flush points only (at-least-once). Driver:
  sqlx 0.9 subcrates, no TLS feature. Connector docs: `docs/connectors/jdbc.md`.
- discover / infer-schema (roadmap T14): the console's 501 placeholders are
  real — Kafka topic listing and tail sampling (rdkafka metadata), Doris /
  MySQL `SHOW`/`DESC` over the 9030 text protocol, PostgreSQL
  `information_schema`; datasource types gain the mysql/postgresql family
  and `jdbc`→`JDBC_KEY` cursor semantics; web datasources flow wired.
- Dynamic Kafka partition discovery (roadmap T15, option b): new partitions
  join the poll set without a pipeline restart (measured discovery latency
  26.8–31.2 s, three stable runs); fingerprint regression tests were written
  first. Split-parallelization was NOT implemented — the card's retreat gate
  fired (byte-level equivalence could not be constructed honestly); deferred
  to the v1.x agenda with the narrow latest+subscribe window.
- Opt-in jemalloc allocator for `tailrace-node` (roadmap T20): `jemalloc`
  feature (default OFF) via tikv-jemallocator; the heartbeat `resources`
  payload gains `peakRssBytes` (Linux `/proc/self/status` VmHWM). A/B
  numbers and caveats: `docs/BENCHMARKS.md` §六.
- i18n wire channel (roadmap T30): console error codes collapsed to
  single-point constants, and error responses carry an optional
  `message_key` field (wire-compatible: omitted by default, current response
  bytes unchanged) — groundwork for dictionary-based localization.

### Changed

- README (en/zh) connector matrix now states reality: jdbc source landed
  (T13), starrocks is an alias sharing the Doris Stream Load path (full
  parity = roadmap T12), mqtt/s3 are placeholders; the scripted smoke check
  is quoted per caliber — 25 assertions (default, dependency-free) / 44
  with `--full` (Kafka→Doris via docker compose).
- SQL editor migrated from monaco to CodeMirror6 (roadmap T22, landing the
  T17 verdict): editor async chunk gzip 883.22 kB → 138,962 B (T9-anchored
  single caliber, ≤300 kB gate), SQL keyword completion and FROM-alias
  awareness added, completion+`-- @tag:ds` insert split into two undo steps;
  the monaco dependency is removed from `web/package.json` and the T9
  late-CSS/layout-collapse patch class is gone.
- Connector feature trimming is real (roadmap T16): with
  `--no-default-features --features connector-file,connector-doris` the
  node actually drops kafka/rdkafka from its link graph — slim binary
  −9.4% (55,788,080 B vs 61,571,936 B default); `smoke.sh --slim`
  file→doris e2e green (30/30); CI gains a hard slim-features gate.
- Live-gated tests report as `ignored` instead of silently passing
  (roadmap T25, T18 ⑦b): `#[ignore = "live: …"]` + CI
  `--include-ignored` — skip counts are now visible in the honest ledger.
- OpenAPI components and `docs/protocol/console-api.md` synced with
  discover/infer-schema (roadmap T24); the StarRocks e2e is archived as
  the re-runnable `scripts/e2e/starrocks.sh`.
- Release surface (roadmaps T26/T27): all 21 published crates carry
  complete metadata (description/repository/keywords/categories) and ship
  in lockstep at 1.0.0; `tailrace-console` packages self-contained — the
  SPA build is embedded in the crate tarball (crate-local `dist/` via
  rust-embed), so `cargo install tailrace-console` builds without the
  repository.
- MSRV raised from 1.85 to 1.95: `rusqlite_migration` 2.6 (the locked
  version) declares `rust-version = 1.95`, so the previous claim was
  unenforceable. The `msrv` CI job now actually compiles the workspace on
  1.95.

### Fixed

- R3-W1 review fixes to the (unreleased) JDBC connector and runtime:
  - `Cursor::JdbcKey` fingerprint now folds key+value, so keyset position
    advances are visible to the NO_PROGRESS poison-data detector (three
    transient database outages no longer misjudge as poison).
  - The connect probe fails fast unless the cursor column is unique by
    itself (PRIMARY KEY or a single-column UNIQUE index) — keyset polling
    `WHERE col > ? LIMIT n` without a tie-breaker would silently skip
    duplicate values across a LIMIT boundary.
  - The CI chaos job now runs the JDBC real-database tests (testcontainers
    mysql:8.0 / postgres:15); previously JDBC coverage in CI was zero.
  - PostgreSQL FLOAT4 cursor columns: the kind probe accepted FLOAT4 but row
    conversion had no cursor arm, so every poll failed with a misleading
    "cursor column is NULL" and permanent backoff; FLOAT4 now mirrors FLOAT8
    with an exact widen-to-f64 canonical (a naive f32 shortest-repr would
    re-match the same row every poll).
  - Pipeline validation diagnostics now name the JDBC/JDBC_TABLE mix-up:
    `jdbc` is the MySQL/PostgreSQL source kind, `jdbc_table` is the Doris
    sink alias (a source declared `jdbc_table` resolves to doris and gets
    a pointed hint instead of a bare "got doris"); the sink diagnostic list
    now includes starrocks, which the whitelist already accepted.
- Contract-test suite built as a single package no longer ran against an
  empty connector registry (R3-W3 review finding): `tests/contract` pins
  `tailrace-runtime` `features=["connector-file"]` explicitly, so
  `cargo test -p tests-contract` exercises file→file as documented instead
  of failing executor determinism 3/3.

## [0.1.0] - 2026-10-13

Initial baseline: the SQL pipeline engine, management console, and embedded
web UI.

### Added

- Engine workspace (`engine/`): SQL pipeline parser/IR/transform, runtime with
  offset-based cursors, file source/sink, Kafka source, Doris Stream Load sink
  (mqtt/jdbc/s3 connector crates are placeholders), node REST API (`/v1`) and
  heartbeat self-registration against the console.
- Console (`console/`): auth (argon2 + AES-256-GCM credential storage), SQLite
  store, pipeline/node/datasource management API, artifact deploy chain; web
  console SPA (`web/`, Vue 3) embedded into the single-binary console via
  rust-embed.
- Engineering baseline: `cargo fmt` / `clippy -D warnings` / `cargo test`
  gates with dependency caching, MSRV (1.85 at tag time) and `--no-default-features`
  checks, `cargo deny` supply-chain gates (advisories/bans/licenses/sources),
  end-to-end smoke script (`scripts/smoke.sh`, 25 assertions), 4-target
  release matrix (linux amd64/arm64, macOS amd64/arm64) with aggregate
  `checksums.txt`, and community health files (CONTRIBUTING, SECURITY,
  CODE_OF_CONDUCT, issue/PR templates).

[Unreleased]: https://github.com/SeekerGalahad/tailrace/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/SeekerGalahad/tailrace/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/SeekerGalahad/tailrace/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/SeekerGalahad/tailrace/compare/v0.1.0...v1.0.0
[0.1.0]: https://github.com/SeekerGalahad/tailrace/releases/tag/v0.1.0
