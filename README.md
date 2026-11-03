# tailrace

[![CI](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml/badge.svg)](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tailrace-node.svg)](https://crates.io/crates/tailrace-node)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)

A single-binary data ingestion engine with a built-in web console. SQL-first, no
JVM, no containers — move data from Kafka, files and databases into Doris /
StarRocks from the edge, and run the Day-2 loop around that write path (table
DDL, deterministic backfill, partial-column update, Prometheus metrics, an MCP
server) from the same two binaries.

> No Temporal, no sidecars, no 8GB floor.

## Status

**Released: v1.2.0** — CHANGELOG cut 2026-10-31; the v1.2.0 tag and the
GitHub Release (9 assets: 8 tarballs + checksums.txt) went live on
2026-11-02. The workspace is versioned 1.2.0 in lockstep (22 publishable
crates). **As of 2026-11-03 the crates.io publish has not been executed** —
crates.io still shows 1.1.0 (checked: `tailrace-ir` / `tailrace-console` /
`tailrace-runtime` max_version 1.1.0; `tailrace-ddl` was never published),
so `cargo install tailrace-node --version 1.2.0` does not resolve yet; use
the GitHub Release tarballs or build from the tag. It ships the engine data
plane (kafka / file / jdbc sources; Doris + StarRocks Stream Load and file
sinks), datasource discovery & schema inference (`discover` /
`infer-schema`), the SSH deploy chain, the performance ledger
(docs/BENCHMARKS.md), the 8-job CI matrix and the capabilities below — on
top of v1.1.0 (2026-10-25), which put the first 21 crates on crates.io with
its tag and a 9-asset release.

**Since v1.1.0** six development rounds landed in v1.2.0 — `/metrics`,
Debezium CDC decoding, an MCP server, broadcast dimension joins, the DDL
closed loop, deterministic backfill, partial-column update, a 16-word
column type vocabulary, `TableObject` options, sink transaction hooks,
`sequence_col` DDL, workspace-wide `forbid(unsafe_code)`, then (release
line) VARIANT passthrough, vector (`ARRAY<FLOAT>`) columns, console
alerting with SLO exposure, the open `Cursor::Custom` variant and
pipeline-as-code (`POST /api/apply` + CLI). **After the tag, the v1.3
development line (R11) added**: idle-aware alerting (a running-but-silent
pipeline no longer pages on lag), DLQ row-key dedup (metrics keep attempt
counts), the MCP apply pair (14 → 16 tools, budget full), SLO lag p50/p95/
p99 over a 1h sample ring, and the **1.3.0 BREAKING** closure of the
unknown-column-type migration window (warn-and-fold became a hard error).
The round-by-round account is
[docs/design/architecture.zh.md](docs/design/architecture.zh.md) §12, and
[CHANGELOG.md](CHANGELOG.md) is the release log (Unreleased = the v1.3
line).

Sink delivery tiers, stated plainly: **label idempotency is delivered**, in
two modes. Resident pipelines use *process-scoped* labels — an in-process
retry reuses one label and Doris dedups it; across a restart, no-duplication
comes from the committed cursor plus the table's key model, not from the
label. Backfills use *deterministic* labels — the same range maps to the same
labels across processes, Doris answers `label already exists`, the sink
records `Dedup` (e2e: replaying one range twice on a DUPLICATE KEY table
leaves the row count unchanged). The transaction-interface (exactly-once)
and group-commit throughput tiers are **not delivered**: the `Sink` trait now
carries `prepare / commit / abort` hooks (no-op defaults) plus a
`SinkCapabilities` declaration as the extension point, and no production sink
claims `transactional` yet. mqtt / s3 connectors remain one-line placeholders.

## Quickstart

Prerequisites: Rust ≥ 1.95 (rustup; the repo pins a stable toolchain), Node.js 22, pnpm.

```bash
# 1. build the web bundle, embed it, and build both binaries
./scripts/build.sh

# 2. start the console (web UI on http://127.0.0.1:8080)
./target/release/tailrace-console --data-dir ./data
#    First boot prints the one-time admin password and the bootstrap token to
#    stderr — copy both now, they are not shown again.
#    Add --mcp-allow-write to enable the MCP write tools (default: read-only).

# 3. start an engine node that self-registers with the console
./target/release/tailrace-node serve \
  --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 \
  --advertise 127.0.0.1:18100 \
  --bootstrap-token <token-printed-at-first-boot>
```

Then open http://127.0.0.1:8080, log in with `admin` + the captured password, and create a
pipeline. For a scripted end-to-end check, run `./scripts/smoke.sh` (25 assertions, no
external dependencies) or `./scripts/smoke.sh --full` (44 assertions — Kafka→Doris via
docker compose). Prometheus scrapes `/metrics` on both binaries; AI clients
speak MCP on `POST /mcp` of the console.

## Connector matrix

| Kind | Source | Sink | Notes |
|---|---|---|---|
| kafka | ✅ | ✅ | primary source, per-partition offset cursors, dynamic partition discovery; the sink produces one JSON object per row to the topic — producer `enable.idempotence` within a session, no broker-side dedup label (at-least-once on replay), no partition-key routing (`engine/crates/tailrace-conn-kafka/src/lib.rs:48` / `:502`) |
| file | ✅ | ✅ | local / mounted files; today the only source the console backfill accepts (byte-offset bounds); the sink appends jsonl or rolls a parquet part file per flush (`engine/crates/tailrace-conn-file/src/lib.rs:21` / `:374`). Neither sink claims `partial_update` / `transactional`; the parser's sink whitelist is kafka / file / doris / starrocks (`engine/crates/tailrace-parser/src/lib.rs:159`) |
| jdbc | ✅ | — | MySQL / PostgreSQL keyset batch source ([docs](docs/connectors/jdbc.md)) |
| dim | JOIN-only | — | JDBC-backed **broadcast** dimension table (`kind = dim`, referenced as `${ds:dim.<alias>}` in a JOIN): loaded full-table at assembly, 500k-row / 64 MiB caps, unique key required — not a streaming join |
| doris | — | ✅ | Stream Load; label idempotency (process-scoped + deterministic), partial-column update, sequence column |
| starrocks | — | ✅ | Stream Load via the Doris sink with kind-gated StarRocks deltas (FE 307 redirect following, header set); e2e'd against `starrocks/allin1` (`docker compose --profile starrocks`); partial-column update is Doris-only |
| mqtt | — | — | placeholder, not implemented (trigger-gated — docs/ASSESSMENT-AND-OUTLOOK.md §五) |
| s3 | — | — | placeholder, not implemented (trigger-gated, same) |

Source decoders (the table object's `format`): `ogg` (default), `json` /
`jsonl`, `csv`, `parquet`, `debezium-json` — Debezium CDC events already on
Kafka; tailrace consumes them, it does not read binlogs.

## Capability matrix (v1.2.0 + the v1.3 development line)

Everything below landed in this source tree after v1.1.0, covered by unit /
contract tests plus the listed e2e scripts. Rows tagged **[1.2.0]** shipped
in the release (tag 2026-11-02); rows tagged **[1.3-dev]** are the R11
round, in CHANGELOG `Unreleased` and not yet released. Anchors are
`path:line` in this repository.

| Capability | What it does | Where (anchor) |
|---|---|---|
| **Prometheus `/metrics`** [1.2.0] | 10-metric surface in hand-written text format, zero exporter crates (9 at 1.2.0; the source-idle gauge below is the 10th); node renders it from live pipeline counters at scrape time, console adds `tailrace_console_db_write_seconds` plus the SLO target line and, since R11, lag quantiles | `engine/crates/tailrace-api/src/metrics.rs:9` (surface) `:246` (`node_metrics_text`); `console/crates/tailrace-console/src/metrics.rs:160` (`scrape`), route `console/crates/tailrace-console/src/lib.rs:208` |
| **Debezium CDC decoding** [1.2.0] | `format = "debezium-json"` decoder: `c`/`r`/`u` events to rows, `d` and tombstones skipped, schemas-enabled wrapper unwrapped, malformed events to the DLQ. Narrow-path CDC: consume events, never capture binlogs | `engine/crates/tailrace-decode/src/lib.rs:1204` (`DebeziumJsonDecoder`); dispatch `engine/crates/tailrace-runtime/src/lib.rs:1636` |
| **MCP server** [1.2.0 → 16 tools in 1.3-dev] | `POST /mcp` (JSON-RPC 2.0, Streamable HTTP): **16 tools — 9 read, 7 write** (14 at 1.2.0; the R11 apply pair spent the ≤16 budget). Write tools need **both** `--mcp-allow-write` / `MCP_ALLOW_WRITE=1` and `"confirm": true`; every call is audited as `MCP_TOOL_CALL`, `actor = mcp:<client>`, with credentials redacted at every depth; the read-only `apply_dry_run` refuses declaration files carrying credentials (no password oracle on an unauthenticated endpoint). Tool descriptions are generated from the OpenAPI document — nothing to drift | `console/crates/tailrace-console/src/mcp.rs:104` (`TOOLS`), route `:191`; audit redaction `:1281`; flag `console/crates/tailrace-console/src/main.rs:32` |
| **Broadcast dimension join** [1.2.0] | `… JOIN ${ds:dim.<alias>} d ON …` loads the dim table full-table over JDBC at assembly into a static MemTable; 500 000-row / 64 MiB caps fail fast, unique key required, INNER / LEFT only (RIGHT / FULL / CROSS rejected at compile) | `engine/crates/tailrace-runtime/src/dim.rs:36` `:39` (caps); join-type gate `engine/crates/tailrace-parser/src/lib.rs:358-359` |
| **DDL closed loop** [1.2.0] | draft → human-confirmed key model (required; 422 otherwise) → execute with full-text audit → evolve = `ADD COLUMN` only (Light Schema Change; retype / drop reported, never run). Pure generator: schema + hints in, DDL + "this is a guess" annotations out; 13 scalar vocabulary words + `variant` → `VARIANT` + `list<float32[,dim]>` → `ARRAY<FLOAT>`, unmapped types (`null`) are an error, not a fold | `engine/crates/tailrace-ddl/src/lib.rs:180` (`generate_draft`) `:438` (`doris_type`); exec `engine/crates/tailrace-metadata/src/lib.rs:233` (`mysql_exec`); endpoints `console/crates/tailrace-console/src/api/datasources.rs:1930` `:2047` `:2206`; web wizard `web/src/api/datasources.ts` |
| **Deterministic backfill** [1.2.0] | `POST /api/pipelines/{id}/backfill` with `dryRun` (zero side effects) → bounded run in a `bf-<job>` ledger namespace and its own consumer group; labels `prefix-stmt-<sha256(from,to)[..8]>_<seq>` so a replayed range dedups; orphaned jobs are failed at console start. Source gate today: `file` only (Kafka per-partition seeding is a follow-up) | `engine/crates/tailrace-conn-doris/src/lib.rs:153` (`range_fingerprint`) `:187` (`LabelMode`); `engine/crates/tailrace-runtime/src/lib.rs:849` (`run_bounded`) `:1676` (`backfill_state_dir`); `console/crates/tailrace-console/src/api/pipelines.rs:2341` (POST) `:2437` (jobs) `:2265` (sink gate) |
| **Partial-column update** [1.2.0] | The switch is SQL: `INSERT INTO sink (k, a) SELECT …` — the INSERT column list declares the subset, a bare INSERT writes whole rows. Four gates: registered `key_model = unique` → live `SHOW CREATE TABLE` must be UNIQUE merge-on-write (FE unreachable = 502, not a guess) → no in-flight schema change (409) → sink capability + `sequence_column ∈ subset`. Stream Load sends `partial_columns:true` **and** a `columns` header (without it Doris silently NULLs the unlisted columns — live-probed) | parser `engine/crates/tailrace-parser/src/lib.rs:93-98` `:281` (`insert_column_list`); `engine/crates/tailrace-conn-doris/src/lib.rs:589` (`with_partial_columns`); gates `console/crates/tailrace-console/src/api/pipelines.rs:369` `:462` `:632` |
| **16-word column types** [1.2.0 + 1.3-dev BREAKING] | `utf8 string int8 int16 int32 int64 float32 float64 decimal(p,s) timestamp(unit) date bool binary list<float32[,dim]> variant null`; parametric parse, canonical Display. Unknown words: 1.2.x warned and folded to `utf8` (one-minor migration window); **this tree (1.3.0) is a hard error at assembly** (`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW = false`) — audit hand-registered type strings before upgrading | `engine/crates/tailrace-ir/src/column_type.rs:31` (`ColumnType`) `:96` (`parse`) `:173` (window const, now false); re-exported as `tailrace_catalog::ColumnType` |
| **`TableObject` options** [1.2.0] | 5 generic fields (`datasource / name / kind / format / columns`) + `options` map; connector fields read through typed accessors (`KafkaTableExt` / `DorisTableExt` / `JdbcTableExt` / `FileTableExt`); key names canonical in `option_keys`; legacy flat fields still read and written (serde double-write) | `engine/crates/tailrace-catalog/src/lib.rs:38` (`option_keys`) `:83` (`TableObject`) `:110` (`options`); accessors `tailrace-conn-kafka:31` `tailrace-conn-doris:256` `tailrace-conn-jdbc:80` `tailrace-conn-file:49` |
| **Sink transaction hooks + capabilities** [1.2.0] | `prepare / commit / abort` on the `Sink` trait with no-op defaults (every existing sink unchanged) and `SinkCapabilities { idempotent_label, transactional, overwrite, partial_update, partition_aware }`; the runtime refuses `partial_columns` on a sink without `partial_update` — the flag is load-bearing, not decorative | `engine/crates/tailrace-connector/src/sink.rs:18` (`SinkCapabilities`) `:107` `:113` `:119` (hooks) `:126` (`capabilities`) |
| **`sequence_col` DDL property** [1.2.0] | Under UNIQUE KEY a declared `sequence_column` is emitted as `"function_column.sequence_col"` — without it Doris silently ignores the engine's `sequence_column` Stream Load header (probed on 2.1.7 and 3.1.4) | `engine/crates/tailrace-ddl/src/lib.rs:370` |
| **VARIANT passthrough** [1.2.0] | A `variant` column carries its nested JSON document through unparsed (serialized text on the wire); Doris parses it server-side — the sink needed zero changes (probe-captured Stream Load body proves the wire shape). Known trade-off: a *root-level string* value is re-parsed server-side (kept for the Debezium-encodes-JSON-as-string case) | DDL `engine/crates/tailrace-ddl/src/lib.rs:451`; marker `engine/crates/tailrace-ir/src/column_type.rs:216` (`VARIANT_FIELD_METADATA`); probe `scripts/e2e/evidence/variant-probe-2026-10-30/` |
| **Vector column** [1.2.0] | `list<float32[,dim]>` → `ARRAY<FLOAT>` with the dimension as a column comment; every row is validated against the declared dim before any write (bad vector → whole row to the DLQ — Doris would silently NULL-degrade it); dim ≤ 65 536, elements finite; a list column never takes the key or a partition. HNSW/IVF indexing stays the user's DDL | DDL `engine/crates/tailrace-ddl/src/lib.rs:482` `:495`; guard `engine/crates/tailrace-decode/src/lib.rs:899` (`check_vector_cells`); cap `engine/crates/tailrace-ir/src/column_type.rs:267` |
| **Console alerting** [1.2.0 + 1.3-dev] | Rules engine in the console (the node has none): five rule kinds (`lag_seconds / state_failed / heartbeat_age / dlq_window / flush_errors_window`) evaluated every 15s against the same aggregated node `/metrics` snapshot the scrape serves. Episodes are **per series** with **three-state verdicts** (Firing / Clear / Unknown-holds: window without a base, unreachable node, idle source); rows land on transitions only; resolved notices are always delivered (condition cleared / series gone / disabled / deleted); delivery = webhook POST (off the eval loop, fire-and-log) + SSE `GET /api/alerts/stream`; audited rule CRUD at `/api/alerts/rules`. Companion `docs/ops/prometheus-alerts.yaml` (7 rules) + `docs/ops/grafana-dashboard.json` | `console/crates/tailrace-console/src/alerting.rs:78` (kinds) `:559` (`evaluate_rule`) `:1026` (`spawn_evaluator`); store `console/crates/console-store/migrations/04_alert.sql` + `05_alert_series.sql`; API `console/crates/tailrace-console/src/api/alerts.rs` |
| **SLO: target + achieved** [1.2.0 target line; 1.3-dev quantiles] | Every enabled lag rule exposes its threshold as `tailrace_slo_lag_target_seconds` under the lag series' labels (1.2.0). The achieved side (R11): a 1h lag sample ring (`slo_lag_sample`, one sample per pipeline/statement per evaluator pass from the same snapshot; idle series not sampled), nearest-rank p50/p95/p99 with sample counts on `/metrics` and `GET /api/slo?windowSecs=` — every quantile is a stored sample, recomputable | `console/crates/tailrace-console/src/slo.rs:118` (`record_pass`) `:133` (`nearest_rank`); `console/crates/console-store/migrations/06_slo_lag_sample.sql`; `console/crates/tailrace-console/src/api/slo.rs:93`; metrics `console/crates/tailrace-console/src/metrics.rs:214` `:270` |
| **Idle-aware alerting** [1.3-dev] | A running-but-silent pipeline no longer pages on lag forever: sources carry a **positional** idle verdict (file at EOF; kafka next offsets at every partition's end offset for 3 consecutive checks — never a clock), the statement publishes idle = source clean **and** nothing pending in the sink buffer (a failing sink's real lag keeps evaluating), and the console's lag rule holds (Unknown) an idle series while absent/0 verdicts behave exactly as 1.2.0. Exposed as `tailrace_pipeline_source_idle{pipeline,statement}` (omitted when the source cannot tell) and `sourceIdle` in stats | trait `engine/crates/tailrace-connector/src/source.rs:98` (`idle`); verdict `engine/crates/tailrace-runtime/src/lib.rs:211` (`publish_idle`) `:226` (`IdleResetGuard`); kafka tracker `engine/crates/tailrace-conn-kafka/src/lib.rs:100`; hold `console/crates/tailrace-console/src/alerting.rs:567-579` |
| **DLQ row-key dedup** [1.3-dev] | The DLQ side file is a *set* of bad rows, not a log of isolation attempts: the key is `(statement, row position, rawB64)` where the row position is the chunk's own cursor advance (kafka `(partition, next offset)`, file `next byte position`) — one bad row lands exactly once however a replay cuts its polls. The attempt counters (`tailrace_dlq_rows_total`, stats `bad`) are untouched — the replay storm stays visible | `engine/crates/tailrace-runtime/src/dlq.rs:19` (semantics) `:208` (`record_key`) `:254` (`seed_index`); contract `tests/contract/tests/executor_loop.rs:246` `:481` |
| **`Cursor::Custom`** [1.2.0] | The cursor enum gains a connector-defined variant `Custom { kind, payload }` (CDC LSN, Iceberg snapshot id, …) — `kind` names the semantics so a connector can refuse a foreign cursor; the four built-in variants serialize byte-identically and old ledgers load without migration (read old, write new, legacy fixtures pinned). No shipped connector writes one yet — this is the door, not the WAL/LSN source | `engine/crates/tailrace-ir/src/cursor_sourcechunk_impl.rs:64`; ledger `engine/crates/tailrace-state/src/lib.rs:34-41`; fixtures `engine/crates/tailrace-state/tests/fixtures/` |
| **Pipeline as code** [1.2.0 + 1.3-dev MCP] | One declarative JSON file (`datasources` / `tables` / `pipelines`, any subset) through one server-side pair: `POST /api/apply/dry-run` computes a structured field-level diff (read-only, unaudited), `POST /api/apply` performs the idempotent upsert — both share `plan()`, so the diff a human reviews is the diff the machine applies. Identity is the business key (names, never ids); an absent key makes no statement, a present key is the declared full set and un-named objects are reported as **orphans, never deleted**; per-object bodies ARE the REST DTOs (serde single-source); credentials compared by presence only, never echoed. CLI thin shell `tailrace-console apply -f <file> [--dry-run]`; MCP tools `apply` / `apply_dry_run` (R11); field table in `docs/ops/pipeline-file.md` | `console/crates/tailrace-console/src/api/apply.rs:148` (dry-run) `:183` (apply) `:347` (`plan`); CLI `console/crates/tailrace-console/src/apply_cli.rs`; routes `console/crates/tailrace-console/src/lib.rs:137-138`; MCP `console/crates/tailrace-console/src/mcp.rs:643` |
| **Engineering governance** [1.2.0] | `unsafe_code = "forbid"` for the whole workspace; `debug = "line-tables-only"` for dependencies and test builds (one build tree shrank to the 16 GiB range after a disk-full incident); `cargo deny` advisories paid down as they land | `Cargo.toml:55-56` (lints) `:71-72` `:84-85` (profiles); `deny.toml` |

e2e coverage for the rounds above, against real Doris (compose 2.1.7 and an
external 3.1.4): `scripts/e2e/ddl_create.sh` (48/48), `backfill_replay.sh`
(31/31), `partial_update.sh` (38/38 compose + 37/37 external),
`type_precision.sh` (decimal(18,2) precision, sequence_col ordering),
`variant.sh` (41/41 on 3.1.4 + 42/42 on 2.1.7), `variant_vector.sh`
(46/46 on 3.1.4 + **47/47 on compose 2.1.7, re-verified 2026-11-02**) and
`alert_flow.sh` (**69/69** after the R11 rewrite around idle/DLQ-dedup
semantics), plus four dogfooding rounds (`scripts/dogfood/`, reports in
`docs/dev/`; round 4 = 36/36 driving pipeline-as-code through the MCP face
only). Evidence under `scripts/e2e/evidence/`; earlier counts are the
figures recorded at the time each round landed — not re-run for this README.

## Benchmarks

All figures below are single-machine measurements on developer machines, in
debug-tier builds unless noted — **not comparable across machines**, and
debug-tier numbers do not represent release-build performance. The sole
source for these figures (methodology, per-run samples, machine ledger) is
[docs/BENCHMARKS.md](docs/BENCHMARKS.md).

| Bench (100k rows unless noted) | Machine / build | Result |
|---|---|---|
| file→file full chain | Intel i7-9750H, debug | **21739 rows/s** (v0.1.0 baseline, 2026-10-17) |
| kafka(P=6)→file, 4 runs | Apple M4, debug | **median 27861 rows/s** (23918 / 27678 / 28043 / 28474) — first M4 tier of this bench; cross-machine-incomparable with the Intel-side 24658 rows/s |
| file→doris single pipe, default batch (T32) | Apple M4, debug | **33852 rows/s** vs same-day file→file anchor 33568 (+0.8%) — no measurable sink overhead at large batch; small batch (500 rows) 11017 |
| file→file, DataFusion 48.0.1 → 55.2.0 (T33) | Apple M4, debug | **41528 rows/s** vs DF48 anchor 33568 (+23.7%, same machine, different day — recorded as "no regression", not as a gain) |
| connector-trimmed `tailrace-node` (file+doris, no kafka) | release build | binary size **−9.4%** (61,571,936 B → 55,788,080 B) |
| dynamic Kafka partition discovery (2→4 partitions, no restart) | testcontainers Kafka | **26.8 s** (subscribe) / **30.7 s** (assign) |

## FAQ

**How does tailrace differ from Flink CDC / SeaTunnel / Vector / Airbyte?**
Flink CDC and SeaTunnel are JVM data-integration platforms; Airbyte is a
multi-container ELT platform (Temporal included); Vector and Redpanda Connect
are single binaries with private transform languages (VRL / Bloblang) and no
control plane. tailrace is a deliberately small ingestion engine — one SQL
statement per pipeline, at-least-once delivery with sink-side label
idempotency, two binaries, no cluster — that also ships the operations loop
around the Doris / StarRocks write path: DDL drafting with a confirmed key
model, deterministic backfill, partial-column update, `/metrics`, an MCP
server and an SSH deploy chain. Streaming joins, watermarks and state
backends are explicit non-goals. Pick a platform when you need one; pick
tailrace to feed Doris / StarRocks from a small box at the edge.

**Why no JVM, and how is it a single binary?**
Engine and console are pure Rust. The web console is embedded in the console
binary (rust-embed); librdkafka is built in-tree (cmake) and openssl is
vendored. You ship two artifacts — `tailrace-console` (control plane + UI)
and `tailrace-node` (engine) — targeting 2–4 GB machines. A connector-trimmed
node build drops the whole kafka stack (`--no-default-features --features
connector-file,connector-doris`, −9.4% binary size; BENCHMARKS §六).

**How do I do a partial-column update?**
Write the column list in the INSERT: `INSERT INTO ${ds:…} (k, a) SELECT k, a
FROM …` updates only `k` and `a` and keeps the other columns' old values; a
bare `INSERT INTO ${ds:…} SELECT …` writes whole rows. The target must be a
Doris UNIQUE KEY merge-on-write table (checked against the live table at
deploy, not just the registry), the `sequence_column` must be in the list,
and deploy is refused with 409 while an `ADD COLUMN` job is still running on
that table — the DDL evolve endpoint refuses in the other direction too.
StarRocks sinks do not support this mode.

**Is a backfill idempotent, and for how long?**
Yes, by label: a backfill's Stream Load labels are
`prefix-<statement>-<sha256(from,to)[..8]>_<batch>`, so replaying the same
range from any process reuses the same labels and Doris rejects the
duplicates (`label already exists` → `Dedup`). The idempotency window is
Doris' **label retention period** (default 3 days) — a replay older than that
falls back to the table's key model. Backfills run in their own `bf-<job>`
ledger namespace and consumer group and never touch the resident pipeline's
cursor. Today the console accepts backfills for `file` sources only, with
byte-offset bounds; `dryRun: true` previews the label prefix with zero side
effects (the MCP tool defaults to it).

**Is the dimension join a streaming join?**
No — it is a broadcast join and the red line stands. The dim table is a
`kind = dim` JDBC table object loaded full-table at pipeline assembly into a
static in-memory table; no watermark, no state backend, no refresh while the
pipeline runs (a restart reloads it). Guardrails fail fast at compile: 500 000
rows / 64 MiB caps, the dim table must have a primary / unique key, and only
`INNER` / `LEFT [OUTER] JOIN` are accepted. Statements with a dim join cannot
be backfilled yet.

**Does tailrace do CDC?**
It consumes CDC, it does not capture it: set `format = "debezium-json"` on a
Kafka table object and the decoder turns Debezium `c` / `r` / `u` events into
rows (`after` payload), skips `d` events and tombstones, unwraps the
schemas-enabled envelope and routes malformed events to the DLQ. Reading
binlogs / WAL yourself stays out of scope — point Debezium at the database
and tailrace at the topic.

**What is the MCP server for?**
AI clients can operate the console over `POST /mcp` (JSON-RPC 2.0): 16 tools
— list / inspect / compile / stats / logs / DDL draft / backfill status /
apply-dry-run are read-only and always on; create / deploy / pause-resume /
backfill / DDL execute / DDL evolve / apply are write tools that need the
console started with `--mcp-allow-write` (or `MCP_ALLOW_WRITE=1`) **and**
`"confirm": true` in the call. Every call lands in the audit log as
`MCP_TOOL_CALL` with `actor = mcp:<client>`, credentials redacted at every
depth; `apply_dry_run` additionally refuses a declaration file that carries
datasource credentials (an unauthenticated endpoint must not become a
password oracle). Tool descriptions are generated from the same OpenAPI
document that serves `/swagger-ui`. The 16-of-16 budget is full — a new tool
needs a budget ruling first.

**Which column types can a table object declare?**
Sixteen words: `utf8 string int8 int16 int32 int64 float32 float64
decimal(p,s) timestamp(unit) date bool binary list<float32[,dim]> variant
null`. The four legacy words keep their meaning byte for byte. Unknown
words: 1.2.0 logged a warning and folded to `utf8` (one-minor migration
window); **the 1.3 development line in this tree is already a hard error at
pipeline assembly** (`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW = false`) — audit
hand-registered type strings before upgrading. The DDL generator maps the
13 scalar words to Doris types, `variant` to `VARIANT` (nested JSON is
passed through unparsed, not flattened) and `list<float32[,dim]>` to
`ARRAY<FLOAT>` (the dimension rides as a column comment and every row is
validated against it before the write); `null` is the one word left
unmapped.

**Why does a running pipeline with no data stop firing lag alerts?**
Because its source reports idle, by position — not by clock. The lag gauge
is wall time since the last data-carrying flush, so a silent pipeline would
"lag" forever; since the 1.3 line, the file source reports idle at EOF and
the Kafka source once its next offsets have matched every partition's end
offset for 3 consecutive checks (a slow consumer with rows ahead is lagging,
not idle), the statement only publishes idle when the sink buffer is also
empty (a failing sink's real lag keeps evaluating), and the console's lag
rule holds an idle series instead of firing — while sources without a
verdict behave exactly as before. The PromQL twin is in
`docs/ops/prometheus-alerts.yaml` (`unless ... tailrace_pipeline_source_idle
== 1`).

**How do I manage pipelines from a file (GitOps)?**
Write a declaration JSON (`datasources` / `tables` / `pipelines`, any
subset — field table and examples in `docs/ops/pipeline-file.md`), then
`tailrace-console apply -f file.json --dry-run` previews a structured
field-level diff and the same command without `--dry-run` performs the
idempotent upsert (REST `POST /api/apply[/dry-run]` and the MCP `apply` /
`apply_dry_run` tools hit the same server-side `plan()`). Identity is the
business name, never a numeric id; objects that already match are untouched;
objects the file does not name are reported as orphans and **never deleted**
— v1 has no prune. Credentials compare by presence only and are never
echoed back.

**What is the StarRocks story vs Doris?**
One Stream Load implementation: StarRocks reuses the Doris sink inside
`tailrace-conn-doris` with kind-gated deltas (FE 307 redirect following,
header set, `read_json_by_line` carrier, label idempotency, buffer-flush
thresholds). Verified end-to-end against `starrocks/allin1`
(`docker compose --profile starrocks`). Partial-column update is Doris-only.

**When will mqtt / s3 connectors land?**
Not scheduled; both are one-line placeholders gated on real demand (MQTT: ≥3
real issues or a self-use scenario, PoC first, 12-month window; S3: after a
lakehouse sink is actually asked for). The CDC half of the old "S3/CDC" item
has landed as the `debezium-json` decoder. The public roadmap is
docs/ASSESSMENT-AND-OUTLOOK.md §五.

**Which platforms and Rust version are supported?**
MSRV is Rust 1.95 (workspace `rust-version`, enforced by the CI msrv job).
CI runs 8 jobs across ubuntu-latest and macos-latest (fmt/clippy/test, msrv,
feature-contract builds, web, chaos on ephemeral testcontainers, end-to-end
smoke). The release pipeline cross-builds linux x64/arm64 and macOS
x64/arm64 — two binaries per target.

## Documentation

- [CHANGELOG.md](CHANGELOG.md) — release log
- [docs/ASSESSMENT-AND-OUTLOOK.md](docs/ASSESSMENT-AND-OUTLOOK.md) — assessment & outlook (§〇 post-R11 baseline on top of the v1.1.0 snapshot; the public roadmap is its §五)
- [docs/design/architecture.zh.md](docs/design/architecture.zh.md) — architecture design, post-R11 (Chinese)
- [docs/ops/](docs/ops/) — ops bundles: grafana-dashboard.json (incl. the SLO row), prometheus-alerts.yaml (7 rules), pipeline-file.md (apply declaration)
- [docs/protocol/console-api.md](docs/protocol/console-api.md) / [docs/protocol/engine-v1.md](docs/protocol/engine-v1.md) — console REST + MCP surface / engine `/v1` protocol
- [docs/connectors/jdbc.md](docs/connectors/jdbc.md) — JDBC batch source
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — performance ledger (sole source of the numbers above)
- [docs/dev/](docs/dev/) — technical decision records and dogfooding reports
- [docs/book/](docs/book/) — mdBook docs site (Chinese single-track; build locally with `mdbook build`)

## Development

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build (web/)
./scripts/dev.sh     # console (debug) + Vite dev server with /api proxy
```

## i18n

Console UI and API error messages are Chinese-only (single-track) today.
The wire already reserves a `message_key` dictionary channel (responses
stay byte-compatible), and localization proper is on the v1.x agenda.

License: Apache-2.0. 中文文档见 [README.zh-CN.md](README.zh-CN.md)。
