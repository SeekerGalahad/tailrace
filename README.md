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

**Released: v1.1.0 (2026-10-25).** All 21 workspace crates are on crates.io at
1.1.0 (`cargo install tailrace-node --version 1.1.0 --locked`), with the
v1.1.0 git tag and a 9-asset GitHub release. It ships the engine data plane
(kafka / file / jdbc sources; Doris + StarRocks Stream Load and file sinks),
datasource discovery & schema inference (`discover` / `infer-schema`), the SSH
deploy chain, the performance ledger (docs/BENCHMARKS.md) and the 8-job CI
matrix.

**Unreleased, in this tree.** Four development rounds since v1.1.0 landed
the twelve capabilities listed in the [capability matrix](#capability-matrix-post-v110-unreleased)
below: `/metrics`, Debezium CDC decoding, an MCP server, broadcast dimension
joins, the DDL closed loop, deterministic backfill, partial-column update, a
16-word column type vocabulary, `TableObject` options, sink transaction hooks,
`sequence_col` DDL and workspace-wide `forbid(unsafe_code)`. **crates.io and
the binary releases are still 1.1.0 — the development line is ahead of the
published version.** The next version number is set at release time, not
here; the round-by-round account is
[docs/design/architecture.zh.md](docs/design/architecture.zh.md) §12, and
[CHANGELOG.md](CHANGELOG.md) is the release log.

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

## Capability matrix (post-v1.1.0, unreleased)

Everything below is in this source tree and covered by unit / contract tests
plus the listed e2e scripts, but **not yet published** to crates.io (see
Status). Anchors are `path:line` in this repository.

| Capability | What it does | Where (anchor) |
|---|---|---|
| **Prometheus `/metrics`** | 9-metric surface in hand-written text format, zero exporter crates; node renders it from live pipeline counters at scrape time, console adds `tailrace_console_db_write_seconds` | `engine/crates/tailrace-api/src/metrics.rs:9` (surface) `:235` (`node_metrics_text`); `console/crates/tailrace-console/src/metrics.rs:40` (`scrape`), route `console/crates/tailrace-console/src/lib.rs:189` |
| **Debezium CDC decoding** | `format = "debezium-json"` decoder: `c`/`r`/`u` events to rows, `d` and tombstones skipped, schemas-enabled wrapper unwrapped, malformed events to the DLQ. Narrow-path CDC: consume events, never capture binlogs | `engine/crates/tailrace-decode/src/lib.rs:950` (`DebeziumJsonDecoder`); dispatch `engine/crates/tailrace-runtime/src/lib.rs:1532` |
| **MCP server** | `POST /mcp` (JSON-RPC 2.0, Streamable HTTP): 14 tools — 8 read, 6 write. Write tools need **both** `--mcp-allow-write` / `MCP_ALLOW_WRITE=1` and `"confirm": true`; every call is audited as `MCP_TOOL_CALL`, `actor = mcp:<client>`. Tool descriptions are generated from the OpenAPI document — nothing to drift | `console/crates/tailrace-console/src/mcp.rs:90` (`TOOLS`), route `:177`; flag `console/crates/tailrace-console/src/main.rs:26` |
| **Broadcast dimension join** | `… JOIN ${ds:dim.<alias>} d ON …` loads the dim table full-table over JDBC at assembly into a static MemTable; 500 000-row / 64 MiB caps fail fast, unique key required, INNER / LEFT only (RIGHT / FULL / CROSS rejected at compile) | `engine/crates/tailrace-runtime/src/dim.rs:36` `:39` (caps); join-type gate `engine/crates/tailrace-parser/src/lib.rs:351-359` |
| **DDL closed loop** | draft → human-confirmed key model (required; 422 otherwise) → execute with full-text audit → evolve = `ADD COLUMN` only (Light Schema Change; retype / drop reported, never run). Pure generator: schema + hints in, DDL + "this is a guess" annotations out; 13 scalar vocabulary words mapped, unmapped types are an error, not a fold | `engine/crates/tailrace-ddl/src/lib.rs:169` (`generate_draft`) `:385` (`doris_type`); exec `engine/crates/tailrace-metadata/src/lib.rs:233` (`mysql_exec`); endpoints `console/crates/tailrace-console/src/api/datasources.rs:1887` `:2002` `:2162`; web wizard `web/src/api/datasources.ts` |
| **Deterministic backfill** | `POST /api/pipelines/{id}/backfill` with `dryRun` (zero side effects) → bounded run in a `bf-<job>` ledger namespace and its own consumer group; labels `prefix-stmt-<sha256(from,to)[..8]>_<seq>` so a replayed range dedups; orphaned jobs are failed at console start. Source gate today: `file` only (Kafka per-partition seeding is a follow-up) | `engine/crates/tailrace-conn-doris/src/lib.rs:153` (`range_fingerprint`) `:187` (`LabelMode`); `engine/crates/tailrace-runtime/src/lib.rs:773` (`run_bounded`) `:1566` (`backfill_state_dir`); `console/crates/tailrace-console/src/api/pipelines.rs:2319` (POST) `:2420` (jobs) `:2277-2289` (source gate) |
| **Partial-column update** | The switch is SQL: `INSERT INTO sink (k, a) SELECT …` — the INSERT column list declares the subset, a bare INSERT writes whole rows. Four gates: registered `key_model = unique` → live `SHOW CREATE TABLE` must be UNIQUE merge-on-write (FE unreachable = 502, not a guess) → no in-flight schema change (409) → sink capability + `sequence_column ∈ subset`. Stream Load sends `partial_columns:true` **and** a `columns` header (without it Doris silently NULLs the unlisted columns — live-probed) | parser `engine/crates/tailrace-parser/src/lib.rs:93-98` `:281` (`insert_column_list`); `engine/crates/tailrace-conn-doris/src/lib.rs:574` (`with_partial_columns`); gates `console/crates/tailrace-console/src/api/pipelines.rs:359` `:452` `:622` |
| **16-word column types** | `utf8 string int8 int16 int32 int64 float32 float64 decimal(p,s) timestamp(unit) date bool binary list<float32[,dim]> variant null`; parametric parse, canonical Display. Unknown words: warn + fold to `utf8` during a one-minor migration window, hard error after (`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW`) | `engine/crates/tailrace-ir/src/column_type.rs:30` (`ColumnType`) `:84` (`parse`) `:158` (window); re-exported as `tailrace_catalog::ColumnType` |
| **`TableObject` options** | 5 generic fields (`datasource / name / kind / format / columns`) + `options` map; connector fields read through typed accessors (`KafkaTableExt` / `DorisTableExt` / `JdbcTableExt` / `FileTableExt`); key names canonical in `option_keys`; legacy flat fields still read and written (serde double-write) | `engine/crates/tailrace-catalog/src/lib.rs:35` (`option_keys`) `:80` (`TableObject`) `:107` (`options`); accessors `tailrace-conn-kafka:31` `tailrace-conn-doris:256` `tailrace-conn-jdbc:80` `tailrace-conn-file:49` |
| **Sink transaction hooks + capabilities** | `prepare / commit / abort` on the `Sink` trait with no-op defaults (every existing sink unchanged) and `SinkCapabilities { idempotent_label, transactional, overwrite, partial_update, partition_aware }`; the runtime refuses `partial_columns` on a sink without `partial_update` — the flag is load-bearing, not decorative | `engine/crates/tailrace-connector/src/sink.rs:18` (`SinkCapabilities`) `:107` `:113` `:119` (hooks) `:126` (`capabilities`) |
| **`sequence_col` DDL property** | Under UNIQUE KEY a declared `sequence_column` is emitted as `"function_column.sequence_col"` — without it Doris silently ignores the engine's `sequence_column` Stream Load header (probed on 2.1.7 and 3.1.4) | `engine/crates/tailrace-ddl/src/lib.rs:327-329` |
| **Engineering governance** | `unsafe_code = "forbid"` for the whole workspace; `debug = "line-tables-only"` for dependencies and test builds (one build tree shrank to the 16 GiB range after a disk-full incident); `cargo deny` advisories paid down as they land | `Cargo.toml:55-56` (lints) `:71-72` `:84-85` (profiles); `deny.toml` |

e2e coverage for the rounds above, against real Doris (compose 2.1.7 and an
external 3.1.4): `scripts/e2e/ddl_create.sh` (48/48), `backfill_replay.sh`
(31/31), `partial_update.sh` (38/38 compose + 37/37 external),
`type_precision.sh` (decimal(18,2) precision, sequence_col ordering), plus two
dogfooding rounds (`scripts/dogfood/`, reports in `docs/dev/`). These counts
are the figures recorded at the time each round landed — not re-run for this
README.

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
AI clients can operate the console over `POST /mcp` (JSON-RPC 2.0): 14 tools
— list / inspect / compile / stats / logs / DDL draft / backfill status are
read-only and always on; create / deploy / pause-resume / backfill / DDL
execute / DDL evolve are write tools that need the console started with
`--mcp-allow-write` (or `MCP_ALLOW_WRITE=1`) **and** `"confirm": true` in the
call. Every call lands in the audit log as `MCP_TOOL_CALL` with
`actor = mcp:<client>`. Tool descriptions are generated from the same OpenAPI
document that serves `/swagger-ui`.

**Which column types can a table object declare?**
Sixteen words: `utf8 string int8 int16 int32 int64 float32 float64
decimal(p,s) timestamp(unit) date bool binary list<float32[,dim]> variant
null`. The four legacy words keep their meaning byte for byte. An unknown
word currently logs a warning and folds to `utf8` (one-minor migration
window); the next minor turns that into a hard error at pipeline assembly.
The DDL generator maps the 13 scalar words to Doris types; `variant` and
`list<float32>` are in the vocabulary but their Doris DDL mapping and
end-to-end write are later work.

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
- [docs/ASSESSMENT-AND-OUTLOOK.md](docs/ASSESSMENT-AND-OUTLOOK.md) — assessment & outlook (post-R8 baseline on top of the v1.1.0 snapshot; the public roadmap is its §五)
- [docs/design/architecture.zh.md](docs/design/architecture.zh.md) — architecture design, post-R8 (Chinese)
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
