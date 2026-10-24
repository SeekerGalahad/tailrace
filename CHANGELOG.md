# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
  synchronously and `upgrade` walks the full chain asynchronously; the 501
  placeholders in the console admin API are gone (non-SSH-managed nodes
  get 400, SSH-side failures 502 SSH_FAILED). Web nodes page wired;
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
  33568 = +23.7% — recorded as no-regression, not a confirmed gain).
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

[Unreleased]: https://github.com/SeekerGalahad/tailrace/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/SeekerGalahad/tailrace/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/SeekerGalahad/tailrace/compare/v0.1.0...v1.0.0
[0.1.0]: https://github.com/SeekerGalahad/tailrace/releases/tag/v0.1.0
