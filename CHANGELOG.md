# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- JDBC batch source connector (`tailrace-conn-jdbc`, roadmap T13): MySQL /
  PostgreSQL keyset incremental polling — each poll runs
  `SELECT * FROM t [WHERE cursor > ?] ORDER BY cursor LIMIT n` and emits one
  JSON object per row (`kind=jdbc` pairs with `format=json`); the cursor
  (`Cursor::JdbcKey`) commits at flush points only (at-least-once). Driver:
  sqlx 0.9 subcrates, no TLS feature. Connector docs: `docs/connectors/jdbc.md`.
- Opt-in jemalloc allocator for `tailrace-node` (roadmap T20): `jemalloc`
  feature (default OFF) via tikv-jemallocator; the heartbeat `resources`
  payload gains `peakRssBytes` (Linux `/proc/self/status` VmHWM). A/B
  numbers and caveats: `docs/BENCHMARKS.md` §六.

### Changed

- README (en/zh) connector matrix now states reality: jdbc source landed
  (T13), starrocks is an alias sharing the Doris Stream Load path (full
  parity = roadmap T12), mqtt/s3 are placeholders; the scripted smoke check
  is quoted per caliber — 25 assertions (default, dependency-free) / 44
  with `--full` (Kafka→Doris via docker compose).
- Editor evaluation (roadmap T17, PoC branch `poc/codemirror6`, not merged):
  CodeMirror6 measures 883.22→140.45 kB gzip on the editor async chunk
  (−84.1%, T9-anchored); production migration scheduled as roadmap T22 (R4).
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
    sink alias (a source declared `jdbc_table` resolves to doris and gets a
    pointed hint instead of a bare "got doris"); the sink diagnostic list
    now includes starrocks, which the whitelist already accepted.

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

[Unreleased]: https://github.com/SeekerGalahad/tailrace/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/SeekerGalahad/tailrace/releases/tag/v0.1.0
