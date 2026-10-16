# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  gates with dependency caching, MSRV (1.85) and `--no-default-features`
  checks, `cargo deny` supply-chain gates (advisories/bans/licenses/sources),
  end-to-end smoke script (`scripts/smoke.sh`, 25 assertions), 4-target
  release matrix (linux amd64/arm64, macOS amd64/arm64) with aggregate
  `checksums.txt`, and community health files (CONTRIBUTING, SECURITY,
  CODE_OF_CONDUCT, issue/PR templates).

[Unreleased]: https://github.com/SeekerGalahad/tailrace/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/SeekerGalahad/tailrace/releases/tag/v0.1.0
