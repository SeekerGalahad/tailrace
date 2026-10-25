# tailrace

[![CI](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml/badge.svg)](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tailrace-node.svg)](https://crates.io/crates/tailrace-node)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)

A single-binary data ingestion engine with a built-in web console. SQL-first, no JVM, no containers — move data into Doris / StarRocks from the edge.

> No Temporal, no sidecars, no 8GB floor.

## Status

R3 complete (2026-10-22): engine data plane with kafka / file / jdbc sources
(MySQL/PostgreSQL keyset), Doris + StarRocks Stream Load sinks and a file
sink; datasource discovery & schema inference (`discover` / `infer-schema`);
a full performance ledger (docs/BENCHMARKS.md) and an 8-job CI matrix.
R4 (1.0 release) shipped 2026-10-23: all 21 workspace crates on crates.io
at 1.0.0 plus the v1.0.0 git tag / GitHub release. R5 (1.1) is on its
final step: the tree is versioned 1.1.0 and ships to crates.io
(`cargo install tailrace-node --version 1.1.0 --locked`); the v1.1.0
git tag / GitHub release follows (roadmap T38).

Sink delivery tiers, stated plainly (route review T18): **label idempotency
is delivered** — including restart-idempotency e2e (replaying a batch under
the same Stream Load label dedups; table row counts do not grow). The
transaction-interface (exactly-once) and group-commit throughput tiers are
**not in 1.0** — they sit on the v1.x agenda with documented triggers
(docs/dev/roadmap.md). mqtt/s3 connectors remain placeholders, also v1.x
with explicit trigger conditions.

## Quickstart

Prerequisites: Rust ≥ 1.95 (rustup; the repo pins a stable toolchain), Node.js 22, pnpm.

```bash
# 1. build the web bundle, embed it, and build both binaries
./scripts/build.sh

# 2. start the console (web UI on http://127.0.0.1:8080)
./target/release/tailrace-console --data-dir ./data
#    First boot prints the one-time admin password and the bootstrap token to
#    stderr — copy both now, they are not shown again.

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
docker compose).

## Connector matrix

| Kind | Source | Sink | Notes |
|---|---|---|---|
| kafka | ✅ | — | primary source, offset-based cursors |
| file | ✅ | — | local / mounted files |
| jdbc | ✅ | — | MySQL / PostgreSQL keyset batch source ([docs](docs/connectors/jdbc.md)) |
| doris | — | ✅ | Stream Load |
| starrocks | — | ✅ | Stream Load via the Doris sink with kind-gated StarRocks deltas (FE 307 redirect following, header set); e2e'd against `starrocks/allin1` (`docker compose --profile starrocks`) |
| mqtt | — | — | placeholder, not yet implemented (route decision pending, roadmap T18) |
| s3 | — | — | placeholder, not yet implemented (route decision pending, roadmap T18) |

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
| connector-trimmed `tailrace-node` (file+doris, no kafka) | release build | binary size **−9.4%** (61,571,936 B → 55,788,080 B) |
| dynamic Kafka partition discovery (2→4 partitions, no restart) | testcontainers Kafka | **26.8 s** (subscribe) / **30.7 s** (assign) |

## FAQ

**How does tailrace differ from Flink CDC / SeaTunnel-class platforms?**
They are JVM data-integration platforms; tailrace is a deliberately small
ingestion engine — one SQL statement per pipeline, at-least-once delivery
with sink-side label idempotency, two binaries, no cluster. Streaming joins,
watermarks and state backends are explicit non-goals (roadmap red lines).
Pick a platform when you need one; pick tailrace to feed Doris / StarRocks
from a small box at the edge.

**Why no JVM, and how is it a single binary?**
Engine and console are pure Rust. The web console is embedded in the console
binary (rust-embed); librdkafka is built in-tree (cmake) and openssl is
vendored. You ship two artifacts — `tailrace-console` (control plane + UI)
and `tailrace-node` (engine) — targeting 2–4 GB machines. A connector-trimmed
node build drops the whole kafka stack (`--no-default-features --features
connector-file,connector-doris`, −9.4% binary size; BENCHMARKS §六).

**What is the StarRocks story vs Doris?**
One Stream Load implementation: StarRocks reuses the Doris sink inside
`tailrace-conn-doris` with kind-gated deltas (FE 307 redirect following,
header set, `read_json_by_line` carrier, label idempotency, buffer-flush
thresholds). Verified end-to-end against `starrocks/allin1`
(`docker compose --profile starrocks`), including restart label dedup.

**When will mqtt / s3 connectors land?**
Not in 1.0. The route review (T18, 2026-10-21) parked both on v1.x with
explicit triggers: MQTT needs ≥3 real issues or a self-use scenario (PoC
first, 12-month window, otherwise dropped from the backlog); S3/CDC wait on
lakehouse prerequisites, with CDC preferring Debezium-format decoding. See
docs/dev/roadmap.md.

**Which platforms and Rust version are supported?**
MSRV is Rust 1.95 (workspace `rust-version`, enforced by the CI msrv job).
CI runs 8 jobs across ubuntu-latest and macos-latest (fmt/clippy/test, msrv,
feature-contract builds, web, chaos on ephemeral testcontainers, end-to-end
smoke). The release pipeline cross-builds linux x64/arm64 and macOS
x64/arm64 — two binaries per target.

## Documentation

- [CHANGELOG.md](CHANGELOG.md)
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — performance ledger (sole source of the numbers above)
- [docs/design/architecture.zh.md](docs/design/architecture.zh.md) — architecture design (Chinese)
- [docs/book/](docs/book/) — mdBook docs site skeleton (Chinese single-track; build locally with `mdbook build`)
- [docs/connectors/jdbc.md](docs/connectors/jdbc.md) — JDBC batch source
- [docs/dev/roadmap.md](docs/dev/roadmap.md) — roadmap and the v1.x agenda (delivery-tier triggers included)

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
