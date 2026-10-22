# tailrace

A single-binary data ingestion engine with a built-in web console. SQL-first, no JVM, no containers — move data into Doris / StarRocks from the edge.

> No Temporal, no sidecars, no 8GB floor.

## Status

R3 complete (2026-10-22): engine data plane with kafka / file / jdbc sources
(MySQL/PostgreSQL keyset), Doris + StarRocks Stream Load sinks with label
idempotency, and a file sink; datasource discovery & schema inference
(`discover` / `infer-schema`); a full performance ledger (docs/BENCHMARKS.md)
and an 8-job CI matrix. mqtt/s3 connectors remain placeholders (route decision
in docs/dev/roadmap.md T18). R4 (1.0 release preparation) is underway.

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

## Development

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build (web/)
./scripts/dev.sh     # console (debug) + Vite dev server with /api proxy
```

License: Apache-2.0. 中文文档见 [README.zh-CN.md](README.zh-CN.md)。
