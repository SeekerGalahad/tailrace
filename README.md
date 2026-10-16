# tailrace

A single-binary data ingestion engine with a built-in web console. SQL-first, no JVM, no containers — move data into Doris / StarRocks from the edge.

> No Temporal, no sidecars, no 8GB floor.

## Status

v0.1.0 baseline (2026-10-13). Working: engine data plane in `engine/` (SQL
transforms, offset cursors, kafka/file sources, Doris Stream Load sink), plus
the management console (`console/` + `web/`). mqtt/jdbc/s3 connectors are
placeholders.

## Quickstart

Prerequisites: Rust ≥ 1.85 (rustup; the repo pins a stable toolchain), Node.js 22, pnpm.

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
pipeline. For a scripted end-to-end check (25 assertions, no external dependencies), run
`./scripts/smoke.sh`.

## Connector matrix

| Kind | Source | Sink | Notes |
|---|---|---|---|
| kafka | ✅ | — | primary source, offset-based cursors |
| file | ✅ | — | local / mounted files |
| doris | — | ✅ | Stream Load |
| starrocks | — | ✅ | P2 |
| jdbc | ✅ | — | P2 |
| mqtt | ✅ | — | P2 |
| s3 | ✅ | — | P2 |

## Development

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build (web/)
./scripts/dev.sh     # console (debug) + Vite dev server with /api proxy
```

License: Apache-2.0. 中文文档见 [README.zh-CN.md](README.zh-CN.md)。
