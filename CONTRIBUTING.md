# Contributing to tailrace

Thanks for your interest in contributing! tailrace is a single-binary data
ingestion engine with a built-in web console (Rust workspace in `engine/` +
`console/`, Vue 3 SPA in `web/`).

## Getting started

Start with the [Development](README.md#development) section of the README for
the toolchain prerequisites (Rust ≥ 1.85, Node.js 22, pnpm) and the everyday
commands:

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build (web/)
./scripts/dev.sh     # console (debug) + Vite dev server with /api proxy
./scripts/smoke.sh   # end-to-end smoke: 25 assertions, no external dependencies
```

## Before you open a PR

The CI gates (`.github/workflows/ci.yml`) are the same checks we run locally:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --workspace --locked`
- `cargo deny check advisories bans licenses sources`

Please make sure these pass for your change; `smoke.sh` should stay green if
you touch the console, node, or deploy chain.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`,
`fix:`, `docs:`, `chore:`, `refactor:` …). Release notes are generated from
them.

## Ground rules

- New dependencies must pass the license/advisory gates in `deny.toml`; check
  `cargo deny check licenses` before adding one.
- Keep the protocol docs (`docs/protocol/engine-v1.md`, `console-api.md`) in
  sync when you change the `/v1` or console API surface.
- Bug reports and feature requests: please use the issue templates.
