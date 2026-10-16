<!-- Thank you! Conventional Commits in the title keep the changelog automatable. -->

## What & why

<!-- One or two sentences: what changes and why. Link the issue if one exists. -->

## How it's tested

<!-- Which checks you ran locally, at minimum:
     cargo fmt --all --check
     cargo clippy --workspace --all-targets --locked -- -D warnings
     cargo test --workspace --locked
     ./scripts/smoke.sh (if console/node/deploy chain touched)
-->

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --locked -- -D warnings`
- [ ] `cargo test --workspace --locked`
- [ ] `./scripts/smoke.sh` still green (if applicable)

## Checklist

- [ ] New dependencies pass `cargo deny check advisories bans licenses sources`
- [ ] Protocol docs updated (`docs/protocol/engine-v1.md` / `console-api.md`) if any API surface changed
- [ ] No credentials, tokens, or secrets in the diff
