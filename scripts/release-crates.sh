#!/usr/bin/env bash
# ============================================================================
# scripts/release-crates.sh — T27 crates.io full-tree release driver (21 pkgs)
# ============================================================================
# Publish topology (roadmap §九 T27; verified against this tree with
# `cargo metadata --no-deps` — every L(n) package depends only on L(<n)):
#   L0 ir / decode / transform / conn-mqtt / conn-s3 / metadata /
#      console-store / console-proxy          (zero internal deps)
#   L1 state / catalog / parser / auth
#   L2 connector
#   L3 conn-kafka / conn-file / conn-doris / conn-jdbc
#   L4 runtime
#   L5 api
#   L6 node / console
# tests-contract is excluded (publish = false).
#
# Modes:
#   --list                print the 21 package names in publish order
#   --dry-run             per package `cargo publish --dry-run`. A failure
#                         whose ONLY cause is unpublished internal deps
#                         ("no matching package named ..." — the expected
#                         state before the first real publish) falls back to
#                         `cargo package --list` (validates manifest,
#                         metadata and the packaged file set WITHOUT the
#                         registry resolve; note `--no-verify` still
#                         resolves and is NOT usable pre-publication) and is
#                         labeled "package" (full verify deferred to the
#                         publish wave, where deps resolve from the
#                         registry). Any other failure is a hard stop.
#                         No retries here: nothing is propagating yet.
#   --publish [--resume]  IRREVERSIBLE. Per package: dry-run gate with
#                         registry-propagation retry (<=10 tries x 15s,
#                         retrying only "no matching package named"), then
#                         the real publish. --resume queries the crates.io
#                         API per package and skips any whose max_version is
#                         already $VERSION (restart after a partial wave).
#
# Registry: every cargo call passes --registry crates-io — this machine
# replaces crates.io with the rsproxy-sparse mirror (~/.cargo/config.toml),
# and the flag is what routes uploads/queries to the real crates.io. Mirror
# lag for freshly published deps is what the retry loop absorbs.
# Token: cargo reads it from the environment (CARGO_REGISTRY_TOKEN, or
# CARGO_REGISTRIES_CRATES_IO_TOKEN). This script never reads, writes, or
# echoes any token or credential.
# Tree state: --dry-run passes --allow-dirty (the prep tree is deliberately
# uncommitted); --publish does NOT — cargo refuses a dirty tree, an
# intentional extra brake on an irreversible action. The console crate-local
# dist/ is gitignored, so materializing the SPA keeps the tree clean.
#
# Audit log: every attempt appends timestamped DRY/PUB/SKIP lines to
# scripts/release-log-<YYYYMMDD>.txt — kept in-tree but gitignored (audit
# trail survives on the release machine for the wave gate; stays out of VCS;
# /tmp was rejected because it does not survive reboots).
set -euo pipefail
cd "$(dirname "$0")/.."

export CARGO_PROFILE_DEV_DEBUG=0
VERSION="1.0.0"
REGISTRY="crates-io"
MAX_TRIES=10
RETRY_SLEEP=15
LOGFILE="scripts/release-log-$(date +%Y%m%d).txt"

LEVELS=(
  "tailrace-ir tailrace-decode tailrace-transform tailrace-conn-mqtt tailrace-conn-s3 tailrace-metadata console-store console-proxy"
  "tailrace-state tailrace-catalog tailrace-parser console-auth"
  "tailrace-connector"
  "tailrace-conn-kafka tailrace-conn-file tailrace-conn-doris tailrace-conn-jdbc"
  "tailrace-runtime"
  "tailrace-api"
  "tailrace-node tailrace-console"
)

log() { printf '%s %s\n' "$(date +%FT%T)" "$*" | tee -a "$LOGFILE"; }

all_packages() {
  local level pkg
  for level in "${LEVELS[@]}"; do
    for pkg in $level; do printf '%s\n' "$pkg"; done
  done
}

# Integrity: the wave acceptance counts 21 packages; refuse to run on a
# silently mutated topology.
[ "$(all_packages | wc -l | tr -d ' ')" -eq 21 ] || {
  echo "error: topology integrity check failed (expected 21 packages)" >&2
  exit 1
}

# --- console SPA self-containment (T27 ①) ---------------------------------
# rust-embed embeds the crate-local console/crates/tailrace-console/dist/
# (src/spa.rs; build.rs keeps it fresh in dev). cargo's packaging file walk
# happens BEFORE build.rs ever runs, so the driver materializes dist/ itself
# and refuses to ship the placeholder (no dist/assets => not a real SPA).
materialize_console_dist() {
  if [ ! -f web/dist/index.html ]; then
    command -v pnpm >/dev/null 2>&1 || {
      echo "error: web/dist missing and pnpm not found — run 'make web' first" >&2
      exit 1
    }
    ( cd web && pnpm install --frozen-lockfile && pnpm build )
  fi
  rsync -a --delete web/dist/ console/crates/tailrace-console/dist/
  [ -d console/crates/tailrace-console/dist/assets ] || {
    echo "error: SPA materialization failed (no dist/assets — placeholder must not ship)" >&2
    exit 1
  }
}

mode="${1:-}"
case "$mode" in
  --list)
    all_packages
    exit 0
    ;;
  --dry-run | --publish)
    shift
    ;;
  *)
    echo "usage: $0 --list | --dry-run | --publish [--resume]" >&2
    exit 2
    ;;
esac

resume=false
if [ "${1:-}" = "--resume" ]; then
  [ "$mode" = "--publish" ] || {
    echo "error: --resume only pairs with --publish" >&2
    exit 2
  }
  resume=true
  shift
fi
[ $# -eq 0 ] || { echo "error: unexpected argument(s): $*" >&2; exit 2; }

# ---------------------------------------------------------------------------
if [ "$mode" = "--dry-run" ]; then
  pass=0
  total=0
  for level in "${LEVELS[@]}"; do
    for pkg in $level; do
      total=$((total + 1))
      [ "$pkg" = "tailrace-console" ] && materialize_console_dist
      if out="$(cargo publish --dry-run --registry "$REGISTRY" --allow-dirty -p "$pkg" 2>&1)"; then
        log "DRY $pkg OK (verify)"
        pass=$((pass + 1))
        continue
      fi
      if grep -q "no matching package named" <<<"$out"; then
        # Expected pre-publish state: internal deps not on the registry yet.
        # Fall back to package-list validation (manifest/metadata/files,
        # no registry resolve).
        if cargo package --registry "$REGISTRY" --list --allow-dirty -p "$pkg" >/dev/null 2>&1; then
          log "DRY $pkg OK (package; verify deferred — internal deps not yet on registry)"
          pass=$((pass + 1))
          continue
        fi
        log "DRY $pkg FAIL (package stage)"
        printf '%s\n' "$out" >&2
        exit 1
      fi
      log "DRY $pkg FAIL (verify)"
      printf '%s\n' "$out" >&2
      exit 1
    done
  done
  log "dry-run summary: $pass/$total green (version $VERSION)"
  [ "$pass" -eq "$total" ] || exit 1
  exit 0
fi

# --- --publish (irreversible; wave-gate only) ------------------------------
if [ -z "${CARGO_REGISTRY_TOKEN:-}${CARGO_REGISTRIES_CRATES_IO_TOKEN:-}" ]; then
  echo "error: publish needs CARGO_REGISTRY_TOKEN (or CARGO_REGISTRIES_CRATES_IO_TOKEN)" >&2
  echo "       exported by the operator; this script never reads token files" >&2
  exit 1
fi

registry_max_version() {
  local api
  api="$(curl -sf -A "tailrace-release-script (T27)" "https://crates.io/api/v1/crates/$1" 2>/dev/null || true)"
  # grep exits 1 on no match (crate not yet published); with set -e inside $()
  # that would silently kill the caller — always succeed with possibly-empty output.
  printf '%s' "$api" | grep -o '"max_version": *"[^"]*"' | head -1 | sed -E 's/.*"max_version": *"([^"]*)".*/\1/' || true
}

for level in "${LEVELS[@]}"; do
  for pkg in $level; do
    [ "$pkg" = "tailrace-console" ] && materialize_console_dist

    if $resume; then
      max="$(registry_max_version "$pkg")"
      if [ "$max" = "$VERSION" ]; then
        log "SKIP $pkg (crates.io max_version already $VERSION)"
        continue
      fi
    fi

    # tailrace-console only: the tree is legitimately "dirty" with the dist/
    # that materialize_console_dist just wrote (cargo's dirty check counts even
    # gitignored files); every other package keeps the strict clean-tree rule.
    dirty_flag=""
    [ "$pkg" = "tailrace-console" ] && dirty_flag="--allow-dirty"
    # Per-package dry-run gate; retry ONLY on registry propagation lag.
    ok=0
    for try in $(seq 1 "$MAX_TRIES"); do
      if out="$(cargo publish --dry-run --registry "$REGISTRY" $dirty_flag -p "$pkg" 2>&1)"; then
        ok=1
        break
      fi
      if ! grep -q "no matching package named" <<<"$out"; then
        log "DRY $pkg FAIL (attempt $try — non-propagation error, not retried)"
        printf '%s\n' "$out" >&2
        exit 1
      fi
      log "DRY $pkg attempt $try/$MAX_TRIES failed on propagation — sleeping ${RETRY_SLEEP}s"
      sleep "$RETRY_SLEEP"
    done
    if [ "$ok" -ne 1 ]; then
      log "DRY $pkg FAIL (deps did not propagate after $MAX_TRIES x ${RETRY_SLEEP}s)"
      exit 1
    fi
    log "DRY $pkg OK"

    if out="$(cargo publish --registry "$REGISTRY" $dirty_flag -p "$pkg" 2>&1)"; then
      log "PUB $pkg OK ($VERSION)"
    else
      log "PUB $pkg FAIL — wave stopped; re-run with --resume after diagnosis"
      printf '%s\n' "$out" >&2
      exit 1
    fi
  done
done
log "publish wave complete: 21 packages at $VERSION"
