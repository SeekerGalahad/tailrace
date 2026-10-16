#!/usr/bin/env bash
# Local dev loop (design doc §8.1): console in debug mode — rust-embed reads
# web/dist from disk on every request in debug builds, so the SPA can be
# refreshed without rebuilding the binary. Vite dev server proxies /api to it.
#   console: http://localhost:8080    vite: http://localhost:5173
set -euo pipefail
cd "$(dirname "$0")/.."

# rust-embed contract (§8.2): web/dist must exist before cargo builds.
if [ ! -f web/dist/index.html ]; then
  ( cd web && pnpm install && pnpm build )
fi

cargo run -p tailrace-console &
console_pid=$!
trap 'kill "$console_pid" 2>/dev/null || true' EXIT

( cd web && pnpm dev )
