#!/usr/bin/env bash
# Package release tarballs (design doc §8.2 step 3):
#   target/package/tailrace-console-<ver>-<target>.tar.gz  (tailrace-console + VERSION)
#   target/package/tailrace-node-<ver>-<target>.tar.gz     (tailrace-node + VERSION; nodectl.sh ships with the deploy chain)
# Cross builds: set CARGO_BIN=cross and TARGETS="aarch64-unknown-linux-gnu ..." (release.yml does this).
set -euo pipefail
cd "$(dirname "$0")/.."

# rust-embed contract (§8.2): web/dist must exist before cargo builds.
if [ ! -f web/dist/index.html ]; then
  echo "error: web/dist/index.html missing — run ./scripts/build.sh first" >&2
  exit 1
fi

CARGO_BIN="${CARGO_BIN:-cargo}"
VERSION="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n1)"
: "${VERSION:?failed to read workspace version from Cargo.toml}"
DEFAULT_TARGET="$(rustc -vV | sed -n 's/^host: //p')"
# shellcheck disable=SC2206
TARGETS=(${TARGETS:-$DEFAULT_TARGET})

for target in "${TARGETS[@]}"; do
  "$CARGO_BIN" build --release --locked --target "$target" -p tailrace-console -p tailrace-node
  out="target/package"
  for name in tailrace-console tailrace-node; do
    dir="$out/$name-$VERSION-$target"
    rm -rf "$dir"
    mkdir -p "$dir"
    cp "target/$target/release/$name" "$dir/"
    printf '%s\n' "$VERSION" > "$dir/VERSION"
    tar -czf "$out/$name-$VERSION-$target.tar.gz" -C "$out" "$name-$VERSION-$target"
    echo "packaged $out/$name-$VERSION-$target.tar.gz"
  done
  # Per-target sha256 manifest (eng H2): a same-named checksums.txt written by
  # each matrix runner would overwrite its siblings — release.yml's aggregate
  # job downloads all tarballs and regenerates one checksums.txt over all 8.
  (cd "$out" && shasum -a 256 \
    "tailrace-console-$VERSION-$target.tar.gz" \
    "tailrace-node-$VERSION-$target.tar.gz" > "checksums-$target.txt")
  echo "checksums  $out/checksums-$target.txt"
done
