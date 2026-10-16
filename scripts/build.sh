#!/usr/bin/env bash
# 一键构建：前端 → 内嵌 → 双二进制 → 程序包
set -euo pipefail
cd "$(dirname "$0")/.."

# 1. 前端（Node 工具链仅此一步需要——CI 或开发者本机）
( cd web && pnpm install --frozen-lockfile && pnpm build )          # 产物 web/dist/

# 2. Rust：rust-embed 把 web/dist 嵌入 console 二进制（release 单文件分发）
#    Built with an explicit --target so package.sh's rebuild hits the cargo
#    cache instead of recompiling the whole dependency tree into a second
#    target dir (disk-constrained dev machines; artifacts land in
#    target/<triple>/release). The P0 gate expects target/release/, so the
#    two binaries are copied over — same bytes, canonical path preserved.
TARGET="$(rustc -vV | sed -n 's/^host: //p')"
cargo build --release --locked --target "$TARGET" -p tailrace-console -p tailrace-node
mkdir -p target/release
cp "target/$TARGET/release/tailrace-console" "target/$TARGET/release/tailrace-node" target/release/

# 3. 程序包（可选，交叉构建走 package.sh）
#    tailrace-console-<ver>-<target>.tar.gz  含 tailrace-console + VERSION
#    tailrace-node-<ver>-<target>.tar.gz     含 tailrace-node + nodectl.sh + VERSION（部署链包内约定）
./scripts/package.sh
