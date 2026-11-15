# 门禁纪律：命令不得带 `CARGO_PROFILE_DEV_DEBUG=0` 前缀（2026-11-14，R12 T-D）

> 一条给所有后续轮次（人与子代理）的机器门纪律，结论来自 R2 与 R11 两次实查 +
> 本轮（R12 W4）的定向复现。本文只记纪律与证据，不改历史草稿与历史台账。

## 一、纪律原文

**门禁（fmt / clippy / test / build）命令一律裸跑，不得加 `CARGO_PROFILE_DEV_DEBUG=0`
（或任何 `CARGO_PROFILE_*_DEBUG`）前缀。**

```bash
# 正确
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

# 禁止（会造成第二套指纹、第二棵 target 树）
CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked
```

## 二、根因：前缀改变指纹，同一 target/ 下长出两套完整工件

Cargo 把生效的 profile 设置计入 fingerprint 哈希。`CARGO_PROFILE_DEV_DEBUG=0` 是
对环境变量覆盖层的一次真实改写——同一个 `target/` 目录下会并存两套 fingerprint
与两套编译产物（`target/debug/.fingerprint/<crate>-<hashA>` 与 `<crate>-<hashB>`），
两套产物各自完整链接依赖树，磁盘占用近似翻倍。

- **R2 事故（2026-10-18 档）**：目标机 466GB 长期 99% 满，根因一即「有无混用该前缀
  导致 cargo 指纹分裂双树（target 曾 23GB）」；当时立的规矩是「统一前缀」。
- **R11 门禁失败定位**：门禁脚本带前缀、子代理自己的构建不带 → **两套指纹各建
  一套完整工件（~31GB × 2）**，六轮工作流的门禁脚本一直在重犯；`docs/ASSESSMENT-AND-OUTLOOK.md`
  记的磁盘峰值 40G+/轮即由此而来。**定案：R12 起门禁删除该前缀。**

## 三、为什么现在可以删：debuginfo 已由 Cargo.toml 管住

R6 把 debuginfo 策略写进了 profile 本身，环境变量覆盖不再必要，反而只剩副作用：

| 位置 | 内容 | 效果 |
|---|---|---|
| `Cargo.toml:71-72` | `[profile.dev.package."*"]` `debug = "line-tables-only"` | 依赖侧 rlib 瘦身（datafusion 等 147MB 级 rlib 收敛） |
| `Cargo.toml:84-85` | `[profile.test]` `debug = "line-tables-only"` | 测试扇出瘦身（~30 个测试二进制的链接体量） |

两段注释（`Cargo.toml:63-70` / `:74-83`）记的正是 R6 磁盘三连事故（链接期
ENOSPC，dev/test 双指纹 ~30+GiB）的修复。backtrace 仍保留 file:line；需要全变量
信息时用 `CARGO_PROFILE_TEST_DEBUG=2` **局部**覆盖——这是「单次调试」而非「门禁
常态」，不构成本纪律的例外。

## 四、本轮定向复现（2026-11-14，scratch crate，可复跑）

```bash
cargo new fp-demo --vcs none && cd fp-demo
cargo build -q                       # 裸跑
ls target/debug/.fingerprint | wc -l # → 1
CARGO_PROFILE_DEV_DEBUG=0 cargo build -q
ls target/debug/.fingerprint | wc -l # → 2（fp-demo-ba46578ba71a273a / fp-demo-f6e95f793f891893）
du -sh target                        # 1.1M → 1.6M（真实工作区按依赖树体量等比放大）
```

判据：**加了前缀后 fingerprint 目录数从 1 变 2**，且旧指纹工件未被清理——两套并存
就是「双树」在单 target 目录内的形态。本波门禁（fmt/clippy/test）已按裸跑执行。

## 五、已知仍带前缀的位置（如实登记，不在本纪律管辖）

| 位置 | 为什么保留 |
|---|---|
| `scripts/release-crates.sh:63` `export CARGO_PROFILE_DEV_DEBUG=0` | 发布脚本，不是门禁；发布窗内只有它一家在跑 cargo，无双写风险。若将来与门禁并发（CI 打包 + 本地门禁同 target），应改为显式 `--profile` 或直接删除 |
| `docs/BENCHMARKS.md:357/380/458/545/546/631`、`docs/dev/t33-golden-attribution.md` | **历史记录**：记录的是当时的实测命令与口径（「构建前缀」一行本身就是口径注记），改写会污染已发布数字的可复现性——保持原样 |
| `.zcode/workflow-drafts/` 历史草稿 | 已落盘的旧轮脚本不回溯修改（R12 立项明文）；新轮模板不带前缀 |

## 六、后继动作

1. 新轮工作流模板（含并发子代理的门禁段）以本文为唯一口径：`cargo` 裸跑；
2. 发现门禁命令带前缀的提交，评审按「双指纹风险」打回（不是风格问题）；
3. 磁盘仍吃紧时先删 `target/debug/incremental` 与旧 fingerprint 目录，不动
   已发布的 release 产物与证据目录。
