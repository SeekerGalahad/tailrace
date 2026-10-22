# tailrace

[![CI](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml/badge.svg)](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tailrace-node.svg)](https://crates.io/crates/tailrace-node)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)

单二进制、自带 Web 控制台的轻量数据接入引擎——用 SQL 把 Kafka、文件与数据库接入 Doris / StarRocks，无 JVM、无容器、无最低 8GB 门槛。

## 状态

R3 收官（2026-10-22）：数据面连接器 kafka / file / jdbc（MySQL/PostgreSQL
keyset 批源）为源，Doris + StarRocks Stream Load 与 file 为汇；数据源发现与
模式推断（discover / infer-schema）已落地；性能账本（docs/BENCHMARKS.md）与
8 job CI 矩阵齐备。R4（1.0 发布）收尾中：21 个 workspace crate 已全部在
crates.io 发至 1.0.0（`cargo install tailrace-node --version 1.0.0 --locked`），
v1.0.0 git tag / GitHub Release 为余下步骤（roadmap T31）。

汇端交付档位如实表述（路线评审 T18）：**label 幂等档已交付**——含重启幂等
e2e（同 label 重放去重，表行数不增长）；事务接口（exactly-once）与
group commit 吞吐档**不在 1.0**——列入 v1.x 议程、触发条件在档
（docs/dev/roadmap.md）。mqtt/s3 连接器仍为占位，同样排 v1.x、带显式触发条件。

## 快速开始

前置条件：Rust ≥ 1.95（rustup；仓库 pin 了 stable 工具链）、Node.js 22、pnpm。

```bash
# 1. 构建前端并内嵌，再构建双二进制
./scripts/build.sh

# 2. 启动管理台（Web UI 在 http://127.0.0.1:8080）
./target/release/tailrace-console --data-dir ./data
#    首次启动会在 stderr 打印一次性 admin 初始密码与 bootstrap token——
#    当场抄下，之后不再展示。

# 3. 启动引擎节点并向管理台自注册
./target/release/tailrace-node serve \
  --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 \
  --advertise 127.0.0.1:18100 \
  --bootstrap-token <首启打印的 token>
```

浏览器打开 http://127.0.0.1:8080，用 `admin` + 抄下的密码登录建管道；或跑
`./scripts/smoke.sh` 做脚本化端到端自检（默认 25 步断言、无外部依赖；加 `--full`
为 44 步——经 docker compose 走 Kafka→Doris）。

## 连接器矩阵

| 类型 | 源 | 汇 | 说明 |
|---|---|---|---|
| kafka | ✅ | — | 主力源，分区位点游标 |
| file | ✅ | — | 本地/挂载文件 |
| jdbc | ✅ | — | MySQL / PostgreSQL keyset 批源（[文档](docs/connectors/jdbc.md)） |
| doris | — | ✅ | Stream Load |
| starrocks | — | ✅ | 经 Doris 汇走 Stream Load，按 kind 门控 StarRocks 差异（FE 307 重定向跟随、头部集）；已对 `starrocks/allin1` e2e（`docker compose --profile starrocks`） |
| mqtt | — | — | 占位，未实现（路线裁决在 roadmap T18） |
| s3 | — | — | 占位，未实现（路线裁决在 roadmap T18） |

## 基准

下表数字均为开发机单机测量、除注明外为 debug 档构建——**跨机器不可比**，
debug 档数字亦不代表 release 构建性能（口径限定随账本同载）。数字唯一来源=
[docs/BENCHMARKS.md](docs/BENCHMARKS.md)（方法学、逐轮样本、机型账册）。

| 基准（除注明外 10 万行） | 机型 / 构建 | 结果 |
|---|---|---|
| file→file 全链 | Intel i7-9750H，debug | **21739 rows/s**（v0.1.0 基线，2026-10-17） |
| kafka(P=6)→file，4 轮 | Apple M4，debug | **中位 27861 rows/s**（23918 / 27678 / 28043 / 28474）——该 bench 在 M4 的首档；与 Intel 侧 24658 rows/s 跨机器不可比 |
| 连接器裁剪档 `tailrace-node`（file+doris，无 kafka） | release 构建 | 二进制体积 **−9.4%**（61,571,936 B → 55,788,080 B） |
| Kafka 动态分区发现（2→4 分区不重启） | testcontainers Kafka | **26.8 s**（subscribe）/ **30.7 s**（assign） |

## 常见问题（FAQ）

**与 Flink CDC / SeaTunnel 这类平台有何区别？**
它们是 JVM 数据集成平台；tailrace 是刻意做小的接入引擎——一条 SQL 一条
管道、至少一次投递 + 汇端 label 幂等、两个二进制、无集群。流式 join、水位线
与状态后端是明确的非目标（roadmap 红线）。需要完整平台就用平台；要在边缘
小盒子把数据喂进 Doris / StarRocks，选 tailrace。

**为什么无 JVM、怎么做单二进制？**
引擎与管理台全 Rust。Web 控制台内嵌在 console 二进制里（rust-embed）；
librdkafka 走 cmake 树内构建、openssl vendored。交付两个产物——
`tailrace-console`（控制面 + UI）与 `tailrace-node`（引擎）——目标 2–4 GB
内存的机器。连接器裁剪档 node 构建可整体去掉 kafka 栈
（`--no-default-features --features connector-file,connector-doris`，
二进制 −9.4%；BENCHMARKS §六）。

**StarRocks 与 Doris 通道是什么关系？**
一条 Stream Load 实现：StarRocks 复用 `tailrace-conn-doris` 内的 Doris 汇，
按 kind 门控差异（FE 307 重定向跟随、头部集、`read_json_by_line` 载体、
label 幂等、缓冲落盘阈值）。已对 `starrocks/allin1` 端到端验证
（`docker compose --profile starrocks`），含重启 label 去重。

**mqtt / s3 连接器什么时候有？**
不在 1.0。路线评审（T18，2026-10-21）两项均排 v1.x 且带显式触发器：MQTT 需
≥3 个真实 issue 或自用场景（先 PoC，12 个月窗口，未触发即从队列移除）；
S3/CDC 等 lakehouse 前置，CDC 优先走 Debezium 格式解码。见
docs/dev/roadmap.md。

**支持哪些平台与 Rust 版本？**
MSRV 为 Rust 1.95（workspace `rust-version`，CI msrv job 真实编译验证）。
CI 共 8 个 job，覆盖 ubuntu-latest 与 macos-latest（fmt/clippy/test、msrv、
feature 契约构建、web、testcontainers 临时栈 chaos、端到端 smoke）。发布
流水线交叉构建 linux x64/arm64 与 macOS x64/arm64——每目标两个二进制。

## 文档

- [CHANGELOG.md](CHANGELOG.md)
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md)——性能账本（上表数字唯一来源）
- [docs/design/architecture.zh.md](docs/design/architecture.zh.md)——架构设计
- [docs/book/](docs/book/)——mdBook 文档站骨架（中文单轨；本地 `mdbook build` 构建）
- [docs/connectors/jdbc.md](docs/connectors/jdbc.md)——JDBC 批源
- [docs/dev/roadmap.md](docs/dev/roadmap.md)——开发路线与 v1.x 议程（含交付档位触发条件）

## 开发

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build（web/）
./scripts/dev.sh     # console（debug）+ Vite 开发服务器（/api 代理）
```

## i18n

控制台界面与 API 错误文案目前为中文单轨。wire 侧已预留 `message_key`
字典化通道（响应字节级不变），多语言本体列入 v1.x 议程。

许可证：Apache-2.0。English readme: [README.md](README.md)。
