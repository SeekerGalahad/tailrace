# tailrace

[![CI](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml/badge.svg)](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tailrace-node.svg)](https://crates.io/crates/tailrace-node)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)

**tailrace — 单二进制数据接入引擎，把数据从 Kafka / 文件 / 数据库可靠地写进 Doris / StarRocks。**

不依赖 JVM，不需要容器编排，2-4 GB 内存的盒子就能跑。SQL-first：一条 `INSERT INTO 库.表 SELECT …` 就定义一个管道；内置 Web 控制台和 MCP Server，建表、部署、查延迟都在同一个二进制里。

## 版本状态

- **源码树 = v1.3.0**（2026-11-15 定版切点，23 个 workspace 成员 lockstep 继承、其中 22 个可发布；`tests-contract` 为 `publish = false`）。**v1.3.0 的 git tag、GitHub Release 与 crates.io 发包均未执行**——那是用户保留的放行决策；先定版、后发布（R5 先例）。
- **crates.io 与 GitHub Release 现值 = v1.2.0**（Release 9 资产 2026-11-02；22 包 2026-11-03 全部 1.2.0）。放行前 crates.io 上只有 1.2.0——它已在干净机器上实测可跑（`docs/dev/dogfooding-round5-2026-11-14.md`），所以下面快速开始里注明了回退写法。
- **v1.3.0 里有什么**（逐条锚点见 [CHANGELOG.md](CHANGELOG.md) `[1.3.0]` 段）：idle 感知告警 + 两拍 Clear、DLQ 行键去重、SLO lag p50/p95/p99、MCP 16 工具（8 读 + 8 写，含 `publish_pipeline`）、列类型词汇预检。其中**有一条 BREAKING**：未知列类型词自 1.3.0 起在管道装配期硬错，且本轮新增的预检让它在 **register / apply / DDL-draft / publish 就被 400 拒绝**（错误信封给出第几列、什么词、建议词表）——旧声明若含未知词，现在会在登记处直接报错并列出建议词，不再等到 deploy 才炸。

## 30 秒快速开始

```bash
# 1. 安装两个二进制（本树已按 1.3.0 定版；crates.io 发包紧随放行，
#    放行前改用 --version 1.2.0 —— 那两条实测可装）
cargo install tailrace-node --version 1.3.0 --locked
cargo install tailrace-console --version 1.3.0 --locked

# 2. 启动控制台（首次启动会打印一次性管理员密码，复制下来）
tailrace-console --data-dir ./data   # 浏览器打开 http://127.0.0.1:8080
#    用户名 admin，密码在终端日志里第一次打印，之后不会再显示
#    加 --mcp-allow-write 启用 MCP 写工具（建表/部署/回填等写操作）

# 3. 启动引擎节点，自动注册到控制台
tailrace-node serve \
  --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 \
  --advertise 127.0.0.1:18100 \
  --bootstrap-token <第 2 步打印的 token>
```

然后在 http://127.0.0.1:8080 登录，创建管道（文件源 → Doris 表），数据就能流起来。完整端到端断言请跑 `./scripts/smoke.sh`（25 断言，无外部依赖）或 `--full`（44 断言，Docker compose 起 Kafka+Doris）。

## 什么时候选 tailrace / 什么时候不用

| 选 tailrace | 不选 tailrace |
|---|---|
| 边缘/小集群（2-4 GB 内存的单机或小集群） | 跨地域大规模集群（几十上百节点的舰队） |
| 写入 Doris / StarRocks，关心 label 幂等、部分列更新、确定性回填 | 主要路数是 Kafka→S3、Elasticsearch、ClickHouse 等非 Doris/StarRocks 目标 |
| 想要内置 Web 管理台 + MCP 让 AI 客户端操作管道 | 只需要纯 CLI，不需要图形界面或 AI 客户端集成 |
| SQL-first：一条 `INSERT INTO … SELECT …` 就能定义管道 | 偏好私有 DSL 或图形化拖拽搭建管道 |
| 不需要真正的流式 join、水位线、状态后端 | 需要多源流式关联、窗口聚合、状态存储 |

## 能力概览（源码树 = v1.3.0）

| 能力 | 说明 |
|---|---|
| **建表闭环** | 推断 schema，生成 DDL 草稿，人工确认键模型后执行；`ADD COLUMN` 演进 |
| **回填** | `POST /backfill`，确定性 label（区间指纹），同区间重放行数不变；dryRun 预览 |
| **部分列更新** | `INSERT INTO 库.表 (列 A, 列 B) SELECT …` 只写声明列，其余保留旧值（Doris UNIQUE KEY + merge-on-write 表） |
| **广播维表 join** | JDBC 全表加载为静态内存表，50 万行 / 64 MiB 上限，唯一键必检，INNER/LEFT JOIN |
| **观测与告警** | Prometheus `/metrics`（手写 text format，node 与 console 两面；v1.3.0 起 node 面含 `tailrace_pipeline_source_idle` idle gauge），告警规则（lag / 故障 / 丢失 / 脉冲），SLO 分位（1h 环），idle 感知 hold / 两拍 Clear，Grafana 面板 |
| **MCP 控制面** | 16 个工具（8 读 + 8 写；1.2.0 为 14 = 9 读 + 5 写），读写双门禁（`--mcp-allow-write` + `confirm: true`），全部审计 |
| **v1.3 新增**（1.3.0 定版，待用户放行发布） | 列类型词汇预检（register/apply/ddl-draft/publish 四处 400，含第几列+建议词表）；idle 两拍 Clear（排干后告警自行关闭）；MCP `publish_pipeline`（纯 MCP GitOps 闭环）；DLQ 行键去重（指标仍计尝试次数） |

**明确不做**：真正的流式 join、水位线、状态后端；私有 DSL；连接器数量竞赛。SQL 就是 DSL。

## 快速参考：三个最常用命令

```bash
# 部署一个管道（控制台或 MCP）
tailrace-console --data-dir ./data --mcp-allow-write

# 启动引擎节点
tailrace-node serve --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 \
  --advertise 127.0.0.1:18100 \
  --bootstrap-token <token>

# 抓取指标（Prometheus 兼容格式）
curl http://127.0.0.1:8080/metrics | grep tailrace_pipeline_lag_seconds
```

## 文档与社区

- [CHANGELOG.md](CHANGELOG.md) — 发布记录
- [docs/ASSESSMENT-AND-OUTLOOK.md](docs/ASSESSMENT-AND-OUTLOOK.md) — 项目评估与路线图
- [docs/book/](docs/book/) — mdBook 文档站（中文，`mdbook build` 即可本地预览）
- [docs/protocol/console-api.md](docs/protocol/console-api.md) — 控制台 HTTP API
- [docs/protocol/engine-v1.md](docs/protocol/engine-v1.md) — 引擎 `/v1` 协议
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — 性能基准（同机环境下的基准数，不做跨机对比）
- Issues / Discussions — Bug 报告、功能建议、使用问题

License: Apache-2.0。中文文档见 [README.zh-CN.md](README.zh-CN.md)。