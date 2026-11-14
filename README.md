# tailrace

[![CI](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml/badge.svg)](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tailrace-node.svg)](https://crates.io/crates/tailrace-node)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)

**tailrace — 单二进制数据接入引擎，把数据从 Kafka / 文件 / 数据库可靠地写进 Doris / StarRocks。**

不依赖 JVM，不需要容器编排，2-4 GB 内存的盒子就能跑。SQL-first：一条 `INSERT INTO 库.表 SELECT …` 就定义一个管道；内置 Web 控制台和 MCP Server，建表、部署、查延迟都在同一个二进制里。

## 30 秒快速开始

```bash
# 1. 安装两个二进制（crates.io 已发布）
cargo install tailrace-node --version 1.2.0 --locked
cargo install tailrace-console --version 1.2.0 --locked

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

## 能力概览（v1.2.0）

| 能力 | 说明 |
|---|---|
| **建表闭环** | 推断 schema，生成 DDL 草稿，人工确认键模型后执行；`ADD COLUMN` 演进 |
| **回填** | `POST /backfill`，确定性 label（区间指纹），同区间重放行数不变；dryRun 预览 |
| **部分列更新** | `INSERT INTO 库.表 (列 A, 列 B) SELECT …` 只写声明列，其余保留旧值（Doris UNIQUE KEY + merge-on-write 表） |
| **广播维表 join** | JDBC 全表加载为静态内存表，50 万行 / 64 MiB 上限，唯一键必检，INNER/LEFT JOIN |
| **观测与告警** | Prometheus `/metrics`（9 指标），告警规则（lag / 故障 / 丢失 / 脉冲），Grafana 面板 |
| **MCP 控制面** | 10 个工具（6 读 + 4 写），读写双门禁（`--mcp-allow-write` + `confirm: true`），全部审计 |

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