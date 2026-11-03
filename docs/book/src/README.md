# 前言

Tailrace：单二进制、自带 Web 控制台的轻量数据接入引擎。SQL-first、无 JVM、无容器——在边缘把 Kafka、文件与数据库的数据接入 Doris / StarRocks，并用同样两个二进制把这条写路径的 Day-2 运维闭环（建表 DDL、确定性回填、部分列更新、Prometheus 指标、MCP 服务）一并跑起来（项目一句话，与 README.zh-CN.md 同源）。

**版本声明**：本站描述 post-R11 工作线实况。**v1.2.0 已发布**（CHANGELOG 版本切点 2026-10-31；tag 与 GitHub Release 9 资产于 2026-11-02 上线）——v1.1.0 后的六轮（扩展轮 / R6 / R7 / R8 / R9 / R10）能力随之出门：`/metrics`、Debezium CDC 解码、MCP 服务、维表广播 join、建表闭环、确定性回填、部分列更新、十六词列类型词汇、`TableObject` options 化、Sink 事务钩子、`sequence_col` DDL 属性、`forbid(unsafe_code)`，加 VARIANT 直写、向量列（`ARRAY<FLOAT>`）、管理台告警与 SLO 目标线、`Cursor::Custom`、管道即代码；**但 crates.io 现值仍为 1.1.0**（22 包 1.2.0 lockstep publish 未执行、`tailrace-ddl` 从未上架——2026-11-03 实查）。其后的 **R11（v1.3 开发线，Unreleased）**：idle 感知告警、DLQ 行键去重、MCP apply 对（16 工具）、SLO p50/p95/p99（1h 样本环）、未知类型词迁移窗关闭（1.3.0 BREAKING）。逐项代码锚点见仓库 README「能力矩阵」与本站[架构设计](architecture.md)章。

三命令快速开始（构建 → 启动管理台 → 启动引擎节点）见仓库 README「快速开始」：<https://github.com/SeekerGalahad/tailrace/blob/main/README.zh-CN.md#快速开始>

本站中文单轨（mdBook `language = "zh-CN"`）；英文入口为仓库 [README.md](https://github.com/SeekerGalahad/tailrace/blob/main/README.md)。

章节导览：[架构设计](architecture.md) → 协议（[引擎 /v1](protocol-engine.md)、[Console API](protocol-console.md)）→ 连接器（[JDBC 批源](connectors-jdbc.md)、[矩阵](connectors-matrix.md)——含解码格式与列类型词汇）→ [性能基准摘要](benchmarks.md) → [现状评估与未来展望](assessment.md) → [开发指针](development.md)。
