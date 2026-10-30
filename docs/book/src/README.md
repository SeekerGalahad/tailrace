# 前言

Tailrace：单二进制、自带 Web 控制台的轻量数据接入引擎。SQL-first、无 JVM、无容器——在边缘把 Kafka、文件与数据库的数据接入 Doris / StarRocks，并用同样两个二进制把这条写路径的 Day-2 运维闭环（建表 DDL、确定性回填、部分列更新、Prometheus 指标、MCP 服务）一并跑起来（项目一句话，与 README.zh-CN.md 同源）。

**版本声明**：本站描述 post-R8 工作线实况——v1.1.0（2026-10-25，crates.io 21 包 + GitHub Release）之后四轮开发（扩展轮 / R6 / R7 / R8）落地的十二项能力（`/metrics`、Debezium CDC 解码、MCP 服务、维表广播 join、建表闭环、确定性回填、部分列更新、十六词列类型词汇、`TableObject` options 化、Sink 事务钩子、`sequence_col` DDL 属性、全 workspace `forbid(unsafe_code)`）**均在源码树中但尚未发包**，crates.io 与二进制 Release 现值仍为 1.1.0；下一个版本号在发布时定。逐项代码锚点见仓库 README「能力矩阵」与本站[架构设计](architecture.md)章。

三命令快速开始（构建 → 启动管理台 → 启动引擎节点）见仓库 README「快速开始」：<https://github.com/SeekerGalahad/tailrace/blob/main/README.zh-CN.md#快速开始>

本站中文单轨（mdBook `language = "zh-CN"`）；英文入口为仓库 [README.md](https://github.com/SeekerGalahad/tailrace/blob/main/README.md)。

章节导览：[架构设计](architecture.md) → 协议（[引擎 /v1](protocol-engine.md)、[Console API](protocol-console.md)）→ 连接器（[JDBC 批源](connectors-jdbc.md)、[矩阵](connectors-matrix.md)——含解码格式与列类型词汇）→ [性能基准摘要](benchmarks.md) → [现状评估与未来展望](assessment.md) → [开发指针](development.md)。
