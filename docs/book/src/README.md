# 前言

Tailrace：单二进制、自带 Web 控制台的轻量数据接入引擎——用 SQL 把 Kafka、文件与数据库接入 Doris / StarRocks，无 JVM、无容器、无最低 8GB 门槛（项目一句话，与 README.zh-CN.md 同源）。

三命令快速开始（构建 → 启动管理台 → 启动引擎节点）见仓库 README「快速开始」：<https://github.com/SeekerGalahad/tailrace/blob/main/README.zh-CN.md#快速开始>

本站中文单轨（mdBook `language = "zh-CN"`）；英文入口为仓库 [README.md](https://github.com/SeekerGalahad/tailrace/blob/main/README.md)。

章节导览：[架构设计](architecture.md) → 协议（[引擎 /v1](protocol-engine.md)、[Console API](protocol-console.md)）→ 连接器（[JDBC 批源](connectors-jdbc.md)、[矩阵](connectors-matrix.md)）→ [性能基准摘要](benchmarks.md) → [开发指针](development.md)。
