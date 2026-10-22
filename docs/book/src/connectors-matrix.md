# 连接器矩阵

> 占位章：矩阵各行与仓库 README.zh-CN.md「连接器矩阵」一致（jdbc 行的文档链接改指本站 [JDBC 批源](connectors-jdbc.md) 章）；逐连接器文档页随各自连接器交付后逐篇开页。

| 类型 | 源 | 汇 | 说明 |
|---|---|---|---|
| kafka | ✅ | — | 主力源，分区位点游标 |
| file | ✅ | — | 本地/挂载文件 |
| jdbc | ✅ | — | MySQL / PostgreSQL keyset 批源（[连接器文档](connectors-jdbc.md)） |
| doris | — | ✅ | Stream Load |
| starrocks | — | ✅ | 经 Doris 汇走 Stream Load，按 kind 门控 StarRocks 差异（FE 307 重定向跟随、头部集）；已对 `starrocks/allin1` e2e（`docker compose --profile starrocks`） |
| mqtt | — | — | 占位，未实现（路线裁决在 roadmap T18） |
| s3 | — | — | 占位，未实现（路线裁决在 roadmap T18） |
