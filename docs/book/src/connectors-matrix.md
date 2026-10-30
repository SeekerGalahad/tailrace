# 连接器矩阵

> 矩阵各行与仓库 `README.zh-CN.md`「连接器矩阵」同源（post-R8 工作线实况；jdbc 行的文档链接改指本站 [JDBC 批源](connectors-jdbc.md) 章）。逐连接器文档页随各自连接器交付后逐篇开页——目前只有 JDBC 一篇（维表装载也在其 §四·五）。代码锚点为仓库内 `路径:行号`，与 README 能力矩阵同一批实读。

## 一、源 / 汇矩阵

| 类型 | 源 | 汇 | 说明 |
|---|---|---|---|
| kafka | ✅ | ✅ | 主力源，分区位点游标，动态分区发现；汇侧按行产出 JSON 对象到 topic——生产者会话内 `enable.idempotence`，broker 侧无去重 label（重放为 at-least-once），不做分区键路由（`engine/crates/tailrace-conn-kafka/src/lib.rs:48` / `:502`） |
| file | ✅ | ✅ | 本地/挂载文件；目前管理台回填唯一接受的源（字节位边界）；汇侧每次 flush 追加 jsonl 或滚出一个 parquet part 文件（`engine/crates/tailrace-conn-file/src/lib.rs:21` / `:374`）。两汇均不声明 `partial_update` / `transactional`；parser 汇白名单为 kafka / file / doris / starrocks（`engine/crates/tailrace-parser/src/lib.rs:159`） |
| jdbc | ✅ | — | MySQL / PostgreSQL keyset 批源（[JDBC 批源](connectors-jdbc.md)）；`tailrace-node` feature `connector-jdbc`，**默认不开** |
| dim | 仅 JOIN | — | JDBC 承载的**广播**维表（`kind = dim`，在 JOIN 中以 `${ds:dim.<别名>}` 引用）：装配期全表装载，50 万行 / 64 MiB 上限，必须有唯一键——不是流式 join。**同样要求 `connector-jdbc` feature**（runtime 把 `dim` 改写为 `jdbc` 后经注册表取源工厂，`engine/crates/tailrace-runtime/src/dim.rs:104-111`） |
| doris | — | ✅ | Stream Load；label 幂等（进程作用域 + 确定性）、部分列更新、sequence column |
| starrocks | — | ✅ | 经 Doris 汇走 Stream Load，按 kind 门控 StarRocks 差异（FE 307 重定向跟随、头部集）；已对 `starrocks/allin1` e2e（`docker compose --profile starrocks`）；部分列更新仅 Doris |
| mqtt | — | — | 占位，未实现（触发器制——[现状评估与未来展望](assessment.md) §五） |
| s3 | — | — | 占位，未实现（触发器制，同上） |

编译注册：`tailrace-node` default = kafka / file / doris（starrocks 随 doris crate feature）；jdbc 与 dim 需显式 `--features connector-jdbc`；mqtt / s3 为占位 crate 不注册（`engine/bin/tailrace-node/Cargo.toml` `[features]`、`src/main.rs:25-38` `build_registry`）。未编译的 kind 在 plan 装配时报「源/sink 类型未支持（本二进制未编译该连接器 feature）」。

## 二、源解码格式（表对象的 `format`）

派发臂在 `engine/crates/tailrace-runtime/src/lib.rs:1526` `build_decoder`，六个取值：

| `format` | 解码器 | 说明 |
|---|---|---|
| `ogg`（缺省） | `OggDecoder` | OGG JSON 信封，`op_type` 白名单下推 |
| `json` / `jsonl` | `JsonDecoder` | 每条消息 / 每行一个 JSON 对象（jdbc 源每行产出即此形态） |
| `csv` | `CsvDecoder` | |
| `parquet` | `ParquetDecoder` | |
| `debezium-json` | `DebeziumJsonDecoder` | **消费** Kafka 上已有的 Debezium CDC 事件：`c`/`r`/`u` 取 `after` 转行、`d` 与墓碑（空消息）跳过、`schemas.enable` 外壳（`schema`+`payload`）解包、畸形事件（非对象根、`after` 非对象、缺 `after` 键）进 DLQ。**窄口 CDC：只消费事件，不抓 binlog**（`engine/crates/tailrace-decode/src/lib.rs:950`） |

其它取值在装配期报 `decode format unsupported`。console 侧 kafka `infer-schema` 的显式格式门同样收 `debezium-json`（R8 A3，`console/crates/tailrace-console/src/api/datasources.rs:1200`）。

## 三、列类型词汇（表对象 `columns` 的类型串）

post-R8 为**十六词**线契约（`engine/crates/tailrace-ir/src/column_type.rs:30` `ColumnType`，经 `tailrace_catalog::ColumnType` re-export）——4 个旧词字节级兼容 + 12 个新词，小写精确拼写、`parse → Display` 往返即规范形：

| 词 | Arrow | Doris DDL（建表闭环 `tailrace-ddl` `doris_type`，`engine/crates/tailrace-ddl/src/lib.rs:385`） |
|---|---|---|
| `utf8`（旧） | Utf8 | VARCHAR(65533) |
| `string` | LargeUtf8 | STRING（不可作键列） |
| `int8` / `int16` / `int32` / `int64`（旧） | Int8…Int64 | TINYINT / SMALLINT / INT / BIGINT |
| `float32` / `float64`（旧） | Float32 / Float64 | FLOAT / DOUBLE |
| `decimal(p,s)` | Decimal128(p,s)，p ≤ 38、s ≤ p | DECIMAL(p,s) |
| `timestamp(unit)`，unit ∈ s/ms/us/ns | Timestamp(unit, 无时区) | DATETIME(6)（Doris 无时区，各 unit 同落） |
| `date` | Date32 | DATE |
| `bool`（旧） | Boolean | BOOLEAN |
| `binary` | Binary | STRING（base64 约定） |
| `list<float32[,dim]>` | List(Float32)，dim 仅声明注记 | **未映射**（A5 向量列的 Doris ARRAY 落地留后续轮） |
| `variant` | Utf8（原始 JSON 透传） | **未映射**（A4 VARIANT 直写留后续轮） |
| `null` | Null | **未映射** |

未知词：一个 minor 版本的迁移窗内 warn（列名 + 已知词表）并折为 `utf8`——与 G2 前 `_ => Utf8` 臂行为一致；窗口常量翻转后转硬错误、装配期拒绝（`column_type.rs:14-20`）。建表闭环对三个未映射词**显式报错不折叠**。`infer-schema` 对 SQL 后端的 DDL 列型按同一词表收窄（`engine/crates/tailrace-metadata/src/infer.rs:257` `ddl_arrow_type` / `:296` `pg_arrow_type`）。

## 四、表对象的 `options`（R8 G1）

表对象 = 5 个通用字段（`datasource` / `name` / `kind` / `format` / `columns`）+ `options` 映射；连接器专属键的拼写正典在 `engine/crates/tailrace-catalog/src/lib.rs:35` `option_keys`（`bootstrap` / `stream_load_url` / `auth_env` / `label_prefix` / `sequence_column` / `partial_columns`），各连接器经 typed accessor 读取（`KafkaTableExt` / `DorisTableExt` / `JdbcTableExt` / `FileTableExt`）。旧顶层扁平字段在一个 minor 版本的兼容窗内双写双读（`lib.rs:54` `LEGACY` 六键）。
