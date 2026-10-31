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
| `list<float32[,dim]>` | List(Float32)，dim 走字段元数据（`tailrace.vector_dim`，decoder 侧维度守卫的依据） | `ARRAY<FLOAT>`（A5，DESC 回读折为 `array<float>` → `list<float32>`，dim 不在类型里——evolve 对 `list<float32,N>` 不判改型）；带 dim 时附列 COMMENT `'vector dim=N'`；dim 上限 65536 |
| `variant` | Utf8（原始 JSON 透传） | `VARIANT`（A4，DESC 回读 `variant`，2.1.7/3.1.4 双版本探针实测） |
| `null` | Null | **未映射**（显式报错不折叠） |

未知词：一个 minor 版本的迁移窗内 warn（列名 + 已知词表）并折为 `utf8`——与 G2 前 `_ => Utf8` 臂行为一致；窗口常量翻转后转硬错误、装配期拒绝（`column_type.rs:14-20`）。建表闭环对唯一未映射词 `null` **显式报错不折叠**。`infer-schema` 对 SQL 后端的 DDL 列型按同一词表收窄（`engine/crates/tailrace-metadata/src/infer.rs:257` `ddl_arrow_type` / `:296` `pg_arrow_type`；`array<float>` 折回 `list<float32>`，dim 对 DESC 不可见）。

### 3.1 VARIANT 与向量列的读回语义（探针实测，如实入档）

VARIANT 列写入时引擎把**整段原始 JSON 子文档**序列化为字符串直写（不解析叶子），Doris 服务端再把内容解析为 variant 结构。由此产生的服务端行为（探针证据 `scripts/e2e/evidence/variant-probe-2026-10-30/`，2.1.7 与 3.1.4 双版本）：

- **根级 JSON 字符串值不保真**：引擎对字符串单元格原样（不加引号）写入，服务端会把其内容**再当 JSON 解析一遍**——空串与字串 `null` 落成 `{}`、数字字串落成数值、布尔字串落成 `1`。对象内部的字符串叶**不受影响**。这是为 Debezium 把 MySQL JSON 列编码成字符串的场景让路的显式取舍（该场景下「字符串→再解析」恰是期望语义）；若上游根级值本身是字符串载荷，勿用 variant 列承载。
- **点键（子列）的 raw 回显不可信**：3.1.4 上 `SELECT col['key']` 的 raw 显示与写入形态有出入，以结构比对为准；数组 raw 回显在 2.1.7 与 3.1.4 之间有版本差。
- **向量列的 dim 只是登记侧守卫**：decoder 在入口按声明 dim 校验行（长度不符、非数值元素、或 |v| 超过 f32::MAX 的元素 → DLQ），Doris 自身不持有数组维度——5 元素数组能装进 dim=3 列（探针 B9）；维度契约由引擎侧守卫执行，`ARRAY<FLOAT>` 的列 COMMENT 仅为读 SHOW CREATE TABLE 的人记录该契约。

## 四、表对象的 `options`（R8 G1）

表对象 = 5 个通用字段（`datasource` / `name` / `kind` / `format` / `columns`）+ `options` 映射；连接器专属键的拼写正典在 `engine/crates/tailrace-catalog/src/lib.rs:35` `option_keys`（`bootstrap` / `stream_load_url` / `auth_env` / `label_prefix` / `sequence_column` / `partial_columns`），各连接器经 typed accessor 读取（`KafkaTableExt` / `DorisTableExt` / `JdbcTableExt` / `FileTableExt`）。旧顶层扁平字段在一个 minor 版本的兼容窗内双写双读（`lib.rs:54` `LEGACY` 六键）。
