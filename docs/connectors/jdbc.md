# JDBC 批源（kind: jdbc）

> 首篇连接器文档（roadmap §九 T23⑧）。实现：`engine/crates/tailrace-conn-jdbc`；
> 立项卡 roadmap T13（caa842e）+ R3-W1 评审修复（13d1c2b 唯一性探针 / 488efcc 游标指纹 /
> low#1 FLOAT4 游标臂）。本文只描述已落地行为，出处均在文内点名。

## 一、定位

MySQL / PostgreSQL 批源：keyset（seek）增量轮询，Flink JDBC-connector 形态。每轮 poll 执行

```sql
SELECT * FROM <table> [WHERE <cursor_col> > <last>] ORDER BY <cursor_col> ASC LIMIT <max_rows>
```

（首轮无下界：`SELECT * FROM <table> ORDER BY <cursor_col> ASC LIMIT <max_rows>`），
**每行产出一条 JSON 对象 chunk**——`kind=jdbc` 配 `format=json`，下游走标准
JsonDecoder、decode 策略与 Bad→DLQ 通道，与 kafka/file 源完全同构。

注册形态：`register_source("jdbc", …)`——**仅源，无汇**（parser 源白名单
kafka/file/jdbc；汇白名单 kafka/file/doris/starrocks）。注意 `jdbc_table` 是
**Doris 汇**的历史别名（catalog `normalize_kind` 归一化为 `doris`），与 `jdbc`
源是两个不同的 kind，别混用——写错会在编译诊断里收到明确提示。

## 二、连接参数（TableObject 映射）

| 字段 | 含义 | 约束 |
|---|---|---|
| `kind` | `jdbc` | 源专属；`jdbc_table` 是 Doris 汇别名 |
| `bootstrap` | 数据库 URL，scheme 选方言 | `mysql://`/`mariadb://` → MySQL；`postgres://`/`postgresql://` → PostgreSQL；其它 scheme 工厂即报错。形如 `mysql://user:pass@host:port/db`、`postgres://…` |
| `sequence_column` | 增量游标列 | 必填；PK 或单列 UNIQUE 时间列；NOT NULL；**自身唯一**（见 §四，连接探针强制） |
| `format` | 建议 `json` | 源产出即 JSON |
| `name` | 表名 | 标识符门：`[A-Za-z_][A-Za-z0-9_$]*` 点分段（`validate_ident`，防拼接 SQL 注入；`;DROP TABLE` 之类到不了 wire） |

驱动：sqlx 0.9 子 crate（core+mysql+postgres，`time`/`json`/`uuid` 类型 feature，
**不带 tls feature**）。引擎数据面保持同步：源自有专属 current-thread tokio
runtime，经 `block_on` 桥接（与 tailrace-node 信号线程同款）。

启用：`tailrace-node` feature `connector-jdbc`（**默认不开**，default =
kafka/file/doris）——`cargo build -p tailrace-node --features connector-jdbc`。

## 三、游标语义

- 游标形状：IR `Cursor::JdbcKey { key: <游标列名>, value: <已见最大值的规范串> }`。
- **仅在 flush 点提交**（至少一次锚点）：崩溃重放最后一个未 flush 窗口——重启后以
  已存值为排他下界续跑同一窗口查询，**不丢不重**（前提＝游标列自身唯一）。
- poll 内整批暂存：任一行转换失败即整轮返回错误，游标不动（失败轮不推进）。
- `open()` 游标 key 与当前 `sequence_column` 不匹配（改了游标列的编辑）→ warn 并视为
  无游标重新开始（kafka 位点重定位同款策略）。
- 游标值 canonical（回灌绑定的解析依据）：整数→十进制串；浮点→**最短往返 f64 串**
  （FLOAT4 先无损加宽到 f64 再格式化——`0.3f32` 直接格式化成 `"0.3"`，解析回的
  f64 低于加宽值 `0.30000001192092896…`，会让同一行每轮重匹配）；字符串→原样；
  DATETIME/TIMESTAMP→RFC3339（naive 按假设 UTC；tz 感知归一 UTC）；UUID→规范串。
- 绑定回灌按探针类型走精确类型绑定（PG `ts > $1::text` 无操作符，故必须类型化）；
  PG 无符号整数按有符号窄化（值来自整型列，无损）。

## 四、keyset 唯一性要求（连接期 fail-fast）

`WHERE col > ? ORDER BY col LIMIT n` **没有 tie-breaker**：非唯一列的并列值若跨
LIMIT 边界，剩余并列行会被下一窗口 `>` 永久跳过——**静默丢数**。因此连接探针
（`assert_unique_cursor`）经目录视图强制游标列**自身唯一**（PRIMARY KEY 或单列
UNIQUE 索引），不唯一即拒启：

- MySQL：`information_schema.STATISTICS`，`NON_UNIQUE = 0` 且无同索引其它键列
  （`NOT EXISTS` 反查——复合唯一索引 `UNIQUE (ts, id)` 的前导列**不算**唯一）；
  未限定库名时 `COALESCE(?, DATABASE())` 取连接默认库。
- PostgreSQL：`pg_index`，`indisunique` 且 `indpred IS NULL`（部分唯一索引对未
  覆盖行不成立）且 `indnkeyatts = 1`（INCLUDE 列不计）且该列即 `indkey[0]`；
  未加引号标识符 PG 折小写，探针按小写绑定。

探针通过后每轮 poll 若游标列出现 NULL → 整轮失败并提示「声明 NOT NULL 游标列」。

## 五、类型面（首版刻意裁剪）

**游标列可用类型**（`mysql_cursor_kind` / `pg_cursor_kind`）：

| 类别 | MySQL | PostgreSQL |
|---|---|---|
| 整数 | TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT（含 UNSIGNED） | INT2/INT4/INT8 |
| 浮点 | FLOAT/DOUBLE | FLOAT4/FLOAT8 |
| 字符串 | CHAR/VARCHAR/TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT/ENUM/SET | TEXT/VARCHAR/CHAR/NAME |
| 时间 | DATETIME（naive）/TIMESTAMP（tz） | TIMESTAMP/TIMESTAMPTZ |
| 其它 | — | UUID |

**数据列 → JSON**：BOOL→boolean；整数→number；FLOAT/DOUBLE→number
（NaN/Infinity 不可 JSON 表示→null）；字符串→string；二进制（BLOB 族/BYTEA）→
**小写 hex string 无前缀**；DATETIME/TIMESTAMP→RFC3339 串；DATE/TIME→字符串；
MySQL YEAR→number；JSON/JSONB→内嵌 JSON 对象；PG UUID→规范串。

**不支持**（poll 遇到即按列名报错）：MySQL DECIMAL/BIT/GEOMETRY；PG
numeric/数组/扩展类型。类型图收窄是首版有意为之（sqlx 需要 decimal feature 才
能取 DECIMAL，见 crate 文档 Limitations）。

## 六、TLS 边界与运维细节

- **TLS 关闭**：驱动选型刻意不带 rustls/native-tls/aws-lc（供应链面最小化）——
  适用内网/localhost；要求 TLS 的云库首版连不上。
- 连接错误（DB 未起等）延迟到 poll 退避重试；**配置错误**（缺 bootstrap/
  sequence_column、坏 scheme、坏标识符）工厂期即报。
- 空轮询在源内睡 ~250ms——runtime 50ms 空闲环打不到数据库（≤~4 qps）。
- 无 statement_timeout：查询由 `LIMIT max_rows` 界定，节奏由 runtime 控制。
- 占位符按方言：MySQL `?`，PG `$1`。

## 七、测试与现状边界

- 集成测试（`tests/jdbc_sources.rs`）：`TAILRACE_TESTCONTAINERS=1` 起
  mysql:8.0 / postgres:15（tag 钉住本地预热镜像，不拉取）——断言重启续读不丢不
  重、totals 单调、坏行走 `Decoded::Bad`→DLQ；无门控变量时跳过（conn-kafka
  live-gate 惯例）。CI chaos job 已纳入（19bb80b）。
- console 数据源类型当前只有 kafka/doris/file（`api/datasources.rs`
  `registry_type`）——jdbc 表对象目前经 FileCatalog / plan 通道下发；console 侧
  mysql/postgresql 类型与 `cursor_semantics=JDBC_KEY` 派生由 roadmap T14 补。
