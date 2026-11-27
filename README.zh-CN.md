# tailrace

[![CI](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml/badge.svg)](https://github.com/SeekerGalahad/tailrace/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tailrace-node.svg)](https://crates.io/crates/tailrace-node)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)

单二进制、自带 Web 控制台的轻量数据接入引擎。SQL-first、无 JVM、无容器——在边缘
把 Kafka、文件与数据库的数据接入 Doris / StarRocks，并用同样两个二进制把这条写路径
的 Day-2 运维闭环（建表 DDL、确定性回填、部分列更新、Prometheus 指标、MCP 服务）
一并跑起来。

> 无 Temporal、无 sidecar、无最低 8GB 门槛。

## 状态

**当前发行版：v1.3.0**——CHANGELOG 版本切点 2026-11-15；v1.3.0 git tag、GitHub
Release（9 资产 = 8 tarball + checksums.txt）与 crates.io 发包（22 个可发布 crate
全部 1.3.0，lockstep）已于 **2026-11-15** 上线。两条渠道任选安装：

```bash
cargo install tailrace-node --version 1.3.0 --locked    # crates.io
cargo install tailrace-console --version 1.3.0 --locked
# 或从 https://github.com/SeekerGalahad/tailrace/releases/tag/v1.3.0 取 tarball
```

v1.3.0 的源码树有 23 个 workspace 成员（22 个可发布，`tests-contract` 为
`publish = false`）。上一版 v1.2.0（2026-11-02）仍可安装；两版行为差异见
`[1.3.0]` 段的 BREAKING 与升级指南。

随版交付：引擎数据面（kafka / file / jdbc 为源，Doris + StarRocks Stream Load
与 file 为汇）、数据源发现与模式推断（`discover` / `infer-schema`）、SSH 部署链、
性能账本（docs/BENCHMARKS.md）、8 job CI 矩阵与下方全部能力——叠在 v1.0.0
（2026-10-23）、v1.1.0（2026-10-25）与 v1.2.0（2026-11-02）之上。

**v1.1.0 之后**六轮开发随 v1.2.0 发布：`/metrics`、Debezium CDC 解码、MCP 服务、
维表广播 join、建表闭环、确定性回填、部分列更新、十六词列类型词汇、`TableObject`
options 化、Sink 事务钩子、`sequence_col` DDL 属性、全 workspace
`forbid(unsafe_code)`——以及发布线上的 VARIANT 直写、向量列（`ARRAY<FLOAT>`）、
管理台告警与 SLO 暴露、开放的 `Cursor::Custom` 变体、管道即代码
（`POST /api/apply` + CLI）。**1.2.0 之后，v1.3 线（R11 + R12）**：idle 感知告警
（运行中但无数据的管道不再反复触发 lag 告警；R12 两拍 idle 让排干后已 firing 的
episode 自行关闭）、DLQ 行键去重（指标仍计尝试次数）、MCP 工具扩到 16 并补上
`publish_pipeline`（纯 MCP GitOps 闭环，替换调试型 `get_pipeline_logs`）、SLO lag
p50/p95/p99（1h 样本环）、**列类型词汇预检**（register/apply/ddl-draft/publish 四处
400，含第几列+建议词表），以及 **1.3.0 BREAKING**——未知列类型词迁移窗关闭
（warn+折叠改装配期硬报错）。逐轮账见
[docs/design/architecture.zh.md](docs/design/architecture.zh.md) §十二，
[CHANGELOG.md](CHANGELOG.md) 为发布日志（`[1.3.0]` 段含 BREAKING 与升级指南）。

汇端交付档位如实表述：**label 幂等档已交付**，分两种模式。常驻管道用*进程作用域*
label——同进程内重试复用同一 label，Doris 去重；跨重启的不重复来自已提交游标加表的
键模型，不来自 label。回填用*确定性* label——同一区间在任何进程映射到同一组 label，
Doris 回 `label already exists`，汇记为 `Dedup`（e2e：DUPLICATE KEY 表上同区间重放
两次，行数不变）。事务接口（exactly-once）与 group commit 吞吐档**未交付**：`Sink`
trait 现已带 `prepare / commit / abort` 钩子（默认 no-op）与 `SinkCapabilities`
能力声明作为扩展点，但尚无生产 sink 声明 `transactional`。mqtt / s3 连接器仍为单行
占位。

## 快速开始

两个已发布的二进制。console 已内嵌 Web UI，不需要 Node 工具链，也不用克隆
源码；首次 `cargo install` 在桌面机上约需 10–17 分钟编译（2026-11-14 的一次
新用户实测：1.2.0 在 Apple M4 上 node 10m12s、console 6m29s），且需要 Rust
工具链——Release tarball 两者都不需要。

```bash
# 1. 安装两个二进制
cargo install tailrace-node --version 1.3.0 --locked
cargo install tailrace-console --version 1.3.0 --locked

# 2. 启动管理台（Web UI 在 http://127.0.0.1:8080）
tailrace-console --data-dir ./data
#    首次启动会在 stderr 打印一次性 admin 初始密码与 bootstrap token——
#    当场抄下，之后不再展示。
#    加 --mcp-allow-write 开启 MCP 写工具（默认只读）。

# 3. 启动引擎节点并向管理台自注册
tailrace-node serve \
  --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 \
  --advertise 127.0.0.1:18100 \
  --bootstrap-token <首启打印的 token>
```

浏览器打开 http://127.0.0.1:8080，用 `admin` + 抄下的密码登录，登记数据源与
表对象，再建管道。有三件事 UI 目前不会告诉你——来自一次逐字照做本节的
新用户实测（`docs/dev/dogfooding-round5-2026-11-14.md`）：

- **Doris / StarRocks 汇的凭据要放在 node 的环境变量里。** 凭据绝不进 plan
  文件：汇的数据源是 Doris 或 StarRocks **且**存了密码时，console 只把变量名
  `TAILRACE_DS_<数据源id>` 下发给引擎，node 用时现读。启动 node 时把它设成
  HTTP Basic 凭据，例如数据源 id 为 1 时
  `TAILRACE_DS_1="Basic $(printf 'user:password' | base64)"`——否则部署会以
  `ENGINE_REJECTED` 失败（"environment variable TAILRACE_DS_1 not set"）。SSH
  部署链也不会注入，需要在 node 自己的环境里设好。
- **`file` 数据源的表对象要手工登记。** 控制台的「发现资产」对 file 数据源
  不可用（返回 400 并提示直接登记表对象），UI 也还没有手工登记表单。在它有
  之前，调用 `POST /api/datasources/{id}/tables`，请求体形如
  `{"dbName":"local","tableName":"orders_in","format":"jsonl","columnsJson":[["order_no","utf8"],["amt","decimal(18,2)"]]}`。
  file 表的表名就是数据源目录下的文件名——引擎打开的是 `<数据源 path>/<表名>`，
  所以 `orders_in` 指一个名字就叫 `orders_in` 的文件，而不是 `orders.jsonl`。
- **默认二进制不含 JDBC 源与维表 join。** 已发布的 `tailrace-node` 与 Release
  tarball 只编入 kafka、file、doris 三个连接器；`connector-jdbc` 是需显式打开
  的 Cargo feature（`cargo build -p tailrace-node --features connector-jdbc`，
  见 [docs/connectors/jdbc.md](docs/connectors/jdbc.md)）。

告警规则、SLO 历史、管道即代码（`apply` / `dry-run`）与 DLQ 查看目前没有
Web 页面，只能走 REST（[docs/protocol/console-api.md](docs/protocol/console-api.md)）。
想从源码树做脚本化端到端自检，跑 `./scripts/smoke.sh`（默认 25 步断言、无外部
依赖；加 `--full` 为 44 步——经 docker compose 走 Kafka→Doris）。Prometheus 抓
两个二进制各自的 `/metrics`；AI 客户端走管理台的 `POST /mcp`。

## 连接器矩阵

| 类型 | 源 | 汇 | 说明 |
|---|---|---|---|
| kafka | ✅ | ✅ | 主力源，分区位点游标，动态分区发现；汇侧按行产出 JSON 对象到 topic——生产者会话内 `enable.idempotence`，broker 侧无去重 label（重放为 at-least-once），不做分区键路由（`engine/crates/tailrace-conn-kafka/src/lib.rs:48` / `:502`） |
| file | ✅ | ✅ | 本地/挂载文件；目前管理台回填唯一接受的源（字节位边界）；汇侧每次 flush 追加 jsonl 或滚出一个 parquet part 文件（`engine/crates/tailrace-conn-file/src/lib.rs:21` / `:374`）。两汇均不声明 `partial_update` / `transactional`；parser 汇白名单为 kafka / file / doris / starrocks（`engine/crates/tailrace-parser/src/lib.rs:159`） |
| jdbc | ✅ | — | MySQL / PostgreSQL keyset 批源；**默认二进制不含**，需显式打开 `connector-jdbc` feature（[文档](docs/connectors/jdbc.md)） |
| dim | 仅 JOIN | — | JDBC 承载的**广播**维表（`kind = dim`，在 JOIN 中以 `${ds:dim.<数据源id>.<表>} [AS] <别名>` 引用——填数字数据源 id 而不是名称；同 jdbc 需 `connector-jdbc`）：装配期全表装载，50 万行 / 64 MiB 上限，必须有唯一键——不是流式 join |
| doris | — | ✅ | Stream Load；label 幂等（进程作用域 + 确定性）、部分列更新、sequence column |
| starrocks | — | ✅ | 经 Doris 汇走 Stream Load，按 kind 门控 StarRocks 差异（FE 307 重定向跟随、头部集）；已对 `starrocks/allin1` e2e（`docker compose --profile starrocks`）；部分列更新仅 Doris |
| mqtt | — | — | 占位，未实现（触发器制——docs/ASSESSMENT-AND-OUTLOOK.md §五） |
| s3 | — | — | 占位，未实现（触发器制，同上） |

源解码格式（表对象的 `format`）：`ogg`（默认）、`json` / `jsonl`、`csv`、`parquet`、
`debezium-json`——消费 Kafka 上已有的 Debezium CDC 事件；tailrace 只消费事件，不读
binlog。

## 能力矩阵（v1.2.0 + v1.3.0）

下表全部为 v1.0.0 之后的能力，有单测/契约测试与所列 e2e 脚本覆盖。
标 **[1.2.0]** 的行随 v1.2.0 发布（tag 2026-11-02）；标 **[1.3.0]** 的行随 v1.3.0
发布（tag 2026-11-15）。锚点为本仓库内的 `路径:行号`（按 v1.3.0 tag 的树）。

| 能力 | 做什么 | 代码位置（锚点） |
|---|---|---|
| **Prometheus `/metrics`** [1.2.0] | 10 指标面、手写 text format、零 exporter 依赖（1.2.0 为 9 项，第 10 项 idle gauge 见下）；node 端在抓取时从活管道计数器现算渲染，console 端另加 `tailrace_console_db_write_seconds`、SLO 目标线与 R11 起的 lag 分位数行 | `engine/crates/tailrace-api/src/metrics.rs:9`（指标面）`:246`（`node_metrics_text`）；`console/crates/tailrace-console/src/metrics.rs:160`（`scrape`），路由 `console/crates/tailrace-console/src/lib.rs:208` |
| **Debezium CDC 解码** [1.2.0] | `format = "debezium-json"` 解码器：`c`/`r`/`u` 事件转行、`d` 与墓碑跳过、schemas-enabled 外壳解包、畸形事件进 DLQ。窄口 CDC：只消费事件，不抓 binlog | `engine/crates/tailrace-decode/src/lib.rs:1204`（`DebeziumJsonDecoder`）；派发 `engine/crates/tailrace-runtime/src/lib.rs:1636` |
| **MCP 服务** [1.2.0，16 工具在 1.3.0] | `POST /mcp`（JSON-RPC 2.0，Streamable HTTP）：**16 工具——8 读、8 写**（1.2.0 为 14 = 9 读 + 5 写；R11 apply 对用满 ≤16 预算，R12 以 `publish_pipeline` **替换**调试型 `get_pipeline_logs` 快照工具而非扩张预算）。写工具须**同时**满足 `--mcp-allow-write` / `MCP_ALLOW_WRITE=1` 与 `"confirm": true`；每次调用审计为 `MCP_TOOL_CALL`、`actor = mcp:<client>`，凭据任意深度脱敏；只读的 `apply_dry_run` 拒收携凭据的声明文件（免会话端点不得成为密码 oracle）。工具描述由 OpenAPI 文档生成——无第二份手写文案可漂移 | `console/crates/tailrace-console/src/mcp.rs:108`（`TOOLS`），路由 `:197`；审计脱敏 `:1241-1242`；开关 `console/crates/tailrace-console/src/main.rs:32` |
| **维表广播 join** [1.2.0] | `… JOIN ${ds:dim.<数据源id>.<表>} [AS] d ON …` 在装配期经 JDBC 全表装载维表为静态 MemTable；500 000 行 / 64 MiB 上限 fail-fast，必须有唯一键，仅 INNER / LEFT（RIGHT / FULL / CROSS 编译期拒绝）；依赖需显式打开的 `connector-jdbc` feature | `engine/crates/tailrace-runtime/src/dim.rs:36` `:39`（上限）；join 类型门 `engine/crates/tailrace-parser/src/lib.rs:358-359` |
| **建表闭环** [1.2.0] | draft → 人工确认键模型（必选；缺省 422）→ execute 全文审计 → evolve 仅 `ADD COLUMN`（Light Schema Change；改类型/删列只报告不执行）。纯函数生成器：schema + hints 进，DDL + 「这是猜测」标注出；R6 映射 13 个标量词，R9 起 `variant` → `VARIANT`、`list<float32[,dim]>` → `ARRAY<FLOAT>`，未映射类型（`null`）报错而非折叠 | `engine/crates/tailrace-ddl/src/lib.rs:180`（`generate_draft`）`:438`（`doris_type`）；执行 `engine/crates/tailrace-metadata/src/lib.rs:233`（`mysql_exec`）；端点 `console/crates/tailrace-console/src/api/datasources.rs:1979` `:2112` `:2271`；web 向导 `web/src/api/datasources.ts` |
| **确定性回填** [1.2.0] | `POST /api/pipelines/{id}/backfill` 带 `dryRun`（零副作用）→ 在 `bf-<job>` 台账命名空间与独立消费组内有界运行；label 为 `前缀-语句号-<sha256(from,to)[..8]>_<批序号>`，重放同区间即去重；console 启动期把孤儿 job 标 failed。源门现状：仅 `file`（Kafka 分区级种子为后续项） | `engine/crates/tailrace-conn-doris/src/lib.rs:153`（`range_fingerprint`）`:187`（`LabelMode`）；`engine/crates/tailrace-runtime/src/lib.rs:849`（`run_bounded`）`:1676`（`backfill_state_dir`）；`console/crates/tailrace-console/src/api/pipelines.rs:2433`（POST）`:2529`（jobs）`:2384`（源门） |
| **部分列更新** [1.2.0] | 开关即 SQL：`INSERT INTO sink (k, a) SELECT …`——INSERT 列清单声明子集，裸 INSERT 整行写。四道门：登记 `key_model = unique` → 活表 `SHOW CREATE TABLE` 须为 UNIQUE merge-on-write（FE 不可达 = 502，不猜）→ 无进行中 Schema Change（409）→ sink 能力 + `sequence_column ∈ 子集`。Stream Load 同时发 `partial_columns:true` **与** `columns` 头（缺头时 Doris 静默把未列出的列置 NULL——活探针实证） | parser `engine/crates/tailrace-parser/src/lib.rs:93-98` `:281`（`insert_column_list`）；`engine/crates/tailrace-conn-doris/src/lib.rs:589`（`with_partial_columns`）；门 `console/crates/tailrace-console/src/api/pipelines.rs:456`（`partial_declarations_gate`）`:549`（`live_key_model_gate`）`:1183`（门聚合） |
| **十六词列类型** [1.2.0 + 1.3.0 BREAKING] | `utf8 string int8 int16 int32 int64 float32 float64 decimal(p,s) timestamp(unit) date bool binary list<float32[,dim]> variant null`；参数化解析、规范 Display。未知词：1.2.x 为迁移窗内 warn + 折叠为 `utf8`；**本树（1.3.0）已在装配期硬报错**（`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW = false`），**且 console 预检让它在 register/apply/ddl-draft/publish 即 400**（见下「列类型词汇预检」行）——升级前先核查手工登记的类型字符串 | `engine/crates/tailrace-ir/src/column_type.rs:31`（`ColumnType`）`:96`（`parse`）`:173`（迁移窗常量，现 false）；经 `tailrace_catalog::ColumnType` re-export |
| **`TableObject` options 化** [1.2.0] | 5 个通用字段（`datasource / name / kind / format / columns`）+ `options` 映射；连接器字段经 typed accessor 读取（`KafkaTableExt` / `DorisTableExt` / `JdbcTableExt` / `FileTableExt`）；键名正典在 `option_keys`；旧扁平字段仍可读可写（serde 双写） | `engine/crates/tailrace-catalog/src/lib.rs:40`（`option_keys`）`:85`（`TableObject`）`:112`（`options`）；accessor `tailrace-conn-kafka:31` `tailrace-conn-doris:256` `tailrace-conn-jdbc:80` `tailrace-conn-file:49` |
| **Sink 事务钩子 + 能力声明** [1.2.0] | `Sink` trait 上的 `prepare / commit / abort` 默认 no-op（现有 sink 零改动）与 `SinkCapabilities { idempotent_label, transactional, overwrite, partial_update, partition_aware }`；runtime 拒绝「无 `partial_update` 能力的 sink 带 `partial_columns`」——旗标真承重，不是装饰 | `engine/crates/tailrace-connector/src/sink.rs:18`（`SinkCapabilities`）`:107` `:112` `:118`（钩子）`:126`（`capabilities`） |
| **`sequence_col` DDL 属性** [1.2.0] | UNIQUE KEY 下声明的 `sequence_column` 以表级属性 `"function_column.sequence_col"` 输出——没有它 Doris 会静默忽略引擎的 `sequence_column` Stream Load 头（2.1.7 与 3.1.4 实探） | `engine/crates/tailrace-ddl/src/lib.rs:370` |
| **VARIANT 直写** [1.2.0] | `variant` 列把嵌套 JSON 子文档**原文透传**（wire 上是序列化字符串），Doris 服务端自行解析——sink 零改动（探针抓包 Stream Load 体证明 wire 形态）。已知取舍：根级字符串值会被服务端再解析（为 Debezium 把 JSON 编码为字符串的场景让路） | DDL `engine/crates/tailrace-ddl/src/lib.rs:451`；标记 `engine/crates/tailrace-ir/src/column_type.rs:216`（`VARIANT_FIELD_METADATA`）；探针 `scripts/e2e/evidence/variant-probe-2026-10-30/` |
| **向量列** [1.2.0] | `list<float32[,dim]>` → `ARRAY<FLOAT>`、维度以列注释附带；每行写前按声明 dim 整行校验（坏向量 → 整行进 DLQ——Doris 会静默降级为 NULL）；dim ≤ 65536、元素须有限；list 列不作键列/分区列。HNSW/IVF 索引是用户自己的建表 DDL | DDL `engine/crates/tailrace-ddl/src/lib.rs:482` `:495`；守卫 `engine/crates/tailrace-decode/src/lib.rs:899`（`check_vector_cells`）；上限 `engine/crates/tailrace-ir/src/column_type.rs:267` |
| **管理台告警** [1.2.0 + 1.3.0] | 规则引擎在 console（node 无告警面）：五类规则（`lag_seconds / state_failed / heartbeat_age / dlq_window / flush_errors_window`）每 15s 对 scrape 所服务的**同一**聚合 `/metrics` 快照评估。episode **逐序列**、**三态判决**（Firing / Clear / Unknown 保持——窗口无基线、node 不可达、源 idle 三源）；事件行只在状态迁移时落；resolved 必达（条件清除/序列消失/禁用/删除）；投递 = webhook POST（脱离评估循环，fire-and-log）+ SSE `GET /api/alerts/stream`；规则 CRUD 审计化。随附 `docs/ops/prometheus-alerts.yaml`（7 条）+ `docs/ops/grafana-dashboard.json` | `console/crates/tailrace-console/src/alerting.rs:78`（五类）`:559`（`evaluate_rule`）`:1066`（`spawn_evaluator`）；台账 `console/crates/console-store/migrations/04_alert.sql` + `05_alert_series.sql`；API `console/crates/tailrace-console/src/api/alerts.rs` |
| **SLO：目标线 + 达成侧** [1.2.0 目标线；1.3.0 分位数] | 每条启用的 lag 规则把阈值暴露为 `tailrace_slo_lag_target_seconds`、标签集与其监视的 lag 序列相同（1.2.0）。达成侧（R11）：1h lag 样本环（`slo_lag_sample`，评估器每轮同快照一样本/序列；idle 序列不采样），nearest-rank p50/p95/p99 与样本数上 `/metrics` 与 `GET /api/slo?windowSecs=`——每个分位数都是一条存储样本、可复算 | `console/crates/tailrace-console/src/slo.rs:118`（`record_pass`）`:133`（`nearest_rank`）；`console/crates/console-store/migrations/06_slo_lag_sample.sql`；`console/crates/tailrace-console/src/api/slo.rs:93`；指标 `console/crates/tailrace-console/src/metrics.rs:214` `:270` |
| **idle 感知告警** [1.3.0] | 运行中但无数据的管道不再反复触发 lag 告警：源携带**位置**判决（file 到 EOF；kafka 消费位连续 3 次检查等于各分区端位——绝不用时钟），语句级 idle = 源无积压**且** sink 缓冲空（坏 sink 的真 lag 照常评估）。console 的 lag 规则对 idle 序列先 hold（Unknown）——**单拍 idle 保持**（防抖动）；**连续两拍 idle = 真静默 → Clear**（R12 T-B：排干后已 firing 的 episode 自行关闭，不再卡到 80s 超时）；非 idle 拍重置计数并正常评估。无判决的源行为与 1.2.0 逐字节同。暴露为 `tailrace_pipeline_source_idle{pipeline,statement}`（不可判时不出序列）与 stats 的 `sourceIdle` | trait `engine/crates/tailrace-connector/src/source.rs:98`（`idle`）；判决 `engine/crates/tailrace-runtime/src/lib.rs:211`（`publish_idle`）`:226`（`IdleResetGuard`）；kafka 判定器 `engine/crates/tailrace-conn-kafka/src/lib.rs:100`；hold/两拍 `console/crates/tailrace-console/src/alerting.rs:673`（`threshold_verdicts`）`:772`（`idle_ticks`）；契约 `console/crates/tailrace-console/tests/alerts_flow.rs:413` |
| **DLQ 行键去重** [1.3.0] | DLQ side 文件是坏行的**集合**而非隔离尝试日志：行键 `(statement, 行位点, rawB64)`，行位点 = chunk 自己的游标推进（kafka `(分区, next offset)`、file `行尾字节位`）——重放无论怎么切批，一条坏行恰落一次。尝试计数器（`tailrace_dlq_rows_total`、stats `bad`）不变——重放风暴在审计面保持可见 | `engine/crates/tailrace-runtime/src/dlq.rs:121`（`BadRowDlq`）`:208`（`record_key`）`:254`（`seed_index`）；契约 `tests/contract/tests/executor_loop.rs:246` `:481` |
| **列类型词汇预检** [1.3.0（1.3.0 BREAKING 的护脚）] | console 三入口 + 发布门校验 `columnsJson` / `columnTypes` 的每个类型词：register / apply / ddl-draft / publish 首个未知词即 400，信封 `<表> 第 N 列「<词>」` + 16 词建议表；不再等到 deploy/重启重组装才由引擎硬错。`GET /api/datasources/tables` 行增可选 `unknownColumnTypes`（1.2.0 存量提示，只提示不改行为）。已知 16 词全放行，1.2.0 合法声明零影响（契约测试含存量兼容例） | 校验器 `console/crates/tailrace-console/src/api/datasources.rs:1941`（`validate_column_type_words`）；register `console/crates/tailrace-console/src/api/datasources.rs:1332`、apply `console/crates/tailrace-console/src/api/apply.rs:592`、draft `console/crates/tailrace-console/src/api/datasources.rs:2014`；发布门 `console/crates/tailrace-console/src/api/pipelines.rs:375`（`unknown_type_gate`）`console/crates/tailrace-console/src/api/pipelines.rs:1196`；存量提示 `console/crates/tailrace-console/src/api/datasources.rs:354` `console/crates/tailrace-console/src/api/datasources.rs:1408`；词汇 `engine/crates/tailrace-ir/src/column_type.rs:202`（`KNOWN_VOCABULARY`）；契约 `console/crates/tailrace-console/tests/datasources_crud.rs:770` |
| **`Cursor::Custom`** [1.2.0] | 游标枚举新增连接器自定义变体 `Custom { kind, payload }`（CDC LSN / Iceberg 快照 id…）——`kind` 命名语义使连接器能拒收外来游标；四个内置变体序列化字节不变、旧台账零迁移装载（读旧写新，legacy fixture 钉住）。尚无在树连接器写它——这是门，不是 WAL/LSN 源本身 | `engine/crates/tailrace-ir/src/cursor_sourcechunk_impl.rs:64`；台账 `engine/crates/tailrace-state/src/lib.rs:34-41`；fixture `engine/crates/tailrace-state/tests/fixtures/` |
| **管道即代码** [1.2.0 + 1.3.0 MCP] | 一份声明 JSON（`datasources` / `tables` / `pipelines` 任意子集）经同一服务端对：`POST /api/apply/dry-run` 算结构化字段级 diff（只读不审计），`POST /api/apply` 做幂等 upsert——两者共用 `plan()`，人审的 diff 就是机器落的 diff。身份=业务名（绝不用数字 id）；键缺席=不表态、键存在=声明全集，未命名对象报 **orphan 永不删除**；条目体即 REST DTO（serde 单源）；凭据按存在性比较、绝不回显。CLI 薄壳 `tailrace-console apply -f <file> [--dry-run]`；MCP 工具 `apply` / `apply_dry_run`（R11）与 `publish_pipeline`（R12，GitOps 最后一环）——**纯 MCP 客户端可完成 apply → dry-run → publish → 查 lag 全链**；字段表在 `docs/ops/pipeline-file.md` | `console/crates/tailrace-console/src/api/apply.rs:148`（dry-run）`:183`（apply）`:347`（`plan`）；CLI `console/crates/tailrace-console/src/apply_cli.rs`；路由 `console/crates/tailrace-console/src/lib.rs:137-138`；MCP `console/crates/tailrace-console/src/mcp.rs:680`（`publish_pipeline`） |
| **工程治理** [1.2.0] | 全 workspace `unsafe_code = "forbid"`；依赖与测试构建 `debug = "line-tables-only"`（一次磁盘写满事故后单套产物收敛到 16 GiB 量级）；`cargo deny` 新 advisory 随到随清 | `Cargo.toml:55-56`（lints）`:71-72` `:84-85`（profile）；`deny.toml` |

上述各轮的 e2e 覆盖，对真 Doris（compose 2.1.7 与外部 3.1.4）：`scripts/e2e/ddl_create.sh`
（48/48）、`backfill_replay.sh`（31/31）、`partial_update.sh`（compose 38/38 +
外部 37/37）、`type_precision.sh`（decimal(18,2) 精度守恒、sequence_col 定序）、
`variant.sh`（3.1.4 上 41/41 + 2.1.7 上 42/42）、`variant_vector.sh`（3.1.4 上 46/46 +
**compose 2.1.7 上 47/47，2026-11-02 复验**）、`alert_flow.sh`（R11 按 idle/DLQ 去重
语义重写后 **69/69**；R12 修正前提断言后连跑两次 **69/69**，证据
`scripts/e2e/evidence/alert_flow-r12w4-2026-11-15/`）、`mcp_publish_chain.sh`（R12
新增，纯 `/mcp` 面走 apply → dry-run → 幂等重放 → publish → 查 lag；脚本随波交付，
定版时未留运行记录），另有五轮 dogfooding（`scripts/dogfood/`，报告在 `docs/dev/`；
round4 = 36/36 且在 R12 树上重跑仍 36/36，全程只经 MCP 面驱动管道即代码；round5 =
真实新用户从 crates.io 装 1.2.0 逐字跑 README）。证据在 `scripts/e2e/evidence/`；
更早的数字是各轮落地当时登记的结果——未为本 README 重跑。

## 基准

下表数字均为开发机单机测量、除注明外为 debug 档构建——**跨机器不可比**，
debug 档数字亦不代表 release 构建性能（口径限定随账本同载）。数字唯一来源=
[docs/BENCHMARKS.md](docs/BENCHMARKS.md)（方法学、逐轮样本、机型账册）。

| 基准（除注明外 10 万行） | 机型 / 构建 | 结果 |
|---|---|---|
| file→file 全链 | Intel i7-9750H，debug | **21739 rows/s**（v0.1.0 基线，2026-10-17） |
| kafka(P=6)→file，4 轮 | Apple M4，debug | **中位 27861 rows/s**（23918 / 27678 / 28043 / 28474）——该 bench 在 M4 的首档；与 Intel 侧 24658 rows/s 跨机器不可比 |
| file→doris 单管道·默认批（T32） | Apple M4，debug | **33852 rows/s** vs 同日 file→file 锚点 33568（+0.8%）——大批下汇端零可测开销；小批（500 行）11017 |
| file→file，DataFusion 48.0.1 → 55.2.0（T33） | Apple M4，debug | **41528 rows/s** vs DF48 锚点 33568（+23.7%，同机不同日——登记为「无回归」而非增益） |
| 连接器裁剪档 `tailrace-node`（file+doris，无 kafka） | release 构建 | 二进制体积 **−9.4%**（61,571,936 B → 55,788,080 B） |
| Kafka 动态分区发现（2→4 分区不重启） | testcontainers Kafka | **26.8 s**（subscribe）/ **30.7 s**（assign） |

## 常见问题（FAQ）

**与 Flink CDC / SeaTunnel / Vector / Airbyte 有何区别？**
Flink CDC 与 SeaTunnel 是 JVM 数据集成平台；Airbyte 是多容器 ELT 平台（含
Temporal）；Vector 与 Redpanda Connect 是带私有转换语言（VRL / Bloblang）、无控制面的
单二进制——而且 2026 年起两者都自带原生 Doris Stream Load sink，「能写 Doris 的
单二进制」本身已不再稀缺。tailrace 是刻意做小的接入引擎——一条 SQL 一条管道、至少一次投递 + 汇端
label 幂等、两个二进制、无集群——同时把 Doris / StarRocks 写路径的运维闭环一并交付：
带键模型确认的 DDL 草稿、确定性回填、部分列更新、`/metrics`、MCP 服务与 SSH 部署链。
流式 join、水位线与状态后端是明确的非目标。需要完整平台就用平台；要在边缘小盒子把
数据喂进 Doris / StarRocks，选 tailrace。

**为什么无 JVM、怎么做单二进制？**
引擎与管理台全 Rust。Web 控制台内嵌在 console 二进制里（rust-embed）；
librdkafka 走 cmake 树内构建、openssl vendored。交付两个产物——
`tailrace-console`（控制面 + UI）与 `tailrace-node`（引擎）——目标 2–4 GB
内存的机器。连接器裁剪档 node 构建可整体去掉 kafka 栈
（`--no-default-features --features connector-file,connector-doris`，
二进制 −9.4%；BENCHMARKS §六）。

**部分列更新怎么用？**
把列清单写进 INSERT：`INSERT INTO ${ds:…} (k, a) SELECT k, a FROM …` 只更新 `k` 与
`a`、其余列保留旧值；裸 `INSERT INTO ${ds:…} SELECT …` 整行写。目标表必须是 Doris
UNIQUE KEY merge-on-write 表（部署时对活表实查，不只看登记），`sequence_column`
必须在列清单内；该表上有 `ADD COLUMN` 作业未完结时部署被 409 拒绝——DDL evolve
端点反向也拒。StarRocks 汇不支持此模式。

**回填幂等吗？窗口多长？**
按 label 幂等：回填的 Stream Load label 为
`前缀-<语句号>-<sha256(from,to)[..8]>_<批序号>`，同一区间在任何进程重放都复用同一组
label，Doris 拒绝重复（`label already exists` → `Dedup`）。幂等窗口 = Doris 的
**label 保留期**（默认 3 天）——超期重放回落到表的键模型。回填在独立的 `bf-<job>`
台账命名空间与消费组内运行，永不触碰常驻管道的游标。目前管理台只接受 `file` 源的
回填、字节位边界；`dryRun: true` 零副作用预览 label 前缀（MCP 工具默认即此）。

**维表 join 是流式 join 吗？**
不是——是广播 join，红线不变。维表是 `kind = dim` 的 JDBC 表对象，管道装配期全表
装载为静态内存表；无水位线、无状态后端、运行期不刷新（重启重载）。护栏在编译期
fail-fast：50 万行 / 64 MiB 上限，维表必须有主键/唯一键，只接受
`INNER` / `LEFT [OUTER] JOIN`。含维表 join 的语句暂不能回填。

**tailrace 做 CDC 吗？**
消费 CDC、不抓取 CDC：给 Kafka 表对象设 `format = "debezium-json"`，解码器把
Debezium 的 `c` / `r` / `u` 事件转成行（取 `after`），跳过 `d` 事件与墓碑，解包
schemas-enabled 外壳，畸形事件路由到 DLQ。自己读 binlog / WAL 不在范围内——把
Debezium 指向数据库，把 tailrace 指向 topic。

**MCP 服务是干什么的？**
AI 客户端可经 `POST /mcp`（JSON-RPC 2.0）操作管理台：16 工具（8 读 + 8 写，
1.2.0 为 14）——列表 / 查看 / 编译 / 统计 / DDL 草稿 / 回填状态 / apply 预演为只读、
恒开；创建 / **发布** / 部署 / 暂停恢复 / 回填 / DDL 执行 / DDL 演进 / apply 为写
工具，需要管理台以 `--mcp-allow-write`（或 `MCP_ALLOW_WRITE=1`）启动**且**调用带
`"confirm": true`。每次调用落审计 `MCP_TOOL_CALL`、`actor = mcp:<client>`，凭据任意
深度脱敏；`apply_dry_run` 另拒收携数据源凭据的声明文件（免会话端点不得成为密码
oracle）。工具描述由与 `/swagger-ui` 同源的 OpenAPI 文档生成。三个 1.3.0 面：
`publish_pipeline`（R12，取代调试型的 `get_pipeline_logs` 快照工具——REST 日志端点
原样保留，映射 `POST /api/pipelines/{id}/publish`）、`apply` / `apply_dry_run`
（R11），以及上面那条列类型词汇预检（`apply` 的 tables 段同样受检）。16/16 预算已
满——新工具先过预算裁决（R12 即「替换而非扩张」的先例）。

**表对象能声明哪些列类型？**
十六个词：`utf8 string int8 int16 int32 int64 float32 float64 decimal(p,s)
timestamp(unit) date bool binary list<float32[,dim]> variant null`。四个旧词语义
逐字节不变。未知词：1.2.0 记 warning 并折叠为 `utf8`（一个 minor 版本的迁移窗）；
**v1.3.0 起改在管道装配期硬报错**（`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW =
false`），且 console 的预检让它更早在 **register / apply / ddl-draft / publish**
就 400（信封给出第几列、什么词、建议词表）——旧声明若含未知词，现在在登记处直接
报错并列出建议词，不必等到 deploy。DDL 生成器映射 13 个标量词到 Doris
类型，`variant` → `VARIANT`（嵌套 JSON 原文透传、不展平）、`list<float32[,dim]>` →
`ARRAY<FLOAT>`（维度以列注释附带，每行写前按维度校验）；`null` 是唯一仍未映射的词。

**为什么运行中但没数据的管道不再触发 lag 告警？**
因为源按**位置**报告了 idle——不是按时钟。lag gauge 是「最近一次含数 flush 的墙
钟」，静默的管道本会永远「lag」下去；1.3 线起，file 源读到 EOF、kafka 源的消费位
连续 3 次检查等于各分区端位即判 idle（前面还有行的慢消费者是 lag 不是 idle），语句
级还要求 sink 缓冲为空才发布 idle（坏 sink 的真 lag 照常评估），console 的 lag 规则
对 idle 序列先 hold（Unknown）而非触发；**连续两拍 idle 视为真静默 → Clear**（R12
T-B，排干后已 firing 的 episode 自行关闭）——无判决的源行为与之前逐字节相同。PromQL
孪生在 `docs/ops/prometheus-alerts.yaml`（`unless ... tailrace_pipeline_source_idle
== 1`）。

**怎么用文件管理管道（GitOps）？**
写一份声明 JSON（`datasources` / `tables` / `pipelines` 任意子集——字段表与示例在
`docs/ops/pipeline-file.md`），`tailrace-console apply -f file.json --dry-run` 预览
结构化字段级 diff，去掉 `--dry-run` 同命令做幂等 upsert（REST `POST
/api/apply[/dry-run]` 与 MCP 的 `apply` / `apply_dry_run` 工具打到同一服务端
`plan()`）。身份是业务名、绝不用数字 id；已匹配的对象零触碰；文件未命名的对象报为
orphan、**永不删除**——v1 没有 prune。凭据按存在性比较、绝不回显。

**StarRocks 与 Doris 通道是什么关系？**
一条 Stream Load 实现：StarRocks 复用 `tailrace-conn-doris` 内的 Doris 汇，
按 kind 门控差异（FE 307 重定向跟随、头部集、`read_json_by_line` 载体、
label 幂等、缓冲落盘阈值）。已对 `starrocks/allin1` 端到端验证
（`docker compose --profile starrocks`）。部分列更新仅 Doris。

**mqtt / s3 连接器什么时候有？**
未排期；两项均为单行占位、按真实需求触发（MQTT：≥3 个真实 issue 或自用场景，先
PoC，12 个月窗口；S3：等 lakehouse 汇被真正提出）。原「S3/CDC」项的 CDC 一半已以
`debezium-json` 解码器落地。公开路线图在 docs/ASSESSMENT-AND-OUTLOOK.md §五。

**支持哪些平台与 Rust 版本？**
MSRV 为 Rust 1.95（workspace `rust-version`，CI msrv job 真实编译验证）。
CI 共 8 个 job，覆盖 ubuntu-latest 与 macos-latest（fmt/clippy/test、msrv、
feature 契约构建、web、testcontainers 临时栈 chaos、端到端 smoke）。发布
流水线交叉构建 linux x64/arm64 与 macOS x64/arm64——每目标两个二进制。

## 文档

- [CHANGELOG.md](CHANGELOG.md)——发布日志
- [docs/ASSESSMENT-AND-OUTLOOK.md](docs/ASSESSMENT-AND-OUTLOOK.md)——现状评估与未来展望（v1.1.0 时点评估之上叠 §〇 post-R11 基线；公开路线图在其 §五）
- [docs/design/architecture.zh.md](docs/design/architecture.zh.md)——架构设计（post-R11 版）
- [docs/ops/](docs/ops/)——运维三件：grafana-dashboard.json（含 SLO row）、prometheus-alerts.yaml（7 条规则）、pipeline-file.md（apply 声明文件）
- [docs/protocol/console-api.md](docs/protocol/console-api.md) / [docs/protocol/engine-v1.md](docs/protocol/engine-v1.md)——管理台 REST + MCP 面 / 引擎 `/v1` 协议
- [docs/connectors/jdbc.md](docs/connectors/jdbc.md)——JDBC 批源
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md)——性能账本（上表数字唯一来源）
- [docs/dev/](docs/dev/)——技术决策记录与 dogfooding 报告
- [docs/book/](docs/book/)——mdBook 文档站（中文单轨；本地 `mdbook build` 构建）

## 开发

从源码构建需要 Rust ≥ 1.95（rustup；仓库 pin 了 stable 工具链）、Node.js 22 与
pnpm；`./scripts/build.sh` 构建前端并内嵌，再构建双二进制。

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build（web/）
./scripts/dev.sh     # console（debug）+ Vite 开发服务器（/api 代理）
```

## i18n

控制台界面与 API 错误文案目前为中文单轨。wire 侧已预留 `message_key`
字典化通道（响应字节级不变），多语言本体列入 v1.x 议程。

许可证：Apache-2.0。English readme: [README.md](README.md)。
