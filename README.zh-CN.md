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

**已发布：v1.1.0（2026-10-25）。** 21 个 workspace crate 在 crates.io 发至 1.1.0
（`cargo install tailrace-node --version 1.1.0 --locked`），v1.1.0 git tag 与
9 资产 GitHub Release 在档。随版交付：引擎数据面（kafka / file / jdbc 为源，Doris +
StarRocks Stream Load 与 file 为汇）、数据源发现与模式推断（`discover` /
`infer-schema`）、SSH 部署链、性能账本（docs/BENCHMARKS.md）与 8 job CI 矩阵。

**在本树中、尚未发布。** v1.1.0 之后四轮开发落地了下方[能力矩阵](#能力矩阵v110-之后未发布)
所列十二项能力：`/metrics`、Debezium CDC 解码、MCP 服务、维表广播 join、建表闭环、
确定性回填、部分列更新、十六词列类型词汇、`TableObject` options 化、Sink 事务钩子、
`sequence_col` DDL 属性与全 workspace `forbid(unsafe_code)`。**crates.io 与二进制
Release 现值仍为 1.1.0——开发线能力超前于已发布版。** 下一个版本号在发布时
定、不在这里定；逐轮账见 [docs/design/architecture.zh.md](docs/design/architecture.zh.md)
§十二，[CHANGELOG.md](CHANGELOG.md) 为发布日志。

汇端交付档位如实表述：**label 幂等档已交付**，分两种模式。常驻管道用*进程作用域*
label——同进程内重试复用同一 label，Doris 去重；跨重启的不重复来自已提交游标加表的
键模型，不来自 label。回填用*确定性* label——同一区间在任何进程映射到同一组 label，
Doris 回 `label already exists`，汇记为 `Dedup`（e2e：DUPLICATE KEY 表上同区间重放
两次，行数不变）。事务接口（exactly-once）与 group commit 吞吐档**未交付**：`Sink`
trait 现已带 `prepare / commit / abort` 钩子（默认 no-op）与 `SinkCapabilities`
能力声明作为扩展点，但尚无生产 sink 声明 `transactional`。mqtt / s3 连接器仍为单行
占位。

## 快速开始

前置条件：Rust ≥ 1.95（rustup；仓库 pin 了 stable 工具链）、Node.js 22、pnpm。

```bash
# 1. 构建前端并内嵌，再构建双二进制
./scripts/build.sh

# 2. 启动管理台（Web UI 在 http://127.0.0.1:8080）
./target/release/tailrace-console --data-dir ./data
#    首次启动会在 stderr 打印一次性 admin 初始密码与 bootstrap token——
#    当场抄下，之后不再展示。
#    加 --mcp-allow-write 开启 MCP 写工具（默认只读）。

# 3. 启动引擎节点并向管理台自注册
./target/release/tailrace-node serve \
  --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 \
  --advertise 127.0.0.1:18100 \
  --bootstrap-token <首启打印的 token>
```

浏览器打开 http://127.0.0.1:8080，用 `admin` + 抄下的密码登录建管道；或跑
`./scripts/smoke.sh` 做脚本化端到端自检（默认 25 步断言、无外部依赖；加 `--full`
为 44 步——经 docker compose 走 Kafka→Doris）。Prometheus 抓两个二进制各自的
`/metrics`；AI 客户端走管理台的 `POST /mcp`。

## 连接器矩阵

| 类型 | 源 | 汇 | 说明 |
|---|---|---|---|
| kafka | ✅ | ✅ | 主力源，分区位点游标，动态分区发现；汇侧按行产出 JSON 对象到 topic——生产者会话内 `enable.idempotence`，broker 侧无去重 label（重放为 at-least-once），不做分区键路由（`engine/crates/tailrace-conn-kafka/src/lib.rs:48` / `:502`） |
| file | ✅ | ✅ | 本地/挂载文件；目前管理台回填唯一接受的源（字节位边界）；汇侧每次 flush 追加 jsonl 或滚出一个 parquet part 文件（`engine/crates/tailrace-conn-file/src/lib.rs:21` / `:374`）。两汇均不声明 `partial_update` / `transactional`；parser 汇白名单为 kafka / file / doris / starrocks（`engine/crates/tailrace-parser/src/lib.rs:159`） |
| jdbc | ✅ | — | MySQL / PostgreSQL keyset 批源（[文档](docs/connectors/jdbc.md)） |
| dim | 仅 JOIN | — | JDBC 承载的**广播**维表（`kind = dim`，在 JOIN 中以 `${ds:dim.<别名>}` 引用）：装配期全表装载，50 万行 / 64 MiB 上限，必须有唯一键——不是流式 join |
| doris | — | ✅ | Stream Load；label 幂等（进程作用域 + 确定性）、部分列更新、sequence column |
| starrocks | — | ✅ | 经 Doris 汇走 Stream Load，按 kind 门控 StarRocks 差异（FE 307 重定向跟随、头部集）；已对 `starrocks/allin1` e2e（`docker compose --profile starrocks`）；部分列更新仅 Doris |
| mqtt | — | — | 占位，未实现（触发器制——docs/ASSESSMENT-AND-OUTLOOK.md §五） |
| s3 | — | — | 占位，未实现（触发器制，同上） |

源解码格式（表对象的 `format`）：`ogg`（默认）、`json` / `jsonl`、`csv`、`parquet`、
`debezium-json`——消费 Kafka 上已有的 Debezium CDC 事件；tailrace 只消费事件，不读
binlog。

## 能力矩阵（v1.1.0 之后，未发布）

下表全部已在本源码树中，有单测/契约测试与所列 e2e 脚本覆盖，但**尚未发布**到
crates.io（见「状态」）。锚点为本仓库内的 `路径:行号`。

| 能力 | 做什么 | 代码位置（锚点） |
|---|---|---|
| **Prometheus `/metrics`** | 9 指标面、手写 text format、零 exporter 依赖；node 端在抓取时从活管道计数器现算渲染，console 端另加 `tailrace_console_db_write_seconds` | `engine/crates/tailrace-api/src/metrics.rs:9`（指标面）`:235`（`node_metrics_text`）；`console/crates/tailrace-console/src/metrics.rs:40`（`scrape`），路由 `console/crates/tailrace-console/src/lib.rs:189` |
| **Debezium CDC 解码** | `format = "debezium-json"` 解码器：`c`/`r`/`u` 事件转行、`d` 与墓碑跳过、schemas-enabled 外壳解包、畸形事件进 DLQ。窄口 CDC：只消费事件，不抓 binlog | `engine/crates/tailrace-decode/src/lib.rs:950`（`DebeziumJsonDecoder`）；派发 `engine/crates/tailrace-runtime/src/lib.rs:1532` |
| **MCP 服务** | `POST /mcp`（JSON-RPC 2.0，Streamable HTTP）：14 工具——8 读、6 写。写工具须**同时**满足 `--mcp-allow-write` / `MCP_ALLOW_WRITE=1` 与 `"confirm": true`；每次调用审计为 `MCP_TOOL_CALL`、`actor = mcp:<client>`。工具描述由 OpenAPI 文档生成——无第二份手写文案可漂移 | `console/crates/tailrace-console/src/mcp.rs:90`（`TOOLS`），路由 `:177`；开关 `console/crates/tailrace-console/src/main.rs:26` |
| **维表广播 join** | `… JOIN ${ds:dim.<别名>} d ON …` 在装配期经 JDBC 全表装载维表为静态 MemTable；500 000 行 / 64 MiB 上限 fail-fast，必须有唯一键，仅 INNER / LEFT（RIGHT / FULL / CROSS 编译期拒绝） | `engine/crates/tailrace-runtime/src/dim.rs:36` `:39`（上限）；join 类型门 `engine/crates/tailrace-parser/src/lib.rs:351-359` |
| **建表闭环** | draft → 人工确认键模型（必选；缺省 422）→ execute 全文审计 → evolve 仅 `ADD COLUMN`（Light Schema Change；改类型/删列只报告不执行）。纯函数生成器：schema + hints 进，DDL + 「这是猜测」标注出；映射 13 个标量词汇，未映射类型报错而非折叠 | `engine/crates/tailrace-ddl/src/lib.rs:169`（`generate_draft`）`:385`（`doris_type`）；执行 `engine/crates/tailrace-metadata/src/lib.rs:233`（`mysql_exec`）；端点 `console/crates/tailrace-console/src/api/datasources.rs:1887` `:2002` `:2162`；web 向导 `web/src/api/datasources.ts` |
| **确定性回填** | `POST /api/pipelines/{id}/backfill` 带 `dryRun`（零副作用）→ 在 `bf-<job>` 台账命名空间与独立消费组内有界运行；label 为 `前缀-语句号-<sha256(from,to)[..8]>_<批序号>`，重放同区间即去重；console 启动期把孤儿 job 标 failed。源门现状：仅 `file`（Kafka 分区级种子为后续项） | `engine/crates/tailrace-conn-doris/src/lib.rs:153`（`range_fingerprint`）`:187`（`LabelMode`）；`engine/crates/tailrace-runtime/src/lib.rs:773`（`run_bounded`）`:1566`（`backfill_state_dir`）；`console/crates/tailrace-console/src/api/pipelines.rs:2319`（POST）`:2420`（jobs）`:2277-2289`（源门） |
| **部分列更新** | 开关即 SQL：`INSERT INTO sink (k, a) SELECT …`——INSERT 列清单声明子集，裸 INSERT 整行写。四道门：登记 `key_model = unique` → 活表 `SHOW CREATE TABLE` 须为 UNIQUE merge-on-write（FE 不可达 = 502，不猜）→ 无进行中 Schema Change（409）→ sink 能力 + `sequence_column ∈ 子集`。Stream Load 同时发 `partial_columns:true` **与** `columns` 头（缺头时 Doris 静默把未列出的列置 NULL——活探针实证） | parser `engine/crates/tailrace-parser/src/lib.rs:93-98` `:281`（`insert_column_list`）；`engine/crates/tailrace-conn-doris/src/lib.rs:574`（`with_partial_columns`）；门 `console/crates/tailrace-console/src/api/pipelines.rs:359` `:452` `:622` |
| **十六词列类型** | `utf8 string int8 int16 int32 int64 float32 float64 decimal(p,s) timestamp(unit) date bool binary list<float32[,dim]> variant null`；参数化解析、规范 Display。未知词：一个 minor 版本迁移窗内 warn + 折叠为 `utf8`，之后硬报错（`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW`） | `engine/crates/tailrace-ir/src/column_type.rs:30`（`ColumnType`）`:84`（`parse`）`:158`（迁移窗）；经 `tailrace_catalog::ColumnType` re-export |
| **`TableObject` options 化** | 5 个通用字段（`datasource / name / kind / format / columns`）+ `options` 映射；连接器字段经 typed accessor 读取（`KafkaTableExt` / `DorisTableExt` / `JdbcTableExt` / `FileTableExt`）；键名正典在 `option_keys`；旧扁平字段仍可读可写（serde 双写） | `engine/crates/tailrace-catalog/src/lib.rs:35`（`option_keys`）`:80`（`TableObject`）`:107`（`options`）；accessor `tailrace-conn-kafka:31` `tailrace-conn-doris:256` `tailrace-conn-jdbc:80` `tailrace-conn-file:49` |
| **Sink 事务钩子 + 能力声明** | `Sink` trait 上的 `prepare / commit / abort` 默认 no-op（现有 sink 零改动）与 `SinkCapabilities { idempotent_label, transactional, overwrite, partial_update, partition_aware }`；runtime 拒绝「无 `partial_update` 能力的 sink 带 `partial_columns`」——旗标真承重，不是装饰 | `engine/crates/tailrace-connector/src/sink.rs:18`（`SinkCapabilities`）`:107` `:113` `:119`（钩子）`:126`（`capabilities`） |
| **`sequence_col` DDL 属性** | UNIQUE KEY 下声明的 `sequence_column` 以表级属性 `"function_column.sequence_col"` 输出——没有它 Doris 会静默忽略引擎的 `sequence_column` Stream Load 头（2.1.7 与 3.1.4 实探） | `engine/crates/tailrace-ddl/src/lib.rs:327-329` |
| **工程治理** | 全 workspace `unsafe_code = "forbid"`；依赖与测试构建 `debug = "line-tables-only"`（一次磁盘写满事故后单套产物收敛到 16 GiB 量级）；`cargo deny` 新 advisory 随到随清 | `Cargo.toml:55-56`（lints）`:71-72` `:84-85`（profile）；`deny.toml` |

上述各轮的 e2e 覆盖，对真 Doris（compose 2.1.7 与外部 3.1.4）：`scripts/e2e/ddl_create.sh`
（48/48）、`backfill_replay.sh`（31/31）、`partial_update.sh`（compose 38/38 +
外部 37/37）、`type_precision.sh`（decimal(18,2) 精度守恒、sequence_col 定序），另有两轮
dogfooding（`scripts/dogfood/`，报告在 `docs/dev/`）。这些数字是各轮落地当时登记的
结果——未为本 README 重跑。

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
单二进制。tailrace 是刻意做小的接入引擎——一条 SQL 一条管道、至少一次投递 + 汇端
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
AI 客户端可经 `POST /mcp`（JSON-RPC 2.0）操作管理台：14 工具——列表 / 查看 / 编译 /
统计 / 日志 / DDL 草稿 / 回填状态为只读、恒开；创建 / 部署 / 暂停恢复 / 回填 / DDL
执行 / DDL 演进为写工具，需要管理台以 `--mcp-allow-write`（或 `MCP_ALLOW_WRITE=1`）
启动**且**调用带 `"confirm": true`。每次调用落审计 `MCP_TOOL_CALL`、
`actor = mcp:<client>`。工具描述由与 `/swagger-ui` 同源的 OpenAPI 文档生成。

**表对象能声明哪些列类型？**
十六个词：`utf8 string int8 int16 int32 int64 float32 float64 decimal(p,s)
timestamp(unit) date bool binary list<float32[,dim]> variant null`。四个旧词语义
逐字节不变。未知词当前记 warning 并折叠为 `utf8`（一个 minor 版本的迁移窗）；下个
minor 改为管道装配期硬报错。DDL 生成器映射 13 个标量词到 Doris 类型；`variant` 与
`list<float32>` 已在词汇表中，但其 Doris DDL 映射与端到端直写是后续工作。

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
- [docs/ASSESSMENT-AND-OUTLOOK.md](docs/ASSESSMENT-AND-OUTLOOK.md)——现状评估与未来展望（v1.1.0 时点评估之上叠 post-R8 基线；公开路线图在其 §五）
- [docs/design/architecture.zh.md](docs/design/architecture.zh.md)——架构设计（post-R8 版）
- [docs/protocol/console-api.md](docs/protocol/console-api.md) / [docs/protocol/engine-v1.md](docs/protocol/engine-v1.md)——管理台 REST + MCP 面 / 引擎 `/v1` 协议
- [docs/connectors/jdbc.md](docs/connectors/jdbc.md)——JDBC 批源
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md)——性能账本（上表数字唯一来源）
- [docs/dev/](docs/dev/)——技术决策记录与 dogfooding 报告
- [docs/book/](docs/book/)——mdBook 文档站（中文单轨；本地 `mdbook build` 构建）

## 开发

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build（web/）
./scripts/dev.sh     # console（debug）+ Vite 开发服务器（/api 代理）
```

## i18n

控制台界面与 API 错误文案目前为中文单轨。wire 侧已预留 `message_key`
字典化通道（响应字节级不变），多语言本体列入 v1.x 议程。

许可证：Apache-2.0。English readme: [README.md](README.md)。
