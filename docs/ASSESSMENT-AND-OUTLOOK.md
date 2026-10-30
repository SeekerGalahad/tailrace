# tailrace 现状评估与未来展望

> 本文按日期分两段、自上而下以新覆旧。**§〇 post-R8 基线（2026-10-30）**描述 v1.1.0 之后四轮开发（扩展轮 / R6 / R7 / R8）落地后的实况，是当前有效结论。其后的 **v1.1.0 基线（2026-10-26）**为时点评估原文，逐字保留不改——其中引用的 `roadmap.md`、`R3-升格评审`、`R5-lineage-judge` 等过程档已于 2026-10-26（提交 `1bed48d`，公私仓文档分离）移入私有工作流，作为当时的证据链原样保留、公开仓内不再可点开。两段结论不一致处，以 §〇 为准。

## 〇、post-R8 基线（2026-10-30）

> **基线**：工作树分支 `feature/docs-v1.2`（HEAD `8ce8a95`，正典文档对齐轮），上承 `feature/type-foundation@3690090`（R8 收口）；`origin/main` 现值 `aead2b5`（R7 合并），R8 七提交待随文档轮合入。
> **版本诚实声明**：git tag 与 GitHub Release 止于 **v1.1.0**（本段 `gh release list` 实查：v1.1.0 = Latest，2026-10-25T12:30:30Z）；`Cargo.toml:39` `version = "1.1.0"`——四轮能力**均未发包**，开发线超前于已发布版；下一个版本号在发布时定，本文不预定。
> **引用纪律**：代码锚点 `路径:行号` 均为 2026-10-30 本段落稿会话在上述工作树实读；性能数字只引 `docs/BENCHMARKS.md`（本段未新测）；e2e 通过数为各轮落地时登记值（本段未重跑）；GitHub 数字为本段 `gh api` 实查。

### 0.1 一页纸结论（post-R8）

- v1.1.0 之后**四轮开发、十二项能力面**落地（§0.2 表，逐条带代码锚点与 e2e），**crates.io / Release 现值仍 1.1.0**——「能力超前于已发布版」是本段最重要的版本事实。
- 对照演化蓝图（2026-10-26；公开代码注释中以 blueprint §x.y 引用）的承诺范围：W0 地基 **4/6**（G1 TableObject options、G2 十六型词汇、G4 Sink 钩子、G5 多源注册 ✅；G3 Cursor 开放化、G6 管道即代码未做）；W1 Doris 原生写入 **6/8**（A1 建表闭环、A2 回填、A3 类型面、A6 metrics、A7 部分列更新、A8 dogfooding ✅；A4 VARIANT 直写、A5 向量列——词汇已落，Doris 侧映射与 e2e 未做）；W2 控制面核心 **1/2**（E4 MCP ✅ 14 工具；E2 告警/SLO 未做）；W3 期权两项提前兑现（C 窄口 debezium-json、D1 维表广播 join），其余期权未动。
- 健康分 **7 → 7.5 / 10**（§0.3）：技术深度 8→9、工程纪律 9 持平（`forbid(unsafe_code)` 全仓落地把「实践为 0」变成「编译器保证为 0」）；公开面一致性 5→6（双 README 与架构正典本轮对齐、修复 roadmap.md 删档断链，但 SECURITY.md「pre-1.0」与 issue 模板 `TODO` URL 仍在）；社区 2 持平（本段 `gh api` 实查：stars 0 / forks 0 / open_issues 0 / watchers 0，2026-10-30）。
- 三条差异化从「有数据」升级为「有闭环」：①SQL-first 的 SQL 面扩为「INSERT 列清单 = 部分列更新开关」（tailrace-parser/src/lib.rs:93-98）+「`${ds:dim.…}` 维表引用」（:322），仍无私有 DSL；②Doris 深度工程化新增建表闭环、确定性回填 label、UNIQUE 部分列更新、`sequence_col` 表级属性；③控制面新增 `/metrics`、MCP 14 工具、DDL/回填端点与 web 向导（§0.5）。
- 第一性问题未变：**0 用户**。四轮拆掉的是「用户第一次试用就会撞到的墙」（建表、补数、指标、类型面——蓝图 §2.3 前 6 行缺口中的 4 行），不是用户数本身。下一步仍是被看见（§0.6），不是再加功能。

### 0.2 十二项能力面（v1.1.0 后四轮，逐条锚点）

| # | 能力 | 轮次 | 代码锚点（2026-10-30 实读） | e2e（落地时登记） |
|---|---|---|---|---|
| 1 | Prometheus `/metrics`（9 指标手写 text format，node+console） | 扩展轮 | `tailrace-api/src/metrics.rs:9`（指标面）`:235`（`node_metrics_text`）；`tailrace-console/src/metrics.rs:40`（`scrape`）、路由 `tailrace-console/src/lib.rs:189` | 蓝图 §7.4 面审计（单测 `text_format_validity` 等 5 组） |
| 2 | `debezium-json` 解码（C 窄口：消费事件不抓 binlog；`c/r/u` 转行、`d`+墓碑跳过、schemas 外壳解包、畸形进 DLQ） | 扩展轮 | `tailrace-decode/src/lib.rs:950`（`DebeziumJsonDecoder`）；派发 `tailrace-runtime/src/lib.rs:1532` | 单测 5 组（`debezium_decode_c_r_u_events` 等） |
| 3 | MCP Server（JSON-RPC 2.0 over `POST /mcp`；14 工具=8 读+6 写；写需进程开关+`confirm:true` 双门禁；全调用审计 `actor=mcp:<client>`；工具描述 OpenAPI 同源生成） | 扩展轮+R8 | `tailrace-console/src/mcp.rs:90`（`TOOLS` 表）、路由 `:177`；开关 `main.rs:26` | dogfooding round2（24/24：initialize 握手 + tools/list=14 + 缺 confirm 拒绝） |
| 4 | 维表广播 join（`kind=dim` JDBC 表装配期全量装载；50 万行 / 64 MiB 上限；唯一键必检；RIGHT/FULL/CROSS 编译期拒） | 扩展轮 | `tailrace-runtime/src/dim.rs:36` `:39`（上限）；`tailrace-parser/src/lib.rs:351-359`（join 类型门） | `dim_join_flow` e2e（扩展轮评审修复轮） |
| 5 | 建表闭环（draft→键模型必选 422→execute 全文审计→evolve 仅 ADD COLUMN；纯函数生成器+猜测标注） | R6 | `tailrace-ddl/src/lib.rs:169`（`generate_draft`）、`mysql_exec`（`tailrace-metadata/src/lib.rs:233`）；端点 `api/datasources.rs:1887/:2002/:2162` | `ddl_create.sh` **48/48**（含分区全程、evolve 重启不断、改类型 409） |
| 6 | 回填与确定性 label（`range_fingerprint`=sha256(from,to)[..8]；`bf-` 台账/消费组隔离；崩溃恢复孤儿 job 标 failed；源门=file only） | R6 | `tailrace-conn-doris/src/lib.rs:153` `:187`（`LabelMode`）；`run_bounded`（`tailrace-runtime/src/lib.rs:773`）；`api/pipelines.rs:2319/:2420/:2277-2289` | `backfill_replay.sh` **31/31**（DUPLICATE KEY 表同区间重放两次行数不变） |
| 7 | 部分列更新（开关=INSERT 列清单；注册门+活表门+evolve 双向互斥+引擎门四道；`columns` 头必带） | R7 | `tailrace-parser/src/lib.rs:93-98` `:281`；`with_partial_columns`（`tailrace-conn-doris/src/lib.rs:574`）；`api/pipelines.rs:359/:452/:622` | `partial_update.sh` **38/38**（compose）+ **37/37**（外部栈） |
| 8 | 十六型类型词汇（参数化 parse、规范 Display；未知词迁移窗 warn+折 utf8，下个 minor 翻硬错） | R8 | `tailrace-ir/src/column_type.rs:30` `:84` `:158`（`UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW`） | `type_precision.sh`（decimal(18,2) 精度守恒+sequence_col 定序，compose 40/40+外部 39/39） |
| 9 | TableObject options 化（5 通用+options；typed accessor；`option_keys` 正典；旧字段 serde 双写双读） | R8 | `tailrace-catalog/src/lib.rs:35` `:80` `:107`；`*TableExt`：conn-kafka:31 / conn-doris:256 / conn-jdbc:80 / conn-file:49 | 交叉窗口契约测试（R8 评审修复轮补） |
| 10 | Sink 长事务钩子+能力声明（prepare/commit/abort 默认 no-op；`SinkCapabilities` 五旗标真承重） | R7 | `tailrace-connector/src/sink.rs:18` `:107/:113/:119/:126` | 契约 sink_capabilities 5 组 |
| 11 | `sequence_col` 表级属性（UNIQUE+sequenceColumn 时生成器附 `"function_column.sequence_col"`） | R8 | `tailrace-ddl/src/lib.rs:327-329` | `type_precision.sh` 对照失效形态（2.1.7/3.1.4 双栈探针） |
| 12 | 工程治理（`forbid(unsafe_code)` 全 workspace；debug=line-tables-only；deny 随到随清） | R6-R8 | `Cargo.toml:55-56`（lints）`:71-72/:84-85`（profile）；`deny.toml` | R8 收口 RUSTSEC 清偿（`3f4c7d8`，libz-sys 等） |

### 0.3 现状评估（post-R8，分项）

| 维度 | v1.1.0 | post-R8 | 依据（锚点） |
|---|---|---|---|
| 工程纪律与质量门 | 9 | **9**（+治理面） | `forbid(unsafe_code)` 全仓（Cargo.toml:55-56，v1.1.0 时「实践为 0 非编译器保证」的差距闭合）；deny 新 advisory 两轮清偿（R8 `3f4c7d8`）；控制面 e2e 五脚本对真 Doris（2.1.7 compose + 3.1.4 外部栈）；CI 8 job 结构未变 |
| 技术深度 | 8 | **9** | 十六型 wire 契约（column_type.rs:30）；DDL 生成器以「实测可建形态」为准（R6 评审教训固化）；确定性 label 的区间级幂等（conn-doris:187-213）；partial 四道门含活表实查 fail-closed（pipelines.rs:452）；MCP 描述 OpenAPI 同源防漂移（mcp.rs:739）。扣分项：exactly-once/group commit 仍未交付、回填源=file only、A4/A5 未变现 |
| 诚实账本 | 8 | **8** | 架构正典 676 行带锚点刷新（docs/design/architecture.zh.md，post-R8 版）；dogfooding 两轮摩擦清单公开（docs/dev/dogfooding-*.md）；「estimatedRows=null 诚实降级」「缺 columns 头=Doris 静默置 NULL」等负面事实全部在档。扣分项：CHANGELOG 尚无 Unreleased 段（本轮 W4 交付） |
| 公开面一致性 | 5 | **6** | 双 README 本轮对齐（十二项能力矩阵、版本诚实声明、roadmap.md 删档断链修复）；README 基准表补 T32/T33 行。残留：SECURITY.md:5「pre-1.0 (`0.1.x`)」、`config.yml:4` `TODO` URL、mdBook 未部署（book.toml:5）、console-api.md 局部漂移（评估时点 :216 仍写 10 工具、:28 仍写 4 类词汇——前者 W3 `fb52046` 已改十四工具表；后者连同 :4「49 路径」、:84「仅 int64 可分区」两处同类 pre-A3 残留由 R8.5 评审修复轮清偿） |
| 社区与采用 | 2 | **2** | 本段 `gh api`（2026-10-30）：stars 0 / forks 0 / open_issues 0 / watchers 0——四轮 12 项能力对采用信号零贡献，印证蓝图 §10.1 反向意见⑥ |

### 0.4 蓝图承诺范围对账（W0-W2）

| 阶段 | 蓝图承诺 | post-R8 实况 | 未清项 |
|---|---|---|---|
| W0 地基 | G1-G6 + 公开面清偿 | **4/6 落地**（G1/G2/G4/G5）；公开面清偿部分完成（README ✅ 本轮；SECURITY/模板 TODO 未清） | G3 Cursor 开放化、G6 管道即代码 |
| W1 Doris 原生写入 | A1/A2/A6/A7/A8 + A4/A5 | **6/8 落地**；出口标准「新用户零 DDL 建表」「Grafana 抓 lag」「重放行数不变」均达成 | A4 VARIANT 直写、A5 向量列（词汇已落，R9 候选） |
| W2 控制面核心 | E2 告警/SLO + E4 MCP | **1/2 落地**（E4 超 blueprint §7.5 的 10 工具预算至 14，双门禁+审计） | E2 告警/SLO（lag/freshness/DLQ 率→Webhook） |
| W3 期权 | C 窄口 / D1 / E3 / E5 / B 按需 | **C 窄口+D1 提前兑现**（成本判断正确：C 实际 ~1.5 人日量级）；其余未动且触发器未响 | E3 GitOps、E5 RBAC、B Iceberg（三触发器制）、D3/D4（不做/永不做的红线不变） |

### 0.5 竞争位置（post-R8）

v1.1.0 的四竞品对位表（§二）左列仍成立，tailrace 侧右列更新：

| 竞品 | post-R8 对位（增量） |
|---|---|
| Vector | 仍是「可观测管道 + VRL」；tailrace 的差距面不变，新增领先面：`/metrics` 原生、DDL 闭环、回填、MCP——单二进制竞品的 Day-2 闭环仍是空白 |
| Airbyte | 仍是多容器 ELT；tailrace「两个二进制 + 2-4GB」定位面新增「建表→写入→补数→部分列更新」全链不经 K8s |
| SeaTunnel | 连接器矩阵的差距**有意保留**（红线），但 CDC 一项经 `debezium-json` 解码覆盖了「已上 Debezium 的库」（消费位）；写入语义（label 幂等/部分列/sequence_col）继续做深 |
| Redpanda Connect | Bloblang vs SQL 不变；控制面（内置 UI+MCP+SSH 部署链）仍是结构性差异 |

三大差距（v1.1.0 §2.2）post-R8 状态：①连接器面窄——不变（5 真+1 dim，mqtt/s3 占位）；②交付语义——仍止于 at-least-once + label 幂等，但 label 分双模式（进程作用域/确定性）且 `SinkCapabilities.transactional` 扩展点已立，无 sink 声明事务；③社区与国际化——不变（0 信号、中文单轨）。**谁会切换**：候选画像从「要写 Doris 的小团队」收窄为「要在 2-4GB 边缘盒子上完成建表→写入→补数全闭环、且能被 AI/脚本经 MCP 操作的 Doris/StarRocks 用户」。

### 0.6 路线展望（post-R8）

- **近期（本文件所在文档轮 W3-W5）**：mdBook 八章刷新与 `mdbook build` 门禁；CHANGELOG 增 Unreleased 段（不预定版本号）；四正典逐条「声明→锚点」核查。**下一发布窗**：G2 迁移窗翻转（column_type.rs:158 常量置 false，未知类型词由 warn+折叠转硬错）排入下个 minor——这是四轮里唯一「随发布必须动行为」的开关。
- **公开面清偿（v1.1.0 §六②③的存量债，半天量级）**：SECURITY.md:5 改「1.x 默认分支修复」；`config.yml:4` `TODO`→真实仓库；mdBook 上 gh-pages（book.toml:5 的 docs job）；~~console-api.md 10 工具/4 类词汇两处漂移修正~~（W3 与 R8.5 评审修复轮已清偿）。
- **中期（下轮候选，蓝图余项）**：A4 VARIANT 直写与 A5 向量列（词汇已落，差 Doris 侧映射+e2e）；E2 告警/SLO（W2 另一半）；回填 kafka 源（分区级种子，pipelines.rs:2277 注释已登记路径）；`tailrace_pipeline_bytes_total` 喂数（metrics.rs:17 声明未喂数，待 sink ack 携带字节）；G3/G6 维持「需要时再做」。
- **长线不变项**：exactly-once / group commit 触发器制（T36 谱系裁决维持）；MQTT 12 个月计时（2027-10-21 到期）；B 湖仓三触发器；**零用户问题的解法在分发不在功能**——0 star 与 12 项能力的并存是本段最强的实证。

### 0.7 本段数字对账表

| 数字 | 含义 | 来源 |
|---|---|---|
| 4 轮 / 12 项 | v1.1.0 后开发轮数与能力面数 | §0.2 表（逐条锚点） |
| 14（8R+6W） | MCP 工具数 | `mcp.rs:90` TOOLS 表（本段实数） |
| 16 | 列类型词汇词数 | `column_type.rs:30-72`（枚举 16 变体，本段点数） |
| 13 | DDL 生成器映射的标量词数 | `tailrace-ddl/src/lib.rs:385` `doris_type`（ddl/src/lib.rs:9-13 模块文档） |
| 500 000 / 64 MiB | 维表行/内存上限 | `dim.rs:36/:39` |
| 3 天 | Doris label 保留期=回填幂等窗口默认 | `conn-doris/src/lib.rs:183-185` |
| 48/48、31/31、38/38+37/37、40/40+39/39、24/24 | e2e 各轮登记值 | docs/design/architecture.zh.md §11.1；dogfooding-round2-2026-10-29.md（本段未重跑） |
| 676 / 5 / 21 / 1.1.0 | 架构正典行数 / SECURITY「pre-1.0」所在行 / crates.io 包数 / 现行版本 | `wc -l docs/design/architecture.zh.md` = 676（评审轮更正——原记「48 / 24」为笔误，与 §0.3 自写的 676 矛盾）；SECURITY.md:5（全文 27 行）；CHANGELOG 1.1.0 段；Cargo.toml:39 |
| 0 / 0 / 0 / 0 | stars / forks / open_issues / watchers（2026-10-30） | 本段 `gh api repos/SeekerGalahad/tailrace` 实查 |
| aead2b5 / 3690090 / 8ce8a95 | origin/main / R8 HEAD / 本文档轮 HEAD | 本段 `git log` 实查 |

---

# tailrace 现状评估与未来展望（v1.1.0 基线）

> **基线**：tag `v1.1.0`（指向 `6cedcf7`，2026-10-25）；工作树 HEAD `b19661b`；评估日 2026-10-26。
> **引用纪律**：性能数字只引 `docs/BENCHMARKS.md`；交付事实只引 `CHANGELOG.md` / `CHANGELOG.md (delivery facts) / docs/BENCHMARKS.md (performance)` / 两份裁决纪要；代码锚点一律 `路径:行号`。文中「本会话实测」均为 2026-10-26 在工作树实跑所得，命令随行写明；未重跑的数字一律注明来源与其测量日。
> **来源注记**：原计划作为本文输入的四份领域评估稿（`/tmp/assess-{code,community,competitive,depth}.md`）在落稿时**不存在于磁盘**。本文全部结论直接取自仓内正典文件与本会话实测，不转述任何二手结论。
> **版本标签注记**：§五 的「v1.1.x / v1.2 / v2.x」是本文的分期框架；仓内正典（roadmap、T18/T36 纪要）使用的是「v1.x / R6 / 12 个月触发器」口径，映射关系在 §五 逐段标注。

**一页纸结论**：

- tailrace v1.1.0 是一个**工程纪律极严、技术语义完整、但尚无任何外部用户**的 Rust 数据接入引擎。北极星「一条 SQL、两个二进制、零 JVM、Doris/StarRocks label 幂等档」（CHANGELOG.md:8 (v1.1.0 section)）已按档位兑现。
- 总体健康分 **7/10**：工程质量 9、技术深度 8、诚实账本 8 拉高；公开面一致性 5、社区 2 拉低（§1.1）。
- 真正的差异化只有三条且都有数据：SQL-first（DataFusion 55.2.0）、Doris/StarRocks 深度工程化、单二进制+内置管理台+一键 SSH 部署（§2.1）。
- 随 1.1.0 带出门的最大技术缺口是 **DF55 树上的 L3 `smoke.sh --full` 未跑**（BENCHMARKS.md:739-741）；最大非技术缺口是公开面若干滞后（SECURITY.md:5「pre-1.0」、README「follows」、模板 `TODO` URL）。
- 下一步不是加功能：T18/T36 已把 group commit / 2PC / WAL / MQTT / deltalake 的触发器全部写死为「真实需求出现」（§4.2），当前外部信号为零——**先清债、先被看见，再谈谱系升级**（§六）。

## 一、现状评估（v1.1.0 基线）

### 1.1 总体健康分：**7 / 10**

分项与依据（权重按「可信 → 可用 → 可维护 → 可传播」排序，不取算术平均）：

| 维度 | 分 | 依据（锚点） |
|---|---|---|
| 工程纪律与质量门 | 9 | CI 8 job（ci.yml:16 rust / :35 msrv / :45 no-default-features / :55 slim-features / :76 macos / :85 web / :102 chaos / :135 smoke），最新 run `40191202705` @ b19661b **success**（本会话 `gh run list`）；全仓 `unsafe` **0 处**（本会话 `grep -rn '\bunsafe\b' engine console tests`）；`cargo fmt --all --check` **exit 0**、`cargo deny check advisories bans licenses sources` **四门 ok**（本会话实跑，cargo-deny 0.20.2）；`yanked = "deny"`（deny.toml:39） |
| 技术深度 | 8 | at-least-once 游标 + flush 点整图提交（architecture.zh.md:176、:193-199）；NO_PROGRESS 写死 3 次不配置化（registry.rs:26-30）；台账 tmp→sync→rename→fsync 与 fail-closed（tailrace-state/src/lib.rs:1-14）；label 幂等 `FlushOutcome::{Success,Dedup}`（conn-doris/src/lib.rs:198-205）；DataFusion 55.2.0（Cargo.toml:66-67）；SSH 八步状态机 **1104 行**（deploy.rs:1-24；本会话 `wc -l`） |
| 诚实账本与内部文档 | 8 | BENCHMARKS.md 748 行 / CHANGELOG.md 133 行 / architecture.zh.md 635 行均带行号锚点；缺口逐条显式登记而非掩盖——L3 未跑（BENCHMARKS.md:739-741）、同机不同日（:377）、跨机不可比（:349-350） |
| 公开面一致性 | 5 | SECURITY.md:5 仍写「pre-1.0 (`0.1.x`)」；README.md:19-22 仍写 v1.1.0 tag「follows」而 tag 已发；issue 模板安全链接 `github.com/TODO/tailrace`（.github/ISSUE_TEMPLATE/config.yml:4）；T38 验收项「收官报告存在」（roadmap.md:366）未兑现——docs/dev/ 仅有 `v1.0-release-2026-10-23.md` |
| 社区与采用 | 2 | 本会话 `gh api repos/SeekerGalahad/tailrace`：stars **0** / forks **0** / open_issues **0** / watchers **0**；issues 全历史 **0**（与 T36 纪要 2026-10-25 复测一致，R5-lineage-judge-20261025.md:17-20）；UI/API 文案中文单轨（README.md:143） |

**7 分的含义**：作为单人维护的工程，它处于上四分位——质量门与诚实账本的严苛程度超过多数同体量开源项目。拉低分数的是两件可直接修的事（公开面滞后、收官报告缺位）和一件代码解决不了的事（零外部用户）。

### 1.2 代码质量摘要

- **结构**：workspace 22 成员（Cargo.toml:8-35）= engine 16 lib crate + 1 bin `tailrace-node` + console 4 crate + `tests/contract`（`publish = false`，tests/contract/Cargo.toml:11）→ **21 包上 crates.io**，lockstep 1.1.0（Cargo.toml:38；architecture.zh.md:505 逐包复核 21×1.1.0）。21 包 description/keywords/categories 三键齐全（本会话逐 Cargo.toml grep，21/21）。
- **测试**：L1 全 workspace **194 passed / 0 failed / 8 ignored**，契约套件 **7/7**（architecture.zh.md:483，2026-10-26 落稿会话实跑——**本文未重跑**）；T33 时点为 190/0/7（roadmap.md:333）。live-gate 测试自 T25 起 `#[ignore]` + CI `--include-ignored` 真跑（ci.yml:125/129/133），跳过计数可见、不再静默计 passed。
- **静态门与供应链**：fmt / clippy `-D warnings` / deny 四门全进 CI rust job（ci.yml:16）；msrv job 以 1.95 真实编译（ci.yml:35-44；ADR-001 roadmap.md:21-26）；slim-features job 用 `cargo tree` 防 rdkafka/openssl 栈泄漏回图（ci.yml:55）。
- **内存安全**：`unsafe` 0 处（本会话 grep）；但**无 `#![forbid(unsafe_code)]`**（grep `unsafe_code` 0 命中）——现状是「实践为 0」而非「编译器保证为 0」。
- **错误处理**：engine `/v1` 错误信封 8 规范码常量单点（tailrace-api/src/lib.rs:18-35）；console zh 字典现值 **39 key / 57 调用点**（messages.rs:12-16；CHANGELOG.md:72 记 T34 交付时 36/50，T35 +3 key/+7 站点）；34 处 `ApiError` 字面量登记为豁免并由测试钉住（CHANGELOG.md:74-75）。
- **锁中毒防御**：runtime 8 处 `state.lock()` 统一 `unwrap_or_else(|e| e.into_inner())`（CHANGELOG.md:100-104）。
- **对标 Rust 一线项目的差距**：无 fuzz（T21/T37 挂起，roadmap.md:359-361）；无 `forbid(unsafe_code)`；文档站无 CI 部署（book.toml:5「CI docs job 未立」）。

### 1.3 社区就绪度摘要

**已就位**：README 双语，4 枚徽章（README.md:3-6）、三命令 quickstart（:36-51）、FAQ 5 问（:85-122）；CONTRIBUTING.md 44 行写明四条本地门（:24-27）与 Conventional Commits（:34-36）；CODE_OF_CONDUCT.md 70 行；SECURITY.md 私密通报流程（:10-20）；issue 模板 bug/feature + `blank_issues_enabled: false`（config.yml:1）；PR 模板带四项检查清单（PULL_REQUEST_TEMPLATE.md:16-19）；CHANGELOG 遵循 Keep a Changelog（CHANGELOG.md:5-6），[1.1.0] 段逐卡入账（:8-122）；mdBook 9 章（docs/book/src/SUMMARY.md:3-11）。

**滞后与缺口（均可低成本修）**：

1. SECURITY.md:5「tailrace is pre-1.0 (`0.1.x`)」——与 1.1.0 现状矛盾，「supported versions」语义已失效。
2. README.md:19-22 / README.zh-CN.md:17-19「v1.1.0 git tag / GitHub release follows」——tag 与 9 资产 Release 已于 2026-10-25T12:30:30Z 发布（本会话 `gh release view v1.1.0`，assets=9）。
3. .github/ISSUE_TEMPLATE/config.yml:4 安全通报 URL 为 `https://github.com/TODO/tailrace/...`——占位未替换，外部用户点击即 404。
4. README.md:14 状态段仍以「R3 complete」开头；基准表（:78-83）未纳入 T32 Doris 档与 T33 DF55 档。
5. mdBook 仅本地构建、未部署（book.toml:5）；外部读者没有在线文档入口。
6. T38 收官报告 `docs/dev/v1.1-release-<date>.md` 不存在（roadmap.md:365-366 列为验收项）。
7. 文案中文单轨（README.md:143）——对英文社区是实质门槛；`message_key` 通道已就位但 en 轨为空（architecture.zh.md:366）。

### 1.4 技术深度摘要

- **数据面**：一条 `INSERT INTO ${ds:sink} SELECT … FROM ${ds:source}`（architecture.zh.md:126；parser 仅 INSERT，tailrace-parser/src/lib.rs:53 `compile`）；parser→IR→DataFusion 55.2.0 物化计划，每语句 parse+plan 一次（architecture.zh.md:162-164）；并行 decode 阈值 `PARALLEL_DECODE_MIN_CHUNKS = 512`（runtime/src/lib.rs:270）；攒批三阈值单一谓词 `flush_due()`（runtime/src/lib.rs:94）。
- **容错语义**：at-least-once；三处 flush 点先 `sink.flush()` 再 `state.save()`（architecture.zh.md:193-199）；Cursor 四型 Kafka/File/JdbcKey/None（tailrace-ir/src/cursor_sourcechunk_impl.rs:4 起）；游标指纹 u64 折叠 + `NO_PROGRESS_LIMIT = 3`（registry.rs:30）；poll 退避 1s→300s 封顶、连续 ≥10 次才交重启循环（architecture.zh.md:227）。
- **DLQ**：坏行在游标跨批前统一处置；Dlq 落盘写失败即 fatal，拒绝跨过未持久化坏行（architecture.zh.md:231-249）。
- **sink 语义**：Doris/StarRocks 同工厂 `SinkFlavor` 分味（architecture.zh.md:264）；label 幂等档已交付并 e2e 重放去重（CHANGELOG.md:128-133）；group commit / 2PC **零实现**（R5-lineage-judge-20261025.md:31-33，grep 0 命中）。
- **控制面**：argon2id + 5 连败锁 60s、会话 token 仅存 sha256、AES-256-GCM 凭据、master key 解析 fail-closed（architecture.zh.md:308）；SQLite 14 表 WAL（:310-312）；openapi 52 path / 70 DTO 全登记（:285）。
- **部署链**：russh 纯 Rust、仅公钥认证、八步状态机 + 任一步失败回滚 + REGISTER 核对 engine_version（deploy.rs:1-24；CHANGELOG.md:42-62）；docker 目标机 e2e 含中途失败回滚场景 **1 passed / 49.13s**（roadmap.md:350）。
- **性能账本**（全部 debug 档、单机、跨机不可比——BENCHMARKS.md:20、:349-350）：T7 transform identity **19.348 M → 30.647 M rows/s（+58%）**（:110）；T3 sink flush **234.4 K → 458.2 K（+95%）**（:136）；T16 裁剪档 node **61,571,936 → 55,788,080 B（−9.4%）**（:365）；T32 doris 单管大批 **33852** vs 同日 file→file **33568**（+0.8%，sink 零可测开销，:372）；T33 DF55 宏基准 **41528 rows/s**（:377，同机不同日 +23.7%，仅登记为无回归）；M4 DF55 微基准首档 identity **237.38 M** / filter **65.89 M** / groupby **53.47 M**（:378）。
- **深度短板**：proto 计划解码 `b64_to_plan` 仍 gated（architecture.zh.md:162）；分区发现只增不减（roadmap.md:388）；jdbc 管道 console 端到端未通（architecture.zh.md:567，引 docs/connectors/jdbc.md §七:116-119）。

## 二、竞争位置

> **口径声明**：下表左列为竞品的**公开产品定位**（架构类别 / 配置语言 / 部署形态），属业界常识性描述——本文**未核验**其星标数等外部数字，故不引用；右列 tailrace 侧每条均有仓内锚点。

| 竞品 | 公开定位 | tailrace 对位（仓内锚点） |
|---|---|---|
| Vector | Rust 单二进制可观测数据管道，转换用私有 VRL 语言 | 同为 Rust 单二进制，但转换用 **SQL（DataFusion）**、不学私有 DSL（roadmap.md:14）；Vector 面向日志/指标，tailrace 面向 **Doris/StarRocks 入库**（roadmap.md:15） |
| Airbyte | 多容器编排（含 Temporal）、资源门槛高的 ELT 平台 | 「No Temporal, no sidecars, no 8GB floor」（README.md:10）；两个二进制、2-4 GB 盒子（roadmap.md:13；README.md:98-99） |
| SeaTunnel | JVM 数据集成平台，以连接器数量取胜 | 零 JVM（README.md:8、:95-97）；红线「不拼连接器数量」（roadmap.md:17）；FAQ 明示「pick a platform when you need one」（README.md:87-93） |
| Redpanda Connect（原 Benthos） | Go 单二进制流处理器，转换用 Bloblang | 同为单二进制、无 JVM；差异在 **SQL vs Bloblang** 与 **内置 Web 控制台**（Vue SPA 内嵌 console 二进制，architecture.zh.md:40） |

### 2.1 三大差异化（真实、有数据支撑）

1. **SQL-first + DataFusion 55.2.0，无私有 DSL**：管道即一条 INSERT…SELECT（architecture.zh.md:126）；计划物化后 transform 微基准 M4 identity 237.38 M rows/s（BENCHMARKS.md:378）——transform 段远非瓶颈，用户只需会 SQL。
2. **Doris/StarRocks 深度工程化**：JSON-lines 载体（T11 实证 CSV 载体在 be-2.1.7 毁毒行，architecture.zh.md:263）、label 幂等 Dedup（conn-doris/src/lib.rs:198-205）、SinkFlavor 同工厂分味、StarRocks 一等 datasource 类型（CHANGELOG.md:30-36）、Doris sink 大批零可测开销 33852 vs 33568（BENCHMARKS.md:372）。竞品多把 Doris/StarRocks 当「又一个 JDBC/HTTP 汇」。
3. **单二进制 + 内置管理台 + 可裁剪 + 一键部署**：console 内嵌 Vue SPA（architecture.zh.md:40）；node 裁剪档 −9.4% 并真实把 rdkafka/openssl 栈踢出链接图（依赖 435→413 包，BENCHMARKS.md:365-367）；SSH 八步部署 + 回滚（CHANGELOG.md:42-62）——边缘盒子场景的「交付完整度」是竞品单二进制方案不自带的。

### 2.2 三大差距（相对竞品，诚实列出）

1. **连接器面窄且是有意为之**：5 个真连接器（源 kafka/file/jdbc；汇 doris/starrocks/file），mqtt/s3 为 1 行占位（architecture.zh.md:110-111、:266-267）；无 CDC、无对象存储、无 HTTP 源。红线（roadmap.md:17）决定它永远不会在「连接器矩阵」上赢。
2. **交付语义止于 at-least-once + label 幂等**：exactly-once（2PC）与 group commit 均在 v1.x/R6（R5-lineage-judge-20261025.md:41、:107）；file/kafka 汇无幂等键，崩窗重放会真重复（R3-升格评审-2026-10-21.md:72）。Flink 系竞品已有端到端 exactly-once。
3. **社区与国际化为零**：0 star / 0 fork / 0 issue（本会话 gh api）；UI/API 文案中文单轨（README.md:143）；文档站未部署（book.toml:5）；无讨论区。竞品均有成熟社区与英文文档。

**谁会切换**：已在用 Doris/StarRocks、被 JVM 平台的资源门槛与运维复杂度困扰、需要在 2-4 GB 边缘盒子上用 SQL 做轻量入库的团队（README.md:92-93 自述目标用户）。

### 2.3 切换成本与障碍（仓内可核事实）

| 面 | 现状 | 对切换者的含义 |
|---|---|---|
| 构建前置 | Rust ≥ 1.95 + Node.js 22 + pnpm（README.md:34）；或 `cargo install tailrace-node --version 1.1.0 --locked`（README.md:21） | 源码构建需三套工具链；crates.io 安装只需 Rust——console 包自带 SPA 产物（CHANGELOG.md:186-189） |
| 首次运行 | 三命令 quickstart（README.md:36-51）；首启 admin 密码与 bootstrap token **仅打印一次**（README.md:42-43） | 五分钟可见界面；但密码/令牌一次性打印对自动化部署不友好 |
| 产物体积 | release 档 node 默认 61,571,936 B，裁剪档 55,788,080 B（BENCHMARKS.md:365） | 单文件分发，无运行时依赖；边缘盒子可接受 |
| 批量上机 | SSH 八步链 + 回滚，仅公钥认证（deploy.rs:8-14；CHANGELOG.md:42-62） | 已有 SSH 密钥分发体系的团队零摩擦；host key 首连即信、无 known_hosts 钉住（deploy.rs:12-14）需在可信 LAN |
| 语言 | UI/API 文案中文单轨（README.md:143） | 非中文团队目前不可用 |
| 已知功能坑 | jdbc 管道 console 端到端未通（architecture.zh.md:567） | MySQL/PG 源用户会在部署期撞到 |
| 文档 | 架构档 635 行中文（docs/design/architecture.zh.md）；mdBook 未部署（book.toml:5） | 需 clone 仓库或本地 `mdbook build` 才能读 |

## 三、v1.1.0 交付了什么（CHANGELOG.md:8-122）

| 项 | 关键数字 / 事实 | 锚点 |
|---|---|---|
| Doris/StarRocks sink 宏基准（T32） | file→doris 单管大批 **33852** rows/s vs file→file **33568**（+0.8%）；小批 500 行 **11017**（−67%）；P=4 小批 **26781**（2.43× 单管小批，−21% vs 大批）；P=4 poll 地板批 **97752**（2.89×）；starrocks node 直跑 **8750**（口径不同源） | CHANGELOG.md:20-29；BENCHMARKS.md:641-646 |
| starrocks 一等 console 类型（T39 ⑥） | connJson 复用 doris 形态（jdbcUrl+streamLoadUrl 必填）；独立 table kind=starrocks、cursor NONE；不再需 `node run` 旁路 | CHANGELOG.md:30-36 |
| `columnsJson` DTO（T39 ⑤） | `GET /api/datasources/tables` 行携 `[name,type]` 对；web `db.table.` 列补全端到端 | CHANGELOG.md:37-41 |
| SSH 部署链（T35） | 八步状态机 PENDING→PREFLIGHT→UPLOAD→UNPACK→CONFIGURE→START→HEALTH_WAIT→REGISTER→REGISTERED；russh 纯 Rust、仅公钥；hosts[] 批量 / nodeId 重部署；任一步失败回滚；start/stop/restart/rollback/uninstall 同步 + upgrade 异步；deploy.rs **1104 行** | CHANGELOG.md:42-62；deploy.rs:1-24 |
| i18n 承诺兑现（T34） | engine 10 处错误携 `code`（8 规范码）；console 字典 36 key / 50 调用点（交付时）；38 处信封携 `message_key` | CHANGELOG.md:66-75 |
| DataFusion 48.0.1 → 55.2.0（T33） | 七版本直跳；arrow 59.3.1 / sqlparser 0.62.0 / prost 0.14.4；MSRV 不变 1.95；契约 golden 零字节 diff；宏基准 **41528** rows/s（+23.7% 同机不同日，登记为无回归非增益） | CHANGELOG.md:76-99 |
| 锁中毒防御（T39 ④） | runtime 8 处 `state.lock()` 改 `unwrap_or_else(\|e\| e.into_inner())` | CHANGELOG.md:100-104 |
| 供应链收紧（T39 ⑦） | utoipa-swagger-ui 10.0.2 → zip 9.0.5；三条 RUSTSEC ignore 回收；`yanked = "deny"` | CHANGELOG.md:105-112；deny.toml:39 |
| 安全补丁 | ctutils 0.4.4（RUSTSEC-2026-0385）、tokio 1.53.3（RUSTSEC-2026-0386） | CHANGELOG.md:116-122 |
| 发布面 | 21 包 crates.io lockstep 1.1.0；GitHub Release **9 资产**（8 tarball + checksums.txt） | architecture.zh.md:505；本会话 `gh release view v1.1.0` assets=9 |

**v1.1.0 不包含（避免误读）**：

- T36 谱系裁决是**纯文档决议**，无任何用户可见变更（CHANGELOG.md:11-14）——group commit / 2PC / WAL 代码面仍为零。
- `POST /api/nodes/{id}/migrate` 与票据链 `ticket` 仍为 501（CHANGELOG.md:59-60；architecture.zh.md:360），属 P2。
- SSH 链不支持密码认证、不钉 known_hosts（deploy.rs:12-14）。
- DF55 树的 Doris 全链 smoke **未重跑**（CHANGELOG.md:96-99）。
- mqtt / s3 连接器仍为 1 行占位（architecture.zh.md:110-111），与 1.0.0 无变化。
- 文案仍中文单轨；`message_key` 是通道不是翻译（CHANGELOG.md:66-75；README.md:143-145）。

## 四、已知局限（诚实评估）

### 4.1 明确不做（红线，roadmap.md:17）

1. **不拼连接器数量**——mqtt/s3 占位不注册（architecture.zh.md:266-267）；S3 栈会把 T16 的 −9.4% 轻量卖点吃回（R3-升格评审-2026-10-21.md:95）。
2. **不做真正流式 join / 水位线 / 状态后端**——README FAQ 明示为 non-goals（README.md:90-91）；SQL 面仅支持 INSERT（tailrace-parser/src/lib.rs:53）。

另两条实质性「不做」：exactly-once 不在 1.x 默认交付（北极星按档位交付，CHANGELOG.md:8 (v1.1.0 section)）；文案不做多语言（中文单轨，README.md:143）。

### 4.2 延后项——T18 升格评审（2026-10-21）与 T36 谱系裁决（2026-10-25）

| # | 议题 | T18 结论 | T36 更新 | 续挂触发器 |
|---|---|---|---|---|
| ① | deltalake sink | 排 v1.x（硬耦合 arrow ^59） | 硬耦合**已松**（T33 后 arrow 59.3.1），仍需求门控 | lakehouse 真实需求（R5-lineage-judge-20261025.md:160） |
| ② | DataFusion 48→55 | 排 v1.x + RUSTSEC 硬触发 | **已消费**（T33 落地） | — |
| ③ | StarRocks/Doris 2PC | 排 v1.x | 维持；**谱系预定分支**（代价封闭性：只动 sink 协议面） | exactly-once 硬需求 + 实际损害（:147-149；估 ~3-4 人日） |
| ④ | group commit | 排 v1.x，先补基准 | 维持 v1.x/**R6**；T1 已满足；T2 瓶颈归因成立（小批域 45.4 ms/次 FE 往返）但「默认档不达标」被同数据否定；T3 机制面 2.43× 成立、需求面未触发 | ④-T2'（真实小批多管道 + 实际损害）/ ④-T3'（外部信号）（:95-98；R6 卡估 ~2-3 人日，:100-105） |
| ⑤ | 本地两步 WAL | 排 v1.x | 维持；**仅限翻转场景**——损害落在无幂等键 sink（file/kafka）才开卡 | :137-143、:150-151（估 ~3 人日） |
| ⑥ | MQTT 源 | 不升格；12 个月计时 | 计时中：起算 2026-10-21，**2027-10-21** 无触发即从 P2 移除 | issue ≥3 / 自用场景 / PoC 先行（R3-升格评审-2026-10-21.md:85；R5-lineage-judge-20261025.md:162） |
| ⑦a | S3/CDC | 排 v1.x；CDC 走 debezium JSON 格式廉价路径 | 无变化 | JDBC 轮询不满足→debezium decoder（~1.5 人日）；①后→S3（R3-升格评审-2026-10-21.md:96） |
| ⑦b | live-gate 形态 | 立即整改 | **已落地**（T25） | — |

### 4.3 随 v1.1.0 如实携带的未清债务（architecture.zh.md:562-570）

1. **T33 L3 `smoke.sh --full` 未在 DF55 树上跑**（磁盘 <5G 红线两次拦截）——引擎覆盖以 L1 契约 + 宏基准行数守恒为限（BENCHMARKS.md:739-741；CHANGELOG.md:96-99；roadmap.md:333）。T33 因此**不标 ✅**。
2. **SSH 部署 e2e 未入 CI chaos job**——需 `--add-host=host.docker.internal:host-gateway` 回连与 v1.0.0 fixture 两处适配在 Linux docker 主机验证（roadmap.md:368 ⑤）。
3. **jdbc 管道 console 端到端未通**——`build_plan_table` 取 `bootstrap` 键而 mysql/pg 源只有 jdbcUrl，部署期才炸（architecture.zh.md:567）。
4. **分区发现单向**——死分区项永不清理、cursor 携带死项（roadmap.md:388）。
5. **T21/T37 fuzz 挂起**（roadmap.md:359-361）。
6. **OTLP 默认开未决**（roadmap.md:199）；proto 计划解码 gated（architecture.zh.md:162）；`migrate`/`ticket` 端点留 501（architecture.zh.md:360）。
7. **T36 裁决输入缺口**：BENCHMARKS §6.4 Doris 六档系 DF48 树测得，DF55 树 Doris 档未复测（R5-lineage-judge-20261025.md:27-30）。

## 五、未来路线展望

### 5.1 近期 v1.1.x（= 仓内「post-release 窗口 / T38 尾注」口径；纯清债，不加功能）

1. **L3 smoke 本地补跑**：腾出 ≥5G 磁盘，按 BENCHMARKS §6.4 环境配方（:679-684）起 Doris 栈跑 `smoke.sh --full`；绿后 T33 补 ✅、改写 BENCHMARKS §6.5「本小节不覆盖」段、顺带复测 DF55 树 Doris 六档填 T36 缺口。这是 1.1.0 唯一带出门的引擎覆盖缺口，**优先级最高**。
2. **CI SSH e2e**：在 ubuntu runner 验证 `--add-host=host.docker.internal:host-gateway` 回连与 fixture 版本钉法，把 `nodes_deploy_docker_e2e` 加入 chaos job 的 `--include-ignored` 清单（ci.yml:102-133 同款形态）。
3. **README / 公开面打磨**：改 README.md:19-22 与 README.zh-CN.md:17-19 的「follows」为已发布；状态段改以 v1.1.0 开头；基准表补 T32/T33 行；SECURITY.md:5 改为「1.x 默认分支修复」；替换 config.yml:4 的 `TODO` 为 `SeekerGalahad`；补写 `docs/dev/v1.1-release-2026-10-25.md` 收官报告（roadmap.md:366 验收项）。
4. **jdbc console 端到端修通**（architecture.zh.md:567）——这是用户会撞到的功能性 bug，不是债。

### 5.2 中期 v1.2（= 仓内「v1.x / R6」口径；全部**触发器制**，不按日历排）

按 T36 裁决（R5-lineage-judge-20261025.md），谱系升级的开卡顺序与条件已写死：

- **group commit（R6 卡，~2-3 人日）**：触发器 ④-T2'（真实出现 ≥4 管道 × <1000 行批且吞吐差距对业务有损害）或 ④-T3'（外部点名）；卡内方案 = datasource 级 `group_commit` off/sync/async 透传 **+ 引擎侧跨管道攒批合并作为同目标替代方案一并评审**；实验三律沿 T32（同压 pollRows、大小批双锚、行数守恒）；在 DF55 树重测前后对比（:100-105）。目标区数据：P=4 小批 26781 vs 单管大批 33852 的 −21% 缺口（BENCHMARKS.md:374）。
- **2PC 事务接口（③，~3-4 人日）**：触发器 = exactly-once 硬需求 + 实际损害案例；预定为谱系卡首选分支——Sink trait 增 prepare/commit/abort 默认实现、conn-doris `two_phase_commit` header + PRECOMMITTED 状态机 + 孤儿事务对账（:147-149；R3-升格评审-2026-10-21.md:54）。
- **本地 WAL（⑤，~3 人日）**：**仅当**损害落在无幂等键 sink（file/kafka）时开卡，与 2PC 不同时做（:137-143）。
- **升级量化门**：任何谱系卡须在 BENCHMARKS 增前后对比；WAL 的可预期代价 = flush 点持久写翻倍，直接打击 41528 锚点与 97752 天花板（:120-126）。

### 5.3 长期 v2.x（= 仓内「v1.x 带长触发器 / P2」口径）

- **MQTT 源**：12 个月计时器 **2027-10-21** 到期（R5-lineage-judge-20261025.md:162）；触发 = issue ≥3 或自用 MQTT→Doris 场景，先 1 人日 PoC（rumqttc async→同步 pull 包装 + Cursor 新变体）再立项，估 ~4 人日（R3-升格评审-2026-10-21.md:85-86）。无触发即从 P2 移除——这是项目**唯一带「自动删除」的路线项**。
- **deltalake sink**：技术前置已就绪（arrow 59.3.1 ≥ ^59，R5-lineage-judge-20261025.md:160），估 ~5-7 人日，仍等 lakehouse 真实需求（R3-升格评审-2026-10-21.md:24-30）。
- **S3 / CDC**：CDC 优先走 `tailrace-decode` 新增 debezium JSON 格式（~1.5 人日，conn-kafka/runtime 零改动，R3-升格评审-2026-10-21.md:94、:97）；S3 源押后到 deltalake 之后以共享 object_store 栈摊薄体积代价（:95）。
- **可观测**：OTLP 是否默认开启按真实部署反馈裁决（roadmap.md:199）。

### 5.4 社区：什么会驱动采用

当前基线：仓库建于 2026-10-16（本会话 gh api `created_at`），10 天内发 v0.1.0 → v1.0.0 → v1.1.0 三个 tag、21 包上 crates.io、9 资产 Release ×2，但外部信号全零。按「最小可信里程碑」排序：

1. **首个 star / 首个 issue**：前提是**有人能读到**——mdBook 部署到 gh-pages（book.toml:5 的未立 docs job）、README 英文面消除「follows」类滞后、SECURITY/模板 TODO 修正。零成本的「被发现面」先于任何功能。
2. **首个真实用户场景**：T36 的全部触发器（④-T2' / ③T1 / ⑤T1 / ⑥）都在等「真实部署」——一篇「从 Kafka 到 Doris 的 2 GB 盒子实战」教程 + 可复现 compose 栈，比新功能更能把触发器打响。
3. **首个外部贡献者**：当前 CONTRIBUTING.md:9-30 只讲门禁；缺「good first issue」标签、架构导读入口（architecture.zh.md 已是现成 635 行导读，但只有中文）、模块所有权图。英文轨（`message_key` en 轨空、architecture 无英文版）是外部贡献的硬门槛。
4. **可信度放大器**：BENCHMARKS 的诚实口径（跨机不可比、同机不同日、±17% 噪声自认，:380）本身是稀缺的社区资产——把它写成一篇对外文章比再测一轮更有传播价值。

## 六、建议（按影响排序，Top 5）

1. **【最高】补跑 L3 `smoke.sh --full` 并收口 T33**——1.1.0 唯一的引擎覆盖缺口，关系 DF55 树 Doris 全链是否真绿；同窗复测 Doris 六档填 T36 裁决输入缺口。成本 = 一次 ≥5G 磁盘窗口 + 约十余分钟（T11 首跑 11m20s 口径，BENCHMARKS.md:336）。
2. **【高】公开面一次性对齐 v1.1.0**——README.md:19-22 / README.zh-CN.md:17-19「follows」、SECURITY.md:5「pre-1.0」、config.yml:4 `TODO` URL、README 状态段与基准表、补 `docs/dev/v1.1-release-*.md`。全是文本改动，半天可清，却直接决定第一个访客的信任。
3. **【高】部署文档站 + 英文入口**——mdBook 上 gh-pages（新增 docs job）；至少为 architecture.zh.md 与 FAQ 提供英文摘要页。没有可点开的英文文档，star / issue / contributor 三个里程碑都不会发生。
4. **【中】修 jdbc console 端到端 + SSH e2e 入 CI**——前者是用户可见 bug（architecture.zh.md:567），后者让 v1.1.0 的旗舰特性（部署链）有持续回归保护（roadmap.md:368 ⑤）。两项合计 ~1-2 人日。
5. **【中】加 `#![forbid(unsafe_code)]` 并启动 T37 fuzz 最小集**——unsafe 已为 0，一行 lint 把「实践为 0」变成「编译器保证为 0」；fuzz 两目标（parser SQL 文法 / decode 毒输入，roadmap.md:361）以 T4/T11 毒语料做种子，30 分钟零 panic 即可登记。这是对标 Rust 一线项目质量门的最后两块短板。

**不建议现在做的事**：在零外部信号下启动 group commit / 2PC / WAL / MQTT / deltalake 任何一张卡——T18 与 T36 两轮裁决已把触发器写死，提前开卡等于推翻自己的纪律。

## 七、数字对账表（供评审逐项核对）

本文引用的每个关键数字与其唯一来源行；评审时以来源文件现值为准，本文若与来源不一致以来源为正。

| 数字 | 含义 | 来源（行号） |
|---|---|---|
| 33852 / 33568 | file→doris 单管大批 vs 同日 file→file 锚点（+0.8%） | BENCHMARKS.md:372、:641-642；CHANGELOG.md:23-24 |
| 11017 | file→doris 单管小批 500 行（−67%） | BENCHMARKS.md:373、:643；CHANGELOG.md:25 |
| 26781 / 2.43× / −21% | P=4 并发小批 | BENCHMARKS.md:374、:644；CHANGELOG.md:25-26 |
| 97752 / 2.89× | P=4 并发 poll 地板批 | BENCHMARKS.md:375、:645；CHANGELOG.md:27 |
| 8750 | file→starrocks node 直跑档（口径不同源） | BENCHMARKS.md:376、:646；CHANGELOG.md:27 |
| 41528 / 2.408 s / +23.7% | DF55 宏基准 file→file vs DF48 锚点 33568（同机不同日，无回归） | BENCHMARKS.md:377、:710；CHANGELOG.md:93-95 |
| 237.38 M / 65.89 M / 53.47 M | M4 DF55 transform 微基准首档 identity / filter / groupby | BENCHMARKS.md:378、:711-713 |
| 19.348 M → 30.647 M（+58%） | T7 transform identity（Intel 机） | BENCHMARKS.md:110 |
| 234.4 K → 458.2 K（+95%） | T3 sink flush（磁盘敏感） | BENCHMARKS.md:136 |
| 61,571,936 → 55,788,080 B（−9.4%） | T16 裁剪档 node release 体积 | BENCHMARKS.md:365；CHANGELOG.md:176 |
| 435 → 413 | T16 依赖图包数 | BENCHMARKS.md:367 |
| 44/44、11m20s | T11 `smoke.sh --full` 首跑 | BENCHMARKS.md:336 |
| 45.4 ms/次 | 小批域每 flush 同步 stream load 往返 | R5-lineage-judge-20261025.md:57-58 |
| 36 key / 50 call sites | T34 交付时 console 字典规模 | CHANGELOG.md:72 |
| 39 key / 57 调用点 | console 字典现值（T35 后） | messages.rs:12-16 |
| 8 规范码 / 10 站点 | engine `/v1` 错误 `code` | CHANGELOG.md:67-68；tailrace-api/src/lib.rs:18-35 |
| 194 / 0 / 8 | L1 workspace 测试 passed / failed / ignored（2026-10-26 落稿会话） | architecture.zh.md:483 |
| 190 / 0 / 7 | 同上，T33 时点 | roadmap.md:333；BENCHMARKS.md:741 |
| 1 passed / 49.13 s | T35 docker 目标机 e2e（含回滚场景） | roadmap.md:350 |
| 1104 行 | console/src/deploy.rs | 本会话 `wc -l` |
| 22 / 21 | workspace 成员 / crates.io 发包数 | Cargo.toml:8-35；tests/contract/Cargo.toml:11 |
| 8 job | ci.yml | ci.yml:16/35/45/55/76/85/102/135 |
| 9 资产 | GitHub Release v1.1.0 | 本会话 `gh release view v1.1.0`；architecture.zh.md:505 |
| 0 / 0 / 0 / 0 | stars / forks / open_issues / watchers（2026-10-26） | 本会话 `gh api`；R5-lineage-judge-20261025.md:17-20（2026-10-25 同值） |
| 0 | 全仓 `unsafe` | 本会话 grep |
| 2026-10-21 → 2027-10-21 | MQTT 12 个月触发器起止 | R5-lineage-judge-20261025.md:162；R3-升格评审-2026-10-21.md:85 |

---

*本文落稿：2026-10-26，工作树 HEAD `b19661b`（tag v1.1.0 = `6cedcf7`）。**本会话实跑并通过**：`cargo fmt --all --check`（exit 0）、`cargo deny check advisories bans licenses sources`（四门 ok，cargo-deny 0.20.2）、`gh run list`（run 40191202705 success）、`gh release view v1.1.0`（9 资产）、`gh api repos/SeekerGalahad/tailrace`（stars/forks/issues/watchers 全 0）。**未在本会话重跑**：`cargo test --workspace`、`smoke.sh`、任何基准——相关数字均引自正典文件并带锚点。*
