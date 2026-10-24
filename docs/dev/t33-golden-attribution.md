# T33 DF 48.0.1 → 55.2.0 升级 golden 归因表（2026-10-24）

> 任务：roadmap §四 T33「DataFusion 48→55.2.0 升级」第②步核心迁移。
> 纪律要求（卡内③）：契约 golden 重生成**显式登记归因表**，每条字节 diff
> 归因「proto 版本字段变化 / 默认值漂移 / 语义漂移」三选一，语义漂移项
> 逐条论证等价性或修复；**禁止手编 golden 凑绿**。
> 前置输入：`/tmp/t33-changes.md`（第①步 breaking-changes 核实清单，
> 12 触面文件逐 API 对照 + DF 49-55 CHANGELOG）。

## 一、结论速览

| 面 | 结果 | 说明 |
|---|---|---|
| golden_layer_a.json（proto 字节） | **0 条 diff** | DF55 下 `UPDATE_GOLDEN=1` 重生成与 DF48 fixture **逐字节一致**（`diff` 空、`git diff --stat` 空） |
| golden_layer_b.txt（执行 pretty 快照） | **0 条 diff** | 同上，逐字节一致 |
| 非 golden 行为面 | **2 条漂移**，均已归因/处置 | 见 §三（metrics 展示格式 1 条 + 指标面解锁 1 条） |
| 重写 select_sql（C3，非 golden 契约面） | **0 条 diff** | sqlparser 0.55 vs 0.62 Display 输出逐字节一致（探针实证，§四） |

**归因二分框架（清单 §D 预设）**：本仓 proto 编码无版本信封
（`logical_plan_to_bytes_with_extension_codec` 是裸 LogicalPlanNode 消息，
55 版 bytes/mod.rs 实测无头部/版本字段）——因此**不存在「proto 版本字段
变化」与「默认值漂移」两个通道**，任何 diff 只可能是第三类「计划树形态
（语义）漂移」。实测结果：一类都没有出现，零 diff。

## 二、golden 重生成协议与证据

| 步骤 | 命令 | 结果 |
|---|---|---|
| 1. 不重生成直接跑（漂移检测） | `CARGO_PROFILE_DEV_DEBUG=0 cargo test -p tailrace-ir --locked` | 2 passed（layer_a/layer_b 双绿，fixture 未动） |
| 2. 显式重生成 | 备份 fixture 至 /tmp 后 `UPDATE_GOLDEN=1 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p tailrace-ir --locked layer_` | 2 passed，fixture 被重写 |
| 3. 字节级归因 | `diff /tmp/golden_layer_a.json.bak tests/golden_layer_a.json`；layer_b 同 | 双双无输出（IDENTICAL）；`git diff --stat engine/crates/tailrace-ir/tests/` 空 |

逐条归因（全部 diff 条数为 0，逐条=逐层）：

| # | 层 | diff 字节 | 归因 | 依据 |
|---|---|---|---|---|
| 1 | layer_a（proto_plan_b64） | 0 | 无漂移：消息 schema 逐字段一致（LogicalPlanNode oneof projection=3/selection=4/empty_relation=10 及 Projection/Selection/EmptyRelation/LogicalExpr/BinaryExpr/Schema/Field/ScalarValue 字段号，48↔55 清单 §D 已逐字段对照）；prost 0.13.5→0.14.4 不改 wire format；本层无语义漂移（简单 filter/projection 计划树 49-55 分析器未改形） | 步骤 1/2/3 实测 + 清单 §D 源码对照 |
| 2 | layer_b（执行输出 pretty） | 0 | 无漂移：Utf8/Int64 列 filter/projection 语义与 pretty 渲染两版一致 | 同上 |

**零 diff 的机制解释（为何 7 个大版本零漂移）**：golden 通道上的消息
（projection/selection/empty_relation/column/literal/binary_expr）恰是
DF 分析器 49-55 改动最少的面；`op: format!("{op:?}")` 的操作符字符串化
两版同源（清单 §D 逐字比对）。

## 三、非 golden 行为面漂移（2 条，逐条结论）

### B-1（语义层·展示漂移）：metrics 计数渲染 `8000` → `8.00 K`

- **现象**：`explain_metrics_batch_count_drops` 首跑 FAILED——DF55 的
  EXPLAIN ANALYZE 表中 `output_rows=8000` 变为 `output_rows=8.00 K`
  （AggregateExec 与 ProjectionExec 行均是）。
- **归因**：**语义层的展示格式漂移**（非 proto 字段/默认值）。DF55 起
  Count 指标 Display 走 `human_readable_count`
  （datafusion-common `src/display/human_readable.rs:50-65`：≥1000 →
  `"{:.2} K"` 形态；datafusion-physical-expr-common
  `src/metrics/value.rs:51-55` `impl Display for Count` 改调它）。
- **等价性论证**：`human_readable_count(8000)` ≡ 8000——纯十进制展示
  换算（8000/1000=8.0），无舍入歧义（该函数对 K 档保留两位小数，8000
  恰为整千）；执行行数、批数、算子集合实测均未变（见 B-1 修复后断言）。
- **修复**：断言改双形态容忍（`output_rows=8000` OR `output_rows=8.00 K`），
  并保留 `AggregateExec` 存在性断言；测试语义不变（钉「聚合算子执行了、
  8000 行全流过」）。物理优化器大改（清单 C2：EnsureRequirements 合并/
  hash agg 流拆分等）**未**改变本测试计划的算子形态与批行为
  （fixed=1 / pinned=8000 断言原样通过）。
- **无测试外的生产面影响**：本仓不解析 EXPLAIN 文本（transform 只把
  explain 表作为诊断输出），metrics 展示漂移不触达 wire/存储。

### B-2（指标面解锁·清单 E2 兑现）：`output_batches` 全算子可见

- **现象**：DF48 下 `output_batches` 仅 join/coalesce/unnest 手工埋点；
  DF55 的 EXPLAIN ANALYZE 中 AggregateExec/ProjectionExec 自带
  `output_batches=1`（首跑失败输出直接可见）。
- **归因**：非漂移，是**行为超集**（BaselineMetrics 标准化：
  datafusion-physical-expr-common `src/metrics/baseline.rs:69-98`，
  `output_bytes`/`output_batches` 入标准指标，RecordOutput 自动记录；
  DF48 时代 joins/utils.rs 的手工计数已删）。
- **处置**：测试注记按 E2 改写（DF48 限制已解除）；新增
  `output_batches=` 存在性断言把解锁钉进机器门。
- **下游**：BENCHMARKS §4.4「output_batches 指标面窄（:184-187）」一行
  已过时，**留待 T33 后续 L4/BENCHMARKS 步骤统一改写**（本步不越文件域）。

## 四、C3（sqlparser Display 漂移）实证：零漂移

清单 C3 预告 sqlparser 0.55→0.62 的 Display 格式可能漂移（影响生产 IR 的
debug_sql_echo/计划缓存键，非 golden 面）。用临时探针（parser crate
integration test，跑完即删）对 `examples/pipeline.sql` 双语句取
`rewrite_query` 产物 `select_sql` 全串：

- DF48（sqlparser 0.55.0）输出：`/tmp/select_sql_48.txt`
- DF55（sqlparser 0.62.0）输出：`/tmp/select_sql_55.txt`
- `diff` 结果：**逐字节一致**（`SELECT after.order_no AS ... WHERE
  after.table = 'SHOPDB.ORDERS' AND op_type IN ('I', 'U')` 两条全同）。

结论：debug_sql_echo 与计划缓存键在升级面**零漂移**，C3 注销
（残余风险仅在用户写出含冷门 AST 节点的 SQL 时，parser 子串级 golden
不覆盖，已属既有测试盲区、非本次引入）。

## 五、编译面适配台账（对清单的勘误：实测多 4 处）

清单 A 区预告 2 处必改（A1/A2，均 tailrace-ir）；实测编译另破 4 处，
全部位于 12 触面文件内，逐条登记：

| # | 变更点 | 清单状态 | 实测与修法 |
|---|---|---|---|
| A1 | `LogicalExtensionCodec::try_decode`/`try_decode_table_provider` ctx 参数 `&SessionContext`→`&TaskContext` | ✅ 预告命中 | placeholder.rs 两方法签名改 `&TaskContext`，Default 透传同型，方法体零改动 |
| A2 | `logical_plan_from_bytes_with_extension_codec` 第 2 参改 TaskContext | ✅ 预告命中 | lib.rs 传 `&ctx.task_ctx()`（`SessionContext::task_ctx` 55 版仍在，context/mod.rs:2038 实测） |
| X1 | `datafusion::execution::task_context` 模块消失 | ⚠️ 清单漏（预告的是"task_context 或等价路径"，模块本身已删） | 改 facade 再导出 `datafusion::execution::TaskContext`（execution/mod.rs `pub use datafusion_execution::*` ← 其 lib.rs:54） |
| X2 | `TableProvider::as_any` 成员方法移除（trait 改 `Any + Debug + Sync + Send` 超 trait，datafusion-session table.rs:52） | ❌ 清单漏（B 区误记"其余面不变"） | impl 删 `as_any` 方法（trait 不再有此成员） |
| X3 | `TableReference` 从 `datafusion::sql` 迁走（datafusion-sql lib.rs 仅再导出 sqlparser） | ❌ 清单漏 | 两处改 `datafusion::common::TableReference`（datafusion::common = datafusion_common 全量再导出，lib.rs:798-800） |
| X4 | sqlparser 0.62 `ObjectNamePart` 新增 `Function(_)` 变体（ast/mod.rs:423）致 parser 非穷尽匹配 | ❌ 清单漏（B 区只核了 Identifier 同形） | parser `ident_name` 加显式 `Function(_) => None` 臂（语义=既有多段拒绝：`${ds:}` 占位符只认单段纯标识符） |

## 六、依赖面与 deny 四门（C4 兑现）

- **锁面**：datafusion/datafusion-proto 55.2.0；arrow **59.3.1**（清单
  记 DF 自身 workspace 为 59.2.0，注册表解析取其声明区间内最新 patch，
  arrow/parquet 家族一致 59.3.1）；sqlparser 0.62.0；prost 0.14.4；
  object_store 0.13.2。新增传递 crate（datafusion-session/
  datafusion-proto-models/datafusion-pruning 等）与纯 Rust 压缩栈
  （libbz2-rs-sys/liblzma 系，替换 C 版 libbz2/xz2——**paste 1.0.15 与
  lzma-sys/xz2/thrift 出图**）。
- **advisories**：ok。RUSTSEC-2024-0436（paste unmaintained）豁免随 crate
  出图**退役**（deny.toml ignore 清空并留档注释；运行时 cargo-deny 实证
  "advisory was not encountered"）。
- **licenses**：ok（新增 `bzip2-1.0.6` 进 allow——libbz2-rs-sys 0.2.5 经
  bzip2 0.6.1→compression-codecs→async-compression→datafusion-datasource
  入图；bzip2/libbzip2 v1.0.6 为 SPDX 标准宽松许可（BSD 风格再分发条款），
  与既有 BSD-2/3 同族，非回退门意义上的不可豁免冲突）。既有 ISC 未命中
  警告为 main 既有（stash 对照实证），未动。
- **bans / sources**：ok（multiple-versions=warn 档位，警告为既有现象）。

## 七、门禁与验收状态（本步）

| 门 | 命令 | 结果 |
|---|---|---|
| L0-fmt | `cargo fmt --all --check` | ✅（rustfmt 施于 transform 断言改写处后复检通过） |
| L0-clippy | `CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --locked -- -D warnings` | ✅ exit=0 |
| L0-deny | `cargo deny check advisories bans licenses sources` | ✅ 四门 ok，exit=0 |
| L1 | `CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked` | ✅ **190 passed / 0 failed / 7 ignored**（58 个测试二进制；ignored=7 与 T25 后账本口径一致——live-gate 标记面：api 1/jdbc_sources 2/conn-kafka 2/chaos 1/t6 1） |
| workspace check | `CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace --all-targets --locked` | ✅（含 benches/runtime tests/jdbc tests，覆盖清单 F 区 6 文件） |

未做（属 T33 后续步骤，非本步文件域）：L3 `smoke.sh --full`、L4 前后
同机同日对比与 BENCHMARKS「DF 升级前后」小节、BENCHMARKS §4.4 两行
限制改写（E1 memory pool 注记 55 版逐字仍在=维持；E2 见 §三 B-2）、
裁剪档 check、git commit/push。

## 八、E1 复核（memory pool 部分算子不强制）

datafusion-execution 55.2.0 `src/runtime_env.rs:429` 逐字仍在：
`/// Note DataFusion does not yet respect this limit in all cases.`
——BENCHMARKS §4.4 该条限制**在 55 上维持原样**（清单 §E 预核一致，
本步一手复核）。`with_memory_limit` 行为面同构（TrackConsumersPool
Greedy，同清单 §E）。
