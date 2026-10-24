# tailrace 基准报告（BENCHMARKS.md）

> R2 性能轮（roadmap §四）的唯一数字账本：T1（本文件）建立基线；
> T3/T5/T6/T7 每项优化的**前后对比表**追加到对应小节，不覆盖基线行。
> 首批基线采集：2026-10-17，工作树 HEAD `8e3b1fd`（W1 波内并行提交仅涉及
> console/web，decode/transform/conn-file 三 crate 自 `967b91a` 起无源码变更，
> 基准产物与 HEAD 对应源码一致）。

## 一、机器规格

| 项 | 值 |
|---|---|
| 机型 | MacBook Pro (2019, Intel) |
| CPU | Intel Core i7-9750H @ 2.60GHz，6 核 12 线程 |
| 内存 | 32 GB |
| 系统 | macOS 15.7.4（x86_64） |
| 工具链 | rustc 1.99.1（stable，workspace rust-version 1.95） |
| 基准库 | criterion 0.8.2（workspace dev-dependency） |

> 所有数字均为该机器单机测量；跨机器不可比。优化前后对比必须同机同日复测。

## 二、微基准（criterion）

### 方法学

- 命令：`cargo bench -p <crate>`（bench profile = release 继承；`--no-run` 仅做编译验证，进 CI）。
- 每次迭代 = **一个 8000 行微批**（decode/transform；等于 runtime 默认 `poll_rows`=8000）或
  **一个 10000 行 flush**（sink；等于默认 `flush_rows`=10000）。
- `Throughput::Elements(行数)` → criterion 报告行/秒（下表取中位估计）。
- 种子确定性生成（下标派生），迭代外构造一次；含 `ColBatch::finish()`（decode）
  与 collect 全流程（transform），不涉磁盘——sink 组除外（见下）。
- 默认采样（warmup 3s + 100 samples ≈ 5s）；解析度不足以填满 5s 时 criterion
  自动加样（日志有提示，数字仍为 100 样本中位）。

### 2.1 decode（tailrace-decode/benches/decode.rs）

行形态：5 列（utf8×3 + int64 + float64/bool），JSON/OGG 行宽 ≈95-130B，CSV ≈75B。

| bench | 每批 8000 行耗时 | 吞吐（rows/s） | 首批基线 |
|---|---|---|---|
| `decode/json_root_5col` | 26.52 ms | 301.7 K | 2026-10-17 |
| `decode/ogg_envelope_5col` | 27.69 ms | 288.9 K | 2026-10-17 |
| `decode/csv_5col` | 12.26 ms | 652.4 K | 2026-10-17 |

观察：JSON 路径含 `raw.to_vec()` 全量拷贝 + 每行重建列名 `Vec<String>`
（T3 零拷贝改造的直接目标）；CSV 路径无 JSON 解析，快 ~2.2 倍。

### 2.2 transform（tailrace-transform/benches/transform.rs）

走 `transform_cached`（runtime 热路径，缓存 SessionContext；v0.1.0 每微批仍重新
parse+plan——T7 物化 LogicalPlan 的改造对象）。

| bench | 每批 8000 行耗时 | 吞吐（rows/s） | 首批基线 |
|---|---|---|---|
| `transform/identity_select_star` | 418.4 µs | 19.12 M | 2026-10-17 |
| `transform/filter_project` | 1.090 ms | 7.34 M | 2026-10-17 |
| `transform/groupby_aggregate` | 1.303 ms | 6.14 M | 2026-10-17 |

观察：DataFusion 执行远快于 decode（同一行流过全链，decode 是瓶颈级，
~0.3M rows/s vs transform ≥6M rows/s）——R2 优化重心应放在 decode/拷贝路径。

### 2.3 sink 序列化（tailrace-conn-file/benches/sink.rs）

行形态：3 列（int64 + utf8(~40B) + float64），10000 行/flush（默认 `flush_rows`）。

| bench | 每 10000 行耗时 | 吞吐（rows/s） | 磁盘敏感 | 首批基线 |
|---|---|---|---|---|
| `sink/jsonl_encode_10k_rows` | 1.186 ms | 8.43 M | 否（纯内存编码） | 2026-10-17 |
| `sink/jsonl_flush_10k_rows` | 48.61 ms | 205.7 K | **是**（含 tmp 写+fsync×2+整读回+append） | 2026-10-17 |

观察：整 flush 比纯编码慢 ~41 倍——fsync×2 与「写 tmp→fsync→整读回内存→append」
路径主导（T3 顺手项的怀疑已被数字证实；该行是磁盘敏感数字，只做同机对比）。

## 三、宏基准（file→file 全链）

### 方法学

- 脚本：`scripts/bench/macro.sh`（console + engine node 真进程，loopback）。
- 口径（**追平**）：`--rows N` 行 JSON 4 列种子（order_no utf8 / qty int64 /
  price float64 / note utf8，行宽 ≈95B）一次性写满源文件 → deploy →
  轮询 stats 至 `flushed ≥ N`；elapsed = deploy 返回 200 → 达标时刻；
  **吞吐 = N / elapsed**。含管道启动与 sink 落盘（fsync），不含 console/node
  进程启动与种子写入。结束时断言 sink 文件行数 ≥ N。
- 二进制档位：**默认 debug（未优化构建）**——v0.1.0 本机快速基线档；
  debug 数字只作同机纵向对比，**不代表 release 性能**（口径在报告与脚本内注明）。
  `--mode release` 用 release 二进制（缺则现场构建）。
- 复现：`bash scripts/bench/macro.sh --rows 100000 --report /tmp/out.md`。

### 首批基线

| 日期 | git | 档位 | rows | elapsed | 吞吐（rows/s） | 输入 | 输入 MB/s | sink 校验 |
|---|---|---|---|---|---|---|---|---|
| 2026-10-17 | `8e3b1fd` | **debug** | 100000 | 4.600 s | **21739** | 9.8 MB | 2.13 | 100000 行 ✓ |

吞吐行（机读）：`throughput=21739 rows/s（rows=100000, elapsed=4.600s, mode=debug, git@8e3b1fd）`

观察：debug 全链 21.7K rows/s ≪ 微基准各段能力（decode 单段 302K，release），
差距来自 debug 构建未优化 + fsync 落盘 + console stats 轮询粒度；单语句单线程
串行（T5 分区并行的对象）。release 档基线待 W4 起优化任务对比时补测。

## 四、优化前后对比账本（T3/T5/T6/T7 逐项追加，不删旧行）

| 任务 | bench/口径 | 基线 | 优化后 | 变化 | 日期 |
|---|---|---|---|---|---|
| （待 T3 填） | decode/json_root_5col | 301.7 K rows/s | — | — | — |
| （待 T3 填） | transform 各项 | 见 §2.2 | — | — | — |
| （待 T3 填） | sink/jsonl_flush_10k_rows | 205.7 K rows/s（磁盘敏感） | — | — | — |
| （待 T5 填） | macro 多分区对比 | — | — | — | — |
| T6 | 批大小分布指标 | — | 默认 6 批全 16000；字节 128KiB 档 13 批 p50=8000 | 见 §4.6 | 2026-10-17 |
| T7 | transform/identity_select_star | 19.348 M rows/s | 30.647 M rows/s | **+58%** | 2026-10-17 |
| T7 | transform/filter_project | 7.454 M rows/s | 8.958 M rows/s | **+20%** | 2026-10-17 |
| T7 | transform/groupby_aggregate | 6.219 M rows/s | 8.072 M rows/s | **+30%** | 2026-10-17 |
| T7 | macro 吞吐（file→file debug） | 25138 rows/s | 25094 rows/s | 持平（噪声内，见 §4.4） | 2026-10-17 |
| T7 | macro node 峰值 RSS | 46888 KiB | 49124 KiB | +2.2 MiB（物化计划常驻，见 §4.4） | 2026-10-17 |
| T5 | macro 多分区 kafka(P=6)→file 吞吐（debug） | 21396 rows/s（串行 decode） | 24658 rows/s | **+15.2%**（交错 A/B，见 §4.5） | 2026-10-17 |
| T5 | macro file→file 吞吐（debug，顺带受益） | 24697-24876 rows/s | 28802-29394 rows/s | **+16~19%** | 2026-10-17 |

### §4.1 T3 热路径零拷贝（2026-10-17，同机同日复测）

改造内容：Decoder trait 收 `Vec<u8>` 所有权（JSON/OGG simd-json 原地解析，kafka→decode
全链每消息拷贝 2→1，剩余 1 次为 rdkafka C 堆→Rust 堆的必要边界拷贝）；ColBatch 借用
split（`parts()`）消除每行 `Vec<String>` 列名克隆（N 列 = N 次分配/行）；ParquetDecoder
`Bytes::from(Vec)` 零拷贝包装（原整文件再拷）；FileSource 读栈字节化（完整行 move 进
chunk，原每行 extend+lossy+to_vec 双拷贝）；FileSink jsonl flush 直接 append（原
tmp 写+fsync+**整读回**+append+fsync）；DorisSink 载体 `BytesMut→Bytes`（重试 reqwest
body 为 refcount clone，原每次尝试 `body.to_vec()` 全量拷贝）。

「前」口径：**当前 HEAD `d979e06` 复测**（T4 已将 CSV 载体换 RFC4180 csv crate，
§2.1 的 652.4K 为 T4 前旧数，不可再作 CSV 前值）；「后」口径：T3 工作树改造后。

| bench | 前（d979e06 复测） | 后（T3） | 变化 | 备注 |
|---|---|---|---|---|
| decode/json_root_5col | 328.9 K rows/s | 534.9 K（另一次 438.5 K） | **+33%（保守）~ +63%** | 两次 after 样本并列 |
| decode/ogg_envelope_5col | 319.4 K rows/s | 463.2 K rows/s | **+45%** | |
| decode/csv_5col | 45.0 K rows/s | 50.0 K rows/s | +11% | 列名 clone 消除；csv crate 解析主导 |
| sink/jsonl_flush_10k_rows（磁盘敏感） | 234.4 K rows/s | 458.2 K rows/s | **+95%** | 免 tmp 写+整读回+一次 fsync |
| sink/jsonl_encode_10k_rows | 8.02 M rows/s | 6.18 M rows/s | −23% | **未改动的对照项**——本表噪声界参考 |
| transform/identity_select_star | 22.0 M rows/s | 23.8 M rows/s | +4%（噪声内） | 零改动对照，无回归 |
| transform/filter_project | 7.47 M rows/s | 7.61 M rows/s | +4%（噪声内） | 零改动对照 |
| transform/groupby_aggregate | 6.09 M rows/s | 6.21 M rows/s | +1%（criterion 判 no change） | 零改动对照 |

机器噪声说明：本机（Intel MBP 2019）当日同代码重复测量漂移可达 ±20%+（未改动的
encode bench 复测 −23% 即量级参照）；表中变化远超该界者（json/ogg/flush）为确定增益，
csv +11% 与噪声界同量级、只作方向参考。**宏基准**（file→file 全链，debug 档）：

| 日期 | git | 档位 | rows | elapsed | 吞吐（rows/s） | 对照基线 |
|---|---|---|---|---|---|---|
| 2026-10-17 | d979e06+T3 工作树 | debug | 100000 | 4.057 s | **24649** | 基线 21739（§三）→ **+13.4%** |
| 2026-10-17 | d979e06+T3 工作树（二次采样） | debug | 100000 | 3.961 s | **25246** | → **+16.1%** |

（两次采样区间 24.6–25.2K；全链增益受 debug 档未优化构建稀释，fsync 落盘与
console 轮询粒度主导尾延迟。）

语义注记（如实登记）：JSON/OGG Bad 行还回的 buffer 可能已被 simd-json 的转义展开改写
（实测 `{"ok":"\u0041","bad":` 类毒行；普通毒行字节不变）——DLQ 隔离样本在毒行含转义
时为「解析视图」而非逐字节原样。这是消除每消息全量拷贝的固有取舍，已在
`Decoded::Bad` 的 doc 注释中写明；DLQ 无字节重放机制，样本保持排查可读。

### §4.4 T7 内存治理（2026-10-17，同机同日复测）

改造内容：

1. **batch_size 固定为 poll_rows**：session 组装改为
   `TransformOptions { max_rows_per_batch = RunOptions.poll_rows }` 一次定型——原实现用
   **第一个微批的行数**定格 SessionConfig batch_size（首批 1 行 → 该语句线程恒为 1）。
2. **SQL 物化为 LogicalPlan 每语句一次**：`tailrace-ir` 新增 pub `build_exec_plan`
   （组装期 parse+plan 一次，坏 SQL 在部署组装即失败——fail fast），执行层新入口
   `tailrace-transform::transform_plan` 每微批仅替换 TableScan("src") 的 source `Arc`
   （qualifier/投影由物化计划锚定，绕开 CustomScan qualifier 边界）；优化器仍每批执行
   （物理计划需要活数据），SQL parse/analyze 不再进热路径。
3. **DataFusion memory pool 进 session 组装**：`RuntimeEnvBuilder::with_memory_limit`
   （GreedyMemoryPool，默认 256 MiB/语句，`TransformOptions.memory_pool_bytes` 可配）。

实测机制修正（探针实证，DataFusion 48.0.1 内存表路径）：任务卡描述的「后续 8000 行微批
被切成 8000 个单行 RecordBatch」对 scan/filter/project 形语句**不成立**——MemoryStream
按存储批次原样产出且 target_partitions=1 无 repartition；**聚合输出**才是 batch_size 的
切批点（hash agg 按批吐出）：batch_size=1 时 8000 组聚合 → **8000 个单行 RecordBatch**
流过计划与 sink 写路径，batch_size=8000 → 1 批。

验收单测（tailrace-transform 4 passed / tailrace-runtime 10 passed / tailrace-ir 2
passed，契约 7/7）：

- `explain_metrics_batch_count_drops`：首批 1 行 + 后续 8000 行 → 计划执行批数
  **8000→1**。**如实注记**：DF48 的 `output_batches` 指标仅由 join/coalesce/unnest 算子
  记录（datafusion-physical-plan 源码确认），聚合/投影计划不产出该指标，故执行批数取
  计划输出 collect 计数，EXPLAIN ANALYZE 表断言其真实展示项（AggregateExec 执行、
  output_rows=8000）——偏差在测试注释与本节注明，不按卡面字样造假。
  〔**DF55 已解除**（T33，2026-10-24）：`output_batches` 入 BaselineMetrics 标准指标
  （datafusion-physical-expr-common `src/metrics/baseline.rs`，RecordOutput 自动记录），
  AggregateExec/ProjectionExec 均自带 `output_batches=1`；测试注记已按此改写并加
  `output_batches=` 存在性断言钉住。归因见 docs/dev/t33-golden-attribution.md §三 B-2。〕
- `batch_size_fixed_from_options_not_first_batch`：同场景，固定 8000 → 输出 1 批 vs
  旧定格（模拟 batch_size=1）→ 8000 批。
- `t7_exec_plan_wiring`（runtime 接线）：声明 schema 的 `build_exec_plan` 计划 +
  `transform_plan(poll_rows)`：8001 行微批 1 个执行批、4001 行过滤语义、投影列序
  （order_no, seq10）与表达式值（seq*10）不变。
- `memory_pool_wired_with_configured_cap`：pool 装配断言
  （`memory_pool.memory_limit() == Finite(cap)`）。**如实登记**：DF 官方注明 pool
  "not yet respected in all cases"；实测 64 KiB 池下 8000 组 hash agg 照常完成（不
  强制拒绝），pool 是装配正确的护栏而非硬限制。〔**DF55 维持**（T33，2026-10-24）：
  datafusion-execution 55.2.0 `src/runtime_env.rs:429` 注记 "does not yet respect this
  limit in all cases" 逐字仍在，`with_memory_limit` 行为面同构——本条限制在 55 上原样
  成立。复核见 docs/dev/t33-golden-attribution.md §八。〕
- 过程修复：契约套件（v1_file_pipeline_lifecycle / v1_bad_rows_dlq）抓到
  `build_exec_plan` 在节点 serve 部署 handler（异步上下文）内嵌套建 runtime → panic；
  已改为专用线程建 runtime（一次性部署成本）。

微基准（前=HEAD `98772be` stash 复测，后=T7 工作树；口径同 §2.2）：

| bench | 前（98772be） | 后（T7） | 变化（criterion 判定） |
|---|---|---|---|
| transform/identity_select_star | 19.348 M rows/s | 30.647 M rows/s | **+58~59%**（time −37%，p<0.05） |
| transform/filter_project | 7.454 M rows/s | 8.958 M rows/s | **+20%**（p<0.05） |
| transform/groupby_aggregate | 6.219 M rows/s | 8.072 M rows/s | **+30%**（p<0.05） |

增益来自每微批 SQL parse+plan 的消除（物化计划命中后每批仅 optimize+physical+execute）。

宏基准（file→file debug 档 + node 峰值 RSS；RSS=外部采样器 200ms 轮询
`tailrace-node.*tailrace-macro-bench` 进程 `ps -o rss=` 峰值，脚本本身未改）：

| 口径 | 前（98772be） | 后（T7 工作树） | 变化 |
|---|---|---|---|
| 吞吐（100000 行追平） | 25138 rows/s | 25094 rows/s | 持平（−0.2%，机器噪声界内） |
| node 峰值 RSS | 46888 KiB（45.8 MiB） | 49124 KiB（48.0 MiB） | +2.2 MiB（物化计划+会话缓存常驻） |

如实解读：宏基准口径的 SQL 是 scan/project 形（无聚合）——batch_size 修复在该形态无
增益空间（机制修正节），全链 debug 档由 decode 与 fsync 落盘主导（§2.2 观察），transform
段微基准增益被链路稀释为持平；聚合形语句的批数收益由单测与微基准
groupby（+30%）证实。RSS 小幅上浮是每语句物化 LogicalPlan + plan 缓存的常驻代价。

### §4.5 T5 分区并行·方案 a（2026-10-17，同机同日复测）

改造内容（roadmap §四 T5）：同一 poll 的 chunks 相互独立——`decode_poll` 把 chunks
按连续切片分组，scoped 线程并行 decode 到**各自 ColBatch**（容量按组行数预分配），
`concat_batches` 按 chunk 序合并为单批后走原 transform→write→flush 路径。
低于扇出门槛（`PARALLEL_DECODE_MIN_CHUNKS=512`×2）的 poll 走原串行路径（复用
statement 常驻 batch，涓流流量零开销不变）。游标语义完全不动：decode 发生在
`source.poll()` 与 flush 点整体提交之间；NO_PROGRESS_LIMIT=3 判据不变。牵连一处
trait 声明：`Decoder: Send + Sync`（`&dyn Decoder` 跨 scoped 线程共享所需；全部
4 个实现是无状态 unit struct，零实现改动）。

**方法学修正（本节数字以此为准）**：多分区对比采用**交错 A/B**——串行版与并行版
两个 node 二进制同窗口交替各跑 4 轮（`scripts/bench/macro-partitions.sh
--rows 100000 --partitions 6`，kafka 6 分区→file，debug 档；串行版=同一工作树把
扇出门槛临时抬到不可达的对照构建）。不交错的先后测量在本机漂移可达 16.4K→25.0K
（同一二进制！），先后口径不可比——这正是 §4.1 噪声界警示的放大版，如实登记。

| 口径 | 串行（现状语义） | 并行（T5） | 变化 |
|---|---|---|---|
| kafka(P=6)→file 4 轮样本 | 21133 / 21358 / 21445 / 21455 | 24021 / 24618 / 24697 / 24802 | 中位 **21396 → 24658（+15.2%）** |
| file→file 2 轮样本（顺带受益） | 24697 / 24876 | 28802 / 29394 | **+16~19%** |
| worker 上限 12（HT）vs 6（物理核） | — | 24366/24814 vs 24131/25044 | 无显著差（保留 available_parallelism() 简单口径） |

复现：`docker compose up -d kafka && bash scripts/bench/macro-partitions.sh --rows
100000 --partitions 6 --report out.md`（脚本断言播种量==N 与 sink 行数==N，不丢行）。
注：kafka 群组首次加入（rebalance）耗时计入 elapsed（两侧同口径）。

语义回归（动工前先写、改造后全绿）：
- `partial_partition_progress_moves_the_cursor_fingerprint`（registry 单测）：
  部分分区推进=进步（指纹必变，NO_PROGRESS 不误杀）；游标冻结=指纹逐位稳定。
- `t5_partial_partition_commit_and_restart_no_loss_no_dup`（runtime 集成测试）：
  部分分区 poll → flush 点整体提交（p2 无数据不进 map；重启后 p1 冻结在 2000）；
  重启不丢不重——真 FileSink 提交面 6800 行恰好 6800 个唯一 order_no；台账
  totals 跨 deploy 单调续涨（5000→6800，无重放）。
- `t5_no_progress_limit_three_failures_terminal`：sink flush 永远失败 → 3 次无
  进步重启 → FAILED 终态（STATEMENT_FAILED 事件含「3 consecutive failures without
  cursor progress」判决）+ 台账零提交。
- `t5_progress_resets_no_progress_counter`：首次 flush 失败→重启重放→成功 → 游标
  过界 → RUNNING 不误杀，sink 恰好一次提交 100 行。
- `t5_parallel_decode_equivalence`（runtime 单测）：4096 chunks（Append/Skip/Bad
  混合，4+ worker）并行 decode 与串行逐 chunk decode 的 RecordBatch 经 arrow
  JSON-lines 编码**字节级一致**（行序锁定）；stats/bad 载荷逐项相等；Fail 策略
  bad_now=全量坏行、首样本在 chunk 序。
- 过程修复：契约套件 `bad_row_policy_branches` 抓到并行化初版把 Fail 策略的
  bad 计数错接为 DLQ 载荷数（bad_now=0 → fail 不触发）——已修（PollDecode.bad_now
  独立字段），该测试恢复绿。

### §4.6 T6 攒批三阈值（2026-10-17）

改造内容（roadmap §四 T6）：行数/字节/时间三阈值**任一满足即 flush**，参数
语义对齐 Doris Flink connector 的 `sink.buffer-flush.max-rows / max-bytes /
interval`（`RunOptions::flush_rows / flush_bytes / flush_interval`，提取为
单一谓词 `RunOptions::flush_due` 供两个语句主循环共用；plan 面同名三维
`flushRows / flushBytes / flushIntervalMs` 经 registry clamp 接线）。
`KafkaSink::buffered_bytes()` 从默认 0（字节边界永不触发）改为**真实在途
计量**：`SinkProducerContext` 投递回调内记账——enqueue +payload len /
delivery report −payload len（`in_flight_bytes` 原子计数；acks=all 语义下
delivery report 即「leader+全部副本已持久化」）。至此 file（pending 批 arrow
内存）/ doris（carrier 字节）/ kafka（在途 payload 字节）三类 sink 的
`buffered_bytes()` 均为真实测量。

验收数字：

- 三阈值隔离单测（`tailrace-runtime` lib `t6_flush_boundary_threshold_isolation`
  + 集成 `t6_{row,byte,time}_boundary_triggers_flush`，真 file→file + 真
  tuning 参数面）：行数场景 500 行/poll 100/flush_rows 200 → RUNNING 期间
  恰 2 次 flush 各 200 行；字节场景 300 行/flush_bytes 1024 → 3 次 flush
  各 100 行；时间场景 100 行/interval 1s → idle 路径 1 次 flush 100 行；
  另两阈值在各自场景中均放到不可达（1M 行/1GiB/600s）保证归属唯一。
- kafka `buffered_bytes` 单测：离线（broker 不可达，send 仅入本地队列）
  计数 == 精确 JSON-lines 字节总量、失败 flush 后保留；testcontainers
  真链路（`kafka_source_live` 8.80s）flush 成功后 == 0（投递确认即清零）。
- 契约测试 7/7 绿；`clippy --all-targets -D warnings` 0 告警。

**批大小分布指标**（数据源：`t6_flush_size_distribution_*` 测试，从 hub
FLUSH_OK 事件提取每次 flush 行数；file→file 100k 行、行形态
`{"order_no":"ORD-<24位>","qty":<i>}` ≈45B/行，debug 档、单语句、无 console
中转，单轮 wall ≈14s——非吞吐口径，吞吐见 §三）：

| 配置 | flush 次数 | min | p50 | p90 | p99 | max | 终局 flush |
|---|---|---|---|---|---|---|---|
| 默认阈值（10k rows / 8MiB / 10s / poll 8000） | 6 | 16000 | 16000 | 16000 | 16000 | 16000 | 4000 |
| 字节阈值 128KiB（其余不可达 / poll 8000） | 13 | 4000 | 8000 | 8000 | 8000 | 8000 | 0（尾部 4000 行自身过界） |

**粒度事实（如实登记）**：flush 检查点在**微批边界**（每 poll 一次，
decode→transform→write 之后），行数边界过界判断只能以整 poll 为步进——
默认阈值下批恒为 2×poll_rows=16000 而非 10000±ε；Doris Flink connector
逐 record 检查，其 max-rows=10k 的批接近 10000。若需消粒度差，需把阈值
检查下沉进 write 路径（逐批/逐 chunk），属 R3+ 取舍，本轮不做。字节阈值
塑形同理以 poll 为步进（128KiB 配置下每 poll ≈320KiB arrow 内存恒过界，
批=poll 大小 8000）。

## 五、CI 口径

- 微基准在 CI 只做**编译验证**（`cargo bench -p <crate> --no-run`），不上传数据、
  不跑测量（runner 抖动大，无参考价值；数字一律本机复测）。
- 2026-10-17（T11）起 CI 增 `chaos` job（testcontainers 起临时 Kafka+Doris，
  chaos kill-9 + conn-kafka 活性；runner 预置 `vm.max_map_count=2000000`），
  `smoke` job 升级为 `SMOKE_FULL_SOAK_SECS=180 ./scripts/smoke.sh --full`
  （compose Kafka+Doris 端到端 + 毒数据浸泡；≥10min 口径由本地默认 600s 承担）。

### §5.1 T11 docker 链路首跑数字（OrbStack，2026-10-17）

| 口径 | 数字 |
|---|---|
| `smoke.sh --full`（600s 毒浸泡，release 二进制） | **44/44 步全绿**，全程 11m20s |
| kafka→doris 端到端（S8） | 10 条 OGG → flushed=10 → `COUNT(*)`=10 |
| 毒数据浸泡（S9，JSON-lines 载体） | produced=consumed=flushed=Doris COUNT=**1700**（守恒）；字段内换行/引号/emoji/中文字节级往返 ✅ |
| 容器内存峰值（runner 预算依据） | kafka 371.7MiB / doris-fe 867.4MiB / doris-be 1.082GiB（23.54GiB 宿主） |
| chaos kill-9（testcontainers 模式） | 1 passed / 60.56s（kill@5s → 重启 → COUNT==5 幂等对账） |
| conn-kafka 活性（testcontainers 模式） | 1 passed / 8.60s（ephemeral kafka 起停 + sink 3 条播种 + source 消回） |
| 现场残留 | `down -v` 后容器/网络均 0 |

（过程与修复清单见 `docs/dev/T11-docker-compose-smoke-full-first-run-2026-10-17.md`；
注：毒浸泡吞吐受 15s/批节奏限制，非吞吐口径——吞吐见 §三宏基准。）

## 六、R3 对比账本（roadmap §九，逐项追加，不删旧行）

> R3 起数字采自**第二台开发机**（下表），与 §一 Intel 机器的 R2 数字**跨机器不可比**
> （§一注记同样适用）；同一小节内的 A/B 均为同机同日交错复测。

| 项 | 值 |
|---|---|
| 机型 | Apple M4（10 核），16 GB |
| 系统 | macOS 26.6.2（arm64，16 KiB 页） |
| 工具链 | rustc 1.98.1（stable，workspace rust-version 1.95） |
| 构建前缀 | `CARGO_PROFILE_DEV_DEBUG=0`（debug 档去 debuginfo；两档一致） |

| 任务 | bench/口径 | 默认档 | 对比档（T20=jemalloc 档；T16=裁剪档） | 变化 | 日期 |
|---|---|---|---|---|---|
| T20 | macro 吞吐（file→file debug，3 轮中位）¹ | 33670 rows/s | 33738 rows/s | 持平（+0.2%，远在噪声界内）¹ | 2026-10-20 |
| T20 | node 峰值 RSS（内核 ru_maxrss，3 轮中位） | 53616 KiB（52.4 MiB） | 57808 KiB（56.5 MiB） | **+4192 KiB（+7.8%）**，两档区间不重叠 | 2026-10-20 |
| T20 | node 二进制体积（debug 档）² | 264,674,112 B | 265,573,776 B | +879 KiB（jemalloc 5.3.1 静态链入） | 2026-10-20 |
| T20 | 碎片（jemalloc `stats.allocated` vs `stats.resident`） | 未测 | 未测 | 未测³ | 2026-10-20 |
| T16 | node 二进制体积（**release 档**，改造后；`--no-default-features --features connector-file,connector-doris`）⁴ | 61,571,936 B（`394cd774…`） | **55,788,080 B**（`c5758910…`） | **−5,783,856 B（−5.52 MiB，−9.4%）** | 2026-10-21 |
| T16 | 同上，**改造前**（旧接线：runtime 默认 feature 把 kafka 栈拉回）⁴ | 61,571,920 B（`e1d479e7…`） | 61,571,872 B（`3b9cda96…`） | −48 B（0.0001%，裁剪纯装饰） | 2026-10-21 |
| T16 | 依赖图包数（`cargo tree -e normal` 去重） | 435 | 413 | −22（rdkafka/rdkafka-sys/curl-sys/openssl/openssl-sys/tailrace-conn-kafka 等出图） | 2026-10-21 |
| T16 | file→doris e2e（裁剪档 node，`smoke.sh --slim`） | —（默认档 `smoke.sh` 25/25 回归） | **30/30**，11.9 s；行数守恒 12==12 | 见 §6.2 | 2026-10-21 |
| T15 | macro-partitions 吞吐（kafka(P=6)→file **debug 档**，4 轮）⁵ | **M4 首档：中位 27861 rows/s**（23918 / 27678 / 28043 / 28474） | —（本卡热路径零改动，无 A/B 档） | **vs §4.5 的 24658 属跨机器（Intel i7→M4），不可比**；本组=该 bench 在 M4 的首个档位 | 2026-10-21 |
| T15 | 动态分区发现延迟（testcontainers 真 kafka，topic 2→4 分区不重开） | subscribe 模式 **26.8 s** | assign 模式 **30.7 s** | 均在口径界内（≤~35 s = 30 s 差异对比 + fetch_metadata ≤5 s，见 §6.3） | 2026-10-21 |
| T22 | web 编辑器异步 chunk **gzip 体积**（`dist/assets/SqlMonaco-*.js`，`vite build` 后 `gzip -c \| wc -c`）⁷ | monaco 档 **883.22 kB**（T9 单口径锚点） | CodeMirror6 档 **138,962 B**（raw 431,013 B） | **−84.3%**；≤300 kB 门槛过；编辑器 CSS 278 B gzip、编辑器相关资产合计 139,240 B；monaco worker/ttf/外部 CSS 整类归零 | 2026-10-21 |
| T32 | macro 吞吐（file→**doris** 单管道·默认批，debug）⁶ | **33852 rows/s** | 同日 file→file 锚点 33568 | +0.8%（噪声界内持平）——大批下 doris sink 零可测开销（见 §6.4） | 2026-10-23 |
| T32 | 同上·小批探针（flushRows 500+pollRows 500，单管道） | **11017 rows/s**（200 次同步 stream load） | —（vs 大批 −67%） | 小批区无 group commit 的同步往返封顶 ≈45ms/次 | 2026-10-23 |
| T32 | 同上·P=4 并发小批（T18 ④T3 判据数据面）⁶ | **26781 rows/s**（50 flush/管） | 单管小批 11017 | **2.43×**（HTTP 往返重叠），仍 −21% vs 单管大批——收益缺口=group commit 目标区 | 2026-10-23 |
| T32 | 同上·P=4 并发·poll 地板批（名义 flush500，实际批 ≈6250）⁶ | **97752 rows/s**（4 flush/管） | 单管大批 33852 | **2.89×**——4 管道并行化全链；console 并发 4 管道无限制（未触发降 P=2 回退门） | 2026-10-23 |
| T32 | file→**starrocks**（node 直跑档，口径不同源）⁶ | **8750 rows/s** | — | allin1 单容器量级参考，不与 console 档直接比（见 §6.4 口径注记） | 2026-10-23 |
| T33 | macro 吞吐（file→file debug，DataFusion 48.0.1→**55.2.0**）⁸ | DF48 同机锚点 **33568 rows/s**（§6.4，2026-10-23） | DF55 **41528 rows/s**（2.408 s） | **+23.7%**（同机**不同日**，非同日 A/B；单轮 debug 档不登记为确定增益）；回退门「宏基准回吐 >10%」**未触发** | 2026-10-24 |
| T33 | transform 微基准三项（criterion release，8000 行/批，DF 55.2.0）⁸ | —（**本机无 DF48 微基准档**：target/criterion 本次首跑） | **M4 DF55 首档**：identity **237.38 M** / filter **65.89 M** / groupby **53.47 M** rows/s | **vs §4.4 的 30.647 M/8.958 M/8.072 M 属跨机器（Intel i7→M4），不可比**；本组=该 bench 在 M4 的首个档位，三项「前后差」无法给出（见 §6.5） | 2026-10-24 |

> ¹ 吞吐口径脚注（R3-W1 评审 low#5 补注）：**debug 档**（`CARGO_PROFILE_DEV_DEBUG=0`，非 release）、单轮全链仅 ~3s、macro.sh stats 轮询步进 0.5s → **量化误差 ±17%**（§6.1 结论段自认）——本口径对 <~17% 的 allocator 效应**无检出力**，「持平」仅指差值未超出该噪声界；allocator 吞吐结论以 release 档长跑补测后为准。
> ² 体积口径脚注（low#5）：两个体积数字来自**基准二进制**（sha256 前缀 `1f7270c7…`/`55066ae4…`，§6.1 方法段），构建树 = **caa842e + T20 提交前工作树**（= 9a9f8a2 落章前的树；其后 488efcc/13d1c2b（engine 改动）、19bb80b（CI）均未包含）——**非任何已提交树的可复现产物**。2026-10-21 复查：本机现存 `target/debug/tailrace-node`（264,705,488 B，`nm` 计 `_rjem_` 符号 0 个）属其后另一树的重建产物，与表中默认档数字不可互推。
> ³ 碎片行（low#5）：T20 卡方案列有「碎片」对比项，实测未做（无 jemalloc-ctl stats 面，§6.1 结论段「未测」）——此行为显式占位，防「表里没有＝测过没事」的误读；补测需 `tikv-jemalloc-ctl` stats feature + release 长跑。
> ⁴ T16 体积口径：**release 档**（`[profile.release]` lto="thin" / codegen-units=1 / strip=true），宿主 target aarch64-apple-darwin（命令不带 `--target`，产物 `target/release/`），`--locked`（Cargo.lock 零变动）；字节数 `stat -f %z`、sha256 `shasum -a 256`，sha256 为前 8 位。「改造前」两行 = `4881c39` 干净树；「改造后」两行 = `4881c39` + T16 工作树（根 Cargo.toml / engine/bin/tailrace-node/Cargo.toml / scripts/smoke.sh / .github/workflows/ci.yml）。方法与结论见 §6.2。
> ⁵ T15 吞吐口径：`TAILRACE_KAFKA_PORT=19092 bash scripts/bench/macro-partitions.sh --rows 100000 --partitions 6`（compose kafka 移位 19092——本机 9092 被常驻 kafka-local 占用；EXTERNAL 通告随端口走，口径不变）。4 有效轮 + 1 轮播种失败重试（console-producer 端 99850≠100000，基础设施毛刺非引擎行为，弃用重跑）；每轮断言播种量==N 与 sink 行数==N（不丢行）。脚注 ¹ 的量化噪声（±17%）同样适用；同组内极差 ±9% 在噪声界内。
> ⁶ T32 吞吐口径：`bash scripts/bench/macro.sh --sink doris [--pipes P] [--flush-rows R] [--poll-rows R] --rows 100000`——console 档口径同脚注 ¹ 的 macro.sh（t0=deploy 200，P=4 档=首条 deploy 200；t1=stats 0.5s 轮询至全部管道 flushed≥目标），单轮单档（非多轮中位）；starrocks 行为 node 直跑口径（t0=run-start 日志、t1=FE COUNT ~1s 轮询）与 console 档不同源。栈=docker-compose doris-fe/be 2.1.7 单 BE（BUCKETS 1）；行数守恒全档过（FE COUNT==100000）。环境配方（swapoff/max_map_count/停 mdop doris 腾内存）与逐档注记见 §6.4。
> ⁷ T22 体积口径（T39 ② 补登记——T22 时文件域=web/，BENCHMARKS 未随行，数字取自 T22 落地提交 f90e6a2 说明）：编辑器异步 chunk = `web/dist/assets/SqlMonaco-*.js`（CodeMirror6 档；monaco 档为 T9 记录的编辑器 chunk 同名口径），体积为 `gzip -c | wc -c` 字节、构建产物确定性输出——**与表内吞吐行不同，不涉机器性能，跨机可比**；「编辑器相关资产合计 1,078→140.70 kB」的 PoC 复合口径已弃用（T23 评审 low#6，统一锚 T9 单口径 883.22 kB）。2026-10-23 T39 ⑤ 在本树复测：列补全接线（columnsJson 上 DTO+编辑器 children 槽填充）后 raw 431,182 B / gzip 139,007 B（+45 B，门槛仍过）。
> ⁸ T33 口径（2026-10-24，R5-W1 波门禁实跑；原始输出 /tmp/r5w1-t33-bench.log、/tmp/t33-macro.md）：代码树=bfbaeb3 的 DF 55.2.0 迁移树（bench 于 11:53 跑在提交前的同树上；macro.sh 报告标签 `git@c2c30e8` 系脚本 `rev-parse HEAD` 取当时 HEAD，bfbaeb3 12:00 才提交——**实跑二进制为 DF55**：`target/debug/tailrace-node` 11:43 构建，`strings` 含 `datafusion-55.2.0` 路径 7 处 / `datafusion-48.0.1` 0 处）。宏基准：`bash scripts/bench/macro.sh --rows 100000 --report /tmp/t33-macro.md`，debug 档、口径同脚注¹（±17% 量化噪声），行数守恒 100000==100000；DF48 对照取 §6.4 同机锚点 33568（2026-10-23），**非同日**——同 bench 的 DF48 跨日漂移 T20 33670（10-20，3 轮中位）↔ T32 33568（10-23）仅 0.3%，+23.7% 远在其外，回退门判定不受「不同日」影响；但单轮单档不登记为 DF55 的确定增益（release 档/多轮中位补测后方可谈增益量）。微基准：`cargo bench -p tailrace-transform`（criterion，release 继承，8000 行/批同 §2.2），区间为 criterion 吞吐 95% 置信区间；**本机从未跑过 DF48 微基准**（target/criterion 本次首跑：base==new 同 mtime、无 change/），§4.4 基线 30.647 M/8.958 M/8.072 M 采自 §一 Intel 机（2026-10-17），按 §六头部口径跨机器不可比——故登记为「M4 首档」而非前后对比；同机 DF48 对照若需补测 = `git worktree add <dir> c2c30e8` 后在该树 `cargo bench -p tailrace-transform`（留后续，非本卡回退门输入——回退门只定义在宏基准上）。

### §6.1 T20 jemalloc + 峰值 RSS 指标（2026-10-20，同机同日交错复测）

改造内容（roadmap §九 T20）：

1. **allocator feature 门控**：`tailrace-node` 新增 feature `jemalloc`（默认 OFF）→
   `tikv-jemallocator 0.7.0`（bundled jemalloc 5.3.1，`_rjem_` 前缀符号，C 侧
   rdkafka/openssl 仍走系统 malloc），`main.rs` 中 `#[cfg(feature = "jemalloc")]
   #[global_allocator]`。**默认档零变化（字节级实证）**：仅改 Cargo.toml/Cargo.lock
   （feature 声明、依赖 OFF）时默认档二进制 sha256 与改造前**完全一致**
   （`c32c65a1…`，debug 档本机可复现构建）；main.rs 加入 cfg 块后仅 182 字节差异
   （Mach-O LC_UUID + 后续 `line!()` 行号常量平移），`nm` 符号表逐项一致、
   `cargo tree` 默认依赖树一致、jemalloc 符号 0 个。feature 档 `nm` 含 516 个
   `_rjem_*` 符号（`__rjem_malloc/calloc/realloc` 在列）。Cargo.lock 仅新增
   `tikv-jemallocator` / `tikv-jemalloc-sys` 两包，无其它版本移动。
2. **峰值 RSS 指标就位（心跳 stats 面）**：node 现有暴露面为心跳 payload 的
   `resources` 键（`tailrace-api/src/heartbeat.rs`，Linux `/proc` 直读，已有瞬时
   `rssBytes`/`memTotalBytes`/`cpuPercent`/`rxBps`/`txBps`）；本卡在 `ResourceSample`
   增 `peak_rss_bytes` = `/proc/self/status` **VmHWM**（内核高水位，进程生命期真峰值，
   不受心跳间隔采样漏峰影响），payload 新增 `peakRssBytes`（首拍即带，非 delta 键）。
   纯加性字段：console 侧 `parse_resources` 按键取值（`api/nodes.rs:235`），无
   `deny_unknown_fields`；console/web 暂不消费该键（node_metrics 表无 peak 列，
   属 console 域后续工作）。**如实登记**：非 Linux（本开发机 macOS）`read_proc_sample`
   返回 None、`resources` 键整体省略——本节 RSS 数字因此全部来自**外部**测量，
   不是该字段的产出。
3. **宏基准脚本 hook**：`scripts/bench/macro.sh` 增环境变量 `MACRO_NODE_WRAP`
   （默认空 = 启动命令行与清理路径逐字不变）；设为 `/usr/bin/time -l` 时 node 由
   包装器拉起，rusage 落 node.log。实测 BSD `time` **不转发 SIGTERM**（杀包装器会
   孤儿化 node 且丢 rusage），故包装模式下清理改为给其子进程发信号。

**方法学**：`bash scripts/bench/macro.sh --rows 100000`（debug 档、file→file、口径同
§三）；两档二进制各构建一次（`cargo build -p tailrace-node --locked` /
`--features jemalloc --locked`，sha256 分别 `1f7270c7…` / `55066ae4…`），每轮前把对应
产物换入 `target/debug/` 并校验 sha256 后再跑；**A/B 交错**（A1 B1 A2 B2 A3 B3），
前置 1 轮弃置预热。峰值 RSS 双口径同轮并行测得：(1) **内核 `ru_maxrss`**——
`MACRO_NODE_WRAP="/usr/bin/time -l"`（macOS 报字节，÷1024 列为 KiB）；(2) 外部采样器
200ms 轮询 node pid `ps -o rss=` 取最大（§4.4 T7 同口径）。

| 轮 | 档 | 吞吐（rows/s） | elapsed | 峰值 RSS·内核 HWM（KiB） | 峰值 RSS·200ms 采样（KiB） |
|---|---|---|---|---|---|
| A1 | default | 33670 | 2.970 s | 53232 | 53200 |
| B1 | jemalloc | 33956 | 2.945 s | 57808 | 55904 |
| A2 | default | 33512 | 2.984 s | 53616 | 53616 |
| B2 | jemalloc | 33704 | 2.967 s | 57632 | 55696 |
| A3 | default | 41305 | 2.421 s | 55376 | 55376 |
| B3 | jemalloc | 33738 | 2.964 s | 58656 | 56896 |
| **中位** | default | **33670** | 2.970 s | **53616** | 53616 |
| **中位** | jemalloc | **33738** | 2.964 s | **57808** | 55904 |

（A1/B1 即任务指定报告 `/tmp/r3w1-bench-default.md` / `/tmp/r3w1-bench-jemalloc.md`；
sink 行数校验 6 轮均 100000 ✓；引擎 `git@caa842e` + T20 工作树。）

**结论（如实）**：

- **吞吐**：两档中位 33670 vs 33738 rows/s，差 0.2%——**无可测差异**。A3 的 41305
  为离群样本：本机全链仅 ~3s 完成，macro.sh 的 stats 轮询步进 0.5s 本身就是 ±17%
  的量化误差，debug 档 decode/fsync 主导下 allocator 不是瓶颈。
- **峰值 RSS**：jemalloc 档**更高**——中位 +4192 KiB（+7.8%），且三轮区间
  53232–55376 vs 57632–58656 **不重叠**，是确定性代价而非噪声。成因符合 jemalloc
  特性：arena/tcache/元数据常驻 + 脏页按 decay 周期（默认 ~10s）延迟归还——对一个
  ~50 MiB、3s 跑完的小进程，开销显露而碎片收益无从体现。
- **口径发现**：默认档两种口径完全吻合（差 0–32 KiB）；jemalloc 档 200ms 采样器
  **系统性低报 1.7–1.9 MiB**（存在采样漏掉的短尖峰）——T7 的采样口径对系统 allocator
  够用，对 jemalloc 不够；这也是心跳字段选 VmHWM 而非「拍间采样最大值」的实证依据。
- **裁决建议**：feature 保持默认 OFF（本轮验收口径「默认档行为零变化」成立）。
  在本工作负载（短跑、单语句、file→file）jemalloc 无吞吐收益且多占 ~4 MiB 常驻；
  其预期收益场景（长时运行多语句的碎片抑制、多线程分配竞争）本轮**未测**——
  碎片指标（`stats.allocated` vs `stats.resident`，需 `tikv-jemalloc-ctl` stats
  feature）与 release 档长跑留待有需求时补测，不在本卡数字内下结论。

复现：`CARGO_PROFILE_DEV_DEBUG=0 cargo build -p tailrace-node --locked [--features jemalloc]`
→ `MACRO_NODE_WRAP="/usr/bin/time -l" MACRO_KEEP=1 bash scripts/bench/macro.sh --rows 100000
--report out.md` → `grep 'maximum resident' $MACRO_DIR/node.log`（macOS；Linux 包装器
改用 GNU time `-v`，或直接读 Linux 心跳 `resources.peakRssBytes`）。

### §6.2 T16 feature 裁剪验证（2026-10-21，同机同日）

目标（roadmap §九 T16 / §一 北极星「2-4 GB 盒子可跑」）：裁剪档 `--no-default-features
--features connector-file,connector-doris` 必须**真实可用**（file→doris 链路实跑）且
**体积数字可复现**。口径与四个二进制的字节数/sha256 见上表 T16 行与脚注 ⁴。

**发现（改造前）**：裁剪是装饰性的。`tailrace-runtime` 自带
`default = ["connector-kafka","connector-file","connector-doris"]`（`engine/crates/tailrace-runtime/Cargo.toml:12`），
node 关掉 `connector-kafka` 只是去掉 `build_registry()` 里的一次 `register()`
（`engine/bin/tailrace-node/src/main.rs:30`），而 runtime 的两条入边（node→runtime、
**tailrace-api→runtime**，后者在 node 的依赖图内）都带 runtime 默认 feature，经 feature
统一把 kafka 整栈拉回：旧接线裁剪图仍含 rdkafka / tailrace-conn-kafka / openssl（`cargo tree`
3 命中），二进制仅 **−48 B**。另一处不一致：`run` 子命令走 runtime 的 `default_registry()`
（`engine/crates/tailrace-runtime/src/lib.rs:1134`），`serve` 走 node 的 `build_registry()`——
旧接线下"裁剪档"的 run 有 kafka、serve 没有。

**改造（最小 wiring，不动 runtime / main.rs 源码；Cargo.lock 零变动）**：

1. 根 `Cargo.toml` workspace 表 `tailrace-runtime` 加 `default-features = false`——只在 node 侧
   写 `default-features = false` **不够**（cargo 1.98 静默接受，但 `tailrace-api→runtime` 边
   仍启用 runtime 的 default：`cargo tree -e features -i tailrace-runtime` 实证链
   `tailrace-api feature "default" → tailrace-runtime feature "default"`，rdkafka 回到图内）；
   workspace 表是**唯一不触碰 tailrace-api 自身 manifest** 的落点（api 不感知连接器
   feature、也不实例化连接器，零改动）。原稿此处写「`tests/contract` 与 api 均不实例化
   连接器，两者零改动」——对 api 成立，对 tests/contract **不实**（评审勘误 2026-10-21）：
   该套件的 `spawn_node` 走 `PipelineRegistry::new → default_registry()`
   （`tests/contract/src/lib.rs:67`），是真的要注册连接器的；workspace 关默认后单包构建
   `cargo test -p tests-contract --locked` 零连接器 feature，executor_loop 3/3 FAILED
   （`源类型未支持：file`）——`--workspace` 运行因与 node 的默认 feature 统一而掩盖了它。
   修复=该套件 manifest 显式 `features = ["connector-file"]`（file→file 自述口径），
   单包复跑全绿，Cargo.lock 仍零变动。
2. `engine/bin/tailrace-node/Cargo.toml`：`connector-{kafka,file,doris}` 各透传
   `tailrace-runtime/connector-*`（两张注册表自此同源，run/serve 一致）；`openssl` 改
   optional 并挂在 `connector-kafka` 下（vendored OpenSSL 只为 rdkafka 的 curl-static 存在，
   裁剪图里没有 curl-sys）。
3. `scripts/smoke.sh --slim`：裁剪档 node + 标准 console，file→doris 最小面；compose 只起
   doris-fe+doris-be；`SMOKE_DORIS_MYSQL` 可把变体指向一台常驻 Doris（见下）；退出 trap
   内 `down -v` 或 `DROP DATABASE … FORCE`。
4. `.github/workflows/ci.yml`：新增 `slim-features` job（`cargo check --locked -p tailrace-node
   --no-default-features --features connector-file,connector-doris`）——现有
   `no-default-features` job 只覆盖**空**组合，可售裁剪组合此前无 CI 契约。

**结果（如实）**：

- 裁剪档 **55,788,080 B vs 默认 61,571,936 B：−5,783,856 B（−5.52 MiB，−9.4%）**；依赖图
  435→413 包，rdkafka / rdkafka-sys / curl-sys / openssl / openssl-sys / tailrace-conn-kafka
  全部出图（`cargo tree -e normal` 0 命中）。二进制层面的佐证（`otool`/`nm`，默认 vs 裁剪）：
  动态导入 297→182 项，掉的全是 librdkafka/libcurl/OpenSSL/zlib 的 C 面（`_deflate*/_inflate*`、
  `_regcomp/_regexec`、`_SCDynamicStoreCopyProxies`、`_dlopen/_dlsym`、`_pthread_rwlock_*`…）；
  链接 dylib 少了 CoreServices / Security / SystemConfiguration 三个框架与 `libz.1.dylib`；
  `__TEXT,__text` −3,495,460 B、`__TEXT,__const` −903,376 B、`__DATA_CONST,__const` −634,128 B、
  `__TEXT,__cstring` −385,771 B。−9.4% 是诚实的数字：剩余 ~42.2 MB `__text` 的归属**本卡未测**
  （`strip = true` 产物无静态符号表，无法按 crate 归因）——连接器裁剪对"单二进制轻量"是
  必要而非充分条件。
- **默认档零行为变化**：改造前后默认依赖图一致（435 包、kafka 栈齐全）、动态导入表（`nm`，
  297 项）与链接 dylib 列表逐项一致（strip 产物无静态符号表，故只能比到这一层）；二进制
  +16 B / sha256 变化（`e1d479e7…`→`394cd774…`）——`otool -l` 对比仅
  `__unwind_info` +8 B 与 `__LINKEDIT` +16 B，其余全部段/节尺寸一致：openssl 从直接依赖改为
  经 `dep:openssl` 启用，rlib 链接序平移（与 §6.1 记录的 182 B 同类布局漂移）。行为由默认档
  `./scripts/smoke.sh` **25/25** 复跑实证。两档 sha256 经 cargo deps/ 指纹缓存二次恢复**完全
  一致**（可复现）。
- **e2e 实跑**（裁剪档 node `c5758910…` + 标准 console，2026-10-21 本机）：

  | 项 | 结果 |
  |---|---|
  | `TAILRACE_FE_MYSQL_PORT=9030 TAILRACE_BE_HTTP_PORT=8040 SMOKE_DORIS_MYSQL="docker exec -i doris-2.1 mysql -h127.0.0.1 -P9030 -uroot" ./scripts/smoke.sh --slim` | **30/30 全绿**，11.9 s（首跑 29/29，未含后加的 S7.5） |
  | file→doris 行数守恒（S7.5，停止后终刷） | consumed=12 == Doris `COUNT(*)`=12 |
  | 负向探针：kafka 源管道部署到裁剪档 node | console 返回 `ENGINE_REJECTED`：`源类型未支持：kafka（本二进制未编译该连接器 feature）`——裁剪是注册表级语义，不只是字节 |
  | 默认档回归 `./scripts/smoke.sh` | 25/25 |
  | 环境残留 | 常驻 Doris `SHOW DATABASES` 前后一致（scratch 库 `tailrace_smoke` 已 FORCE drop）、mdop doris 双容器全程 Up、swap 未动、tailrace compose 容器 0、工作目录已清 |

**配方降级登记（如实）**：契约 §3 的 2.1.7 配方 = privileged `swapoff -a` → 停 mdop doris
腾内存 → `TAILRACE_KAFKA_PORT=39092 docker compose up`。实测 OrbStack VM 8004 MB / 可用
789 MB，`/proc/swaps`：**zram0 已用 4,125,768 KB（≈3.9 GB，压缩驻留 904 MB）** + /vdc 123 MB；
按规则「zram 用量 >0 且余量不足则不 swapoff」：停 doris 腾 ~1.9 GB 后可用 ≈2.7 GB < 回灌
≈4 GB，2.1.7 fe+be 再需 ~1.9 GB——不可行（BE 2.1.7 `start_be.sh` 有活动 swap 即拒启）。故
**未执行** swapoff / `docker stop doris-*` / `compose up`，环境零触碰；e2e 改由常驻 Doris
**3.1.4**（无 swap 检查，FE 9030 / BE 8040 已映射宿主）承接。因此 `--slim` 的 **compose 模式
代码路径本机未实跑**（仅 `bash -n` + 与 `--full` 共用的 doris helper）；CI 也不跑 `--slim`
（新 job 为 cargo check 级）——compose 2.1.7 下的裁剪档 e2e 留待 swap 可清空的机器或 CI 扩展。
`vm.max_map_count` 实测已为 2000000。

复现：`CARGO_PROFILE_DEV_DEBUG=0 cargo build --release --locked -p tailrace-node`（默认档）；
`CARGO_PROFILE_DEV_DEBUG=0 cargo build --release --locked -p tailrace-node --no-default-features
--features connector-file,connector-doris && cp target/release/tailrace-node target/release/tailrace-node-slim`
（裁剪档；再跑一次默认命令即恢复 `target/release/tailrace-node`）→ `shasum -a 256
target/release/tailrace-node*` → `./scripts/smoke.sh --slim`（compose 2.1.7；或按上表
`SMOKE_DORIS_MYSQL` 指向常驻 Doris）。

### §6.3 T15 分区并行·方案 b（2026-10-21，同机同日）

**现状修正（重要）**：任务卡所写「无动态分区发现」过时——Flink 式动态分区发现
自 v0.1.0 基线（`d547630`，2026-10-17）起已落在 `KafkaSource::poll`
（`engine/crates/tailrace-conn-kafka/src/lib.rs`：30s 元数据差异对比
`fetch_metadata`，新分区按语句起始位点〔earliest=0 / latest=高水位〕入游标
map；assign 模式显式刷新 assignment〔BaseConsumer 不自动扩展手动分配〕并补
pause 态）。§4.5 的 24658 基线测量时该代码已在树内。**T15 交付=三层验证 +
口径登记，热路径零改动**（工作树 diff 纯测试 + 文档；Cargo.toml / Cargo.lock
零变动——发现测试的 topic 建/扩用容器内 `kafka-topics.sh` exec 驱动，不引入
异步 AdminClient 及其执行器依赖）。

**动工前先写的回归测试（生产代码未动，全绿）**：

- `t15_growth_under_poison_sink_keeps_no_progress_verdict`（runtime 集成，毒
  计数汇）：毒化（flush 永远失败）后**运行中**长出新分区 + 旧分区追加 →
  未提交的发现/重放消费不得计为「进步」——3 次无进步重启后照常 FAILED
  终态（判决含 3 consecutive failures without cursor progress），台账游标
  冻结在毒化前提交（p2 永不入账），sink 侧零新增提交。发现项形态的指纹
  语义由此黑盒钉住（未 flush 即无进步；发现不逃逸毒数据判决）。
- `t15_new_partitions_join_mid_run_without_restart`（runtime 集成，mock 多分区
  源）：单次 deploy 内**运行中**长出新分区 → 数据进 poll 面、游标 map 长项、
  hub 零 PIPELINE_RESTART；跨发现边界重启不丢不重（真 FileSink 7300 行恰好
  7300 唯一键）；totals 跨发现与重启单调（5000→6800→7300）。
- `kafka_source_discovers_new_partitions_without_reopen`（conn-kafka，真
  rdkafka 路径）：testcontainers 起 ephemeral Kafka（`apache/kafka:3.9.0` JVM
  镜像 GenericImage，docker-compose.yml 同款双监听配方 + 预分配宿主端口；
  建表/扩分区用容器内 `/opt/kafka/bin/kafka-topics.sh` exec——同步 std API，
  零新增依赖）：create --partitions 2→播种 p0/p1→消费→**--alter
  --partitions 4**→播种 p2/p3→同一 source 持续 poll 断言 p2/p3 到达且游标
  map 含 4 分区。subscribe 与 assign 两种 open 模式各一轮。T5 的 5 个语义
  回归 + 字节级并行等价性（`t5_parallel_decode_equivalence`）全部保持绿。

**发现延迟口径（登记）**：差异对比每 **30s** 一次（`fetch_metadata` 超时 5s）
→ 新分区首条数据最晚 **~35s** 进入 poll 面。实测（testcontainers，2→4 分区，
终版测试在树复测）：subscribe 模式 **26.8s** / assign 模式 **30.7s**，均在界内
（先行实现口径 AdminClient 版复测 25.0s/31.1s——两次独立实现同量级，口径
稳定）。已知语义窄窗
（如实登记，未修）：latest + subscribe 首跑时，发现项以**发现时刻高水位**
入游标 map，与 group 实际消费位点（join 时刻高水位）之间若在发现窗口内生产
了新纪录且尚未 poll，flush 提交该发现项后重启会把该窗口内的记录跳过——
  earliest（默认）全路径无此窗；修复需 subscribe 模式逐分区自行记账，属
  split 化同级的热路径重设计，随 T18 议程。**第二个已登记未修边界（评审
  2026-10-21，low）——分区发现是单向（只增不减）**：30s 差异对比只处理
  元数据里新出现的分区（`engine/crates/tailrace-conn-kafka/src/lib.rs:222`
  起），分区消失（topic 删除重建、subscribe 模式 rebalance 撤销）后
  `self.offsets` 中的陈旧项永不清理——后续任一新分区触发 assign 模式 TPL
  重建时按含死分区的整个 offsets map 重建，`cursor()` 也会把死分区项永久
  提交进台账。现实触发面窄（单机 v0.x、Kafka --alter 不能减分区；触发=
  topic 删建重建后陈旧 offset 越界、共享 group 场景），收缩侧剔除 +
  topic 重建回归测试留待 T18 与窄窗修复同批处理。

**split 化：走写死回退门（不硬上）**。评估结论（本节为唯一存档）：游标语义本身在 watermark-通道设计下可证（每 split 交付携带本批
位点，语句线程折叠成水位游标，flush 成功即整图 save——失败路径内存水位随
线程消亡、重启自台账整图恢复，at-least-once 窗口与单 consumer 同类）；但
验收同时要求「T5 口径字节级并行等价」——split 化**构造上**改变跨分区行序
（broker 到达交错序 → split 序），「同一 chunk 序列」的参照不存在，字节级
等价只能对 partition-major 特制夹具成立（夹具特化≠系统等价）；且聚合语句
的微批粒度随批形状改变（每批部分聚合的发射行随之改变；first/last/collect
类不可复原）。等价证明不完整 → 按卡内写死条款降级：**仅交付动态分区发现，
split 化并入 T18 升格评审议程**。

**宏基准（M4 首档）**：kafka(P=6)→file 100k 行 debug 档 4 轮（口径见脚注
⁵）：**中位 27861 rows/s**（23918/27678/28043/28474），每轮不丢行断言过。
与 §4.5 方案 a 基线 24658 的对比**跨机器不可比**（Intel i7-9750H → Apple
M4；§六头部声明与本卡 uname 实证 `arm64` / `Apple M4`）。本卡热路径零改动
（diff 无引擎数据面代码），无同机 A/B 意义（对照二进制=自身）；本组数字
的价值=**该 bench 在 M4 的基线档位**，供 T18 split 化若立项时同机同日对比。
旁证：M4 上 kafka(P=6)/file→file 比 ≈ 27861/33670 ≈ 0.83，与 Intel 机
T5 顺带口径的比值（24658/29394 ≈ 0.84）一致。

复现：`TAILRACE_KAFKA_PORT=19092 docker compose up -d kafka`（等 healthy）
→ `TAILRACE_KAFKA_PORT=19092 bash scripts/bench/macro-partitions.sh --rows
100000 --partitions 6`；发现延迟：`TAILRACE_TESTCONTAINERS=1 cargo test -p
tailrace-conn-kafka kafka_source_discovers -- --nocapture`。

### §6.4 T32 Doris/StarRocks sink 宏基准（2026-10-23，M4 同机同日）

> 首批 Doris sink 吞吐档（T18 共同事实 4 的数据面补全）。全部 debug 档
> （`CARGO_PROFILE_DEV_DEBUG=0`）、行形态同 §三宏基准（JSON 4 列 ≈95B）、
> rows=100000、compose doris-fe/be **2.1.7 单 BE**（BUCKETS 1 /
> replication_num=1）——与多 BE 生产集群不可比；跨机器不可比注记同 §六头部
> （§一 Intel 机的 R2 数字与下表互不可比）。量化噪声：console 档 t1 以 0.5s
> 步进轮询 stats，~3s 窗口的档位噪声 ±17%（脚注¹同款）；小批档窗口 3.7-9.1s
> 噪收窄。吞吐行原始报告：/tmp/r5-{doris,doris-p4,doris-p4-small,
> doris-p1-small,starrocks,file-100k}.md。

| 档 | 配置 | rows | elapsed_s | throughput | flushes |
|---|---|---|---|---|---|
| file→file（同日锚点） | 单管道，默认攒批 | 100000 | 2.979 | **33568 rows/s** | — |
| file→doris 单管道 | 默认攒批（flushRows 10000/pollRows 8000/checkpointMs 2000→flush_interval 2s） | 100000 | 2.954 | **33852 rows/s** | 7 |
| file→doris 单管道·小批 | flushRows 500 + pollRows 500 | 100000 | 9.077 | **11017 rows/s** | 200 |
| file→doris P=4 并发·小批 | 4 管道 ×25000 行（独立源文件→同一表），flushRows 500 + pollRows 500 | 100000 | 3.734 | **26781 rows/s** | 50/管 |
| file→doris P=4 并发·poll 地板批 | 名义 flushRows 500、pollRows 8000（批粒度被 poll 托底 ≈6250） | 100000 | 1.023 | **97752 rows/s** | 4/管 |
| file→starrocks（node 直跑档） | `tailrace-node run` 直跑，默认攒批（10000/10s/8MB） | 100000 | 11.428 | **8750 rows/s** | — |

**口径注记（随行）**：

- console 档（前五行）：口径同 §三——t0=deploy 返回 200（P=4 档=首条 deploy
  200），t1=全部管道 stats flushed≥各自目标，吞吐=N/(t1−t0)；sink 侧以 FE
  `COUNT(*)` 复核（六档全部 100000==100000 行数守恒）。
- **doris sink 零可测开销（大批下）**：单管大批 33852 vs 同日 file→file 33568
  （+0.8%，噪声界内持平）——10k 批 stream load 不在热路径上。与 T20 的 33670
  基线（2026-10-20，跨日）亦一致。
- **小批区（group commit 目标区）无 group commit 的基线**：引擎 DorisSink 不发
  Doris `group_commit` header（`conn-doris` `send_once` 常规头
  Authorization/label/format/Expect，Doris 档另加 read_json_by_line/
  sequence_column——均无 group_commit，
  engine/crates/tailrace-conn-doris/src/lib.rs:361）——本组是「小批×无 group
  commit」基线。单管小批 11017（200 次同步 stream load ≈45ms/次往返封顶，
  −67% vs 大批）；P=4 小批 26781（=单管小批的 2.43×，HTTP 往返部分重叠），
  **仍 −21% vs 单管大批 33852**。T18 ④T3 判据（≥4 管道×<1000 行批收益）的
  数据面结论：并发有相对收益、无绝对追平——收益缺口即 Doris group commit
  （或引擎侧攒批合并）的目标区，T36 谱系裁决输入。
- **P=4 poll 地板批 97752**：4 管道并发把 decode/transform/sink 全链并行化
  （2.89× 单管大批）——宏基准瓶颈在单管道吞吐而非 doris sink；亦证明单机
  BE 2.1.7 可吸收 4 路并发 stream load 无退潮。**并发档未降级**：console API
  4 管道并发建+部署无限制（回退门「降 P=2」未触发）。注：批粒度地板=pollRows
  （pending 每 poll +poll_rows 行）——探小批必须同压 pollRows，本卡首跑实证
  （flushes=4×~6250 而非 50×500）。
- **starrocks 档口径不同源**：console 尚无 starrocks datasource 类型
  （`datasources.rs` `registry_type` 白名单不含），走 `tailrace-node run`
  直跑（`scripts/e2e/starrocks.sh` 同路径）：t0=「pipeline run started
  (standalone)」日志时间戳（排除进程启动/装配），t1=FE COUNT 轮询（~1s 步进，
  ±9% 量化）；攒批=引擎默认（10000 行/10s/8MB，toml 无 tuning 面）。allin1
  单容器（FE+BE+nginx feproxy 拦 307，引擎 307 跟随路径不经此档）——仅作
  sink 差异量级参考，不与 console 档直接比。
- **环境配方（共享宿主机，操作者级——脚本不代做）**：磁盘 ≥5G 查 `df -h`；
  be-2.1.7 拒启于活动 swap → zram 0 用量时 `docker run --rm --privileged
  alpine swapoff -a`；`vm.max_map_count=2000000`；VM 8G → 临时 `docker stop
  doris-2.1 doris-be`（mdop 常驻栈）腾 ~1.9G，完事回 `docker start` + `swapon
  -a`。正典=researchEverything/AGENTS-环境契约与工作纪律.md §3。本档不涉
  kafka（`up -d --wait doris-fe doris-be`），`TAILRACE_KAFKA_PORT` 无关。

**复现**（栈复用：脚本成功后保留栈不自动拆，重跑免健康等待；收尾
`docker compose down -v` + 回挂 mdop 栈/swap）：

```bash
bash scripts/bench/macro.sh --rows 100000                            # file→file 锚点
bash scripts/bench/macro.sh --sink doris --rows 100000 --report /tmp/r5-doris.md          # 单管大批
bash scripts/bench/macro.sh --sink doris --rows 100000 --flush-rows 500 --poll-rows 500   # 单管小批
bash scripts/bench/macro.sh --sink doris --rows 100000 --pipes 4 --flush-rows 500 --poll-rows 500   # P=4 真小批
bash scripts/bench/macro.sh --sink doris --rows 100000 --pipes 4 --flush-rows 500         # P=4 poll 地板批
bash scripts/bench/macro.sh --sink starrocks --rows 100000 --report /tmp/r5-starrocks.md  # node 直跑档
```

### §6.5 T33 DataFusion 48.0.1→55.2.0 升级基准（2026-10-24，M4；**DF55 首档**，非同日前后对比）

> R5-W1 引擎独占波（roadmap T33）④/⑤ 步的数据落档。升级本体=bfbaeb3（datafusion/
> datafusion-proto 48.0.1→55.2.0 直跳，arrow 59.3.1 / sqlparser 0.62.0 / prost 0.14.4），
> 评审修 03bf261（纯登记）。**口径声明**：卡面要求「同机同日前后对比」，实际可得的是
> ——宏基准同机**不同日**（DF48 侧=§6.4 的 2026-10-23 锚点）、微基准**仅 DF55 单侧**
> （本机无 DF48 微基准档）——故本小节按「M4 首档」登记（T15 §六 :362 同款处置），
> 不冒称前后对比；跨机器不可比注记同 §六头部。原始输出：/tmp/r5w1-t33-bench.log、
> /tmp/t33-macro.md（脚注⁸ 含 git 标签勘正）。

| 档 | 口径 | DF48（同机对照） | DF55（本次） | 变化 / 结论 |
|---|---|---|---|---|
| 宏基准 file→file | debug，rows=100000，单管道默认攒批（macro.sh 脚注¹口径） | **33568 rows/s**（2.979 s；§6.4 锚点，2026-10-23） | **41528 rows/s**（2.408 s；sink 行数 100000 ✓） | **+23.7%**，同机不同日；**回退门（宏基准回吐 >10%）未触发** |
| 微基准 transform/identity_select_star | criterion release，8000 行/批（§2.2 口径） | —（本机无档） | **237.38 M rows/s**（33.701 µs/批；CI 236.67–237.92 M） | M4 DF55 首档；vs §4.4 Intel 基线 30.647 M **跨机不可比** |
| 微基准 transform/filter_project | 同上 | —（本机无档） | **65.89 M rows/s**（121.41 µs/批；CI 65.786–65.984 M） | M4 DF55 首档；vs §4.4 Intel 基线 8.958 M 跨机不可比 |
| 微基准 transform/groupby_aggregate | 同上 | —（本机无档） | **53.47 M rows/s**（149.62 µs/批；CI 53.214–53.701 M） | M4 DF55 首档；vs §4.4 Intel 基线 8.072 M 跨机不可比 |

**口径注记（随行）**：

- **回退门判定**：卡面回退门=「宏基准回吐 >10% 且无法归因修复 → 版本回 48.0.1」。
  DF55 宏基准 41528 vs 同机 DF48 锚点 33568 为 **+23.7%**（非回吐）；「不同日」的
  影响以同 bench 的 DF48 跨日漂移为界——T20 33670（10-20，3 轮中位）↔ T32 33568
  （10-23）仅 0.3%，+23.7% 远在其外，判定稳健。本小节**不**把 +23.7% 登记为 DF55
  的确定增益：单轮单档（脚注¹ ±17% 量化噪声）且为 debug 档，仅作「无回归」的证据；
  release 档/多轮中位补测后方可谈增益量。
- **微基准为何无前后差**：target/criterion 在本机为首跑（base==new 同 mtime、无
  change/ 目录），即 M4 上从未留下 DF48 的 transform 微基准档；§4.4 :110-112 的
  30.647 M/8.958 M/8.072 M 采自 §一 Intel i7-9750H（2026-10-17），7.75×/7.36×/6.62×
  的比值是机器差而非 DF 差，**禁止当作升级收益引用**。补测路径（非本卡回退门输入）：
  `git worktree add <dir> c2c30e8` 后在该树 `cargo bench -p tailrace-transform` 得同机
  DF48 档，再与本表 DF55 行按 criterion 口径对比。
- **DF48 两条已登记限制在 55 上的复核**（T33 卡 ⑤，结论已回写 §4.4 对应行旁注）：
  E1 memory pool「部分算子不强制」**维持**（datafusion-execution 55.2.0
  `src/runtime_env.rs:429` 注记逐字仍在）；E2 `output_batches`「仅 join/coalesce/unnest
  记录」**已解除**（BaselineMetrics 标准化，全算子自带 output_batches，测试加存在性
  断言）。逐条归因见 docs/dev/t33-golden-attribution.md §八 / §三 B-2。
- **契约 golden**：layer_a（proto 字节）/ layer_b（执行 pretty）在 DF55 下
  `UPDATE_GOLDEN=1` 重生成后与 DF48 fixture **零字节 diff**，C3 `rewrite_query`
  select_sql 48↔55 逐字节一致（零 diff 面 3）；行为面漂移 2 条（metrics 计数展示
  `8000`→`8.00 K` 等价、output_batches 解锁）逐条归因，无手编 golden——归因表
  docs/dev/t33-golden-attribution.md。
- **本小节不覆盖**：L3 `smoke.sh --full`（Doris 栈窗口按 §6.4 环境配方人工执行，
  R5-W1 波门禁登记「待跑」，T33 卡因此未收口）；DF55 的行数守恒/e2e 以 L1 契约套件
  （190 passed / 0 failed / 7 ignored，58 二进制）与宏基准 sink 行数 100000==100000 为限。

**复现**：

```bash
cargo bench -p tailrace-transform                                            # 微基准三项（DF55 树）
bash scripts/bench/macro.sh --rows 100000 --report /tmp/t33-macro.md          # 宏基准 file→file（debug）
```
