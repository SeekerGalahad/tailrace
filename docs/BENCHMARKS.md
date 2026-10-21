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
- `batch_size_fixed_from_options_not_first_batch`：同场景，固定 8000 → 输出 1 批 vs
  旧定格（模拟 batch_size=1）→ 8000 批。
- `t7_exec_plan_wiring`（runtime 接线）：声明 schema 的 `build_exec_plan` 计划 +
  `transform_plan(poll_rows)`：8001 行微批 1 个执行批、4001 行过滤语义、投影列序
  （order_no, seq10）与表达式值（seq*10）不变。
- `memory_pool_wired_with_configured_cap`：pool 装配断言
  （`memory_pool.memory_limit() == Finite(cap)`）。**如实登记**：DF 官方注明 pool
  "not yet respected in all cases"；实测 64 KiB 池下 8000 组 hash agg 照常完成（不
  强制拒绝），pool 是装配正确的护栏而非硬限制。
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

| 任务 | bench/口径 | 默认档 | jemalloc 档 | 变化 | 日期 |
|---|---|---|---|---|---|
| T20 | macro 吞吐（file→file debug，3 轮中位）¹ | 33670 rows/s | 33738 rows/s | 持平（+0.2%，远在噪声界内）¹ | 2026-10-20 |
| T20 | node 峰值 RSS（内核 ru_maxrss，3 轮中位） | 53616 KiB（52.4 MiB） | 57808 KiB（56.5 MiB） | **+4192 KiB（+7.8%）**，两档区间不重叠 | 2026-10-20 |
| T20 | node 二进制体积（debug 档）² | 264,674,112 B | 265,573,776 B | +879 KiB（jemalloc 5.3.1 静态链入） | 2026-10-20 |
| T20 | 碎片（jemalloc `stats.allocated` vs `stats.resident`） | 未测 | 未测 | 未测³ | 2026-10-20 |

> ¹ 吞吐口径脚注（R3-W1 评审 low#5 补注）：**debug 档**（`CARGO_PROFILE_DEV_DEBUG=0`，非 release）、单轮全链仅 ~3s、macro.sh stats 轮询步进 0.5s → **量化误差 ±17%**（§6.1 结论段自认）——本口径对 <~17% 的 allocator 效应**无检出力**，「持平」仅指差值未超出该噪声界；allocator 吞吐结论以 release 档长跑补测后为准。
> ² 体积口径脚注（low#5）：两个体积数字来自**基准二进制**（sha256 前缀 `1f7270c7…`/`55066ae4…`，§6.1 方法段），构建树 = **caa842e + T20 提交前工作树**（= 9a9f8a2 落章前的树；其后 488efcc/13d1c2b（engine 改动）、19bb80b（CI）均未包含）——**非任何已提交树的可复现产物**。2026-10-21 复查：本机现存 `target/debug/tailrace-node`（264,705,488 B，`nm` 计 `_rjem_` 符号 0 个）属其后另一树的重建产物，与表中默认档数字不可互推。
> ³ 碎片行（low#5）：T20 卡方案列有「碎片」对比项，实测未做（无 jemalloc-ctl stats 面，§6.1 结论段「未测」）——此行为显式占位，防「表里没有＝测过没事」的误读；补测需 `tikv-jemalloc-ctl` stats feature + release 长跑。

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
