# tailrace 开发路线正典（roadmap）

> 本文件是 tailrace 开发的**唯一任务正典**：所有开发任务先在此立项（任务卡），再按波次执行；
> 执行必须围绕本文件与文末的最终目标。状态标记：☐ 待做 ｜ ◐ 进行中 ｜ ✅ 完成。
> 基线：v0.1.0（2026-10-16 首次公开，CI 六 job 全绿，release 9/9 资产）。

## 一、最终目标（北极星）

**v1.0：一条 SQL、两个二进制、零 JVM 的数据接入引擎，管理台内置，Doris/StarRocks 写入语义按档位交付（1.0=label 幂等档；事务接口 exactly-once 与 group commit 吞吐档=v1.x，触发条件见 T18 纪要）。**

三条差异化支柱（约束所有取舍）：

1. **单二进制轻量边缘**：2-4GB 内存的盒子可跑引擎+管理台，无 JVM/Temporal/多容器编排；
2. **SQL-first + DataFusion**：管道即一条 `INSERT INTO 库.表 SELECT …`，不学私有 DSL；
3. **Doris/StarRocks 深度工程化**：写入语义谱系（label 幂等 → 事务接口 exactly-once → group commit 吞吐档）。

两条红线：不拼连接器数量；不做真正流式 join/水位线/状态后端。

轮次结构：**R2 性能与基准（当前轮）→ R3 亮点特性 → R4 1.0 发布**。R3/R4 任务卡在本轮收口后按同格式立项（§七）。

## 二、ADR-001：MSRV 抬升至 1.95（2026-10-17 裁决）

- **背景**：workspace 原声明 `rust-version = 1.85`，但锁定依赖 `rusqlite_migration 2.6.0` 声明 `rust-version = 1.95`——原声明不可执行（CI 的 1.85 job 仅因旧版 cargo 不强制校验依赖声明才通过）。
- **裁决**：抬 MSRV 至 **1.95**（诚实声明），而非降级依赖。理由：rusqlite_migration 2.6 是当前唯一可用版本线；降级引入 API 迁移成本与锁文件回退风险，收益仅为一个口头数字。
- **影响面**：`Cargo.toml` rust-version、`rust-toolchain.toml`、ci.yml msrv job（`dtolnay/rust-toolchain@1.95`）、双 README 前置条件、CONTRIBUTING。msrv job 自此真实编译验证。
- **复核节奏**：每次发版前 `cargo +1.95.0 check --workspace --locked` 本地复跑。

## 三、测试体系（五层，执行纪律）

所有波次与任务严格按层执行；跳层即视为任务未完成。

| 层 | 内容 | 命令（在仓库根执行） | 何时必须 |
|---|---|---|---|
| L0 静态门 | 格式/lint/供应链 | `cargo fmt --all --check`；`cargo clippy --workspace --all-targets --locked -- -D warnings`；`cargo deny check advisories bans licenses sources` | 每任务收尾 + 每波门禁 |
| L1 单元+契约 | 全 workspace 测试（v0.1.0 基线：124 passed / 1 ignored） | `CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked` | 每波门禁（任务内跑受影响 `-p`） |
| L2 端到端冒烟 | file→file 全链 25 步（console+node 真进程） | `./scripts/smoke.sh`（前置 `./scripts/build.sh`） | 涉及数据面/管道生命周期的波；W8 终验必跑 |
| L3 docker 链路 | kafka→doris 真实链路 + 毒数据 + 位点不丢不重 | `docker compose up -d` → `./scripts/smoke.sh --full` → `docker compose down -v` | W3 首次实跑；W8 终验；此后每次触碰 conn-kafka/conn-doris/runtime 提交语义的波 |
| L4 基准 | 微基准 + 宏基准，优化必须前后对比 | `cargo bench -p <crate>`；`bash scripts/bench/macro.sh --rows 100000 --report out.md` | T1 建立基线；T3/T5/T6/T7 每项入 BENCHMARKS.md 对比表 |

**本地测试环境事实**：本机 docker 为 **OrbStack**（docker 29.4.0 / compose v5.1.2，镜像加速已配置）。`docker-compose.yml`（Kafka + Doris）随 v0.1.0 入仓但**从未启动过**——首次实跑属 T11 任务，如实登记结果。L3 纪律：起链 → 等健康检查通过 → 跑 `--full` → **无论成败都 teardown**（`down -v`），失败时保留现场排查后手动清。

**CI 侧**（push 后）：六 job（rust=fmt+clippy+test+deny / msrv / no-default-features / macos / web / smoke）。每波门禁 push 后必须 `gh run watch` 到绿才算波次完成（上限 35 分钟）。

## 四、R2 任务卡（性能与基准轮，共 11 张）

> 原则：**基准先行**——T1 是 T3/T5/T6/T7 的门槛；没有前后对比数据的优化不收。
> 总量评估：~23 业余人日（各卡标注）；agent 执行按波次编排（§五）。

### T1 基准套件（~3 人日）〔门槛任务〕✅

- **现状**：零基准——无 benches/、无 criterion、无 BENCHMARKS.md；v0.1.0 的默认攒批参数（8k 行/100ms/10k 行 flush）与容量自报（8 管/24 语）均无测量支撑。
- **方案**：
  1. criterion 微基准（workspace dev-dependency，per-crate `benches/`）：`tailrace-decode`（JSON/CSV → Arrow 吞吐）、`tailrace-transform`（热点 SQL 算子每批固定成本）、sink 序列化（conn-file jsonl 行编码）；
  2. 宏基准脚本 `scripts/bench/macro.sh`：file→file 全链（起 console+node debug/release、10 万行种子、吞吐+耗时报表落 `--report` 文件）；
  3. `docs/BENCHMARKS.md`：方法学声明（数据集形态/行宽/机器规格占位）+ 首批基线数据；
  4. CI：微基准仅编译验证（`cargo bench --no-run` 进 rust job），不上传数据。
- **验收**：`cargo bench -p tailrace-decode --no-run` 通过；`bash scripts/bench/macro.sh --rows 100000 --report /tmp/r2-baseline.md` 出吞吐行；`test -s docs/BENCHMARKS.md` 且含首批数字。
- **文件域**：engine 各 crate `benches/`、root `Cargo.toml`（criterion workspace dep）、`scripts/bench/`、`docs/BENCHMARKS.md`。
> 结果：fa9f74d decode 302K / transform 19.1M / sink encode 8.43M rows/s；宏基准 100000 行 4.600s=21739 rows/s(debug 档)，docs/BENCHMARKS.md 已立方法学+基线

### T2 可观测：tracing + OTLP（~3 人日）✅

- **现状**：`eprintln!` 是唯一日志通道（engine 侧 37 处：conn-kafka / conn-file / state / runtime / conn-doris / api 各处），无级别过滤、无结构化字段、无管道关联；root Cargo.toml 已声明 tracing / tracing-subscriber(env-filter)（零新基础依赖）。
- **方案**：37 处 eprintln → `tracing::warn!/info!` 带 `pipeline_id`/`statement_idx` 字段；两个二进制 main 初始化 subscriber（env-filter，默认 info）；`PipelineLogHub`/emit 三件套保留并作为 tracing 的一个 layer sink（**console 侧 SSE 消费链路不动**——R1 审计红线）；OTLP exporter 以 feature 门控（`opentelemetry` + `tracing-opentelemetry`，`features = ["otel"]` 默认关），埋点四组：批次大小/批次延迟/sink 在途/sink 成功率；`docker-compose.yml` 增 otel-collector 可选 profile。
- **验收**：`grep -rn 'eprintln' engine/crates --include='*.rs' | wc -l` → **0**；`RUST_LOG=tailrace_runtime=info` 启动可见带 pipeline_id 的结构化行；`cargo test --workspace` 全绿（SSE 桥行为不变由既有测试保证）；`--no-default-features` check 仍绿。
- **文件域**：engine 全部含 eprintln 的文件、两个 main、root Cargo.toml、docker-compose.yml。
> 结果（2026-10-17）：engine/crates 37 处 eprintln 全部转 tracing（connector 层带 topic/partition/path 字段，runtime 带 pipeline_id/statement_idx/kind）；新增 `PipelineLogLayer`（tracing Layer → PipelineLogHub sink，仅 WARN/ERROR 且带 pipeline_id 的事件入环，INFO 仍归 emit——SSE 链路与 emit 三件套零改动，新增单测 `tracing_layer_sinks_pipeline_tagged_warnings`）；node main 初始化 registry+EnvFilter(默认 info)+fmt(**with_writer(stderr)**，fmt 默认是 stdout)+hub layer；OTEL feature 默认关（opentelemetry 0.33.1 + tracing-opentelemetry 0.34 + otlp http-proto/blocking-reqwest + internal-logs），运行时由 OTEL_EXPORTER_OTLP_ENDPOINT 激活，四组埋点=`sink_flush` info_span（rows/flush_ms/in_flight_rows/pipeline_id/statement_idx，六个 flush 点，RUST_LOG=warn 会关掉 INFO 级 span 导出）；docker-compose `--profile otel` 加 otel-collector:0.163.1（默认栈不含）。验证数字：grep eprintln=0；受影响 7 crate 测试 39 passed/0 failed/1 ignored；clippy -D warnings 默认与 --features otel 均 0 告警；`cargo check --workspace --no-default-features --locked` 绿；deny 四门 ok（重复版本 warn 16→18，prost 系带入）；OTLP E2E：本地 HTTP 探针收到 `POST /v1/traces`（application/x-protobuf 555B）；`RUST_LOG=tailrace_runtime=info` stderr 可见 `pipeline run started (standalone) pipeline_id=7001 statements=1 duration_secs=3` 结构化行；compose config 校验默认服务不含 otel-collector、`--profile otel` 才含。注：otel collector 容器实跑留 T11（本机 OrbStack 未启动，docker-compose 配置已过 config 校验）。
> 结果：4b90060 engine/crates 37 处 eprintln 全部转 tracing 结构化宏（runtime 带 pipeline_id/statement_idx/kind，connector 带 topic/partition/path）；新增 PipelineLogLayer 使 PipelineLogHub 成为 tracing layer sink（仅 WARN/ERROR 带 pipeline_id 事件入环，emit 三件套与 console SSE 链路零改动+新增单测）；node main 初始化 EnvFilter(默认 info)+fmt(stderr)+hub layer；OTLP 出口以 otel feature 默认关闭（opentelemetry 0.33.1 + tracing-opentelemetry 0.34，OTEL_EXPORTER_OTLP_ENDPOINT 运行时激活），四组埋点=sink_flush info_span 于全部 6 个 flush 点；docker-compose 增 --profile otel 的 otel-collector:0.163.1。验证：grep eprintln engine/crates=0；受影响 7 crate 测试 39 passed/0 failed/1 ignored；clippy -D warnings 默认与 --features otel 均 0 告警；no-default-features workspace check 绿；deny 四门 ok；OTLP E2E 探针实收 POST /v1/traces（protobuf 555B）。

### T3 热路径零拷贝（~4 人日）✅〔依赖 T1 基线〕

- **现状**（R1 审计确认五分点）：每条消息 `raw.to_vec()` 做 simd-json 可变缓冲（decode），而 SourceChunk.raw 本是 owned Vec——签名强迫二次拷贝；每行 clone 全部列名的 `Vec<String>`（batch_fields，N 列 = N 个 String 分配/行）；kafka payload `.to_vec()` 与前者合计每消息 ≥2 次全量拷贝；parquet 整文件再拷；FileSource 每行 from_utf8_lossy 双拷贝。顺手项（先验证后修）：FileSink jsonl flush 的「写 tmp→fsync→整读回内存→append」多一次全量读；DorisSink 每次 send 把 buf 再拷进 reqwest body ×4 重试。
- **方案**：Decoder 签名改收 `Vec<u8>` 所有权（simd-json 原地解析）；列名→索引映射在 ColBatch 构造时缓存一次；kafka payload move 进 SourceChunk；file 读栈修正；FileSink 直接 `write_all`；DorisSink buf 包 `bytes::Bytes` 共用。**语义不变，纯签名/缓存改造**。
- **验收**：criterion decode/transform 微基准前后对比表入 BENCHMARKS.md（具体倍数以实测为准，不预填）；既有 decode/transform 测试 + 契约测试全绿；宏基准吞吐行前后对比。
- **文件域**：tailrace-decode、conn-kafka（source 侧）、conn-file、conn-doris、tailrace-connector（如 trait 签名牵连）。
> 结果：Decoder trait 收 Vec<u8> 所有权（JSON/OGG 借用封装进内层 walk 保 Bad 还回；Parquet `Bytes::from(Vec)` 零拷贝）；ColBatch `parts()` split-borrow 消除每行 N 个 String 列名分配（schema.fields 即构造时缓存）；FileSource 读栈字节化（完整行 move 进 chunk）+ FileSink flush 直写（免 tmp 写+整读回+一档 fsync）；DorisSink 载体 BytesMut→Bytes（重试 body refcount clone）；kafka 侧验证后无需 diff（`.to_vec()` 是 rdkafka C 堆唯一边界拷贝，全链 2→1 由 decode 签名兑现）。验收数字（同机同日，BENCHMARKS §4.1）：decode json **328.9K→534.9K（+33% 保守，二次样本 438.5K）**、ogg **+45%**、csv +11%（噪声界内）；sink flush **234.4K→458.2K（+95%）**；transform 零改动三项 +1~4%（criterion 判噪声/no-change，无回归）；宏基准 **21739→24649 rows/s（+13.4%，debug 档）**。测试：受影响 6 crate+契约 38 passed/0 failed；clippy --all-targets -D warnings 绿。已知取舍（实测登记）：JSON/OGG Bad 行还回的 buffer 可能已被 simd-json 转义展开改写（毒行含转义时 DLQ 样本为解析视图），happy-path 每消息拷贝消除的固有代价，`Decoded::Bad` doc 注明。
> 结果：a5b48fd T3 热路径零拷贝：Decoder trait 收 Vec<u8> 所有权（walk 内层封装借用，Bad 路径还回 buffer）、ColBatch parts() split-borrow 消除每行 N 个 String 列名分配、Parquet Bytes 零拷贝、FileSource 读栈字节化、FileSink flush 免 tmp 写+整读回+一档 fsync、DorisSink 载体 BytesMut→Bytes 免重试拷贝；kafka→decode 全链每消息拷贝 2→1。同机同日前后对比：decode json 328.9K→534.9K rows/s(+33% 保守)、ogg +45%、sink flush 234.4K→458.2K(+95%)、宏基准 21739→24649/25246 rows/s(+13.4~16.1%)；transform 零改动无回归；受影响 6 crate+契约 38 passed/0 failed。

### T4 毒数据载体：JSON-lines（~1 人日）✅

- **现状**：kafka sink 用 CSV 载体——含换行字段触发序列化 bail → 语句线程 Err → 注册表重启循环 → 游标无推进 ×3 → **FAILED 终态**（毒记录永久杀死管道）；CSV 同时把数字列降级字符串；CSV 解码器无引号转义（RFC4180 不合规）。
- **方案**：sink 载体换 JSON-lines（file sink 已用 `arrow::json::LineDelimitedWriter`，同款替换）；CSV 解码器换 `arrow` csv reader 或 `csv` crate（RFC4180 合规）。
- **验收**：新增单测——含换行/引号/emoji 字段的行经 kafka sink 序列化可被 `arrow::json::Reader` 无损回读；e2e 毒数据场景（L3：kafka→kafka 或 file→kafka 持续 ≥10 分钟 RUNNING 且行数守恒）入 smoke `--full`；`cargo test -p tailrace-conn-kafka -p tailrace-decode` 绿。
- **文件域**：conn-kafka（sink 侧）、tailrace-decode（csv 分支）。
> 结果：8d0a8fc kafka sink载体CSV→JSON-lines（arrow::json::LineDelimitedWriter，与file sink同款，抽出encode_json_lines）消除毒记录bail-重启循环-FAILED终态；CsvDecoder由裸split(',')换csv crate RFC4180记录文法（带引号逗号/双写引号/字段内换行，逐列强制语义不变：坏数字降null不丢行）；毒数据单测4行换行/引号/emoji/控制字符经sink序列化后arrow::json::Reader无损回读（4/4字段相等，int64/float64类型保持，一行一记录不变量）；验收cargo test -p tailrace-conn-kafka -p tailrace-decode --locked＝12 passed/0 failed；依赖仅csv="1.4"（经arrow-csv已在lock，仅+1条边零新包）。

### T5 分区并行·方案 a（~2 人日）✅〔依赖 T1；与 T3/T6 串行〕

- **现状**：每语句单线程 poll 全分区，decode+transform+sink 全部串行在同一 OS 线程——单语句吞吐上限 ≈1 核；一次慢 Doris flush（120s 超时 ×4 重试）期间全部分区停摆。
- **方案 a（最廉价、无语义风险）**：同一 poll 的 chunks 相互独立，并行 decode 到各自 ColBatch，flush 前 `concat_batches` 合并；**游标语义完全不动**（仍按 flush 点整体提交）。方案 b（split 化+动态分区发现）属 R3，先不做。
- **验收**：多分区 topic 宏基准吞吐对比（a vs 现状）入 BENCHMARKS.md；offsets 正确性断言——重启后不丢不重（totals 单调且重启后继续增长）；NO_PROGRESS_LIMIT 语义回归测试；契约测试全绿。
- **文件域**：tailrace-runtime 主循环、conn-kafka poll 路径。
> 结果：`decode_poll` 同 poll chunks 连续切片分组→scoped 线程并行 decode 到各自 ColBatch→`concat_batches` 按 chunk 序合并；低于 512×2 chunks 走原串行路径（涓流零开销）；游标语义（flush 点整体提交）与 NO_PROGRESS_LIMIT=3 完全不动；牵连 `Decoder: Send + Sync` 一处 trait 声明（4 个实现均无状态 unit struct，零实现改动）；conn-kafka poll 路径验证后无需 diff（rdkafka 单 consumer 拉取边界，属 R3 方案 b）。验收数字（交错 A/B，同窗口各 4 轮，BENCHMARKS §4.5）：多分区 kafka(P=6)→file **21396→24658 rows/s（+15.2%，debug 档）**，file→file 顺带 +16~19%；方法学修正：不交错先后测量同二进制漂移 16.4K→25.0K，不可比。动工前先写的 5 个语义回归测试（部分分区指纹/重启不丢不重 6800 行唯一键/totals 单调 5000→6800/NO_PROGRESS=3 FAILED 判决/进步复位不误杀）+ 并行等价性单测（4096 chunks 混合 Append/Skip/Bad，JSON-lines 编码字节级一致）全绿；契约套件抓到并行化初版 Fail 策略 bad_now 错接（=DLQ 载荷数）已修。clippy -D warnings 0；runtime/decode/contract 32 passed/0 failed。
> 结果：50ee4b5 T5 方案a落地：同 poll chunks 连续切片分组→scoped 线程并行 decode 到各自 ColBatch→concat_batches 按 chunk 序合并（<1024 chunks 走原串行路径），游标 flush 点整体提交与 NO_PROGRESS_LIMIT=3 语义不动；交错 A/B 验收 kafka(6分区)→file 21396→24658 rows/s（+15.2%，debug 档），file→file 顺带 +16~19%；动工前先写的 5 个语义回归测试（指纹/重启不丢不重 6800 唯一键/totals 单调/NO_PROGRESS=3/进步复位）+ 并行等价性单测（4096 chunks 字节级一致）全绿，clippy 0 警告，32 测试 0 失败。

### T6 攒批三阈值（~2 人日）✅〔依赖 T1；与 T5 串行〕

- **现状**：攒批参数（行/时间）无字节维度且无下游反压联动；Sink trait 的 `buffered_bytes()` 默认返回 0 = 字节边界永不触发。
- **方案**：行数/字节/时间三阈值任一满足即 flush；参数命名对齐 Doris Flink connector 语义（`sink.buffer-flush.max-rows / max-bytes / interval` 风格）；Doris/kafka/file sink 实现真实 `buffered_bytes()`。（在途反压属 v1.x，不做。）
- **验收**：三阈值各自触发的单测（构造行数触发/字节触发/时间触发三场景）；批大小分布指标入 BENCHMARKS.md；契约测试全绿。
- **文件域**：tailrace-runtime flush 逻辑、tailrace-connector sink trait、conn-doris / conn-kafka / conn-file。
> 结果（2026-10-17）：三阈值条件提取为 `RunOptions::flush_due` 单一谓词（两语句主循环共用；RunOptions/registry 注释钉住 `sink.buffer-flush.max-rows/max-bytes/interval` 参数映射，plan 面同名三维 flushRows/flushBytes/flushIntervalMs 经 clamp 接线不变）；`KafkaSink::buffered_bytes()` 从默认 0 改为**真实在途计量**（SinkProducerContext 投递回调记账：enqueue +payload/ delivery −payload，`in_flight_bytes` 原子计数）——至此 file（arrow 内存）/doris（carrier 字节）/kafka（在途 payload）三类 sink 字节边界全部真实（v0.1.0 时仅 kafka 缺）。验收数字：三阈值隔离单测（谓词级 13 断言 + 真 file→file 集成 3 场景：行数 500/200/100→恰 2 次 200 行 flush；字节 300/1024B/100→3 次 100 行；时间 100/1s→idle 路径 1 次；另两阈值各场景不可达保证归属）全绿，稳定性 6/6 轮；kafka buffered_bytes 离线精确账（==JSON-lines 字节总量、失败 flush 保留）+ testcontainers 真链路 flush 后==0（8.80s）；批大小分布入 BENCHMARKS.md §4.6（默认阈值 6 批全 16000+终局 4000；128KiB 字节档 13 批 p50=8000）+ 粒度事实如实登记（检查点在微批边界，行数边界以 poll_rows 为步进，Doris connector 逐 record 检查）；契约 7/7 绿；clippy -D warnings 0。
> 结果：2eedf12 T6 完成：三阈值（行/字节/时间任一满足即 flush）提取为 RunOptions::flush_due 单一谓词并钉住 Doris Flink connector sink.buffer-flush.max-rows/max-bytes/interval 参数映射；KafkaSink::buffered_bytes() 从默认 0 改为真实在途计量（投递回调 enqueue+payload/delivery−payload 原子账），file/doris/kafka 三 sink 字节边界全部真实；三阈值三场景集成测试+谓词隔离单测全绿（稳定性 6/6 轮），批大小分布指标入 BENCHMARKS.md §4.6（默认阈值 6 批全 16000+终局 4000；128KiB 字节档 13 批 p50=8000），契约 7/7 绿、clippy -D warnings 0 告警。

### T7 内存治理（~2 人日）✅〔依赖 T1；与 T5/T6 串行〕

- **现状**：SessionConfig 的 batch_size 用**第一个微批的行数定格**（首批 1 行则该语句线程永远 batch_size=1，后续 8000 行微批被切成 8000 个单行 RecordBatch 流过整个计划）；SQL 每微批重新 parse+plan。
- **方案**：batch_size 固定为 poll_rows 而非首批行数；SQL 物化为 LogicalPlan 每语句一次（`tailrace-ir` 的 `build_plan` 是现成计划产物，接线执行层）；DataFusion memory pool 配置进 plan options。
- **验收**：`max_rows_per_batch` 生效单测（首批 1 行 + 后续 8000 行 → 计划执行批数下降，DataFusion explain metrics 断言）；宏基准 RSS/吞吐对比入 BENCHMARKS.md；全量测试绿。
- **文件域**：tailrace-runtime（session 组装）、tailrace-transform、tailrace-ir。
> 结果：batch_size 固定为 poll_rows（`TransformOptions`，session 组装一次定型）；SQL 物化为 LogicalPlan 每语句一次（tailrace-ir 新增 pub `build_exec_plan` 组装期 parse+plan——坏 SQL 部署即失败；执行层新入口 `transform_plan` 每微批仅换 `src` source Arc，qualifier 由计划锚定）；memory pool 进 session 组装（`RuntimeEnvBuilder::with_memory_limit`，默认 256MiB/语句；如实登记：DF48 pool 对部分算子不强制，64KiB 池下 8000 组 agg 照常完成，装配有单测断言）。**机制修正（探针实证）**：任务卡所述「8000 行切成单行批」在 DF48 内存表路径仅对**聚合输出**成立（scan/project 不切批），验收测试按实证锚定：首批 1 行+后续 8000 行 → 执行批数 **8000→1**（DF48 无 `output_batches` 指标于聚合/投影计划——已按 explain 表真实指标+collect 批数断言并在 BENCHMARKS §4.4 注明）。微基准（同机同日）：identity **19.3→30.6M（+58%）**、filter **+20%**、groupby **+30%**（p<0.05）；宏基准吞吐持平 25138→25094（scan/project 形 SQL+debug 档 decode/fsync 主导，增益被链路稀释）、node 峰值 RSS 46888→49124 KiB（物化计划常驻 +2.2MiB）。过程修复：契约测试抓到 serve 部署 handler 异步上下文内嵌套建 runtime panic → `build_exec_plan` 改专用线程。测试：transform 4 + runtime 10 + ir 2 passed（golden 字节稳定）、契约 7/7；clippy `-D warnings` 绿。
> 结果：a741db1 T7 内存治理完成：batch_size 固定为 poll_rows（TransformOptions 一次定型）、SQL 物化为 LogicalPlan 每语句一次（tailrace-ir pub build_exec_plan 组装期 parse+plan，transform_plan 每微批仅换 src source Arc）、DataFusion memory pool 进 session 组装（GreedyMemoryPool 默认 256MiB/语句）。微基准 identity +58%/filter +20%/groupby +30% rows/s（p<0.05）；验收单测：首批 1 行+后续 8000 行计划执行批数 8000→1。宏基准吞吐持平（25138→25094，scan/project 形 SQL 由 decode/fsync 主导）、node 峰值 RSS 46888→49124 KiB（+2.2MiB 物化计划常驻）。

### T8 console 心跳写优化（~1 人日）✅

- **现状**：心跳批量写逐行 autocommit，未设 `synchronous=NORMAL`（数千事件 → 数千 fsync 在唯一 Mutex<Connection> 内串行，阻塞所有 API）；多语句写无显式事务（崩溃即半写）。
- **方案**：pragma 增设 `synchronous=NORMAL`（SQLite 官方对 WAL 的推荐）；心跳批量 INSERT 与既有写路径包 `Connection::transaction()`。
- **验收**：`cargo test -p console-store -p tailrace-console` 全绿；新增并发心跳压测单测（N=500 事件批 < 数秒）。
- **文件域**：console-store（nodes.rs 写路径、lib.rs pragmas）、牵连的事务化写闭包。
- **结果**（2026-10-17）：pragmas 增 `synchronous=NORMAL`；新增 `Store::with_tx`（`Connection::transaction()`，失败即回滚）；事务化 7 条多语句/批量写路径（apply_heartbeat / insert_node_events / insert_pipeline_metrics / delete_node_cascade / purge_expired / set_pipeline_published / update_datasource）。验收数字：`cargo test -p console-store` 22 passed（含新增并发压测与 publish 原子回滚两测）+ `-p tailrace-console` 25 passed，0 failed；clippy `-D warnings` 绿。压测（4 并发 × 500 事件批，文件库实测）：最差批 4.50s（修复前 autocommit+FULL）→ **31.7ms**（≈142×）。

### T9 web 首屏：monaco 拆分裁剪（~2 人日）✅

- **现状**：首屏静态加载 monaco 全量 chunk（4.43MB / gzip ≈1.1MB），登录页被拖垮；根因是编辑页静态 `import * as monaco` + advancedChunks 分组把共享预加载助手一并收编。
- **方案**：SqlMonaco 改 `defineAsyncComponent` + 编辑页 loading 骨架；入口裁剪到 `monaco-editor/esm/vs/editor/editor.api` + SQL 词法定义（SQL 无 LSP worker，单 editor worker 保留）；分组收紧（minShareCount）针对「共享助手被吞并」机制。
- **验收**：`cd web && pnpm build` 后 `grep -c monaco dist/index.html` → **0**（首屏零加载）；`dist/assets/` 中 monaco 相关 chunk 合计 **gzip ≤ 1.5MB**；`pnpm typecheck && pnpm build` 绿；编辑页手测可打开（smoke 不覆盖，L2 由 CI web job 保证构建）。
  > **验收修订（2026-10-17，用户裁决）**：原「合计 < 1.5MB（raw）」不可达——实测 monaco-editor 0.57 裁剪到极限（editor.api + sql 词法、零 contrib）即 2,706,886 B（2.58MiB），且零 contrib 时 suggest/find 控制器不注册、SQL 表补全（§6.5.2 核心特性）失效；含补全的可用最小面 ≈3.91MiB raw / 883kB gzip。验收按 gzip 口径执行（raw→gzip 修订理由：异步 chunk 的网络相关量是 gzip 字节；首屏性能目标不受影响）。首屏零加载目标不变且已达成。
- **文件域**：web/src/pages/pipelines/SqlMonaco.vue、pages/pipelines/edit.vue、vite.config.ts。
- **结果**（2026-10-17）：SqlMonaco 入口裁到 editor.api + sql 词法 + 25 个编辑侧 contrib（editor.main 的语言/LSP 面全砍）；edit.vue defineAsyncComponent + NSkeleton 骨架 + 错误兜底；advancedChunks minShareCount=2（共享预加载助手不再被吞，独立 75.7kB chunk）；修复异步化引入的两个真实缺陷（CSS 晚到→编辑器 5px 塌缩，加 whenLaidOut+whenMonacoCssReady 双门与宿主 RO 强制 relayout；codicon.css 无 exports 映射→vite alias）。验收数字：`grep -c monaco dist/index.html` = **0**；`pnpm typecheck && pnpm build` 绿；monaco 相关 chunk 合计 gzip **883.22 kB ≤ 1.5MB**（raw 3,467,180+8,840 JS / 162,369 CSS / 153,228 ttf / 303,843 editor-worker）。对照 v0.1.0：首屏 monaco 4,433,097 B 静载 → **0**；dist 内 css/html/json/ts 四 LSP worker 9,183,152 B → **0**；monaco JS −22%。浏览器实测（stub API + v0.1.0 对照构建）：编辑页打开、编辑器 924px 正确创建、完整 DOM（ime-textarea/scrollable/minimap）、v-model 管线通、零运行时错误；rAF 级视觉/补全交互因测试面板被 OS 遮挡无法执行（详见提交说明）。
> 结果：31305ae T9 完成：monaco 全量入口裁到 editor.api+SQL 词法+25 个编辑侧 contrib 并异步化（defineAsyncComponent+骨架+minShareCount=2），首屏零 monaco 字节（grep -c monaco dist/index.html=0），monaco 相关 chunk 合计 gzip 883.22kB ≤1.5MB（raw 4,095,460B），v0.1.0 对照确认 LSP worker 9.18MB 全消 + monaco JS 缩 22%，并修复 CSS 塌缩与预加载助手被吞两个异步化真实缺陷。

### T10 web SSE 日志续读（~1 人日）✅

- **现状**：SSE 断线重连尾屏重放 + 显示层丢行（无 seq 去重；桥游标是每连接局部变量）；未部署管道的监控页无限重连死循环。
- **方案**：后端 SSE 桥支持 `after_seq` 查询参数（引擎日志条目自带全局 seq）；前端 LogTerminal 维护 lastSeq，`seq <= lastSeq` 丢弃，重连不清空已有行；监控页 `v-if="deployed"` 才挂 LogTerminal。
- **验收**：新增集成测试——断开重连后不重放已见行；未部署管道日志区不发起 SSE；`cargo test -p tailrace-console` + `pnpm typecheck` 绿。
- **文件域**：console api/pipelines.rs（SSE 桥）、web LogTerminal.vue、monitor.vue。
- **结果**（2026-10-17）：SSE 桥接受 `after_seq` 查询参数种子化引擎游标（>0 首拉即增量、跳过尾屏重放；背压结构 bounded 64/断连感知/300s 未动）；LogTerminal 接管重连（2s 节奏）：重建 channel 携带 `&after_seq=<lastSeq>`、`seq<=lastSeq` 丢弃、重连不清空已显示行；monitor.vue `v-if="deployed"` 才挂 LogTerminal（未部署管道不再发起 SSE → 无无限重连死循环）。验收数字：新增集成测试 2 个——重连不重放（首屏收 seq 1..3 断开，引擎补 seq 4，`after_seq=3` 重连仅流 seq=4，且 mock 引擎侧证实首拉即 `after_seq=3` 而非 tail）与未部署单帧 `eof:not_deployed` 即收流；`cargo test -p tailrace-console` **25 passed / 0 failed**；clippy `--all-targets --locked -D warnings` 绿；`pnpm typecheck` + `pnpm build` 绿。

### T11 CI/测试深化 + docker 链路首跑（~2 人日）✅

- **现状**：契约测试已进 CI，但 chaos/活性测试仍 live-gated 跳过；`docker-compose.yml`（Kafka+Doris）与 `smoke.sh --full` 编排就绪**从未实跑**（v0.1.0 如实登记的遗留）。
- **方案**：
  1. **本地首跑（OrbStack）**：`docker compose up -d` 等健康 → `./scripts/smoke.sh --full`（kafka→doris 端到端 + 位点不丢不重断言 + T4 毒数据场景）→ 结果如实登记本文件与 BENCHMARKS.md → `down -v`；
  2. CI：`tests/chaos.rs` 与 conn-kafka 活性测试改 **testcontainers-rs**（CI 起临时 Kafka），live-gate 语义保留为本地可选；
  3. smoke job 增 `--full`（GitHub runner 内起 compose Kafka+Doris，超时与内存预算先行验证）。
- **验收**：本地 OrbStack 全链绿（截图/日志摘要入 docs/dev/）；CI 新增 chaos job 绿；`docker compose down -v` 后现场无残留（`docker ps` 空）。
- **文件域**：tests/chaos.rs、conn-kafka 测试、docker-compose.yml、smoke.sh、ci.yml。
- **结果**（2026-10-17）：本地 OrbStack 首跑**两轮 38/44 → 修复后 44/44 全绿**（全程 11m20s 含 600s 毒数据浸泡；S9 守恒 1700==1700==1700==COUNT，S9.9 换行/引号/emoji/中文字节级往返；`down -v` 后容器/网络 0 残留；容器内存峰值 kafka 372MiB / fe 867MiB / be 1.08GiB——runner 预算依据）。首跑挖出并修掉一串真问题（详见 `docs/dev/T11-docker-compose-smoke-full-first-run-2026-10-17.md` F1–F10）：镜像源（bitnami 消失/Doris tag 改名）、宿主盘满两轮复发（BDB `DiskLimitException`，清 17G target/debug 后 BE 5s 注册 Alive）、console 部署计划 `stream_load_url` 丢斜杠→全量 flush 404（已加回归断言钉住完整 URL）、BE healthcheck 不感知 FE 注册 + smoke 建表步假绿（`mysql -e` 出错仍 exit 0）、**Doris CSV 载体在官方 be-2.1.7 无 RFC4180 模式**（实测矩阵：任一 enclose/escape 组合都毁掉双写引号/逗号行）→ conn-doris 载体换 **JSON lines**（`format:json`+`read_json_by_line`，serde_json 转义全覆盖；新增 `doris_json_carrier_poison_roundtrip` 单测钉毒值往返）。testcontainers 接入：chaos 与 conn-kafka 活性在 `TAILRACE_TESTCONTAINERS=1` 下自起 ephemeral Kafka+Doris（确定端口/泄漏清理重试/nofile ulimit/Alive 门/group 加入窗口，F7/F8 八轮调试收敛），本地实跑 chaos **1 passed 60.56s**（kill-9@5s → 重启 → COUNT==5 幂等对账）、conn-kafka 活性 **1 passed 8.60s**，默认 live-gate 语义保留；ci.yml 新增 chaos job（+runner vm.max_map_count=2e6）并把 smoke job 升级 `SMOKE_FULL_SOAK_SECS=180 --full`（≥10min 口径由本地默认 600s 承担）。CI 绿门禁：首跑两轮全红——GH runner 默认 4096MB swap 致 Doris BE start_be.sh 拒启且被镜像 entrypoint 洗白为容器 exit(0)（**T11 文档 F11**，chaos/smoke 两 job 同源）；chaos/smoke job 加 `sudo swapoff -a` 后 smoke 即绿，chaos 另修 4-vCPU runner 冷 broker 首次 group join 23s+ 的时序预算（phase2 25→60s、活性 poll 30→90s）；**run 39024628616 七 job 全绿**（chaos 149.41s 含 reconcile 5/5、smoke 8m44s 含 --full）。
> 结果：9a2a7a7 docker-compose(Kafka+Doris)+smoke --full 本地首跑从两轮 38/44 修到 44/44 全绿（11m20s 含 600s 毒数据浸泡，1700 行端到端守恒、毒值字节级往返）；修掉 console plan URL 丢斜杠(404)、DorisSink CSV→JSON-lines 载体、BE 注册感知健康检查等 F1–F10；chaos 与 conn-kafka 活性接 testcontainers（本地 chaos 1 passed 60.56s、活性 1 passed 8.60s）并新增 CI chaos job；数字已登记 roadmap/BENCHMARKS.md/T11 文档。

## 五、波次编排（agent 工作流执行协议）

文件域冲突分析驱动的编排（同波任务文件域不相交；engine 热路径任务严格串行）：

| 波 | 任务（并行 ‖） | 引擎锁 | 波门禁 |
|---|---|---|---|
| W1 | T1 基准套件 ‖ T8 console 心跳写 ‖ T10 web SSE 续读 | 无冲突 | L0+L1+push+CI 绿 |
| W2 | T2 tracing+OTLP ‖ T9 web monaco 首屏 | T2 持引擎锁 | L0+L1+push+CI 绿 |
| W3 | T4 毒数据载体 ‖ T11 docker 链路首跑+CI 深化 | T4 持引擎锁 | L0+L1+**L3(OrbStack)**+push+CI 绿 |
| W4 | T3 热路径零拷贝（含 bench 前后对比） | 串行 | L0+L1+L4+push+CI 绿 |
| W5 | T7 内存治理（含 bench 对比） | 串行 | L0+L1+L4+push+CI 绿 |
| W6 | T5 分区并行 a（含多分区 bench） | 串行 | L0+L1+L4+push+CI 绿 |
| W7 | T6 攒批三阈值（含批分布指标） | 串行 | L0+L1+L4+push+CI 绿 |
| W8 | 终验：全量 L0-L4 + BENCHMARKS.md 终稿 + 收官报告 | — | 全绿收官 |

**执行纪律（对所有参与 agent 生效）**：

1. 工作目录固定 `~/GLMProjects/tailrace`（双开发机一致的正典路径；本地 `~/IdeaProjects/tailrace` 为指向此处的软链）；开工先读本文件对应任务卡与「做得好的红线」；
2. 红线：状态账本四步持久化与 assign-only 恢复路径不动；emit 结构化事件不被 tracing 化破坏；SSE 桥背压结构（bounded 64/断连感知/300s 上限）不动；smoke.sh 骨架不拆；
3. 提交：Conventional Commits（perf:/feat:/test:/chore:），一任务一提交；**只 `git add` 自己任务卡的文件域**；禁止 amend/rebase/改 tag/动 v0.1.0；push 只由波门禁执行；
4. 测试：按 §三分层严格跑；失败即修，修不好即停波上报（不静默降级）；
5. 每张任务卡完成后：把本文件中该卡状态改 ✅ 并附一行结果摘要（提交哈希+关键验证数字）。

## 六、R2 收口标准（全轮验收）

```bash
cd ~/GLMProjects/tailrace
cargo fmt --all --check && CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked          # ≥ 基线 124 passed，0 failed
grep -rn 'eprintln' engine/crates --include='*.rs' | wc -l         # = 0
cargo deny check advisories bans licenses sources                  # 四门 ok
bash scripts/bench/macro.sh --rows 100000 --report /tmp/r2-final.md # 吞吐行存在
docker compose up -d && ./scripts/smoke.sh --full && docker compose down -v   # 全链绿（OrbStack）
cd web && pnpm build >/dev/null 2>&1 && grep -c monaco dist/index.html; cd ..  # = 0
git push origin main && gh run watch <最新>                          # CI 六 job 绿
test -s docs/BENCHMARKS.md                                        # 含基线+4 组前后对比
```

## 七、R3 / R4 展望（本轮收口后按同格式立项任务卡）

**R3 亮点特性（~3-4 周）**（2026-10-20 已立项为任务卡，见 §九 T12-T21）：StarRocks sink 完整化（现仅注册别名，共享 Stream Load 协议）；JDBC 批源（MySQL/PostgreSQL，cursor_semantics 登记轮询位点；验收=新增连接器零改动 runtime/api/parser 本体）；discover / infer-schema 落地（替换 501 占位，Kafka/Doris 元数据客户端）；分区并行方案 b（split 化+动态分区发现，动工前先写部分分区推进的指纹回归测试）；feature 裁剪验证（`--no-default-features --features connector-file,connector-doris` 跑通 file→doris）；选做：SSH 部署链八步、jemalloc（RSS 指标先就位）、parser/decode fuzz 起步。**R3 末升格评审**：deltalake sink / DataFusion 升级 / 事务接口 / group commit / 本地两步 WAL 五项，默认结论=排 v1.x，纪要落 docs/dev/。

**R4 1.0 发布（~1.5-2 周）**：~~开工第一动作=crates.io 注册占位~~（**已完成 2026-10-21**：`tailrace`/`tailrace-node`/`tailrace-console` 三名各以 0.0.0 占位发布注册并经 API 核验，风险 R1 关闭；R4 发包以真实现版本号覆盖即可）。**2026-10-22 已立项为任务卡（§九 T25-T31；T22 承接自 R3 不重写）**：T25 ⑦b live-gate 整改（W0 前置小卡，T18 ⑦b 裁决落地）；T26 crate 发布元数据补齐+doc-test 起步；T27 crates.io 全树发包（拓扑序 21 包、lockstep 1.0.0、全树 dry-run 门槛、publish 不可逆独占波）；T28 README 双语定稿（徽章矩阵/FAQ/北极星措辞按 T18 §九档位口径/基准外显携跨机器不可比注记）；T29 mdBook 文档站骨架（docs 8 篇为骨架基础；dev 内部档不入站，实际入站 5 篇=architecture.zh.md/engine-v1/console-api/jdbc/BENCHMARKS+前言，roadmap 以链接入开发指针）；T30 i18n 错误码字典化接口预留（message_key wire 兼容+中文单轨明示）；T31 v1.0.0 终验（tag→release 9 资产+checksums+全新机器三命令 quickstart+基准报告收口）。波次编排见 §十 R4-W1~W4；收口标准见 §十一 R4 段。MQTT / S3 / CDC 路线已由 T18 裁决关闭（⑥不升格带 12 个月触发器、⑦a 排 v1.x，纪要存档）。T19/T21 维持「待用户点名」现状，不并入本轮。

## 八、开放问题（随轮裁决）

1. `utoipa-swagger-ui 10.0.1` 锁定的 `zip 8.6.0`（仅 build-dep，构建脚本解压 crate 自带的 vendored swagger-ui dist）：2026-10-13 后被上游 yank（现又恢复可装，deny.toml `yanked="warn"` 保留）；2026-10-17 RustSec 连发 RUSTSEC-2026-0367/0368/0369 三个 extract 漏洞（修复仅入 zip 9.0.x，`^8.6` 不可达且无 8.x backport；build-dep 威胁模型不适用——不链接进产物二进制、不解压外部归档）——deny.toml 已 ignore 三条带跟踪注释。其发新版（zip 9 线）后回收 warn+ignore 并重新收紧；**【2026-10-23 关闭（T39 ⑦）】**触发器已到：crates.io API 实查 `utoipa-swagger-ui 10.0.2`（2026-10-20，rust_version 1.88≤MSRV 1.95，feature 面与 10.0.1 全同）build-dep 为 **`zip ^9.0`**；`cargo update -p utoipa-swagger-ui` 落 10.0.1→10.0.2、zip 8.6.0→**9.0.5**（+新传递依赖 cfg_aliases 0.2.2/nix 0.31.3，无版本重复）；deny.toml 三条 RUSTSEC ignore 已删、`yanked` 回 `"deny"`，`cargo deny check advisories bans licenses sources` 四门绿。附带勘正：上文「解压 crate 自带的 vendored swagger-ui dist」措辞失真——实读 build.rs，默认（无 `vendored` feature）是构建期用系统 curl 下载 `github.com/swagger-api/swagger-ui/.../v5.32.6.zip` 到 OUT_DIR 再解压，`vendored` feature 才走随包 dist；威胁模型结论不变（zip 不入产物、归档源为固定上游 tag URL），但「不下载」的说法作废。本条关闭。
2. OTLP 的 `opentelemetry` 0.33 / `tracing-opentelemetry` 0.34 版本锚定后是否升格默认开启（当前 feature 门控默认关）；**【2026-10-23 T39 ⑧ 观察注记，不裁】** v1.0.0 已发（2026-10-23，feature 默认关随发），本条转入「按使用反馈裁决」态：裁决输入=v1.x 期间的真实部署反馈（有无用户要求零配置出 trace、默认开启对无 collector 环境的启动/导出失败噪音与 RSS 影响）；T36 谱系裁决卡不承接本条，待有反馈再立卡。本条续挂。
3. R3 升格评审五项的门槛数据（本 R2 的 BENCHMARKS.md 即门槛输入）；
4. CodeMirror6 替换 monaco 评估（编辑器体积档位，R3 裁决；T9 实测 monaco gzip 883kB vs CM6 预期 ~200-300kB 级，2026-10-17 立项）——R3 由 T17 承接。**2026-10-20 T17 裁决（PoC 分支 poc/codemirror6@7f32c26，不进主干）**：实测编辑器异步 chunk gzip **883.22→140.45 kB（−84.1%，T9 记录锚定的单口径）**，好于本条 200-300kB 预期档——CM6 零 worker/零字体/零外部 CSS（T9 的 CSS 晚到/布局塌缩补丁整类消失）。（2026-10-21 T23 对齐，评审 low#6：原「编辑器相关资产合计 1,078→140.70 kB」为 PoC 复合口径——JS+CSS gzip 与 worker/ttf 折算分项未存档，不可从 T9 仓库记录复算，弃用，统一锚 T9 已记录的 883.22 单口径。）能力面为现用面超集：`db.` 表补全 parser 驱动+FROM 别名感知、SQL 关键字补全（monaco 无）、SQLNamespace 列槽位为 T14 infer-schema 就绪（`db.table.` 列补全届时零编辑器侧代码）；delta 三项（库名大小写敏感 vs monaco 不敏感、`db.e` 后关键字混入、补全+插 `-- @tag:ds` 为单 undo 步）均有廉价修法（自写 ~30 行 CompletionSource 可全对齐）。迁移面：PoC 已完成内核替换 7 文件（SqlMonaco.vue 365→246 行、edit.vue 零改动、对外 props/emits 不变），typecheck+build 绿+浏览器实测（补全弹窗/接受插 tag/诊断波浪线/v-model 双向/主题切换/tables 热更新全过）。**结论：升级为 R4 落地卡（替换 monaco，估 ~2 人日=PoC 基线 + 视觉走查/大小写对齐决策/IME 手测/真页 e2e）；PoC 分支保留为实施基线不删不合并。**（2026-10-21 用户复核通过，落为 §九 T22 卡。）

## 九、R3 任务卡（亮点特性轮，共 10 张，2026-10-20 立项）

> 立项依据：R2 收官内部档 §五 草案 + 2026-10-20 对公仓 @0fe8f6f 的代码核实（现状字段均带代码锚点，非草案转抄）。
> 总量评估：必做 T12-T18 共 ~17 业余人日；选做 T19-T21 共 ~6 人日（R2 同期 ~23 人日口径）。
> 红线（全轮生效）：**新增连接器零改动 runtime/api/parser 本体**（架构验收，git diff 钉住）；游标 flush 点整体提交语义不动摇；SSE/emit 红线沿用 §五。

### T12 StarRocks sink 完整化（~3 人日）✅

- **现状**：`conn-doris` 的 `starrocks` feature 仅注册同名别名指向 Doris sink 工厂（`conn-doris/src/lib.rs:127-128`）；catalog normalize（`tailrace-catalog/src/lib.rs:120`）与 parser kind 白名单（`tailrace-parser/src/lib.rs:138`）已认 starrocks——即类型链路通、协议差异零核对、零 e2e。
- **方案**：Doris 通道共享为底（Stream Load 同构），核对差异矩阵（URL/端口/`read_json_by_line` 载体/label 幂等/buffer-flush 三阈值语义），StarRocks 特有默认值与差异点落在 conn-doris 内 feature 门控或 datasource 配置项，不另起 crate；docker-compose 增 `--profile starrocks`（starrocks allin1，本地档）；README 连接器矩阵补列。
- **验收**：file→starrocks e2e（本地 compose profile）+ 重启 label 幂等回归；`cargo test -p tailrace-conn-doris` 绿；差异矩阵文档化入卡内结果。回退门：allin1 镜像本机不可拉时降级 mock http 契约测试 + 矩阵文档，e2e 缺口如实登记留待有环境补。
- **文件域**：engine/crates/tailrace-conn-doris、docker-compose.yml、README.md / README.zh-CN.md。
> 结果：1cd38d0 T12 提交完成：1cd38d0（main，父 d9f51bd）feat: T12 StarRocks sink 完整化——差异矩阵+compose profile+e2e。git add 严格限 5 个文件域路径（conn-doris Cargo.toml+lib.rs、docker-compose.yml、双 README），+491/−115，Cargo.lock 零变化；提交说明完整承载差异矩阵（官方文档+源码+allin1 实测三方交叉五项）、SinkFlavor 实现、compose feproxy 架构注记、e2e 实跑证据与验证数字（默认 5/5、no-default-features 3/3、CI 双守卫 exit 0、compose config 过）。e2e 为实跑未回退（allin1 swap 墙未触发：consumed=10/flushed=10、重启表不增、同 label 重放 Dedup 不双写、down 拆净）。提交后工作树干净、未 push（ahead 2，留波门禁）。

### T13 JDBC 批源（~4 人日）✅

- **现状**：`tailrace-conn-jdbc` 为 1 行占位 crate（`//! Placeholder ... real engine code lands in the migration stage (P0)`）；node feature `connector-jdbc` 已接线但注册零（`engine/bin/tailrace-node/Cargo.toml:17`）；conn-s3/conn-mqtt 同为占位不在本卡。
- **方案**：占位 crate 内实现 Source trait：MySQL/PostgreSQL 两方言，按主键/时间列区间增量轮询，`cursor_semantics` 登记轮询位点（对齐 IR 游标语义）；驱动选型须过 cargo deny licenses（倾向 sqlx 或各官方 async 驱动，deny 报告先行）；注册走 node feature 门控，不动 registry 本体。
- **验收**：testcontainers 起 mysql:8 + postgres:15 批源测试（chaos job 先例复用）；位点断言——重启续读不丢不重、totals 单调；parser/catalog 认 jdbc kind；**`git diff` 零触碰 runtime/api 本体 + parser 仅 kind 白名单两行**（红线验收字面化；R3-W1 评审 low#3 修正——原措辞"零触碰 runtime/api/parser 本体"与文件域"parser kind 枚举"不是同一集合，证据须明示 parser 触碰行而非换一组路径宣称零触碰）。
- **文件域**：engine/crates/tailrace-conn-jdbc、engine/bin/tailrace-node（注册行）、tailrace-parser/tailrace-catalog kind 枚举、tests/。
> 结果：caa842e T13 已提交（`feat: T13 JDBC 批源——MySQL/PostgreSQL 增量轮询 Source（testcontainers 双方言测试）`）：严格限 8 个文件域路径（Cargo.toml/Cargo.lock/conn-jdbc crate+tests/node 注册行/parser 白名单/catalog normalize），1520 增 / 8 删；提交说明含驱动选型结论（sqlx 0.9 子 crate——门面 sqlx-sqlite 与 rusqlite `links` 冲突改走子 crate，无 tls）与验证数字（conn-jdbc 7/0、workspace 156/0、clippy 0、deny 四门 ok）；提交后工作树干净，**runtime/api 本体零触碰；parser 触碰=`tailrace-parser/src/lib.rs:124` kind 白名单两行（域内允许，如实点名——评审 low#3 修正，原证词"runtime/api/console 零触碰"以 console 顶替 parser 属换组宣称）**，未 push（ahead 1，留波门禁）。

### T14 discover / infer-schema 落地（~3 人日）✅

- **现状**：console `api/datasources.rs:680/702` 两 handler 返回 `not_implemented`（HTTP 501，"需要 Kafka/Doris 元数据客户端，尚未交付"）；openapi 已登记路径；web 侧对应入口空挂。
- **方案**：engine 侧元数据客户端（Kafka list/describe topics；Doris SHOW TABLES/DESC 走 9030 mysql 协议）落独立模块；console 转发并缓存；infer-schema 由样本行或 DDL 推 Arrow schema（复用 tailrace-decode 推断面）；web 数据源页接线（发现/建表辅助动线）。**顺带承接 R3-W1 评审 low#4**：console 静态类型注册表（`datasources.rs` `list_types`）补 jdbc 家族（mysql/postgresql），console-store `cursor_semantics` 派生补 `"jdbc"→"JDBC_KEY"`（对齐 IR `Cursor::JdbcKey`），一并加测试断言。
- **验收**：501 → 真实现；testcontainers/mock 集成测试（kafka 返回 topic 列表、doris 返回表列表、infer-schema 输出 schema JSON 契约快照）；web 手测动线（typecheck+build 绿）；datasource-types 含 jdbc 家族且 cursor_semantics 断言绿。
- **文件域**：console/crates/tailrace-console/src/api/datasources.rs、console/crates/console-store/src/datasources.rs、engine 元数据客户端新模块、web/src/pages/datasources/。
> 结果：1c5dcf6 T14 已提交：1c5dcf6（main，父提交 1cd38d0 线性，未推送——push 留波门禁）。git add 严格限 8 条域路径（根 Cargo.toml+Cargo.lock、console-store/src/datasources.rs、tailrace-console 的 Cargo.toml/src/api/datasources.rs/tests/datasources_crud.rs、web/src/pages/datasources/parts/DiscoverModal.vue、engine/crates/tailrace-metadata/ 六文件），Conventional 提交：feat: T14 discover/infer-schema 落地——Kafka/Doris 元数据客户端+jdbc 类型登记+web 动线；正文记录实现面（kafka rdkafka fetch_metadata+OffsetTail 采样、Doris/MySQL 9030 文本协议 SHOW/DESC、PG information_schema、decode 链同款类型格推断、jdbc→JDBC_KEY 派生、web format 回传）、验证数字（-p 两包全绿、workspace 172/0、clippy -D warnings 零告警、deny 四门 ok、pnpm typecheck+build 绿、501 grep=0、live 3/3 实跑）与遗留项（Doris 直连未实测/openapi 组件与 protocol 文档待同步/jdbc 管道端到端留后续）。提交后工作树干净（status 0 行）。

### T15 分区并行·方案 b（~4 人日）〔引擎热路径；本卡动游标边界，R3 最险〕✅

- **现状**：方案 a 后单 consumer 拉取边界仍在（T5 记录：rdkafka 单 consumer poll 串行，decode 已并行）；无 split 化、无动态分区发现；T5 的 5 个语义回归测试是现成防线。
- **方案**：**动工前先写部分分区推进的指纹回归测试**（先测后改）；split 化=分区子集独立拉取执行单元（评估游标拆分语义）；动态分区发现=新增分区自动纳入 poll 面（不重启管道）。
- **验收**：指纹回归+重启不丢不重+NO_PROGRESS 语义全绿后收工；多分区宏基准 vs 方案 a（24658 rows/s 基线，BENCHMARKS §4.5）对比入档；并行等价性字节级一致测试沿用 T5 口径。
- **回退门（写死）**：split 化若无法等价证明"游标按 flush 点整体提交"语义，降级为仅交付动态分区发现，split 化并入 T18 升格评审议程——不硬上。
- **文件域**：engine/crates/tailrace-conn-kafka、tailrace-runtime 主循环、tailrace-state。
> 结果：288c3cf T15 提交完成：288c3cf（main，父提交 33cf61e 线性，未推送——push 留波门禁）。git add 严格限 3 条域路径（docs/BENCHMARKS.md、tailrace-conn-kafka/src/lib.rs〔唯一 hunk 全在 #[cfg(test)] 模块内，生产代码零改动〕、新增 tailrace-runtime/tests/t15_partition_discovery.rs），Conventional 提交：feat: T15 分区并行方案 b——动态分区发现+指纹回归先行（split 化回退门兑现），正文记录现状修正（发现机制自 v0.1.0 已在）、三层测试（运行中长分区不重启吸纳/毒汇下 NO_PROGRESS 判据不被发现扰动/真 kafka 双模式不重开即消费）、发现延迟口径（30s 差异对比+fetch≤5s→最晚 ~35s，实测 26.8-31.2s 三跑稳定）、**split 化未实施、按回退门降级**（字节级等价口径不可诚实构造→并入 T18 议程，回退门如约兑现不硬上）、宏基准 M4 首档中位 27861 rows/s（跨机器不可比已登记）。提交后工作树干净（status 0 行）。
> t15-scope 门禁修复过程存档：两轮失败均因超出任务卡文件域（第一轮 Cargo.lock/Cargo.toml 的 tokio dev-dep——重写为容器内 kafka-topics.sh exec 驱动零依赖版测试；第二轮 roadmap.md/registry.rs——R3 任务提交 git 证据显示二者从不由任务提交触碰，指纹覆盖改为黑盒毒汇测试后两文件 checkout 还原）；终版 3 文件域门禁全绿。

### T16 feature 裁剪验证（~1 人日）✅

- **现状**：node features default=kafka/file/doris（`engine/bin/tailrace-node/Cargo.toml:12`）；CI `no-default-features` job 只验编译，未验链路；裁剪档二进制体积无数字。
- **方案**：`--no-default-features --features connector-file,connector-doris` 构建 tailrace-node，跑通 file→doris 端到端（依赖 L3 compose Doris）；体积对比（default vs 裁剪档）入 BENCHMARKS/README——单二进制轻量卖点需要数字支撑。
- **验收**：裁剪档 smoke 变体（file→doris 最小面）绿；体积对比表入档；CI 增可选 job 或本地档登记。
- **文件域**：engine/bin/tailrace-node、scripts/smoke.sh（裁剪档变体）、.github/workflows/ci.yml。
> 结果：33cf61e T16 已按门禁指令提交为 33cf61e（main，父 4881c39 线性，非 amend）：staging 严格限文件域 5 文件（.github/workflows/ci.yml、Cargo.toml、engine/bin/tailrace-node/Cargo.toml、scripts/smoke.sh、docs/BENCHMARKS.md，+296/−47）；主题与指令逐字一致，body 分段记录装饰性裁剪根因与最小 wiring、体积/图/导入表数字（裁剪档 55,788,080 B vs 默认档 61,571,936 B，−9.4%）、**本机 e2e 实跑（非降级）30/30**（`smoke.sh --slim` file→doris 裁剪档，BENCHMARKS §六 T16 行 11.9 s 行数守恒 12==12）与负向探针 ENGINE_REJECTED、2.1.7 配方降级缘由、CI slim-features 硬门禁、L0/L1 与环境复原自检结果。工作树提交后干净，未 push（留波门禁）。〔文件域勘正：根 Cargo.toml 与 docs/BENCHMARKS.md 为实施所需落点——workspace 表 `tailrace-runtime` 加 `default-features = false` 是唯一不触碰 tailrace-api manifest 的修法（BENCHMARKS §6.2），体积表入档为验收项。〕

### T17 CodeMirror6 评估（~1 人日）〔裁决卡，PoC 不进主干〕✅

- **现状**：T9 实测 monaco 裁剪极限 gzip 883.22kB（raw 3.46MB，含 SQL 补全可用面）；§八.4 已挂号。
- **方案**：CM6 PoC（@codemirror/lang-sql + autocomplete 面板）量三件事：gzip chunk 体积、补全功能面（表/列补全数据源 API 可用性）、迁移工作量估计（SqlMonaco.vue 替换面）。
- **验收**：对比表（体积/能力/工作量）产出，裁决结论写回 §八.4 并关闭或升级为落地卡——本卡不落地编辑器替换。
- **文件域**：PoC 分支（不进 main）；结论落本文件 §八。
> 结果：7f32c26（poc/codemirror6，保留不合并）编辑器异步 chunk gzip **883.22→140.45 kB（−84.1%，T9 记录锚定的单口径）**，CM6 侧零 worker/字体/外部 CSS；能力为现用面超集（+SQL 关键字补全、FROM 别名感知，SQLNamespace 列槽位就绪 T14）；迁移=PoC 已换核（edit.vue 零改动），转产工作量 **~2 人日（含 PoC 基线，与 §八.4/T22 统一口径）**——裁决=升级 R4 落地卡（详见 §八.4）。〔2026-10-21 T23 对齐（评审 low#6）：原「转产余量 ~1-1.5 人日」与 §八.4「~2 人日」两口径并存，统一为后者（含 PoC 基线）；原「编辑器相关资产合计 1,078→140.70 kB」为 PoC 复合口径（JS+CSS gzip+worker/ttf 折算），分项未存档、不可从 T9 仓库记录复算，弃用，统一锚 T9 已记录的 883.22 编辑器异步 chunk gzip。〕

### T18 R3 末升格评审（~1 人日）〔门槛数据=BENCHMARKS.md〕✅

- **方案**：**七项评审**——deltalake sink / DataFusion 升级 / StarRocks 事务接口 / group commit / 本地两步 WAL 五项（默认结论=排 v1.x），另加：**MQTT 源路线**（设计 §7.4 P2 序 #3、称差异化最明显，但未进任何轮次——升格 R4/v1.x 或明确砍掉）与 **S3/CDC 路线**（P2 序 #4）；**jdbc live-gate 形态裁决**（`return Ok` 计 passed vs `#[ignore]`+`--ignored`，与 conn-kafka 同裁，R3-W1 评审附议）。逐项给门槛数据（R2+R3 的 BENCHMARKS 为输入）、触发条件与结论；纪要落 `docs/dev/R3-升格评审-<date>.md`。
- **验收**：纪要存在且七项各有结论+依据；若有升格项，转 R4 任务卡或 v1.x 议程。
- **文件域**：docs/dev/。
> 结果：5b44c76 T18 纪要已提交为 5b44c76（main，父 288c3cf 线性，主题逐字=docs: T18 R3 升格评审纪要——七项结论与门槛数据）。git add 严格限 docs/dev/，单文件 docs/dev/R3-升格评审-2026-10-21.md +134 行；提交 body 承载七项结论一览（① deltalake 排 v1.x 硬耦合 DF 升级；② DF 48→55 排 v1.x+RUSTSEC 硬触发器；③ StarRocks 2PC 排 v1.x 与⑤合并谱系；④ group commit 排 v1.x 先补 Doris 宏基准；⑤ 两步 WAL 排 v1.x；⑥ MQTT 不升格 R4 带 12 个月触发器；⑦a S3/CDC 排 v1.x；⑦b live-gate 裁决 #[ignore]+--include-ignored 唯一立即整改）、门槛数据锚点与门禁记录（t18-accept PASS / t18-scope 修复后 PASS）。提交后工作树干净（porcelain 空），未 push（ahead 4，留波门禁）。

### T19 SSH 部署链八步（~3 人日）〔选做〕

- **现状**：console `api/nodes_admin.rs:749` 起为设计文档 §4.4 八步链的 501 标记（/api/nodes/deploy 等）。
- **方案**：八步落地（上传/解压/停旧/替二进制/启新/健康/回滚/版本核对——以设计文档 §4.4 为准）；console 编排 + SSH 客户端；失败回滚路径是验收重心。
- **验收**：501 → 真实现；本地 docker 目标机集成测试（含中途失败触发回滚）；smoke 不强制纳入。
- **文件域**：console api/nodes_admin.rs、web 部署页（如有）、tests/。

### T20 jemalloc + RSS 指标（~1 人日）〔选做〕✅

- **现状**：T7 已有 DataFusion memory pool+RSS 探针口径（峰值 RSS 49124 KiB 入档）；未做 allocator 切换对比。
- **方案**：tikv-jemallocator feature 门控默认关；RSS 指标先就位（stats 面）；A/B 宏基准（吞吐+峰值 RSS+碎片）入 BENCHMARKS。
- **验收**：feature 档构建绿+对比表入档；默认档行为零变化（不引入 jemalloc）。
- **文件域**：engine/bin/tailrace-node、Cargo.toml（feature）、scripts/bench/、engine/crates/tailrace-api/src/heartbeat.rs（stats 面实际所在，首跑实证勘正）。
> 结果：9a9f8a2 Committed T20 as 9a9f8a2 on main (parent caa842e, linear, not pushed): exactly the 7 file-domain files (Cargo.toml, Cargo.lock, engine/bin/tailrace-node/{Cargo.toml,src/main.rs}, engine/crates/tailrace-api/src/heartbeat.rs, scripts/bench/macro.sh, docs/BENCHMARKS.md; +168/−4) with the prescribed subject `feat: T20 jemalloc 门控+RSS 指标——A/B 宏基准对比入 BENCHMARKS` and a body recording the VmHWM/file-domain tradeoff and the A/B numbers (throughput 33670 vs 33738 rows/s +0.2%; kernel peak RSS 53616 vs 57808 KiB +7.8% jemalloc higher; feature stays default OFF); working tree clean, branch ahead 2 of origin/main.

### T21 parser/decode fuzz 起步（~2 人日）〔选做〕

- **方案**：cargo-fuzz 目标两个——parser SQL 文法、decode JSON/CSV/OGG 毒输入；种子=T4/T11 毒数据语料；CI 定时档或本地手动档（不上常规 job）。
- **验收**：fuzz 目标 `cargo fuzz build` 绿；首轮语料 30 分钟零 panic（发现的 panic 修复或登记）；deny licenses 无新增问题。
- **文件域**：fuzz/（独立 workspace 新目录）、.github/workflows/（可选定时 job）。

### T22 CodeMirror6 转产落地（~2 人日）✅〔R4 承接；2026-10-21 T17 裁决落章〕

- **现状**：PoC 基线 `poc/codemirror6@7f32c26` 已完成内核替换（SqlMonaco.vue 365→246 行、edit.vue 零改动、对外 props/emits 不变），编辑器异步 chunk gzip 883.22→140.45 kB；全部依据见 §八.4。
- **方案**：以 PoC 分支为实施基线落到 main；三项 delta 对齐（~30 行自写 CompletionSource：库名大小写不敏感、`db.e` 后关键字混入过滤、补全+插 `-- @tag:ds` 拆为两步 undo）；SQLNamespace 列槽位与 T14 infer-schema 接线；视觉走查 + IME 中文输入手测 + 真页 e2e。
- **验收**：CI web job 绿；编辑器相关 chunk gzip ≤300kB；真页 e2e（编辑/补全/撤销/主题切换）；monaco 依赖从 package.json 移除；T9 的 CSS 晚到/塌缩补丁整类移除并在 BENCHMARKS/CHANGELOG 登记。
- **文件域**：web/。
> 结果：f90e6a2 T22 提交完成：f90e6a2（main，父 7d4c7c6 线性，未 push——留波门禁）。git add 严格限 web/ 五文件（package.json/pnpm-lock.yaml/SqlMonaco.vue/editor-worker.ts/vite.config.ts，+401/−311，剔除 PoC harness 后的落地面），Conventional 主题与门禁指令逐字一致，body 分五段承载：①落地方式（PoC 7f32c26 cherry-pick -n 等效 rebase 落 main、harness 两文件剔除、edit.vue 零改动契约保真）；②三 delta 对齐实现（memberCompletion 大小写不敏感泛化深度遍历/keyword 点链门控/applyTable 双 dispatch 两步 undo）；③gzip 实测数字（138,962 B ≤300kB 门槛 + T9 883.22kB 单口径对照）；④monaco 全链移除+T9 CSS 晚到/塌缩补丁整类移除实证+真页 e2e 七项与 IME 记录；⑤T14 接线核对（console TableObjectItem DTO 不回 columns_json 为列补全最后一跳，编辑器侧双槽位就绪）。提交后工作树干净。（gzip 数字与 e2e 摘要保留）

### T23 R3-W1 评审 low 债清偿 + 文档对齐（~1 人日）〔R3-W2 前置波 W0，2026-10-21 立项〕✅

- **现状**：R3-W1 评审 low 六条中五条可修未修（low#4 已归 T14、low#7 留 T18）；另评估发现工程面漂移——README 连接器矩阵自相矛盾（matrix 全 ✅ 标 P2 vs Status 段自认 placeholder，jdbc 已落地未更新、starrocks 仅别名）、quickstart "smoke 25 assertions" 数字漂移（实际 --full 44 步）、CHANGELOG Unreleased 未入账 R3-W1、`docs/connectors/` 空目录（设计 §7.3 第 7 步从未执行）。
- **方案**：① low#1：FLOAT4 游标臂补 is_cursor（镜像 FLOAT8）或 `pg_cursor_kind` 拒绝 FLOAT4——从优并加测试；② low#2：JDBC（源）/JDBC_TABLE（Doris 汇）诊断文案与 catalog 注释、parser 诊断点明；③ low#3：本文件 T13 卡验收措辞修正为"零触碰 runtime/api 本体 + parser 仅 kind 白名单两行"；④ low#5：BENCHMARKS §六 表行加脚注（debug 档/短跑/±17% 量化）+补"碎片：未测"行+基准二进制精确树标注；⑤ low#6：本文件统一 T17 工作量口径（~2 人日含 PoC 基线），1,078kB 补分项或改 T9 883.22 单口径；⑥ README 双语矩阵勘正（jdbc source ✅、starrocks 标"别名共享 Stream Load，完整化=T12"、mqtt/s3 标占位、smoke 步数改 44）；⑦ CHANGELOG Unreleased 入账 T13/T17/T20+评审修复；⑧ `docs/connectors/jdbc.md` 首篇（补设计 §7.3 第 7 步欠账）。
- **验收**：`cargo test -p tailrace-conn-jdbc --locked` 绿（含 FLOAT4 新测试）；L0 绿；`grep -c 'P2' README.md` 矩阵段为 0、CHANGELOG 含 T13/T20 条目、`test -s docs/connectors/jdbc.md`。
- **文件域**：engine/crates/tailrace-conn-jdbc、engine/crates/tailrace-catalog（注释）、engine/crates/tailrace-parser/src/lib.rs、Cargo.lock、docs/dev/roadmap.md、docs/BENCHMARKS.md、docs/connectors/、README.md、README.zh-CN.md、CHANGELOG.md。（2026-10-21 落档勘正补入 parser 与 Cargo.lock：W2 首跑实证 parser 诊断是方案②交付；RUSTSEC-2026-0382 令 cc46490 CI 红，Cargo.lock 升级 rustls-webpki 0.103.15→0.103.16 是唯一合法修复落点。）
> 结果：d9f51bd T23 已按波门禁指令提交为 d9f51bd（main，父 cc46490 线性，10 文件 +338/−34，含新文件 docs/connectors/jdbc.md）。staging 严格限文件域：CHANGELOG.md、Cargo.lock（授权的 rustls-webpki 0.103.16 单包门禁修复）、README.md、README.zh-CN.md、docs/BENCHMARKS.md、docs/dev/roadmap.md、tailrace-catalog/tailrace-conn-jdbc/tailrace-parser 三 crate 源、docs/connectors/jdbc.md——git status 实测无外来变更混入。提交主题与指令逐字一致，body 分段记录八项交付（low#1 FLOAT4 加宽 canonical/low#2 JDBC·JDBC_TABLE 诊断/low#3 T13 措辞/low#5 BENCHMARKS 三脚注/low#6 T17 口径统一）+门禁修复授权与验证数字（conn-jdbc 6+2+0、parser 7/catalog 4、L0 三件全绿，均上一轮实测）。未 push（留波门禁），工作树干净。

### T24 R3-W2 评审尾债与文档债清偿（~1 人日）〔R3-W3 前置波 W0，2026-10-21 立项〕✅

- **现状**：R3-W2 评审 low 两条（T12 allin1 e2e 无归档脚本可复跑——live gate 静默 skip 空过；`docs/connectors/jdbc.md` §七"console 尚无 mysql/postgresql 类型"被同日 T14 过时化）+ T14 自登记文档债两笔（openapi components 缺 discover/infer-schema 新 DTO 致 swagger $ref 悬空；`docs/protocol/console-api.md` 仍留 ⛔ 501 行）。
- **方案**：① `scripts/e2e/starrocks.sh`：把 T12 手工 e2e 固化为可重跑脚本（`TAILRACE_SR_*` 覆盖变量、`--wait` 语义、DDL/数据/断言/清理全内建；live 栈不可达时打印一行 skip 说明再空过——对齐 `tests/jdbc_sources.rs:110` 惯例）；② `docs/connectors/jdbc.md` §七改为 T14 后现状（mysql/postgresql 已登记、JDBC_KEY 派生、discover/infer-schema 通道可用），剩余缺口点名 T14 登记遗留；③ console `openapi.rs` components 补新 DTO；④ `docs/protocol/console-api.md` 同步 discover/infer-schema 真实现。
- **验收**：`bash scripts/e2e/starrocks.sh --help` 出用法（栈不可达时 skip 行可见）；`cargo test -p tailrace-console --locked` 绿；`grep -c '501' docs/protocol/console-api.md` 对应段落归零；jdbc.md 无"尚未登记"表述。
- **文件域**：scripts/e2e/、docs/connectors/jdbc.md、docs/protocol/console-api.md、console/crates/tailrace-console/src/openapi.rs。
> 结果：4881c39 T24 按门禁指令提交为 4881c39（main，父 3b71f94 线性），staging 严格限 4 条文件域路径（scripts/e2e/starrocks.sh 新建可执行、docs/connectors/jdbc.md、docs/protocol/console-api.md、console/crates/tailrace-console/src/openapi.rs），+370/−9。提交主题与指令逐字一致，body 完整承载四项交付（① e2e 归档脚本含 TAILRACE_SR_* 覆盖/--wait/GRANT INSERT,SELECT ON ALL TABLES/三档断言/[skip] live-gate/T12 证据链；② jdbc.md §七 T14 后现状与剩余缺口；③ openapi 四 DTO+$ref 全文档走查测试；④ console-api.md discover/infer-schema 真实现契约）与验证数字。工作树干净，未 push。


### 以下 R5 任务卡（2026-10-23 立项，四裁决点按草案推荐案：T33 纳入/T35 纳入/T37 挂起/T38 随轮发；锚点移植时抽查核稳）

### T32 Doris/StarRocks sink 宏基准（~2 人日）〔T18 ④T1；无触发争议〕 ✅

- **现状**（今日锚点）：`scripts/bench/macro.sh` 仅 file→file 单管道（:2 自述注释；:110/:147 datasource/pipeline 经 console API 建法；:188 吞吐口径行）；`docs/BENCHMARKS.md` 全部 Doris 提及为 e2e 行数守恒（:330-331）、容器内存峰值（:332）、体积/裁剪（:358/:361）——**无任何 Doris sink 吞吐行**（T18 共同事实 4 复核一致）。
- **方案**：① `macro.sh --sink doris`：L3 compose 起 Doris（环境配方=`AGENTS-环境契约与工作纪律.md` §3；磁盘 <5G 红线先查）→ 建 doris datasource（stream_load_url 完整性由 T11 回归断言口径钉住）+ 目标表 DDL → file→doris 单管道吞吐（N/(t1−t0)，对齐 :188 口径，走 label 幂等路径）；② **多管道并发档**：P=4 管道 × 小批（flushRows≈500）——group commit 收益区探针（T18 ④ 判据 T3 的数据面）；③ StarRocks 可选档：allin1 profile（T12 先例）可达则跑，不可达打印 skip 行降级登记（对齐 `scripts/e2e/starrocks.sh` live-gate 惯例）；④ 数据入 BENCHMARKS §六新小节，跨机器不可比注记随行。
- **验收**：`bash scripts/bench/macro.sh --sink doris --rows 100000 --report /tmp/r5-doris.md` 出吞吐行；并发档 ≥1 组吞吐/耗时行；BENCHMARKS 新小节含口径注记；栈 teardown 无残留（`docker ps` 空）。
- **回退门**：Doris 栈本机不可起（磁盘/健康检查）→ 如实登记跳过不造数据，卡降级为「file→file 对照+Doris e2e 复用数字」并在 T36 纪要注明裁决输入缺口；并发档受 console API 并发建管道限制 → 降 P=2 并登记。
- **文件域**：scripts/bench/macro.sh、docs/BENCHMARKS.md、（仅注记级）docker-compose.yml。
> 结果：cb825f6 六档数字全落（M4 同机同日/debug/100k/行数守恒全过）：file→file 锚点 33568；doris 单管大批 33852（+0.8% 持平=大批 sink 零开销）；单管小批 500 行 11017（200 次同步往返封顶 -67%）；P=4 真小批 26781（2.43x 单管小批、-21% vs 大批——**T18 ④T3 数据面：并发有相对收益无绝对追平，缺口即 group commit 目标区**）；P=4 poll 地板批 97752（瓶颈在单管道非 sink）；starrocks 8750（口径不同源量级参考）。macro.sh 205→583 行（--sink doris|starrocks + --pipes/--flush-rows/--poll-rows）；BENCHMARKS §6.4+总表五行+脚注6；环境复原全过（compose 零残留/mdop doris 回滚/swap 回挂）；开发中实证修 3 个脚本缺陷（deploy 漏 token/bash3.2 set-u/孤儿 node）。T36 谱系裁决门槛输入（④T3）齐备。
- 产出 = T36 谱系裁决的门槛输入（③④共同）。

### T33 DataFusion 48→55.2.0 升级（~4-6 人日）〔引擎串行独占波；裁决点1〕

- **现状**（今日锚点）：根 Cargo.toml:63-64 `datafusion = "48.0.1"` / `datafusion-proto = "48.0.1"`；engine 侧 DF 触面 **12 文件**（今日 grep 实测清单：transform/runtime/ir{lib,placeholder}/connector{sink.rs,registry.rs}/catalog/parser/decode/conn-doris/conn-file/conn-kafka 各 src/lib.rs）；契约 golden 两测（tailrace-ir/tests/golden_layer_a.json、golden_layer_b.txt，字节稳定断言）；DF48 两条已登记限制（BENCHMARKS §4.4 memory pool 部分算子不强制 :193-196、output_batches 指标面窄 :184-187）——升级后须复核是否仍成立。**MSRV 前提已核实（§一新事实）：55.2.0 rust_version=1.94.0 ≤ 1.95，ADR-001 兼容。**
- **方案**：① 直跳 55.2.0（不停中间版本），datafusion-proto 同步；② **动工前先列 DF 49-55 breaking-changes 核实清单**（DataFrame/SessionContext API、arrow 55→59 数组/位图/ffi 面、sqlparser 升级）落卡内再动代码；③ 契约 golden 重生成**显式登记归因表**（每条字节 diff 归因：proto 版本字段/默认值漂移 vs 语义漂移；**禁止手编 golden 凑绿**）；④ L4 全量前后对比**同机同日**（微基准 identity/filter/groupby vs §4.4 基线 30.6M/9.0M/8.1M；宏基准 file→file vs §六基线带）；⑤ DF48 两条限制在 55 上的复核结论入 BENCHMARKS；⑥ deny 四门（arrow 系新版本 advisories/licenses）。
- **验收**：L0+L1 workspace 全绿（ignored 计数口径沿用 T25 后账本）；golden 归因表存在且逐条有结论；BENCHMARKS 新「DF 升级前后」小节（微基准三项+宏基准+注记）；`smoke.sh --full` 绿（L3）；裁剪档 `--no-default-features --features connector-file,connector-doris` check 绿。
- **回退门（写死）**：宏基准回吐 >10% 且无法归因修复 → 版本回 48.0.1、回退报告留档、②续挂（触发器重等 RUSTSEC）；deny 出现不可豁免 license 冲突 → 同回退。
- **文件域**：根 Cargo.toml、Cargo.lock、engine 12 触面文件、tests/contract（golden 重生成）、docs/BENCHMARKS.md。
- **立项说明**：T18 ② 的触发器 T-b 原文=「①delatlake 升格→②为其前置波」；本草案以「①为 R6 意向」预支该前置（反向驱动），另两条理由：MSRV 已核实无阻塞；v1.0 刚发、下游零依赖历史包袱，是 7 个大版本跨度的最廉窗口。裁决点1 若否决 → W1 波移除，①解锁路径续挂。

### T34 i18n 承诺债清偿（~1.5 人日）〔T30 登记 v1.x 项兑现〕 ✅

- **现状**（今日锚点）：engine `tailrace-api` 11 处 ad-hoc `"error"` JSON（今日 grep=11，与 T30 登记一致）；console 中文 `format!` 直写 44 处（今日 grep=44，T30 登记 43，漂移 +1）；`message_key` wire 通道与 6 规范码常量单点已由 T30 交付（console-auth/error.rs + api/mod.rs）。
- **方案**：① engine api 11 处错误响应补 `code` 字段（additive wire 变更）——动工前核实 console 侧 node client 的反序列化容错（Value 直取/非 deny_unknown_fields 则兼容），加 wire 兼容断言测试钉住；② console 44 处文案收敛 message_key 查表（zh 单轨字典模块，en 留空轨——T30 通道兑现；audit 日志类文案可豁免，**豁免边界写死在卡内**）；③ 字典完整性测试（扫描无漏网直写）。
- **验收**：engine api 11 处全带 code 且 wire 兼容断言绿；console 直写中文降至豁免边界内并登记豁免清单；`cargo test -p tailrace-api -p tailrace-console --locked` 绿；L0 绿。
- **文件域**：engine/crates/tailrace-api、console/crates/tailrace-console/src/api/*、console 新字典模块、（如牵连）console node client 反序列化点。
> 结果：3cf738b+ffb1d55 engine api 10 处错误响应补 code（8 规范码常量；11 处口径中 1 处系数据字段如实区分）+wire 兼容双锁（4 端点真起服断言 legacy 形态+console-proxy Value 容错钉住）+信封键集机器门（评审修升格）；console 44 处中文 format! 收敛 message_key 查表（zh 单轨字典 **36 key/50 调用点**，渲染字节一致）+38 处 §4.0 信封携 message_key 上 wire+完整性测试钉住 format! 类；字面量类 ApiError 残留 **34 站点**机器钉住登记（评审稿 18 为行扫口径、ffb1d55 多行扫实测 34——datasources 11/artifacts 10/pipelines 7/nodes_admin 3/auth 2/nodes 1；评审修：豁免②如实列分布+扫描扩展）；review low 三笔（37/51 计数勘正、负测试树内固化、BENCHMARKS send_once 措辞）随评审修或登记。〔**2026-10-23 T39 ③ 勘正落定**：3cf738b 提交说明的「37 key/51 调用点」为双 off-by-one 误记（实测：`template` 匹配臂/KEY_LIST 均 36 项；`grep -rn 'messages::' src/ --include='*.rs'` 排除本模块 =50），已落为 messages.rs 模块文档「规模口径」段 + `templates_use_only_positional_placeholders` 内 `KEY_LIST.len()==36` 机器钉 + CHANGELOG Unreleased T34 条目（36 keys / 50 call sites）；不可改写的提交说明为唯一残留，T38 入账直接引用本行即可。〕

### T35 SSH 部署链八步（~3 人日）〔T19 承接；裁决点2〕

- **现状**（今日锚点）：`console/crates/tailrace-console/src/api/nodes_admin.rs:749` 起 §4.4 八步 501 标记区（:757 utoipa 501 responses 登记、:760 `not_implemented`）；设计 §4.4 八步链（上传/解压/停旧/替二进制/启新/健康/回滚/版本核对）定案未实现；web 侧入口空挂。
- **方案**：① SSH 客户端选型先过 `cargo deny check licenses`（倾向 russh 纯 Rust；openssh 包装需系统依赖不取）；② console 编排八步状态机，每步审计事件入台账（复用 audit 面）；③ **失败回滚路径=验收重心**（任一步失败→回滚旧二进制→健康确认）；④ 本地 docker 目标机集成测试（容器当远端节点、tailrace-node 真进程、含中途失败触发回滚用例）；⑤ web 部署动线接线；⑥ `docs/protocol/console-api.md` 路径契约同步（501 行清除）。
- **验收**：501 → 真实现；docker 目标机 e2e 含回滚用例绿；`cargo test -p tailrace-console --locked` 绿；web typecheck+build 绿；`grep -c '501' docs/protocol/console-api.md` 对应段落归零。
- **文件域**：console/crates/tailrace-console/src/api/nodes_admin.rs、web/src/pages/nodes/、console tests/、根 Cargo.toml+Cargo.lock（新依赖）、docs/protocol/console-api.md。
- **依赖**：排 T33 后串行（同动根 Cargo.toml/Cargo.lock；R3-W2「T23 先行」同因先例）。

### T36 R5 末谱系裁决卡（~1 人日）〔T18 ③④⑤ 合并裁决的正式召开〕

- **方案**：输入 = T32 Doris 基线（单管道+并发档）+ T33 后链路基线；按 T18 预设判据逐项检验——④ T2（瓶颈在 FE 事务/BE compaction 队列而非网络/CPU）/T3（≥4 管道×<1000 行批收益）；③⑤ 二选一（2PC 服务端裁决 vs WAL 客户端裁决，不同时做）；产出纪要 `docs/dev/R5-谱系裁决-<date>.md`（T18 模式复用：结论+依据+续挂触发器）；升格项转 R6 任务卡草案。
- **验收**：纪要存在且 ③④⑤ 各有结论；升格项有卡草案要点，未触发项触发器续挂明示。
- **文件域**：docs/dev/。

### T37（选做）parser/decode fuzz 起步（~2 人日）〔T21 承接；裁决点3，推荐挂起〕

按 T21 卡原案：fuzz/ 独立 workspace 两目标（parser SQL 文法、decode JSON/CSV/OGG 毒输入）、种子=T4/T11 毒数据语料、首轮语料 30 分钟零 panic、deny licenses 无新增问题。R5-W2 若提前收口可作加餐，不占关键路径。

### T38 v1.1.0 收口（~1.5 人日）〔裁决点4；推荐随轮发〕

- **方案**：CHANGELOG `[1.1.0]` 入账（R5 各卡——**R5 执行纪律：各卡不动 CHANGELOG，统一本卡入账**，避免并行波冲突）→ lockstep bump 1.1.0（T27 配方：根 Cargo.toml 单文件+Cargo.lock 再生）→ `scripts/release-crates.sh --publish` 发 21 包（拓扑序/dry-run 门槛/断点续发已固化；1.0.0 已发包不可重发，bump 前确认 main 已含全部 R5 卡）→ `git tag v1.1.0` → release.yml 9 资产 → 收官报告 `docs/dev/v1.1-release-<date>.md`。
- **验收**：crates.io API 21×1.1.0；`cargo install tailrace-node --version 1.1.0 --locked` 实装；`gh release view v1.1.0` 资产 9/9；收官报告存在。
- **文件域**：根 Cargo.toml、Cargo.lock、CHANGELOG.md、docs/dev/。

### R3-W1 评审登记（2026-10-21）

> **medium 三条已修（各一提交）**：① `tailrace-runtime/src/registry.rs` `Cursor::JdbcKey` 指纹常量 13 → fold key+value（JDBC 位点推进对 NO_PROGRESS 可见，3 次瞬时断库不再误判毒数据；488efcc）；② `tailrace-conn-jdbc` keyset `WHERE col > ? LIMIT n` 无 tie-breaker → 连接探针经目录视图强制游标列自身唯一（PK / 单列 UNIQUE，非唯一即 fail-fast；13d1c2b）；③ CI chaos job 追加 `jdbc_sources` 真库步（此前 CI 对 JDBC 真库覆盖为 0；19bb80b）。注：① 按评审建议触碰了 runtime 本体 1 处——属既有判据缺陷修复而非连接器引入，单独提交可审计。
>
> **low 六条仅登记未修，待后续卡 / 收口承接**〔2026-10-21 T23 已清偿 low#1/#2/#3/#5/#6（见 T23 卡与 CHANGELOG Unreleased）；low#4 已由 T14 清偿（1c5dcf6：console 静态类型注册表补 mysql/postgresql+console-store cursor_semantics 派生补 jdbc→JDBC_KEY，单测+集成双断言，见 T14 卡）；low#7 留 T18 不变〕：
>
> 1. `tailrace-conn-jdbc/src/lib.rs` `pg_cursor_kind` 接受 FLOAT4 为游标类型，但 `pg_cell` FLOAT4 臂无 is_cursor 分支、永不产出 canonical → FLOAT4 游标列探针通过后每轮 poll 以"cursor column is NULL"文案失败进入永久退避。处置：补 FLOAT4 臂 is_cursor（镜像 FLOAT8）或在 pg_cursor_kind 拒绝 FLOAT4。
> 2. `tailrace-catalog/src/lib.rs:121` 新增 `"JDBC"→"jdbc"`（源）紧邻既有 `"JDBC_TABLE"→"doris"`（汇）：MySQL 源写 kind=jdbc_table 会得 doris 并收到"source only supports kafka/file/jdbc (got doris)"费解诊断；新臂功能冗余（fallthrough 已小写化）；`lib.rs:25` canonical kind 注释未加 jdbc。处置：normalize_kind 注释与 parser 诊断点明 JDBC（源）/ JDBC_TABLE（Doris 汇）之别，补 :25 注释。
> 3. T13 卡验收"`git diff` 零触碰 runtime/api/parser 本体"与文件域"parser kind 枚举"不是同一集合：caa842e 改了 `tailrace-parser/src/lib.rs:124` 白名单两行（域内允许），而 commit 证据改述为"runtime/api/console 本体为空"。处置：卡片措辞改为"零触碰 runtime/api 本体 + parser 仅 kind 白名单"，证据中明示 parser 触碰行，而非换一组路径宣称零触碰。
> 4. `console-store/src/datasources.rs:227` cursor_semantics 派生仅 kafka→KAFKA_OFFSET / file→FILE_POINTER，jdbc 落 "NONE"；当前因 console 无 mysql/postgres 数据源类型（`tailrace-console/src/api/datasources.rs:104-110` registry_type 返回 None）不可达，一旦补类型即静默误标。处置：console 域后续项——补 "jdbc"→"JDBC_KEY"（对齐 IR `Cursor::JdbcKey`），新增数据源类型时一并加测试断言。
> 5. `docs/BENCHMARKS.md:354` §六 T20 汇总表"持平（+0.2%）"为 debug 档 / 单轮 ~3s / stats 0.5s 量化口径（文中自认 ±17%），对 <~17% 的 allocator 效应无检出力但表行不带注记；卡方案"碎片"口径无表行（仅结论段"未测"）；:356 默认档体积 264,674,112 B 与本机 target/debug 现存默认二进制（264,703,456 B、0 个 `_rjem_` 符号）不一致——基准二进制非最终提交树产物，仅标"caa842e + T20 工作树"。处置：表行加脚注（debug/短跑/量化误差）、补"碎片：未测"行、记录基准二进制对应精确树；release 长跑补测后再下 allocator 结论。
> 6. `docs/dev/roadmap.md:251` T17 结果"转产余量 ~1-1.5 人日"与 §八.4（:201）"估 ~2 人日"两个工作量口径；"1,078 kB" monaco 资产合计仅见于 T17 文本，T9 记录（:130）只有 883.22 kB gzip 与 CSS/ttf/worker raw 字节、无 gzip 值与折算规则，不可从仓库复算（883.22→140.45 / −84.1% 两处一致且锚定 T9；PoC 7f32c26 未混入 main）。处置：统一一个工作量数字并说明是否含 PoC 基线；补 1,078 的分项或改用 T9 已记录的 883.22 单口径。
> 7. （附，评审建议未采纳项）`jdbc_sources` 未门控时以 `return Ok` 跳过并计 passed（conn-kafka 同款 live-gate 惯例）：评审建议改 `#[ignore]`+`--ignored` 使跳过在计数中可见——本波只补 CI 真库覆盖，门控形态留作后续项与 conn-kafka 一并裁决。〔2026-10-21 T18 ⑦b 已裁决采纳本条建议：改 `#[ignore]` + CI `--include-ignored`（本次评审唯一立即整改，~0.5 人日，建议 R4-W0 小卡；5 测+jdbc/kafka/chaos 三处、ci.yml:125-130 追加，见 T18 卡与纪要 ⑦b）——裁决已立、实施待 R4-W0。〕

### R3-W3 评审登记（2026-10-21）

> **medium 一条已修+勘误一并（修复提交 6f855f7，main，父 5b44c76）**：T16 workspace 级 `default-features = false` 后，tests/contract 单包构建（`cargo test -p tests-contract --locked`）得零连接器 runtime——spawn_node→`PipelineRegistry::new`→`default_registry()` 注册表为空，executor_loop 3/3 FAILED（"源类型未支持：file"），与套件自述 file→file 矛盾；CI rust job 跑 `--workspace` 因与 node 默认 feature 统一而掩盖。修复=该套件 manifest 显式 `tailrace-runtime` `features=["connector-file"]`（套件仅行使 file→file：executor_loop.rs:33/43、lib.rs:103 均 kind:"file"），Cargo.lock 零变动；同笔勘误 BENCHMARKS §6.2 与根 Cargo.toml 注释——原「tests/contract 与 api 均不实例化连接器，两者零改动」对 api 成立、对 tests/contract 不实（spawn_node 走 default_registry() 是真注册连接器的）。验证（本机 CARGO_PROFILE_DEV_DEBUG=0，--locked 全程）：修复前 3/3 FAILED 复现→修复后 executor_loop 3+v1_protocol 4 全绿；`-p tailrace-api -p console-store` 22+15 绿（CI macos job 命令）；`--workspace` 176 passed/0 failed/2 ignored（与评审基线一致）；Cargo.lock 零 diff。
>
> **low 一条仅登记未修（BENCHMARKS §6.3 已记，与 latest+subscribe 窄窗同批）**：分区发现单向（只增不减）——30s 差异对比只处理新分区（conn-kafka lib.rs:222-251），分区消失后 offsets 陈旧项永不清理，TPL 重建（lib.rs:260-263）与 `cursor()` 携带死分区项；现实触发面窄（Kafka `--alter` 不能减分区），收缩侧剔除+topic 重建回归测试与窄窗修复同批留待（窄窗已排 v1.x、届时并入 split 化同张热路径卡，T18 纪要 :131）。

### R4 任务卡（1.0 发布轮，T25-T31 共 7 张，2026-10-22 立项；T22 已于 2026-10-21 落章上文、引用不重写）

> 立项依据：§七 R4 段 + 2026-10-22 勘察实录（现状锚点均为一手核实：cargo tree/dry-run 实跑、manifest 逐个 grep、ci.yml/README/release.yml/docs 树实读；T18 纪要 ⑦b 与 §九）。
> 总量评估：T25-T31 共 ~7.5 业余人日 + T22 承接 ~2 人日（§八.4 统一口径）≈ 9.5 人日，合 R4 ~1.5-2 周档。
> 红线（全轮生效）：§五既有红线沿用（状态账本/emit/SSE/smoke 骨架；**禁改 tag/动 v0.1.0**）；**cargo publish 不可逆——独占波（R4-W2）集中执行，发包前必过全树 dry-run 与查名**；版本 lockstep 单文件 bump（根 Cargo.toml 为唯一改动点）。

#### T25 ⑦b live-gate 整改（~0.5 人日）✅〔R4-W0 前置小卡；T18 ⑦b 裁决〕

- **现状**：T18 ⑦b 裁决采纳 R3-W1 评审 low#7 建议——`return Ok` 计 passed 改 `#[ignore]`+CI `--include-ignored`（纪要 docs/dev/R3-升格评审-2026-10-21.md ⑦b 与 :120 五测清单）。五处逐一核实（勘察实测行号）：`engine/crates/tailrace-conn-jdbc/tests/jdbc_sources.rs:111`、`:293` 两处 `return Ok(());`；`engine/crates/tailrace-conn-kafka/src/lib.rs:667`（TcpStream 9092 探测）、`:674`（topic 未播种跳过）、`:904`（纪要 :903 漂移 +1，分区发现测试 `kafka_source_discovers_new_partitions_without_reopen`@fn:898）；`engine/bin/tailrace-node/tests/chaos.rs:625`（chaos_kill9_restart_idempotent@fn:612）。CI 配套：`.github/workflows/ci.yml` chaos job 三条命令均无 `--include-ignored`（:125 chaos 全文件 / :126 kafka 过滤 / :130 jdbc 全文件）。**整改陷阱（勘察实测）**：:126 过滤词 `kafka_source_live` 不匹配 :904 的 discovery 测试名——只加 `--include-ignored` 不放过滤词，该测在 CI 永不被真跑；过滤词放宽为公共前缀 `kafka_source` 可两测兼收。
- **方案**：5 处 live-gate 块改 `#[ignore = "live: …"]` 属性（ignore 消息注明启用方式，`[skip]` 探测文案保留）；ci.yml chaos job 三条命令追加 `--include-ignored`，:126 过滤词放宽为 `kafka_source`；工作区级 :29 `cargo test --workspace --locked` 不动（ignored 默认跳过，跳过语义从「计 passed」变「ignored 计数可见」——诚实账本）。
- **验收**：`cargo test --locked -p tailrace-conn-jdbc --test jdbc_sources` 输出含 `2 ignored`（原静默 passed 转可见计数）；`cargo test --locked -p tailrace-conn-kafka` 含 `2 ignored`；`cargo test --locked -p tailrace-node --test chaos` 含 `1 ignored`；`cargo test --workspace --locked` 总数守恒（passed+ignored 之和不变，基线 R3-W3 176 passed/2 ignored → 预期 171/7）；push 后 `gh run watch` 绿且 chaos job 日志含 5 测真执行。
- **文件域**：engine/crates/tailrace-conn-jdbc/tests/jdbc_sources.rs、engine/crates/tailrace-conn-kafka/src/lib.rs（live-gate 测试块）、engine/bin/tailrace-node/tests/chaos.rs、.github/workflows/ci.yml。
> 结果：7d4c7c6 T25 live-gate 整改已按门禁指令提交为 7d4c7c6（main，父 14eb66a 线性，无 merge）。staging 严格限卡内 4 条文件域路径（ci.yml / chaos.rs / jdbc_sources.rs / conn-kafka lib.rs），+19/−8，无域外混入；主题与指令逐字一致（test: T25 live-gate 整改——#[ignore] 可见化+CI --include-ignored 真跑（T18 ⑦b 裁决）），body 按仓例承载验收证据：5 测 #[ignore] 清单、过滤词陷阱实证 1 passed/4 filtered out vs 2 passed/3 filtered out、workspace 176/2→171/7 守恒、L0 全绿。提交后工作树干净（porcelain 空），未 push（ahead 1，留波门禁执行）。

#### T26 crate 发布元数据补齐 + doc-test 起步（~1.5 人日）✅

- **现状**：`[workspace.package]` 仅 version/edition/license/rust-version 四键（Cargo.toml:37-41，实读）——repository/homepage/documentation/readme/keywords/categories 于 22 个 manifest 全 0 命中（勘察 grep 实测；license 是唯一就位的发布元数据）；description 缺 6 个：engine 侧 tailrace-conn-mqtt/tailrace-conn-s3 + console 全员 4 个（tailrace-console/console-store/console-auth/console-proxy），已有 16/22；`tests/contract` manifest 无 `publish = false`（实读——name=tests-contract 会误入发包面）；doc-test 全空跑：21 crate `///` 示例围栏 0 个（`//!` 亦 0；文档注释本身 1652+424 行存在），`cargo test --doc -p tailrace-ir`/`-p console-store` 实跑 0 passed 0 failed——「过」但为空跑。
- **方案**：① 共享键 repository/homepage/documentation 入 `[workspace.package]`，成员 manifest 以 `.workspace = true` 继承（两个 bin crate 的 readme 单独指）；② 6 个缺 description crate 补一句话描述（与 crate 功能面对齐）；③ keywords/categories 逐 crate 定制（crates.io 官方 category slug，以 `cargo publish` 校验为准）；④ `tests/contract/Cargo.toml` 加 `publish = false`；⑤ doc-test 最小面 3 个对外 API crate：tailrace-connector（Source/Sink trait+ConnectorRegistry）、tailrace-ir（Cursor/SourceChunk）、tailrace-state（OffsetStore commit 语义），示例合计 ≤10 个，令 `cargo test --doc --workspace` 非零（勘察建议口径）。
- **验收**：`cargo publish --dry-run --registry crates-io -p tailrace-ir 2>&1 | grep -c 'no documentation, homepage or repository'` = 0（叶 crate 无 metadata warning；conn-mqtt/conn-s3 同验）；`cargo metadata --no-deps` 21 个发布成员均有 description/repository；`cargo test --doc --workspace --locked` 非 0 passed；L0（fmt/clippy/deny）绿。
- **文件域**：根 Cargo.toml（workspace.package 键）、22 个成员 Cargo.toml（继承行+description/keywords/categories+publish=false）、tailrace-connector/tailrace-ir/tailrace-state 的 src 文档示例（不动逻辑）。
> 结果：bf2848f T26 收口提交完成：git add 严格限 28 文件域（显式路径列表，非 add -A），以门禁给定 Conventional 消息提交为 bf2848f（main，父 f90e6a2 线性追加，28 files changed +361/−0），主题=chore: T26 crate 发布元数据补齐+doc-test 起步（21 成员 description/repository 就位+3 crate 文档示例）。工作树提交后干净，未 push（留波门禁）。

#### T27 crates.io 全树发包（~1.5 人日）〔依赖 T26〕〔publish 不可逆，独占波〕✅

- **现状**：crates.io 侧 tailrace-* 19 个 lib 名从未发布（勘察 dry-run 查无 tailrace-catalog 佐证）；`tailrace`/`tailrace-node`/`tailrace-console` 三名 0.0.0 占位已注册（§七，2026-10-21）。内部依赖 path+version 双声明 19 条（Cargo.toml:132-162 实读：[workspace.dependencies] 内部包区 16 tailrace-* 含 conn-s3@:138 + 3 console-*；sqlx 等外部项散布其间）——打包时按注册表解析，依赖未发布则叶 crate dry-run 必失败（实测 `-p tailrace-connector` → error: no matching package named 'tailrace-catalog'；对照零内部依赖的 `-p tailrace-ir` dry-run 全通过）——**必须按拓扑序先发依赖**。发布拓扑（勘察 cargo tree 闭包核实）：L0 ir/decode/transform/conn-mqtt/conn-s3/metadata/console-store/console-proxy → L1 state(ir)/catalog(ir)/parser(catalog)/auth(store) → L2 connector(ir,catalog) → L3 conn-kafka/conn-file/conn-doris/conn-jdbc → L4 runtime → L5 api → L6 node/console；tests-contract 排除后实发 **21 包**。版本事实：单源 workspace version=0.1.0，lockstep bump 改根 Cargo.toml 单文件（[workspace.package].version + workspace.dependencies 19 条内部 version 串）；git tag v0.1.0 已存在且禁改（§五纪律 3）；CHANGELOG.md:5 声明 SemVer。**前置风险（勘察未覆盖，第一动作核查）**：release.yml:55-56 构建 web bundle 是 rust-embed 前置——`tailrace-console` 发包产物必须自包含可构建（embed 资产随包或 build.rs 降级方案），否则 `cargo install` 必失败；不可行则 console bin 走 release tarball 单渠道并如实登记本卡。
- **方案**：① 查名：21 包名逐一 crates.io API 核验未被抢注（勘察明示未查）——命中抢注即停波上报裁决；② rust-embed 自包含核查（上述风险项）定 console 发包形态；③ lockstep bump **1.0.0**（R4 定名 1.0，与 tag v1.0.0/CHANGELOG 对齐；单文件改动+Cargo.lock 再生）；CHANGELOG Unreleased → [1.0.0] 段定稿；④ `scripts/release-crates.sh`：内置拓扑序，逐包 `cargo publish --dry-run` 绿→实发→`--list` 输出包清单（供收口循环）；同级多包发完等注册表传播（下一级 dry-run 失败即重试窗口）；⑤ 发后 API 核验 21×1.0.0。registry token 由发布机环境提供（严禁入库）。占位名 `tailrace` 维持 0.0.0 不动（无对应 crate）。
- **验收**：`for c in $(bash scripts/release-crates.sh --list); do curl -sf https://crates.io/api/v1/crates/$c | grep -o '"max_version": *"1\.0\.0"'; done | wc -l` = **21**；发包日志留档（每包 dry-run→publish 双行）；`cargo install tailrace-node --version 1.0.0 --locked` 从注册表装出可执行二进制（console 视 ② 核查结论同验或登记 tarball 单渠道）；L0+L1 绿。
- **文件域**：根 Cargo.toml（version bump）、Cargo.lock、CHANGELOG.md、scripts/release-crates.sh（新）。
> 结果：5a44727 T27 落档「发包准备就绪、实发未启动」：lockstep bump 1.0.0（根 Cargo.toml 单文件+Cargo.lock 再生+CHANGELOG [1.0.0] 定稿）与 scripts/release-crates.sh（21 包拓扑序/dry-run 门槛/--resume 断点续发）入提交。console rust-embed 自包含核查**可行**（SPA 随包，非 tarball 单渠道）：spa.rs 改嵌 crate-local dist/（原 ../../../web/dist 仓内路径）、Cargo.toml include 列表覆盖 gitignore 携带 /dist/**、build.rs 三态（repo web/dist 镜像/发包副本原样/placeholder 兜底）、发包脚本打包走查前物化 dist/ 且无 assets 拒发（placeholder 不许上车）；tailrace-console dry-run 打包档三轮通过（19:43:37/19:56:41/20:13:01 三轮 summary 21/21 green）。**publish 未启动（非部分成功、全成未达）**：审计日志零 PUB/SKIP 行（全 DRY）；断点=末轮 dry-run 20:15:47 tailrace-api OK 后 **20:17:59 DRY tailrace-node FAIL (verify)** 硬停（非传播类错误，stderr 未入档），tailrace-console 未及跑、无 summary 行——按方案④ dry-run 门槛未全绿即不实发。续发口径：诊断 node 包后 `--dry-run` 全绿，由波门禁独占执行 `bash scripts/release-crates.sh --publish [--resume]`（--resume 逐包查 crates.io max_version=1.0.0 跳已发；现 0 包已发=全量 21 包拓扑序；--publish 不带 --allow-dirty 需净树，token 运行时环境注入不落盘）。发后核验**未过**：卡内 API 循环实跑 **0/21**（spot：tailrace-node/tailrace-console 200+max_version 0.0.0、tailrace-ir 404——19 lib 名从未发布、三占位名仍 0.0.0）；`cargo install tailrace-node --version 1.0.0 --locked` 实装实跑 exit 101（注册表无 1.0.0）。发包日志留档 scripts/release-log-20261022.txt（99 行 DRY/PUB/SKIP 时间戳审计；.gitignore:26 出 VCS 留档发布机）。〔**2026-10-23 补录：发包完成**——21 包 @1.0.0 全部落账（API 逐包核验 21/21；无尘 `cargo install tailrace-node --version 1.0.0 --locked` 6m38s 编译实装、二进制响应 1.0.0）。过程两修：① release-crates.sh `registry_max_version` 末端 grep 管道在 crate 未注册时 exit 1，`set -e` 在 `$()` 赋值处静默杀脚本（波门禁曾因此叠加 `| tail` 掩码假通过——verify 门禁诚实拦截，零包误发）→ 管道补 `|| true`；② cargo 脏树检查连 gitignored 文件也计脏（物化 dist/ 68 文件），console 的 dry-run 门禁与实发精准加 `--allow-dirty`（其余 20 包维持净树纪律）。平台障碍：crates.io 新 crate 限速 ~1 个/10 分钟（429 滚动窗口）+境内 SSL 抖动，`--resume` SKIP 链幂等续发 4 轮完成；一次上传超时但服务端已落账由 resume 查证自愈。审计日志 release-log-20261022/23.txt 双文件（按日切）。〕

#### T28 README 双语定稿（~1.5 人日）〔基准外显；北极星措辞按 T18 §九〕✅

- **现状**：README.md 62 行 / README.zh-CN.md 58 行（wc 实测），Status 段已是 R3 complete→R4 underway（README.md:7-14）；Quickstart 三命令在位（:20-35：build.sh→console→node serve，25/44 断言口径 :38-40）；连接器矩阵为 T23 勘正后现状（:42-52：jdbc ✅ / starrocks kind-gated 语义 / mqtt、s3 占位）。缺口（勘察 grep 实测，本轮复核一致：双 README `[!`/shields.io 均 0 命中）：**徽章矩阵全无**（CI/crates.io/license/MSRV 四徽章位全空）、**FAQ 章节无**、design/architecture.zh.md 与 CHANGELOG.md（85 行 Unreleased）均未链。北极星措辞债：现行「Doris/StarRocks 写入语义完整」（roadmap.md:9）在 v1.0 时点不诚实——T18 §九裁决按档位表述：「label 幂等档已交付（+重启幂等 e2e），事务接口/group commit 为 1.0 后演进档」。基准外显数据源（BENCHMARKS.md 608 行勘察实读）：§三宏基准表（:91-93，file→file 全链 21739 rows/s@debug+机读行）与 §六 M4 机数字（kafka(P=6)→file 中位 27861 rows/s、T16 裁剪档二进制 −9.4%、T15 分区发现 26.8s/30.7s（subscribe/assign，BENCHMARKS :363；T15 提交过程三跑口径 26.8-31.2s 见本文件 T15 结果行——README 摘录唯一源=BENCHMARKS 终稿））；§二 criterion 微基准偏内部不外显；**跨机器不可比注记（§六表前注）必须随数字携带**。
- **方案**：① 徽章四枚双语同步（GitHub Actions CI、crates.io 版本、License Apache-2.0、MSRV 1.95）；② FAQ 章新增（≥5 问：与 Flink/SeaTunnel CDC 类之别、为何无 JVM/单二进制、StarRocks 与 Doris 通道关系、mqtt/s3 何时有（引 T18 裁决触发器）、平台/MSRV 矩阵）；③ 北极星/卖点措辞按 T18 §九档位口径改写（「label 幂等已交付+重启幂等 e2e；事务接口/group commit 见 roadmap v1.x 议程」）；④ 基准外显段：§三+§六摘录+跨机器不可比注记随行+链接 BENCHMARKS 全文；⑤ 链接补齐：CHANGELOG/design/architecture.zh.md/docs 站（T29 产物）；⑥ i18n 中文单轨明示一句（T30 呼应）；⑦ zh 侧与 EN 互为镜像（既有惯例，尾行互链保留）。
- **验收**：`grep -cE '\[!\[' README.md` ≥4 且 `grep -cE '\[!\[' README.zh-CN.md` ≥4；`grep -ci '^## .*faq' README.md README.zh-CN.md` 各 ≥1；`grep -c 'architecture.zh.md' README.md` ≥1 且 CHANGELOG 链接存在；EN 侧无 "complete/full write semantics" 全谱系措辞（`grep -ci 'write semantics' README.md` 仅命中档位句）+ zh 侧 `grep -c '写入语义完整' README.zh-CN.md` = 0；基准段 `grep -c '27861' README.md` =1 且 `grep -c '跨机器' README.zh-CN.md` ≥1（注记随行）。
- **文件域**：README.md、README.zh-CN.md。
> 结果：d1838dd T28 已提交：d1838dd（main，父 f69bf4e 线性）。staging 严格限文件域 2 文件（README.md/README.zh-CN.md，+164/−9），主题=docs: T28 README 双语定稿——徽章四枚+FAQ+北极星档位措辞+基准外显（跨机注记随行）。验收复跑（HEAD f4624e9 评审修后口径）全绿：徽章 [![: 双语各 4（≥4）；FAQ 章双语各 1；EN 侧 architecture.zh.md 链 1+CHANGELOG 链 1；EN 'write semantics' 0 命中、zh「写入语义完整」0 命中（全谱系措辞清零；档位句正向锚=README.zh-CN.md:19「label 幂等档已交付」）；基准段 27861 ×1（README.md:79，四轮 23918/27678/28043/28474 与 BENCHMARKS.md:362/:597 一字不差）+zh「跨机器」注记 ×2（README.zh-CN.md:63/:70，含与 Intel 侧 24658 不可比句随行）；README 62→145 行 / zh 58→130 行（wc 实测）。评审修 f4624e9 随行补齐 roadmap.md:9+architecture.zh.md:24 北极星档位化镜像（README 侧一次到位）。未 push（留波门禁）。工作树干净。

#### T29 mdBook 文档站骨架（~1 人日）✅

- **现状**：docs/ 全量 8 篇 2300 行（wc 实测，本轮复核一致）：BENCHMARKS.md 608、design/architecture.zh.md **541（`## ` 章 13 个、mermaid 块 8 个——对表勘正：§七原口径 12 章，实测 13）**、dev/roadmap.md 354、dev/T11 248、protocol/engine-v1.md 229、dev/R3 评审 134、connectors/jdbc.md 124、protocol/console-api.md 62；全仓无 book.toml/SUMMARY.md（勘察 0 命中）。§七 R4 段「设计文档以脱敏干净版重新入仓」已由现状基线图文版 architecture.zh.md 兑现（公仓 @fbad97f 镜像），可直接作骨架章。
- **方案**：mdBook 骨架（book.toml + docs/book/src/SUMMARY.md）：前言（项目一句话+链接 README quickstart）→ 架构（`{{#include}}` design/architecture.zh.md）→ 协议（protocol/engine-v1.md + console-api.md）→ 连接器（connectors/jdbc.md + 矩阵章占位）→ 基准（BENCHMARKS 摘要章+全文链接）→ 开发指针（roadmap/CHANGELOG 链接，不全文入站）；dev/ 评审纪要与 T11 事故档不入站（内部档）；中文单轨明示（book title/语言标注）；构建档默认本地 `mdbook build` + CI artifacts（免 gh-pages 部署面；升 gh-pages 留卡内裁决注记）。
- **验收**：`mdbook build` 零错误退出且 `test -f docs/book/book/html/index.html`（或构建输出目录）；SUMMARY 展开 ≥6 章节页且 `grep -c 'architecture' docs/book/src/SUMMARY.md` ≥1、include 生效（产物 HTML 含架构章正文文本抽样）；mermaid 块以源码块或预渲染占位呈现不丢内容；CI（如立 job）绿或本地构建档登记入卡结果。
- **文件域**：book.toml（新）、docs/book/（新，SUMMARY+前言/占位章）、.github/workflows/ci.yml（仅当立 docs job）。
> 结果：f69bf4e T29 已提交：f69bf4e（main，父 83b16b1 线性）。staging 11 文件 +112：book.toml（19 行，mdbook v0.5.4、build-dir=docs/book/book/html 对齐卡内验收路径、gh-pages 升档留头注裁决注记）+docs/book/src/ 9 页（SUMMARY 8 章：前言/架构/引擎协议/Console API/JDBC 批源/连接器矩阵/基准摘要/开发指针；4 篇 {{#include}} 全文引入正典 architecture.zh.md/engine-v1/console-api/jdbc，基准为摘要章+全文链接，矩阵章占位）+.gitignore +3（docs/book/book/ 构建产物不入库——文件域外伴行项如实点名）。验收复跑全绿：mdbook build（v0.5.4）零错误退出+docs/book/book/html/index.html 存在；SUMMARY 8 章（≥6）且 'architecture' 命中 1；include 生效（architecture.html 含架构正文样本）；mermaid 8 块以源码块原样呈现不丢（源 mermaid 围栏 8=html language-mermaid 8）；CI docs job 未立→走卡内「本地构建档登记入卡结果」路线。评审修 f4624e9 补 engine-v1 动作端点标题（:141 {action} 尾加括注 pause/resume/stop，重建后 protocol-engine.html 标题含 {action}、锚点 id 完整）。未 push（留波门禁）。工作树干净。

#### T30 i18n 错误码字典化接口预留（~0.5 人日）✅〔§七 R4 段 i18n 最小动作〕

- **现状**：console 错误码面已集中两处——`ErrorResponse/ErrorBody{code:String,message:String}`（console/crates/tailrace-console/src/api/mod.rs:22-29）与 `ApiError{status,code,message,refs}`（console/crates/console-auth/src/error.rs:11-16），后者 5 个构造帮助方法硬编码 **6 个规范码**（UNAUTHENTICATED/INVALID_ARGUMENT/NOT_FOUND/INTERNAL/IN_USE/BAD_NODE_TOKEN）——码面单点在 error.rs，枚举化只差 String→常量；中文文案散布 api/*.rs+spa.rs 约 67 行（43 处 format! 带中文，勘察 grep）；engine 侧 tailrace-api 无错误码（11 处 ad-hoc `json!({"error":…})`）——**不在本卡，登记 v1.x**。
- **方案**：最小动作=接口预留、文案不动：① 6 码收敛为常量/枚举单点（error.rs），构造方法签名与现值不变；② `ErrorBody` 加 `message_key: Option<String>`（serde `skip_serializing_if = "Option::is_none"`——wire 兼容，默认响应零变化），为字典化查表预留通道；③ 中文单轨现状一句入 README（文案句由 T28 承接）；文案字典化本体（67 行收敛 key 查表）与 engine 侧 code 字段补齐留 v1.x 议程（卡内登记）。
- **验收**：`cargo test -p console-auth -p tailrace-console --locked` 全绿含新增 wire 兼容断言（现网形态响应序列化后无 `message_key` 键）；`grep -c '"UNAUTHENTICATED"' console/crates/console-auth/src/error.rs` 收敛为常量定义单点（构造方法引用常量名而非字面量）；L0 绿。
- **文件域**：console/crates/console-auth/src/error.rs、console/crates/tailrace-console/src/api/mod.rs（+对应测试）。
> 结果：31fea8a T30 已按波门禁指令提交为 31fea8a（main，父 bf2848f 线性，非 amend）。staging 严格限文件域 2 文件（console-auth/src/error.rs +35、tailrace-console/src/api/mod.rs +48，共 +76/−7），无外来变更混入。提交主题与指令逐字一致（feat: T30 i18n 接口预留——错误码单点收敛+message_key wire 兼容通道），body 记录：6 规范码常量单点收敛（构造签名/现值不变）、message_key wire 兼容通道（默认响应 byte 级不变）、wire 兼容断言×2、cargo test -p console-auth -p tailrace-console --locked 全绿与 L0 三件绿（上一轮实测）、域外不动项（4 处 IN_USE 调用点/67 行中文文案/engine 侧留 v1.x，README 归 T28）。未 push（留波门禁）。工作树干净。

#### T31 v1.0.0 终验：tag→release + checksums + 基准报告收口（~1 人日）〔门槛=T22/T25-T30 全 ✅〕 ✅

- **现状**：git tag 仅 v0.1.0（实跑 `git tag -l`），禁改禁动；release.yml 已自动化全链（实读）：tag v* → preflight（clippy --locked -D warnings）→ 4 目标矩阵（linux x64/arm64 cross + macos x64/arm64）→ package.sh 每 target 2 tarball（console+node）共 8 → aggregate `shasum -a 256` 出 checksums.txt → GitHub Release 共 **9 资产**（v0.1.0 基线 9/9，release.yml:76-96）；基准报告入 README 为 §七 R4 清单项（外显面 T28 已定，BENCHMARKS 608 行就绪）。
- **方案**：① 全卡收口后 `git tag v1.0.0 && git push origin v1.0.0` 触发 release.yml；② 资产核验 9/9（8 tarball+checksums.txt）+ checksums 覆盖核对（下载抽验 `shasum -a 256 -c`）；③ README 基准段终验（与 BENCHMARKS 终稿数字一致、注记在位）；④ **全新机器三命令 quickstart 实测**：干净环境（rustup+Node 22+pnpm，无 target/无 pnpm store）按 README Quickstart 逐字执行三命令（build.sh→console→node serve）全成功；`cargo install` 档按 T27 ② 核查结论同验或登记；⑤ 收官报告 `docs/dev/v1.0-release-<date>.md`（发包 21 包/资产 9/基准摘要/全新机器实测记录/v1.x 遗留议程指针）；⑥ 本文件 §七 R4 段与各卡状态收口。
- **验收**：`gh release view v1.0.0 --json assets --jq '.assets | length'` = **9** 且资产名单含 checksums.txt + 8 tarball（4 目标×2 bin）；§十一 R4 收口标准全绿（含 crates.io 21×1.0.0 复核）；quickstart 实测记录入收官报告；收官报告存在 `test -s docs/dev/v1.0-release-*.md`。
- **文件域**：docs/dev/v1.0-release-<date>.md（新）、docs/dev/roadmap.md（状态收口）、README.md/README.zh-CN.md（仅终验勘误，如需）。
> 结果：2026-10-23 v1.0.0 正式发布——tag bd8f301→release.yml 四目标全绿 9/9 资产；checksums 抽验双 OK（首轮失败=下载侧网络截断，镜像复验排除）；全新环境三命令 quickstart 实测全过（build.sh 双二进制/console 首启 healthz 200+token/node 自注册 m2m 通）；crates.io 21×1.0.0+无尘 cargo install 实装（R4-W2）；终验全文 docs/dev/v1.0-release-2026-10-23.md。R4 全轮收官（T22/T25-T31 ✅；选做 T19/T21 待点名；v1.x 议程=T18 纪要+R5 草案）。

### T39 集中尾债清偿（~2 人日）〔R5-W0.5 波，2026-10-23 立项——R5-W1 高风险波前的全面清账〕

- **现状**（2026-10-23 全仓横扫，含 roadmap 三轮评审登记+§八+各波 caveat）：① CHANGELOG 1.0.0 后无 Unreleased 段（T25/T22/T26/T30/T32/T34 六卡未入账——R5 纪律留 T38 统一，但 T38 只做版本号转正，条目本体先立）；② T22 编辑器 gzip 138,962B 未入 BENCHMARKS（文件域=web/ 时域外）；③ R5-W0 评审 low#1 36 key/50 调用点双 off-by-one 勘正；④ 引擎主循环 `state.lock().unwrap()` 三处（runtime lib.rs:775/:879/:912）——registry 侧已 `unwrap_or_else(into_inner)` 防御而主循环未（对称化，Mutex 中毒即 panic 的防御）；⑤ console TableObjectItem（datasources.rs:258）不回 columnsJson——列补全全链路缺 console 一跳（T22 caveat 登记、SqlMonaco.vue:99 注释已预留消费端）；⑥ starrocks datasource 类型在 console 注册表缺（T32 caveat——bench 走 node run 绕行，同口径对照需类型登记）；⑦ zip §八#1 触发器检查：utoipa-swagger-ui 10.0.2（2026-10-20）是否解 zip ^8.6 锁——是则升+回收 deny ignore，否则续挂注记；⑧ OTLP 默认开启裁决（§八#2）——v1.0 已发，按使用反馈决定（本轮只登记不裁）。
- **方案**：① CHANGELOG Unreleased 段立起（六卡条目，Keep a Changelog 风格，T38 届时改版本号即可）；② BENCHMARKS §六增 CM6 行（138,962B gzip 编辑器 chunk，T22 提交说明数字，口径注记）；③ T34 结果行/messages.rs 模块文档 36 key/50 调用点勘正；④ runtime 三处 `unwrap()` → `unwrap_or_else(|e| e.into_inner())`（registry 同款防御，加一行注释说明理由——Mutex 中毒=持锁线程 panic，锁内状态仍一致可继续）；⑤ TableObjectItem 加 `columns_json` 字段+list 端点回传+openapi DTO 更新+web 端消费核验（SqlMonaco 列补全链通——加一条 e2e 或集成测试断言 DTO 携带列）；⑥ console datasource-types 注册表加 starrocks（starrocks_sink 侧 connJson 复用 doris 格式）+`table_kind_for`+`cursor_semantics` 派生+集成测试；⑦ 查 utoipa-swagger-ui 10.0.2 的 zip 依赖（crates.io API 或本地 `cargo update -p`）——9.x 可达则升+deny.toml 回收三条 ignore+§八#1 关闭注记；⑧ §八#2 OTLP 加一行 v1.0 后观察注记（不裁）。
- **验收**：`grep -q '^## \[Unreleased\]' CHANGELOG.md` 且含 T25/T22/T26/T30/T32/T34 六卡关键词；`grep -c '138,962\|138962' docs/BENCHMARKS.md` ≥1；runtime lib.rs `grep -c 'state.lock().unwrap()'` =0（三处全防御化）；TableObjectItem 含 columns_json 且 web SqlMonaco 补全链 e2e 断言（或集成测试）绿；`grep -q starrocks console/crates/tailrace-console/src/api/datasources.rs`（注册表行）；⑦ 的结果（升级或续挂）写入卡内结果行；L0+L1 全绿。
- **文件域**：CHANGELOG.md、docs/BENCHMARKS.md、docs/dev/roadmap.md（③勘正）、engine/crates/tailrace-runtime/src/lib.rs（④）、console/crates/tailrace-console/src/api/datasources.rs+openapi.rs（⑤⑥）、web/src/（⑤消费核验）、Cargo.toml+Cargo.lock+deny.toml（⑦若升）、tests/（⑤⑥新测试）。
> 结果：2026-10-23 工作树（提交 SHA 由波门禁补）八项全清——① CHANGELOG `## [Unreleased]` 立起（Added×3/Changed×3，Keep a Changelog）：T32/T34 为 v1.0.0 后真未入账项如实入账，T39 自身 ④⑤⑥⑦ 四项用户可见变更同段立条目（版本号转正留 T38）；**卡内「六卡未入账」现状勘正**：T22/T25/T26/T30 四卡提交（f90e6a2/7d4c7c6/bf2848f/31fea8a）经 `git merge-base --is-ancestor` 均为 v1.0.0（bd8f301）祖先且早在 `[1.0.0]` 段（`roadmap T22`/`T25`/`T26/T27`/`T30` 四处），按 Keep a Changelog 不重复入账，但段首加一句 scope 注记（含 T22/T25/T26/T30 四卡名与其已随 1.0.0 发布的事实），验收「六卡关键词」在 Unreleased 段内与 CHANGELOG 全文均全命中；② BENCHMARKS §六 增 T22 行（CodeMirror6 档 **138,962 B** gzip vs monaco 档 883.22 kB，−84.3%）+脚注⁷口径注记（数字取自 f90e6a2 说明；体积=确定性产物、跨机可比；本树 ⑤ 接线后复测 139,007 B/+45 B）；③ 36 key/50 调用点实测复核成立（`template` 臂/KEY_LIST 各 36；`grep -rn 'messages::' src/` 排除本模块 =50）→ messages.rs 模块文档「规模口径」段+`KEY_LIST.len()==36` 机器钉+T34 结果行勘正（附带同行勘正 ApiError 残留 18→34 站点，按 ffb1d55 多行扫口径）；④ runtime lib.rs **实为 8 处**非防御 `state.lock()`（卡内锚点 :775/:879/:912 为 `run_statement_serve` 的 save 三处；另 `run_statement` 的 save 三处 :966/:1064/:1096 + 两函数入口多行 `.lock().unwrap()` resume load 各 1 处——单行 grep 看不见），全部 → `unwrap_or_else(|e| e.into_inner())`，理由落 `run_statement` 文档注释（serve 侧 resume load 行内一句指针），注释零净增行——卡内锚点 :775/:879/:912 行号在改后树原位可核；验收 `grep -c 'state.lock().unwrap()'`=0 且 perl 多行扫=0；pipeline_log.rs 另 5 处 `.lock().unwrap()`（log hub mirror，非 state 锁、非本卡文件域）如实登记不动；⑤ `TableObjectItem.columns_json`（`Option<Vec<(String,String)>>`，null=未登记列，键恒在）list 端点回传——按 id 走既有 `table_object_columns_json` 访问器（pipelines.rs 装配同款）以不动 console-store 行模型/文件域；openapi 组件随 ToSchema 自动更新，openapi.rs 新测 `table_object_item_exposes_columns_json` 钉住 `TableObjectItem.properties.columnsJson`（旧键同在）；**web 接线落地**：api/datasources.ts `TableObject.columnsJson`、edit.vue 映射 `columns`、SqlMonaco.vue buildSchema children 槽+memberCompletion 叶节点填列（T22「列数据到位即通」实为槽位空挂 `children: []`/`new Map()`，需本卡 ~20 行接线补齐）；集成测试 `table_list_carries_columns_json`（verbatim 回传/null/键恒在/旧字段不动）绿；web `pnpm typecheck && pnpm build` 双 0；⑥ starrocks 注册表成员（doris connJson 形态 jdbcUrl+streamLoadUrl 必填、fenodes 可选，label「StarRocks」；doris 条目 label「Doris / StarRocks」→「Doris」避免双选项歧义）+registry_type/required_conn_keys/conn_spec（FE MySQL 协议共用 discover/infer-schema）+**table_kind_for 勘正**：原 `"doris" | "starrocks" => "doris"` 折叠会让 starrocks 表对象以 doris kind 下发引擎，而 conn-doris 据 `obj.kind == "starrocks"`（lib.rs:175）选 StarRocks 头集（契约测试断言不发 read_json_by_line/sequence_column）——折叠即失效；改为各自独立 kind（conn-doris Cargo.toml `default = ["starrocks"]` 注释所述设计），cursor_semantics 派生 NONE；集成测试 `starrocks_type_registered_through_console`（缺 streamLoadUrl 400→建成→kind=starrocks/cursor NONE→kind 过滤独立→未知类型消息含 starrocks）绿；`err.datasource.unknown_type_registry` 文案加 starrocks；⑦ **已升级（触发器到）**：crates.io API + 本地 registry 源双证 utoipa-swagger-ui 10.0.2（2026-10-20，rust_version 1.88，feature 面与 10.0.1 全同）build-dep `zip = "9.0"`（10.0.1 为 "8.6"）→ `cargo update -p utoipa-swagger-ui`：10.0.1→10.0.2、zip 8.6.0→**9.0.5**（+传递 cfg_aliases 0.2.2/nix 0.31.3，无重复版本；根 Cargo.toml `"10"` 无需改）；deny.toml 删三条 RUSTSEC-2026-0367/0368/0369 ignore、`yanked` warn→**deny**；`cargo deny check advisories bans licenses sources` 四门 ok；10.0.2 构建脚本自然路径（curl 下载 v5.32.6.zip）本机实跑成功；§八#1 关闭注记落（附带勘正「vendored dist」措辞）；⑧ §八#2 OTLP 观察注记落（不裁、续挂）。门禁：L0 `cargo fmt --all --check` 0 diff / `cargo clippy --workspace --all-targets --locked -- -D warnings` exit 0 / deny 四门 ok；L1 `CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked` **190 passed / 7 ignored / 0 failed**（含 doc-test 6；datasources_crud 5→7、openapi.rs 1→2）；WEB typecheck+build exit 0/0。域外登记（文件域外不动）：docs/protocol/console-api.md:23 注册表列表句未加 starrocks。未 commit/未 push（按指令留波门禁）。

### R4-W1 评审登记（2026-10-22）

> 本波无独立评审档（docs/dev/ 现存仅 R3 升格评审纪要与 T11 事故档）；发现均随卡实测得来，登记如下：
>
> - **已修（修复即任务提交 7d4c7c6，无遗留）**：T25 过滤词陷阱实证——ci.yml kafka 过滤命令原过滤词 `kafka_source_live` 不匹配 discovery 测试名，只加 `--include-ignored` 不放宽过滤词则该测在 CI 永不被真跑；实测对照 1 passed/4 filtered out（未放宽）vs 2 passed/3 filtered out（放宽后），修复=过滤词放宽为公共前缀 `kafka_source`（立项勘察已预警、本波实证收口，卡内交付非债）。
> - **low 仅登记未修（T30 域外不动项，卡内方案即预留 v1.x/后续卡承接）**：① console 侧 67 行中文文案字典化查表本体留 v1.x（本波只落 6 码常量单点+message_key wire 兼容通道）；② engine 侧 tailrace-api 11 处 ad-hoc error JSON 补码留 v1.x；③ 中文单轨现状一句入 README 归 T28（文案句由 T28 承接）；④ 4 处 IN_USE 构造调用点不动（构造签名/现值不变契约，字面量收敛后仅存于常量定义处）。

## 十、R3/R4 波次编排（文件域冲突分析，动工前按最终文件域复核）

| 波 | 任务（并行 ‖） | 冲突分析 | 波门禁 |
|---|---|---|---|
| R3-W1 | T13 JDBC 批源 ‖ T17 CM6 评估(PoC 分支) ‖ T20 jemalloc | 新 crate / PoC 分支 / node bin feature，零交集 | L0+L1+push+CI 绿 |
| R3-W2 | W0：T23 评审债清偿（串行先行）→ T12 StarRocks ‖ T14 discover/infer-schema | T23 动 README/CHANGELOG 与 T12 文件域相交故先行；其后 conn-doris+compose vs console+engine 元数据模块零交集 | W0：L0+push+CI 绿；W1：L0+L1+L3(starrocks profile 本地档)+push+CI 绿 |
| R3-W3 | W0：T24 尾债清偿（串行先行）→ T16 feature 裁剪验证（本机 e2e，环境配方=AGENTS-环境契约 §3） | T24 动 docs/scripts 与 T16 文件域不相交；T16 裁剪档 e2e 依赖 Doris 栈窗口 | W0：L0+push+CI 绿；W1：L0+L1+裁剪档 e2e+体积表+环境复原终检+push+CI 绿 |
| R3-W4 | T15 分区并行 b（指纹回归先行） | 引擎热路径独占波 | L0+L1+L3+L4+push+CI 绿 |
| R3-W5 | T19 SSH 部署链〔选做〕 | console 域 | L0+L1+push+CI 绿 |
| R3-W6 | T21 fuzz 起步〔选做〕 | fuzz/ 新目录 | 语料跑绿 |
| R3-W7 | T18 升格评审 + 终验：全量 L0-L4 + BENCHMARKS 终稿 + 收官报告 | — | 全绿收官 |
| **R4-W1** | W0：T25 ⑦b live-gate 整改（串行先行）→ T22 CM6 转产 ‖ T26 元数据+doc-test ‖ T30 i18n 接口预留 | 四域零交集（T25=engine 三测试文件+ci.yml；T22=web/；T26=22 manifest+connector/ir/state 文档示例；T30=console-auth+console api）；T25 先行定 CI 计数口径（ignored 可见化），后续波门禁测试账本诚实；T22 依赖 T14 SQLNamespace 已就绪（R3 交付） | W0：L0+L1+push+CI 绿（chaos job 含 `--include-ignored` 真跑）；余波：L0+L1+push+CI 绿 |
| **R4-W2** | T27 crates.io 全树发包〔依赖 T26〕 | **publish 不可逆→发包独占波**：集中执行留审计（逐包 dry-run→publish 双行日志）；动根 Cargo.toml（lockstep bump）与 T26 同文件故不同波；README/文档本波不动（T28 徽章需已发布版本号，排 W3） | L0+L1+查名 21 包+console rust-embed 自包含核查+全树 dry-run→实发→API 核验 21×1.0.0+`cargo install` 抽验+push+CI 绿 |
| **R4-W3** | T28 README 双语定稿 ‖ T29 mdBook 骨架 | 文件域零交集（README\*/CHANGELOG 链接 vs book.toml+docs/book/ 新增）；软依赖互链（README↔文档站 URL）以最终路径占位、波门禁一并验；T29 只读引用 docs/ 既有 8 篇不改动 | L0（clippy+deny；fmt 对 md 不生效）+`mdbook build` 绿+README/zh 双语 grep 验收+push+CI 绿 |
| **R4-W4** | T31 v1.0.0 终验：tag→release+checksums+基准报告收口 | 门槛=T22/T25-T30 全 ✅；tag/release 不可逆动作独占波（release.yml 自动化 9 资产） | §十一 R4 收口标准全绿收官（含 21×1.0.0 复核+资产 9/9+全新机器三命令 quickstart） |
| **R5-W0** | T32 Doris 宏基准 ‖ T34 i18n 债清偿 | T32=scripts/bench+BENCHMARKS；T34=engine api+console；零交集（单树串行执行） | L0+L1（受影响 -p）+push+CI 绿 |
| **R5-W1** | T33 DF 48→55.2.0 升级 | 引擎全树串行独占波（12 触面文件+golden+lock） | L0+L1 全量+L3 smoke --full+L4 前后对比+push+CI 绿 |
| **R5-W2** | T35 SSH 部署链 ‖ T36 谱系裁决卡 | T35 串行于 T33 后（同动 lock）；T36=纯 docs | T35：L0+L1+docker e2e+push+CI；T36：纪要落档 |
| **R5-W3** | T38 v1.1.0 收口（门槛=T32-T36 全 ✅） | publish/tag 不可逆→独占波 | §十一 R5 收口全绿（21×1.1.0+资产 9/9+收官报告） |

## 十一、R3/R4 收口标准（分轮验收：R3 全量基线 + R4 增量段）

```bash
cd ~/IdeaProjects/tailrace
cargo fmt --all --check && CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked          # 0 failed，不低于 R2 收官通过数
cargo deny check advisories bans licenses sources                  # 四门 ok
bash scripts/bench/macro.sh --rows 100000 --report /tmp/r3-final.md # 吞吐行存在（vs R2 24649-25246 对比）
docker compose up -d && ./scripts/smoke.sh --full && docker compose down -v   # 全链绿（OrbStack；kafka 端口与常驻栈冲突时 TAILRACE_KAFKA_PORT 覆盖）
CARGO_PROFILE_DEV_DEBUG=0 cargo build --locked --no-default-features --features connector-file,connector-doris   # T16 裁剪档
git push origin main && gh run watch <最新>                          # CI 七 job 绿
test -s docs/dev/R3-升格评审-*.md                                   # T18 纪要存在
```

### R4 收口标准（1.0 发布轮验收，叠加于 R3 全量基线之上）

```bash
cd ~/GLMProjects/tailrace
cargo fmt --all --check && CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked          # 0 failed；ignored 计数可见（T25 后 5 测转 ignored，总数守恒）
cargo test --doc --workspace --locked                              # 非 0 passed（T26 doc-test 起步，现状为 0 空跑）
cargo deny check advisories bans licenses sources                  # 四门 ok
bash scripts/bench/macro.sh --rows 100000 --report /tmp/r4-final.md # 吞吐行存在（vs R3 账本同机对比，跨机器不可比注记随行）
docker compose up -d && ./scripts/smoke.sh --full && docker compose down -v   # 全链绿（OrbStack）
# —— 发包与 release（R4 增量）——
for c in $(bash scripts/release-crates.sh --list); do curl -sf https://crates.io/api/v1/crates/$c | grep -o '"max_version": *"1\.0\.0"'; done | wc -l   # = 21（cargo publish 全树验证）
cargo install tailrace-node --version 1.0.0 --locked               # 注册表可装（console 按 T27 核查结论同验或登记 tarball 单渠道）
gh release view v1.0.0 --json assets --jq '.assets | length'       # = 9（8 tarball + checksums.txt，release 资产 9/9）
mdbook build                                                        # 文档站构建绿（T29）
grep -cE '\[!\[' README.md README.zh-CN.md                          # 各 ≥4（徽章矩阵）
# 全新机器三命令 quickstart 口径：干净环境（rustup+Node 22+pnpm，无缓存）按 README Quickstart 逐字执行
#   ./scripts/build.sh → ./target/release/tailrace-console … → ./target/release/tailrace-node serve …
# 三命令全成功；实测记录入 docs/dev/v1.0-release-<date>.md（T31 收官报告）
git push origin main && gh run watch <最新>                          # CI job 全绿（现役 8 job 口径，README.md:13；以 watch 实测为准）
test -s docs/dev/v1.0-release-*.md                                  # T31 收官报告存在
```

## 十二、R5 收口标准（叠加于 R4 基线之上，2026-10-23 立项）

```bash
cd ~/GLMProjects/tailrace
cargo fmt --all --check && CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked          # 0 failed，ignored 计数沿用 T25 后账本
cargo deny check advisories bans licenses sources                  # 四门 ok（DF/arrow 新版本重点）
bash scripts/bench/macro.sh --rows 100000 --report /tmp/r5-final.md                 # file→file vs T33 前后同机对比在档
bash scripts/bench/macro.sh --sink doris --rows 100000 --report /tmp/r5-doris.md    # Doris 吞吐行存在（T32）
test -s docs/dev/R5-谱系裁决-*.md                                   # T36 纪要存在
# T38（随轮发）：21×1.1.0 API 复核 + v1.1.0 release 9 资产 + 收官报告
```

R5 增补纪律：各卡不动 CHANGELOG（T38 统一入账）；禁改 tag 扩围至 v1.0.0。
