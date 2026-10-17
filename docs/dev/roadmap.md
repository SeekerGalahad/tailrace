# tailrace 开发路线正典（roadmap）

> 本文件是 tailrace 开发的**唯一任务正典**：所有开发任务先在此立项（任务卡），再按波次执行；
> 执行必须围绕本文件与文末的最终目标。状态标记：☐ 待做 ｜ ◐ 进行中 ｜ ✅ 完成。
> 基线：v0.1.0（2026-10-16 首次公开，CI 六 job 全绿，release 9/9 资产）。

## 一、最终目标（北极星）

**v1.0：一条 SQL、两个二进制、零 JVM 的数据接入引擎，管理台内置，Doris/StarRocks 写入语义完整。**

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

1. 工作目录固定 `~/GLMProjects/tailrace`；开工先读本文件对应任务卡与「做得好的红线」；
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

**R3 亮点特性（~3-4 周）**：StarRocks sink 完整化（现仅注册别名，共享 Stream Load 协议）；JDBC 批源（MySQL/PostgreSQL，cursor_semantics 登记轮询位点；验收=新增连接器零改动 runtime/api/parser 本体）；discover / infer-schema 落地（替换 501 占位，Kafka/Doris 元数据客户端）；分区并行方案 b（split 化+动态分区发现，动工前先写部分分区推进的指纹回归测试）；feature 裁剪验证（`--no-default-features --features connector-file,connector-doris` 跑通 file→doris）；选做：SSH 部署链八步、jemalloc（RSS 指标先就位）、parser/decode fuzz 起步。**R3 末升格评审**：deltalake sink / DataFusion 升级 / 事务接口 / group commit / 本地两步 WAL 五项，默认结论=排 v1.x，纪要落 docs/dev/。

**R4 1.0 发布（~1.5-2 周）**：README 双语定稿（徽章矩阵+三命令 quickstart+连接器矩阵+FAQ）；mdBook 文档站骨架（协议/连接器/架构；设计文档以脱敏干净版重新入仓）；crate 元数据补齐 + doc-test；crates.io 发包（`tailrace-node` + `tailrace-console` 两 bin + `tailrace-connector` trait 契约面）；tag v1.0.0 → release（9 资产）；基准报告入 README；i18n 最小动作（错误码字典化接口预留，中文单轨明示）。

## 八、开放问题（随轮裁决）

1. `utoipa-swagger-ui 10.0.1` 锁定的 `zip 8.6.0`（仅 build-dep，构建脚本解压 crate 自带的 vendored swagger-ui dist）：2026-10-13 后被上游 yank（现又恢复可装，deny.toml `yanked="warn"` 保留）；2026-10-17 RustSec 连发 RUSTSEC-2026-0367/0368/0369 三个 extract 漏洞（修复仅入 zip 9.0.x，`^8.6` 不可达且无 8.x backport；build-dep 威胁模型不适用——不链接进产物二进制、不解压外部归档）——deny.toml 已 ignore 三条带跟踪注释。其发新版（zip 9 线）后回收 warn+ignore 并重新收紧；
2. OTLP 的 `opentelemetry` 0.33 / `tracing-opentelemetry` 0.34 版本锚定后是否升格默认开启（当前 feature 门控默认关）；
3. R3 升格评审五项的门槛数据（本 R2 的 BENCHMARKS.md 即门槛输入）；
4. CodeMirror6 替换 monaco 评估（编辑器体积档位，R3 裁决；T9 实测 monaco gzip 883kB vs CM6 预期 ~200-300kB 级，2026-10-17 立项）。
