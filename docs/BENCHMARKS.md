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
| （待 T6 填） | 批大小分布指标 | — | — | — | — |
| （待 T7 填） | macro RSS/吞吐 | — | — | — | — |

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
