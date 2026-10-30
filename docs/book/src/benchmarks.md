# 性能基准摘要

> 摘要档：只摘关键行，数字与注记与仓库 `docs/BENCHMARKS.md` 全文一字不差；微基准（§二）、优化前后账本（§四）、CI 口径（§五）不在此摘录。
> 全文（748 行）：<https://github.com/SeekerGalahad/tailrace/blob/main/docs/BENCHMARKS.md>
> **口径限定**：下列数字均为开发机单机测量、除注明外为 debug 档——**跨机器不可比**，debug 档亦不代表 release 性能；v1.1.0 后四轮开发（扩展轮/R6/R7/R8）**未新增基准行**，本页数字即当前有效账本。

## 一、宏基准：file→file 全链（§三首批基线）

| 日期 | git | 档位 | rows | elapsed | 吞吐（rows/s） | 输入 | 输入 MB/s | sink 校验 |
|---|---|---|---|---|---|---|---|---|
| 2026-10-17 | `8e3b1fd` | **debug** | 100000 | 4.600 s | **21739** | 9.8 MB | 2.13 | 100000 行 ✓ |

吞吐行（机读）：`throughput=21739 rows/s（rows=100000, elapsed=4.600s, mode=debug, git@8e3b1fd）`

口径注记（§三方法学）：默认 **debug（未优化构建）** 档——debug 数字只作同机纵向对比，**不代表 release 性能**；吞吐含管道启动与 sink 落盘（fsync），不含 console/node 进程启动与种子写入。

## 二、对比账本摘录（§六，R3 起逐项追加、不删旧行）

> 跨机器不可比注记（§六表前注原文）：「R3 起数字采自**第二台开发机**（下表），与 §一 Intel 机器的 R2 数字**跨机器不可比**（§一注记同样适用）；同一小节内的 A/B 均为同机同日交错复测。」——该表机型 Apple M4（10 核），16 GB，macOS 26.6.2（arm64）。

| 任务 | bench/口径 | 关键数字 | 日期 |
|---|---|---|---|
| T15 | macro-partitions 吞吐（kafka(P=6)→file **debug 档**，4 轮） | **M4 首档：中位 27861 rows/s**（23918 / 27678 / 28043 / 28474） | 2026-10-21 |
| T16 | node 二进制体积（**release 档**，裁剪档 vs 默认档） | 61,571,936 B → **55,788,080 B**，**−5,783,856 B（−5.52 MiB，−9.4%）** | 2026-10-21 |
| T15 | 动态分区发现延迟（testcontainers 真 kafka，topic 2→4 分区不重开） | subscribe 模式 **26.8 s** / assign 模式 **30.7 s** | 2026-10-21 |
| T32 | macro 吞吐（file→**doris** 单管道·默认批，debug） | **33852 rows/s**；同日 file→file 锚点 33568；+0.8%（噪声界内持平）——大批下 doris sink 零可测开销（见 §6.4） | 2026-10-23 |
| T32 | 同上·小批探针（flushRows 500+pollRows 500，单管道） | **11017 rows/s**（200 次同步 stream load）；vs 大批 −67%；小批区无 group commit 的同步往返封顶 ≈45ms/次 | 2026-10-23 |
| T32 | 同上·P=4 并发小批（T18 ④T3 判据数据面） | **26781 rows/s**（50 flush/管）；单管小批 11017；**2.43×**（HTTP 往返重叠），仍 −21% vs 单管大批——收益缺口=group commit 目标区 | 2026-10-23 |
| T32 | 同上·P=4 并发·poll 地板批（名义 flush500，实际批 ≈6250） | **97752 rows/s**（4 flush/管）；单管大批 33852；**2.89×**——4 管道并行化全链；console 并发 4 管道无限制（未触发降 P=2 回退门） | 2026-10-23 |
| T33 | macro 吞吐（file→file debug，DataFusion 48.0.1→**55.2.0**） | DF48 同机锚点 **33568 rows/s**（§6.4，2026-10-23）；DF55 **41528 rows/s**（2.408 s）；**+23.7%**（同机**不同日**，非同日 A/B；单轮 debug 档不登记为确定增益）；回退门「宏基准回吐 >10%」**未触发** | 2026-10-24 |
| T33 | transform 微基准三项（criterion release，8000 行/批，DF 55.2.0） | **M4 DF55 首档**：identity **237.38 M** / filter **65.89 M** / groupby **53.47 M** rows/s；**vs §4.4 的 30.647 M/8.958 M/8.072 M 属跨机器（Intel i7→M4），不可比**；本组=该 bench 在 M4 的首个档位 | 2026-10-24 |

> T15 吞吐口径脚注（§六脚注 ¹/⁵ 同样适用）：debug 档 stats 轮询步进 0.5 s、单轮全链仅 ~3 s → 量化误差 ±17%；跨机器对比（如 vs §4.5 的 24658，Intel i7→M4）不可比。
> T32 口径脚注（§六脚注 ⁶）：`bash scripts/bench/macro.sh --sink doris [--pipes P] [--flush-rows R] [--poll-rows R] --rows 100000`——console 档口径同脚注 ¹，单轮单档（非多轮中位）；栈=docker-compose doris-fe/be 2.1.7 单 BE（BUCKETS 1）；行数守恒全档过（FE COUNT==100000）。
> T33 口径脚注（§六脚注 ⁸）：DF48 对照取 §6.4 同机锚点 33568（2026-10-23），**非同日**——同 bench 的 DF48 跨日漂移仅 0.3%，+23.7% 远在其外，回退门判定不受「不同日」影响；但单轮单档不登记为 DF55 的确定增益（release 档/多轮中位补测后方可谈增益量）。微基准**本机从未跑过 DF48 档**（target/criterion 本次首跑），故登记为「M4 首档」而非前后对比。

## 三、全文入口

- 仓库文件：`docs/BENCHMARKS.md`（§一 机器规格 / §二 微基准（criterion）/ §三 宏基准 / §四 优化前后账本 / §五 CI 口径 / §六 R3 对比账本）
- GitHub 全文：<https://github.com/SeekerGalahad/tailrace/blob/main/docs/BENCHMARKS.md>
