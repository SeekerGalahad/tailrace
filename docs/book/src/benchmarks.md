# 性能基准摘要

> 摘要档：只摘关键行，数字与注记与仓库 `docs/BENCHMARKS.md` 全文一字不差；微基准（§二）、优化前后账本（§四）、CI 口径（§五）不在此摘录。
> 全文（608 行）：<https://github.com/SeekerGalahad/tailrace/blob/main/docs/BENCHMARKS.md>

## 一、宏基准：file→file 全链（§三首批基线）

| 日期 | git | 档位 | rows | elapsed | 吞吐（rows/s） | 输入 | 输入 MB/s | sink 校验 |
|---|---|---|---|---|---|---|---|---|
| 2026-10-17 | `8e3b1fd` | **debug** | 100000 | 4.600 s | **21739** | 9.8 MB | 2.13 | 100000 行 ✓ |

吞吐行（机读）：`throughput=21739 rows/s（rows=100000, elapsed=4.600s, mode=debug, git@8e3b1fd）`

口径注记（§三方法学）：默认 **debug（未优化构建）** 档——debug 数字只作同机纵向对比，**不代表 release 性能**；吞吐含管道启动与 sink 落盘（fsync），不含 console/node 进程启动与种子写入。

## 二、R3 对比账本摘录（§六）

> 跨机器不可比注记（§六表前注原文）：「R3 起数字采自**第二台开发机**（下表），与 §一 Intel 机器的 R2 数字**跨机器不可比**（§一注记同样适用）；同一小节内的 A/B 均为同机同日交错复测。」——该表机型 Apple M4（10 核），16 GB，macOS 26.6.2（arm64）。

| 任务 | bench/口径 | 关键数字 | 日期 |
|---|---|---|---|
| T15 | macro-partitions 吞吐（kafka(P=6)→file **debug 档**，4 轮） | **M4 首档：中位 27861 rows/s**（23918 / 27678 / 28043 / 28474） | 2026-10-21 |
| T16 | node 二进制体积（**release 档**，裁剪档 vs 默认档） | 61,571,936 B → **55,788,080 B**，**−5,783,856 B（−5.52 MiB，−9.4%）** | 2026-10-21 |
| T15 | 动态分区发现延迟（testcontainers 真 kafka，topic 2→4 分区不重开） | subscribe 模式 **26.8 s** / assign 模式 **30.7 s** | 2026-10-21 |

> T15 吞吐口径脚注（§六脚注 ¹/⁵ 同样适用）：debug 档 stats 轮询步进 0.5 s、单轮全链仅 ~3 s → 量化误差 ±17%；跨机器对比（如 vs §4.5 的 24658，Intel i7→M4）不可比。

## 三、全文入口

- 仓库文件：`docs/BENCHMARKS.md`（§一 机器规格 / §二 微基准（criterion）/ §三 宏基准 / §四 优化前后账本 / §五 CI 口径 / §六 R3 对比账本）
- GitHub 全文：<https://github.com/SeekerGalahad/tailrace/blob/main/docs/BENCHMARKS.md>
