# 引擎协议 /v1

> 本章经 mdBook include 全文引入仓库正典 `docs/protocol/engine-v1.md`（**304 行**；2026-11-14 R12 W4 刷新 stats 节：字段全集对齐 `StatementCountersSnapshot`（含 A6 三项 `flushMsTotal`/`lastFlushDurMs`/`lastDataMs`），并注明 `sourceIdle` 尚未随任何已发布版本（crates.io 最新 1.2.0）上架。历史：2026-10-30 正典文档对齐轮同步 R7/R8 面——plan 表项 `options` 形态（G1）、`columns` 十六词词汇（G2）、`partial_columns`（T-A7）与 jdbc/dim 的 feature 注册实况；R10 增 badRowPolicy 六段 config 与 DLQ at-least-once 披露、R11 增 T-V2 行键唯一（`(statement, 行位置, rawB64)`、表唯一化计数不变）与 stats `sourceIdle`（T-V1）；仓库内原文：<https://github.com/SeekerGalahad/tailrace/blob/main/docs/protocol/engine-v1.md>）。

{{#include ../../protocol/engine-v1.md}}
