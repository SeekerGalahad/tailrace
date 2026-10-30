# 开发指针

> 本站只入对外档。`docs/dev/` 现为技术决策记录与 dogfooding 报告（两轮 dogfooding 实录、T33 归因），不入站；原 roadmap / 评审纪要 / 事故档等过程档已于 2026-10-26 移入私有工作流（公私仓分离），公开仓内不再可点开——[现状评估与未来展望](assessment.md) 章 v1.1.0 基线段对它们的引用作为当时的证据链原样保留。

- 路线展望：`docs/ASSESSMENT-AND-OUTLOOK.md` **§0.6（post-R8，当前有效）** 与 §五（v1.1.0 时点原文）——两段不一致处以 §〇 为准；本站 [现状评估与未来展望](assessment.md) 章全文引入。
- 变更日志：`CHANGELOG.md` — <https://github.com/SeekerGalahad/tailrace/blob/main/CHANGELOG.md>（发布日志；v1.1.0 后四轮能力尚未发包，见[前言](README.md)版本声明）。
- 本地开发：`make fmt lint test`（cargo fmt / clippy / test）、`make web`（pnpm install + build，`web/`）、`./scripts/dev.sh`（console debug + Vite 开发服务器，`/api` 代理）——与仓库 README「开发」同源。
- 端到端脚本（对真 Doris）：`scripts/e2e/ddl_create.sh`（建表闭环）、`backfill_replay.sh`（确定性回填重放）、`partial_update.sh`（部分列更新）、`type_precision.sh`（decimal 精度守恒 + `sequence_col` 定序）、`starrocks.sh`；两轮 dogfooding 在 `scripts/dogfood/`，报告在 `docs/dev/`。通过数为各轮落地时登记值，本站未重跑。
- 自检：`./scripts/smoke.sh`（默认 25 步断言、无外部依赖；`--full` 44 步经 docker compose 走 Kafka→Doris）。
