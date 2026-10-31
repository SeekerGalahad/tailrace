# R10 评审修复轮 · alert_flow e2e 取证（2026-10-31，badRowPolicy 非默认值端到端）

R10 独立评审 medium #1：a435afe 给 console `PipelineConfig` 补了 `bad_row_policy` 字段，契约测试
`pipeline_config_wires_every_plan_options_key` 钉住 DTO 键集 == 引擎 `PlanOptions` 键集——但那只钉
serde 往返；「引擎真按 fail/skip 行事」在仓内没有任何落盘证据，也没有任何 e2e/dogfood 脚本设置过
非默认值（此前 evidence 目录全部命中均为 `bad_row_policy=dlq`）。dogfooding round3 恰是靠**实跑**
非默认值才发现 serde 静默丢弃，修复后不实跑同类盲区仍在。

本目录 = 修复：`scripts/e2e/alert_flow.sh` 新增 **B 段**（12 步），在规则面清空之后把 skip / fail /
非法值三种形态推过 console → `config_json` → `plan["options"]` → 引擎 `BadRowPolicy::parse_strict`，
断言的是**引擎做了什么**，不是 DTO 里有什么。同次运行覆盖原 38 步（R10 复跑门语义不变）。

载体：file→file 全本地栈（不需要 Doris），`target/release` 双二进制（a435afe 后无产品代码改动，
与 `-r10recert` 同一构建）。命令：`bash scripts/e2e/alert_flow.sh --keep`。

## 结果：**50/50**（`e2e-50of50.log`，首跑即绿）

| 步 | 形态 | 引擎侧证据（本目录文件） |
|---|---|---|
| B1.2 | skip 透传 | `review-node.log` 管道 #3 启动行 `bad_row_policy=skip`（tracing 字段，引擎自己打的） |
| B1.3 | skip 行为 | `review-bp_skip-sink.jsonl` 恰两好行 S-1/S-2，中间坏行被丢；`tailrace_pipeline_state=0` 非 FAILED |
| B1.4 | skip ≠ dlq | 工作目录**无 `bad_rows/` 目录**（skip 不建隔离文件）；`review-node-events.log` 第 2 行 `DECODE_BAD … badRowPolicy=skip (rows dropped, counted only)` |
| B2.2 | fail 透传 | `review-node.log` 管道 #4 启动行 `bad_row_policy=fail` |
| B2.3/B2.4 | fail 行为 | `tailrace_pipeline_state=3`；`review-node-events.log` 第 3 行 `STATEMENT_FAILED … decode bad row under badRowPolicy=fail (statement halted before the cursor crossed the batch) — first bad row sample: not json {` ——引擎原文，证明走的是 fail 分支而非 F 段的投影错序 |
| B2.5 | fail 越批前即止 | 同批好行 X-1 **未**落 sink（`bp_fail` 文件不存在）；且 `offsets-4.json` 不存在——该语句从未成功 flush，从未 save 游标 |
| B3.1 | 非法值 | `badRowPolicy="drop"` 过 create/publish（console 只携带），deploy 被引擎拒 → **502 ENGINE_REJECTED** 响应体点名 `options.badRowPolicy`（严格解析，不静默退回 dlq） |

## 顺带钉住的开放项形态（`review-offsets-3-skip-ledger.json`）

skip 管道（#3）stop 后的 ledger：`cursor.byte_pos=37`（= 源文件 EOF）、`totals.consumed=3 / flushed=2 /
flushes=1`。这**正是** `docs/dev/open-item-cursor-flush.md` §1.7 推演的「cursor 到 EOF 但 flushed 少
一行」的合法形态——坏行走 Bad 路径（这里是 skip；dlq 路径同构）游标照常越过、flushed 不计、
`consumed` 比 `flushed` 多出正好是坏行数。开放项结论 3 取证清单里要的 `consumed`/`bad` 计数痕迹，
本文件是第一份落盘件。

## 时态清理（同提交）

脚本头注与 F 段注释此前仍写「badRowPolicy 在 console 的 PipelineConfig 无字段——带上会被 serde 静默
丢弃」（a435afe 前的事实），已改为：F 段**刻意**用投影错序造 FAILED（告警语义不依赖坏行策略），
非默认 badRowPolicy 由 B 段专门覆盖。

`review-console.log` 中 e2e 临时栈的一次性 bootstrap 初始密码已打码（栈跑完即 `rm -rf`，非真实凭据）。
