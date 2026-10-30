# Dogfooding round 3（2026-10-30 立项 / 2026-10-31 实跑）—— variant/vector 管道 + 告警真触发全链

> R9 W4。用 R9 落的三块面——T-A4 VARIANT 透传、T-A5 向量列（`list<float32[,dim]>`）、
> T-E2 告警（规则引擎/webhook/SSE/SLO）——对**在役** Doris 真跑一轮全链自用：A1 建
> variant+vector 混合表 → file 源管道写入嵌套 JSON 与向量数组 → Doris 子列/数组断言 →
> dim 守卫负例进 DLQ → 一条 lag 规则真 firing/resolved + webhook 收两条 POST。
> 全部结论来自本轮实跑，证据路径逐条给出；e2e 机器验证同轮全绿
> （`scripts/e2e/variant_vector.sh` 在役 3.1.4 外部模式 **46/46**、
> `scripts/e2e/alert_flow.sh` 纯本地栈 **38/38** 三连绿）。

## 一、对象与事实

| 项 | 实测 |
|---|---|
| 在役栈 | 容器 `doris-2.1`（FE）+ `doris-be`（BE）＝ **doris-3.1.4-rc02-7f5ba43de6**；本轮中 FE JVM 曾膨胀至 4.7GiB 后 mysql 面假死（running、非 OOM、零日志），经主会话裁决 `docker restart doris-2.1` 恢复（内存回落 1.35GiB，全部库完好）——见摩擦 #1 |
| 共享 VM 内存 | OrbStack 8GiB；FE+BE+kafka+langbot 同住，`MemAvailable` 全天在 230-560MB 摆动，BE 低水位线 400MB——间歇 `MEM_LIMIT_EXCEEDED` 拒载窗口的根源（摩擦 #1/#2） |
| 二进制 | `./scripts/build.sh` release（console 102MB / node 89MB），轮间冷编译 ~7 分钟 |
| dogfood 面 | `bash scripts/dogfood/round3.sh --keep`（27 步，最终版实测 **21s 全绿**、无交互噪声）；同轮 e2e 与探针矩阵路径见 §首 |
| 告警面 | console `TAILRACE_ALERT_INTERVAL_SECS=2`（生产默认 15s）；`GET /api/alerts` + `POST/DELETE /api/alerts/rules` + `/metrics` SLO 线 + mock webhook（python 单文件，录 POST 落 JSONL） |

## 二、操作实录（`scripts/dogfood/round3.sh`，27/27，21s）

```
D0  SHOW FRONTENDS 取真实版本 → 专库 tailrace_dogfood3 DROP FORCE+CREATE
D1  console（评估器 2s 拍）→ REST login → doris+file 数据源 → node serve ONLINE
    → mock webhook 起监听（127.0.0.1:18195）
D2  A1 建 docs_emb：[doc_id utf8 / meta variant / content variant /
    emb list<float32,3> / scores list<float32>] × duplicate —— draft 断言
    VARIANT×2 + `emb` ARRAY<FLOAT> COMMENT 'vector dim=3' + 裸 scores →
    execute → SHOW CREATE TABLE 钉活表；带 dim 声明的 evolve → 409 retype
    建议（摩擦 #5）
D3  【BE 写路径健康门】→ 管道 file jsonl → doris 全列部署 RUNNING → 3 行嵌套
    JSON+向量 → 子列断言（meta.source='api'、meta.verified→1、四层
    content['l1']['l2']['l3']）+ 数组逐元素断言（含 1e-07 的 f32 边值）
    → dim 守卫负例：G-1 合法行落库、B-1 [1,2] 进 node-state DLQ
D4  lag 规则（threshold=6s + webhook）→ /metrics SLO target 线与 lag 系列
    同标签对照 → pause 造真 lag → 4s 后 FIRING → 追加 DOC-4 + resume →
    2s 后 RESOLVED → stop → webhook 收该 episode 的 firing/resolved 两条
    （同 eventId）；空闲 re-fire 一条如实记录（摩擦 #6）→ DELETE 规则 204
D5  DROP DATABASE FORCE（在役栈其余库未触碰）
```

全文日志与 webhook 记录：`scripts/dogfood/evidence/round3-2026-10-31/`
（`round3-2026-10-31-27of27.log` + `round3-webhook.jsonl` + console/node/events
+ summary；密码字符串已剥离）。

## 三、摩擦观察（每条含复现，按优先级）

### #1 【HIGH，环境】共享 VM 内存面紧：BE 间歇 MEM_LIMIT 拒载 + FE JVM 膨胀至假死

- **现象**：BE 全天间歇对一切 stream load 回
  `Status=Fail Reason=[MEM_LIMIT_EXCEEDED]…sys available memory < low water mark 400MB`
  （VM MemAvailable 230-560MB 摆动；`process memory used` 甚至报负值）。窗口可长达
  20-40 分钟。更重的一次：FE（doris-2.1）JVM 从 4.2GiB 涨到 4.7GiB 后 mysql 面
  假死——容器 running、`OOMKilled=false`、近 30 分钟零日志、CPU 0.00%、`SELECT 1`
  15s 无响应；主会话裁决 `docker restart doris-2.1` 后 20s 恢复、内存回落 1.35GiB、
  全部库完好。
- **复现**：variant_vector run4（`evidence/variant-vector-e2e-2026-10-31/
  e2e-run4-env-memwindow.log`，P 档连建表查询都失败）；round3 run1/run8 的
  `PIPELINE_RESTART`×2 + `STATEMENT_FAILED`（`round3-node-events.log` 家族）；
  FE 假死现场为 escalate 时主会话核验。
- **处置（脚本侧装甲，不改引擎）**：两脚本加 `be_write_gate`（部署前用一次性
  probe 表裸 stream load 探写路径、等窗口过去 ≤6×10s）+ 一次性 stop→re-deploy
  装甲（毒数据判据不可等，见 #2）；FE 假死属在役容器，按纪律 escalate 由主会话处置。
- **建议**：R10 后考虑给在役 FE 显式 JVM 堆上限/容器 memory limit 与巡检项
  （`docs/ops/` 已有面板，可加 `MemAvailable` 视角）；操作手册写明
  「VM 内存紧时 BE 拒载是环境信号，先看容器内存再查管道」。

### #2 【HIGH，引擎候选】MEM_LIMIT 被归为不可重试 + 环境性连败被毒数据判据误杀

- **现象**：BE 的 MEM_LIMIT Fail 落进 DorisSink 的「Other Fail outcomes …
  deterministic」分支（`engine/crates/tailrace-conn-doris/src/lib.rs` 的
  `FlushError::fatal`）→ 语句错误 → 30s/60s 重启退避 → 连续 3 次无游标进展即判
  毒数据 terminal FAILED（`NO_PROGRESS_LIMIT=3`，`registry.rs`）。环境性内存窗口
  与真毒数据同判：一轮 e2e 里好数据的管道被窗口打成 FAILED，`T4.3a` 装甲
  stop+re-deploy 才救回。
- **复现**：`scripts/e2e/evidence/variant-vector-e2e-2026-10-31/run1-node-events.log`
  （2×restart 后成功，DLQ 3×3）；round3 run1（3 连败 → `STATEMENT_FAILED`）。
- **建议**：`Status=Fail` 文案含 `mem_limit/exhausted/Allocator` 归 retryable
  （或至少不进 poison 计数）；毒数据判据按「同类确定性错误」分桶而非纯次数。

### #3 【MEDIUM】DLQ 是 at-least-once：flush 失败重放会把同一坏行再隔离一次

- **现象**：bad 管道首批 2 好 3 坏 → DLQ 3 行 → flush 遇 MEM_LIMIT 失败 →
  重启重放 → DLQ 再 +3（cursor 未过坏行）→ 共 9 行、`tailrace_dlq_rows_total`=9，
  而唯一坏行只有 3 条。侧文件与指标计的是「隔离次数」不是「坏行数」。
- **复现**：`run1-dlq-p2-s0.jsonl`（9 行、ts 三组、cursor 相同）；
  `run1-offsets-2.json`（consumed=15）。
- **处置**：variant_vector.sh 的 T4.5-T4.7 断言改为「≥3 且去重集恰为 3 条种子行
  字节级匹配 + 指标=侧文件行数」；头注释写明语义。
- **建议**：操作文档明示 DLQ 消费方需按 rawB64 幂等去重；v1.3 若做 DLQ 回放工具
  要内置去重。

### #4 【MEDIUM，R10 候选】badRowPolicy 在 console 的 PipelineConfig 无字段——带上即被 serde 静默丢弃

- **现象**：引擎 `PlanOptions` 有 `badRowPolicy`（dlq/fail/skip，deploy 期严格校验），
  但 console 的 `PipelineConfig` DTO（reliability/recovery/tuning/groupPrefix/
  startOffset 五段）没有该键——config 里写 `"badRowPolicy":"fail"` 被反序列化
  **无声丢掉**，落库 config_json 与推给引擎的 plan options 都不见它。alert_flow
  run2 的 fail 管道因此仍是 `bad_row_policy=dlq`（node.log 铁证），「毒数据造
  FAILED」的设计落空。
- **复现**：alert_flow run2 `run2-node.log`（`pipeline_id=2 … bad_row_policy=dlq`
  尽管请求带 fail）；对照 `engine/crates/tailrace-catalog/src/plan_options.rs`
  与 `console/crates/tailrace-console/src/api/pipelines.rs:41`。
- **建议**：R10 给 DTO 补 `badRowPolicy` 透传（或 console 对未知 config 键告警——
  静默丢弃比报错更伤）；e2e 的 FAILED 注入现改用投影列序错（引擎确定性 bail，
  纯 file 链路，稳定）。

### #5 【LOW-MEDIUM】带 dim 向量列的 evolve 恒 409 retype 建议——误导性噪音

- **现象**：`emb list<float32,3>` 建表后 evolve 恒 `409 SCHEMA_CHANGE_UNSUPPORTED`
  ——dim 只活在 DDL COMMENT，DESC 折叠回 `array<float>` 带不回 dim，精确词汇比较
  恒 diff（vector probe README W4 注记②在本轮操作者路径上兑现）。每次 evolve 都要
  看一遍「假 diff」。
- **复现**：round3 D2.3（http=409）；variant_vector T1.4（`retyped=none` 的 409）。
- **建议**：evolve 的 diff 把 dim 视为 declaration-only（比较前剥 `,dim` 后缀），
  或 409 文案注明「向量列 dim 声明不参与比较」。

### #6 【LOW，语义澄清】空闲活管道的 lag 规则会合法 re-fire——值守与断言都要容忍

- **现象**：file 源处理完存量后 `last_data_ms` 冻结，lag 随墙钟增长——规则在最后
  一个数据 flush 后 ~6s 再次 firing，直到 resume/新数据/stop。这是 lag=数据新鲜度
  的**正确语义**，但「跑完就空闲」的批式管道会周期性 firing/resolved 震荡。
  多轮 e2e 的「恰 2 条」断言都输给过它；stop 与 6s 窗的竞速只有 ~2s 余量。
- **复现**：alert_flow run3（W1 n=3）、round3 run3-5（`firing,resolved,firing`，
  第三条 v=6.9=停机竞速窗口）；时序证据 `round3-webhook.jsonl`。
- **建议**：文档写明 lag 规则适配「持续有流量」的管道；批式场景建议用
  heartbeat_age/state_failed 或给 lag 规则配「静默宽限」参数（v1.3 候选）。

### #7 【LOW，跨波遗留】round2.sh 的 step 计时从第 2 步起恒 0

- **现象**：`local now; now=$(date +%s); local dt=$((now - TSTEP)); TSTEP=now`
  （赋值缺 `$`）——此后 bash 算术把裸 `now` 当变量名，`dt=$((now - now))` 恒 0。
  round2 的 summary 实测即如此：首步 1s、其余 23 步全 0s、wall 总计 11s
  （README 里「24 步 10s」的总量说法没错，逐步耗时全是假 0）。round3.sh 已修
  （`TSTEP=$now`，最终版逐步耗时真实：D3.2 3s、D4.4 firing 4s、D4.6 resolved 2s）；
  round2.sh 不在本波文件域，仅记录。

### #8 【OPEN，未确认】BE 内存窗口下「stop 前喂行」出现 cursor 越过未落盘行

- **现象**：round3 run5（BE 窗口期）在 resolved 后追加 DOC-5 再 stop：ledger
  `cursor=435`（EOF）但 `totals.flushed=4`、表 4/5 行、60s 轮询不补——游标越过了一行。
  **定向复现失败**：file→file 链路 5 轮「append→0.15s 内 stop」全部一致
  （sink 3 行=cursor=EOF，stop finalization 确认会 flush pending 后才存 cursor，
  `engine/crates/tailrace-runtime/src/lib.rs` stop finalization 块）。差异只剩
  DorisSink 路径 + MEM_LIMIT 窗口，未及复现（FE 已重启、窗口关闭）。
- **处置**：按未确认开放项记录；round3.sh 停止前不再喂行（注释载明）。R10 评审
  若要闭项：在 BE 内存窗口下对 DorisSink 重放同场景（喂行→立刻 stop→比对
  cursor/flushed/表行数）。
- **证据**：round3 run5 现场（offsets cursor=435/flushed=4/docs_in 5 行）；
  复现脚本结论见提交信息。

### #9 【流程】e2e/dogfood 脚本首跑 bug 家族（W1 注记④同款）+ 本机无 shellcheck

- **现象**：本轮三脚本首跑合计 8 处自身 bug：columnsJson 双反斜杠转义坍塌
  （两处，`\"[[\\\"id\\\"…` 令表登记静默变废）、step 文本内反引号触发命令替换、
  双引号串内反引号撕裂引号结构、`doris_wait_noop` 笔误、DLQN 行引号嵌套错、
  `3.1.4 GROUP_CONCAT 无 ORDER BY…SEPARATOR`、`-u root`（缺冒号）触发交互密码
  提示污染日志、deploy 响应无 `.status` 字段误读。全部首跑暴露、逐个修掉。
- **建议**：CI 加 `shellcheck scripts/**/*.sh`（本机未装）；e2e 脚本的
  columnsJson 一律变量承载（本轮惯例已固化）。

## 四、正反馈（同样来自实跑）

- variant+vector 混合表**一次通**：A1 草稿/活表类型/dim COMMENT 全对，嵌套 JSON
  子列与向量逐元素（含 `1e-07`、f32::MAX）在引擎链路上逐值精确；
- 告警链**极稳**：最终版 round3 全轮 21s，pause→firing 4s、resume→resolved 2s、
  webhook 两条同 episode 无一漏发；SLO target 线与 lag 系列同标签对照即开即用；
- dim 守卫**不误杀**：坏行前后夹好行，落库值与各自 id 精确对齐；DLQ rawB64
  字节级可回放；
- 载体抓取（variant_vector C 档）证明 variant 透传确未解析叶子：整段子文档以
  JSON 字符串到场，嵌套对象+内数组结构相等——评审重点项落了实证。

## 五、反哺 R10 排期（按本轮证据排序）

| 优先 | 项 | 来源 |
|---|---|---|
| R10-1 | badRowPolicy 补进 console PipelineConfig（或未知 config 键告警） | 摩擦 #4 |
| R10-2 | MEM_LIMIT 归 retryable / poison 判据分桶 | 摩擦 #2 |
| R10-3 | evolve diff 把向量 dim 视为 declaration-only | 摩擦 #5 |
| R10-4 | 文档：lag 规则的持续流量前提 + DLQ at-least-once 去重须知 | 摩擦 #6/#3 |
| R10-5 | CI 加 shellcheck（脚本首跑 bug 家族的机器防线） | 摩擦 #9 |
| 开放项 | BE 内存窗口下 stop 前喂行的 cursor/flushed 不一致（DorisSink 复现） | 摩擦 #8 |
| 运维 | 在役 FE JVM 堆上限/容器限额巡检（主会话已重启一次） | 摩擦 #1 |

## 六、未做 / 边界

- kafka 源、MCP 工具面、SSE 流（`/api/alerts/stream`）未在本轮 live 覆盖——
  SSE/webhook/规则的契约已有 `tests/alerts_flow.rs` 6 项钉住，live 只走 webhook 路径；
- compose 2.1.7 栈未起（VM 内存面紧，双栈同跑必然互挤——round2 摩擦 #5 的复发
  预防）；两脚本的 compose 分支保留、按 live-gate 空过；
- `heartbeat_age`/`dlq_window`/`flush_errors_window` 三类规则未 live 真触发
  （dlq 计数在 variant_vector T4.7 有指标级钉子；flush error 系列引擎尚未发射，
  见 alerting.rs 模块注释）；v1.3 候选；
- 摩擦 #8 保持 OPEN：file-sink 定向复现 5/5 一致、机制未明，闭项需 BE 内存窗口
  下的 DorisSink 复现（本轮 FE 重启后窗口关闭，不再主动制造）。
