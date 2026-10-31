# 开放项有界调查：BE 内存窗口期「cursor 到 EOF 但 totals.flushed 少一行」（R10）

> 来源：dogfooding round3 #8（`dogfooding-round3-2026-10-30.md` §三）——run5（BE 内存
> 窗口期）在 resolved 后追加 DOC-5 再 stop：ledger `cursor=435`（EOF）但
> `totals.flushed=4`、表 4/5 行、60s 轮询不补。file→file 链路 5 轮定向复现（append→
> 0.15s 内 stop）全部一致，差异只剩 DorisSink 路径 + BE 内存窗口。
> 本文档 = R10 开放项有界调查交付：源码证据链分析 + DorisSink 路径定向复现尝试，
> 结论二选一（复现并定位 / 以证据关闭）。

## 一、源码证据链分析（本轮实读）

以下全部结论来自本轮对 `engine/` 的逐路径阅读，行号以本提交为基准。

### 1.1 ledger 的写入点恰好三处，全部在 flush 之后

serve 模式语句线程 `run_statement_serve`（`engine/crates/tailrace-runtime/src/lib.rs:819`）
中，`FileOffsetStore::save`（cursor + idempotency_key + totals 同一 `LedgerEntry`）只在：

| # | 位置 | 条件 | 前置 |
|---|---|---|---|
| S1 | `lib.rs:929` | idle-poll 且 `pending_rows>0` 且 flush_interval 到期 | `sink.flush()?` 成功 + `obs_flush(ack.rows)` 已把行计入 counters.flushed |
| S2 | `lib.rs:1034` | `flush_due(...)` 三阈值任一满足 | 同上 |
| S3 | `lib.rs:1068` | **stop finalization，无条件** | `pending_rows>0` 时先 `sink.flush()?`（**失败即 Err 返回、不 save**）；`pending_rows==0` 时直接 save |

S1/S2 在 save 之前已执行 `stats.flushed_rows += ack.rows` 与 `obs_flush`（counters 原子
累加，`lib.rs:169-172`），因此**这两处不可能产生「cursor 越过 N 行而 flushed 不含
N 行」的 ledger 记录**。S3 在 `pending_rows==0` 分支没有 flush——这就是唯一能写下
「cursor 越过某行但 flushed 不含该行」的 save 点，其必要条件是：**该行已被 poll
（source.cursor 已推进）但 runtime 侧 `st.pending_rows` 没有增加**。

### 1.2 `pending_rows` 的不变量与合法的「不增」路径

`st.pending_rows` 只在 `st.sink.write(out)?` 成功后按输出行数增加
（`lib.rs:993-996`）。poll 之后 pending 保持 0、cursor 照常越过的引擎内合法路径：

- **(a) 该行解码为 Bad（badRowPolicy=dlq）**：`decode_poll` 产出的 `merged` 不含
  Bad 行（好坏混合时好行照常合并，`lib.rs:470-482`）；`handle_bad_rows` 先把 raw
  bytes 落 DLQ 侧文件再放行游标（`lib.rs:238-253`，「宁可不过游标也不丢原始
  字节」），`counters.bad +1`，首个坏批必发 `DECODE_BAD` 事件（`lib.rs:256-277`）。
  flush 不经过这些行 → flushed 不加、consumed 加（consumed 在 decode 之前按
  chunk 数累加，`lib.rs:939-942`）。
- **(b) transform 过滤**（`out` 行数为 0，`lib.rs:992-995`）。
- **(c) 该 poll 全 Bad / 全空**：`pd.merged == None` → `continue`（`lib.rs:970-972`）。

### 1.3 DorisSink 侧不存在「flush Ok 但行未计入」的路径

`DorisSink::flush`（`engine/crates/tailrace-conn-doris/src/lib.rs:876`）对三种 Ok
结果——`Success`、`Publish Timeout`、`Dedup`（label already exists）——一律返回
`rows = self.pending_rows` 全额计入（`lib.rs:892-909`）；失败路径把 buffer 交还并
`bail!`（`lib.rs:933-939`），runtime 侧 `flush()?` 短路返回、**不 save cursor**。
特别注意：Dedup（重启重放撞 label）数据不重写但 **rows 照计**——它只会造成
「表少一行而 flushed 正常」，**不会**造成 flushed 少一行，与 run5 观测
（flushed=4）不符，可排除。

### 1.4 FileSource 半行与截断处理无窗口

文本模式半行不推进 `byte_pos`（缓冲到换行补全才推进，
`engine/crates/tailrace-conn-file/src/lib.rs:295-312`）；截断/轮转检测会 rewind 重读
（`lib.rs:260-276`）。parquet 模式整文件一批（`lib.rs:234-237`），本场景不适用。

### 1.5 ledger 写入原子、观测面直读 ledger

`FileOffsetStore::save` 为「读整个 map → 改该 statement 条目 → tmp+fsync+rename 原子
替换」（`engine/crates/tailrace-state/src/lib.rs:258-307`），cursor 与 totals 是**同一条
`LedgerEntry` 的原子写入**——观测到的 `(cursor=435, flushed=4)` 必然是某一次 save 的
完整结果，不是两半拼接。`GET /v1/pipelines/{id}/offsets` 直读该文件
（`registry.rs:668-686` offsets = ledger pass-through），无内存合成。

### 1.6 重启/双线程拼法逐一排除

- 重启退避以 1s 切片响应 stop（`registry.rs:497-501`）；打断后 re-assemble →
  `run_statement_serve` 首轮 `is_stopped` 即 break → finalization 以 **resume 原值**
  cursor 原样 save，无害。
- `stop_pipeline` 先 remove entry 再 `join`（阻塞到线程退出，`registry.rs:560-578`）；
  重推走 stop-old-start-new。若 stop 的 join 被长 flush 重试卡住且上层超时后立刻
  re-deploy，理论上新旧线程短暂并存（save 的注释也自认 concurrent writers 存在、
  last-writer-wins，`tailrace-state/src/lib.rs:292-294`），但任何线程的 save 组合都满足
  1.2/1.3 的不变量：**cursor 越过某行 ⟺ 该行已过成功 flush（计入 flushed）或走了
  Bad/过滤路径（不计入）**。双线程拼不出第三种组合。

### 1.7 与观测全量吻合的唯一引擎机制

run5 观测 = {cursor=EOF, flushed=4, 表 4/5 行, 60s 不补}。逐一对照：

| 假设 | cursor=EOF | flushed=4 | 表 4/5 | 判定 |
|---|---|---|---|---|
| DOC-5 走 **(a) Bad→DLQ**（或 (b) 过滤） | ✅ 照常越过 | ✅ 不计 | ✅ 不入表 | **唯一全吻合** |
| stop finalization flush 失败（MEM_LIMIT） | ❌ 停在 DOC-4 后 | ✅ | ✅ | 排除（cursor 不会到 EOF） |
| flush Ok 但 Doris 未可见（Publish Timeout 极端形态） | ✅ | ❌ flushed=5 | ✅ | 排除 |
| Dedup 撞 label | ✅ | ❌ rows 照计=5 | ✅ | 排除（见 1.3） |
| 半行/截断 | ❌ 不推进 | ✅ | ✅ | 排除（见 1.4） |

即：**若观测数字无误，唯一自洽解释是 DOC-5 在 run5 那次 poll 中走了 Bad→DLQ 隔离
（或被投影过滤）**——按 dlq 策略这是**正确行为**（行可从侧文件恢复），不是数据
丢失；且该路径会留下三个可查痕迹：`consumed=5`、bad 计数 ≥1、
`bad_rows/p{P_ID}-s0.jsonl` 追加 + `DECODE_BAD` 事件。**run5 现场未留存这些字段**
（evidence 目录只含最终 27/27 轮，`round3-node-events.log` 仅 1 行且属 df3-bad 管道），
无法回溯证实或证伪——这正是需要定向复现的原因。

## 二、DorisSink 路径定向复现尝试

> **证据归档状态（R10 评审修复轮如实标注）**：本节三次尝试的运行产物——本地 HTTP 代理脚本、
> 各次 node.log、`GET offsets` 原始 JSON、代理拒载日志——**均未归档入仓**；§2.2 的数字与时序是
> 执行者当场观测的记录，第三方**不能据仓内文件复核或重放**这三次尝试。这与 R9/R10 e2e 证据目录
> （`scripts/e2e/evidence/*`，脚本+日志+台账全落盘）的可审计标准**不对等**。因此本文档的「证据」
> 分两层读：§一 源码不变量分析（行号可核）+ 已归档的单测/e2e 落盘件 = **可独立复核**；§二 三次
> 定向复现 = **不可复核的执行叙述**，只作旁证、不作独立结论依据。§三 结论据此措辞。

### 2.1 设计

目标：在 **BE 拒载窗口**下重放 round3 run5 场景（file→doris，喂行后立即 stop），
比对 ledger cursor / totals.flushed / 表行数 / consumed / bad / DLQ。

- **窗口制造**：先在在役 BE（3.1.4）上实测 `exec_mem_limit` / `mem_limit` 头——
  3.1.4 已不按 header 限制单次 stream load 内存（4 组探测载荷全部 Success），不可
  用；遂采用**本地 HTTP 代理混合方案**：窗口关闭时代理把 stream load 逐字节直通
  在役 BE（真写真读真事务），窗口打开时代理返回 **round3 现场实录形态的
  MEM_LIMIT_EXCEEDED Fail 体**（`[MEM_LIMIT_EXCEEDED]…low water mark 400.00 MB`）。
  引擎的重试/退避/游标语义全部走真实代码路径，仅拒载来源是代理；系统内存零扰动
  （不在 7.8GiB 共享 VM 上人为制造内存压力——那是 round3 #1 的灾难场景）。
- **栈**：本仓 debug 二进制 console+node（含本轮 MEM_LIMIT retryable 改动），数据源
  streamLoadUrl 指向代理，Doris 表建在专用库 `tailrace_openitem`（收尾 DROP FORCE）。
- 三次尝试对应 round3 现场真实存在的三种时序形态（见各次记录）。
- 尝试期间在役 FE 曾第三次同形态半挂（mysql+http 双面超时），按纪律上报，由主会话
  `docker restart doris-2.1` 处置后 20s 就绪、库完好——每次尝试前后各探活一次。

### 2.2 执行记录（3 次预算用尽，全部完成）

**尝试 1（run5 精确形态）**：4 好行窗口关→flush 成功（label `_0`，ledger
`cursor=112/consumed=4/flushed=4`）→ **开窗**→追加 DOC-5→**0.15s 内 stop**→finalization
flush（label `_1`）按 1s/4s/16s 退避重试 3 次被拒耗尽→语句 Err。结果：ledger
**cursor=112**（DOC-4 之后，未到 EOF 142）、flushed=4、表 4 行、stop 200 STOPPED。
——与 §1.7 排除表第 2 行一致：**finalization flush 失败 → cursor 不越过**。

**尝试 2（run1 形态+毒数据+窗口中段 stop）**：文件含 4 好 + 1 坏行（非 JSON），
**窗口从部署前起开**，badRowPolicy=默认 dlq。部署→首轮 poll：坏行落 DLQ、4 好行
flush 被拒耗尽→语句 Err→重启退避→重放（**坏行第二次落 DLQ**）→再被拒……t=70s
（窗口仍开）发 stop→STOPPED。结果：**ledger 文件不存在**（cursor 从未 save——从未
有过一次成功 flush）、表 0 行、**DLQ 2 条**（同一坏行隔离两次：DLQ at-least-once
在 DorisSink 路径的重复记账实证，与 dogfooding #3 同构）、代理累计拒载 8 次。
——cursor 从不越过未 flush 的好行，at-least-once 重放边界保持。

**尝试 3（armor re-deploy 形态）**：round3 脚本真实做过的「stop→窗口关→同管道
re-deploy」。前段同尝试 1（4 好 flushed、DOC-5 finalization 被拒耗尽、cursor 停
112）→关窗→re-deploy→resume=112→重放 DOC-5→flush（label `_1`——**label floor 有效，
新 sink 序号从已提交 `_0` 之后起，不撞已提交 label**）→直通 BE 成功。结果：ledger
**`cursor=142`（EOF）+ `consumed=5/flushed=5/flushes=2`**、表 **5 行**。
——窗口过后的 armor 重放**零丢失零重复**，Dedup 丢行路径未被触发（floor 防撞实证）。

（另：尝试 1 首跑因 node 侧缺 `TAILRACE_DS_{id}` 凭据环境变量 deploy 502，修复栈
引导后完成——属环境装配，不是机制结果；三次机制尝试均完整执行。）

### 2.3 附带真链路实证（顺带钉住的本轮改动）

- **badRowPolicy 透传**（R10 ①）：尝试 1 的 config 带 `"badRowPolicy":"skip"`，node.log
  启动行打 `bad_row_policy=skip`——console DTO 补齐后端到端生效（dogfooding #4 的
  「仍是 dlq」不复存在）。**该项已不依赖本节叙述**：R10 评审修复轮把 skip / fail / 非法值
  三形态写进 `scripts/e2e/alert_flow.sh` B 段（12 步）并实跑 50/50，node.log 启动行、
  DECODE_BAD/STATEMENT_FAILED 事件原文、sink 内容、502 ENGINE_REJECTED 全部落盘——
  `scripts/e2e/evidence/alert-flow-e2e-2026-10-31-r10review/`（README 逐步对照）。
- **MEM_LIMIT retryable**（R10 ②）：三次尝试的拒载全部走
  `BE memory pressure (transient): Status=Fail Reason=[MEM_LIMIT_EXCEEDED]…` 的
  **retryable** 分类（node.log WARN 逐次可见 1s/4s/16s 退避），不再落入 fatal；
  窗口关闭后同 label 重发即成功（尝试 3）。可复核部分 = `tailrace-conn-doris` 的
  `is_memory_pressure` 拼写单测 `memory_pressure_classification_pins_doris_spellings`（正例为
  3.1.4 实录文本）与状态矩阵 ⑧/⑧b 用例；「活 BE 窗口内逐次退避」本身只有本节叙述。
- **DLQ at-least-once**（R10 ③）：尝试 2 的 2 条重复隔离记录是文档披露的又一实证
  （已归档的同构落盘件 = `scripts/e2e/evidence/variant-vector-e2e-2026-10-31/run1-dlq-p2-s0.jsonl`
  的 3 坏行 × 3 次隔离 = 9 条）。

## 三、结论：以源码证据链关闭（无缺陷定位，不复现；定向复现叙述未归档、仅作旁证）

关闭依据按可复核性分层：**主依据** = §一 源码不变量分析（行号可核）+ 已归档落盘件
（R10 评审修复轮 `alert_flow.sh` B 段：skip 管道的 ledger 落成 `cursor=EOF / consumed=3 /
flushed=2`，见 `scripts/e2e/evidence/alert-flow-e2e-2026-10-31-r10review/review-offsets-3-skip-ledger.json`
——这是 §1.7 推演的「Bad 路径越过游标、flushed 不计、consumed 多出坏行数」形态的第一份
落盘实证）；**旁证** = §二 三次 DorisSink 定向复现（run5 精确形态 / run1 重启风暴+毒数据 /
armor re-deploy）据执行叙述**均未出现**「cursor 到 EOF 但 totals.flushed 少一行」的组合，每次
cursor/flushed/表行数与 §一 不变量一致——但产物未归档，第三方不可复核。结合 §1.7 排除表，判定：

1. **引擎侧未发现「好行被游标越过而不入账/不入表」的窗口**。源码层：flush 失败（含 BE 内存
   拒载）时游标从不越过未确认的行（S1/S2 save 在 flush 成功之后、S3 flush 失败即 Err 不 save）；
   窗口过后的重放（含 stop→re-deploy armor）把行补写且 flushed/表行数一致；label floor 防撞有效，
   Dedup 路径不丢行——后三点在 §二 叙述中得到印证，但该印证不可独立复核。
2. round3 run5 的观测 {cursor=EOF, flushed=4, 表 4/5} 与引擎行为的**唯一自洽解释**
   仍是 §1.7 路径 (a)：DOC-5 在该次 poll 中被判 Bad 隔离进 DLQ（dlq 策略下的**正确**
   行为，行可恢复）或被投影过滤。该路径会产生的 ledger 形态**已有落盘件**（上述 r10review
   `offsets-3`：skip 与 dlq 在游标/计数上同构，仅侧文件有无之别）。但 run5 现场的
   `consumed`/`bad` 计数、DLQ 文件或 `DECODE_BAD` 事件未留存（evidence 目录只含最终 27/27 轮），
   **无法回溯证实或证伪**。次要可能：观测为操作者口头记录、原始 offsets JSON 未留存，读数时点
   与最终 ledger 的一致性无法复核。
3. **不做强行归因**：本项以证据关闭为「机制未明；引擎不变量在源码层成立、在一组可归档 e2e
   （skip 路径）下实证、在三次未归档的 DorisSink 定向尝试下据叙述未被推翻」。若再次出现同形
   观测，取证清单（按优先级）：`GET offsets` 原始 JSON 全文（cursor/totals 三项——`consumed`
   与 `flushed` 的差即过 Bad/过滤路径的行数）、`GET stats`（consumed/bad）、
   `bad_rows/p{P}-s{N}.jsonl` 尾部与 mtime、node.log 中该管道的 DECODE_BAD/FLUSH_OK/final flush
   行、Doris `SHOW TRANSACTION`/`SHOW PROC '/transactions/…'` 的 label 状态。收齐即可在
   「DLQ 隔离（正确行为）」与「新机制」之间一锤定音；**取证件须随 evidence 目录入仓**，不再
   以叙述代替。
