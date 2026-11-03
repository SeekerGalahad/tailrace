# Dogfooding round 4（2026-10-31 立项 / 2026-11-02 实跑）—— MCP apply 走 GitOps 全程 + SLO 历史 + R11 新能力 e2e 重认证

> R11 W5。用 R11 落的三块面——T-V3 MCP apply 对（14→16 工具）、T-V4 SLO 历史
> （p50/p95/p99 采样环）、T-V1/T-V2（idle 感知告警 + DLQ 写入去重）——按操作者
> 日常再走一轮自用：一份声明文件经 **MCP 工具面**（非 REST）走完 dry-run diff →
> apply → 幂等二连 → GitOps 修改 → orphan/缺省语义探针 → 审计回读；SLO p95 以
> 样本数支撑读回；同时把 alert_flow e2e 按 R11 语义重写并三连跑绿。全部结论来自
> 本轮实跑，证据路径逐条给出；机器验证同轮全绿（`scripts/e2e/alert_flow.sh`
> 纯本地栈 **69/69**、`scripts/dogfood/round4.sh` 本地栈 **36/36**）。

## 一、对象与事实

| 项 | 实测 |
|---|---|
| 栈 | 本地 file→file 栈（console `--mcp-allow-write` + engine node serve）；**在役 Doris 本轮不在路径上**（见 §六），Docker 可达与否不影响两脚本 |
| 二进制 | `./scripts/build.sh` release（console 102MB / node 89MB，1.2.0 版本号——v1.2.0 发包仍是用户保留决策，未动） |
| e2e 面 | `bash scripts/e2e/alert_flow.sh --keep`：**69/69**（50 步旧版 + R11 新增 19 步；基线见摩擦 #1） |
| dogfood 面 | `bash scripts/dogfood/round4.sh --keep`：**36/36，30s**（声明文件 v1/v2 + 两个探针全部走 MCP JSON-RPC 面；12 次 tools/call 逐条留痕） |
| MCP 面 | `initialize`（session id + instructions 列出 16 工具）→ `tools/list`（apply W / apply_dry_run R，描述 OpenAPI 同源带 `POST /api/apply` 路由前缀）→ `tools/call`；写闸门 `--mcp-allow-write` × `confirm:true` 双重确认，缺 confirm 实测 `-32602 CONFIRM_REQUIRED` |
| SLO 面 | 评估器 2s 一拍；`GET /api/slo?pipeline=1` → p50=0.005 / p95=0.023 / p99=0.023，samples=3 起步、排空期累计 10；`/metrics` 侧 `tailrace_slo_lag_p95_seconds` 与 `tailrace_slo_lag_samples`、`tailrace_slo_lag_target_seconds` 同标签对照 |
| 吞吐事实 | pollRows=1 + flushRows=1（行级 durability）下 120000 行排空 **22s ≈ 5454 行/s**（≈0.18ms/行）；同文件去掉 flushRows 后 60000 行 **4s** 排完（≈15000 行/s，仅 6 次 flush）——per-row flush 的 fsync+游标落盘就是每行 ~0.11ms 的差价 |

## 二、操作实录（`scripts/dogfood/round4.sh`，36/36，30s）

```
S0/D1 双二进制 → console --mcp-allow-write（日志「write tools ENABLED」）→ REST
    login → node serve ONLINE
M0  initialize（serverInfo tailrace-console + session）→ tools/list：16 工具
    9R+7W，apply 对描述路由前缀同源
G1  声明文件 v1（1 file 源 / 2 表 / 1 管道，tuning pollRows+flushRows 声明在文件里）
    → apply_dry_run：summary create=4 全 create、id=null、零副作用（store 空集）
    → apply 无 confirm → -32602 CONFIRM_REQUIRED
    → apply confirm:true → create=4 == dry-run 同一 plan()，id 落定
    → 读回 DRAFT、dsMapping 名→id 解析、tuning 逐字携带
    → 再 dry-run：unchanged=4 收敛 → 再 apply：summary 全零、updatedAt 字节不动、
    generation 不 +1（幂等二连）
G3  MCP deploy_pipeline 于 DRAFT → isError 400「管道尚未发布」（摩擦 #3）
    → REST publish 一发 → 回 MCP deploy → ok=true → RUNNING
S2  120000 行积压先落盘再部署 → /api/slo p50/p95/p99 有值有样本数（排空期非 idle
    拍才采样）→ /metrics p95+samples+target 同标签 → 排空 22s/5454 行/s
    → 排空后 idle=1，环内 10 样本保留（1h），静默期不再累积
G4  v2 改 checkpointMs 2000→5000 → dry-run 恰一条 update，changes=[{field:
    checkpointMs, from:2000, to:5000}] 字段级 diff → apply → PENDING_CHANGES
    （RUNNING 管道本体不动）→ 再 apply 收敛 unchanged=4
G5  orphan 探针：tables 节只点名 orders_in → orphans 报 orders_out（只报不删，
    total 仍 2）；缺省 config 探针：dry-run 明示 config {…} -> null（导出丢
    config 会在 apply 时抹掉 tuning——文档已警示的锐边）
A1  sqlite3 回读审计：MCP_INITIALIZE + 恰 12 条 MCP_TOOL_CALL（含被拒的 apply），
    恰 2 条 APPLY 汇总行（v1 建 + v2 改；两次 no-op 重放**零写零审计**），
    PIPELINE_CREATE/UPDATE/DEPLOY 全部 actor=mcp:dogfood-r4，CREATE 带 via=apply
D9  stop + 删规则收尾
```

全文留痕：`scripts/dogfood/evidence/round4-2026-11-02/`（36of36 日志 + MCP
请求/响应 JSONL 双向 + console/node 日志 + summary；初始密码已剥离）。

## 三、摩擦观察（每条含复现，按优先级）

### #1 【HIGH，流程】R11 W1-W4 落了 T-V1/T-V2，但没有重跑 e2e——alert_flow 的造 lag 前提被 T-V1 直接推翻

- **现象**：本波开工先跑存量 `alert_flow.sh` 基线：**40/50**，`L1.1 firing
  state=inactive` 起 10 步连锁失败。根因是设计使然：旧脚本用 **pause 一个已到
  EOF 的 file 源**造 lag——T-V1 之后该源 `tailrace_pipeline_source_idle=1`（暂停
  期间判据冻结在上次 poll 的 EOF），console 对 lag 规则 **hold（不 firing）**，
  于是旧断言「pause→firing」永不成立。W1 提交信息只列了 fmt/clippy/test 三门，
  e2e 未跑（R10 曾同轮重认证过，R11 四波全没跑）。
- **修复（本波，e2e 语义升级而非放宽断言）**：造 lag 改为操作者语义的**真积压**
  ——sink 路径换成目录（FileSink 每次 flush 重开目标文件）→ 首个积压行 flush
  即败 → 语句错误进 20s 重启退避，退避期判据冻结在 **idle=0（有积压）**、
  last_data_ms 停在故障前 → lag 越 6 → **firing**；退避窗内修复 sink → 重启
  排空积压（非 idle、lag≈0）→ 同一 episode 真 Clear → resolved；排空回 EOF 后
  lag 再爬升 → **hold 不再 re-fire**（round 3 #6 的震荡在此根除）。idle 场景
  （EOF 静默、暂停静默、新数据恢复）独立成 I 段正断言。
- **复现**：基线 `evidence/alert-flow-e2e-2026-11-02-r11w5/baseline-pre-revision-
  40of50.log`（L1.1 起连锁）；重写后 `e2e-69of69.log`（I1.1–W5 全绿）。
- **建议**：改告警/指标语义的波，机器门必须含 alert_flow e2e（写进发包预检或
  CI）；「unit+contract 全绿」不覆盖「e2e 前提被推翻」这类语义级破坏。

### #2 【MEDIUM，语义】T-V1 的 hold 会遮蔽 Clear：积压在一个评估拍内排空时 episode 卡 firing

- **现象**：真实积压开的 lag episode，靠排空期的**非 idle Clear 拍**收口。但
  file 源排空即 EOF→idle=1——若积压在两个评估拍之间排完（本机 0.07-0.3ms/行，
  几千行就是亚秒级），Clear 拍不存在，episode **卡在 firing** 直到下一行数据、
  stop 或删规则。run2 实锤：3000 行积压 <1s 排完（SLO 环里排空期样本 0 个），
  L4.1 等 80s 不 resolve；同一判据放大到 40000 行（排空 ~8s 跨 ≥3 拍）后 L4.1
  2s 即 resolve。语义争议点：**idle=1 且 lag≈0**（刚有数据落盘、位置上无积压）
  的拍按现行代码仍是 Unknown——而「刚落过盘」恰是最强的 Clear 证据。
- **复现**：`evidence/alert-flow-e2e-2026-11-02-r11w5/run2-65of69-backlog3000.log`
  （L4.1/L4.2/W1/W3 四步失败；SLO 环 10 个样本全部来自 20s 退避期、排空期 0 个）；
  对照 `console/crates/tailrace-console/src/alerting.rs` 的 `threshold_verdicts`
  （hold 优先于 Clear）。
- **建议**：lag 规则的 hold 收窄为「idle=1 **且** value>threshold」；idle=1 且
  value≤threshold 判 Clear（数据刚落盘即清账）。kafka 源受同样影响（追平后
  idle=1、lag 事实上≈0，episode 挂到下一条消息）。R12 候选。

### #3 【MEDIUM，接口】纯 MCP 的 GitOps 断在 publish：≤16 预算里没有它的位置

- **现象**：`apply` 建的是 DRAFT；紧接着 `deploy_pipeline` 实测
  `isError http 400「管道尚未发布（无 spec 快照），先执行发布」`。发布只有
  REST `POST /api/pipelines/{id}/publish`（会话鉴权），MCP 面没有 publish 工具
  ——16 工具预算已被 apply 对用满。一个只拿 MCP 面的 GitOps agent 的部署闭环
  必须回落 REST + console 登录态。
- **复现**：round4 G3.1（36of36 日志原文）；`console/crates/tailrace-console/
  src/mcp.rs` TOOLS 表（16/16 满）。
- **建议**：三选一——①deploy 工具在 DRAFT 且无未发布变更语义冲突时自动
  publish-then-deploy（一次工具调用收口）；②17 工具预算裁决（破坏「≤16」的
  R8 承诺，需明示）；③apply 增 `autoPublish` 选项（声明文件表态，服务端收口）。
  操作手册先行注明「MCP GitOps 目前止步于 DRAFT+deploy，publish 走 REST」。

### #4 【LOW】表对象读写路径形状不一致：POST `/api/datasources/{id}/tables`，GET 却是 `/api/datasources/tables?datasourceId=`

- **现象**：按 REST 直觉 GET 同路径拿表清单 → **405 Method Not Allowed**
  （allow: POST）。清单在 query 参数形态的路径上。round4 首跑 G5.2 撞了一次
  （405 的空 body 让断言读成空串，报错信息也不指路）。
- **复现**：round4 run2 G5.2 FAIL 行；`console/crates/tailrace-console/src/lib.rs`
  路由表（`/{id}/tables` 仅 POST，GET 在 `/tables`）。
- **建议**：GET 也挂到 `/{id}/tables`（同路径双方法），旧 query 路径保留；
  或 405 响应 body 里带上正确 URI。

### #5 【LOW，脚本自伤】本轮脚本首跑 bug 家族 + alert_flow 存量 wait_hook 形参错位（R9 起每次白等 60s）

- **现象**：round4 首跑 3 处自伤——`step()` 尾行 `[ -f summary ] && …` 在文件
  不存在时泄漏 rc 1，令所有 `cond && step pass || step fail` **双分支连跑**（ok
  与 FAIL 同时落行）；jq `// empty` 吞字面 `false`（isError，round 3 曾栽过同
  款）；MCP 信封把 REST body 经 `serde_json::Value` 重排键序，字符串比对
  summary 需 `-S`。另修出 alert_flow 一处**存量 bug**：`wait_hook` 注释写
  `<count> [tries]`、函数体却读 `$2/$3`——`wait_hook 1 15` 实际等 **15 条**
  通知、默认 30×2s=60s 超时后才返回真实条数。R9/R10 run 笔记里的「60s 神秘
  间隙」（曾归因为「脚本侧时序」）就是它；本轮 run1 里这 60s 还把 sink 修复
  推过 20s 退避窗、三连败打穿 no-progress 上限成 FAILED。
- **修复**：全部在本波两脚本内修掉（step 尾行 `return 0`、`// false | tostring`、
  `jq -cS`、wait_hook 改读 `$1/$2` 并在注释里记下 R9 以来的影响面）。
- **建议**：CI 加 `shellcheck`（round 3 #9 旧账仍在）；e2e 断言对 jq 布尔一律
  `| tostring`。

### #6 【观察，语义边界】file 源的 SLO 形状：EOF 即 idle → 采样环只记排空窗口，批式 file 管道常态无样本

- **现象**：T-V4 一致性裁决「idle 不采样」（防静默期把 p95 拖到小时级）对
  kafka 连续流是本能，但 file 源**每次 poll 读到 EOF 就 idle=1**——只有积压
  排空期才进环。round 4 靠 12 万行积压 + 行级 flush 撑出 22s 观察窗、10 个
  样本；一个跑完存量就静默的批式管道，`GET /api/slo` 常态空集，「健康」与
  「无数据」在 SLO 面不可区分（要靠 `/metrics` 的 idle gauge 补读）。
- **复现**：round4 S2.5（排空后环内样本冻结在 10）；alert_flow run2 的 SLO 环
  里排空期 0 样本（见 #2）。
- **建议**：文档明示「SLO 面的适用对象是持续有流量的源（kafka/常驻 tail）」；
  批式场景读 idle gauge + heartbeat_age。或 v1.4 考虑 idle 期记 0 样本的
  「健康心跳」型序列（与 lag 分开命名）。

### #7 【正反馈，闭项确认】round 3 的 #3（DLQ at-least-once 重复隔离）与 #6（空闲 re-fire）在 e2e 断言下双面钉住

- DLQ 重放风暴（sink 为目录、退避 [1,8] 重放同一批）：`tailrace_dlq_rows_total`
  达 6（3 坏行 × 2 次尝试）时侧文件仍恰 3 行；修复后两好行恰落一次、非
  FAILED；终态指标=3×尝试数、文件=唯一坏行集、rawB64 逐字节回放三行原文
  （alert_flow D 段 11/11 步）。空闲活管道 lag 再越 6 不再 re-fire（W3 步：
  全程恰 2 条通知）。round 3 提出的「DLQ 消费方需按 rawB64 幂等去重」须知，
  现在引擎侧已代偿。

## 四、正反馈（同样来自实跑）

- MCP apply 链**一次通**且全程未碰 REST（除 publish）：dry-run 零副作用字节级
  成立（store 空集 + 审计只多信封行）、apply 落的 summary 与 dry-run 同源同值、
  幂等二连零写零审计（updatedAt/generation 字节不动）；
- 字段级 diff 的可读性好：v2 一条 `checkpointMs 2000→5000` 变更一眼可审；
  orphan 只报不删、缺省 config 显式 `-> null`，声明文件的锐边都在 dry-run 里
  预演得到，不是 apply 后才发现；
- 审计面完整得可以当操作日志用：12 次 tools/call 逐条 MCP_TOOL_CALL（含被拒
  调用），域行（CREATE/UPDATE/DEPLOY）全带 `mcp:dogfood-r4` 归因，no-op 不落
  APPLY 行——「谁经什么面改了什么」一查便知；
- 真 lag 告警链在 2s 拍下依旧脆快：sink 故障→firing 4s（含 20s 退避冻结判据
  的窗口）、修复→resolved 3s、webhook 全程恰 2 条；
- e2e 重写后 69/69：idle 三段（EOF 静默/暂停/新数据）+ 真积压 firing/resolved
  + DLQ 去重双断言 + SLO 交叉核验（p95 有样本支撑）一屏可述。

## 五、反哺 R12 排期（按本轮证据排序）

| 优先 | 项 | 来源 |
|---|---|---|
| R12-1 | lag 规则 hold 收窄：idle=1 且 value≤threshold 判 Clear（防 episode 卡 firing） | 摩擦 #2 |
| R12-2 | 改告警/指标语义的波，机器门纳入 alert_flow e2e | 摩擦 #1 |
| R12-3 | MCP GitOps 的 publish 缺口（deploy 自动 publish / apply autoPublish / 预算裁决三选一） | 摩擦 #3 |
| R12-4 | 表清单 GET 同路径 405 → 双方法或 405 带 URI | 摩擦 #4 |
| R12-5 | CI shellcheck（round 3 #9 旧账 + 本轮 #5 家族） | 摩擦 #5 |
| 文档 | SLO 面的持续流量前提 + 批式读 idle/heartbeat 的指引（grafana-dashboard 已有 SLO 行） | 摩擦 #6 |
| 运维 | per-row flush 的成本可见性（≈0.11ms/行差价）：flush 耗时直方图已有，操作手册补「行级 durability 的吞吐代价」一节 | §一 吞吐事实 |

## 六、未做 / 边界

- **在役 Doris 不在本轮路径**：round 4 主题是控制面（MCP/SLO）与告警语义，
  file→file 栈即可钉住全部断言且不受 BE 内存窗口（round 3 #1/#2）干扰；doris
  数据源凭据封存比对（相同不重封）的 apply 幂等已有契约测试钉住，未 live 重演；
- kafka 源的 idle（IdleTracker 3 拍零间隙）未 live 触发——无本地 kafka；其
  streak 逻辑与「追平后 hold」语义靠单测 + 本轮 #2 的推演，live 补验挂 R12；
- SSE 流（`/api/alerts/stream`）与 webhook 并投未走（round 3 同边界，契约测试
  在）；
- MCP 面的限速（100 req/s）与 batch 拒绝等协议负例未重演（R8 契约测试在，
  本轮只走正路径 + confirm 负例）；
- v1.2.0 的合并/tag/发包是用户保留决策，本轮未触碰；CHANGELOG 只动了
  [Unreleased] 段。
