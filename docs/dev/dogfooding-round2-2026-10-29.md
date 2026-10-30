# Dogfooding round 2（2026-10-29 立项 / 2026-10-30 实跑）—— 新类型词汇 + MCP 新工具自用全链

> R8 W5。用 R8 刚落的两块面——G2 类型词汇（`decimal(18,2)` / `timestamp(us)` / `date`）与
> MCP 工具面（`ddl_draft` / `ddl_execute` / `create_pipeline` / `deploy_pipeline` /
> `trigger_backfill` / `backfill_status`）——对**在役** Doris 真跑一轮全链自用：
> MCP 建表 → file 源管道写入 → 回填 dryRun/执行/重放。全部结论来自本轮实跑，证据路径逐条给出。
> 可重跑：`bash scripts/dogfood/round2.sh`（24 步，实测 10s，复跑亦全绿）；
> 同轮 e2e `bash scripts/e2e/type_precision.sh`（compose 2.1.7 **40/40**）或
> `TAILRACE_E2E_DORIS_MYSQL="docker exec -i doris-2.1 mysql" bash scripts/e2e/type_precision.sh`
> （在役 3.1.4 外部模式 **39/39**）；裸探针矩阵
> `scripts/e2e/evidence/type-precision-probe-2026-10-30/`（双版本逐行一致）。

## 一、对象与事实

| 项 | 实测 |
|---|---|
| 在役栈 | 容器 `doris-2.1`（FE）+ `doris-be`（BE）＝ **doris-3.1.4-rc02-7f5ba43de6**，BE Alive=1（round1 摩擦 #2 的称呼问题延续：容器名仍叫 doris-2.1） |
| compose 栈 | `docker-compose.yml` doris-fe/doris-be = **2.1.7**（39030/38040），R6 配方（swap override + `vm.max_map_count`），e2e 结束外科拆除 |
| 二进制 | `./scripts/build.sh` release + 本轮 decode 修复后重编（见摩擦 #1） |
| 磁盘 | 开工仅 5GiB（历史 `target/debug` 43G 叠加，触 OrbStack 红线并已在役栈被杀一轮，见摩擦 #5）；清 debug 产物后 47GiB，全程红线外 |
| MCP 面 | console `--mcp-allow-write` 启动；`initialize` 握手（session 头）+ `tools/list` = **14 工具**；写工具 `confirm:true` 门禁生效（D3.2 负例：缺 confirm → `CONFIRM_REQUIRED`，无 DDL 执行） |

## 二、操作实录（`scripts/dogfood/round2.sh`，24/24，10s）

```
D0  SHOW FRONTENDS/BACKENDS 取真实版本 → 专库 tailrace_dogfood2 DROP FORCE+CREATE
D1  console（--mcp-allow-write）→ REST login → MCP initialize（session 头）→ tools/list=14
D2  REST 注册数据源 + 表对象 df2_ledger：G2 列 [order_no/amt decimal(18,2)/ts timestamp(us)/
    d date/seq/note] + sequenceColumn=seq + keyModel=unique
D3  MCP ddl_draft → DDL 含 DECIMAL(18,2)+DATETIME(6)+DATE+function_column.sequence_col
    → ddl_execute 缺 confirm 被拒（CONFIRM_REQUIRED）→ 带 confirm 执行 →
    SHOW CREATE TABLE 活表类型+属性齐全（零手写 DDL）
D4  file 表 + 10 行种子（decimal 字符串形态、微秒 ts、date、seq=10）→ node serve ONLINE
D5  MCP compile_sql（读）+ create_pipeline（confirm）→ 【摩擦 #3：REST publish 补桥】→
    MCP deploy_pipeline（confirm）→ RUNNING → SELECT 断言：
    SUM(amt)=100000000000024.12 精确、上限 99999999999999.99 与尾零 0.10 逐行精确、
    ts 微秒 10/10、date 10/10
D6  MCP trigger_backfill dryRun=true（【摩擦 #4：仍需 confirm】jobId=null）→
    dryRun=false 执行 job 20261030-01 → MCP backfill_status 轮询 done rowsWritten=10 →
    同区间重放被 label dedup 吸收（表仍 10 行）→ 主管道 RUNNING
D7  DROP DATABASE FORCE（在役栈其余库未触碰）
```

全文日志：`scripts/dogfood/evidence/round2-2026-10-30-24of24.log`（逐步耗时
`round2-2026-10-30-summary.txt`）。

## 三、摩擦观察（每条含复现，按优先级）

### #1 【HIGH，本轮已修，波次缝隙】G2 新词汇在引擎 decode 层全是 `unimplemented!()`——首个 decimal 行直接 panic 杀死 node

- **现象**：type_precision e2e 首跑，管道 deploy 后引擎节点崩：
  `panicked at engine/crates/tailrace-decode/src/lib.rs:43: not implemented: decode column type Decimal128(18, 2)`，
  随后所有 deploy `ENGINE_UNREACHABLE`。W1 落了词汇表、W3 落了 DDL 映射，但
  jsonl/csv/ogg/debezium 四个 decoder 的 builder 面（`ColBatch::new` 五型 + `unimplemented`）
  没有联动——词汇到 arrow 类型的「最后一公里」断在 decode。
- **复现**：`git stash` 回 W4 提交后注册 `decimal(18,2)` 列的 file 表并 deploy 即崩
  （本轮 `/tmp/tp-e2e-compose-run2.log` 原始现场）。
- **处置（本轮修复，decode 单文件）**：`Decimal128` builder（含
  `with_precision_and_scale`——不设则 `finish()` 静默产出 `Decimal128(38,10)` 被 schema
  校验拒收，arrow builder 的隐藏默认值坑）+ `Timestamp` 四 unit + `Date32`；
  精确十进制解析 `parse_decimal_i128`（零依赖、half-up、溢出检查）；四个 json walk 与
  CSV 走 `append_g2_json` 类型参数分派；单测 4 个（20/20 绿，clippy 0 警告）。
- **残留**：其余新词（string/int8/int16/int32/float32/binary/list\<float32\>/variant/null）
  在 decode 仍 `unimplemented!()` panic——**fail-fast 而非静默**，但 DDL 面已能建表，
  「建得起来写不进去」的落差对使用者是坑。建议 R9 把 decode 面与词汇表对齐
  （或建表期即告警「该词汇引擎暂不支持」）。
- **教训**：类型面变更的波次验收应含「引擎真实跑一行新类型数据」，词汇/映射/golden
  全绿仍可能在 decode 断链（与 R7 摩擦 #1 同构：mock/单测钉不住全链）。

### #2 【MEDIUM】jsonl decimal 列 number 形态大值丢分——string 形态才精确，文档未提示

- **现象**：种子写 `"amt":99999999999999.99`（json number）→ 引擎 decode 走 f64 中转
  （simd-json 解析后原字面量已丢）→ 落库 `99999999999999.98`（差 1 分）；改
  `"amt":"99999999999999.99"`（string）→ 逐分精确。服务端同病更重：Doris 自身解析
  json number 直接落 `100000000000000.00`（探针 P2，双版本一致）。
- **复现**：e2e 第 4 轮 `/tmp/tp-e2e-compose-run4.log`（number 形态 sum=…24.11 rows=9）
  vs 第 5 轮 `e2e-compose-2.1.7-40of40.log`（string 形态全绿）；
  `scripts/e2e/evidence/type-precision-probe-2026-10-30/` P1/P2 对照。
- **建议**：file 源/向导对 decimal 列明示「字符串写法守恒、number 写法受 IEEE754 限制
  （与 Doris 服务端一致）」；engine 端 number→decimal 的 f64 路径注释已写明该语义。

### #3 【MEDIUM，R9】MCP 工具面缺 `publish`——create_pipeline→deploy_pipeline 断链，操作者被迫混用 REST

- **现象**：MCP `create_pipeline`（confirm）落 DRAFT；`deploy_pipeline`（confirm）直接
  400 `管道 #1 尚未发布（无 spec 快照），先发布再回填` 同族错误。14 工具里没有
  publish（REST `POST /api/pipelines/{id}/publish` 存在）。
- **复现**：round2 首跑 `/tmp/dogfood2-run1.log` D5.2；修后 D5.2 注释「MCP face has no
  publish tool — friction」。
- **建议**：R9 把 publish 并入 `deploy_pipeline`（deploy 未发布时自动 publish，语义是
  「上线」）或补第 15 个工具；上限 16 仍有余量。

### #4 【LOW，R9】`trigger_backfill` 的 dryRun 预览也被 confirm 门拦

- **现象**：blueprint §7.5 说 MCP 回填默认 dryRun（零副作用预览），但工具整体写类分派：
  `dryRun:true` 不带 confirm 仍被 `CONFIRM_REQUIRED` 拒。
- **复现**：round2 第二轮 D6.1（`/tmp/dogfood2-run2.log`）；修后 D6.1 注释。
- **建议**：门禁按「实际副作用」细分（dryRun 视为读），或工具描述明示「dryRun 也需
  confirm」；现状脚本已照门禁办事。

### #5 【MEDIUM，环境】两套 Doris 栈同机双跑将在役栈挤死 + 历史构建产物贴爆磁盘

- **现象**：compose 2.1.7 e2e 期间，在役 3.1.4 两容器同刻被杀（`Exited (137)`/`(143)`，
  OOM/终止信号）；叠加历史 `target/debug` 43G，磁盘一度只剩 5GiB（OrbStack 杀停红线）。
  外部模式 e2e 因此整轮失败一轮（`Connection refused`）。
- **复现**：`docker ps -a`（137/143 时间戳与 compose e2e 运行窗重合）；`df -g`；
  `du -sh target/debug`（清理前 43G）。
- **建议**：跑 compose e2e 前先停/确认在役栈可被挤；`target/debug` 定期清
  （`rm -rf target/debug` 回收 43G，release 不受影响）；R7 教训「越界全仓测试写满磁盘」
  的常态化防御。

## 四、反哺 R9 排期（按本轮证据排序）

| 优先 | 项 | 来源 |
|---|---|---|
| R9-1 | decode 面与其余 G2 词汇对齐（或建表期告警「词汇引擎未支持」） | 摩擦 #1 残留 |
| R9-2 | publish 并入 deploy 或补工具（15/16） | 摩擦 #3 |
| R9-3 | decimal 列字符串写法的文档/向导提示（number 形态精度边界） | 摩擦 #2 |
| R9-4 | dryRun 的门禁语义细分 | 摩擦 #4 |
| 流程 | 类型面/请求形状变更的波次验收必须含「引擎真实写一行」（本轮 #1 与 R7 #1 同构教训） | 摩擦 #1 |

## 五、未做 / 边界

- `list<float32>` / `variant` 等 9 个未落 decode 的词汇未实跑（DDL 建表面 W3 已映，
  引擎写入会 panic——摩擦 #1 残留，非本轮验收面）。
- kafka 源未跑（round1 同边界；本轮 file 源 + 在役栈）。
- MCP 审计行（`MCP_TOOL_CALL`，actor=mcp:dogfood-round2）本轮只验证了门禁负例的
  错误码，未查审计表落行——W4 的契约测试已覆盖，live 审计查询留给 R9。
- 三套 live 日志（compose e2e 40/40、在役外部 39/39、dogfood 24/24）均已入库，路径见 §首。
