# Dogfooding round 5（2026-11-14）—— 真实新用户视角：crates.io 装 1.2.0 → README Quickstart 逐字 → Doris → DDL → 回填

> R12 W4（T-E）。本轮**刻意用新用户姿势**：不读源码、不开工作树、不用 `scripts/build.sh`
> ——从 crates.io 装**已发布的 1.2.0**，把 `README.md` 的「30 秒快速开始」当唯一说明书
> **逐字执行**，一路走到「数据进 Doris」，再跑 DDL 闭环与回填。**结论先行：README 的
> 三条命令本身可逐字执行（含两处 `<占位>` 的填法），但它承诺的「然后在浏览器登录、
> 创建管道（文件源 → Doris 表），数据就能流起来」——从零用户此刻没有文档指引的三步断在
> 中间**（§三 #1/#2/#3）。** 全部结论来自本轮实跑，每条摩擦带复现步骤。
>
> 环境：macOS 26.6 (arm64, 228GB)、rustc/cargo 1.98.1、cargo 走 rsproxy 镜像
> （`~/.cargo/config.toml` 全局已是 sparse+rsproxy.cn）、Doris 在役栈 `doris-2.1`
> （FE 3.1.4-rc02，`127.0.0.1:9030` / BE `127.0.0.1:8040`，单 BE）。
> **门禁面**：`bash scripts/e2e/alert_flow.sh` 首跑 **60/69** → 定位为脚本 I/L 段前提
> 过时（非代码回归，§八 #3）→ 修断言后连跑两次 **69/69**；
> `bash scripts/dogfood/round4.sh` **36/36**；`bash scripts/dogfood/round2.sh` **23/24**
> （唯一 FAIL 是 D1.3 记的 round 2 自己的 14 工具面，HEAD 已 16）。
> 工作目录 `/tmp/tailrace-r5/user`（干净目录，此前无任何 tailrace 状态）。

## 一、安装（README 第 1 步，逐字）

```bash
cargo install tailrace-node --version 1.2.0 --locked      # 10m12s，37 个 crate 被编译
cargo install tailrace-console --version 1.2.0 --locked   #  6m29s，22 个 crate
```

| 项 | 实测 |
|---|---|
| 可装性 | **两条命令都成功**（`~/.cargo/bin/tailrace-node` 171,774,416 B / `tailrace-console` 102,842,544 B） |
| 版本 | `tailrace-node 1.2.0`、`tailrace-console 1.2.0`——与 README 写死的 `--version 1.2.0` 一致 |
| 耗时 | **node 10m12s / console 6m29s（合计 ~16.7 分钟）**。README 标「30 秒快速开始」——**30 秒是第二步起的运行时间，不含首次编译**（摩擦 #5） |
| crates.io 面 | 22 包 max_version 全为 1.2.0（`crates.io/api/v1/crates/<name>` 逐包查，含首上的 `tailrace-ddl`）；发布日志 `scripts/release-log-20261103.txt` 记 20 条 `PUB … OK` 与若干 `SKIP already 1.2.0` |
| 前置 | 只装了 crates.io 上的 crate；**无需 Node/pnpm**（README 首屏承诺成立——1.2.0 的 console 二进制自带内嵌 SPA） |

## 二、README 第 2/3 步逐字执行

```bash
tailrace-console --data-dir ./data        # 终端 A
tailrace-node serve --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 --advertise 127.0.0.1:18100 \
  --bootstrap-token <第 2 步打印的 token>  # 终端 B
```

| 断言 | 实测 |
|---|---|
| console 起得来 | ✅ 日志：`Database migrated to version 6` → 打印 `admin` + 初始密码（一行）→ **bootstrap token 单独一段打印**（`tr_b_…`，与 README 的「第 2 步打印的 token」对应）→ `listening on http://0.0.0.0:8080`、`mcp: … (write tools disabled)` |
| SPA 在 | ✅ `GET /` → 200 `text/html` 804B（内嵌 `assets/index-DdbhWDcK.js` 等）；`/swagger-ui/` 200 |
| node 自注册 | ✅ `self-registration ok node_key=sn-…`；console `GET /api/nodes` → `engine-node ONLINE engineVersion=1.2.0` |
| 登录 | ✅ `POST /api/auth/login` 用日志里的密码换到 token（53 字符）；浏览器 UI 同样可登（左下角「管理员」） |
| `--bootstrap-token` 占位 | **README 的 `<第 2 步打印的 token>` 是字面占位**，复制粘贴会得到 `invalid value` 一类错误——但 token 打印成一段独立文字，人工抄录无歧义（摩擦 #6，低） |

## 三、摩擦清单（按优先级，每条带复现）

### #1 【HIGH】「创建管道（文件源 → Doris 表）」在 UI 里**没有一条可走通的新手路径**

README 的承诺是「登录，创建管道（文件源 → Doris 表），数据就能流起来」。逐字照做时：

- **表对象怎么登记？** 数据源页的「发现资产」按钮**对 file 数据源永远是 disabled**
  （`web/src/pages/datasources/index.vue:452`：`discoverTarget` 只在表对象筛选下拉**选中
  某个具体数据源**时非空；下拉默认「全部」）。即便选中了，file 源的 discover 端点本身
  也拒绝：
  `POST /api/datasources/2/discover -d '{}'` → `{"error":{"code":"INVALID_ARGUMENT",
  "message":"文件数据源不支持发现（路径下的文件请直接登记表对象）"}}`。
  而**UI 里不存在任何「手动登记表对象」入口**（全仓 `datasourcesApi.registerTable`
  唯一调用方是 `DiscoverModal.vue:149`，即只在 discover 出资产的路径上）。→ **新用户
  用 UI 无法为 file 源登记表对象，页面上给出的方向（「发现资产」）对 file 源是死路。**
  复现：登录 → 数据源 → 注册 file 数据源（路径填一个放 jsonl 的目录）→ 表对象卡「发现资产」
  灰 / 点 EmptyState 的 CTA 提示「请先在筛选中选择一个数据源」→ 选中该源 → discover 返回上
  述 400。
- 渡过的做法（**文档里没有**）：`POST /api/datasources/{id}/tables` 手写
  `{dbName,tableName,format,columnsJson}`。我这样登记了 `local.orders_in`
  （jsonl，`order_no utf8 / amt decimal(18,2) / ts timestamp(us) / d date / seq int64 /
  note utf8`）。

### #2 【HIGH】Doris 汇的管道**部署必失败**，除非手工给 node 注入 `TAILRACE_DS_{id}`

按 REST 建管道（`INSERT INTO tailrace_r5.df5_ledger SELECT … FROM local.orders_in`，
dsMapping 名→id）→ publish 成功（specJson 物化成 `${ds:1.…}`）→ deploy：

```
POST /api/pipelines/1/deploy → 502 {"code":"ENGINE_REJECTED",
"message":"引擎装配失败: … environment variable TAILRACE_DS_1 not set
(credentials never enter files; inject before running)"}
```

`TAILRACE_DS_1` = `Basic base64(root:)`（见 `console-api.md` 语义裁决 2 与
`api/pipelines.rs:1570-1572` 的 authEnv 派生）。**README / README.zh-CN / 快速开始三处
都没提这条**——`grep TAILRACE_DS README.md README.zh-CN.md docs/ops/*.md` = 0 命中，只在协议文档的裁决表里。
新用户的第一次 `deploy` 必然 502。复现：上述四步；修法 = 重启 node 时加
`env TAILRACE_DS_1="Basic cm9vdDo="`（或任何 `Basic b64(user:pass)`）。
→ 缓解建议：README Quickstart 加一行「Doris/StarRocks 汇：engine 侧需要
`TAILRACE_DS_<datasourceId>`=Basic base64(user:pass)」；更根本的是让 deploy 错误信封
直接带这条指引（现在 message 已带，但只在 502 detail 里）。

### #3 【MED-HIGH】file 源的表名 = **目录下的文件名**，README 未说明

登记 `local.orders_in` 后，engine 解析到的路径是
`<数据源 connJson.path>/orders_in`（`conn-file/src/lib.rs:73-79` `resolve_path`，
即 `dir.join(table name)`），**不是**目录里任意 `.jsonl`。我的源目录里放的是
`orders.jsonl`，于是 node 日志连刷 `source file temporarily missing … (text:
/tmp/tailrace-r5/user/inbox/orders_in)`，100 拍后判「moved away」进退避（2/4/8/16/32s），
管道状态仍 `RUNNING` 但 `consumed=0`。把文件改名为 `orders_in` 后下一拍即开跑。
复现：登记表对象 `orders_in` + 目录里只有 `orders.jsonl`。→ 文档应写明
「表名即目录下的文件名（可带绝对路径）」；退避日志 100 拍窗口外行看着像正常空转，
建议把 `source file missing` 的处置也写进 Quickstart 的排错一行。

### #4 【MED】建表向导的副本数默认走集群默认 → 单 BE 栈必失败两次

UI 建表向导第 2 步「副本数」留空 = 集群默认。单 BE 的 3.1.4 栈执行时：
`DDL 执行失败: … replication num should be less than the number of available
backends. replication num is 3, available backend num is 1`。执行前还先撞过一次
`Unknown database 'tailrace_r5'`——**向导不会帮我建库**（表对象登记时 dbName 是自由文本，
Doris 侧库不存在时错误只能由 Doris 原文回传）。
把副本数选 1（页面已有提示「单 BE 集群填 1」）并手建库后，第三次执行成功。
复现：新 Doris 空库 → 注册 doris 源 → 登记 `tailrace_r5.df5_ledger` → 向导第 2 步留空副本数
→ 执行 → 上述报错。→ 建议：向导在 execute 前对 `SHOW BACKENDS` 后端数做一次探测并预填
（页面提示原文在 `TableDdlWizard.vue:455`：console 不代查 `SHOW BACKENDS` = 已知取舍），
至少在报错文案里点出「单 BE 填 1」。

### #5 【LOW-MED】「30 秒快速开始」的标题口径

真正的 30 秒只在两个二进制已装好时成立；首装是 **~16.7 分钟**（node 10m12s +
console 6m29s，本机 rsproxy 镜像）。README 标题「30 秒快速开始」下面的第一条命令却是
十分钟级的安装。→ 建议标题改「从零到跑通」并在第一行显式写「首次 `cargo install`
约 10-17 分钟（编译 35+22 个 crate）」。

### #6 【LOW】`<第 2 步打印的 token>` 是字面占位，复制即报错

`tailrace-node serve --bootstrap-token <第 2 步打印的 token>` 直接粘贴 →
cli 解析失败（新增：给出看不清的 `invalid value`）。token 打印是独立段落、无歧义，
所以只算低优；建议占位写 `<token>` 并把「日志里 `[Tailrace]   tr_b_…` 那一行」写进注释。

### #7 【LOW】控制台页脚写死 `tailrace-console v0.1.0`

登录页页脚 `tailrace-console v{{ APP_VERSION }}`，`APP_VERSION = pkg.version` 取自
`web/package.json:4`（`"version": "0.1.0"`，自 f90e6a2 起未动）。装的是 1.2.0，界面说 0.1.0。
复现：打开 `http://127.0.0.1:8080/` → 页脚。→ 修法：构建时把 package.json version 与
workspace version 对齐（或直接读 console 的 `CARGO_PKG_VERSION`），一个字段的改动。

### #8 【LOW】管道编辑器（CodeMirror）在浏览器自动化下**无法键入**

不是产品缺陷，但影响「AI 客户端/自动化测试能不能走 UI」的可行性评估：`.cm-content`
的 contenteditable 在 IAB 下坐标点击 + `cua.type` 只写入半截或不写入（读回
`textContent` 始终空），Playwright locator 对该节点报 `not visible`。本轮因此改走
REST（与 UI 同一批端点）完成管道创建/发布/部署；记录在此供后续「UI e2e 自动化」评估。

## 四、走到的终点（逐项断言）

| 步 | 结果 |
|---|---|
| 数据进 Doris | ✅ `SELECT * FROM tailrace_r5.df5_ledger` → 2 行：`U-001 12.34 2026-11-14 10:00:01.000001 2026-11-14 1`、`U-002 0.10 …`——**decimal 精确、微秒 ts 保留、date 落 DATE** |
| stats 面 | 1.2.0 的 stats JSON：`consumed=2 flushed=2 flushes=1 flushMsTotal=252 lastDataMs=…`，**无 `sourceIdle`**（该字段随 v1.3 开发线才引入——见 §六） |
| DDL 闭环（evolve） | ✅ `POST …/ddl/evolve` → `{"changed":false,"statements":[],"added":[],"elapsedMs":0, partialUpdateNotice:…}`（登记列 == 实表 → 无操作，正确） |
| 回填 dryRun | ✅ `{jobId:null, estimatedRows:null, labelPrefix:"tr-p1-s0-bf"}`——零副作用 |
| 回填真跑 | ✅ `jobId=20261114-01`，`state: running → done`，`rowsWritten=2`；重放后**表仍是 2 行**（确定性 label 让 Doris 回 `label already exists`→Dedup），行数守恒 |
| 建表闭环 | ✅ 向导三步：列确认（6 列逐列类型下拉，`seq` 自动标 `sequence_column`）→ 键模型（UNIQUE KEY，键列 `order_no` 带「猜测」徽标，必选门生效）→ DDL 预览 → 执行 → `建表成功（已入审计 TABLE_DDL_EXECUTE）`；`SHOW CREATE TABLE` 核对：`DECIMAL(18,2)`/`DATETIME(6)`/`DATE`/`UNIQUE KEY(order_no)`/`enable_unique_key_merge_on_write=true`/`function_column.sequence_col=seq` 全落 |

## 五、README Quickstart「是否逐字可执行」——直接结论

**三条命令：可逐字执行（占位需人工替换，见 #6）。承诺的后续动作：不能只靠 README。**

| README 原文 | 逐字可执行？ | 证据 |
|---|---|---|
| `cargo install tailrace-node --version 1.2.0 --locked` | ✅ | 10m12s，装成 |
| `cargo install tailrace-console --version 1.2.0 --locked` | ✅ | 6m29s，装成 |
| `tailrace-console --data-dir ./data` | ✅ | 起得来；日志两段一次性打印（密码 + bootstrap token）符合 README 描述 |
| `tailrace-node serve … --bootstrap-token <第 2 步打印的 token>` | ⚠️ 命令形态正确，占位需替换 | 自注册 ok，console 见 ONLINE |
| 「登录，创建管道（文件源 → Doris 表），数据就能流起来」 | ❌ **不可只靠 README** | file 源表对象无 UI 登记路径（#1）；Doris 汇 deploy 缺 `TAILRACE_DS_*` 必 502（#2）；file 表名=文件名未说明（#3） |

即：**门面承诺（「单二进制、装上就能跑」）在「启动」这一段是兑现的**，在「第一次把数据
写进 Doris」这一段**断在三条未写进 README 的前置条件上**（表对象登记、凭据 env、文件名
约定）。这三条都是「知道了就 1 分钟、不知道就卡住」的类型——正是 round 5 想找的东西。

## 六、顺带记录：已发布 1.2.0 与 HEAD 的能力差（供 README/文档对账）

| 面 | 1.2.0 实测（本轮装的） | HEAD（R12 树） |
|---|---|---|
| stats JSON | `consumed/flushed/skipped/bad/flushes/lastFlushMs/flushMsTotal/lastFlushDurMs/lastDataMs`——**无 `sourceIdle`** | + `sourceIdle`（R11 T-V1） |
| MCP `tools/list` | **14 工具**（含 `get_pipeline_logs`、无 `apply`/`apply_dry_run`/`publish_pipeline`） | 16 工具（8R+8W，`get_pipeline_logs` 已换成 `publish_pipeline`，R12 T-C） |
| node `/metrics` 家族 | 7 个（缺 `tailrace_pipeline_bytes_total`——文档已披露该指标未喂数） | 10（含 `source_idle`） |
| 手工建表词汇预检 | 无（未知类型词一路通过，deploy 才炸） | T-A 三入口预检（R12） |
| idle 告警 | 无 idle 判决 | T-V1 + T-B 两拍 Clear |

→ README 现状（第 52-53 行「9 指标」「10 个工具（6 读 + 4 写）」）**在 1.2.0 上是 7 指标 /
14 工具**（sourceIdle 的 idle gauge 与 apply 对都在 `[Unreleased]`）。zh README 已写
「16 工具在 1.3 线、1.2.0 为 14」——**英文 README 那一行是唯一没跟上的**（摩擦 #9，文档面）。

### #9 【LOW，文档】英文 README 能力表按 HEAD 写、装在手上的是 1.2.0

`README.md:52` 「9 指标」、`:53` 「10 个工具（6 读 + 4 写）」——分别早于 1.2.0 的 7 指标与
14 工具，也都不是 HEAD 的 10 指标/16 工具。复现：装 1.2.0 后 `curl :18100/metrics` 数家族 / `POST /mcp tools/list` 数工具。→ 建议在那一行加「[1.2.0: 7 指标 / 14 工具；1.3 线 10 / 16]」或改成版本无关措辞。

## 七、证据

`scripts/dogfood/evidence/round5-2026-11-14/`：

- `console-1.2.0.log`（首启：迁移 + 双一次性打印 + 监听行；密码/token 已脱敏）
- `node-1.2.0.log`（自注册 + `source file missing` 退避链 + 恢复后 flush 行）
- `console-metrics-1.2.0.txt`（1.2.0 console `/metrics` 家族快照 = 8 行、7 个 node 家族）
- `round4-rerun-summary.txt` / `round4-rerun-mcp-calls.jsonl` / `round4-rerun-console.log`
  —— 同轮 `scripts/dogfood/round4.sh` 在 R12 树上重跑 **36/36**（验证 T-D 的两处断言修正）

`scripts/e2e/evidence/alert_flow-r12w4-2026-11-15/`（§八 #3 的修复验证）：

- `alert_flow-69of69-run1.log` / `-run2.log` —— `bash scripts/e2e/alert_flow.sh` 连跑
  两次 **69/69**（首跑 60/69 的 9 个 FAIL 全部消解；密码已脱敏）

## 八、本轮对 docs/dev 卫生波（T-D）的输入

1. `round2.sh` 的 `TSTEP=now`（缺 `$`）——已在 T-D 修掉，本轮复跑 24 步逐步耗时真实
   （1s/3s/2s 等），唯一 FAIL 是 D1.3 记的是 round 2 自己的 14 工具面（HEAD 16 →
   按设计 FAIL，fail 行现在带上「round 2's 14-tool face; HEAD may differ」并修了
   `set -u` 下 `MCP_BODY` 未绑定的崩溃）；
2. `round4.sh` 两处弱断言——S2.5 从恒真的 `>=` 改成「排空边样本数 == 静默 4s 后样本数
   且 idle gauge == 1」（本轮实测 9 → 9 冻结）；G1.7 从「generation 非空」改成
   「GEN1 == GEN2」（本轮实测 1 → 1）。同轮重跑 36/36，两条新断言各自有真实读数。
3. **`scripts/e2e/alert_flow.sh` 的 I/L 段前提已过时（本轮门禁首跑 60/69，修后两次
   69/69）**——9 个 FAIL 是同一根因的连锁：脚本写于 `1658d36`，断言「暂停时 idle 冻结为
   1、退避期 idle=0」，而 R11 评审修复 `9799a61` 已把两条路径改成**复位 unknown**
   （`engine/crates/tailrace-runtime/src/lib.rs:936-939` 暂停、`:961-962` poll 错误，
   注释原文「a frozen "idle" would hold the lag rules for the whole pause」；
   `metrics.rs:452-455`「idle gauge … omitted when the source cannot tell」）。
   实测 `I2.2 … idle=`（空）、`L0.2 … idle=`（空），规则因**源不可判照常评估**而 firing
   ——正是 `docs/protocol/console-api.md:224` 裁决 (c) 写的「真正有积压（idle=0）或源
   不可判（无 idle 序列）时照常评估」。I 段因此合法贡献一对 firing+resolved，下游六个
   `==2/3/4` 绝对计数断言随之偏移。修法：I2/L0.2 改断言新语义（idle 序列缺失 = 不是 1），
   并在 I 段末尾记一次 `HOOK_BASE`，W1/W3/F2.2/F3.2/S3.1/D3.4 全部改成
   `HOOK_BASE + n` 偏移。证据 `scripts/e2e/evidence/alert_flow-r12w4-2026-11-15/`。
   （业务代码未改——改的是断言，代码与正典一致。）
