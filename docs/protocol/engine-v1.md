# Engine /v1 Protocol — engine-v1

> 引擎节点对外 HTTP 协议正典（据迁移后源码冻结：`engine/crates/tailrace-api/src/lib.rs`、`heartbeat.rs`）。
> 首个发布后 `/v1` 即对外契约；变更须版本化（`/v2`）+ 兼容窗口声明。
> `tests/contract/` 契约测试与本文件同步演进，随 CI 运行。

## 一、传输与通用约定

- 监听：`tailrace-node serve --addr`（默认 `127.0.0.1:18100`）。
- 编码：请求/响应体均为 JSON（`Content-Type: application/json`）。
- 成功判定：响应体 `"ok": true`；失败携带 `"error"`（诊断类另有 `"diagnostics"`）。
- 健康：`GET /healthz` → `{"ok": true}`（无鉴权）。
- **鉴权（2026-10-13 起，engine H1）**：`/v1` 全表面要求共享 node token——请求头
  `Authorization: Bearer <node token>` 或 `X-Node-Token: <node token>`；缺失/错误 →
  **HTTP 401** + `{"ok":false,"error":"unauthorized: ..."}`。`/healthz` 是唯一开放路由。

### 鉴权细节（node token 的来源与轮换）

节点侧接受的 token 集合（均为节点本地材料，常量时间比较）：

1. **当前 token**，按优先级解析：`--api-token` 旗标 > 环境变量
   `TAILRACE_NODE_API_TOKEN` > 注册回声领养的 per-node token
   （`state_dir/node_token`，0600）> `--bootstrap-token` 旗标（部署链共享 token，
   自注册首准入凭据）> 自生成 token（`state_dir/api_token` 持久化并在启动日志
   打印一次——裸节点绝不以空凭据运行）。
2. **辅助 token**（进程生命周期内始终接受）：启动时的 `--api-token` 与
   `--bootstrap-token`——console 在注册时签发新 per-node token 引发的轮换不会
   把部署链/console 代理锁在门外。

轮换联动：register 响应携带服务端新生成的 per-node token（见 §三）→ 节点持久化
到 `state_dir/node_token`、`/v1` 当前 token 原地轮换、后续 m2m 心跳改用之。
console 代理（plan 下推/动作/观测）以 `Authorization: Bearer <console 全局
bootstrap token>` 携带凭据——节点以 `--bootstrap-token` 侧的辅助集合接纳。
票据链（600s 单次票据、凭据仅存内存）为 v1.x 结构性修复，本版本不做。

## 二、/v1 全路由

### GET /v1/pipelines

管道列表（以节点内 PipelineRegistry 为真相源）。

```json
{
  "version": "0.1.0",
  "pipelines": [
    { "id": 42, "status": "RUNNING|PAUSED|FAILED|STOPPED",
      "statementCount": 2, "statements": [], "error": null }
  ]
}
```

### POST /v1/compile

SQL 编译诊断（编辑器内联报错数据源）。

- 请求：`{"sql": "INSERT INTO ${ds:...} SELECT ...", "pipelineId": 42}`
- 响应：`{"ok": bool, "statements": [...], "diagnostics": [{"statement": 0|null, "message": "..."}]}`
- 语义：`${ds:id.db.table}` 引用提取 → FlinkDialect 解析（v1 仅 INSERT INTO sink SELECT … FROM source）→ 语句集；游标组约定 `tr.p{pipeline}.s{idx}`（plan 配置 groupPrefix 时为 `{前缀}.s{idx}`）。

### PUT /v1/pipelines/{id}/plan

控制面下推 plan（SQL-first 正典路径）。

- 请求：

```json
{
  "pipelineId": 42,
  "sqlContent": "INSERT INTO ${ds:3.shop.orders} SELECT ... FROM ${ds:12.default.orders_ogg}",
  "tables": [
    { "reference": "3.shop.orders", "kind": "doris_table", "columns": [["order_no","utf8"]],
      "stream_load_url": "http://be:8040/api/db/t/_stream_load", "auth_env": "TAILRACE_DORIS_AUTH" },
    { "reference": "12.default.orders_ogg", "kind": "kafka", "columns": [], "bootstrap": "kafka:9092" }
  ],
  "checkpointMs": 10000,
  "dispatchId": 7831294175112,
  "initialState": "PAUSED",
  "options": {
    "reliability": { "dorisFlushRetries": 3, "dorisFlushBackoffBaseMs": 2000,
                     "dorisRequestTimeoutMs": 120000, "kafkaSendTimeoutMs": 180000,
                     "maxPollIntervalMs": 600000 },
    "recovery":   { "autoRestart": true, "autoRestartMax": 5, "restartBackoffSecs": [30,60,300] },
    "tuning":     { "flushRows": 10000, "flushIntervalMs": 10000, "flushBytes": 8388608,
                    "pollRows": 8000, "pollSliceMs": 100 },
    "groupPrefix": "etl.orders.eu",
    "startOffset": "earliest"
  }
}
```

- `tables` 随 plan 携带表对象元数据（缺引用表整体拒绝，不启动半装配管道）；`kind` 规范化为注册表键：`topic→kafka`、`doris_table`/`JDBC_TABLE→doris`（旧值作别名兼容）。
- **表项形态（R8 G1 options 化）**：每个表项 = 5 个通用字段 `reference` / `kind` / `format` /
  `columns` / `options`，连接器专属键全部收进 `options` 映射（键名正典
  `tailrace-catalog/src/lib.rs:35` `option_keys`：`bootstrap` / `stream_load_url` /
  `auth_env` / `label_prefix` / `sequence_column` / `partial_columns`；额外键原样透传，
  连接器经 typed accessor 读取）。上例的**顶层扁平写法在一个 minor 版本的兼容窗内仍有效**：
  这六键**双写双读**——写侧 options 与顶层同时输出，读侧顶层键折进 options（显式
  `options` 键优先、顶层 `null` 视为未设；`lib.rs:54` `LEGACY` 六键表、`:715`
  `PlanTableObject`、`:745` Raw 折叠镜像）。窗口关闭后只认 `options`。
- **`columns` 的类型词汇（R8 G2）**：`[列名, 类型串]` 的类型串按**十六词词汇**解析
  （`tailrace-ir/src/column_type.rs:30` `ColumnType`，经 `tailrace_catalog::ColumnType`
  re-export）：4 个旧词 `utf8` / `int64` / `float64` / `bool` 字节级兼容 + 12 个新词
  `string` / `int8` / `int16` / `int32` / `float32` / `decimal(p,s)` / `timestamp(unit)` /
  `date` / `binary` / `list<float32[,dim]>` / `variant` / `null`；小写精确拼写。未知词在
  迁移窗内 warn（列名 + 已知词表）并折为 `utf8`（与 G2 前 `_ => Utf8` 臂行为一致），
  窗口常量翻转后转硬错误、装配期拒绝（`column_type.rs:14-20`）。
- **`partial_columns`（R7 T-A7，部分列更新）**：汇表项可带 `partial_columns:[…]`（声明顺序
  的列名子集）——引擎据此让 `DorisSink` 以 `partial_columns=true` **且** `columns:<子集>`
  双头装载（`tailrace-conn-doris/src/lib.rs:574` `with_partial_columns`，**仅 Doris flavor**——
  starrocks + `partial_columns` 在 doris 工厂层即拒绝装配，`lib.rs:339-346` 的 `ensure!(flavor != StarRocks)`）。装配期通用能力门：任何
  声明了 `partial_columns` 的汇若 `capabilities().partial_update == false`（file / kafka 及未来
  任何未实现部分更新的汇）即拒绝装配并点名能力缺失（`tailrace-runtime/src/lib.rs:541-545`），
  不会把投影收窄后无声写出缺列的行。未声明的表项不带该键，与 A7 前形态逐字节一致。
- **file 表的 `bootstrap` = 登记目录**（R6 评审修复轮）：file 表对象在 `tables[]` 里
  携带 `bootstrap`（console 由 file 数据源 connJson 的 `path` 填充）时，file 源/汇以
  `bootstrap/表名` 解析文件路径——与进程 cwd 无关，常驻 node 与 console 内回填执行器
  打开同一文件。无 `bootstrap`（TOML 本地 catalog）时沿用裸名按 cwd 解析，行为不变；
  表名为绝对路径时优先。
- **`stream_load_url` 域白名单（engine H1）**：Doris/SR 表对象的 `stream_load_url`
  主机域必须在节点侧允许清单内，否则整个 plan 被拒绝（`ok:false`，错误指明越界
  host 与允许集）。URL 域由 console 登记表对象（数据源登记的 streamLoadUrl）决定
  而非 plan body 任意指定；节点默认策略为**仅环回**（localhost/127.0.0.1/::1/
  0.0.0.0），跨主机部署以 `--stream-load-hosts host1,host2` 或环境变量
  `TAILRACE_STREAM_LOAD_HOSTS` 显式抬升。scheme 仅接受 http(s)。
- `dispatchId` 重复下推幂等拒绝；`initialState=PAUSED` 装配即挂分区（升级/迁移/对账恢复路径）。
- `options` 全段缺省=引擎默认（双向向前兼容）；`options.badRowPolicy` 取值
  `dlq|fail|skip`（默认 `dlq`，见「坏行死信（DLQ）」节），非法值整体拒绝 plan。
- **维表打宽（T-D1，`JOIN ${ds:dim.<datasource>.<table>} [AS] <alias>`）**：
  SELECT 的 JOIN 目标可引用维表（目录对象 `kind=dim`，JDBC 来源，声明
  `sequence_column`=主键/唯一键列）。装配期（编译期）经 JDBC 全表翻页载入为静态
  `dim_<alias>` MemTable 嵌入执行计划——**广播维表，非流式 join**：无水位线、无
  状态后端、运行期不刷新（重启才重载快照）。显式上限超限即整体拒绝装配（fail-fast
  而非 OOM）：`tuning.dimMaxRows`（默认 500,000 行/表）、`tuning.dimMaxBytes`
  （默认 64 MiB/表，按解码后 Arrow 内存计）。维表声明的键列无主键/单列唯一索引 →
  拒绝编译（复用 JDBC keyset 唯一性门，防笛卡尔放大）。`tables` 中该维表对象的
  `reference` 即 SQL 里 `dim.` 之后的两段（`<datasource>.<table>`）。
  **连接词仅接受 `INNER JOIN`/`JOIN`/`LEFT [OUTER] JOIN`**（打宽契约）：维表侧是
  每语句静态快照，RIGHT/FULL 会对每个微批重发全部未匹配维表行、CROSS 按
  |src|×|dim| 逐批放大——编译期以诊断拒绝（唯一键门不防这两类逐批输出放大）。
  console 侧写法与登记（`kind=dim` + `sequenceColumn`、publish 门禁）见
  docs/protocol/console-api.md §三.7。
- 响应：`{"ok": true, "pipelineId": 42, "statements": 2, "mode": "sql-compiled+serve-executing"}`
- 兼容路径：旧 `specJson` 已废弃（`ok:false` + 指引重发布）。

### GET /v1/pipelines/{id}/stats

实时流入流出（原子计数器，累计跨重启连续——种子来自账本）。

```json
{ "ok": true, "stats": {
    "pipelineId": 42, "status": "RUNNING",
    "totals":  { "consumed": 0, "flushed": 0, "skipped": 0, "bad": 0, "flushes": 0, "lastFlushMs": 0 },
    "statements": [ { "consumed": 0, "flushed": 0, "skipped": 0, "bad": 0, "flushes": 0, "lastFlushMs": 0 } ] } }
```

不在册（未部署/已停止）→ `{"ok": false, "error": "pipeline #42 not registered ..."}`（日志仍可查）。

### GET /v1/pipelines/{id}/logs

结构化事件日志（内存环形：单管道 500 条、全节点 5000 管道 LRU；seq 全局单调）。

- 参数：`after_seq=N`（游标增量拉取，幂等）｜`tail=1&limit=200`（首屏尾部；limit ≤500）。
- 响应：`{"ok": true, "entries": [{"seq":1,"tsMs":0,"level":"INFO","kind":"FLUSH_OK","message":"...","statementIdx":0}], "lastSeq": 7}`
- WARN/ERROR 事件同时镜像落盘 `state_dir/events.log`（进程重启后节点侧仍有现场）。

### GET /v1/pipelines/{id}/offsets（新增）

透传磁盘账本（`offsets-{id}.json`）——账本为真相源，已停/未部署管道同样可查。

```json
{ "ok": true, "pipelineId": 42, "deployed": false,
  "statements": [
    { "statement": "0",
      "ledger": { "cursor": { "Kafka": { "topic": "t", "group": "tr.p42.s0", "offsets": { "0": 7 } } },
                  "lastAck": "p42-r123_0", "totals": { "consumed": 10, "flushed": 9, "flushes": 2 } } }
  ] }
```

- 账本损坏（隔离后 .bak 亦坏）→ `ok:false`（fail-closed，宁死不脏）。
- 旧格式条目（裸 Cursor / 无 totals 键）原样透传，`lastAck`/`totals` 缺省。

### POST /v1/pipelines/{id}/{action}（pause/resume/stop）

动作状态机：`pause`（停拉取保游标，PAUSED 挂分区防组逐出）｜`resume`（续推）｜`stop`（收尾 flush+游标提交后移除注册）。

- 响应：`{"ok": true, "pipelineId": 42, "status": "PAUSED|RUNNING|STOPPED"}`

## 二·五、坏行死信（DLQ，engine H2）

解码失败（`Decoded::Bad`）的行不再随游标越过而蒸发。策略随 plan 下推
（`options.badRowPolicy`），默认 `dlq`：

| 策略 | 行为 |
|---|---|
| `dlq`（默认） | raw bytes + 游标 + 时间落 side 文件，游标照常越过（行可恢复） |
| `fail` | 首个坏行即语句错误（报文含 120 字符样本）→ 重启循环 / FAILED；游标不越过 |
| `skip` | 仅计数（旧行为，行丢失） |

- **side 文件**：`state_dir/bad_rows/p{pipeline}-s{statement}.jsonl`，追加式，每行
  一条记录：`{"tsMs":169...,"pipeline":42,"statement":0,"cursor":{...},"rawB64":"..."}`。
  raw 为 base64 原始字节（坏行常为非法 UTF-8）；cursor 为 poll 时点游标（人工恢复
  的重放锚点）。
- **事件带 DLQ 路径**：`DECODE_BAD` 事件消息携带 side 文件路径（替代旧的 120 字符
  样本）；首个坏批即发一次，其后每 1000 行节流一次。stats 的 `bad` 计数全策略累加。
- DLQ 写失败 = 语句错误（宁可不过游标也不丢原始字节——越过未持久化的坏行等于
  数据销毁）。

## 三、心跳协议（console → node 反向：node → console）

启用条件：`tailrace-node serve --control-plane URL`。未指定时完全不启用。

### 身份（三层优先级）

1. 启动旗标 `--node-key`（部署链渲染，控制面权威分配）；
2. `state_dir/identity.json`（跨重启稳定，sn-\<16hex\>）；
3. 自生成 `sn-<16hex>`（自注册兜底）。

**控制面回声权威**：心跳响应 `node.nodeKey` 与本地不一致且形态合法 → 收编（改用并落盘）。

### POST {base}/api/nodes/register（自注册，幂等按 node_key）

```json
{ "nodeKey": "sn-0123456789abcdef", "apiUrl": "http://10.1.1.5:18100",
  "capacityJson": "{\"pipelines\":8,\"statements\":24,\"memory_mb\":2048,...}",
  "bootstrapToken": "..." }
```

**per-node token（console H1，2026-10-13 起）**：`bootstrapToken` 字段携带节点当前
凭据——已领养 per-node token 的节点必须回呈它（console 侧按存储的 sha256 比对）；
未知 node_key 仅以 console 全局 bootstrap token 首次准入。每次成功 register 的
响应回显**服务端新生成的 per-node token 明文（仅此一次）**：

```json
{ "node": { "id": 1, "nodeKey": "sn-0123456789abcdef", "name": "..." },
  "bootstrapToken": "tr_b_<48hex>" }
```

节点侧消费（容忍式解析：顶层 `bootstrapToken` 为正典字段）：领爱即持久化到
`state_dir/node_token`（0600）、原地轮换 `/v1` 当前 token（见 §一鉴权细节）、
后续 register/heartbeat 改用之，并发出 `NODE_TOKEN_ADOPTED` 事件。若以已领养
token register 遭 401（console 侧已吊销/轮换），节点丢弃本地领养、下一周期回退
bootstrap token 自愈。

### POST {base}/api/nodes/heartbeat（默认每 `--heartbeat-secs`=30s）

```json
{
  "nodeKey": "sn-0123456789abcdef", "name": "engine-node",
  "apiUrl": "http://10.1.1.5:18100", "engineVersion": "0.1.0",
  "capacityJson": "...(含 running_pipelines/running_statements 动态自报)",
  "pipelineStates": [ { "id": 42, "state": "RUNNING", "statements": 2,
                        "totals": { "in": 100, "out": 98, "flushes": 7 } } ],
  "events": [ { "ts": 1, "level": "ERROR", "kind": "SINK_WRITE_FAILED",
                "pipelineId": 42, "message": "..." } ],
  "bootstrapToken": "...（当前 m2m 凭据：领养的 per-node token，无则 bootstrap token）",
  "resources": { "rssBytes": 0, "memTotalBytes": 0, "cpuPercent": 3.5,
                 "rxBps": 100, "txBps": 233 }   // Linux /proc 采样差分；非 Linux/首拍键省略
}
```

- `events`：环形缓冲（容量 64，满弃最旧并计数），HTTP 200 确认后清除；断网保留待恢复补报。`pipelineId` camelCase。
- `pipelineStates[].totals`：单调累计（控制面差值算速率）；不在册管道无 totals 键。
- 流转：404/401/未注册 → 先 register 再立即心跳。
- 优雅停机：SIGTERM/SIGINT → `NODE_STOP` 事件 + 末次心跳（不带 resources）→ `registry.shutdown_all()`（全部管道收尾 flush+游标提交）→ 进程退出。

## 四、凭据与连接器语义（与 /v1 交互面相关）

- **凭据永不入 plan body**：plan 携带 `auth_env` 环境变量名（仅允许 `TAILRACE_` 前缀，防指向任意环境变量外泄）；节点侧现取现焚。
- 连接器按 Cargo feature 编译注册（`tailrace-node` default = kafka,file,doris；starrocks 随 doris crate feature；**jdbc 为真实连接器但默认不开**——`--features connector-jdbc` 注册 MySQL/PostgreSQL keyset 批源（`engine/bin/tailrace-node/src/main.rs:36-38`），**维表 `kind=dim` 的全表装载也经注册表的 `jdbc` 源工厂**（`tailrace-runtime/src/dim.rs:104-111`），故维表 join 同样要求该 feature；mqtt/s3 仍为占位 crate 不注册）——未编译的 kind 在 plan 装配时报「源/sink 类型未支持（本二进制未编译该连接器 feature）」。
- at-least-once 锚：sink flush 成功才提交游标账本；暂停=挂分区（Kafka 防组逐出）。
