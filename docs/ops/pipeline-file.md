# 管道声明文件（pipeline-as-code，T-G6）

> 声明文件 = 用一份 JSON 描述数据源 / 表对象 / 管道的期望状态，经 `POST /api/apply/dry-run`（只看差异）与 `POST /api/apply`（幂等 upsert）收敛 console。
> CLI 薄壳：`tailrace-console apply -f <file> [--dry-run] [--server <URL>]`。协议细节见 `docs/protocol/console-api.md` 语义裁决 12。
> v1 落稿 2026-10-31。

## 一、为什么只有 JSON（设计裁决）

serde_yaml 已停止维护（RUSTSEC 风险），v1 不引入。YAML 用户在管道里加一行 `yq` 转换即可：

```bash
yq -o=json '.' pipelines.yaml > pipelines.json
tailrace-console apply -f pipelines.json --dry-run
```

schema 由编译期 serde 同源保证（声明条目直接复用 REST 建/改端点的 DTO），不另设第二份 schema 文件。

## 二、文件形状

顶层三键，**全部可选；「缺省」与「声明」语义不同**：

- **键缺省** = 本文件不对该类对象做任何表态——不 diff、不改动、不报 orphan；
- **键存在（哪怕空数组）** = 本文件声明了该类对象的**全集**——console 里该类对象凡未被本文件点名的，都以 orphan 条目**明示**（v1 只报告，绝不删除）；响应同构：未声明的节在响应里整键省略，声明节哪怕空数组也在场。

```json
{
  "datasources": [ /* DatasourceUpsertBody[] */ ],
  "tables":      [ /* ApplyTable[] */ ],
  "pipelines":   [ /* PipelineUpsertBody[] */ ]
}
```

对象身份用业务键（库表 UNIQUE 约束的原键），**不用数字 id**——换一个 console 环境同一份文件照用：

| 类别 | 身份键 |
|---|---|
| 数据源 | `name` |
| 表对象 | `(datasource 名, dbName, tableName)` |
| 管道 | `name` |

## 三、datasources 条目字段

与 `POST /api/datasources` 的 body 同源（`DatasourceUpsertBody`）：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | string | ✅ | 身份键；全局唯一 |
| `type` | string | ✅ | `kafka` / `doris` / `starrocks` / `file` / `mysql` / `postgresql` |
| `connJson` | object | ✅ | 按类型必填键：kafka→`bootstrap`；doris/starrocks→`jdbcUrl`+`streamLoadUrl`；file→`path`；mysql/postgresql→`jdbcUrl` |
| `username` / `password` | string | — | 凭据（AES-256-GCM 密封落库）。**已存凭据且相同 → unchanged**（服务端解密比对，不重封） |
| `generation` | int | — | **apply 忽略**（服务端自行取当前代做 CAS，apply 中途被并发修改 → 409 拒绝重试） |

不 diff 的服务端字段：`status`、`generation` 本身。

## 四、tables 条目字段

与 `POST /api/datasources/{id}/tables` 的 body 同源（`TableRegisterBody`）+ 多一个属主字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `datasource` | string | ✅ | 属主数据源**名称**（REST 端点放在 path 里的 id，这里用名） |
| `dbName` / `tableName` | string | ✅ | 身份键后两段 |
| `format` | string | — | 解码格式（kafka 表：ogg/json/jsonl/debezium-json…） |
| `columnsJson` | `[[列名,词汇型]…]` | — | 列清单（十六词词汇，与 REST 登记同源）；null=未登记 |
| `kind` | string | — | 缺省按数据源类型派生；唯一可覆盖值 `dim`（仅 mysql/postgresql） |
| `sequenceColumn` | string | — | 键列（dim 必填；jdbc 增量游标列；doris UNIQUE KEY 去重列） |
| `keyModel` | string | — | `unique` / `duplicate`，仅 doris/starrocks 表 |

diff 的字段：`kind`、`format`、`columnsJson`、`sequenceColumn`、`keyModel`（逐字段，变哪个报哪个；apply 的 update 一并重写，`cursor_semantics` 随 kind 重新派生）。

## 五、pipelines 条目字段

与 `POST /api/pipelines` 的 body 同源（`PipelineUpsertBody`）：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | string | ✅ | 身份键 |
| `sqlContent` | string | ✅ | 管道 SQL（`db.table` × dsMapping 改写规则同 REST） |
| `dsMapping` | `{别名: 数据源名\|id}` | — | **值推荐写数据源名**（可移植）；数字 id 仍接受 |
| `checkpointMs` | int | — | 缺省 10000 |
| `preferredNodeId` | int | — | 环境相关，跨环境文件建议留空 |
| `config` | object | — | **六段** PlanOptions（reliability/recovery/tuning/groupPrefix/startOffset/**badRowPolicy**），与 REST 同源。`badRowPolicy` 取 `dlq`（缺省）/`fail`/`skip`，console 只携带不校验，引擎在 deploy 时严格解析（非法值整个计划被拒 → 502 ENGINE_REJECTED）。**条目里省略 `config` = 声明该管道「无高级配置」**，不是「不管」：已有 config 的管道会被 apply 更新为 `null`（dry-run 以 `config: {…} -> null` 明示）——从 UI 建的管道导出成文件时务必把 `config` 一并带上 |
| `generation` | int | — | apply 忽略 |

**apply 不发布不部署**：新建管道落 DRAFT；更新已发布管道与 `PUT /api/pipelines/{id}` 同语义——非 DRAFT 保存后转 `PENDING_CHANGES`，需再走 publish/deploy 才生效。已匹配（unchanged）的管道 apply **不触碰**（不翻状态、不动 updated_at）。

## 六、示例

```json
{
  "datasources": [
    {"name": "edge-kafka", "type": "kafka",
     "connJson": {"bootstrap": "10.1.1.5:9092,10.1.1.6:9092"}},
    {"name": "shop-mysql", "type": "mysql",
     "connJson": {"jdbcUrl": "jdbc:mysql://10.1.1.7:3306/shop"}},
    {"name": "doris-sink", "type": "doris",
     "connJson": {"jdbcUrl": "jdbc:mysql://10.1.1.20:9030/ods",
                  "streamLoadUrl": "http://10.1.1.20:18040"},
     "username": "loader", "password": "${DORIS_PW}"}
  ],
  "tables": [
    {"datasource": "edge-kafka", "dbName": "shop", "tableName": "orders_ogg",
     "format": "ogg", "columnsJson": [["id","int64"],["payload","utf8"],["op_type","utf8"]]},
    {"datasource": "shop-mysql", "dbName": "shop", "tableName": "dim_user",
     "kind": "dim", "sequenceColumn": "id", "columnsJson": [["id","int64"],["name","utf8"]]},
    {"datasource": "doris-sink", "dbName": "ods", "tableName": "orders",
     "keyModel": "unique", "columnsJson": [["id","int64"],["payload","utf8"]]}
  ],
  "pipelines": [
    {"name": "orders→ods",
     "sqlContent": "INSERT INTO ods.orders SELECT id, payload FROM shop.orders_ogg WHERE op_type IN ('I','U')",
     "dsMapping": {"shop": "edge-kafka", "ods": "doris-sink"},
     "checkpointMs": 10000,
     "config": {"reliability": null, "recovery": null, "tuning": null,
                "groupPrefix": null, "startOffset": "earliest",
                "badRowPolicy": "dlq"}}
  ]
}
```

> 示例里 `"${DORIS_PW}"` 是**占位示意**——apply 不做模板展开；凭据要么明文写进文件（注意别入库进 Git，或仅首次 bootstrap 用完即删），要么首次 apply 后把凭据字段从文件中删掉（此后不声明凭据 = 保持库内已存凭据不变，§4.2 blank-password 语义）。

## 七、CLI

```bash
# 看差异（不写任何东西）
tailrace-console apply -f pipelines.json --dry-run

# 收敛
tailrace-console apply -f pipelines.json --server http://10.1.1.2:8080

# 凭据三种给法（任选一）
tailrace-console apply -f pipelines.json --token <会话token>
TAILRACE_CONSOLE_TOKEN=… tailrace-console apply -f pipelines.json
tailrace-console apply -f pipelines.json --user admin --password '…'
```

人类可读输出（三态标记 `+` create / `~` update / `=` unchanged，逐字段 from → to，orphan 单列；`sqlContent` 只报长度差避免刷屏）：

```
datasources:
  + edge-kafka                          create
  ~ doris-sink                          update  (#3)
      connJson.streamLoadUrl: "http://10.1.1.20:8040" -> "http://10.1.1.20:18040"
      credential: "<redacted>" -> "<redacted>"
  = shop-mysql                          unchanged  (#2)
tables:
  + edge-kafka/shop.orders_ogg          create
orphans (in the console, not in this file — apply never deletes):
  - pipeline  old-one  (#3)
summary: create 2, update 1, unchanged 1, orphan 1
(dry-run: nothing was written)
```

退出码：0 = 2xx（含零变更）；1 = HTTP 失败 / 网络失败；2 = 参数错。凭据值在任何输出与 diff 中都只有 `<redacted>`。

## 八、语义速查

- **幂等**：同文件连 apply，第二次全 `unchanged`、零写、零审计行；差异只在真有漂移的字段上落地（数据源 CAS 代不无谓 +1，管道状态不翻 PENDING_CHANGES）。
- **条目内字段省略 = 声明为缺省值**（与顶层三键的「键缺省=不表态」不同）：管道条目省略 `config` 即把该管道的高级配置声明为空——已有 config 的管道在首次 apply 时会被清成 `null`（dry-run 里是一行 `config: {…} -> null` 的 update）；二连 apply 收敛（`null == null`）。同理 `checkpointMs` 省略 = 10000。要「保持线上不变」就把当前值写进文件，apply 没有「不管这个字段」的语义。
- **永不删除**：从文件删条目 ≠ 删线上对象；orphan 只是提醒。清理仍走各 REST DELETE（带 IN_USE 守卫）。
- **全文件校验前置**：任何一个未知引用（表指向不存在的数据源、dsMapping 引用不存在的别名）→ 整个文件 400，一个字都不写。
- **执行顺序**：datasources → tables → pipelines（依赖方向）。跨对象非单事务：中途失败可能留下前半段写入——重跑同一文件即收敛（幂等是恢复路径）。
- **审计**：每个真实写一行既有动作名（DATASOURCE_CREATE/UPDATE、TABLE_CREATE/**TABLE_UPDATE**、PIPELINE_CREATE/UPDATE，detail 带 `"via":"apply"`）+ 一行 `APPLY` 汇总（仅在实际有写时）；dry-run 不审计。
