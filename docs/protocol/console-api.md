# Console API — console-api

> 管理面 REST 协议正典（`tailrace-console` 二进制）。随 utoipa 注解同步维护；
> 机器可读正典 = 运行时 `/api-docs/openapi.json`（44 路径），Swagger UI 在 `/swagger-ui/`。
> 首版 2026-10-11；鉴权协议安全修复改版 2026-10-13。

## 一、通用约定

- **Base** `/api`；**认证** `POST /api/auth/login` 签发不透明 token（24h），经 `Cookie: tr_session=…`（HttpOnly; SameSite=Lax；SSE/EventSource 唯一免手工凭据通道）或 `Authorization: Bearer …` 携带。例外免会话端点：`POST /api/auth/login`、`POST /api/nodes/register`、`POST /api/nodes/heartbeat`（m2m，bootstrap/节点 token 在 JSON body 自证）。
- **信封**：成功直返资源 JSON；失败 `HTTP 4xx/5xx + {"error":{"code","message"}}`。唯一例外：`POST /api/pipelines/compile` 的 400 按其自带 diagnostics 形态（`{"ok":false,"statements":[],"diagnostics":[…]}`），前端从 error body 读 diagnostics。
- **操作人**：请求头 `X-Operator` 可选覆盖（默认会话用户名），落审计。
- **错误码语义**：400 INVALID_ARGUMENT / 401 UNAUTHENTICATED·BAD_NODE_TOKEN / 404 NOT_FOUND / 409 CONFLICT·IN_USE·GENERATION_CONFLICT / 413 PAYLOAD_TOO_LARGE / 501 NOT_IMPLEMENTED / 502 ENGINE_UNREACHABLE·ENGINE_REJECTED。
- **时间戳**：ISO-8601 UTC（`2026-10-11T08:00:00Z`；存储层 SQLite `datetime('now')` 序列化时转换）。
- **审计**：所有管理面变更操作写 `audit_log`（DATASOURCE_CREATE/UPDATE/DELETE/TEST、TABLE_CREATE/DELETE、NODE_MANUAL_REGISTER/REVOKE/DELETE、ARTIFACT_UPLOAD/DELETE、PIPELINE_CREATE/UPDATE/DELETE/PUBLISH/DEPLOY/RECOVER/ACTION；LOGIN/NODE_REGISTER 沿用 P0）。

## 二、端点清单（P1 实装状态）

| 状态 | 端点 |
|---|---|
| ✅ | POST /api/auth/login · POST /api/auth/logout · GET /api/auth/me（P0） |
| ✅ | POST /api/nodes/register · POST /api/nodes/heartbeat（m2m；协议 2026-10-13 安全修复改版：**register 响应携带服务端新生成的 per-node token** `bootstrapToken`（`tr_b_<48hex>`，一次性回显，节点须持久化并在此后 register/heartbeat 携带；每次成功 register 轮换并重新回显）；`node.bootstrap_token` 列仅存 sha256(token)，token 原文不再驻留库中；全局 bootstrap token 仅准入未知 node_key 的**首次注册**——已注册节点持全局 token 一律 401 BAD_NODE_TOKEN；未知节点心跳 401；吊销（digest 置 NULL）401） |
| ✅ | GET /healthz · GET /swagger-ui/ · GET /api-docs/openapi.json |
| ✅ | GET /api/datasource-types（静态注册表自述：kafka/doris/file + mysql/postgresql（T14，jdbc 批源族，jdbcUrl 字段），动态表单单一事实源） |
| ✅ | GET /api/datasources?type= · POST /api/datasources · GET/PUT/DELETE /api/datasources/{id} |
| ✅ | POST /api/datasources/test · POST /api/datasources/{id}/test（HTTP 恒 200，结果在 status 字段） |
| ✅ | POST /api/datasources/{id}/tables · GET /api/datasources/tables?datasourceId=&kind= · DELETE /api/datasources/tables/{id} |
| ✅ | POST /api/datasources/{id}/discover（T14，2026-10-21；非 501——engine `tailrace-metadata` 内嵌转发：kafka 经 rdkafka fetch_metadata 列 topics（首点拆 db.table，带 partitions）；doris/mysql 走 9030 mysql **文本协议** SHOW DATABASES/SHOW TABLES；postgres 走 information_schema（扩展协议绑定参数）；回 `{"assets":[{dbName,tableName,kind,format:null,partitions?}]}`，kind 按 `table_kind_for` 派生（kafka→kafka、doris→doris、mysql/postgresql→jdbc）；成功缓存 30s（键=datasource id+generation+connJson 逐字节，失败不缓存以便改参即重探）；file 型/connJson 缺字段 400、数据源不存在 404、后端不可达 502） · POST /api/datasources/{id}/infer-schema（body `{dbName,tableName,format?,sampleRows?}`：kafka 尾采 sampleRows 条（默认 20 钳 1..=100）经 decode 链类型格推断列 + 给出 format 建议（ogg/json/jsonl，其它显式格式 400）；SQL 后端读 DDL 列型（DESC / information_schema.columns），format 恒 null；回 `{"columns":[[列名,arrow 型]…],"format":null|"ogg"|"json"}`——**不落库**，登记仍走 POST /tables；同档 400/404/502） |
| ✅ | GET /api/nodes（90s 无心跳判 OFFLINE，只读计算） · GET /api/nodes/{id} |
| ✅ | POST /api/nodes/manual（先探活 `GET http://host:port/healthz`，200 才登记；node_key 由控制面生成 `sn-<16hex>`，per-node token 响应一次性回显，库内仅存 sha256；探活失败 502） |
| ✅ | POST /api/nodes/{id}/revoke（204） · DELETE /api/nodes/{id}（被 assigned 管道引用 409 IN_USE；远端清理按「跳过并警告」分支：`{"remoteCleaned":false,"warning":"…"}`） |
| ✅ | GET /api/nodes/{id}/events?hours=168 · GET /api/nodes/events?hours&level |
| ✅ | GET /api/nodes/{id}/metrics?hours=6（1 分钟桶，窗口钳 1..24h） |
| ✅ | GET /api/nodes/{id}/pipelines-runtime（代理引擎 GET /v1/pipelines 原样透传；不可达 502） |
| ✅ | GET /api/nodes/{id}/deploy-log?limit=100（升序；SSH 链落盘前恒空） |
| ⛔ 501 | POST /api/nodes/deploy（SSH 八步状态机） · POST /api/nodes/{id}/action（进程级 start/stop/restart/upgrade/rollback/uninstall + 暂停编排） · POST /api/nodes/{id}/migrate（P2） |
| ✅ | GET /api/artifacts · POST /api/artifacts（multipart：file(.tar.gz)+name([A-Za-z0-9._-]，默认 tailrace-node)+version；sha256 服务端算；name+version 重复 409；>512MB 413；落 `<data-dir>/artifacts/<name>-<version>.tar.gz`） · DELETE /api/artifacts/{id}（被节点引用 409） |
| ✅ | GET /api/pipelines?page&pageSize · POST /api/pipelines · GET/PUT/DELETE /api/pipelines/{id}（detail 回 `config`（五段 PlanOptions 原文，null 段序列化为 null——编辑页据此原样回传，保存不清空高级配置）；非 DRAFT 保存→PENDING_CHANGES；RUNNING 删除先自动 stop，引擎不可达 502 阻断） |
| ✅ | POST /api/pipelines/compile（console 内嵌 tailrace-parser 本地编译；`db.table`×dsMapping→`${ds:id.db.table}` 物化后编译；diagnostics 走 400 自带形态） |
| ✅ | POST /api/pipelines/{id}/publish（门禁：语句引用表对象全部可解析→version+1→`pipeline_version` 快照） |
| ✅ | POST /api/pipelines/{id}/deploy（plan 下推：nodeId 缺省自动分配——preferred 合格优先→运行管道最少→id 升序；引擎 ok:false→DEPLOY_FAILED+502） · POST /api/pipelines/{id}/recover（重推当前 plan 到指派节点） |
| ✅ | POST /api/pipelines/{id}/pause · resume · stop（代理引擎 POST /v1/pipelines/{id}/{action}；stop 回 PUBLISHED 并清指派） |
| ✅ | GET /api/pipelines/{id}/stats · GET /api/pipelines/{id}/offsets（引擎透传，ok:false 也 200；不可达 502） |
| ✅ | GET /api/pipelines/{id}/logs?lines=200（SSE 桥：首屏 tail=1&limit 钳 50..500 → 1.5s after_seq 增量轮询；15s keepalive；300s 会话上限；引擎瞬断连续 5 次发 eof） |
| ✅ | GET /api/pipelines/{id}/metrics/trend?hours=24（5 分钟桶；速率=相邻桶差分/300s） |
| ✅ | GET /api/pipelines/templates（SQL 模板内嵌） |
| ⛔ 501 | POST /api/nodes/ticket（票据链——依赖凭据加密与引擎侧 CredentialProvider，未在本阶段交付） |

## 三、关键语义裁决

1. **compile 的 `db.table` 改写**：编辑器 SQL 用 `库名.表名`，console 端仅改写 dsMapping 命中的两段名（别名 `o.id`、数字字面量 `1.5`、三段链、字符串/引号内不动）；未映射的 INSERT/FROM 目标由 parser 以「非 `${ds:}` 占位符」诊断拒绝（引用对拍）。group 约定 `tr.p{pipelineId}.s{index}`（compile 请求可带 pipelineId，缺省 0）。
2. **plan authEnv 用 `TAILRACE_DS_{datasourceId}`**：早期示例写 `TR_DS_4`，但引擎 `credential_from_env` 只放行 `TAILRACE_` 前缀（engine-v1.md §四、tailrace-catalog lib.rs:219）——按运行引擎契约为准。
3. **凭据存储为 AES-256-GCM 密文**（静态半边已落地，2026-10-13：`datasource_credential` 两列分别存 base64 的 12B nonce 与 ciphertext，`console-store/src/secretbox.rs`；master key 解析 fail-closed——`TAILRACE_MASTER_KEY` 优先，否则 `<data-dir>/master.key` 首启生成，解析失败 console 拒绝启动，绝不降级）；凭据永不出现在任何响应（仅 `hasPassword`）。
4. **探测语义**：TCP 连通（kafka bootstrap 逐主机、doris FE jdbcUrl+BE streamLoadUrl 全部可达才 CONN_OK）+ file 路径存在性。注意：开发机 TUN 代理会劫持非环回 TCP connect（瞬时假成功）；环回地址不受影响。协议级探测（Kafka metadata / Doris·MySQL·PG 握手）已随 T14 discover/infer-schema 交付（engine `tailrace-metadata`，见端点清单）。
5. **自动分配合格节点** = ONLINE（心跳 90s 内）且已上报 api_url。
6. **dispatchId** = epoch-ms*1000+进程内序号（每次下推唯一；引擎幂等拒同 ID 重复下推）。

## 四、未决 / 后续

- ticket 端点（票据链，600s 单次票据）仍未交付——凭据加密静态半边已落地（见「关键语义裁决」3），票据链为 v1.x 结构性修复。
- SSH 部署链八步 + 节点进程动作的「逐条暂停→执行→自动恢复」编排。
- catalog 快照浏览 / 样本预览 / lag 告警规则 / SQL 片段助手（后续版本族；discover / infer-schema 已于 T14 交付）。jdbc 管道 console 端到端未通（plan 下推 bootstrap/sequence_column 两键，见 docs/connectors/jdbc.md §七）。
- 90s OFFLINE 目前为读时计算；未做后台定时器回写。
