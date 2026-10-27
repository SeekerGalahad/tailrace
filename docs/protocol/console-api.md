# Console API — console-api

> 管理面 REST 协议正典（`tailrace-console` 二进制）。随 utoipa 注解同步维护；
> 机器可读正典 = 运行时 `/api-docs/openapi.json`（47 路径），Swagger UI 在 `/swagger-ui/`。
> 首版 2026-10-11；鉴权协议安全修复改版 2026-10-13；MCP 面正典化 + 维表（T-D1）动线增补 2026-10-27；建表闭环（T-A1）三端点增补 2026-10-28。

## 一、通用约定

- **Base** `/api`；**认证** `POST /api/auth/login` 签发不透明 token（24h），经 `Cookie: tr_session=…`（HttpOnly; SameSite=Lax；SSE/EventSource 唯一免手工凭据通道）或 `Authorization: Bearer …` 携带。例外免会话端点：`POST /api/auth/login`、`POST /api/nodes/register`、`POST /api/nodes/heartbeat`（m2m，bootstrap/节点 token 在 JSON body 自证）。
- **信封**：成功直返资源 JSON；失败 `HTTP 4xx/5xx + {"error":{"code","message"}}`。唯一例外：`POST /api/pipelines/compile` 的 400 按其自带 diagnostics 形态（`{"ok":false,"statements":[],"diagnostics":[…]}`），前端从 error body 读 diagnostics。
- **操作人**：请求头 `X-Operator` 可选覆盖（默认会话用户名），落审计。
- **错误码语义**：400 INVALID_ARGUMENT / 401 UNAUTHENTICATED·BAD_NODE_TOKEN / 404 NOT_FOUND / 409 CONFLICT·IN_USE·GENERATION_CONFLICT·SCHEMA_CHANGE_UNSUPPORTED / 413 PAYLOAD_TOO_LARGE / 422 KEY_MODEL_REQUIRED·DDL_REQUIRED·KEY_MODEL_MISMATCH（T-A1 建表确认门：确认要素缺失或自相矛盾，未触碰 Doris）/ 501 NOT_IMPLEMENTED / 502 ENGINE_UNREACHABLE·ENGINE_REJECTED·UNAVAILABLE·DDL_FAILED。
- **时间戳**：ISO-8601 UTC（`2026-10-11T08:00:00Z`；存储层 SQLite `datetime('now')` 序列化时转换）。
- **审计**：所有管理面变更操作写 `audit_log`（DATASOURCE_CREATE/UPDATE/DELETE/TEST、TABLE_CREATE/DELETE、TABLE_DDL_EXECUTE/EVOLVE（T-A1：detail_json 带 DDL 全文 + elapsedMs + ok；执行被 Doris 拒绝同样落行，ok:false + error 原文——拒绝也是审计证据）、NODE_MANUAL_REGISTER/REVOKE/DELETE/DEPLOY/ACTION、ARTIFACT_UPLOAD/DELETE、PIPELINE_CREATE/UPDATE/DELETE/PUBLISH/DEPLOY/RECOVER/ACTION；LOGIN/NODE_REGISTER 沿用 P0；MCP 面 MCP_INITIALIZE/MCP_TOOL_CALL 见 §五）。

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
| ✅ | **建表闭环（T-A1，2026-10-28；蓝图 §7.2 四步：推断→草稿→人工确认→执行审计）** `POST /api/datasources/{id}/tables/{db}/{table}/ddl/draft`（body 可省或 `{keyColumn?,keyModel?:"unique"\|"duplicate",replicationNum?}`——首轮空 body 得纯启发式草稿，二轮回灌人工确认项重生成；仅 doris/starrocks 数据源（生成 Doris 方言 DDL）且表对象须已登记 columnsJson；engine `tailrace-ddl` 纯函数生成：4 类词汇映射 utf8→VARCHAR(65533)/int64→BIGINT/float64→DOUBLE/bool→BOOLEAN、有 sequence_column 或 op_type 列推荐 UNIQUE KEY（MoW）否则 DUPLICATE KEY、键列 id/`*_id` 首列否则第一列**并标注猜测**、时间形列名→按天 RANGE 动态分区候选；回 `{ddl,keyModel,keyModelReason,keyColumn,keyColumnGuessed,partition:{column,guessed}|null,notes[],columns,sequenceColumn}`；**只读不审计**；`replicationNum` 由调用方给（单 BE 集群须传 1，缺省省略属性走集群默认——BE 数探测本波不在 console 内置，向导侧 `SHOW BACKENDS` 自填）；非 doris 类型/无列清单/hint 指向未知列或超纲类型 400、未登记 404） · `POST .../ddl/execute`（body `{ddl,keyModel}`：**`keyModel` 缺省或非 unique/duplicate → 422 KEY_MODEL_REQUIRED**、`ddl` 缺省 → 422 DDL_REQUIRED、`keyModel` 与 DDL 键子句关键字不符 → 422 KEY_MODEL_MISMATCH——三者皆在开连接前拒绝、不落审计；通过后 engine `tailrace-metadata::mysql_exec` 原文执行（不重写、不白名单——人工确认的全文即门）；成功 200 `{ok:true,keyModel,elapsedMs,rowsAffected}` + 审计 TABLE_DDL_EXECUTE（detail 带 DDL 全文/elapsedMs/ok:true）；Doris 拒绝 → **502 DDL_FAILED，message 为 Doris 报错原文**，同样落审计 ok:false+error） · `POST .../ddl/evolve`（无 body：对登记 columnsJson 与 `DESC` 实表（经 infer-schema 同一元数据客户端折叠为 4 类词汇再比对，列名大小写不敏感）做差；仅「登记有、实表无」的列生成 `ALTER TABLE … ADD COLUMN … AFTER <末列>` 逐列链式执行（Light Schema Change）→ 200 `{changed:true,statements[],added[],elapsedMs,partialUpdateNotice}` + 审计 TABLE_DDL_EVOLVE；实表已含全部登记列 → 200 `changed:false` 不执行不审计；**检测到改类型或删列 → 409 SCHEMA_CHANGE_UNSUPPORTED 不执行**，`error.refs` 带 `suggestedDdl[]`（ADD→MODIFY→DROP 顺序的建议全文）/`added`/`retyped[{column,registered,current}]`/`removed`/`partialUpdateNotice` 供人工评估；登记列超纲类型 400、DESC 不可达 502 UNAVAILABLE、ADD COLUMN 被拒 502 DDL_FAILED。**互斥提示（蓝图红线 T-A1 ⑤）**：evolve 每种响应均携带 `partialUpdateNotice`——Doris 中 Schema Change 进行中的表不支持部分列更新（partial update），ADD COLUMN 完成前（`SHOW ALTER TABLE COLUMN` 为 FINISHED）勿对该表启用部分列更新写入；T-A7 落地后同样遵守） |
| ✅ | GET /api/nodes（90s 无心跳判 OFFLINE，只读计算） · GET /api/nodes/{id} |
| ✅ | POST /api/nodes/manual（先探活 `GET http://host:port/healthz`，200 才登记；node_key 由控制面生成 `sn-<16hex>`，per-node token 响应一次性回显，库内仅存 sha256；探活失败 502） |
| ✅ | POST /api/nodes/{id}/revoke（204） · DELETE /api/nodes/{id}（被 assigned 管道引用 409 IN_USE；T35 起 SSH 托管节点先 best-effort 远端卸载（停进程+清安装目录）再删行，失败降级「跳过并警告」：`{"remoteCleaned":bool,"warning":…}`） |
| ✅ | GET /api/nodes/{id}/events?hours=168 · GET /api/nodes/events?hours&level |
| ✅ | GET /api/nodes/{id}/metrics?hours=6（1 分钟桶，窗口钳 1..24h） |
| ✅ | GET /api/nodes/{id}/pipelines-runtime（代理引擎 GET /v1/pipelines 原样透传；不可达 502） |
| ✅ | GET /api/nodes/{id}/deploy-log?limit=100（升序；§4.4 八步链逐步落盘：PREFLIGHT/UPLOAD/UNPACK/CONFIGURE/START/HEALTH_WAIT/REGISTER/REGISTERED，任一步失败附 ROLLBACK 行） |
| ✅ | POST /api/nodes/deploy（T35，2026-10-24；SSH 八步状态机 PENDING→PREFLIGHT→UPLOAD→UNPACK→CONFIGURE→START→HEALTH_WAIT→REGISTER→REGISTERED，russh 纯 Rust 传输。两模式：`hosts[]` 批量新机（控制面建 node 行 register_mode=deployed+生成 node_key/per-node token）或 `nodeId` 重部署（仅 SSH 托管节点）；响应 `{deploys:[{nodeId,status:"PENDING"}]}` 后台推进，进度走 deploy-log。PREFLIGHT=SSH 连通+install dir 可写+prev 链接锚定；UPLOAD=制品流式上传+远端 sha256 核验；UNPACK=解包 `releases/<version>` 并定位 tailrace-node；CONFIGURE=prev 快照+停旧进程（SIGTERM→SIGKILL）+current 软链切换；START=nohup 拉新二进制（--control-plane/--node-key/--state-dir）；HEALTH_WAIT=/healthz 200 窗口；REGISTER=引擎自注册心跳+版本核对（engine_version==制品 version 不符即 FAIL）。**任一步失败→回滚**：prev→current 恢复+重启旧二进制+健康确认，deploy_status=FAILED、artifact_id 不动；首部署无 prev 则移除损坏链接留 FAILED。SSH 凭据=console 部署私钥 `TAILRACE_SSH_KEY`（口令 `TAILRACE_SSH_KEY_PASSPHRASE`），仅公钥认证、不收凭据于请求体；`TAILRACE_CONSOLE_ADVERTISE_URL` 指引擎回连地址、`TAILRACE_DEPLOY_HEALTH_TIMEOUT_SECS`（默认 90）调健康窗口） · POST /api/nodes/{id}/action（T35：start/stop/restart/rollback/uninstall 同步执行（stop=pidfile SIGTERM→SIGKILL；rollback=current⇄prev 对调+重启+健康确认）；upgrade 异步走完整八步链（需 artifactId）；非 SSH 托管节点 400、SSH 侧失败 502 SSH_FAILED） |
| ⛔ 501 | POST /api/nodes/{id}/migrate（P2 三步编排，未在本阶段交付） |
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
7. **维表 join（T-D1）的 SQL 写法与登记**（2026-10-27 增补）：
   - SQL 侧维表引用**不走 dsMapping 改写**，直接写占位符
     `LEFT JOIN ${ds:dim.<datasourceId>.<table>} [AS] <alias> ON …`（parser 契约：
     `dim.` 之后恰两段；`<datasourceId>` 为数字数据源 id，无库名段——库由数据源
     jdbcUrl 钉死）。仅 `INNER JOIN`/`JOIN`/`LEFT [OUTER] JOIN` 接受——RIGHT/FULL
     会对每个微批重发全部未匹配维表行、CROSS 按 |src|×|dim| 放大，编译期拒绝。
   - 维表对象在 mysql/postgresql 数据源下登记：`POST /api/datasources/{id}/tables`
     带 `"kind":"dim"` + `"sequenceColumn":"<主键/单列唯一键列>"`（缺 sequenceColumn
     400——引擎全表加载以该列为分页键且复用 JDBC keyset 唯一性门）；列表接口回
     `sequenceColumn` 字段。
   - publish 门禁同时校验维表引用：可解析 + `kind=dim` + 已声明键列，任一不满足
     400 diagnostics（`err.pipeline.dim_*`）。
   - deploy 的 plan `tables[]` 为维表多带一条元数据：`reference` = SQL 引用去掉
     `dim.` 的两段（`<dsId>.<table>`）、`kind=dim`、`bootstrap` = connJson `jdbcUrl`
     去掉 `jdbc:` 前缀（引擎方言选 `mysql://`/`postgres://`）、`sequence_column` =
     登记键列。表删除守卫对维表按 `${ds:dim.<dsId>.<table>}` 针匹配。
   - `sequence_column` 同样随 jdbc 批源表（kind=jdbc，增量游标列）与 doris
     UNIQUE KEY 去重列下发——此前恒 null，Doris 乱序去重经 console 不可达。
8. **建表闭环（T-A1）的信任边界**（2026-10-28 增补）：
   - 草稿是纯函数（engine `tailrace-ddl`），console 只做门禁与审计：**不选键模型不给执行**
     （422 三码见 §一），execute 的 DDL 原文执行、不重写、不设白名单——人工确认过的
     全文就是门；失败把 Doris 报错原样回传并同样落审计（拒绝也是证据）。
   - 「猜测」必标：键列启发式（id/`*_id` 首列否则第一列）与时间形列名分区候选都带
     `guessed:true`，向导据此高亮并要求确认；人工回灌 `keyColumn` 后标志清零。
   - evolve 只做加法：ADD COLUMN（Light Schema Change）执行；改类型/删列只给建议
     （409 + `refs.suggestedDdl`），永不代执行。类型比对在 4 类词汇层面（DESC 经
     `ddl_arrow_type` 折叠），DECIMAL/DATETIME 等在 Doris 侧折叠为 utf8——与登记类型
     不一致时宁判「改类型」走 409 建议，不静默执行（保守方向；G2 扩词汇后收窄）。
   - **与部分列更新互斥**：Schema Change 进行中的表不支持 partial update，evolve 响应
     恒带 `partialUpdateNotice`；T-A7 的 partial update 开关落地时须读此文案做同一互斥。
   - 回退门：目标 Doris 不支持 Light Schema Change 时 ADD COLUMN 会被 FE 拒绝 → 502
     DDL_FAILED 原文回传（本波未做 FE 版本探测降级，蓝图回退门留 W6/评审轮按实测定）。

## 四、未决 / 后续

- ticket 端点（票据链，600s 单次票据）仍未交付——凭据加密静态半边已落地（见「关键语义裁决」3），票据链为 v1.x 结构性修复。
- ~~SSH 部署链八步 + 节点进程动作~~（T35 已交付，见端点清单；「逐条暂停→执行→自动恢复」的管道级暂停编排仍留 v1.x——当前动作直接作用于进程，不自动暂停在跑管道）。
- catalog 快照浏览 / 样本预览 / lag 告警规则 / SQL 片段助手（后续版本族；discover / infer-schema 已于 T14 交付）。~~jdbc 管道 console 端到端未通~~（T-D1 2026-10-27 清偿大半：plan 下推对 dim/jdbc 表由 connJson `jdbcUrl`（去 `jdbc:` 前缀）填 `bootstrap`、由表对象 `sequence_column` 列填 `sequence_column`——正是 jdbc 源工厂必填两键；维表动线有 console e2e 测试钉住，jdbc 流式批源动线仍未有 console 侧 e2e 覆盖，留后续补）。
- 90s OFFLINE 目前为读时计算；未做后台定时器回写。
- 建表闭环（T-A1）console 侧未内置 BE 数探测（`replicationNum` 由调用方/向导传入）；
  成功建表 + `INSERT INTO` 管道写入对账、ADD COLUMN 后管道重启不失败两条验收走 W6
  testcontainers Doris e2e（console 契约测试只覆盖门禁、审计形状与不可达路径）；
  web 三步建表向导随 W5 交付。

## 五、MCP 面（`POST /mcp`，T-E4 / 蓝图 §7.5）

- **传输**：Streamable HTTP 请求/响应（无服务端 SSE 流）；单 body 单 JSON-RPC 消息，batch 拒绝（-32600）；`initialize` 发 `Mcp-Session-Id`（后续调用按 clientInfo.name 归因，未带回落 `X-MCP-Client`/`User-Agent`/anonymous）。协议版本 `2025-06-18`（echo 旧已知版本）。无 console 会话——全局固定窗口限速（默认 100 req/s，超限 429 + -32002）。
- **写闸门**：四个写工具需**双重确认**——进程级开关 `--mcp-allow-write` / `MCP_ALLOW_WRITE=1` **且**调用参数 `"confirm": true`；缺一以 `WRITE_DISABLED`(-32001) / `INVALID_PARAMS`(code CONFIRM_REQUIRED) 拒绝。
- **审计**：每次工具调用——成功、失败、被拒——都落 `audit_log` 为 `MCP_TOOL_CALL`（actor=`mcp:<client>`），含从未到达工具执行的尝试（未知工具名 / 缺名 / 非对象 arguments，以 UNKNOWN_TOOL / INVALID_PARAMS 登记）；过闸的写调用另产生对应 REST 域审计行（PIPELINE_CREATE 等，operator 归到 MCP 身份）。
- **十工具表**（`tools/list` 顺序；R=读恒开，W=写需闸门）——仓库内正典即 `tailrace-console/src/mcp.rs` 的 `TOOLS` 表与单测 `tool_table_matches_blueprint_7_5`（10 工具、6R+4W、唯一名、写必 confirm），此处镜像供评审对拍：

| # | 工具 | 读写 | 映射 | 关键参数 |
|---|---|---|---|---|
| 1 | `list_datasources` | R | GET /api/datasources | `type?` |
| 2 | `list_pipelines` | R | GET /api/pipelines | `page?` `pageSize?` |
| 3 | `get_pipeline` | R | GET /api/pipelines/{id} | `id` |
| 4 | `get_pipeline_stats` | R | GET /api/pipelines/{id}/stats | `id` |
| 5 | `get_pipeline_logs` | R | 引擎 tail 单快照（SSE 不可折入单结果） | `id` `lines?`（钳 50..500） |
| 6 | `compile_sql` | R | POST /api/pipelines/compile（纯函数） | `sql` `dsMapping?` `pipelineId?` |
| 7 | `create_pipeline` | W | POST /api/pipelines | `name` `sqlContent` `dsMapping?` `checkpointMs?` `preferredNodeId?` `config?` + `confirm` |
| 8 | `deploy_pipeline` | W | POST /api/pipelines/{id}/deploy | `id` `nodeId?` + `confirm` |
| 9 | `pause_resume_pipeline` | W | POST /api/pipelines/{id}/pause·resume | `id` `action`("pause"|"resume") + `confirm` |
| 10 | `trigger_backfill` | W | T-A2 未交付，恒 501 NOT_IMPLEMENTED | `id` `from?` `to?` `dryRun?` + `confirm` |

- **结果信封**：`content`（文本）+ `structuredContent`（`httpStatus` + 处理器 body 原文）+ `isError`（镜像 REST 状态）。
