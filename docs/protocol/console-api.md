# Console API — console-api

> 管理面 REST 协议正典（`tailrace-console` 二进制）。随 utoipa 注解同步维护；
> 机器可读正典 = 运行时 `/api-docs/openapi.json`（58 路径，`tailrace-console/src/openapi.rs` `paths(…)` 登记数，2026-10-30 实测），Swagger UI 在 `/swagger-ui/`。
> 首版 2026-10-11；鉴权协议安全修复改版 2026-10-13；MCP 面正典化 + 维表（T-D1）动线增补 2026-10-27；建表闭环（T-A1）三端点增补 2026-10-28；回填（T-A2）两端点增补 2026-10-28；部分列更新（T-A7）控制面开关 + evolve 双向互斥 + keyModel 登记增补 2026-10-28；R8 类型地基轮同步 2026-10-30（MCP 10→14 工具 + 工具描述 OpenAPI 同源；建表闭环词汇映射 4→13（A3）；infer-schema 收 `debezium-json` + 列型按十六词词汇收窄；draft body 增 `columnTypes`）；R8.5 评审修复轮 2026-10-30（三处 pre-A3 时态残留清偿：openapi 路径计数 49→58、evolve 比对词汇「4 类」→十六词、分区候选规则改为 A3 名称×类型双判——`int64`/`date`/`timestamp` 皆可取 RANGE 键）。

## 一、通用约定

- **Base** `/api`；**认证** `POST /api/auth/login` 签发不透明 token（24h），经 `Cookie: tr_session=…`（HttpOnly; SameSite=Lax；SSE/EventSource 唯一免手工凭据通道）或 `Authorization: Bearer …` 携带。例外免会话端点：`POST /api/auth/login`、`POST /api/nodes/register`、`POST /api/nodes/heartbeat`（m2m，bootstrap/节点 token 在 JSON body 自证）。
- **信封**：成功直返资源 JSON；失败 `HTTP 4xx/5xx + {"error":{"code","message"}}`。唯一例外：`POST /api/pipelines/compile` 的 400 按其自带 diagnostics 形态（`{"ok":false,"statements":[],"diagnostics":[…]}`），前端从 error body 读 diagnostics。
- **操作人**：请求头 `X-Operator` 可选覆盖（默认会话用户名），落审计。
- **错误码语义**：400 INVALID_ARGUMENT / 401 UNAUTHENTICATED·BAD_NODE_TOKEN / 404 NOT_FOUND / 409 CONFLICT·IN_USE·GENERATION_CONFLICT·SCHEMA_CHANGE_UNSUPPORTED·PARTIAL_UPDATE_ACTIVE·SCHEMA_CHANGE_IN_PROGRESS（T-A7 互斥双向：evolve 遇该表已部署的部分列更新管道 / 部分列更新管道部署遇该表 Schema Change 进行中——详见语义裁决 10）/ 413 PAYLOAD_TOO_LARGE / 422 KEY_MODEL_REQUIRED·DDL_REQUIRED·KEY_MODEL_MISMATCH（T-A1 建表确认门：确认要素缺失或自相矛盾，未触碰 Doris）·BACKFILL_SINK_UNSUPPORTED·BACKFILL_SOURCE_UNSUPPORTED·BACKFILL_DIM_UNSUPPORTED·BACKFILL_NO_CREDENTIAL·BOUNDS_TYPE_UNSUPPORTED·BOUNDS_INVALID·STATEMENT_INDEX_RANGE（T-A2 回填能力门：详见语义裁决 9，一律在创建任何 job 行之前拒绝）·PARTIAL_DECLARATION_INVALID（T-A7 部分列发布门在回填 accept 重跑未过——裁决 10「推送侧重门」）/ 501 NOT_IMPLEMENTED / 502 ENGINE_UNREACHABLE·ENGINE_REJECTED·UNAVAILABLE·DDL_FAILED。
- **时间戳**：ISO-8601 UTC（`2026-10-11T08:00:00Z`；存储层 SQLite `datetime('now')` 序列化时转换）。
- **审计**：所有管理面变更操作写 `audit_log`（DATASOURCE_CREATE/UPDATE/DELETE/TEST、TABLE_CREATE/DELETE、TABLE_KEY_MODEL（T-A7：keyModel PUT）、TABLE_DDL_EXECUTE/EVOLVE（T-A1：detail_json 带 DDL 全文 + elapsedMs + ok；执行被 Doris 拒绝同样落行，ok:false + error 原文——拒绝也是审计证据）、NODE_MANUAL_REGISTER/REVOKE/DELETE/DEPLOY/ACTION、ARTIFACT_UPLOAD/DELETE、PIPELINE_CREATE/UPDATE/DELETE/PUBLISH/DEPLOY/RECOVER/ACTION/BACKFILL（T-A2：detail 带 jobId/statementIndex/from/to/labelPrefix；dryRun 只读不审计）；LOGIN/NODE_REGISTER 沿用 P0；MCP 面 MCP_INITIALIZE/MCP_TOOL_CALL 见 §五）。

## 二、端点清单（P1 实装状态）

| 状态 | 端点 |
|---|---|
| ✅ | POST /api/auth/login · POST /api/auth/logout · GET /api/auth/me（P0） |
| ✅ | POST /api/nodes/register · POST /api/nodes/heartbeat（m2m；协议 2026-10-13 安全修复改版：**register 响应携带服务端新生成的 per-node token** `bootstrapToken`（`tr_b_<48hex>`，一次性回显，节点须持久化并在此后 register/heartbeat 携带；每次成功 register 轮换并重新回显）；`node.bootstrap_token` 列仅存 sha256(token)，token 原文不再驻留库中；全局 bootstrap token 仅准入未知 node_key 的**首次注册**——已注册节点持全局 token 一律 401 BAD_NODE_TOKEN；未知节点心跳 401；吊销（digest 置 NULL）401） |
| ✅ | GET /healthz · GET /swagger-ui/ · GET /api-docs/openapi.json |
| ✅ | GET /api/datasource-types（静态注册表自述：kafka/doris/file + mysql/postgresql（T14，jdbc 批源族，jdbcUrl 字段），动态表单单一事实源） |
| ✅ | GET /api/datasources?type= · POST /api/datasources · GET/PUT/DELETE /api/datasources/{id} |
| ✅ | POST /api/datasources/test · POST /api/datasources/{id}/test（HTTP 恒 200，结果在 status 字段） |
| ✅ | POST /api/datasources/{id}/tables（T-A7 起 body 可带 `keyModel:"unique"\|"duplicate"`——Doris 键模型登记，仅 doris/starrocks 数据源接受、其余 400 `err.table_object.key_model_datasource`；缺省 null=未声明） · GET /api/datasources/tables?datasourceId=&kind=（行带 `keyModel`） · **PUT /api/datasources/tables/{id}**（T-A7，2026-10-28：body `{keyModel}` 记录既有表的键模型——建表向导 execute 成功时已自动记录、此端点给 console 之外建的表补声明；200 `{id,keyModel}` + 审计 TABLE_KEY_MODEL；缺省/非 unique\|duplicate 400、非 doris 表 400、不存在 404） · DELETE /api/datasources/tables/{id} |
| ✅ | POST /api/datasources/{id}/discover（T14，2026-10-21；非 501——engine `tailrace-metadata` 内嵌转发：kafka 经 rdkafka fetch_metadata 列 topics（首点拆 db.table，带 partitions）；doris/mysql 走 9030 mysql **文本协议** SHOW DATABASES/SHOW TABLES；postgres 走 information_schema（扩展协议绑定参数）；回 `{"assets":[{dbName,tableName,kind,format:null,partitions?}]}`，kind 按 `table_kind_for` 派生（kafka→kafka、doris→doris、mysql/postgresql→jdbc）；成功缓存 30s（键=datasource id+generation+connJson 逐字节，失败不缓存以便改参即重探）；file 型/connJson 缺字段 400、数据源不存在 404、后端不可达 502） · POST /api/datasources/{id}/infer-schema（body `{dbName,tableName,format?,sampleRows?}`：kafka 尾采 sampleRows 条（默认 20 钳 1..=100）经 decode 链类型格推断列 + 给出 format 建议（显式 format 门 ogg/json/jsonl/**debezium-json**——R8 A3 把 console 侧门加宽到 debezium-json，引擎侧元数据客户端本就走 Debezium 外壳，`api/datasources.rs:1200`；其它显式格式 400）；SQL 后端读 DDL 列型（DESC / information_schema.columns）**按十六词词汇映射**（R8 A3 收窄：tinyint/smallint/int→int8/int16/int32、decimal(p,s) 保参、datetime/timestamp→timestamp(us)、date→date、text 族→string，未识别型折 utf8——`tailrace-metadata/src/infer.rs:257` `ddl_arrow_type` / `:296` `pg_arrow_type`），format 恒 null；回 `{"columns":[[列名,词汇型]…],"format":null|"ogg"|"json"}`——**不落库**，登记仍走 POST /tables；同档 400/404/502） |
| ✅ | **建表闭环（T-A1，2026-10-28；蓝图 §7.2 四步：推断→草稿→人工确认→执行审计；R6 评审修复轮补分区可执行性规则）** `POST /api/datasources/{id}/tables/{db}/{table}/ddl/draft`（body 可省或 `{keyColumn?,keyModel?:"unique"\|"duplicate",replicationNum?,partition?,columnTypes?}`——首轮空 body 得纯启发式草稿，二轮回灌人工确认项重生成；`columnTypes`（R8 A3，`[[列名,词汇型]…]`）为向导列步骤的逐列类型覆盖：**只导向本次草稿、不改写登记 columnsJson**（推断说 utf8、人知道是金额就选 decimal(18,2)），名字不在登记集 400、类型由生成器词汇门校验（`api/datasources.rs:1587-1594`）；仅 doris/starrocks 数据源（生成 Doris 方言 DDL）且表对象须已登记 columnsJson；engine `tailrace-ddl` 纯函数生成：**13 词映射**（R8 A3 加宽，`tailrace-ddl/src/lib.rs:385` `doris_type`）utf8→VARCHAR(65533)、string→STRING、int8/int16/int32/int64→TINYINT/SMALLINT/INT/BIGINT、float32/float64→FLOAT/DOUBLE、decimal(p,s)→DECIMAL(p,s)、timestamp(unit)→DATETIME(6)、date→DATE、bool→BOOLEAN、binary→STRING（base64 约定）——`list<float32>`/`variant`/`null` 三词**未映射即报错不折叠**（A4/A5 的 Doris 侧映射留后续轮）、有 sequence_column 或 op_type 列推荐 UNIQUE KEY（MoW）否则 DUPLICATE KEY、键列 id/`*_id` 首列否则第一列**并标注猜测**、时间形列名→按天 RANGE 动态分区候选（**A3 起名称×类型双判：时间形名字选候选、类型判可执行——`int64`/`date`/`timestamp(unit)` 可当 RANGE 分区键（时间类型直取、生成真日期边界；int64 走 yyyyMMdd 整数区间约定），UNIQUE/MoW 表分区列必须为键列——其余形态在生成阶段即被跳过并在 notes 说明原因，落库必败的 PARTITION 子句不再产出**，Doris 2.1.7 实测规则，`tailrace-ddl/src/lib.rs` `range_key_capable` / `partition_candidate`）；`partition:false` 取消分区子句、`partition:true` 确认（guessed 清零）；回 `{ddl,keyModel,keyModelReason,keyColumn,keyColumnGuessed,partition:{column,guessed}\|null,notes[],columns,sequenceColumn}`；**只读不审计**；`replicationNum` 由调用方给（单 BE 集群须传 1，缺省省略属性走集群默认——BE 数探测本波不在 console 内置，向导侧 `SHOW BACKENDS` 自填）；非 doris 类型/无列清单/hint 指向未知列或超纲类型 400、未登记 404） · `POST .../ddl/execute`（body `{ddl,keyModel}`：**`keyModel` 缺省或非 unique/duplicate → 422 KEY_MODEL_REQUIRED**、`ddl` 缺省 → 422 DDL_REQUIRED、`ddl` 键子句与 `keyModel` 不符（**须恰含一个键模型关键字——双含或全无均拒**）→ 422 KEY_MODEL_MISMATCH——三者皆在开连接前拒绝、不落审计；通过后 engine `tailrace-metadata::mysql_exec` 原文执行（不重写、不白名单——人工确认的全文即门）；成功 200 `{ok:true,keyModel,elapsedMs,rowsAffected}` + 审计 TABLE_DDL_EXECUTE（detail 带 DDL 全文/elapsedMs/ok:true）**并把 keyModel 记到表对象**（T-A7：部分列更新发布门禁的依据，向导建的表无需再 PUT）；Doris 拒绝 → **502 DDL_FAILED，message 为 Doris 报错原文**，同样落审计 ok:false+error） · `POST .../ddl/evolve`（无 body：对登记 columnsJson 与 `DESC` 实表（经 infer-schema 同一元数据客户端 `ddl_arrow_type` 收窄为**十六词词汇**再比对——R8 A3 口径，同裁决 8；列名大小写不敏感）做差；仅「登记有、实表无」的列生成 `ALTER TABLE … ADD COLUMN … AFTER <末列>` 逐列链式执行（Light Schema Change）→ 200 `{changed:true,statements[],added[],elapsedMs,partialUpdateNotice,partialUpdatePipelines[]}` + 审计 TABLE_DDL_EVOLVE；实表已含全部登记列 → 200 `changed:false` 不执行不审计；**检测到改类型或删列 → 409 SCHEMA_CHANGE_UNSUPPORTED 不执行**，`error.refs` 带 `suggestedDdl[]`（ADD→MODIFY→DROP 顺序的建议全文）/`added`/`retyped[{column,registered,current}]`/`removed`/`partialUpdateNotice` 供人工评估；登记列超纲类型 400、DESC 不可达 502 UNAVAILABLE、ADD COLUMN 被拒 502 DDL_FAILED。**互斥（蓝图红线 T-A1 ⑤，T-A7 升级为双向约束）**：evolve 侧——该表上存在**已部署**（RUNNING/PAUSED 且有指派节点）的部分列更新管道（已发布 SQL 对该汇带 INSERT 列清单）时 → **409 PARTIAL_UPDATE_ACTIVE**，`refs.pipelines[{id,name,status,deployed,columns}]` 点名、不触碰 Doris（门在 DESC 之前——按表而非按差集：即便实表已无需加列也先拒，保守代价一次停/重部署，另一边代价是管道半夜拒载）；每种响应仍携带 `partialUpdateNotice`（文案已双向化），200 另带 `partialUpdatePipelines[]`——该表上所有声明部分列的管道（任何状态，此时无一已部署）作为 ADD COLUMN 完成后的重部署清单。部署侧见 deploy 行） |
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
| ✅ | POST /api/pipelines/compile（console 内嵌 tailrace-parser 本地编译；`db.table`×dsMapping→`${ds:id.db.table}` 物化后编译；diagnostics 走 400 自带形态。**T-A7 部分列更新开关（2026-10-28）**：`INSERT INTO sink (col, …) SELECT …` 的 **INSERT 列清单即语句级部分列声明**——`statements[].partialColumns` 回声明顺序的列名数组，不带列清单的语句为 `null`（全行写入，历史默认，语句其余形态逐字节不变）；列清单含重复列由 parser 拒绝（400 diagnostics「duplicate column」）） |
| ✅ | POST /api/pipelines/{id}/publish（门禁：语句引用表对象全部可解析→version+1→`pipeline_version` 快照。**T-A7 部分列门禁**：带列清单的语句其汇须 ①登记 kind=doris（kafka/file 无部分更新语义、starrocks 的 partial update 是另一套装载契约引擎未接——`err.pipeline.partial_sink_kind`）②键模型已登记且为 unique（Doris 部分列更新仅对 UNIQUE KEY merge-on-write 表合法，DUPLICATE 表的 partial load 会被 Doris 拒载——未登记 `err.pipeline.partial_key_model_missing`、非 unique `err.pipeline.partial_key_model_wrong`）③列清单 ⊆ 登记 columnsJson（`err.pipeline.partial_column_unknown`）④同一汇的所有语句声明一致——同带同列或都不带（plan 一汇一条表项，`err.pipeline.partial_sink_conflict`）；任一不满足 400 diagnostics，不带列清单的管道完全不受影响） |
| ✅ | POST /api/pipelines/{id}/deploy（plan 下推：nodeId 缺省自动分配——preferred 合格优先→运行管道最少→id 升序；引擎 ok:false→DEPLOY_FAILED+502。**T-A7**：声明了部分列的汇其 plan `tables[]` 表项多带 `partial_columns:[…]`（snake_case，引擎 PlanTableObject 同名字段、DorisSink 据此开 `partial_columns=true` 装载模式）——**未声明的表项不带该键**，与 A7 前形态逐字节一致；**推送侧重门（R7 评审修复轮）**：推送前对 spec 快照**重跑 publish 的部分列门禁**（kind=doris / keyModel=unique / 列清单 ⊆ 登记列），任一不满足 → 400 diagnostics（与 publish 同形，不推送、状态不动）——A7 前发布的带列清单快照（当时 parser 忽略列清单=全行写、无门）与发布后键模型被改登记的快照都在此被拦，而非无声翻成 partial 模式后被 Doris 逐批拒载；**部署侧互斥（蓝图红线 T-A1 ⑤ 双向化）**：推送前对每个部分列汇实时查 `SHOW ALTER TABLE COLUMN FROM db WHERE TableName=…`，存在非 FINISHED/CANCELLED 的作业 → **409 SCHEMA_CHANGE_IN_PROGRESS**（`refs.{sink,table,jobs[{jobId,state}]}`，不推送、管道状态不动），FE 不可达/查询失败 → **502 UNAVAILABLE**（与 DDL 端点「DESC 不可达」同口径——前置检查答不出就不部署，不猜）；无部分列汇的管道零 FE 往返） · POST /api/pipelines/{id}/recover（重推当前 plan 到指派节点；同一部署侧互斥前置检查） |
| ✅ | POST /api/pipelines/{id}/pause · resume · stop（代理引擎 POST /v1/pipelines/{id}/{action}；stop 回 PUBLISHED 并清指派） |
| ✅ | GET /api/pipelines/{id}/stats · GET /api/pipelines/{id}/offsets（引擎透传，ok:false 也 200；不可达 502） |
| ✅ | GET /api/pipelines/{id}/logs?lines=200（SSE 桥：首屏 tail=1&limit 钳 50..500 → 1.5s after_seq 增量轮询；15s keepalive；300s 会话上限；引擎瞬断连续 5 次发 eof） |
| ✅ | GET /api/pipelines/{id}/metrics/trend?hours=24（5 分钟桶；速率=相邻桶差分/300s） |
| ✅ | GET /api/pipelines/templates（SQL 模板内嵌） |
| ✅ | **回填（T-A2，2026-10-28；蓝图 §7.3）** `POST /api/pipelines/{id}/backfill`（body `{statementIndex?,from:{"type":"offset","value":N},to:{…},dryRun?}`——from/to 当前仅 offset 边界，file 源按字节位、kafka 源暂不支持【见语义裁决 9】；`dryRun:true` → 200 `{jobId:null,estimatedRows:null,labelPrefix}` **零副作用**——不建 job 行、不落审计、零执行；`dryRun:false` → 202 `{jobId,state:"pending",estimatedRows:null,labelPrefix}` + 审计 PIPELINE_BACKFILL + 后台执行（**控制面进程内执行**：W2 引擎 `run_bounded` + `bf-{jobId}` 台账命名空间 + Deterministic label——见语义裁决 9）；能力门 422：汇非 doris/starrocks（BACKFILL_SINK_UNSUPPORTED）/ 源非 file（BACKFILL_SOURCE_UNSUPPORTED）/ 语句含维表 JOIN（BACKFILL_DIM_UNSUPPORTED）/ 汇数据源未存凭据（BACKFILL_NO_CREDENTIAL）/ timestamp 边界（BOUNDS_TYPE_UNSUPPORTED）/ from≥to 或非整数值（BOUNDS_INVALID）/ statementIndex 越界（STATEMENT_INDEX_RANGE）——全部在创建 job 行之前拒绝；**T-A7**：目标语句带 INSERT 列清单时，accept 路径（非 dryRun）先**重跑 publish 的部分列门禁**（kind=doris / keyModel=unique / 列清单 ⊆ 登记列；未过 → 422 PARTIAL_DECLARATION_INVALID，message 内嵌发布门诊断原文，R7 评审修复轮），再走与 deploy 相同的部署侧互斥前置检查（409 SCHEMA_CHANGE_IN_PROGRESS / 502 UNAVAILABLE，同样在建 job 行之前），执行器 `bounded_plan` 把列清单投影到汇表项、`backfill_doris_sink` 据此以 `partial_columns=true` 装载——与常驻引擎的 sink_factory 同两跳） · `GET /api/pipelines/{id}/backfill/jobs`（该管道的 job 进度列表，新到旧：`{jobs:[{jobId,statementIndex,from,to,state,rowsWritten,error,createdAt,finishedAt}]}`，state 机 pending→running→done\|failed，终态不可变） |
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
     `guessed:true`，向导据此高亮并要求确认；人工回灌 `keyColumn` / `partition:true` 后
     标志清零，`partition:false` 取消分区子句（**分区候选必须先可执行才会被给出**，
     A3 起按名称×类型双判——时间形**名字**选候选、**类型**判可执行：
     Doris 2.1 实测 RANGE 分区键接受整数与日期时间类型——十六词词汇里 `int64`→BIGINT、
     `date`→DATE、`timestamp(unit)`→DATETIME(6) 合格（时间类型直取 RANGE 键，DATETIME/DATE
     列上动态按天分区生成真日期边界），utf8/string/float/bool/decimal 等其余类型的时间形列
     一律跳过并在 `notes` 给出原因（`tailrace-ddl/src/lib.rs` `range_key_capable`）；UNIQUE
     （MoW）表的分区列必须是键列，非键列候选同样跳过并说明；int64 候选附带「值须为
     yyyyMMdd 整数日期，epoch 秒/毫秒会落在所有分区之外被拒载」的警示——该整数约定仅限
     BIGINT，动态按天分区生成 p20261028=[20261028,20261029) 形态的整数区间；date/timestamp
     候选生成真日期边界，无此警示）。
   - execute 的键模型确认门比对键子句：echo 回的 DDL 文本须**恰含一个**键模型关键字
     （UNIQUE KEY / DUPLICATE KEY）且与确认值一致——双含（如注释里走私另一个模型）或
     全无均 422 KEY_MODEL_MISMATCH，不执行不落审计。
   - evolve 只做加法：ADD COLUMN（Light Schema Change）执行；改类型/删列只给建议
     （409 + `refs.suggestedDdl`），永不代执行。类型比对在**十六词词汇**层面（DESC 经
     `ddl_arrow_type` 折叠——R8 A3 起 DECIMAL/DATETIME/DATE/TEXT 族各有其词、不再折成
     utf8，G2 扩词汇后的收窄已落地：`tailrace-metadata/src/infer.rs:257`）；ADD COLUMN
     的列型经 `tailrace_ddl::doris_type` 13 词映射、超纲型显式报错不折叠
     （`api/datasources.rs:1763`）——与登记类型不一致时宁判「改类型」走 409 建议，
     不静默执行（保守方向）。
   - **与部分列更新互斥**：Schema Change 进行中的表不支持 partial update，evolve 响应
     恒带 `partialUpdateNotice`；T-A7 落地后升级为**双向约束**（evolve 遇已部署的部分列
     管道 409、部分列管道部署遇 Schema Change 进行中 409）——详见裁决 10。
   - 回退门：目标 Doris 不支持 Light Schema Change 时 ADD COLUMN 会被 FE 拒绝 → 502
     DDL_FAILED 原文回传（本波未做 FE 版本探测降级，蓝图回退门留 W6/评审轮按实测定）。

9. **回填（T-A2）的执行位置与信任边界**（2026-10-28 增补）：
   - **控制面进程内执行**是有意为之的例外（常态是「console 经 /v1 驱动 node」）：引擎侧
     没有有界执行端点（W2 只交付了 runtime API `run_bounded` + `backfill_state_dir`），
     而 console 已拥有有界重放所需的全部要素——已发布 spec、plan 表解析
     （`build_plan_table` 与 deploy 共用）、密封凭据。代价：console 二进制链接执行栈
     （datafusion/rdkafka，体积显著增大，W7 收口时复测发布体积）。
   - **游标隔离**：执行器组装在 `<data-dir>/backfill/bf-{jobId}/`（W2 命名空间），
     成功/失败一律不读不写主管道台账（主管道台账在引擎节点自身的 state dir，控制面
     进程物理上不可达；console 的 `pipeline` 行也从不被执行器写入——契约测试断言行
     逐字节稳定）。
   - **确定性 label 族与 job 无关**：label 前缀 = `tr-p{pipelineId}-s{statementIndex}-bf`，
     完整 label = `{前缀}-{statementIndex}-{sha256(from,to)前8位}_{批序号}`（W2 渲染）。
     **刻意不把 jobId 编进前缀**——同区间换 job 重放必须重现同一批 label 才能触发
     Doris「label already exists」去重（F1 的病灶正是换进程换 label）；把 jobId 编进去
     会让每个新 job 都拿到全新 label，在 DUPLICATE KEY 表上双写——正是要修的 bug。
     去重窗口 = Doris label 保留期（默认 3 天），超期重放靠表键模型兜底（W2 文档同款
     警示）。汇工厂凭据来自 console 密封库（解密为 `Basic base64(user:pass)` 直连
     stream load，不经引擎进程环境变量）。
   - **kafka 源暂 422**：标量 `from` 无法寻址 kafka 分区游标（W2 的 `seed_from_cursor`
     明示告警并回落到组的起始位置）——静默超集重放在 DUPLICATE KEY 表上是真数据事故，
     诚实拒绝优于惊喜；分区级播种（console 侧预写 Kafka 游标台账）留后续批次。当前
     可回填源 = file（from/to 为文件字节位，行精确）；`estimatedRows` 因 file 边界是
     字节位而恒 null（真实行数看执行后 job 的 `rowsWritten`）。
   - **job id** 为 `{yyyymmdd}-{序号}` 文本主键（进台账目录名、审计与隔离消费组
     `bf-{jobId}.s{n}`；消费组隔离保证回填期间主管道消费组不受 rebalance 扰动）。
   - **file 源路径按登记目录解析**（R6 评审修复轮）：回填执行器在 console 进程内
     组装 file 源，但表文件**不按 console 进程 cwd 解析**——`build_plan_table` 把 file
     数据源 connJson 的 `path`（登记目录）作为 plan `bootstrap` 下发，引擎 file 工厂
     以 `目录/表名` 打开源文件（与常驻 node 同一解析），console 以任意目录启动都能读
     到同一文件；未登记目录（TOML 本地 catalog 无 bootstrap）则沿用按 cwd 的裸名解析。
   - **崩溃恢复**：有界重放不跨进程续跑——console 启动时把遗留 `pending`/`running`
     的 job 行统一置为 `failed`（error 说明「重新触发同区间即可，确定性 label 保证
     重放幂等」）；终态行不动。
   - dryRun 响应的 `jobId` 恒 null（零副作用契约：dry run 不创建任何行）；MCP
     `trigger_backfill` 缺省 `dryRun:true`（蓝图 §7.5）。

10. **部分列更新（T-A7）的开关形态与双向互斥**（2026-10-28 增补）：
   - **开关 = SQL 本身**：`INSERT INTO sink (k, a) SELECT k, a FROM src` 的 INSERT 列清单就是
     语句级部分列声明，无 planOptions 开关、无表级登记项——parser 把它编进
     `StatementSpec.partial_columns`（引擎 catalog 字段，serde-default，A7 前的 spec 原样
     有效），console compile 回 `partialColumns`，publish 门禁校验，deploy/recover/backfill
     把它投影到汇的 plan 表项 `partial_columns`——**运行时读的是表项**（引擎 W2 的
     `TableObject.partial_columns` → `DorisSink::with_partial_columns`），语句字段是编译期
     记录；两者由 console 从同一条 SQL 推导，不会不一致。不带列清单 = 全行写入，plan 表项
     不带该键，与 A7 前逐字节一致（契约测试钉住）。
   - **存量列清单快照的语义变化与推送侧重门**（R7 评审修复轮，2026-10-29）：A7 之前 parser
     **忽略** INSERT 列清单（`INSERT INTO sink (a,b) SELECT …` 与裸 INSERT 等价、全行写入），
     A7 起同一条 SQL 的列清单成了部分列开关——这对已发布快照（`spec_json`）是**语义反转**：
     快照不重编译就不会被 publish 门禁再看一眼。因此 deploy / recover / backfill-accept 在推送前
     **重跑 publish 的部分列门禁**（同一函数、同一诊断：kind=doris / keyModel=unique / 列清单 ⊆
     登记列；deploy/recover 400 diagnostics，backfill 422 PARTIAL_DECLARATION_INVALID）——
     升级后存量快照首次推送若落在 DUPLICATE/未登记键模型的表上在此被拦并指向 PUT keyModel 或
     去掉列清单重发布，而不是无声进入 partial 模式后被 Doris 逐批拒载（"Only unique key merge
     on write support partial update"）；落在已登记 unique 的表上则按 A7 语义进入部分列写入
     （publish 本就会放行的形态）。门禁对无列清单的管道零成本、零变化。仓内模板/文档/测试无
     带列清单的存量用法（`git grep` 零命中），影响面限用户库。
   - **为什么要登记键模型**：Doris 的 partial update 仅对 UNIQUE KEY merge-on-write 表合法，
     DUPLICATE 表的 partial load 在装载时才被拒——那是部署之后、管道运行中的失败。console
     把这条约束前移到 publish：表对象新增 `keyModel`（迁移 03，`unique`/`duplicate`/null），
     建表向导 execute 成功自动记录（向导生成的 UNIQUE 恒带 `enable_unique_key_merge_on_write`），
     console 之外建的表经登记 body 或 `PUT /api/datasources/tables/{id}` 声明；**未登记也拒**
     （「不知道」不等于「安全」——文案指向 PUT）。MoW/MoR 之别 console 看不见：声明 unique
     即视为 MoW，MoR 表由 Doris 自身拒载兜底。
   - **双向互斥的两半**：① evolve 侧（console 自有知识，零 Doris 往返）——该表上有**已部署**
     （RUNNING/PAUSED + 有指派）的部分列管道即 409 PARTIAL_UPDATE_ACTIVE，点名管道，先停再演进；
     门放在 DESC 之前、按表不按差集（保守：无需加列时也先拒，代价一次停/重部署）。
     ② 部署侧（实时真值）——deploy/recover/backfill-accept 对每个部分列汇查
     `SHOW ALTER TABLE COLUMN`，非 FINISHED/CANCELLED 作业存在即 409
     SCHEMA_CHANGE_IN_PROGRESS；**FE 不可达走 502 UNAVAILABLE 而非放行**——与 DDL 端点
     「DESC 不可达」同口径：前置检查答不出时部署等于赌一把，拒绝比猜稳。代价：部分列管道的
     部署多一次 FE 往返并依赖 FE 可达（全行管道零影响）；CI 无 Doris，因此集成测试锚的是
     「前置检查先于引擎推送/先于建 job 行」与 502 口径，进行中→409 的**响应形状**（code /
     message 点名 `id=STATE` / `refs.{sink,table,jobs[{jobId,state}]}`）由纯函数单测钉住
     （R7 评审修复轮，`schema_change_refusal`），进行中的**触发**与 MoW「未写列保值」
     语义由 W4 e2e 对在役 Doris 实跑（空表 Light Schema Change 瞬时完成，在飞作业尚未能
     在 e2e 构造——dogfooding 报告如实记录）。
   - **starrocks 在 publish 即拒**（`err.pipeline.partial_sink_kind`）：引擎 W2 的 sink_factory
     对 starrocks + partial_columns 拒绝装配（StarRocks 的 partial update 是主键表 +
     `partial_update` 头的另一套契约，T12 矩阵未接），console 把这一拒绝前移到发布。
     **引擎侧通用兜底**（R7 评审修复轮）：runtime 装配时对任何声明了 `partial_columns` 的汇检查
     G4 `capabilities().partial_update`——为 false（file/kafka 及未来任何未实现部分更新的汇）即
     拒绝装配并点名能力缺失，而不是把投影收窄后无声写出缺列的行；console 之外的独立路径（TOML
     FileCatalog、手工 plan body）由此与 publish 门同一口径。
   - **回填执行器同样接线**：`bounded_plan` 投影 + `backfill_doris_sink` 应用
     `with_partial_columns`（镜像引擎工厂，含 starrocks 拒绝）——契约测试直接驱动
     `backfill::run_job` 对 header 捕获型 mock BE 断言 `partial_columns: true` **与 `columns:
     <子集>` 双头**（后者缺失时 Doris 2.1.7 会把 json partial load 解析为全列、未写列置 NULL 且
     Status Success——W4 实探结论，R7 评审修复轮把该头纳入契约锁定）且 JSON 行仅含
     声明列（HTTP accept 路径在 CI 里被 FE 前置检查挡在 502，故绕过 handler 直驱执行器）。

## 四、未决 / 后续

- web 客户端类型（`web/src/api/datasources.ts` 的 `DdlEvolveResponse`、pipelines 的 `StatementOut`/表对象 DTO）尚未同步 T-A7 新字段（`partialUpdatePipelines`、`partialColumns`、`keyModel`）与新端点 PUT /tables/{id}——TS 类型非穷尽校验，运行时忽略多出字段不致破坏，但向导/编辑器尚无 UI 呈现；web 不在 W3 文件域，留后续波。
- ticket 端点（票据链，600s 单次票据）仍未交付——凭据加密静态半边已落地（见「关键语义裁决」3），票据链为 v1.x 结构性修复。
- ~~SSH 部署链八步 + 节点进程动作~~（T35 已交付，见端点清单；「逐条暂停→执行→自动恢复」的管道级暂停编排仍留 v1.x——当前动作直接作用于进程，不自动暂停在跑管道）。
- catalog 快照浏览 / 样本预览 / lag 告警规则 / SQL 片段助手（后续版本族；discover / infer-schema 已于 T14 交付）。~~jdbc 管道 console 端到端未通~~（T-D1 2026-10-27 清偿大半：plan 下推对 dim/jdbc 表由 connJson `jdbcUrl`（去 `jdbc:` 前缀）填 `bootstrap`、由表对象 `sequence_column` 列填 `sequence_column`——正是 jdbc 源工厂必填两键；维表动线有 console e2e 测试钉住，jdbc 流式批源动线仍未有 console 侧 e2e 覆盖，留后续补）。
- 90s OFFLINE 目前为读时计算；未做后台定时器回写。
- 回填（T-A2）kafka 源暂 422（分区级 from 寻址未做，见语义裁决 9）；同区间重放
  行数不变（DUPLICATE KEY 实表）与「回填期间主管道持续写入」两条验收由
  `scripts/e2e/backfill_replay.sh` 覆盖（R6 评审修复轮起 console 从仓库根启动、
  file 源按登记目录解析——`rowsWritten>0` 断言专防 0 行空洞成功）；`estimatedRows`
  为记录在案的验收偏离：file 边界是字节位、无源可诚实估行，恒 null（见语义裁决 9）。
- 建表闭环（T-A1）console 侧未内置 BE 数探测（`replicationNum` 由调用方/向导传入）；
  成功建表 + `INSERT INTO` 管道写入对账（含**分区表**：候选→取消→确认→落库→
  SHOW PARTITIONS→数据落分区）与 evolve 两条验收（**ADD COLUMN 后管道 stop/deploy
  重启不失败 + 数据继续落表**、**改类型 409 + refs 建议 + 实表不变**）由
  `scripts/e2e/ddl_create.sh` 覆盖（console 契约测试钉门禁、审计形状与不可达路径）；
  web 三步建表向导随 W5 交付。

## 五、MCP 面（`POST /mcp`，T-E4 / 蓝图 §7.5）

- **传输**：Streamable HTTP 请求/响应（无服务端 SSE 流）；单 body 单 JSON-RPC 消息，batch 拒绝（-32600）；`initialize` 发 `Mcp-Session-Id`（后续调用按 clientInfo.name 归因，未带回落 `X-MCP-Client`/`User-Agent`/anonymous）。协议版本 `2025-06-18`（echo 旧已知版本）。无 console 会话——全局固定窗口限速（默认 100 req/s，超限 429 + -32002）。
- **写闸门**：六个写工具需**双重确认**——进程级开关 `--mcp-allow-write` / `MCP_ALLOW_WRITE=1` **且**调用参数 `"confirm": true`；缺一以 `WRITE_DISABLED`(-32001) / `INVALID_PARAMS`(code CONFIRM_REQUIRED) 拒绝。
- **审计**：每次工具调用——成功、失败、被拒——都落 `audit_log` 为 `MCP_TOOL_CALL`（actor=`mcp:<client>`），含从未到达工具执行的尝试（未知工具名 / 缺名 / 非对象 arguments，以 UNKNOWN_TOOL / INVALID_PARAMS 登记）；过闸的写调用另产生对应 REST 域审计行（PIPELINE_CREATE 等，operator 归到 MCP 身份）。
- **十四工具表**（`tools/list` 顺序；R=读恒开，W=写需闸门）——蓝图 §7.5 的十工具 + R8 四工具（建表闭环 `ddl_draft`/`ddl_execute`/`ddl_evolve` 与回填进度 `backfill_status`，≤16 预算内）；仓库内正典即 `tailrace-console/src/mcp.rs:90` 的 `TOOLS` 表与单测 `tool_table_matches_the_registered_surface`（`mcp.rs:1216`：14 工具、8R+6W、唯一名、写必 confirm）。**工具描述自 R8 起由 OpenAPI 文档同源生成**（`openapi_tool_description`，`mcp.rs:748`——`tools/list` 的 description 取自 utoipa 注解，无第二份手写文案可漂移）。此处镜像供评审对拍：

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
| 10 | `trigger_backfill` | W | POST /api/pipelines/{id}/backfill（T-A2 已接真；`dryRun` 缺省 true——蓝图 §7.5 MCP 恒预览优先） | `id` `from` `to`（`{"type":"offset","value":N}` 对象）`statementIndex?` `dryRun?` + `confirm` |
| 11 | `ddl_draft` | R（只组文本、不触 Doris） | POST /api/datasources/{id}/tables/{db}/{table}/ddl/draft | `datasourceId` `db` `table` + draft body 原样透传（`keyColumn?` `keyModel?` `replicationNum?` `partition?` `columnTypes?`） |
| 12 | `ddl_execute` | W | POST …/ddl/execute | `datasourceId` `db` `table` `ddl` `keyModel` + `confirm` |
| 13 | `ddl_evolve` | W | POST …/ddl/evolve | `datasourceId` `db` `table` + `confirm` |
| 14 | `backfill_status` | R | GET /api/pipelines/{id}/backfill/jobs | `id` |

- **结果信封**：`content`（文本）+ `structuredContent`（`httpStatus` + 处理器 body 原文）+ `isError`（镜像 REST 状态）。
