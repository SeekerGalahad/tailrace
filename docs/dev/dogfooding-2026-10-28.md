# Dogfooding round 1（2026-10-28）—— 自用全链真跑与摩擦清单

> R7 T-A8。对本机**在役** Doris 真跑 tailrace 自己的全链：A1 四步建表（API 层）→ file 源
> 管道写入 → A2 回填 dryRun + 执行 + 重放；同轮 T-A7 e2e 写死断言前的裸 stream load 探针
> 把 W2 引擎的一个形状错误钉了出来。全部结论来自本轮实跑，证据路径逐条给出。
> 可重跑：`bash scripts/dogfood/round1.sh --keep`（22 步，实测 13s）；
> `bash scripts/e2e/partial_update.sh`（compose 2.1.7，38 步）或
> `TAILRACE_E2E_DORIS_MYSQL="docker exec -i doris-2.1 mysql" bash scripts/e2e/partial_update.sh`
> （在役栈外部模式，37 步）。

## 一、对象与事实

| 项 | 实测 |
|---|---|
| 在役栈 | 容器 `doris-2.1`（FE，`apache/doris:fe-3.1.4`，宿主 9030/8030）+ `doris-be`（`be-3.1.4`，8040）；`SHOW FRONTENDS/BACKENDS` Version = **doris-3.1.4-rc02-7f5ba43de6**，BE Alive=1 |
| compose 栈 | `docker-compose.yml` 的 doris-fe/doris-be = **2.1.7**（39030/38040），R6 配方拉起（BE swap override + `vm.max_map_count=2000000`） |
| 二进制 | `./scripts/build.sh` release（含引擎修复 5471782） |
| 磁盘 | 开工 12GiB 可用 → 收工 7GiB（release 构建 ×3 + 两套栈）；全程未触 5GiB 红线 |

**任务卡写"在役 Doris 2.1.7"，实测在役是 3.1.4**；2.1.7 只在 compose 栈。本轮两边都跑了
（摩擦 #2）。

## 二、操作实录

### 2.1 探针（写断言前，裸 curl，compose 2.1.7）

`scripts/e2e/evidence/partial_update-probe-2026-10-28/{probe.sh,probe2.sh,probe.out,probe2.out,README.md}`。
矩阵：UNIQUE MoW / DUPLICATE / UNIQUE MoR / MoW+`function_column.sequence_col` / MoW 无序列属性，
× `partial_columns` 有无 `columns` 头 × `sequence_column` 头。关键输出见 README 六条结论，
其中第 1 条直接导致引擎修复（§三 摩擦 #1）。

### 2.2 T-A7 e2e：`scripts/e2e/partial_update.sh`

| 轮 | 对象 | 结果 | 日志 |
|---|---|---|---|
| 首跑 | compose 2.1.7 | 36/38——核心 T2.2/T5.4 通过；2 FAIL 均为脚本自身 bug（jq `// empty` 吞 `false`；断言前多发 GET 覆盖 `$CODE`），已修 | `evidence/.../e2e-run1-36of38.log` |
| 复跑 | compose 2.1.7（完整 `up --wait` 路径 + 外科式 teardown） | **38/38** | `evidence/.../e2e-run2-38of38.log` |
| 外部模式 | 在役 3.1.4（`TAILRACE_E2E_DORIS_MYSQL`，专库 DROP FORCE 收尾） | **37/37**（外部模式无 S0.5 sysctl 步） | `evidence/.../e2e-ext-standing-3.1.4-37of37.log` |

断言覆盖（与任务卡验收逐条对应）：
- **MoW 表第一轮写 5 列 → partial 只写 2 列 → 未写 3 列保留第一轮值**：T1.2 基线逐行校验
  （note=`first-i`、price=`i+0.5`、flag=`i%2==0`）→ T2.2 partial 后 `SUM(qty)` 45→1045、
  `COUNT(*)` 仍 10、**逐行**三列仍等于第一轮值（10/10）。
- **非 UNIQUE 拒绝路径**：P2.1 Doris 端 `Only unique key merge on write support partial update`
  （DUPLICATE；探针另证 UNIQUE MoR 同文案）；T4.1 console publish 门 400 诊断含
  `keyModel=duplicate`；T4.2 裸 INSERT 同表 publish 200（未声明不受限，回归锚）。
- **sequence 列在场一例**：T5.4 一次 partial load 混合 seq——偶数行 seq=20 落盘
  （qty=200+i）、奇数行 seq=5 被丢弃（seq 仍 10、qty 不变）、未写的 note 全保留。
- 附带：T3.1/T3.2 evolve × partial 互斥 live（409 `PARTIAL_UPDATE_ACTIVE` →
  stop → 200 `changed:false` + `partialUpdatePipelines` 清单）；T6.1 子集缺 seq →
  publish 200 / deploy 502 `ENGINE_REJECTED`。
- 未覆盖：`SCHEMA_CHANGE_IN_PROGRESS` 409 需要一个真在飞的 ALTER COLUMN job，空表上
  Light Schema Change 瞬时完成，无法确定性构造——仍由 W3 契约测试（mock）覆盖。

### 2.3 T-A8 dogfooding：`scripts/dogfood/round1.sh`（在役 3.1.4，专库 `tailrace_dogfood`）

22/22，13s（全文 `scripts/dogfood/evidence/round1-2026-10-28-22of22.log`，逐步耗时
`round1-2026-10-28-summary.txt`）：

```
D0  SHOW FRONTENDS/BACKENDS 取真实版本 → 专库 DROP FORCE+CREATE
D1  console 全新 data-dir 启动 → 从 stderr 抓初始密码 → login
D2  doris（jdbc 9030 + streamLoad 8040，root）/ file 数据源 → 表对象 df_orders [order_no,qty,note]
D3  A1 四步：draft①（无 body：推荐 duplicate，keyColumnGuessed=true，DDL 无 replication_num）
    → draft②（确认 duplicate/order_no，replicationNum=min(alive,3)=1 → guessed=false）
    → execute 缺 keyModel 422 KEY_MODEL_REQUIRED → execute ok（Doris 118ms）
    → SHOW CREATE TABLE = DUPLICATE KEY(order_no) → 表对象 keyModel=duplicate 已记录
D4  file 表 local.df_in + batch1（10 行，run-id 唯一化，570 字节）→ node serve → ONLINE
    → pipeline create/publish/deploy → RUNNING → 3s 内 COUNT(*)=10
D5  A2：dryRun [0,570) → 200 jobId=null labelPrefix=tr-p1-s0-bf、表不动
    → 回填#1 job 20261028-01 done rowsWritten=10 → 表 20
    → 回填#2 同区间 job 20261028-02 done rowsWritten=10 → 表仍 20（label dedup）
    → 主管道仍 RUNNING
D6  DROP DATABASE tailrace_dogfood FORCE
```

## 三、摩擦观察（每条含复现）

### #1 【已修，HIGH】引擎 partial 模式不带 `columns` 头 → Doris 把未写列置 NULL，Status 仍 Success

- **现象**：W2 引擎只发 `partial_columns: true`（注释称 format=json 下 JSON key 即列子集），
  真 Doris 把写入列集解析为全表列，JSON 缺的 key 当 NULL 写入；mock 契约测试钉住了这个
  错误形状并通过。**2.1.7 与 3.1.4 行为一致**（e2e P3.1 两栈都记录 `note = NULL`）。
- **复现**（任一 MoW 表）：
  ```bash
  curl -s -X PUT -u root: -H "label: t1" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" --data-binary '{"order_no":"A-1","qty":1,"note":"first"}' \
    http://127.0.0.1:38040/api/pu_probe/a/_stream_load
  curl -s -X PUT -u root: -H "label: t2" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" -H "partial_columns: true" --data-binary '{"order_no":"A-1","qty":100}' \
    http://127.0.0.1:38040/api/pu_probe/a/_stream_load
  # SELECT * → A-1 100 NULL   （加 -H "columns: order_no,qty" 后 → A-1 100 first）
  ```
  原始输出：`evidence/.../probe.out`（A load2）vs `probe2.out`（A2）。
- **处置**：5471782 在 partial 分支加 `columns: <subset>`，mock 断言补齐，证伪注释改写；
  e2e T2.2/T5.4 live 锁定。**教训**：mock 只能钉"我们以为的形状"，W4 e2e 的 live 断言不可省。

### #2 【MEDIUM】在役容器叫 `doris-2.1`，跑的是 3.1.4；任务卡据此写成"在役 2.1.7"

- **复现**：`docker ps --format '{{.Names}} {{.Image}}' | grep doris` →
  `doris-2.1 apache/doris:fe-3.1.4`；`docker exec -i doris-2.1 mysql -h127.0.0.1 -P9030 -uroot -e "SHOW FRONTENDS\G" | grep Version` → `doris-3.1.4-rc02`。
- **影响**：排障时查错版本文档；ddl_create.sh 头注释第 61 行早已写明"3.1.4 无 BE swap 硬检查"
  但任务卡与 AGENTS 仍沿用 2.1 称呼。本轮 e2e 两版本都跑了，结论一致。
- **建议**：R8 在 `docs/dev` 或 AGENTS 环境契约里把在役栈版本写成实测值；dogfood 脚本 D0.1
  已把真实版本打印在首行。

### #3 【HIGH，R8】A1 生成器对 `sequenceColumn` 只当普通列建表，引擎的 `sequence_column` 头被 Doris 静默忽略——用户以为有定序保护，实际没有

- **现象**：`tailrace-ddl` 生成的 UNIQUE DDL 不带 `function_column.sequence_col`（代码注释：
  "sequence column orders updates, never becomes the key itself"）。表无该属性时 stream load
  带 `sequence_column: seq` → Success，但低 seq 直接覆盖高 seq。
- **复现**：`evidence/.../probe2.out` D2 段：无属性表先写 seq=20 qty=100，再 partial 写
  seq=5 qty=555 → `SELECT` = `C-1 5 555`（反例 C2 段带属性的表 seq=5 被丢弃）。
- **建议**：生成器在 keyModel=unique 且声明 sequenceColumn 时于 PROPERTIES 追加
  `"function_column.sequence_col" = "<col>"`（列类型须与 Doris 支持的序列类型匹配）；
  或注册/evolve 时 `SHOW CREATE TABLE` 校验属性缺失并告警。旁证：
  `ALTER TABLE … ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type"="bigint")`
  可对存量表事后补隐藏序列列（probe.out D 末尾）。

### #4 【MEDIUM，R8】INSERT 列清单缺序列列：publish 放行，deploy 才 502

- **现象**：console publish 门检查 kind/keyModel/列存在性，不检查 sequence membership；
  引擎 `with_partial_columns` 在装配时拒绝 → deploy 502 `ENGINE_REJECTED`，管道落
  `DEPLOY_FAILED`。
- **复现**：e2e T6.1（`INSERT INTO orders_seq (order_no, qty) …` 对 sequenceColumn=seq 的表）：
  publish 200 → deploy 502 body 含 `sequence column seq must be inside the partial subset`。
- **建议**：console 已持有 `table_object.sequence_column`，把该检查前移到
  `partial_declaration_diags`（与 keyModel 检查同处），发布期即拒。

### #5 【MEDIUM，R9】回填重放的 `rowsWritten` 把 label-dedup ack 也计入——台账口径误导对账

- **现象**：D5.3 同区间重放 job done `rowsWritten=10`，表行数 +0（Doris `Label Already
  Exists` 走 Dedup 分支，引擎按 ack 行数记账）。运维看台账会以为重放写入了数据。
- **复现**：`bash scripts/dogfood/round1.sh --keep` → D5.2 vs D5.3 两行（表 20→20，
  rowsWritten 均 10）；backfill_replay.sh T6.2 注释也承认此口径。
- **建议**：job 台账拆 `committedRows`/`dedupRows`（stream load 响应体里 Dedup 状态可区分），
  或在 `rowsWritten` 旁加 `dedup:true` 标记。

### #6 【LOW，R8】draft 第一轮不给 `replication_num`，调用方须自探 `SHOW BACKENDS`

- **现象**：D3.1 draft①的 DDL 无 `replication_num`；若调用方不传 `replicationNum`，Doris 默认
  3 副本在单 BE 栈上建表必挂（R6 注释已述）。三份 e2e 都硬编码 1，dogfood 脚本自己 `awk`
  Alive 数再取 `min(alive,3)`。
- **复现**：`POST …/ddl/draft`（空 body）→ `jq .ddl | grep replication_num` 为空。
- **建议**：console 已有 FE 连接（evolve 用 DESC、deploy 用 SHOW ALTER TABLE），draft 可顺手
  `SHOW BACKENDS` 取 Alive 数作默认值并在 notes 说明来源。

### #7 【LOW，R9】console 初始 admin 密码只在启动 stderr 里

- **复现**：D1.2 / 三份 e2e 的 S3.2 都靠 `grep -o '初始密码：[0-9a-f]*' console.log`。
- **建议**：`--print-initial-password` 或首启把密码写到 data-dir 下 0600 文件；自动化不必
  依赖日志格式（中文冒号曾让 `cut -d` 翻车，smoke.sh S1.2 先例）。

### #8 【LOW】回填 job id 用 UTC 日期

- **复现**：本地 2026-10-29 02:xx（UTC+8）触发的回填 job id 为 `20261028-01/02`（D5.2/D5.3）。
- **建议**：文档明示 job id 日期为 UTC，或改用本地日期 + 时区后缀；对账脚本按日期筛 job 时
  要注意。

## 四、反哺 R8/R9 排期（按本轮证据排序）

| 优先 | 项 | 来源 |
|---|---|---|
| R8-1 | A1 生成器补 `function_column.sequence_col`（或注册期校验告警） | 摩擦 #3 |
| R8-2 | publish 门加 sequence membership 检查（前移 T6.1 的晚失败） | 摩擦 #4 |
| R8-3 | draft 默认 replicationNum 探活 | 摩擦 #6 |
| R8-4 | 环境契约/任务卡校正在役 Doris 版本为 3.1.4 | 摩擦 #2 |
| R9-1 | 回填台账区分 committed/dedup 行数 | 摩擦 #5 |
| R9-2 | 初始密码出口；job id 时区注明 | 摩擦 #7/#8 |
| 流程 | 凡 Doris 请求形状改动，e2e live 断言与 mock 同轮落地（本轮 #1 的教训） | 摩擦 #1 |

## 五、未做 / 边界

- StarRocks + partial 的 console/engine 拒绝只有契约测试（mock），未起 allin1 栈 live 验证
  （不同 load 契约，本来就不接线）。
- `SCHEMA_CHANGE_IN_PROGRESS` 409 未 live（见 §2.2）。
- kafka 源未跑（任务卡 "file/kafka 源"二选一，本轮 file）。
- 三套 live 日志（compose 首跑/复跑、在役外部模式、dogfood）均已入库，路径见 §二。
