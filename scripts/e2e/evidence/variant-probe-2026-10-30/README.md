# VARIANT 探针（2026-10-30，R9 W1 / T-A4 探针先行）

对真库裸 stream load（无 console/engine），先于 `tailrace-ddl` 的 `variant`→`VARIANT`
映射、decoder 透传与任何 e2e 断言（R7 工单③工序）。**双版本**：在役 3.1.4
（容器 `doris-2.1`/`doris-be` 即 `*-3.1.4`，9030/8040）与 compose 2.1.7
（fe/be `apache/doris:*-2.1.7`，39030/38040；BE 需 `compose-be-swap-override.yml`
关掉 start_be.sh 的 swap 检查，R6 配方——首次随本探针入库，不再依赖 /tmp）。

文件：`probe.sh` + `probe-standing-3.1.4.out` / `probe-compose-2.1.7.out`；
`e2e-standing-3.1.4-41of41.log` / `e2e-compose-2.1.7-42of42.log`（`scripts/e2e/variant.sh`
在两栈上的全通日志，compose 模式多一步 sysctl 检查）；`e2e-run1-23of41.log`（首跑：18 处
FAIL 全是脚本自身问题，见下「工序注记」，引擎零改动后 41/41）。

复跑（mysql 客户端在容器内跑，端口恒 9030；BE 是宿主侧 curl）：

```bash
cd <repo>
bash scripts/e2e/evidence/variant-probe-2026-10-30/probe.sh standing-3.1.4 \
     docker exec -i doris-2.1 mysql -- 8040
docker compose -f docker-compose.yml \
     -f scripts/e2e/evidence/variant-probe-2026-10-30/compose-be-swap-override.yml \
     up -d --wait doris-fe doris-be
bash scripts/e2e/evidence/variant-probe-2026-10-30/probe.sh compose-2.1.7 \
     docker compose -f docker-compose.yml \
     -f scripts/e2e/evidence/variant-probe-2026-10-30/compose-be-swap-override.yml \
     exec -T doris-fe mysql -- 38040
```

## 结论（`diff` 两份 .out 仅四处差异，其余逐行一致；均为 2026-10-30 实测）

### 决定引擎设计的核心行（B2 / C1 / C3）

1. **JSON 字符串形态自动解析为结构**——variant 列收到 JSON *string literal*
   `"{\"a\":{\"b\":2,\"c\":\"y\"},\"arr\":[4,5]}"`（B2），落库与直接给 JSON object
   （B1）**等价**：raw `{"a":{"b":2,"c":"y"},"arr":[4,5],...}`，子列
   `v['a']['b']`=2、`v['a']['c']`=y、`v['arr']`=[4, 5]、`CAST(v['a']['b'] AS INT)`=2。
   **这正是引擎 DorisSink 对 Utf8 列的发射形态**（`tailrace-conn-doris` `DataType::Utf8`
   臂 → `serde_json::to_string`）——所以 **sink 不需要 variant 专用的免转义臂**：
   decoder 把嵌套子对象序列化回字符串、sink 照常转义、Doris 解析。UNIQUE MoW 全写
   （C1）与 `partial_columns` 更新 variant 列（C3）同样解析。双版本一致。
2. **UNIQUE MoW × partial_columns**（C）：只更新其他列（`columns: id,name`）时 variant
   列**保留**原值；更新 variant 列本身是**整值替换**而非深合并（C2：旧 `keep` 键变
   NULL，新 `new` 键在场；`name` 保留）。双版本一致。
3. **DESC 回显 `variant`**（小写，A4），SHOW CREATE TABLE 回显 `` `v` variant NULL ``——
   `tailrace-metadata` 的 `ddl_arrow_type("variant")` 应映回 `variant` 词，让 evolve 的
   registered-vs-live diff 对 variant 列安静（与 A3 的 `text`→`string` 同义）。

### DDL（A）

4. DUPLICATE KEY / UNIQUE KEY MoW 表的 **值列**可为 VARIANT（A1/A2）；**键列拒绝**（A3），
   文案有版本差：3.1.4 `Variant type should not be used in key column[v]`，2.1.7
   `JSONB or VARIANT type should not be used in key column[v]`。生成器层面：variant 列
   永不进 key 候选（现有 id/`*_id` 启发式只挑名字，`variant` 列若恰叫 `*_id` 会生成被
   FE 拒绝的草稿——W4/评审项，本波在生成器加一条拒绝：键列不可为 variant）。

### 载入形态（B，19 形态 18+2 行全部 Success、0 过滤——VARIANT 不拒载任何 JSON 值）

5. 对象根（B1）：嵌套 object/array/string/float 全保留；`true`→`1`（bool 存为整数）；
   `null` 叶**丢弃**（`"n":null` 不出现在 raw 中）。
6. JSON 字符串形态的标量（B4）：`"42"`→`42`、`"1.5"`→`1.5`、`"true"`→`1`——字符串被当 JSON
   文本解析；`"null"`→**`{}`**（空对象，非 NULL）。
7. 非 JSON 文本（B5）：`"plain string"`→字符串标量 `"plain string"`（raw 带引号）；非法 JSON
   `"{broken"`→字符串标量 `"{broken"`（**不拒载**，退化为字符串）；空串 `""`→**`{}`**。
   → **语义注记**：一个本意是字符串、内容恰好是合法 JSON 的叶值会被 Doris 解析成结构；
   这是 VARIANT 的服务端行为，引擎无法也不应干预，写进用户文档即可。
8. 数组根 `[10,20,30]` / 整数根 `42` / 布尔根 `true`（B6）均存入（子列查询对非对象根为 NULL）。
9. `null` 与键缺席（B7）均 NULL；`{}` 与 `{"a":{}}`（B8）均折叠为 `{}`。
10. 四层嵌套 `v['l1']['l2']['l3']['l4']`（B9）= deep。
11. **点键冲突（B10，独立表 dk）——真实版本差异**：同表内 `"a.b":"dotted-key"` 与
    `"a":{"b":...}` 并存时，**3.1.4 把点键合并进 `a.b` 子列路径**：raw 显示
    `{"a":{"b":"dotted-key","b":"dotted-key"}}`，且同表其他行的 `a.b` 子列在 raw 中被污染
    成 `{}`（id=2 的 `"b":7` raw 显 `{"a":{"b":{}}}`）——但子列查询 `v['a']['b']` 仍正确
    返回 7；2.1.7 两键分开保留 `{"a":{"b":"nested-b"},"a.b":"dotted-key"}`。首轮探针
    曾把 B10 与 B1/B2 放同一张表，3.1.4 上 B1/B2 的 raw 因此显 `"b":{}`——已拆独立表重跑。
    → 用户文档 caveat：上游键名含 `.` 时 3.1.4 的 raw 回显不可信，以子列查询为准。
12. raw `SELECT v` 的数组回显格式有版本差：3.1.4 `[1, 2, 3]`（逗号后空格）、2.1.7
    `[1,2,3]`；子列 `v['arr']` 两版本都 `[1, 2, 3]`。**e2e 对 raw 文本不可 byte-exact
    断言**，走子列查询或 `CAST(... AS INT/STRING)`。

### 子列类型冲突（D）

13. 同一子列跨行 int/string/float/object 混合（D）：全部可查，字符串以 JSON 文本形态回显
    （带引号 `"one hundred"`）、嵌套对象 `{"nested":1}`——Doris 内部把冲突子列提升为 JSON
    类型，不丢行不报错。双版本一致。

## 工序注记

- 首轮 compose 2.1.7 曾在 R8 探针留下 `/tmp/tp-probe/compose-be-swap-override.yml`；本轮
  把它入库到本目录，复跑不再依赖 /tmp 残留。
- `SELECT VERSION()` 返回 mysql 协议版本 5.7.99；服务端真实版本以 `SHOW FRONTENDS`
  （doris-3.1.4-rc02 / doris-2.1.7-rc03）为准。
- e2e 首跑 23/41 的四类脚本 bug（`e2e-run1-23of41.log`，引擎零改动）：①调用方传相对路径
  `target/debug/tailrace-node`，脚本 `cd $WORK/node` 后找不到 → 节点未上线 → 所有管道
  409 NO_ELIGIBLE_NODE 级联（已改为相对路径锚定仓根）；②mysql `-N` 对 NULL 输出字面
  `NULL`，断言曾期望空串（值本身全对）；③`jq '.changed // empty'` 吞 `false`（type_precision
  探针 README 记过的同一坑，改 `| tostring`）；④step 文本内反引号触发命令替换。
- 二跑 40/41 的唯一 FAIL 是断言形态：冲突子列 `payload.k` 在 e2e 表只有 int+string 两种
  → Doris 提升为 STRING（`CAST ... AS STRING` 给裸 `one hundred`），而探针 D 表四种冲突
  （含 object）→ 提升为 JSON（带引号 `"one hundred"`）——同一事实两种拼写，断言接受两者。
- e2e 用 debug 二进制跑（`TAILRACE_CONSOLE_BIN/NODE_BIN=target/debug/...`），release 构建
  不在 W1 门禁内；W4 dogfooding round3 按 release 复跑。
