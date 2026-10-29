# partial_update 探针证据（2026-10-28，R7 W4）

`scripts/e2e/partial_update.sh` P 档断言的原始依据：在写死 e2e 断言之前，对
compose Doris **2.1.7**（`tailrace-doris-fe/be`，R6 swap override + `vm.max_map_count`
配方拉起）用裸 `curl` stream load 跑的两轮语义探针，输出原样留存。探针脚本当时
在 `/tmp/tailrace-pu-probe/` 下运行（脚本内路径即此），库名 `pu_probe`，运行完
由 e2e 的外科式 teardown 随栈拆除。

| 文件 | 内容 |
|---|---|
| `probe.sh` / `probe.out` | 第一轮：A（UNIQUE MoW 无序列列）/ B（DUPLICATE）/ B2（UNIQUE MoR）/ C（MoW + `function_column.sequence_col`）/ D（MoW 无序列属性但发 `sequence_column` 头） |
| `probe2.sh` / `probe2.out` | 第二轮：同矩阵加 `columns` 头——定位第一轮的保留失败根因 |
| `e2e-run1-36of38.log` | e2e 脚本首跑全文（核心 T2.2/T5.4 通过；两处 FAIL 是脚本自身 bug：jq `// empty` 吞 `false`、断言前多发 GET 覆盖 `$CODE`，已修） |

## 实测结论（写进了引擎修复 5471782 与 e2e 断言）

1. **`partial_columns: true` 单独发（format=json、无 `columns` 头）→ Status Success，但
   未写列全部变 NULL**（probe.out A load2：`A-1 100 NULL NULL NULL`）。Doris 把写入列集
   解析为全表列，JSON 缺的 key 当 NULL 写入——W2 引擎当时正是这个形状（注释声称
   "JSON key 即列子集、无需 columns 头"），mock 契约测试钉住了错误形状并通过了。
2. **加 `columns: order_no,qty` 头 → 未写列保留旧值**（probe2.out A2：`A-1 100 first 1.5 1`）；
   JSON 里多带 `columns` 外的 key 会被忽略。→ 引擎修复：partial 模式同时发 `columns`。
3. **DUPLICATE KEY 与 UNIQUE merge-on-read 表的 partial load 都被拒**：
   `[ANALYSIS_ERROR] ... Only unique key merge on write support partial update`
   （probe.out B / B2）——W3 publish 门的文案与服务端一致；e2e P2.1 钉死。
4. **序列列**（建表带 `function_column.sequence_col=seq`）：partial + `columns` 含 seq，
   高 seq 落盘、低 seq 丢弃（probe2.out C2）；`columns` **不含** seq 时 Doris 2.1.7
   **接受**并保留旧 seq（qty 更新）——引擎侧"子集须含序列列"比服务端更严，是安全裕度
   而非服务端规则（W2 注释/报错文案中"Doris 会报 sequence column must be in the
   columns"系误判，已改写）。
5. **A1 生成器形态**（sequenceColumn 只是普通列、无 `function_column.sequence_col`
   属性）：引擎发的 `sequence_column: seq` 头被 Doris **静默接受且不生效**——低 seq 直接
   覆盖高 seq（probe2.out D2：`C-1 5 555 first`）。用户以为有定序保护，实际没有。
   → dogfooding 报告摩擦项，反哺 R8（生成器补 `function_column.sequence_col` 或在
   注册/部署时告警）。
6. 旁证：`ALTER TABLE ... ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES
   ("function_column.sequence_type"="bigint")` 可对已有表事后补隐藏序列列
   （probe.out D 末尾）。
