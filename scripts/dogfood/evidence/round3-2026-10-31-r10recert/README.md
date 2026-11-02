# R10 e2e 复跑门 · dogfooding round3 取证（2026-10-31，v1.2.0 tag 前置）

R9 收官档 §五 编入项 1：评审修复轮（`efc33a5`）更新了 e2e/dogfood 断言但如实未复跑
（提交信息明示 "bash -n clean but NOT re-run … re-run before v1.2.0 tagging"）。
本目录是 R10 在受测提交 `a435afe`（feature/v1.2-release）上的全量复跑取证，
同批另有 `scripts/e2e/evidence/{variant-vector,alert-flow}-e2e-2026-10-31-r10recert/`。

载体：在役 Doris 3.1.4-rc02（FE 容器 `doris-2.1` → 127.0.0.1:9030，BE → 8040），
`./scripts/build.sh` 产物（target/release 双二进制 1.1.0，lockstep 前版本）。
复跑命令：`bash scripts/dogfood/round3.sh --keep`（零环境变量，默认即在役栈）。

## 结果

| 运行 | 结果 | 说明 |
|---|---|---|
| run1（`round3-run1-26of27.log`） | 26/27 | D2.3 失败——**脚本 bug，产品行为正确**（见下） |
| run2（`round3-run2-27of27.log`） | **27/27** | 修复脚本断言取值后全绿；两 run 间无产品代码改动 |

## run1 D2.3 失败根因（脚本修，不改产品）

服务端返回恰是修复轮要的语义：`http=200` + `{"changed":false,...}`。但 D2.3 取值用了
`jq -r '.changed // empty'`——jq 的 `//` 把布尔字面量 `false` 当假值吞掉（零输出），
`EV_CHANGED` 为空串导致断言失败。同语义的 variant_vector.sh T1.4 用的是
`.changed | tostring`（其 :488 注释本就记着这个坑），故一路通过。已把 round3.sh:218
改成同一 tostring 惯例。这是复跑门的直接产出之一：修复轮更新的断言里藏着这一处
取值 bug，不实跑就带进 v1.2.0。

## 新语义断言实证（对照 R9 修复轮承诺）

- D2.3 dim 向量表 evolve → 200 changed=false（不再永久 409）；
- D4.1 规则按 `pipeline=$P_ID` 限定（逐序列 episode 的前提）；
- D4.2 SLO target 行在首刷后必在（缺席即 FAIL，不再「none yet」空过）；
- D4.7 两条 webhook 通知同 episode 且**同 seriesKey**（`recert`→`round3-run2-webhook.jsonl`
  两行 seriesKey 逐字相等，均为 `tailrace_pipeline_lag_seconds{node="engine-node",pipeline="1",statement="0"}`）；
- D4.8 删规则 204 且 hook 计数不涨（无幻影 resolved）。
