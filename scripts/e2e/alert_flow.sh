#!/usr/bin/env bash
# R9 W4 e2e (R11 W5 revision): the T-E2 alerting chain END TO END against a
# real console + engine node (file→file pipelines — no Doris in the path, so
# the alert semantics are what is under test, not a third-party server's
# mood):
#
#   idle hold    : (T-V1, R11) a file source read to EOF publishes the
#                  POSITIONAL verdict tailrace_pipeline_source_idle=1; the
#                  lag gauge still climbs with wall time (lag = seconds since
#                  the last data-carrying flush) but the console HOLDS the
#                  lag rule (Unknown, reason source_idle) — no firing, no
#                  notice, through a pause as well (the verdict freezes at
#                  its last value while polls are suspended). This is the
#                  1.2.0 false positive dogfooding round 3 #6 recorded
#                  ("空闲活管道的 lag 规则会合法 re-fire"); pre-R11 this script
#                  MANUFACTURED its lag by pausing an EOF source — under T-V1
#                  that is silence, not lag, and it no longer fires (R11 W5
#                  baseline run: 40/50, L1.1 state=inactive);
#   lag rule     : a REAL lag is manufactured as an operator meets one — rows
#                  pile up behind a broken sink. tuning.pollRows=1 makes every
#                  data poll stop short of EOF (verdict idle=0 = backlog);
#                  tuning.flushRows=1 flushes that first backlog row at once;
#                  the sink path has been replaced by a DIRECTORY (FileSink
#                  re-opens per flush) so the flush fails, the statement
#                  errors and the registry backs off before the restart — the
#                  counters are frozen for the backoff: idle=0, last_data_ms
#                  = the pre-outage flush → the lag crosses the threshold →
#                  firing. Repairing the sink inside the backoff lets the
#                  restart drain the backlog (non-idle, lag ≈ 0 → Clear →
#                  the SAME episode resolves); at EOF the source goes idle
#                  again and the climbing lag is HELD — no re-fire, the
#                  round-3 #6 oscillation is gone. Both notices land in
#                  alert_event and on a mock webhook;
#   state rule   : a REAL FAILED state is manufactured by a deterministic
#                  engine fault — the SELECT projection order disagrees with
#                  the sink's registered column order — under
#                  recovery.autoRestart=false (first statement error is
#                  terminal, engine/registry.rs); the rule fires, stopping
#                  the pipeline (series gone) resolves it;
#   badRowPolicy : section B (R10 review) runs the NON-default values
#                  skip / fail and an invalid one end to end — console DTO →
#                  config_json → plan.options → engine parse_strict — and
#                  asserts on what the engine did (dropped+counted / halted
#                  before the cursor / plan rejected), not on the DTO;
#   DLQ dedup    : (T-V2, R11) section D drives a replay storm — 2 good + 3
#                  malformed rows whose flush fails every attempt (sink is a
#                  directory), auto-restart replays the batch from the
#                  uncommitted cursor — and asserts the side file holds each
#                  bad row ONCE while tailrace_dlq_rows_total counts every
#                  isolation attempt (3 × attempts): the table is unique, the
#                  audit face keeps the storm (dogfooding round 3 #3).
#
# 用法：
#   ./scripts/build.sh   # 先构建（release 二进制默认路径 target/release/…）
#   bash scripts/e2e/alert_flow.sh [--help] [--keep]
#
# 断言（任务卡 W4 行：告警真触发+webhook 收到；R11 W5 加 idle/DLQ）：
#   S    栈起：console（TAILRACE_ALERT_INTERVAL_SECS=2，评估器 2s 一拍）→
#        登录 → file 数据源 → engine node ONLINE；mock webhook 起监听；
#   R    规则面：GET /api/alerts 空集 + ruleTypes 五类 + intervalSecs=2；
#        POST 建带 webhook 的 lag 规则 → 201；threshold=0 → 400；未知
#        ruleType → 400（§4.0 负例）；
#   P1   file→file 管道 wl→wl_in RUNNING（tuning pollRows=1/flushRows=1，
#        recovery backoff 20s——L 段的造 lag 装置），1 行落盘（lag≈0，规则
#        inactive）；console /metrics 有 relabel 后的 lag 系列与 SLO target
#        线（tailrace_slo_lag_target_seconds 同标签对照，T-E2 SLO v1）；
#   I    idle hold（T-V1）：I1 源读到 EOF → node 与 console /metrics 的
#        tailrace_pipeline_source_idle=1 与 lag 系列同标签 → lag 随墙钟越过
#        threshold=6 → 规则仍 inactive、webhook 0 条（1.2.0 在此 firing）；
#        I2 pause/resume 走 console 代理（PAUSED/RUNNING 裁决）——暂停时判
#        据冻结为 1，仍 hold；I3 追加 W-2 → 落盘 → lag 重新≈0 → 仍 0 条通知；
#   L    真 lag：sink 文件换成目录 + 追加 BACKLOG_ROWS 行 → 首行 flush 失败
#        → PIPELINE_RESTART（退避 20s）→ 退避期 idle=0 冻结、lag 越 6 →
#        state=firing（open episode 在场）→ webhook 第 1 条（x-tailrace-
#        event: firing，sample.value > 6 数字断言，sample 无 reason）→
#        alert_event 落 firing 行；退避期内修复 sink → 重启排空积压（非
#        idle、lag≈0）→ 同一 event 行翻 resolved（id 不变、resolvedAt/
#        resolvedSample 在场）→ webhook 第 2 条；sink 行数 = 2 + BACKLOG_ROWS
#        （失败的 flush 未写入，重放不重复）；
#   W    idle 尾巴：排空后再次 EOF、lag 再越 6 —— 规则 inactive、webhook 仍
#        恰 2 条（无 re-fire、无 per-tick 重复——防刷屏核心 + T-V1 根修）；
#        console /metrics 有 tailrace_slo_lag_p95_seconds（排空期的非 idle
#        样本，T-V4 交叉核验，samples≥1）→ stop；
#   F    真 FAILED：投影列序与 sink 登记列序相反（SELECT extra, id 对
#        (id, extra)）→ runtime 首个带数据批次 bail「transform output
#        column order != sink」→ recovery.autoRestart=false 即终态 FAILED
#        （node /metrics tailrace_pipeline_state=3）→ 建 state_failed 规则
#        → firing（webhook 第 3 条）→ stop（series 消失）→ resolved（第 4
#        条）；DELETE 规则 → 204 → 空集。注：F 段刻意不依赖 badRowPolicy
#        （坏 JSON 行在默认 dlq 策略下只进 DLQ，不是语句错误——run3 实测），
#        非默认 badRowPolicy 的端到端由 B 段专门覆盖；
#   B    badRowPolicy 非默认值端到端（R10 评审：a435afe 给 PipelineConfig 补
#        了 bad_row_policy 字段，契约测试只钉 serde 往返，这里钉引擎行为）：
#        B1 skip——好/坏/好三行，管道保持 RUNNING，两好行落 sink，**不建**
#        bad_rows 隔离文件，DECODE_BAD 事件写明 dropped+counted，node.log
#        启动行 bad_row_policy=skip；B2 fail——好/坏两行同一批，STATEMENT_
#        FAILED 事件带引擎原文「decode bad row under badRowPolicy=fail」，
#        state=3，且同批好行**不**落 sink（游标越批前即止）；B3 负例——
#        badRowPolicy="drop" 过 create/publish（console 只携带不校验），deploy
#        被引擎 parse_strict 拒绝 → 502 ENGINE_REJECTED 点名 options.
#        badRowPolicy（严格解析，不静默退回 dlq）。B 段在规则面清空之后
#        运行，不会给 webhook 计数添任何一条；
#   D    DLQ 重放风暴去重（T-V2）：sink 目标预建为目录 → 2 好 3 坏同一批 →
#        每次 flush 必败 → recovery backoff [1,8] 自动重启重放 → 指标
#        tailrace_dlq_rows_total 达 6（3 坏 × 2 次尝试）时侧文件
#        bad_rows/p<id>-s0.jsonl 仍恰 3 行 → 退避期内 rmdir 修复 → 两好行落
#        sink、管道非 FAILED → 终态：指标 = 3 × 尝试次数（≥6）、侧文件 3 行、
#        rawB64 解码集合 = 三条坏行原文。D 段无规则在场，webhook 不增。
#
# 覆盖变量：
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址   默认 127.0.0.1:18088
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址 默认 127.0.0.1:18108
#   TAILRACE_E2E_WEBHOOK_PORT   mock webhook 端口  默认 18190（127.0.0.1）
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-af-e2e
#   TAILRACE_E2E_BACKLOG_ROWS   L 段积压行数 默认 40000（pollRows=1 下排空需
#                                跨 ≥2 个评估拍（4s）才能观察到非 idle 的
#                                Clear——见 L 段注释；R11 W5 run2 实测 3000 行
#                                <1s 排完（≈0.3ms/行，SLO 环里一个排空期样本
#                                都没有），单拍内即回 EOF→idle→hold，episode
#                                卡在 firing；太大只是拖慢）
#   TAILRACE_ALERT_INTERVAL_SECS 评估器周期（脚本固定注入 2，加速迁移；
#                                生产默认 15s 不在本脚本射程内）
#
# 标志：--help / --keep（语义同 variant_vector.sh；无 --skip-up——本脚本
#   自起自拆整个本地栈，没有可复用的外部栈）
#
# 依赖：jq、python3（mock webhook + rawB64 解码）、curl、nc。
#
# live-gate：宿主可用磁盘 <5GiB（OrbStack 红线）时空过。Docker/Doris 一概
#   不需要——file sink 管道全程本地。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18088}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18108}"
WEBHOOK_PORT="${TAILRACE_E2E_WEBHOOK_PORT:-18190}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
case "$CONSOLE_BIN" in /*) ;; *) CONSOLE_BIN="$ROOT/$CONSOLE_BIN" ;; esac
case "$NODE_BIN" in /*) ;; *) NODE_BIN="$ROOT/$NODE_BIN" ;; esac
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-af-e2e}"
BACKLOG_ROWS="${TAILRACE_E2E_BACKLOG_ROWS:-40000}"
KEEP=0

for arg in "$@"; do
  case "$arg" in
    --help|-h)
      sed -n '2,/^set -uo pipefail/p' "$0" | sed -e '/^set -uo/d' -e 's/^# \{0,1\}//'
      exit 0
      ;;
    --keep) KEEP=1 ;;
    *) echo "unknown flag: $arg (see --help)" >&2; exit 2 ;;
  esac
done

PASS=0; FAIL=0
CONSOLE_PID=""; NODE_PID=""; HOOK_PID=""
FAILED_STEPS=()
step() { # step <pass|fail> <name> [detail] — wall-clock stamped (R11 W5) so a
         # post-mortem can line steps up with node-state/events.log
  if [ "$1" = "pass" ]; then
    PASS=$((PASS+1)); printf '  ok    %s  %s\n' "$(date +%H:%M:%S)" "$2"
  else
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$2"); printf '  FAIL  %s  %s  %s\n' "$(date +%H:%M:%S)" "$2" "${3:-}"
  fi
}

cleanup() {
  [ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
  [ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
  [ -n "$HOOK_PID" ] && kill -TERM "$HOOK_PID" 2>/dev/null
  wait 2>/dev/null
  [ "$FAIL" -gt 0 ] && KEEP=1
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

req() { # req <METHOD> <path> [json-body] [auth-token] → sets BODY/CODE
  local args=(-s -X "$1" -o "$WORK/body" -w '%{http_code}' "http://$CONSOLE_ADDR$2")
  [ -n "${3:-}" ] && args+=(-H 'Content-Type: application/json' -d "$3")
  [ -n "${4:-}" ] && args+=(-H "Authorization: Bearer $4")
  CODE="$(curl "${args[@]}")"
  BODY="$(cat "$WORK/body" 2>/dev/null)"
}
wait_http() { # wait_http <url> <tries>
  local i
  for ((i=0; i<$2; i++)); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ] && return 0
    sleep 1
  done
  return 1
}
json_sql() { printf '%s' "$1" | jq -Rs .; }
node_metric() { curl -s "http://$NODE_ADDR/metrics" | grep "^$1" | head -1; }
# console_metric <regex> → the first matching line of the CONSOLE /metrics
# (node label injected by the proxy — the evaluator's own view of the gauges)
console_metric() { curl -s "http://$CONSOLE_ADDR/metrics" | grep "^$1" | head -1; }
# metric_value <line> → the sample value (last field), empty for no line
metric_value() { [ -n "$1" ] && awk '{print $NF}' <<<"$1"; }
# lag_now / idle_now → the lag pipeline's two gauges as the console sees them
lag_now()  { metric_value "$(console_metric "tailrace_pipeline_lag_seconds{.*pipeline=\"$LAG_PID\"")"; }
idle_now() { metric_value "$(console_metric "tailrace_pipeline_source_idle{.*pipeline=\"$LAG_PID\"")"; }
# wait_lag_over <threshold> [tries=20] — poll (1s) until the console-side lag
# gauge exceeds <threshold>; echoes the value seen, rc 1 on timeout
wait_lag_over() {
  local r v
  for r in $(seq 1 "${2:-20}"); do
    v="$(lag_now)"
    [ -n "$v" ] && awk -v v="$v" -v t="$1" 'BEGIN{exit !(v>t)}' && { echo "$v"; return 0; }
    sleep 1
  done
  echo "${v:-none}"; return 1
}
# events_for <pipeline-id> <kind> → count of <kind> events the node logged
# for that pipeline (node-state/events.log: ts\tLEVEL\tKIND\tpipeline#N\tmsg);
# grep -c prints 0 itself on no match, only a missing file yields nothing
events_for() {
  local n
  n="$(grep -c $'\t'"$2"$'\t'"pipeline#$1"$'\t' "$WORK/node-state/events.log" 2>/dev/null)"
  echo "${n:-0}"
}
# count_lines <file> → non-empty line count (grep -c prints 0 itself), 0 for
# a missing file
count_lines() { if [ -f "$1" ]; then grep -c . "$1" 2>/dev/null || true; else echo 0; fi; }
# node_log_plain → node.log with the ANSI colour codes stripped (tracing
# writes them even into a file). The engine's start line then reads
# "pipeline started (serve) pipeline_id=N statements=1 dispatch_id=… bad_row_policy=<v>"
node_log_plain() { sed $'s/\x1b\\[[0-9;]*m//g' "$WORK/node.log" 2>/dev/null; }
# rule_state <name> → the rule's evaluator state via GET /api/alerts
rule_state() {
  req GET /api/alerts "" "$TOKEN"
  jq -r --arg n "$1" '.rules[] | select(.name==$n) | .state // "missing"' <<<"$BODY" 2>/dev/null
}
# wait_rule <name> <want> [tries=40] — poll the rule's state
wait_rule() {
  local r s
  for r in $(seq 1 "${3:-40}"); do
    s="$(rule_state "$1")"
    [ "$s" = "$2" ] && { echo "$s"; return 0; }
    sleep 2
  done
  echo "${s:-none}"
}
# hook_seen → number of notices the mock webhook has recorded
hook_seen() { [ -f "$WORK/hook.jsonl" ] && wc -l < "$WORK/hook.jsonl" | tr -d ' ' || echo 0; }
# wait_hook <count> [tries=30] — poll (2s) until the webhook holds at least
# <count> notices; echoes the count seen. R11 W5 fix: the body read $2/$3 for
# <count>/[tries] since R9, so `wait_hook 1 15` waited for 15 notices and
# always ran the full 60s before returning the real count — the "60s gaps"
# the R9/R10 run notes attributed to timing were this (and in the R11 L
# section that 60s pushed the sink repair past the restart backoff).
wait_hook() {
  local r n
  for r in $(seq 1 "${2:-30}"); do
    n="$(hook_seen)"
    [ "$n" -ge "$1" ] 2>/dev/null && { echo "$n"; return 0; }
    sleep 2
  done
  echo "${n:-0}"
}
# deploy_pipeline <name> <sink-table> <source-table> <select-cols> [config-json]
deploy_pipeline() {
  local sql="INSERT INTO local.$2 SELECT $4 FROM local.$3"
  local body="{\"name\":\"$1\",\"sqlContent\":$(json_sql "$sql"),\"dsMapping\":{\"local\":$FDS},\"checkpointMs\":2000"
  [ -n "${5:-}" ] && body="$body,\"config\":$5"
  body="$body}"
  req POST /api/pipelines "$body" "$TOKEN"
  P_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
  [ -n "$P_ID" ] || { echo "  pipeline $1 create failed: http=$CODE body=$BODY" >&2; return 1; }
  req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
  [ "$CODE" = "200" ] || { echo "  pipeline $1 publish failed: http=$CODE body=$BODY" >&2; return 1; }
  req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
  [ "$CODE" = "200" ] || { echo "  pipeline $1 deploy failed: http=$CODE body=$BODY" >&2; return 1; }
  req GET "/api/pipelines/$P_ID" "" "$TOKEN"
  [ "$(jq -r '.status // empty' <<<"$BODY")" = "RUNNING" ]
}

echo "== alert_flow e2e: work=$WORK console=$CONSOLE_ADDR node=$NODE_ADDR webhook=127.0.0.1:$WEBHOOK_PORT =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required (mock webhook)"; exit 2; }

# ---------- live-gate ----------
AVAIL_GIB="$(df -g / | awk 'NR==2 {print $4}')"
if [ "${AVAIL_GIB:-99}" -lt 5 ]; then
  echo "[skip] host free disk ${AVAIL_GIB}GiB < 5GiB (OrbStack kill-line), skipped"
  exit 0
fi
[ -x "$CONSOLE_BIN" ] && step pass "S0.1 console binary $CONSOLE_BIN" \
  || step fail "S0.1 console binary" "missing $CONSOLE_BIN (run ./scripts/build.sh)"
[ -x "$NODE_BIN" ] && step pass "S0.2 engine node binary $NODE_BIN" \
  || step fail "S0.2 node binary" "missing $NODE_BIN (run ./scripts/build.sh)"

# ---------- S1 mock webhook ----------
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state" "$WORK/hook"
cat > "$WORK/hook.py" <<'PY'
# Generated by scripts/e2e/alert_flow.sh — e2e-only mock webhook. Records
# every POST as one JSONL line (path, x-tailrace-event header, raw body).
import json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
port, out = int(sys.argv[1]), sys.argv[2]
class H(BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200)
        self.send_header("content-length", "2")
        self.end_headers()
        self.wfile.write(b"ok")
    def do_GET(self):
        self._ok()
    def do_POST(self):
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n)
        rec = {"path": self.path,
               "event_header": self.headers.get("x-tailrace-event", ""),
               "body": body.decode("utf-8", "replace")}
        with open(out, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self._ok()
    def log_message(self, *a):
        pass
ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
PY
python3 "$WORK/hook.py" "$WEBHOOK_PORT" "$WORK/hook.jsonl" >"$WORK/hook.log" 2>&1 &
HOOK_PID=$!
UP=0
for i in $(seq 1 10); do nc -z 127.0.0.1 "$WEBHOOK_PORT" 2>/dev/null && { UP=1; break; }; sleep 0.5; done
[ "$UP" = "1" ] \
  && step pass "S1.1 mock webhook listening on 127.0.0.1:$WEBHOOK_PORT (records every POST to hook.jsonl)" \
  || step fail "S1.1 mock webhook" "$(tail -3 "$WORK/hook.log")"

# ---------- S2 console + node ----------
TAILRACE_BOOTSTRAP_TOKEN="tr_b_af_e2e_00112233445566778899aabbccddeeff" \
TAILRACE_ALERT_INTERVAL_SECS=2 \
  "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
if wait_http "http://$CONSOLE_ADDR/healthz" 20; then
  step pass "S2.1 console booted (fresh data-dir, TAILRACE_ALERT_INTERVAL_SECS=2)"
else
  step fail "S2.1 console boot" "$(tail -3 "$WORK/console.log")"
fi
ADMIN_PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "S2.2 login → 200 + token" \
  || step fail "S2.2 login" "http=$CODE body=$BODY"

req POST /api/datasources "{\"name\":\"af-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$FDS" ]; } && step pass "S2.3 file datasource (f=$FDS) — the whole e2e is file→file, no third-party server in the path" \
  || step fail "S2.3 datasource" "fds=$FDS body=$BODY"

( cd "$WORK/node" && exec "$NODE_BIN" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "tr_b_af_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 \
  && step pass "S2.4 engine node up (serve)" \
  || step fail "S2.4 node boot" "$(tail -3 "$WORK/node.log")"
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "S2.5 node ONLINE" || step fail "S2.5 node ONLINE" "node.log=$(tail -5 "$WORK/node.log")"
grep -q "alert evaluator started" "$WORK/console.log" \
  && step pass "S2.6 console log carries 'alert evaluator started' (the periodic task is live)" \
  || step fail "S2.6 evaluator started" "$(grep -i alert "$WORK/console.log" | tail -3)"

# ---------- R rule surface ----------
req GET /api/alerts "" "$TOKEN"
RT="$(jq -r '.ruleTypes | length' <<<"$BODY" 2>/dev/null)"
IV="$(jq -r '.intervalSecs // empty' <<<"$BODY" 2>/dev/null)"
NR="$(jq -r '.rules | length' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ "$RT" = "5" ] && [ "$IV" = "2" ] && [ "$NR" = "0" ]; } \
  && step pass "R0.1 GET /api/alerts: ruleTypes=5 kinds, intervalSecs=2, empty rule set" \
  || step fail "R0.1 alerts read" "http=$CODE types=$RT interval=$IV rules=$NR body=$(head -c 200 <<<"$BODY")"
req POST /api/alerts/rules \
  "{\"name\":\"af-lag\",\"ruleType\":\"lag_seconds\",\"params\":{\"threshold\":6},\"webhookUrl\":\"http://127.0.0.1:$WEBHOOK_PORT/hook\"}" "$TOKEN"
LAG_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$LAG_ID" ]; } \
  && step pass "R0.2 lag rule created (threshold=6s, webhook → mock) id=$LAG_ID" \
  || step fail "R0.2 rule create" "http=$CODE body=$BODY"
req POST /api/alerts/rules "{\"name\":\"af-bad\",\"ruleType\":\"lag_seconds\",\"params\":{\"threshold\":0}}" "$TOKEN"
[ "$CODE" = "400" ] \
  && step pass "R0.3 negative: threshold=0 → 400 (strictly positive required)" \
  || step fail "R0.3 threshold 0" "http=$CODE body=$(head -c 200 <<<"$BODY")"
req POST /api/alerts/rules "{\"name\":\"af-bad\",\"ruleType\":\"nonsense\",\"params\":{}}" "$TOKEN"
[ "$CODE" = "400" ] \
  && step pass "R0.4 negative: unknown ruleType → 400" \
  || step fail "R0.4 unknown type" "http=$CODE body=$(head -c 200 <<<"$BODY")"

# ---------- P1 live pipeline with a real lag gauge ----------
COLS_ID='[["id","utf8"]]'
for t in wl wl_in; do
  req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"$t\",\"format\":\"jsonl\",\"columnsJson\":$COLS_ID}" "$TOKEN"
done
printf '%s\n' '{"id":"W-1"}' > "$WORK/node/wl_in"
# Section L's rig (header): pollRows=1 → every data poll stops short of EOF
# (verdict idle=0 while rows remain behind the cursor); flushRows=1 → the
# first backlog row flushes at once, so the broken sink fails BEFORE a next
# poll could reach EOF and flip the verdict back to idle; the 20s backoff is
# the window in which the counters stay frozen (firing) and the sink gets
# repaired. None of this changes sections P1/I: one row per poll, each
# flushed on its own.
CFG_LAG='{"reliability":null,"recovery":{"autoRestart":true,"autoRestartMax":5,"restartBackoffSecs":[20]},"tuning":{"pollRows":1,"flushRows":1},"groupPrefix":null,"startOffset":null}'
if deploy_pipeline af-lag-pipe wl wl_in "id" "$CFG_LAG"; then
  LAG_PID="$P_ID"
  step pass "P1.1 file→file pipeline af-lag-pipe deployed → RUNNING (pipeline #$LAG_PID; tuning pollRows=1/flushRows=1, recovery backoff 20s — section L's rig)"
else
  LAG_PID="${P_ID:-0}"
  step fail "P1.1 pipeline deploy" "deploy chain failed"
fi
# the sink file appears once the first row flushes (checkpointMs=2000 →
# flush_interval 2s; FileSink appends jsonl)
WROWS=0
for i in $(seq 1 30); do
  [ -f "$WORK/node/wl" ] && WROWS="$(grep -c 'W-1' "$WORK/node/wl" 2>/dev/null || echo 0)"
  [ "$WROWS" = "1" ] && break
  sleep 1
done
{ [ "${WROWS:-x}" = "1" ]; } \
  && step pass "P1.2 first row flushed to the sink file (last_data_ms stamped — the lag gauge's basis)" \
  || step fail "P1.2 first flush" "rows=$WROWS node.log=$(tail -5 "$WORK/node.log" 2>/dev/null)"
LAGLINE="$(node_metric 'tailrace_pipeline_lag_seconds{.*pipeline="'"$LAG_PID"'".*}')"
# R9 review fix: compare the CONSOLE-side lag line (node label injected by
# the proxy) with the SLO line, label body to label body — "same labels" is
# asserted, not just "both lines exist".
CLAGLINE="$(curl -s "http://$CONSOLE_ADDR/metrics" | grep "^tailrace_pipeline_lag_seconds{.*pipeline=\"$LAG_PID\"" | head -1)"
SLOLINE="$(curl -s "http://$CONSOLE_ADDR/metrics" | grep "^tailrace_slo_lag_target_seconds{.*pipeline=\"$LAG_PID\"" | head -1)"
LAG_LABELS="$(sed -E 's/^[^{]*(\{[^}]*\}).*/\1/' <<<"$CLAGLINE")"
SLO_LABELS="$(sed -E 's/^[^{]*(\{[^}]*\}).*/\1/' <<<"$SLOLINE")"
{ [ -n "$LAGLINE" ] && [ -n "$CLAGLINE" ] && [ -n "$SLOLINE" ] && [ "$LAG_LABELS" = "$SLO_LABELS" ]; } \
  && step pass "P1.3 console /metrics: lag series $CLAGLINE with the SLO target beside it $SLOLINE — identical label set $LAG_LABELS (T-E2 SLO v1)" \
  || step fail "P1.3 metrics surface" "node_lag='$LAGLINE' console_lag='$CLAGLINE' slo='$SLOLINE' lag_labels='$LAG_LABELS' slo_labels='$SLO_LABELS'"
S="$(wait_rule af-lag inactive 10)"
[ "$S" = "inactive" ] \
  && step pass "P1.4 rule state inactive while the data flows (lag ≈ 0 < 6)" \
  || step fail "P1.4 premature firing" "state=$S"

# ---------- I idle hold (T-V1) ----------
# The poll that delivered W-1 ran into EOF, so the file source's POSITIONAL
# verdict is idle: tailrace_pipeline_source_idle=1 on the very label set
# the lag series carries (node render → console relabel, both gauges). From
# here nothing arrives, last_data_ms stays at W-1's flush and the lag gauge
# climbs with wall time — this is the 1.2.0 false positive (dogfooding
# round 3 #6, and the pause this script used to fire with). The console
# must HOLD the rule (Unknown, sample reason source_idle): no firing, no
# notice, however far past the threshold the lag goes.
IDLE_NODE="$(node_metric 'tailrace_pipeline_source_idle{.*pipeline="'"$LAG_PID"'".*}')"
CIDLELINE="$(console_metric "tailrace_pipeline_source_idle{.*pipeline=\"$LAG_PID\"")"
IDLE_LABELS="$(sed -E 's/^[^{]*(\{[^}]*\}).*/\1/' <<<"$CIDLELINE")"
{ [ "$(metric_value "$IDLE_NODE")" = "1" ] && [ "$(metric_value "$CIDLELINE")" = "1" ] && [ "$IDLE_LABELS" = "$LAG_LABELS" ]; } \
  && step pass "I1.1 CORE source idle verdict: node $IDLE_NODE → console $CIDLELINE — gauge 1 on the lag series' own label set $IDLE_LABELS (file read to EOF = nothing behind the cursor)" \
  || step fail "I1.1 idle gauge" "node='$IDLE_NODE' console='$CIDLELINE' idle_labels='$IDLE_LABELS' lag_labels='$LAG_LABELS'"
# the gauge crosses 6 about 6s after W-1's flush (P1.2 waited part of that)
V="$(wait_lag_over 6 20)"; RC=$?
{ [ "$RC" = "0" ] && [ "$(idle_now)" = "1" ]; } \
  && step pass "I1.2 the lag gauge climbed to ${V}s > threshold 6 while idle stayed 1 — wall time over silence, not a late row" \
  || step fail "I1.2 lag over threshold" "lag=$V idle=$(idle_now)"
sleep 5 # two evaluator ticks (2s) see lag > 6 with idle = 1
S="$(rule_state af-lag)"; N="$(hook_seen)"
{ [ "$S" = "inactive" ] && [ "$N" = "0" ]; } \
  && step pass "I1.3 CORE HELD: ≥2 evaluator passes over lag $(lag_now)s > 6 with idle=1 → rule state=$S, webhook $N notices (1.2.0 fired here — the R11 W5 baseline of the pre-revision script read L1.1 state=inactive for the same reason)" \
  || step fail "I1.3 idle hold" "state=$S hook=$N lag=$(lag_now) idle=$(idle_now)"
# I2 pause/resume through the console proxy. While polls are suspended the
# verdict keeps its last value (the runtime publishes it after each poll),
# so a source paused AT EOF stays idle=1 and the rule stays held — the
# pause that manufactured this script's firing before R11 is silence now.
req POST "/api/pipelines/$LAG_PID/pause" '{}' "$TOKEN"
{ [ "$CODE" = "200" ] && grep -q '"PAUSED"' <<<"$BODY"; } \
  && step pass "I2.1 pipeline paused via the console proxy (engine verdict PAUSED — pulling stops, the idle verdict freezes at its last value 1)" \
  || step fail "I2.1 pause" "http=$CODE body=$BODY"
sleep 5
S="$(rule_state af-lag)"; N="$(hook_seen)"
{ [ "$S" = "inactive" ] && [ "$N" = "0" ] && [ "$(idle_now)" = "1" ]; } \
  && step pass "I2.2 CORE still held through the pause: lag $(lag_now)s, idle=1 frozen, state=$S, webhook $N notices" \
  || step fail "I2.2 paused hold" "state=$S hook=$N lag=$(lag_now) idle=$(idle_now)"
req POST "/api/pipelines/$LAG_PID/resume" '{}' "$TOKEN"
{ [ "$CODE" = "200" ] && grep -q '"RUNNING"' <<<"$BODY"; } \
  && step pass "I2.3 pipeline resumed (engine verdict RUNNING — polls back on, still at EOF)" \
  || step fail "I2.3 resume" "http=$CODE body=$BODY"
# I3 new data: W-2 lands (pollRows=1 → one poll, flushRows=1 → flushed at
# once), last_data_ms re-stamps, the lag gauge drops to ~0, the delivering
# poll's successor reaches EOF again (idle back to 1). Evaluation never
# stopped — there was simply nothing to fire or resolve.
printf '%s\n' '{"id":"W-2"}' >> "$WORK/node/wl_in"
W2C=0
for i in $(seq 1 20); do
  W2C="$(grep -c '"W-2"' "$WORK/node/wl" 2>/dev/null)"
  [ "${W2C:-0}" = "1" ] && break
  sleep 1
done
LAG_AFTER="$(lag_now)"
{ [ "${W2C:-0}" = "1" ] && awk -v v="${LAG_AFTER:-99}" 'BEGIN{exit !(v<6)}'; } \
  && step pass "I3.1 new data: W-2 appended → flushed to the sink (lag re-stamped to ${LAG_AFTER}s < 6)" \
  || step fail "I3.1 new data" "w2=$W2C lag=$LAG_AFTER"
sleep 3
S="$(rule_state af-lag)"; N="$(hook_seen)"
{ [ "$S" = "inactive" ] && [ "$N" = "0" ] && [ "$(idle_now)" = "1" ]; } \
  && step pass "I3.2 after the data landed: idle back to 1 (EOF again), state=$S, webhook still $N — the whole idle episode produced zero notices" \
  || step fail "I3.2 post-data" "state=$S hook=$N idle=$(idle_now)"

# ---------- L a REAL lag: backlog behind a broken sink → firing ----------
# idle=0 is what makes a lag real: rows behind the cursor. The file source
# reports it only when a poll stops short of EOF — with pollRows=1 every
# data poll does. To hold that verdict for seconds the flush of the first
# backlog row has to fail: the sink path becomes a DIRECTORY (FileSink
# re-opens the target per flush), the statement errors, the registry logs
# PIPELINE_RESTART and backs off 20s — and for that backoff the counters are
# frozen: idle=0, last_data_ms = W-2's flush. The lag gauge crosses 6 →
# the console FIRES (no hold: idle is 0). A sink outage with rows piling up
# is the canonical real lag an operator wants paged for.
mv "$WORK/node/wl" "$WORK/node/wl.keep" && mkdir "$WORK/node/wl"
for i in $(seq 1 "$BACKLOG_ROWS"); do printf '{"id":"B-%d"}\n' "$i"; done >> "$WORK/node/wl_in"
[ -d "$WORK/node/wl" ] && [ "$(count_lines "$WORK/node/wl_in")" = "$((2 + BACKLOG_ROWS))" ] \
  && step pass "L0.1 outage rig: sink path wl is now a directory (the two landed rows parked in wl.keep) and $BACKLOG_ROWS backlog rows B-1..B-$BACKLOG_ROWS appended behind the cursor" \
  || step fail "L0.1 rig" "dir=$([ -d "$WORK/node/wl" ] && echo yes || echo no) wl_in_lines=$(count_lines "$WORK/node/wl_in")"
RST=0
for i in $(seq 1 15); do
  RST="$(events_for "$LAG_PID" PIPELINE_RESTART)"
  [ "${RST:-0}" -ge 1 ] 2>/dev/null && break
  sleep 1
done
{ [ "${RST:-0}" -ge 1 ] && [ "$(idle_now)" = "0" ]; } \
  && step pass "L0.2 CORE the first backlog row's flush failed → PIPELINE_RESTART event #$RST (backing off 20s) with the idle gauge frozen at 0 (backlog) — a real lag is now accumulating" \
  || step fail "L0.2 outage" "restart_events=$RST idle=$(idle_now) events=$(grep "pipeline#$LAG_PID" "$WORK/node-state/events.log" 2>/dev/null | tail -2)"
S="$(wait_rule af-lag firing 20)"
OPEN_ID="$(req GET /api/alerts "" "$TOKEN"; jq -r --arg n af-lag '.rules[] | select(.name==$n) | .openEvent.id // empty' <<<"$BODY" 2>/dev/null)"
[ "$S" = "firing" ] \
  && step pass "L1.1 CORE REAL lag fired the rule: state=firing (idle=0 + lag $(lag_now)s > 6, strict >) — the same gauge values the idle sections held on, now with backlog behind them" \
  || step fail "L1.1 firing" "state=$S lag=$(lag_now) idle=$(idle_now) console.log=$(grep -i alert "$WORK/console.log" | tail -3)"
N="$(wait_hook 1 10)"
if [ "$N" -ge 1 ] 2>/dev/null && jq -e 'select(.event_header=="firing") | (.body|fromjson|.sample.value) > 6 and ((.body|fromjson|.sample.reason) == null)' "$WORK/hook.jsonl" >/dev/null 2>&1; then
  V="$(jq -r 'select(.event_header=="firing") | (.body|fromjson|.sample.value)' "$WORK/hook.jsonl" | head -1)"
  step pass "L2.1 CORE webhook notice #1: POST x-tailrace-event=firing, sample.value=$V > 6 (a NUMBER from the real gauge) and no sample.reason (an evaluated verdict, not a held one)"
else
  step fail "L2.1 webhook firing" "n=$N hook=$(head -c 300 "$WORK/hook.jsonl" 2>/dev/null)"
fi
req GET /api/alerts "" "$TOKEN"
EV_STATE="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .state' <<<"$BODY" 2>/dev/null | head -1)"
{ [ "${EV_STATE:-x}" = "firing" ] && [ -n "$OPEN_ID" ]; } \
  && step pass "L3.1 alert_event row landed in state firing (open episode id=$OPEN_ID — one row, transition-only)" \
  || step fail "L3.1 firing episode" "state=$EV_STATE open=$OPEN_ID body=$(head -c 300 <<<"$BODY")"
# L4 repair inside the backoff: the restart re-assembles from the committed
# cursor (after W-2), re-polls B-1 (the failed flush wrote nothing — no
# duplicate) and drains the backlog row by row, each flush re-stamping
# last_data_ms. While rows remain the verdict is idle=0 and the lag ≈ 0,
# so the evaluator reads Clear and resolves the SAME episode. The backlog
# is sized so the drain spans ≥2 evaluator ticks (BACKLOG_ROWS, header):
# T-V1 resolves only on a non-idle Clear — a backlog that drained within a
# single tick would leave the episode open until the next data (or stop).
rmdir "$WORK/node/wl" && mv "$WORK/node/wl.keep" "$WORK/node/wl"
[ -f "$WORK/node/wl" ] \
  && step pass "L4.0 sink repaired inside the 20s backoff (directory removed, the landed rows restored) — the restart will drain the backlog" \
  || step fail "L4.0 repair" "wl is not a file"
S="$(wait_rule af-lag inactive 40)"
[ "$S" = "inactive" ] \
  && step pass "L4.1 CORE recovered: state back to inactive — the restart drained the backlog non-idle (idle=0, lag ≈ 0 → Clear)" \
  || step fail "L4.1 resolved" "state=$S lag=$(lag_now) idle=$(idle_now) sink_lines=$(count_lines "$WORK/node/wl") restarts=$(events_for "$LAG_PID" PIPELINE_RESTART)"
req GET /api/alerts "" "$TOKEN"
EV_ID2="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .id' <<<"$BODY" 2>/dev/null | head -1)"
EV_STATE2="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .state' <<<"$BODY" 2>/dev/null | head -1)"
EV_RESOLVED="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .resolvedAt // empty' <<<"$BODY" 2>/dev/null | head -1)"
EV_RSAMPLE="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .resolvedSample.value // empty' <<<"$BODY" 2>/dev/null | head -1)"
{ [ "${EV_STATE2:-x}" = "resolved" ] && [ -n "$EV_RESOLVED" ] && [ "${EV_ID2:-x}" = "${OPEN_ID:-x}" ] && [ -n "$EV_ID2" ] && awk -v v="${EV_RSAMPLE:-99}" 'BEGIN{exit !(v<6)}'; } \
  && step pass "L4.2 CORE the SAME episode row flipped to resolved (id=$EV_ID2 unchanged, resolvedAt set, resolvedSample.value=${EV_RSAMPLE}s < 6 — a real Clear from the drain, not series_absent) — no second row" \
  || step fail "L4.2 episode flip" "id=$EV_ID2 open=$OPEN_ID state=$EV_STATE2 resolvedAt=$EV_RESOLVED rsample=$EV_RSAMPLE"
N="$(wait_hook 2 10)"
if [ "$N" = "2" ] && jq -e 'select(.event_header=="resolved")' "$WORK/hook.jsonl" >/dev/null 2>&1; then
  step pass "W1 CORE anti-flood: the webhook holds EXACTLY the episode's 2 notices (firing + resolved) — silent while firing persisted through the backoff, silent after it closed"
else
  step fail "W1 anti-flood" "n=$N hook=$(jq -rc '.event_header' "$WORK/hook.jsonl" 2>/dev/null | paste -sd, -)"
fi
# the drain lands every backlog row exactly once on top of W-1/W-2: the
# failed flush wrote nothing, so the replayed B-1 is not a duplicate
WANT=$((2 + BACKLOG_ROWS)); GOT=0
for i in $(seq 1 180); do
  GOT="$(count_lines "$WORK/node/wl")"
  [ "$GOT" = "$WANT" ] && break
  sleep 1
done
B1C="$(grep -c '"B-1"}' "$WORK/node/wl" 2>/dev/null)"
{ [ "$GOT" = "$WANT" ] && [ "${B1C:-0}" = "1" ]; } \
  && step pass "W2.1 sink drained: $GOT lines = W-1 + W-2 + $BACKLOG_ROWS backlog rows, B-1 (the row whose flush failed) present exactly once — the outage cost no row and duplicated none" \
  || step fail "W2.1 drain" "lines=$GOT want=$WANT b1=$B1C"
# W3 the idle tail: the drain reached EOF, the verdict is idle=1 again and
# the lag climbs past 6 once more — 1.2.0 re-fired here (round 3 #6: the
# firing/resolved oscillation every idle stretch; this script used to race
# a stop against it). Under T-V1 the rule is held: still exactly 2 notices.
V="$(wait_lag_over 6 20)"
sleep 5
S="$(rule_state af-lag)"; TOTAL_HOOKS="$(hook_seen)"
{ [ "$S" = "inactive" ] && [ "$TOTAL_HOOKS" = "2" ] && [ "$(idle_now)" = "1" ]; } \
  && step pass "W3 CORE idle tail held: lag back over 6 (${V}s → $(lag_now)s) with idle=1 after the drain, state=$S, STILL exactly 2 notices across the whole episode (outage, backoff, drain, idle tail) — no re-fire, zero per-tick noise" \
  || step fail "W3 idle tail" "state=$S total=$TOTAL_HOOKS lag=$(lag_now) idle=$(idle_now) hook=$(jq -rc '.event_header' "$WORK/hook.jsonl" 2>/dev/null | paste -sd, -)"
# W4 T-V4 cross-check: the drain's non-idle passes were sampled into the SLO
# ring (idle passes are not — slo.rs), so the console renders p95 for this
# pipeline with a sample count that supports it
SLO95="$(console_metric "tailrace_slo_lag_p95_seconds{.*pipeline=\"$LAG_PID\"")"
SLON="$(metric_value "$(console_metric "tailrace_slo_lag_samples{.*pipeline=\"$LAG_PID\"")")"
{ [ -n "$SLO95" ] && [ "${SLON:-0}" -ge 1 ] 2>/dev/null; } \
  && step pass "W4 SLO history (T-V4): console /metrics $SLO95 backed by tailrace_slo_lag_samples=$SLON — only the non-idle drain passes were sampled" \
  || step fail "W4 SLO p95" "p95='$SLO95' samples='$SLON'"
req POST "/api/pipelines/$LAG_PID/stop" '{}' "$TOKEN"
{ [ "$CODE" = "200" ]; } && step pass "W5 lag pipeline stopped (series gone; the rule stays inactive — nothing was open to resolve)" \
  || step fail "W5 stop" "http=$CODE body=$BODY"
# ---------- F a REAL FAILED pipeline → state rule ----------
# Deterministic engine fault, no third-party server involved: the SELECT's
# projection order disagrees with the sink's registered column order — the
# runtime bails on the first data-carrying batch ("transform output column
# order != sink", engine/crates/tailrace-runtime/src/lib.rs) and with
# recovery.autoRestart=false the first statement error is terminal FAILED.
# (badRowPolicy=fail would also produce a FAILED here — since a435afe the
# console's PipelineConfig carries bad_row_policy, the silent serde drop
# dogfooding round 3 hit is gone — but this section keeps the projection
# fault on purpose: the alert semantics under test must not depend on the
# bad-row policy. The non-default policies get their own end-to-end
# section B below. The malformed-JSON variant under the default dlq policy
# rides only the DLQ, which is NOT a statement error — run-3 finding.)
COLS_F='[["id","utf8"],["extra","utf8"]]'
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"fail_sink\",\"format\":\"jsonl\",\"columnsJson\":$COLS_F}" "$TOKEN"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"fail_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_F}" "$TOKEN"
printf '%s\n' '{"id":"F-1","extra":"e1"}' '{"id":"F-2","extra":"e2"}' > "$WORK/node/fail_in"
step pass "F0.1 poison seed: 2 rows; the projection below reads (extra, id) against the sink's (id, extra) — first batch bails"
CFG_FAIL='{"reliability":null,"recovery":{"autoRestart":false},"tuning":null,"groupPrefix":null,"startOffset":null}'
if deploy_pipeline af-fail-pipe fail_sink fail_in "extra, id" "$CFG_FAIL"; then
  FAIL_PID="$P_ID"
  step pass "F0.2 poison pipeline deployed → RUNNING (pipeline #$FAIL_PID, recovery.autoRestart=false — the first statement error is terminal, no restart cycle)"
else
  FAIL_PID="${P_ID:-0}"
  step fail "F0.2 poison pipeline deploy" "deploy chain failed"
fi
ST=""
for i in $(seq 1 45); do
  ST="$(node_metric "tailrace_pipeline_state{.*pipeline=\"$FAIL_PID\".*}" | awk '{print $2}')"
  [ "$ST" = "3" ] && break
  sleep 2
done
{ [ "${ST:-x}" = "3" ]; } \
  && step pass "F1.1 CORE the engine REALLY failed: node /metrics tailrace_pipeline_state{pipeline=\"$FAIL_PID\"}=3 (ControlFlags FAILED — the exact series state_failed reads)" \
  || step fail "F1.1 failed state" "state=$ST node.log=$(tail -5 "$WORK/node.log" 2>/dev/null) events=$(grep -c STATEMENT_FAILED "$WORK/node-state/events.log" 2>/dev/null)"
req POST /api/alerts/rules "{\"name\":\"af-failed\",\"ruleType\":\"state_failed\",\"params\":{},\"webhookUrl\":\"http://127.0.0.1:$WEBHOOK_PORT/hook\"}" "$TOKEN"
ST_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$ST_ID" ]; } \
  && step pass "F1.2 state_failed rule created (no threshold; webhook → mock) id=$ST_ID" \
  || step fail "F1.2 state rule" "http=$CODE body=$BODY"
S="$(wait_rule af-failed firing 15)"
[ "$S" = "firing" ] \
  && step pass "F2.1 CORE the rule fired on the real FAILED pipeline within one evaluator tick (~2s)" \
  || step fail "F2.1 failed firing" "state=$S"
N="$(wait_hook 3 15)"
[ "$N" = "3" ] \
  && step pass "F2.2 webhook notice #3 received (state_failed firing)" \
  || step fail "F2.2 webhook #3" "n=$N"
req POST "/api/pipelines/$FAIL_PID/stop" '{}' "$TOKEN"
{ [ "$CODE" = "200" ]; } && step pass "F3.0 poison pipeline stopped (registry entry gone → the state series disappears from /metrics)" \
  || step fail "F3.0 stop" "http=$CODE body=$BODY"
S="$(wait_rule af-failed inactive 20)"
[ "$S" = "inactive" ] \
  && step pass "F3.1 CORE resolved the honest way: the series is ABSENT while its node still answers /metrics → reason=series_absent (R9 review fix: a node that is DOWN would HOLD the episode instead of faking a recovery)" \
  || step fail "F3.1 failed resolved" "state=$S"
N="$(wait_hook 4 15)"
if [ "$N" = "4" ] && jq -e 'select(.event_header=="resolved") | (.body|fromjson|.sample.reason) == "series_absent"' <(tail -1 "$WORK/hook.jsonl") >/dev/null 2>&1; then
  step pass "F3.2 webhook notice #4 received (state_failed resolved, sample.reason=series_absent) — 4 notices total across two episodes, every one a transition"
else
  step fail "F3.2 webhook #4" "n=$N last=$(tail -1 "$WORK/hook.jsonl" 2>/dev/null | head -c 200)"
fi

# ---------- cleanup of the rule face ----------
# R9 review fix: DELETE closes any open episode with reason=deleted and
# delivers its resolved notice BEFORE the row goes. Nothing is firing here
# (both episodes resolved above), so the hook count must stay at 4 — a 5th
# notice would mean a phantom resolved.
req DELETE "/api/alerts/rules/$LAG_ID" "" "$TOKEN"
D1="$CODE"
req DELETE "/api/alerts/rules/$ST_ID" "" "$TOKEN"
D2="$CODE"
req GET /api/alerts "" "$TOKEN"
NR="$(jq -r '.rules | length' <<<"$BODY" 2>/dev/null)"
sleep 2
NH="$(wc -l < "$WORK/hook.jsonl" 2>/dev/null | tr -d ' ')"
{ [ "$D1" = "204" ] && [ "$D2" = "204" ] && [ "$NR" = "0" ] && [ "$NH" = "4" ]; } \
  && step pass "S3.1 rules deleted (204 ×2) → empty rule set; no extra notice (none was firing — a firing rule would have been closed with reason=deleted and its resolved delivered first)" \
  || step fail "S3.1 rule cleanup" "d1=$D1 d2=$D2 rules=$NR hook_notices=$NH"

# ---------- B badRowPolicy: the NON-default values end to end ----------
# R10 review: a435afe gave PipelineConfig the bad_row_policy field and the
# contract test pins DTO keys == engine PlanOptions keys — that proves the
# serde round-trip, not that the ENGINE acts on fail/skip. Dogfooding round 3
# found the silent drop only by running a non-default value, so this section
# pushes all three forms through console → config_json → plan.options →
# engine parse_strict and asserts on what the engine DID. It runs after the
# rule face is empty: nothing here can add a webhook notice.
COLS_B='[["id","utf8"]]'
for t in bp_skip bp_skip_in bp_fail bp_fail_in; do
  req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"$t\",\"format\":\"jsonl\",\"columnsJson\":$COLS_B}" "$TOKEN"
done
# B1 skip: good / MALFORMED / good — the engine drops+counts the middle row,
# keeps running, writes no quarantine file (that is the dlq policy's job)
printf '%s\n' '{"id":"S-1"}' 'not json {' '{"id":"S-2"}' > "$WORK/node/bp_skip_in"
CFG_SKIP='{"reliability":null,"recovery":{"autoRestart":false},"tuning":null,"groupPrefix":null,"startOffset":null,"badRowPolicy":"skip"}'
if deploy_pipeline af-bp-skip bp_skip bp_skip_in "id" "$CFG_SKIP"; then
  SKIP_PID="$P_ID"
  step pass "B1.1 badRowPolicy=skip pipeline deployed → RUNNING (pipeline #$SKIP_PID; seed = good, MALFORMED, good)"
else
  SKIP_PID="${P_ID:-0}"
  step fail "B1.1 skip pipeline deploy" "http=$CODE body=$(head -c 200 <<<"$BODY")"
fi
if node_log_plain | grep -q "pipeline started (serve) pipeline_id=$SKIP_PID .*bad_row_policy=skip"; then
  step pass "B1.2 CORE node.log start line for pipeline #$SKIP_PID says bad_row_policy=skip — the value crossed console → config_json → plan.options → engine parse_strict (the engine's own log, not the DTO)"
else
  step fail "B1.2 skip passthrough" "$(node_log_plain | grep "pipeline started (serve) pipeline_id=$SKIP_PID" | tail -1)"
fi
SK=0; S1=0; S2=0
for i in $(seq 1 30); do
  S1="$(grep -c '"S-1"' "$WORK/node/bp_skip" 2>/dev/null || echo 0)"
  S2="$(grep -c '"S-2"' "$WORK/node/bp_skip" 2>/dev/null || echo 0)"
  { [ "$S1" = "1" ] && [ "$S2" = "1" ]; } && { SK=1; break; }
  sleep 1
done
SKST="$(node_metric "tailrace_pipeline_state{.*pipeline=\"$SKIP_PID\".*}" | awk '{print $2}')"
{ [ "$SK" = "1" ] && [ "${SKST:-x}" != "3" ]; } \
  && step pass "B1.3 CORE skip semantics: both good rows (S-1, S-2) flushed around the dropped row, tailrace_pipeline_state=$SKST (not FAILED)" \
  || step fail "B1.3 skip behaviour" "s1=$S1 s2=$S2 state=$SKST node.log=$(node_log_plain | tail -3)"
SKIP_DLQ="$WORK/node-state/bad_rows/p$SKIP_PID-s0.jsonl"
if [ ! -e "$SKIP_DLQ" ] && grep -q "DECODE_BAD.*pipeline#$SKIP_PID.*badRowPolicy=skip (rows dropped, counted only)" "$WORK/node-state/events.log" 2>/dev/null; then
  step pass "B1.4 CORE skip ≠ dlq: no bad_rows/p$SKIP_PID-s0.jsonl was created and the DECODE_BAD event reads 'badRowPolicy=skip (rows dropped, counted only)'"
else
  step fail "B1.4 skip vs dlq" "dlq_exists=$([ -e "$SKIP_DLQ" ] && echo yes || echo no) events=$(grep "pipeline#$SKIP_PID" "$WORK/node-state/events.log" 2>/dev/null | tail -2)"
fi
req POST "/api/pipelines/$SKIP_PID/stop" '{}' "$TOKEN"
[ "$CODE" = "200" ] && step pass "B1.5 skip pipeline stopped" || step fail "B1.5 stop" "http=$CODE body=$BODY"

# B2 fail: good / MALFORMED in ONE poll (the file is complete before the
# deploy; FileSource reads to EOF per poll) — the engine halts the statement
# before the cursor crosses the batch, so the good row must not land either
printf '%s\n' '{"id":"X-1"}' 'not json {' > "$WORK/node/bp_fail_in"
CFG_BPFAIL='{"reliability":null,"recovery":{"autoRestart":false},"tuning":null,"groupPrefix":null,"startOffset":null,"badRowPolicy":"fail"}'
if deploy_pipeline af-bp-fail bp_fail bp_fail_in "id" "$CFG_BPFAIL"; then
  BPF_PID="$P_ID"
  step pass "B2.1 badRowPolicy=fail pipeline deployed → RUNNING (pipeline #$BPF_PID; seed = good, MALFORMED — one poll, one batch)"
else
  BPF_PID="${P_ID:-0}"
  step fail "B2.1 fail pipeline deploy" "http=$CODE body=$(head -c 200 <<<"$BODY")"
fi
if node_log_plain | grep -q "pipeline started (serve) pipeline_id=$BPF_PID .*bad_row_policy=fail"; then
  step pass "B2.2 node.log start line for pipeline #$BPF_PID says bad_row_policy=fail"
else
  step fail "B2.2 fail passthrough" "$(node_log_plain | grep "pipeline started (serve) pipeline_id=$BPF_PID" | tail -1)"
fi
ST=""
for i in $(seq 1 45); do
  ST="$(node_metric "tailrace_pipeline_state{.*pipeline=\"$BPF_PID\".*}" | awk '{print $2}')"
  [ "$ST" = "3" ] && break
  sleep 2
done
[ "${ST:-x}" = "3" ] \
  && step pass "B2.3 CORE fail semantics: the malformed row IS a statement error — tailrace_pipeline_state{pipeline=\"$BPF_PID\"}=3 (terminal FAILED under autoRestart=false)" \
  || step fail "B2.3 fail state" "state=$ST node.log=$(node_log_plain | tail -3)"
if grep -q "STATEMENT_FAILED.*pipeline#$BPF_PID.*decode bad row under badRowPolicy=fail" "$WORK/node-state/events.log" 2>/dev/null; then
  step pass "B2.4 CORE STATEMENT_FAILED event carries the engine's own text 'decode bad row under badRowPolicy=fail' — the fail branch ran, not section F's projection fault"
else
  step fail "B2.4 fail text" "$(grep "pipeline#$BPF_PID" "$WORK/node-state/events.log" 2>/dev/null | tail -2)"
fi
XC="$(grep -c '"X-1"' "$WORK/node/bp_fail" 2>/dev/null || echo 0)"
[ "${XC:-0}" = "0" ] \
  && step pass "B2.5 fail halts BEFORE the cursor crosses the batch: the good row that shared the poll with the poison was not written (bp_fail holds 0 × X-1)" \
  || step fail "B2.5 pre-cursor halt" "x1=$XC"
req POST "/api/pipelines/$BPF_PID/stop" '{}' "$TOKEN"
[ "$CODE" = "200" ] && step pass "B2.6 fail pipeline stopped" || step fail "B2.6 stop" "http=$CODE body=$BODY"

# B3 negative: an invalid value. create/publish accept it (the console only
# carries the field); the engine's strict parse rejects the plan at deploy —
# strictness is what keeps a typo from silently degrading to dlq.
CFG_DROP='{"reliability":null,"recovery":null,"tuning":null,"groupPrefix":null,"startOffset":null,"badRowPolicy":"drop"}'
deploy_pipeline af-bp-drop bp_fail bp_fail_in "id" "$CFG_DROP" >/dev/null 2>&1
DROP_CODE="$CODE"; DROP_BODY="$BODY"
if [ "$DROP_CODE" = "502" ] && grep -q 'ENGINE_REJECTED' <<<"$DROP_BODY" && grep -q 'badRowPolicy' <<<"$DROP_BODY"; then
  step pass "B3.1 negative: badRowPolicy=\"drop\" passes create/publish and the ENGINE rejects the plan at deploy → 502 ENGINE_REJECTED naming options.badRowPolicy (strict parse, no silent fallback to dlq)"
else
  step fail "B3.1 invalid policy" "http=$DROP_CODE body=$(head -c 300 <<<"$DROP_BODY")"
fi

# ---------- D DLQ dedup under a replay storm (T-V2) ----------
# Dogfooding round 3 #3: a flush failure replays the batch, and every
# replay isolated the same bad rows again — 3 unique bad rows became 9 DLQ
# lines and tailrace_dlq_rows_total=9. T-V2 keys the side file on
# (statement, cursor, rawB64): the table holds each bad row ONCE while the
# attempt counter still counts every isolation — the at-least-once storm
# stays visible on the metric, the table does not repeat. The storm is the
# contract test's shape (tests/contract/tests/executor_loop.rs
# dlq_replay_storm_dedups_rows_but_counts_attempts) driven through the
# console: the sink target is a DIRECTORY so every flush fails; backoff
# [1,8] = a fast first replay and a wide second backoff to repair in (the
# third consecutive no-progress failure would be terminal). No rule is in
# place, so the webhook count cannot move.
COLS_D='[["id","utf8"]]'
for t in dlq_sink dlq_in; do
  req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"$t\",\"format\":\"jsonl\",\"columnsJson\":$COLS_D}" "$TOKEN"
done
mkdir "$WORK/node/dlq_sink"
printf '%s\n' '{"id":"G-1"}' '{broken-1' 'not json at all' '{"unterminated":' '{"id":"G-2"}' > "$WORK/node/dlq_in"
CFG_DLQ='{"reliability":null,"recovery":{"autoRestart":true,"autoRestartMax":5,"restartBackoffSecs":[1,8]},"tuning":null,"groupPrefix":null,"startOffset":null}'
if deploy_pipeline af-dlq-pipe dlq_sink dlq_in "id" "$CFG_DLQ"; then
  DLQ_PID="$P_ID"
  step pass "D0.1 storm rig: sink path dlq_sink pre-created as a DIRECTORY, seed = good, 3 × MALFORMED, good (one poll, one batch); pipeline af-dlq-pipe deployed → RUNNING (#$DLQ_PID, backoff [1,8])"
else
  DLQ_PID="${P_ID:-0}"
  step fail "D0.1 storm pipeline deploy" "http=$CODE body=$(head -c 200 <<<"$BODY")"
fi
DLQ_FILE="$WORK/node-state/bad_rows/p$DLQ_PID-s0.jsonl"
dlq_total() { metric_value "$(node_metric "tailrace_dlq_rows_total{.*pipeline=\"$DLQ_PID\"")"; }
# attempt 1 isolates 3 and fails its flush at once; the 1s backoff replays
# the batch; attempt 2 isolates the SAME 3 again → the counter reads 6
BAD=0
for i in $(seq 1 20); do
  BAD="$(dlq_total)"
  [ "${BAD:-0}" -ge 6 ] 2>/dev/null && break
  sleep 1
done
DLQN="$(count_lines "$DLQ_FILE")"
{ [ "${BAD:-0}" -ge 6 ] 2>/dev/null && [ "$DLQN" = "3" ]; } \
  && step pass "D1.1 CORE replay storm: tailrace_dlq_rows_total{pipeline=\"$DLQ_PID\"}=$BAD (3 bad rows × ≥2 isolation attempts) while bad_rows/p$DLQ_PID-s0.jsonl holds $DLQN lines — the counter kept the storm, the table did not grow" \
  || step fail "D1.1 storm dedup" "metric=$BAD dlq_lines=$DLQN events=$(grep "pipeline#$DLQ_PID" "$WORK/node-state/events.log" 2>/dev/null | tail -3)"
RSTD="$(events_for "$DLQ_PID" PIPELINE_RESTART)"
[ "${RSTD:-0}" -ge 1 ] 2>/dev/null \
  && step pass "D1.2 the replays were real restarts: $RSTD PIPELINE_RESTART event(s) for pipeline#$DLQ_PID (flush into a directory fails every attempt)" \
  || step fail "D1.2 restart events" "restarts=$RSTD events=$(grep "pipeline#$DLQ_PID" "$WORK/node-state/events.log" 2>/dev/null | tail -3)"
# repair inside the 8s backoff: the next attempt's flush lands the good rows
# and commits the cursor past the whole batch
rmdir "$WORK/node/dlq_sink"
[ ! -e "$WORK/node/dlq_sink" ] \
  && step pass "D2.0 sink repaired inside the second backoff (directory removed — FileSink creates the file on its next flush)" \
  || step fail "D2.0 repair" "dlq_sink still exists"
GOOD=0
for i in $(seq 1 30); do
  GOOD="$(count_lines "$WORK/node/dlq_sink")"
  [ "$GOOD" = "2" ] && break
  sleep 1
done
G1C="$(grep -c '"G-1"' "$WORK/node/dlq_sink" 2>/dev/null)"; G2C="$(grep -c '"G-2"' "$WORK/node/dlq_sink" 2>/dev/null)"
DST="$(node_metric "tailrace_pipeline_state{.*pipeline=\"$DLQ_PID\".*}" | awk '{print $2}')"
{ [ "$GOOD" = "2" ] && [ "${G1C:-0}" = "1" ] && [ "${G2C:-0}" = "1" ] && [ "${DST:-x}" != "3" ]; } \
  && step pass "D2.1 CORE after the repair the good rows landed exactly once (G-1, G-2 — the failed flushes wrote nothing) and the pipeline is not FAILED (tailrace_pipeline_state=$DST): the storm was survivable" \
  || step fail "D2.1 post-repair" "sink_lines=$GOOD g1=$G1C g2=$G2C state=$DST events=$(grep "pipeline#$DLQ_PID" "$WORK/node-state/events.log" 2>/dev/null | tail -3)"
# final totals: the counter is attempts × 3 (the repair may land inside
# attempt 2's flush window or cost one more replayed attempt — either way
# the metric and the file part ways), the file is the row set
BAD="$(dlq_total)"; DLQN="$(count_lines "$DLQ_FILE")"
ATT=$(( ${BAD:-0} / 3 ))
{ [ $(( ${BAD:-1} % 3 )) = 0 ] && [ "$ATT" -ge 2 ] && [ "$DLQN" = "3" ]; } \
  && step pass "D3.1 CORE T-V2 contract: tailrace_dlq_rows_total=$BAD = 3 bad rows × $ATT isolation attempts (every attempt counted), side file = $DLQN lines = the unique bad rows (dogfooding round 3 #3 read 9 lines for 3 rows here)" \
  || step fail "D3.1 totals" "metric=$BAD attempts=$ATT dlq_lines=$DLQN"
# the three lines ARE the three distinct bad rows (rawB64 round-trips the
# trimmed raw bytes), not three copies of one
RAWS="$(python3 -c 'import sys,base64,json
for l in sys.stdin:
    l=l.strip()
    if l: print(base64.b64decode(json.loads(l)["rawB64"]).decode("utf-8","replace"))' < "$DLQ_FILE" 2>/dev/null | sort)"
WANT_RAWS="$(printf '%s\n' '{broken-1' 'not json at all' '{"unterminated":' | sort)"
[ "$RAWS" = "$WANT_RAWS" ] \
  && step pass "D3.2 the side file's rawB64 set decodes to exactly the three seeded bad lines — each quarantined once, byte-exact, replayable" \
  || step fail "D3.2 rawB64 set" "got=$(tr '\n' '|' <<<"$RAWS") want=$(tr '\n' '|' <<<"$WANT_RAWS")"
req POST "/api/pipelines/$DLQ_PID/stop" '{}' "$TOKEN"
[ "$CODE" = "200" ] && step pass "D3.3 storm pipeline stopped" || step fail "D3.3 stop" "http=$CODE body=$BODY"
NH="$(hook_seen)"
[ "$NH" = "4" ] \
  && step pass "D3.4 webhook untouched by sections B and D: still the 4 notices of the two rule episodes" \
  || step fail "D3.4 webhook drift" "hook_notices=$NH"

TOTAL=$((PASS+FAIL))
echo "== alert_flow e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
