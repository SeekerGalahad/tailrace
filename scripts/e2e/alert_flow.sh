#!/usr/bin/env bash
# R9 W4 e2e: the T-E2 alerting chain END TO END against a real console +
# engine node (file→file pipelines — no Doris in the path, so the alert
# semantics are what is under test, not a third-party server's mood):
#
#   lag rule     : a REAL lag is manufactured by pausing a live pipeline
#                  (last_data_ms freezes, the engine's lag gauge climbs with
#                  wall time); appending a row and resuming drops it back to
#                  ~0 → firing → resolved, both episodes landing in
#                  alert_event, both notices POSTed to a mock webhook;
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
#                  before the cursor / plan rejected), not on the DTO.
#
# 用法：
#   ./scripts/build.sh   # 先构建（release 二进制默认路径 target/release/…）
#   bash scripts/e2e/alert_flow.sh [--help] [--keep]
#
# 断言（任务卡 W4 行：告警真触发+webhook 收到）：
#   S    栈起：console（TAILRACE_ALERT_INTERVAL_SECS=2，评估器 2s 一拍）→
#        登录 → file 数据源 → engine node ONLINE；mock webhook 起监听；
#   R    规则面：GET /api/alerts 空集 + ruleTypes 五类 + intervalSecs=2；
#        POST 建带 webhook 的 lag 规则 → 201；threshold=0 → 400；未知
#        ruleType → 400（§4.0 负例）；
#   P1   file→file 管道 wl→wl_in RUNNING，1 行落盘（lag≈0，规则 inactive）；
#        console /metrics 有 relabel 后的 lag 系列与 SLO target 线
#        （tailrace_slo_lag_target_seconds 同标签对照，T-E2 SLO v1）；
#   L    pause（真停拉）→ 轮询 GET /api/alerts 规则 state=firing（open
#        episode 在场）→ webhook 第 1 条 POST（x-tailrace-event: firing，
#        sample.value > threshold 数字断言）→ alert_event 落 firing 行；
#   R2   追加 1 行 + resume → lag 归零 → state=inactive → 同一 event 行翻
#        resolved（id 不变、resolvedAt/resolvedSample 在场）→ **立即 stop**
#        （空闲的活管道 lag 随墙钟重新累积，规则约 6s 后会**合法**再
#        firing——run3 实测；stop 抢在其前）；
#   W    webhook 恰收该 episode 的 2 条（firing/resolved）；stop 的 final
#        flush 后两行都在 sink 文件；排空等待之后仍恰 2 条——从无 per-tick
#        重复（防刷屏核心）；
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
#        运行，不会给 webhook 计数添任何一条。
#
# 覆盖变量：
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址   默认 127.0.0.1:18088
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址 默认 127.0.0.1:18108
#   TAILRACE_E2E_WEBHOOK_PORT   mock webhook 端口  默认 18190（127.0.0.1）
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-af-e2e
#   TAILRACE_ALERT_INTERVAL_SECS 评估器周期（脚本固定注入 2，加速迁移；
#                                生产默认 15s 不在本脚本射程内）
#
# 标志：--help / --keep（语义同 variant_vector.sh；无 --skip-up——本脚本
#   自起自拆整个本地栈，没有可复用的外部栈）
#
# 依赖：jq、python3（mock webhook）、curl、nc。
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
step() { # step <pass|fail> <name> [detail]
  if [ "$1" = "pass" ]; then
    PASS=$((PASS+1)); printf '  ok    %s\n' "$2"
  else
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$2"); printf '  FAIL  %s  %s\n' "$2" "${3:-}"
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
# wait_hook <count> [tries] — poll until the webhook holds exactly <count> notices
wait_hook() {
  local r n
  for r in $(seq 1 "${3:-30}"); do
    n="$(hook_seen)"
    [ "$n" -ge "$2" ] 2>/dev/null && { echo "$n"; return 0; }
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
if deploy_pipeline af-lag-pipe wl wl_in "id"; then
  LAG_PID="$P_ID"
  step pass "P1.1 file→file pipeline af-lag-pipe deployed → RUNNING (pipeline #$LAG_PID)"
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

# ---------- L manufacture a REAL lag → firing ----------
req POST "/api/pipelines/$LAG_PID/pause" '{}' "$TOKEN"
{ [ "$CODE" = "200" ] && grep -q '"PAUSED"' <<<"$BODY"; } \
  && step pass "L0.1 pipeline paused via the console proxy (engine verdict PAUSED — pulling stops, last_data_ms freezes)" \
  || step fail "L0.1 pause" "http=$CODE body=$BODY"
# lag crosses 6s ~7s in; the evaluator ticks every 2s
S="$(wait_rule af-lag firing 40)"
OPEN_ID="$(req GET /api/alerts "" "$TOKEN"; jq -r --arg n af-lag '.rules[] | select(.name==$n) | .openEvent.id // empty' <<<"$BODY" 2>/dev/null)"
[ "$S" = "firing" ] \
  && step pass "L1.1 CORE REAL lag fired the rule: state=firing ~7-10s after the pause (engine gauge climbs with wall time; strict > 6)" \
  || step fail "L1.1 firing" "state=$S console.log=$(grep -i alert "$WORK/console.log" | tail -3)"
N="$(wait_hook 1 15)"
if [ "$N" -ge 1 ] 2>/dev/null && jq -e 'select(.event_header=="firing") | (.body|fromjson|.sample.value) > 6' "$WORK/hook.jsonl" >/dev/null 2>&1; then
  V="$(jq -r 'select(.event_header=="firing") | (.body|fromjson|.sample.value)' "$WORK/hook.jsonl" | head -1)"
  step pass "L2.1 CORE webhook notice #1: POST x-tailrace-event=firing, sample.value=$V > 6 (a NUMBER, from the real gauge)" \
    || true
else
  step fail "L2.1 webhook firing" "n=$N hook=$(head -c 300 "$WORK/hook.jsonl" 2>/dev/null)"
fi
req GET /api/alerts "" "$TOKEN"
EV_STATE="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .state' <<<"$BODY" 2>/dev/null | head -1)"
{ [ "${EV_STATE:-x}" = "firing" ]; } \
  && step pass "L3.1 alert_event row landed in state firing (open episode id=$OPEN_ID — one row, transition-only)" \
  || step fail "L3.1 firing episode" "state=$EV_STATE body=$(head -c 300 <<<"$BODY")"

# ---------- R2 recover → resolved, and the anti-flood contract ----------
printf '%s\n' '{"id":"W-2"}' >> "$WORK/node/wl_in"
req POST "/api/pipelines/$LAG_PID/resume" '{}' "$TOKEN"
{ [ "$CODE" = "200" ] && grep -q '"RUNNING"' <<<"$BODY"; } \
  && step pass "R2.0 pipeline resumed (fresh row appended before the resume — the next flush re-stamps last_data_ms)" \
  || step fail "R2.0 resume" "http=$CODE body=$BODY"
S="$(wait_rule af-lag inactive 30)"
[ "$S" = "inactive" ] \
  && step pass "R2.1 CORE recovered: state back to inactive once data flows again (lag ≈ 0)" \
  || step fail "R2.1 resolved" "state=$S"
req GET /api/alerts "" "$TOKEN"
EV_ID2="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .id' <<<"$BODY" 2>/dev/null | head -1)"
EV_STATE2="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .state' <<<"$BODY" 2>/dev/null | head -1)"
EV_RESOLVED="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .resolvedAt // empty' <<<"$BODY" 2>/dev/null | head -1)"
EV_RSAMPLE="$(jq -r --arg n af-lag '.events[] | select(.ruleName==$n) | .resolvedSample.value // empty' <<<"$BODY" 2>/dev/null | head -1)"
{ [ "${EV_STATE2:-x}" = "resolved" ] && [ -n "$EV_RESOLVED" ] && [ "${EV_ID2:-x}" = "${OPEN_ID:-x}" ] && [ -n "$EV_ID2" ]; } \
  && step pass "R2.2 CORE the SAME episode row flipped to resolved (id=$EV_ID2 unchanged, resolvedAt set, resolvedSample.value=$EV_RSAMPLE) — no second row" \
  || step fail "R2.2 episode flip" "id=$EV_ID2 open=$OPEN_ID state=$EV_STATE2 resolvedAt=$EV_RESOLVED"
# STOP IMMEDIATELY: an idle-but-running file source re-accumulates lag with
# wall time (that IS the freshness semantics) — the rule legitimately
# re-fires ~6s after the last data. Anything slow between the resolved
# check and this stop would add a lawful firing+resolved pair (run-3
# finding, recorded in the header).
req POST "/api/pipelines/$LAG_PID/stop" '{}' "$TOKEN"
{ [ "$CODE" = "200" ]; } && step pass "R2.3 laggy pipeline stopped right after the resolve (before the idle lag could re-cross 6s)" \
  || step fail "R2.3 stop" "http=$CODE body=$BODY"
N="$(wait_hook 2 15)"
if [ "$N" = "2" ] && jq -e 'select(.event_header=="resolved")' "$WORK/hook.jsonl" >/dev/null 2>&1; then
  step pass "W1 CORE anti-flood: the webhook holds EXACTLY the episode's 2 notices (firing + resolved) — silent while firing persisted, silent after it closed, silent through the stop"
else
  step fail "W1 anti-flood" "n=$N hook=$(jq -rc '.event_header' "$WORK/hook.jsonl" 2>/dev/null | paste -sd, -)"
fi
# the stop's final flush + cursor commit leaves both rows in the sink file
# (resume→flush itself is sub-second — isolated pause/resume probe, 0.57s;
# the 60s firing→resolved gap of run 2 matches W-2 reaching wl_in ~61s after
# the firing per the file's mtime — script-side timing, not the engine)
DRAINED=0
for i in $(seq 1 30); do
  W1C="$(grep -c 'W-1' "$WORK/node/wl" 2>/dev/null || echo 0)"
  W2C="$(grep -c 'W-2' "$WORK/node/wl" 2>/dev/null || echo 0)"
  { [ "$W1C" = "1" ] && [ "$W2C" = "1" ]; } && { DRAINED=1; break; }
  sleep 2
done
if [ "$DRAINED" != "1" ]; then
  # stop's final flush is documented as best-effort; one more stop on the
  # (already stopped) pipeline is idempotent and re-joins the thread
  req POST "/api/pipelines/$LAG_PID/stop" '{}' "$TOKEN"
  for i in $(seq 1 15); do
    W2C="$(grep -c 'W-2' "$WORK/node/wl" 2>/dev/null || echo 0)"
    [ "$W2C" = "1" ] && { DRAINED=1; break; }
    sleep 2
  done
fi
{ [ "$DRAINED" = "1" ]; } \
  && step pass "W2.1 sink drained: both rows (W-1, W-2) flushed to the sink file after the stop" \
  || step fail "W2.1 drain" "w1=$(grep -c W-1 "$WORK/node/wl" 2>/dev/null) w2=$(grep -c W-2 "$WORK/node/wl" 2>/dev/null)"
# W3 final totals for the lag episode: still exactly 2 notices after the
# drain wait (the rule went quiet the moment its series vanished — no
# per-tick repeats ever)
TOTAL_HOOKS="$(wait_hook 2 20)"
{ [ "$TOTAL_HOOKS" = "2" ]; } \
  && step pass "W3 CORE anti-flood totals: still exactly 2 notices after the drain — one firing + one resolved across the WHOLE episode (pause 40s+, firing 60s+, stop), zero per-tick noise" \
  || step fail "W3 anti-flood totals" "total=$TOTAL_HOOKS hook=$(jq -rc '.event_header' "$WORK/hook.jsonl" 2>/dev/null | paste -sd, -)"

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

TOTAL=$((PASS+FAIL))
echo "== alert_flow e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
