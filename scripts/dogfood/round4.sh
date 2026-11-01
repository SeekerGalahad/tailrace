#!/usr/bin/env bash
# R11 W5 dogfooding round 4 — drive the two R11 control-plane surfaces the
# way a GitOps operator would, against a LOCAL file→file stack (no Doris in
# the path — this round is about the control plane, see the doc's boundary
# section):
#
#   MCP apply (T-V3) : the declaration file goes through the JSON-RPC face
#                      exclusively — initialize → tools/list (16 tools) →
#                      apply_dry_run (the structured diff) → apply without
#                      confirm (rejected) → apply with confirm (the diff
#                      lands) → apply_dry_run again (converged: all
#                      unchanged) → apply again (the idempotent 二连 —
#                      second summary all-zero, object bytes untouched);
#   the deploy hop   : apply only creates DRAFT — a pure-MCP agent next
#                      calls deploy_pipeline and gets the documented 400
#                      「管道尚未发布」 (there is no publish tool in the ≤16
#                      budget), so publish rides one REST call and deploy
#                      goes back through MCP → RUNNING;
#   GitOps edit      : v2 of the file changes checkpointMs 2000→5000 —
#                      dry-run shows exactly one update with the field
#                      diff, apply lands it (status PENDING_CHANGES, the
#                      RUNNING pipeline untouched), re-apply unchanged;
#   sharp edges      : two dry-run probes — a tables-section that omits
#                      orders_out reports it as an orphan (reported, never
#                      deleted) and a pipeline entry WITHOUT config shows
#                      `config: {…} -> null` (the documented wipe semantics
#                      of an omitted section);
#   SLO history (T-V4): a backlog seeded before the deploy (tuning
#                      pollRows=1, declared IN the file) keeps the source
#                      non-idle for tens of seconds → GET /api/slo?pipeline=N
#                      answers p50/p95/p99 with a sample count, and
#                      console /metrics carries the p95 series beside the
#                      SLO target line.
#
# The audit trail is read back like an operator would (sqlite3 on
# console.db): every MCP call row (MCP_TOOL_CALL, actor mcp:dogfood-r4),
# the APPLY summary rows of the three applies, and that the dry-runs wrote
# no APPLY/domain rows.
#
# 用法：
#   ./scripts/build.sh   # release binaries at target/release/…
#   bash scripts/dogfood/round4.sh [--help] [--keep]
#
# 对象：本地栈（console --mcp-allow-write + engine node serve）。Doris 不在
#   路径上；在役栈可达与否不影响本脚本。
#
# 覆盖变量：
#   DOGFOOD_CONSOLE_ADDR  console 监听   默认 127.0.0.1:18094
#   DOGFOOD_NODE_ADDR     node 监听      默认 127.0.0.1:18114
#   DOGFOOD_WORK          工作目录       默认 /tmp/tailrace-dogfood4
#   DOGFOOD_ROWS          SLO 段积压行数 默认 120000（pollRows=1 +
#                         flushRows=1 下 ≈0.2ms/行 → 排空 ~25s，跨 ~12 个
#                         评估拍；run2 实测不带 flushRows 时 0.07ms/行、4s
#                         排完——只喂了 SLO 环 2 个样本）
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制（默认 target/release/…）
#
# live-gate：磁盘 <5GiB → [skip] exit 0。依赖 jq、python3、curl、nc、sqlite3。
# 产物：$DOGFOOD_WORK/{summary.txt,mcp-calls.jsonl,mcp-responses.jsonl}；
#   --keep 保留工作目录（失败必留）。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
CONSOLE_ADDR="${DOGFOOD_CONSOLE_ADDR:-127.0.0.1:18094}"
NODE_ADDR="${DOGFOOD_NODE_ADDR:-127.0.0.1:18114}"
WORK="${DOGFOOD_WORK:-/tmp/tailrace-dogfood4}"
ROWS="${DOGFOOD_ROWS:-120000}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
case "$CONSOLE_BIN" in /*) ;; *) CONSOLE_BIN="$ROOT/$CONSOLE_BIN" ;; esac
case "$NODE_BIN" in /*) ;; *) NODE_BIN="$ROOT/$NODE_BIN" ;; esac
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --help|-h) sed -n '2,/^set -uo pipefail/p' "$0" | sed -e '/^set -uo/d' -e 's/^# \{0,1\}//'; exit 0 ;;
    --keep) KEEP=1 ;;
    *) echo "unknown flag: $arg (see --help)" >&2; exit 2 ;;
  esac
done

PASS=0; FAIL=0
CONSOLE_PID=""; NODE_PID=""
FAILED_STEPS=()
step() { # step <pass|fail> <name> [detail] — wall-clock stamped (alert_flow W5
         # style); lines also land in $WORK/summary.txt once it exists
  local line
  if [ "$1" = "pass" ]; then
    PASS=$((PASS+1)); line="$(printf '  ok    %s  %s' "$(date +%H:%M:%S)" "$2")"
  else
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$2"); line="$(printf '  FAIL  %s  %s  %s' "$(date +%H:%M:%S)" "$2" "${3:-}")"
  fi
  printf '%s\n' "$line"
  if [ -f "$WORK/summary.txt" ]; then printf '%s\n' "$line" >> "$WORK/summary.txt"; fi
  return 0
}
cleanup() {
  [ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
  [ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
  wait 2>/dev/null
  [ "$FAIL" -gt 0 ] && KEEP=1
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

req() { # REST helper → sets BODY/CODE
  local args=(-s -X "$1" -o "$WORK/body" -w '%{http_code}' "http://$CONSOLE_ADDR$2")
  [ -n "${3:-}" ] && args+=(-H 'Content-Type: application/json' -d "$3")
  [ -n "${4:-}" ] && args+=(-H "Authorization: Bearer $4")
  CODE="$(curl "${args[@]}")"; BODY="$(cat "$WORK/body" 2>/dev/null)"
}
wait_http() { local i; for ((i=0; i<$2; i++)); do [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ] && return 0; sleep 1; done; return 1; }
count_lines() { if [ -f "$1" ]; then grep -c . "$1" 2>/dev/null || true; else echo 0; fi; }

MCP_ID=0; MCP_SESSION=""
mcp() { # mcp <method> <params-json> → MBODY (parsed JSON-RPC response); logs both sides
  MCP_ID=$((MCP_ID+1))
  local payload; payload="$(jq -cn --arg m "$1" --argjson p "$2" --argjson id "$MCP_ID" '{jsonrpc:"2.0",id:$id,method:$m,params:$p}')"
  local args=(-s -o "$WORK/mcp.body" -D "$WORK/mcp.hdr" -X POST "http://$CONSOLE_ADDR/mcp"
              -H 'Content-Type: application/json' -H 'X-MCP-Client: dogfood-r4' -d "$payload")
  [ -n "$MCP_SESSION" ] && args+=(-H "Mcp-Session-Id: $MCP_SESSION")
  curl "${args[@]}" >/dev/null 2>&1
  MBODY="$(cat "$WORK/mcp.body" 2>/dev/null)"
  [ -z "$MCP_SESSION" ] && MCP_SESSION="$(awk 'tolower($1)=="mcp-session-id:" {print $2}' "$WORK/mcp.hdr" 2>/dev/null | tr -d '\r')"
  printf '%s\n' "$payload" >> "$WORK/mcp-calls.jsonl"
  printf '%s\n' "$MBODY" >> "$WORK/mcp-responses.jsonl"
}
tool() { # tool <name> <args-json> → T_HTTP (REST status), T_BODY (REST body), T_ERR, T_RPCERR
  mcp tools/call "$(jq -cn --arg n "$1" --argjson a "$2" '{name:$n,arguments:$a}')"
  T_HTTP="$(jq -r '.result.structuredContent.httpStatus // empty' <<<"$MBODY")"
  T_BODY="$(jq -c '.result.structuredContent.body // empty' <<<"$MBODY")"
  T_ERR="$(jq -r '.result.isError // false | tostring' <<<"$MBODY")"
  T_RPCERR="$(jq -c '.error // empty' <<<"$MBODY")"
}

echo "== dogfooding round4: work=$WORK console=$CONSOLE_ADDR node=$NODE_ADDR rows=$ROWS =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required"; exit 2; }
command -v sqlite3 >/dev/null || { echo "sqlite3 is required (audit-trail readback)"; exit 2; }

# ---------- live-gate ----------
AVAIL_GIB="$(df -g / | awk 'NR==2 {print $4}')"
if [ "${AVAIL_GIB:-99}" -lt 5 ]; then
  echo "[skip] host free disk ${AVAIL_GIB}GiB < 5GiB (OrbStack kill-line), skipped"; exit 0
fi
[ -x "$CONSOLE_BIN" ] && step pass "S0.1 console binary $CONSOLE_BIN" || step fail "S0.1 console binary" "missing (run ./scripts/build.sh)"
[ -x "$NODE_BIN" ] && step pass "S0.2 node binary $NODE_BIN" || step fail "S0.2 node binary" "missing"

# ---------- D1 stack: console (MCP write ON) + node ----------
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"
TAILRACE_BOOTSTRAP_TOKEN="tr_b_df4_00112233445566778899aabbccddeeff" \
TAILRACE_ALERT_INTERVAL_SECS=2 \
  "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" --mcp-allow-write >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
wait_http "http://$CONSOLE_ADDR/healthz" 20 \
  && step pass "D1.1 console booted with --mcp-allow-write (alert evaluator at 2s)" \
  || step fail "D1.1 console" "$(tail -3 "$WORK/console.log")"
grep -qi "mcp: .*write tools enabled" "$WORK/console.log" \
  && step pass "D1.2 console log: '$(grep -o 'mcp: .*' "$WORK/console.log" | head -1)'" \
  || step fail "D1.2 mcp log line" "$(grep -i mcp "$WORK/console.log")"
PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "D1.3 REST login (publish needs a session — see G3)" || step fail "D1.3 login" "http=$CODE"
( cd "$WORK/node" && exec "$NODE_BIN" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "tr_b_df4_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 \
  && step pass "D1.4 engine node up (serve; file tables resolve against $WORK/node)" \
  || step fail "D1.4 node boot" "$(tail -3 "$WORK/node.log")"
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "D1.5 node ONLINE (the deploy in G3 needs it)" || step fail "D1.5 ONLINE" "node.log=$(tail -3 "$WORK/node.log")"

# ---------- M0 the MCP face ----------
mcp initialize '{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"dogfood-r4","version":"1.0"}}'
{ [ -n "$MCP_SESSION" ] && [ "$(jq -r '.result.serverInfo.name // empty' <<<"$MBODY")" = "tailrace-console" ]; } \
  && step pass "M0.1 initialize → serverInfo tailrace-console, session $MCP_SESSION (client dogfood-r4)" \
  || step fail "M0.1 initialize" "session='$MCP_SESSION' body=$(head -c 200 <<<"$MBODY")"
mcp tools/list '{}'
TOOLS_N="$(jq -r '.result.tools | length' <<<"$MBODY" 2>/dev/null)"
HAS_APPLY="$(jq -r '.result.tools[] | select(.name=="apply") | 1' <<<"$MBODY" 2>/dev/null | head -1)"
APPLY_DESC="$(jq -r '.result.tools[] | select(.name=="apply") | .description' <<<"$MBODY" 2>/dev/null | head -1)"
DRY_DESC="$(jq -r '.result.tools[] | select(.name=="apply_dry_run") | .description' <<<"$MBODY" 2>/dev/null | head -1)"
{ [ "$TOOLS_N" = "16" ] && [ -n "$HAS_APPLY" ] && grep -q "POST /api/apply" <<<"$APPLY_DESC" && grep -q "POST /api/apply/dry-run" <<<"$DRY_DESC"; } \
  && step pass "M0.2 tools/list: 16 tools incl. apply (W, confirm-gated) + apply_dry_run (R); descriptions route-prefixed by the OpenAPI same-source generation ('POST /api/apply…')" \
  || step fail "M0.2 tools/list" "n=$TOOLS_N apply=$HAS_APPLY desc=$(head -c 120 <<<"$APPLY_DESC")"

# ---------- G1 the GitOps loop: dry-run diff → apply → idempotent re-apply ----------
cat > "$WORK/pipelines.v1.json" <<JSON
{
  "datasources": [
    {"name": "df4-files", "type": "file", "connJson": {"path": "$WORK/node"}}
  ],
  "tables": [
    {"datasource": "df4-files", "dbName": "local", "tableName": "orders_in",
     "format": "jsonl", "columnsJson": [["id","utf8"],["amount","int64"]]},
    {"datasource": "df4-files", "dbName": "local", "tableName": "orders_out",
     "format": "jsonl", "columnsJson": [["id","utf8"],["amount","int64"]]}
  ],
  "pipelines": [
    {"name": "df4-orders",
     "sqlContent": "INSERT INTO local.orders_out SELECT id, amount FROM local.orders_in",
     "dsMapping": {"local": "df4-files"},
     "checkpointMs": 2000,
     "config": {"reliability": null, "recovery": null,
                "tuning": {"pollRows": 1, "flushRows": 1}, "groupPrefix": null,
                "startOffset": null, "badRowPolicy": "dlq"}}
  ]
}
JSON
step pass "G1.0 declaration file v1 written (1 file datasource, 2 tables, 1 pipeline — tuning pollRows=1/flushRows=1 declared in the file so the SLO drain below is observable at all)"
V1="$(jq -c . "$WORK/pipelines.v1.json")"
tool apply_dry_run "$V1"
SUM1="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
ACTS="$(jq -r '[.. | .action? // empty] | join(",")' <<<"$T_BODY" 2>/dev/null)"
{ [ "$T_HTTP" = "200" ] && [ "$T_ERR" = "false" ] && [ "$(jq -r '.dryRun' <<<"$T_BODY")" = "true" ] \
    && [ "$SUM1" = "$(jq -cS . <<<'{"create":4,"update":0,"unchanged":0,"orphan":0}')" ] && [ "$ACTS" = "create,create,create,create" ]; } \
  && step pass "G1.1 CORE apply_dry_run (read tool, no confirm): dryRun=true, summary $SUM1, every action=create — the diff before anything exists" \
  || step fail "G1.1 dry-run" "http=$T_HTTP err=$T_ERR summary=$SUM1 acts=[$ACTS] body=$(head -c 200 <<<"$T_BODY")"
# zero side effects: nothing exists yet
req GET /api/pipelines "" "$TOKEN"
NPIPES="$(jq -r '.total // empty' <<<"$BODY" 2>/dev/null)"
req GET /api/datasources "" "$TOKEN"
NDS="$(jq -r '(.items // []) | length' <<<"$BODY" 2>/dev/null)"
{ [ "${NPIPES:-x}" = "0" ] && [ "${NDS:-x}" = "0" ]; } \
  && step pass "G1.2 dry-run wrote nothing: 0 pipelines, 0 datasources in the store" \
  || step fail "G1.2 dry-run side effects" "pipelines=$NPIPES datasources=$NDS"
tool apply "$V1"
{ [ "$(jq -r '.error.data.code // empty' <<<"$MBODY")" = "CONFIRM_REQUIRED" ] && [ "$(jq -r '.error.code // empty' <<<"$MBODY")" = "-32602" ]; } \
  && step pass "G1.3 apply WITHOUT confirm → JSON-RPC -32602 CONFIRM_REQUIRED (write gate 2), nothing applied" \
  || step fail "G1.3 confirm gate" "body=$(head -c 200 <<<"$MBODY")"
tool apply "$(jq -c '. + {confirm:true}' "$WORK/pipelines.v1.json")"
SUM2="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
P_ID="$(jq -r '.pipelines[0].id // empty' <<<"$T_BODY" 2>/dev/null)"
{ [ "$T_HTTP" = "200" ] && [ "$(jq -r '.dryRun' <<<"$T_BODY")" = "false" ] && [ "$SUM2" = "$SUM1" ] && [ -n "$P_ID" ]; } \
  && step pass "G1.4 CORE apply WITH confirm → http 200, summary $SUM2 == the dry-run summary (one plan() for both), pipeline id=$P_ID assigned" \
  || step fail "G1.4 apply" "http=$T_HTTP summary=$SUM2 pid=$P_ID body=$(head -c 200 <<<"$T_BODY")"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
STATUS1="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
DSID="$(jq -r '.dsMapping.local // empty' <<<"$BODY" 2>/dev/null)"
POLL_ROWS="$(jq -r '.config.tuning.pollRows // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$STATUS1" = "DRAFT" ] && [ -n "$DSID" ] && [ "$POLL_ROWS" = "1" ]; } \
  && step pass "G1.5 the created pipeline reads back DRAFT, dsMapping name→id resolved (local=$DSID), config.tuning.pollRows=1 carried verbatim — apply 不发布不部署" \
  || step fail "G1.5 readback" "status=$STATUS1 ds=$DSID pollRows=$POLL_ROWS body=$(head -c 200 <<<"$BODY")"
# the idempotent double-apply
tool apply_dry_run "$V1"
SUM3="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
[ "$SUM3" = "$(jq -cS . <<<'{"create":0,"update":0,"unchanged":4,"orphan":0}')" ] \
  && step pass "G1.6 CORE dry-run after apply: summary $SUM3 — the file has converged with the store" \
  || step fail "G1.6 converged dry-run" "summary=$SUM3"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
UA1="$(jq -r '.updatedAt // empty' <<<"$BODY" 2>/dev/null)"
sleep 1
tool apply "$(jq -c '. + {confirm:true}' "$WORK/pipelines.v1.json")"
SUM4="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
UA2="$(jq -r '.updatedAt // .updated_at // empty' <<<"$BODY" 2>/dev/null)"
req GET /api/datasources "" "$TOKEN"
GEN2="$(jq -r --arg n df4-files '(.items // [])[] | select(.name==$n) | (.generation | tostring) // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$SUM4" = "$(jq -cS . <<<'{"create":0,"update":0,"unchanged":4,"orphan":0}')" ] && [ "$UA1" = "$UA2" ] && [ -n "$GEN2" ]; } \
  && step pass "G1.7 CORE 幂等二连: second apply summary $SUM4, pipeline updatedAt byte-unchanged ($UA2), datasource generation still $GEN2 — unchanged objects are not touched" \
  || step fail "G1.7 idempotent apply" "summary=$SUM4 ua1=$UA1 ua2=$UA2 gen=$GEN2"

# ---------- G3 the deploy hop: where pure-MCP GitOps stops ----------
tool deploy_pipeline "{\"id\":$P_ID,\"confirm\":true}"
DEP_MSG="$(jq -r '.error.message // .message // empty' <<<"$T_BODY" 2>/dev/null)"
{ [ "$T_ERR" = "true" ] && [ "$T_HTTP" = "400" ] && grep -q "发布" <<<"$DEP_MSG"; } \
  && step pass "G3.1 FRICTION (by design, recorded): MCP deploy_pipeline on the fresh DRAFT → isError http 400 '$DEP_MSG' — there is no publish tool in the ≤16 budget, so the GitOps loop leaves MCP here" \
  || step fail "G3.1 deploy 400" "err=$T_ERR http=$T_HTTP msg=$DEP_MSG"
req POST "/api/pipelines/$P_ID/publish" '' "$TOKEN"
PUB_CODE="$CODE"
{ [ "$PUB_CODE" = "200" ]; } && step pass "G3.2 publish via ONE REST call (the only step off the MCP face)" || step fail "G3.2 publish" "http=$PUB_CODE body=$(head -c 200 <<<"$BODY")"
# SLO rig: the backlog is seeded BEFORE the deploy so the drain starts with it
python3 - "$WORK/node/orders_in" "$ROWS" <<'PY'
import sys
path, n = sys.argv[1], int(sys.argv[2])
with open(path, "w", encoding="utf-8") as f:
    for i in range(1, n + 1):
        f.write('{"id":"O-%d","amount":%d}\n' % (i, i))
PY
step pass "S1.0 SLO rig: $ROWS backlog rows seeded into orders_in before the deploy (pollRows=1+flushRows=1 keep the source non-idle for tens of seconds)"
T_DRAIN0=$(date +%s)
tool deploy_pipeline "{\"id\":$P_ID,\"confirm\":true}"
RUN_OK="$(jq -r '.ok // empty' <<<"$T_BODY" 2>/dev/null)"
{ [ "$T_HTTP" = "200" ] && [ "$RUN_OK" = "true" ]; } \
  && step pass "G3.3 CORE deploy back through MCP → engine ok=true (the pipeline is live; SQL/tables came from the applied declaration)" \
  || step fail "G3.3 MCP deploy" "http=$T_HTTP ok=$RUN_OK body=$(head -c 200 <<<"$T_BODY")"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
[ "$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)" = "RUNNING" ] \
  && step pass "G3.4 pipeline RUNNING (file→file, declared end to end by pipelines.v1.json)" \
  || step fail "G3.4 running" "status=$(jq -r '.status' <<<"$BODY")"

# ---------- S2 the SLO history: p95 with a sample count ----------
# A lag rule (threshold 30, no webhook) puts the SLO target line beside the
# achieved quantiles, the way the Grafana row reads them.
req POST /api/alerts/rules "{\"name\":\"df4-lag\",\"ruleType\":\"lag_seconds\",\"params\":{\"threshold\":30,\"pipeline\":\"$P_ID\"}}" "$TOKEN"
RULE_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
[ -n "$RULE_ID" ] && step pass "S2.1 lag rule df4-lag (threshold 30s, pipeline=$P_ID, no webhook) → the SLO target line" || step fail "S2.1 rule" "http=$CODE"
SLO_JSON=""
for i in $(seq 1 40); do
  req GET "/api/slo?pipeline=$P_ID" "" "$TOKEN"
  S_JSON="$(jq -c '.series[0] // empty' <<<"$BODY" 2>/dev/null)"
  S_N="$(jq -r '.samples // 0' <<<"$S_JSON" 2>/dev/null)"
  [ "${S_N:-0}" -ge 3 ] 2>/dev/null && { SLO_JSON="$S_JSON"; break; }
  sleep 2
done
S_P95="$(jq -r '.p95 // empty' <<<"$SLO_JSON" 2>/dev/null)"
S_N="$(jq -r '.samples // 0' <<<"$SLO_JSON" 2>/dev/null)"
S_P50="$(jq -r '.p50 // empty' <<<"$SLO_JSON" 2>/dev/null)"; S_P99="$(jq -r '.p99 // empty' <<<"$SLO_JSON" 2>/dev/null)"
[ -n "$S_P95" ] && [ "${S_N:-0}" -ge 3 ] 2>/dev/null \
  && step pass "S2.2 CORE GET /api/slo?pipeline=$P_ID: p50=$S_P50 p95=$S_P95 p99=$S_P99, samples=$S_N — the quantiles carry their support (non-idle drain passes only; T-V1 idle series are not sampled)" \
  || step fail "S2.2 /api/slo" "series=$(head -c 200 <<<"$SLO_JSON") body=$(head -c 200 <<<"$BODY")"
SLO95="$(curl -s "http://$CONSOLE_ADDR/metrics" | grep "^tailrace_slo_lag_p95_seconds{.*pipeline=\"$P_ID\"" | head -1)"
SLOT="$(curl -s "http://$CONSOLE_ADDR/metrics" | grep "^tailrace_slo_lag_target_seconds{.*pipeline=\"$P_ID\"" | head -1)"
SLON="$(curl -s "http://$CONSOLE_ADDR/metrics" | grep "^tailrace_slo_lag_samples{.*pipeline=\"$P_ID\"" | head -1)"
{ [ -n "$SLO95" ] && [ -n "$SLOT" ] && [ -n "$SLON" ]; } \
  && step pass "S2.3 console /metrics: $SLO95 with $SLON beside the target $SLOT — the Grafana SLO row's data plane" \
  || step fail "S2.3 metrics" "p95='$SLO95' samples='$SLON' target='$SLOT'"
# wait out the drain and record the throughput as an operator fact
DRAINED=0
for i in $(seq 1 240); do
  GOT="$(count_lines "$WORK/node/orders_out")"
  [ "$GOT" = "$ROWS" ] && { DRAINED=1; break; }
  sleep 1
done
DT=$(( $(date +%s) - T_DRAIN0 ))
{ [ "$DRAINED" = "1" ]; } \
  && step pass "S2.4 backlog drained: $ROWS rows landed in orders_out in ≤${DT}s (≈$(( ROWS / (DT>0?DT:1) )) rows/s at pollRows=1 — the per-row batch cost is the drain's speed)" \
  || step fail "S2.4 drain" "lines=$GOT want=$ROWS"
# after the drain the source is idle (EOF): no NEW samples, the ring keeps
# the window's history — that is the file-source SLO shape (doc friction)
sleep 4
req GET "/api/slo?pipeline=$P_ID" "" "$TOKEN"
S_N2="$(jq -r '.series[0].samples // 0' <<<"$BODY" 2>/dev/null)"
S_LAG="$(curl -s "http://$CONSOLE_ADDR/metrics" | grep "^tailrace_pipeline_source_idle{.*pipeline=\"$P_ID\"" | head -1)"
[ "${S_N2:-0}" -ge "${S_N:-0}" ] 2>/dev/null \
  && step pass "S2.5 after the drain: idle gauge '$S_LAG' (EOF), the ring keeps its $S_N2 samples (1h retention) — no new samples accrue over silence" \
  || step fail "S2.5 idle ring" "samples=$S_N2 (was $S_N)"

# ---------- G4 GitOps edit: v2 changes checkpointMs ----------
jq '.pipelines[0].checkpointMs = 5000' "$WORK/pipelines.v1.json" > "$WORK/pipelines.v2.json"
V2="$(jq -c . "$WORK/pipelines.v2.json")"
tool apply_dry_run "$V2"
SUM5="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
ACT5="$(jq -r '.pipelines[0].action // empty' <<<"$T_BODY" 2>/dev/null)"
FLD5="$(jq -r '.pipelines[0].changes[0].field // empty' <<<"$T_BODY" 2>/dev/null)"
FRM5="$(jq -r '.pipelines[0].changes[0].from // empty' <<<"$T_BODY" 2>/dev/null)"
TO5="$(jq -r '.pipelines[0].changes[0].to // empty' <<<"$T_BODY" 2>/dev/null)"
{ [ "$SUM5" = "$(jq -cS . <<<'{"create":0,"update":1,"unchanged":3,"orphan":0}')" ] && [ "$ACT5" = "update" ] && [ "$FLD5" = "checkpointMs" ] && [ "$FRM5" = "2000" ] && [ "$TO5" = "5000" ]; } \
  && step pass "G4.1 CORE v2 dry-run: summary $SUM5, exactly one update — pipelines[0].changes=[{field: checkpointMs, from: 2000, to: 5000}] (field-level diff, not a whole-object swap)" \
  || step fail "G4.1 v2 diff" "summary=$SUM5 act=$ACT5 field=$FLD5 $FRM5→$TO5 body=$(head -c 200 <<<"$T_BODY")"
tool apply "$(jq -c '. + {confirm:true}' "$WORK/pipelines.v2.json")"
SUM6="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
CK2="$(jq -r '.checkpointMs // empty' <<<"$BODY" 2>/dev/null)"
ST2="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$SUM6" = "$SUM5" ] && [ "$CK2" = "5000" ] && [ "$ST2" = "PENDING_CHANGES" ]; } \
  && step pass "G4.2 CORE apply v2 → checkpointMs=5000 landed, status=$ST2 (a non-DRAFT save flips PENDING_CHANGES; apply 不发布不部署 — the RUNNING pipeline itself is untouched)" \
  || step fail "G4.2 v2 apply" "summary=$SUM6 ck=$CK2 status=$ST2"
tool apply "$(jq -c '. + {confirm:true}' "$WORK/pipelines.v2.json")"
SUM7="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
[ "$SUM7" = "$(jq -cS . <<<'{"create":0,"update":0,"unchanged":4,"orphan":0}')" ] \
  && step pass "G4.3 v2 re-apply → summary $SUM7 (converged again — the GitOps loop's steady state)" \
  || step fail "G4.3 v2 re-apply" "summary=$SUM7"

# ---------- G5 sharp edges, dry-run only ----------
# present tables-section = the declared FULL set: orders_out is an orphan
jq 'del(.datasources, .pipelines) | .tables = [.tables[0]]' "$WORK/pipelines.v2.json" > "$WORK/probe.orphan.json"
tool apply_dry_run "$(jq -c . "$WORK/probe.orphan.json")"
SUMO="$(jq -cS '.summary' <<<"$T_BODY" 2>/dev/null)"
ORPH="$(jq -r '.orphans[0].key // empty' <<<"$T_BODY" 2>/dev/null)"
{ [ "$SUMO" = "$(jq -cS . <<<'{"create":0,"update":0,"unchanged":1,"orphan":1}')" ] && [ "$ORPH" = "df4-files/local.orders_out" ]; } \
  && step pass "G5.1 orphan probe: a tables-section naming only orders_in → summary $SUMO, orphans[0]=$ORPH — REPORTED, never deleted (v1 safety)" \
  || step fail "G5.1 orphan" "summary=$SUMO orphan=$ORPH body=$(head -c 200 <<<"$T_BODY")"
req GET "/api/datasources/tables?datasourceId=$DSID" "" "$TOKEN"
NTAB="$(jq -r '.total // empty' <<<"$BODY" 2>/dev/null)"
[ "${NTAB:-x}" = "2" ] && step pass "G5.2 the orphaned table still registered (total=$NTAB) — a dry-run never deletes" || step fail "G5.2 orphan kept" "tables=$NTAB"
# omitted config = declared null: the sharp edge the doc warns about
jq 'del(.datasources, .tables) | .pipelines[0] |= del(.config)' "$WORK/pipelines.v2.json" > "$WORK/probe.noconf.json"
tool apply_dry_run "$(jq -c . "$WORK/probe.noconf.json")"
FLDC="$(jq -r '.pipelines[0].changes[]? | select(.field=="config") | (.from|tostring) + " -> " + (.to|tostring)' <<<"$T_BODY" 2>/dev/null | head -1)"
{ [ "$(jq -r '.pipelines[0].action // empty' <<<"$T_BODY" 2>/dev/null)" = "update" ] && [ -n "$FLDC" ] && grep -q "null" <<<"$FLDC"; } \
  && step pass "G5.3 omitted-config probe: dry-run shows pipelines[0] update with a config change '… -> null' — omitting config DECLARES 'no advanced config' (pipeline-file.md §五); an export that drops config would wipe tuning on apply" \
  || step fail "G5.3 config omission" "action=$(jq -r '.pipelines[0].action' <<<"$T_BODY") change='$FLDC'"

# ---------- A audit trail, read back from the store ----------
AUD="$(sqlite3 "$WORK/run/console.db" "select action || '|' || actor from audit_log where actor like 'mcp:%' order by id;" 2>/dev/null)"
A_CALLS="$(grep -c 'MCP_TOOL_CALL' <<<"$AUD")"
A_APPLY="$(sqlite3 "$WORK/run/console.db" "select count(*) from audit_log where action='APPLY';" 2>/dev/null)"
A_VIA="$(sqlite3 "$WORK/run/console.db" "select count(*) from audit_log where action='PIPELINE_CREATE' and actor like 'mcp:%' and json_extract(detail_json,'$.via')='apply';" 2>/dev/null)"
# 12 tools/call invocations above (5 in G1 + 2 in G3 + 3 in G4 + 2 in G5);
# APPLY rows only for applies that WROTE (v1 create + v2 update) — the two
# no-op re-applies audit nothing (idempotency contract: 零写零审计)
{ [ "${A_CALLS:-0}" = "12" ] && [ "${A_APPLY:-0}" = "2" ] && [ "${A_VIA:-0}" -ge 1 ] 2>/dev/null; } \
  && step pass "A1 audit trail: exactly $A_CALLS MCP_TOOL_CALL rows — one per tools/call, actor $(grep -o 'mcp:[a-z0-9-]*' <<<"$AUD" | head -1); exactly $A_APPLY APPLY summary rows (v1 create + v2 update — dry-runs AND the two no-op re-applies audited nothing); PIPELINE_CREATE carries via=apply under the MCP actor" \
  || step fail "A1 audit" "calls=$A_CALLS (want 12) applies=$A_APPLY (want 2) via=$A_VIA rows=$(head -c 200 <<<"$AUD")"

# ---------- D9 teardown ----------
req POST "/api/pipelines/$P_ID/stop" '{}' "$TOKEN"
[ "$CODE" = "200" ] && step pass "D9.1 pipeline stopped" || step fail "D9.1 stop" "http=$CODE body=$BODY"
req DELETE "/api/alerts/rules/$RULE_ID" '' "$TOKEN"
[ "$CODE" = "204" ] && step pass "D9.2 lag rule deleted" || step fail "D9.2 rule" "http=$CODE"

TOTAL=$((PASS+FAIL))
printf '%s\n' "== dogfooding round4 summary: $PASS/$TOTAL steps passed ==" | tee -a "$WORK/summary.txt"
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:" | tee -a "$WORK/summary.txt"; printf '  - %s\n' "${FAILED_STEPS[@]}" | tee -a "$WORK/summary.txt"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
