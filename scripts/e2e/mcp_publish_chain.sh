#!/usr/bin/env bash
# R12 T-C e2e: the pure-MCP GitOps chain end to end against a real console
# + engine node (file→file, no Doris — the MCP surface is under test, not a
#   third-party server):
#
#   S  stack: console (--mcp-allow-write, so the MCP write gates are live)
#         → login → mock metrics tail → engine node ONLINE
#   G  GitOps via MCP only:
#       G1 apply_dry_run  (read tool, no gate) → structured diff
#                  summary create=3 (datasource + 2 tables + 1 pipeline)
#       G2 apply        (write: switch + confirm:true) → idempotent upsert,
#                  pipeline lands DRAFT, summary create=3
#       G3 apply again  (replay of the converged file) → zero changes,
#                  summary create/update 0, unchanged=3 (幂等重放)
#       G4 publish_pipeline (write: switch + confirm:true) → version 1 +
#                  spec snapshot, pipeline PUBLISHED, PIPELINE_PUBLISH audit
#                  row under mcp:<client>
#   L  lag: GET /api/pipelines/{id} → status PUBLISHED, spec_json carries the
#         materialised ${ds:...} spec; console /metrics scraped (the pipeline
#         is published but not deployed, so no lag series is expected — the
#         scrape itself is the "查 lag" hop and must not 5xx).
#
# 用法：
#   ./scripts/build.sh   # release 二进制 target/release/…
#   bash scripts/e2e/mcp_publish_chain.sh [--keep]
#
# 断言（任务卡 T-C e2e：apply → dry-run → publish → 查 lag 全链）：
#   G1 dry-run zero side effects, create=3
#   G2 apply creates datasource/tables/pipeline, pipeline DRAFT
#   G3 re-apply is a no-op (unchanged=3, no domain writes)
#   G4 publish bumps version, spec_json non-empty, status PUBLISHED
#   L  /api/pipelines/{id} 200 + /metrics scrape 200 (lag hop reachable)
#
# 覆盖变量：
#   TAILRACE_E2E_CONSOLE_ADDR   默认 127.0.0.1:18088
#   TAILRACE_E2E_NODE_ADDR      默认 127.0.0.1:18108
#   TAILRACE_E2E_WORK           默认 /tmp/tailrace-mpc-e2e
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN
#                               默认 target/release/tailrace-console|node
#   TAILRACE_ALERT_INTERVAL_SECS 脚本固定注入 2（生产默认 15s 不在射程内）
#
# 标志：--help / --keep（失败时自动保留工作目录）
#
# 依赖：jq、curl、nc。
# live-gate：宿主可用磁盘 <5GiB（OrbStack 红线）时空过。Docker/Doris 一概
#   不需要——file sink 管道全程本地。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18088}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18108}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
case "$CONSOLE_BIN" in /*) ;; *) CONSOLE_BIN="$ROOT/$CONSOLE_BIN" ;; esac
case "$NODE_BIN" in /*) ;; *) NODE_BIN="$ROOT/$NODE_BIN" ;; esac
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-mpc-e2e}"
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
CONSOLE_PID=""; NODE_PID=""
FAILED_STEPS=()
step() { # step <pass|fail> <name> [detail]
  if [ "$1" = "pass" ]; then
    PASS=$((PASS+1)); printf '  ok    %s  %s\n' "$(date +%H:%M:%S)" "$2"
  else
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$2"); printf '  FAIL  %s  %s  %s\n' "$(date +%H:%M:%S)" "$2" "${3:-}"
  fi
}

cleanup() {
  [ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
  [ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
  wait 2>/dev/null
  [ "$FAIL" -gt 0 ] && KEEP=1
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

req() { # req <METHOD> <path> [json-body] → sets BODY/CODE (session cookie)
  local args=(-s -X "$1" -o "$WORK/body" -w '%{http_code}' "http://$CONSOLE_ADDR$2" -b "$WORK/cookie.txt" -c "$WORK/cookie.txt")
  [ -n "${3:-}" ] && args+=(-H 'Content-Type: application/json' -d "$3")
  CODE="$(curl "${args[@]}")"
  BODY="$(cat "$WORK/body" 2>/dev/null)"
}
mcp() { # mcp <tool> <args-json> → sets BODY/CODE (JSON-RPC over /mcp, no cookie)
  local body_json
  body_json="$(jq -n --arg name "$1" --argjson args "$2" \
    '{jsonrpc:"2.0",id:1,method:"tools/call",params:{name:$name,arguments:$args}}')"
  CODE="$(curl -s -X POST -o "$WORK/mcp.body" -w '%{http_code}' \
    "http://$CONSOLE_ADDR/mcp" \
    -H 'Content-Type: application/json' -d "$body_json")"
  BODY="$(cat "$WORK/mcp.body" 2>/dev/null)"
}
mcp_result() { # mcp_result → the structured REST body under
               # .result.structuredContent.body (compact JSON), e.g. the
               # structured diff or publish response
  jq -c '.result.structuredContent.body // empty' "$WORK/mcp.body"
}
wait_http() { # wait_http <url> <tries>
  local i
  for ((i=0; i<$2; i++)); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ] && return 0
    sleep 1
  done
  return 1
}

echo "== mcp_publish_chain e2e: work=$WORK console=$CONSOLE_ADDR node=$NODE_ADDR =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }
command -v curl >/dev/null || { echo "curl is required"; exit 2; }

# ---------- live-gate ----------
AVAIL_GIB="$(df -g / | awk 'NR==2 {print $4}')"
if [ "${AVAIL_GIB:-99}" -lt 5 ]; then
  echo "[skip] host free disk ${AVAIL_GIB}GiB < 5GiB (OrbStack kill-line), skipped"
  exit 0
fi
[ -x "$CONSOLE_BIN" ] && step pass "S0.1 console binary $CONSOLE_BIN" \
  || { step fail "S0.1 console binary" "missing $CONSOLE_BIN (run ./scripts/build.sh)"; exit 2; }
[ -x "$NODE_BIN" ] && step pass "S0.2 engine node binary $NODE_BIN" \
  || { step fail "S0.2 node binary" "missing $NODE_BIN (run ./scripts/build.sh)"; exit 2; }

# ---------- S1 console + node ----------
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"
TAILRACE_BOOTSTRAP_TOKEN="tr_b_mpc_e2e_00112233445566778899aabbccddeeff" \
TAILRACE_ALERT_INTERVAL_SECS=2 \
  "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" --mcp-allow-write >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
wait_http "http://$CONSOLE_ADDR/healthz" 20 \
  && step pass "S1.1 console booted (--mcp-allow-write)" \
  || { step fail "S1.1 console boot" "$(tail -3 "$WORK/console.log")"; exit 2; }
ADMIN_PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
touch "$WORK/cookie.txt"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "S1.2 login → 200 + token" \
  || { step fail "S1.2 login" "http=$CODE body=$BODY"; exit 2; }

( cd "$WORK/node" && exec "$NODE_BIN" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "tr_b_mpc_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 \
  && step pass "S1.3 engine node up (serve)" \
  || { step fail "S1.3 node boot" "$(tail -3 "$WORK/node.log")"; exit 2; }
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "S1.4 node ONLINE (datasource + tables + pipeline will all be created by the MCP apply below)" \
  || { step fail "S1.4 node ONLINE" "node.log=$(tail -5 "$WORK/node.log")"; exit 2; }

# ---------- G GitOps via MCP ----------
DS_NAME="mpc-ds"
FILE="$(jq -n \
  --arg ds "$DS_NAME" \
  --arg path "$WORK/node" \
  '{
    "datasources": [{"name":$ds,"type":"file","connJson":{"path":$path}}],
    "tables": [
      {"datasource":$ds,"dbName":"local","tableName":"wl_in","format":"jsonl","columnsJson":[["id","utf8"]]},
      {"datasource":$ds,"dbName":"local","tableName":"wl","format":"jsonl","columnsJson":[["id","utf8"]]}
    ],
    "pipelines": [
      {"name":"mcp-publish-pipe","sqlContent":"INSERT INTO local.wl SELECT id FROM local.wl_in","dsMapping":{"local":$ds},"checkpointMs":10000}
    ]
  }')"
echo "$FILE" > "$WORK/declare.json"

# G1 apply_dry_run: read tool, no gate — structured diff, zero side effects
mcp apply_dry_run "$FILE"
[ "$CODE" = "200" ] || { step fail "G1 dry-run http" "http=$CODE body=$BODY"; exit 2; }
RESULT="$(mcp_result)"
[ -n "$RESULT" ] || { step fail "G1 dry-run parse" "body=$BODY"; exit 2; }
CREATE="$(jq -r '.summary.create // empty' <<<"$RESULT")"
[ "$CREATE" = "4" ] && step pass "G1 apply_dry_run: diff create=4 (ds+2 tables+1 pipeline)" \
  || { step fail "G1 apply_dry_run summary" "body=$BODY"; exit 2; }
jq -e '.datasources[0].action=="create" and .tables[0].action=="create" and .tables[1].action=="create" and .pipelines[0].action=="create"' <<<"$RESULT" >/dev/null \
  && step pass "G1 all four entries planned create" \
  || { step fail "G1 entry actions" "body=$BODY"; exit 2; }

# G2 apply: write gate (switch + confirm:true) → idempotent upsert, DRAFT
mcp apply "$(jq -c '. + {"confirm":true}' "$WORK/declare.json")"
[ "$CODE" = "200" ] || { step fail "G2 apply http" "http=$CODE body=$BODY"; exit 2; }
RESULT="$(mcp_result)"
CREATE="$(jq -r '.summary.create // empty' <<<"$RESULT")"
[ "$CREATE" = "4" ] && step pass "G2 apply: summary create=4" \
  || { step fail "G2 apply summary" "body=$BODY"; exit 2; }
PID="$(jq -r '.pipelines[0].id // empty' <<<"$RESULT")"
[ -n "$PID" ] || { step fail "G2 pipeline id" "body=$BODY"; exit 2; }
req GET "/api/pipelines/$PID" "" "$TOKEN"
[ "$CODE" = "200" ] && [ "$(jq -r '.status // empty' <<<"$BODY")" = "DRAFT" ] \
  && step pass "G2 pipeline $PID is DRAFT (apply upserts, does not publish)" \
  || { step fail "G2 pipeline status" "http=$CODE body=$BODY"; exit 2; }

# G3 re-apply the converged file: n-op, unchanged=3, no new domain writes
mcp apply "$(jq -c '. + {"confirm":true}' "$WORK/declare.json")"
[ "$CODE" = "200" ] || { step fail "G3 re-apply http" "http=$CODE body=$BODY"; exit 2; }
RESULT="$(mcp_result)"
UNCHANGED="$(jq -r '.summary.unchanged // empty' <<<"$RESULT")"
CREATE="$(jq -r '.summary.create // empty' <<<"$RESULT")"
UPDATE="$(jq -r '.summary.update // empty' <<<"$RESULT")"
[ "$UNCHANGED" = "4" ] && [ "$CREATE" = "0" ] && [ "$UPDATE" = "0" ] \
  && step pass "G3 re-apply is a no-op: unchanged=4 create=0 update=0 (幂等重放)" \
  || { step fail "G3 re-apply summary" "body=$BODY"; exit 2; }

# G4 publish_pipeline: write gate → version 1 + spec snapshot, PUBLISHED
mcp publish_pipeline "$(jq -c --arg id "$PID" '{confirm:true,id:($id|tonumber)}' <<<"{}")"
[ "$CODE" = "200" ] || { step fail "G4 publish http" "http=$CODE body=$BODY"; exit 2; }
RESULT="$(mcp_result)"
VERSION="$(jq -r '.version // empty' <<<"$RESULT")"
SPEC="$(jq -r '.specJson // empty' <<<"$RESULT")"
[ -n "$VERSION" ] && [ -n "$SPEC" ] \
  && step pass "G4 publish: version=$VERSION spec snapshot present" \
  || { step fail "G4 publish body" "body=$BODY"; exit 2; }
req GET "/api/pipelines/$PID" "" "$TOKEN"
[ "$CODE" = "200" ] && [ "$(jq -r '.status // empty' <<<"$BODY")" = "PUBLISHED" ] \
  && step pass "G4 pipeline $PID is PUBLISHED after MCP publish" \
  || { step fail "G4 pipeline status" "http=$CODE body=$BODY"; exit 2; }
echo "$SPEC" | jq -e 'contains("local.wl") and contains("local.wl_in")' >/dev/null \
  && step pass "G4 spec_json carries materialised ${ds:...} references" \
  || { step fail "G4 spec content" "spec=$SPEC"; exit 2; }

# ---------- L lag hop: published but not deployed → no lag series expected,
#         the scrape itself is the "查 lag" hop and must stay 200 ----------
req GET "/api/pipelines/$PID" "" "$TOKEN"
[ "$CODE" = "200" ] && [ -n "$(jq -r '.spec_json // empty' <<<"$BODY")" ] \
  && step pass "L pipeline object readable, spec_json present" \
  || { step fail "L pipeline object" "http=$CODE body=$BODY"; exit 2; }
METRICS="$(curl -s "http://$CONSOLE_ADDR/metrics")"
[ -n "$METRICS" ] && step pass "L console /metrics scrape 200 (lag hop reachable; no series expected — pipeline not deployed)" \
  || { step fail "L metrics scrape" "body=$(head -c 200 <<<"$METRICS")"; exit 2; }

echo
echo "== mcp_publish_chain: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" = "0" ] || { echo "FAILED steps: ${FAILED_STEPS[*]}" >&2; exit 1; }
