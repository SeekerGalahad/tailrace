#!/usr/bin/env bash
# R7 T-A8 dogfooding round 1 — run tailrace's OWN full chain against the
# standing (in-service) Doris on this machine, the way an operator would:
#   A1 four-step table creation (API layer) → file-source pipeline writes →
#   A2 backfill dryRun + execute + replay.
# Not a gate: this is the rerunnable操作实录 behind docs/dev/dogfooding-2026-10-28.md.
# Every step prints a pass/fail line plus wall time, and the observed server
# versions are printed up front (the standing container is NAMED doris-2.1 but
# runs whatever image it runs — friction #1 of the report).
#
# 用法：
#   bash scripts/dogfood/round1.sh [--help] [--keep]
#
# 对象：在役 Doris（AGENTS 环境契约里的常驻栈：FE 容器 doris-2.1 → 127.0.0.1:9030/8030，
#   BE 容器 doris-be → 127.0.0.1:8040）。只建/删专用库 $DOGFOOD_DB，不触碰其余库，
#   不动容器（无 compose up/down）。
#
# 覆盖变量：
#   DOGFOOD_DORIS_MYSQL   mysql 客户端命令前缀   默认 "docker exec -i doris-2.1 mysql"
#   DOGFOOD_FE_PORT       FE mysql 宿主端口      默认 9030
#   DOGFOOD_BE_PORT       BE webserver 宿主端口  默认 8040
#   DOGFOOD_DB            专用库                 默认 tailrace_dogfood
#   DOGFOOD_CONSOLE_ADDR  console 监听           默认 127.0.0.1:18087
#   DOGFOOD_NODE_ADDR     node 监听              默认 127.0.0.1:18107
#   DOGFOOD_WORK          工作目录               默认 /tmp/tailrace-dogfood
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN     二进制（默认 target/release/…）
#
# live-gate：Docker 不可达 / FE 端口不通 / 磁盘 <5GiB → [skip] exit 0。
# 产物：$DOGFOOD_WORK/summary.txt（每步结果与耗时，供报告引用）；--keep 保留工作目录
#   （失败必留）。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
DORIS_MYSQL="${DOGFOOD_DORIS_MYSQL:-docker exec -i doris-2.1 mysql}"
FE_PORT="${DOGFOOD_FE_PORT:-9030}"
BE_PORT="${DOGFOOD_BE_PORT:-8040}"
DB="${DOGFOOD_DB:-tailrace_dogfood}"
CONSOLE_ADDR="${DOGFOOD_CONSOLE_ADDR:-127.0.0.1:18087}"
NODE_ADDR="${DOGFOOD_NODE_ADDR:-127.0.0.1:18107}"
WORK="${DOGFOOD_WORK:-/tmp/tailrace-dogfood}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --help|-h) sed -n '2,/^set -uo pipefail/p' "$0" | sed -e '/^set -uo/d' -e 's/^# \{0,1\}//'; exit 0 ;;
    --keep) KEEP=1 ;;
    *) echo "unknown flag: $arg (see --help)" >&2; exit 2 ;;
  esac
done

PASS=0; FAIL=0; FAILED_STEPS=()
CONSOLE_PID=""; NODE_PID=""
T0=$(date +%s); TSTEP=$T0
SUMMARY=""
step() { # step <pass|fail> <name> [detail] — prints and records wall time since the previous step
  local now; now=$(date +%s); local dt=$((now - TSTEP)); TSTEP=$now
  if [ "$1" = "pass" ]; then
    PASS=$((PASS+1)); printf '  ok    %-4ss  %s\n' "$dt" "$2"; SUMMARY+="ok   ${dt}s  $2"$'\n'
  else
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$2"); printf '  FAIL  %-4ss  %s  %s\n' "$dt" "$2" "${3:-}"; SUMMARY+="FAIL ${dt}s  $2  ${3:-}"$'\n'
  fi
}
cleanup() {
  [ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
  [ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
  wait 2>/dev/null
  [ -d "$WORK" ] && printf '%s' "$SUMMARY" > "$WORK/summary.txt"
  [ "$FAIL" -gt 0 ] && KEEP=1
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

m() { $DORIS_MYSQL -N -h127.0.0.1 -P9030 -uroot -e "$1" 2>/dev/null; }
q() { m "$1" | tr -d '[:space:]'; }
wait_scalar() { local r n; for r in $(seq 1 45); do n="$(q "$1")"; [ "${n:-}" = "$2" ] && { echo "$n"; return 0; }; sleep 2; done; echo "${n:-}"; }
req() { # req <METHOD> <path> [json] [token] → CODE/BODY
  local args=(-s -X "$1" -o "$WORK/body" -w '%{http_code}' "http://$CONSOLE_ADDR$2")
  [ -n "${3:-}" ] && args+=(-H 'Content-Type: application/json' -d "$3")
  [ -n "${4:-}" ] && args+=(-H "Authorization: Bearer $4")
  CODE="$(curl "${args[@]}")"; BODY="$(cat "$WORK/body" 2>/dev/null)"
}
wait_http() { local i; for ((i=0; i<$2; i++)); do [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ] && return 0; sleep 1; done; return 1; }
json_sql() { printf '%s' "$1" | jq -Rs .; }
wait_job() { # wait_job <pid> <job> <tries> → JOB_STATE/JOB_ROWS/JOB_ERROR
  local i row; JOB_STATE=""; JOB_ROWS=""; JOB_ERROR=""
  for ((i=0; i<$3; i++)); do
    req GET "/api/pipelines/$1/backfill/jobs" "" "$TOKEN"
    row="$(jq -r --arg j "$2" '[.jobs[] | select(.jobId==$j)][0] | "\(.state)\t\(.rowsWritten // 0)\t\(.error // "")"' <<<"$BODY" 2>/dev/null)"
    IFS=$'\t' read -r JOB_STATE JOB_ROWS JOB_ERROR <<<"$row"
    { [ "$JOB_STATE" = "done" ] || [ "$JOB_STATE" = "failed" ]; } && return 0
    sleep 2
  done
  return 1
}

echo "== dogfooding round 1: standing doris fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT db=$DB console=$CONSOLE_ADDR work=$WORK =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }
if ! docker info >/dev/null 2>&1; then echo "[skip] docker daemon unreachable"; exit 0; fi
AVAIL_GIB="$(df -g / | awk 'NR==2 {print $4}')"
if [ "${AVAIL_GIB:-99}" -lt 5 ]; then echo "[skip] host free disk ${AVAIL_GIB}GiB < 5GiB (OrbStack kill-line)"; exit 0; fi
if ! nc -z 127.0.0.1 "$FE_PORT" 2>/dev/null; then echo "[skip] standing doris FE 127.0.0.1:$FE_PORT unreachable"; exit 0; fi
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"

# ---------- D0 what are we actually talking to ----------
FE_VER="$(m "SHOW FRONTENDS;" | awk -F'\t' 'NR==1{for(i=1;i<=NF;i++) if($i ~ /^doris-/) print $i}')"
BE_ALIVE="$(m "SHOW BACKENDS;" | awk -F'\t' '$10=="true"{n++} END{print n+0}')"
BE_VER="$(m "SHOW BACKENDS;" | awk -F'\t' 'NR==1{for(i=1;i<=NF;i++) if($i ~ /^doris-/) print $i}')"
FE_CONTAINER="$(docker ps --format '{{.Names}} {{.Image}}' | grep -E 'doris-2.1|doris-fe' | head -1)"
[ -n "$FE_VER" ] && [ "${BE_ALIVE:-0}" -ge 1 ] \
  && step pass "D0.1 standing doris reachable: FE ${FE_VER} / BE ${BE_VER} alive=$BE_ALIVE (container: ${FE_CONTAINER:-?})" \
  || step fail "D0.1 standing doris" "fe_ver=$FE_VER be_alive=$BE_ALIVE"
[ -x "$CONSOLE_BIN" ] && [ -x "$NODE_BIN" ] && step pass "D0.2 release binaries present" \
  || step fail "D0.2 binaries" "run ./scripts/build.sh"
m "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB;" >/dev/null 2>&1
[ -z "$(q "SHOW TABLES FROM $DB;")" ] && step pass "D0.3 dedicated database $DB empty (DROP FORCE + CREATE; nothing else on the standing stack touched)" \
  || step fail "D0.3 dedicated db" "not empty / create failed"

# ---------- D1 console + auth ----------
TAILRACE_BOOTSTRAP_TOKEN="tr_b_dogfood_00112233445566778899aabbccddeeff" \
  "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
wait_http "http://$CONSOLE_ADDR/healthz" 20 && step pass "D1.1 console booted (fresh data-dir)" \
  || step fail "D1.1 console boot" "$(tail -3 "$WORK/console.log")"
# the one-time admin password exists only in the boot log (friction: no --print-password / no API)
ADMIN_PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "D1.2 login with the log-scraped initial password → token" \
  || step fail "D1.2 login" "http=$CODE body=$BODY"

# ---------- D2 datasources + table object ----------
req POST /api/datasources \
  "{\"name\":\"df-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST /api/datasources "{\"name\":\"df-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ -n "$DDS" ] && [ -n "$FDS" ]; } && step pass "D2.1 datasources: doris(id=$DDS, jdbc+streamLoad, root) + file(id=$FDS, path=$WORK/node)" \
  || step fail "D2.1 datasources" "dds=$DDS fds=$FDS body=$BODY"
COLS='[["order_no","utf8"],["qty","int64"],["note","utf8"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$DB\",\"tableName\":\"df_orders\",\"format\":null,\"columnsJson\":$COLS}" "$TOKEN"
{ [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } && step pass "D2.2 sink table object $DB.df_orders registered (3 columns, no CDC marker)" \
  || step fail "D2.2 table object" "http=$CODE body=$BODY"

# ---------- D3 A1 four-step: draft → confirm → 422 gate → execute → live check ----------
req POST "/api/datasources/$DDS/tables/$DB/df_orders/ddl/draft" "" "$TOKEN"
KM="$(jq -r '.keyModel // empty' <<<"$BODY")"; GUESS="$(jq -r '.keyColumnGuessed' <<<"$BODY")"
DRAFT1_REPL="$(grep -o '"replication_num" = "[0-9]*"' <<<"$(jq -r '.ddl // empty' <<<"$BODY")")"
{ [ "$CODE" = "200" ] && [ "$KM" = "duplicate" ] && [ "$GUESS" = "true" ]; } \
  && step pass "D3.1 draft round-1 (no body): recommends $KM, key column guessed=true, default ${DRAFT1_REPL:-replication_num absent} (the caller must probe SHOW BACKENDS itself: alive=$BE_ALIVE)" \
  || step fail "D3.1 draft round-1" "http=$CODE km=$KM guess=$GUESS body=$BODY"
REPL=$(( BE_ALIVE < 3 ? BE_ALIVE : 3 ))
req POST "/api/datasources/$DDS/tables/$DB/df_orders/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"order_no\",\"replicationNum\":$REPL}" "$TOKEN"
DDL="$(jq -r '.ddl // empty' <<<"$BODY")"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.keyColumnGuessed' <<<"$BODY")" = "false" ] && grep -q "\"replication_num\" = \"$REPL\"" <<<"$DDL"; } \
  && step pass "D3.2 draft round-2 (confirmed duplicate/order_no, replicationNum=$REPL) → guessed=false, DDL carries replication_num=$REPL" \
  || step fail "D3.2 draft round-2" "http=$CODE ddl=$(head -c 160 <<<"$DDL")"
req POST "/api/datasources/$DDS/tables/$DB/df_orders/ddl/execute" "{\"ddl\":$(json_sql "$DDL")}" "$TOKEN"
{ [ "$CODE" = "422" ] && [ "$(jq -r '.error.code // empty' <<<"$BODY")" = "KEY_MODEL_REQUIRED" ]; } \
  && step pass "D3.3 execute without keyModel → 422 KEY_MODEL_REQUIRED (gate holds, nothing ran)" \
  || step fail "D3.3 422 gate" "http=$CODE body=$BODY"
req POST "/api/datasources/$DDS/tables/$DB/df_orders/ddl/execute" "{\"ddl\":$(json_sql "$DDL"),\"keyModel\":\"duplicate\"}" "$TOKEN"
EXEC_MS="$(jq -r '.elapsedMs // empty' <<<"$BODY")"
SHOW="$(m "SHOW CREATE TABLE $DB.df_orders;" | tr '[:upper:]' '[:lower:]')"
{ [ "$CODE" = "200" ] && grep -q 'duplicate key(`order_no`)' <<<"$SHOW"; } \
  && step pass "D3.4 execute → ok (Doris elapsed ${EXEC_MS}ms); live SHOW CREATE TABLE = DUPLICATE KEY(order_no) — zero hand-written DDL" \
  || step fail "D3.4 execute" "http=$CODE body=$BODY show=$(head -c 160 <<<"$SHOW")"
req GET "/api/datasources/tables?datasourceId=$DDS" "" "$TOKEN"
[ "$(jq -r '.items[0].keyModel // empty' <<<"$BODY")" = "duplicate" ] \
  && step pass "D3.5 key model recorded on the table object by execute (keyModel=duplicate in the list)" \
  || step fail "D3.5 key model record" "body=$BODY"

# ---------- D4 file source + pipeline ----------
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"df_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
RUN_ID="$(date +%s)"
for i in $(seq 0 9); do printf '{"order_no":"DF-%s-%03d","qty":%d,"note":"batch1"}\n' "$RUN_ID" "$i" "$i"; done > "$WORK/node/df_in"
SIZE1="$(wc -c < "$WORK/node/df_in" | tr -d ' ')"
step pass "D4.1 file table local.df_in + batch1 seeded (10 rows, run-id $RUN_ID, $SIZE1 bytes = replay range end)"
( cd "$WORK/node" && exec env "TAILRACE_DS_$DDS=Basic cm9vdDo=" \
    "$NODE_BIN" serve --addr "$NODE_ADDR" --control-plane "http://$CONSOLE_ADDR" --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" --heartbeat-secs 2 \
      --bootstrap-token "tr_b_dogfood_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 && step pass "D4.2 engine node up (serve; sink credential via TAILRACE_DS_$DDS env)" \
  || step fail "D4.2 node boot" "$(tail -3 "$WORK/node.log")"
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break; sleep 1
done
[ "$ONLINE" = "1" ] && step pass "D4.3 node self-registered → ONLINE" || step fail "D4.3 node ONLINE" "$(tail -5 "$WORK/node.log")"
SQL="INSERT INTO $DB.df_orders SELECT order_no, qty, note FROM local.df_in"
req POST /api/pipelines "{\"name\":\"dogfood-main\",\"sqlContent\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$FDS,\"$DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
PID="$(jq -r '.id // empty' <<<"$BODY")"
req POST "/api/pipelines/$PID/publish" "" "$TOKEN"; PUB="$CODE"
req POST "/api/pipelines/$PID/deploy" '{}' "$TOKEN"; DEP="$CODE"
req GET "/api/pipelines/$PID" "" "$TOKEN"
{ [ -n "$PID" ] && [ "$PUB" = "200" ] && [ "$DEP" = "200" ] && [ "$(jq -r '.status' <<<"$BODY")" = "RUNNING" ]; } \
  && step pass "D4.4 pipeline#$PID create→publish→deploy → RUNNING" \
  || step fail "D4.4 pipeline" "id=$PID pub=$PUB dep=$DEP body=$BODY"
N="$(wait_scalar "SELECT COUNT(*) FROM $DB.df_orders;" 10)"
[ "${N:-x}" = "10" ] && step pass "D4.5 batch1 landed: $DB.df_orders COUNT(*)=10" \
  || step fail "D4.5 batch1 rows" "n=$N node.log=$(tail -5 "$WORK/node.log")"

# ---------- D5 A2 backfill: dryRun → execute → replay ----------
bf() { req POST "/api/pipelines/$PID/backfill" "{\"statementIndex\":0,\"from\":{\"type\":\"offset\",\"value\":$1},\"to\":{\"type\":\"offset\",\"value\":$2},\"dryRun\":${3:-false}}" "$TOKEN"; }
bf 0 "$SIZE1" true
LP="$(jq -r '.labelPrefix // empty' <<<"$BODY")"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.jobId // "null"' <<<"$BODY")" = "null" ] && [ -n "$LP" ] && [ "$(q "SELECT COUNT(*) FROM $DB.df_orders;")" = "10" ]; } \
  && step pass "D5.1 dryRun [0,$SIZE1) → 200 jobId=null labelPrefix=$LP, table untouched (10)" \
  || step fail "D5.1 dryRun" "http=$CODE body=$BODY"
bf 0 "$SIZE1"
J1="$(jq -r '.jobId // empty' <<<"$BODY")"
wait_job "$PID" "$J1" 60
R1="$(wait_scalar "SELECT COUNT(*) FROM $DB.df_orders;" 20)"
{ [ "$JOB_STATE" = "done" ] && [ "${JOB_ROWS:-0}" -gt 0 ] && [ "${R1:-x}" = "20" ]; } \
  && step pass "D5.2 backfill#1 job=$J1 done rowsWritten=$JOB_ROWS → table 10→20 (replay adds under deterministic labels; DUPLICATE makes it visible)" \
  || step fail "D5.2 backfill#1" "state=$JOB_STATE rows=$JOB_ROWS err=$JOB_ERROR table=$R1"
bf 0 "$SIZE1"
J2="$(jq -r '.jobId // empty' <<<"$BODY")"
wait_job "$PID" "$J2" 60
R2="$(q "SELECT COUNT(*) FROM $DB.df_orders;")"
{ [ "$JOB_STATE" = "done" ] && [ "${R2:-x}" = "20" ]; } \
  && step pass "D5.3 backfill#2 same range job=$J2 $JOB_STATE (rowsWritten=$JOB_ROWS ack-side) → table still 20: label dedup held, no double write" \
  || step fail "D5.3 replay idempotency" "state=$JOB_STATE rows=$JOB_ROWS table=$R2 (expected 20)"
req GET "/api/pipelines/$PID" "" "$TOKEN"
[ "$(jq -r '.status' <<<"$BODY")" = "RUNNING" ] && step pass "D5.4 main pipeline still RUNNING after both backfills" \
  || step fail "D5.4 main pipeline" "status=$(jq -r '.status' <<<"$BODY")"

# ---------- teardown (dedicated db only) ----------
[ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
[ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
NODE_PID=""; CONSOLE_PID=""
m "DROP DATABASE IF EXISTS $DB FORCE;" >/dev/null 2>&1 && step pass "D6.1 DROP DATABASE $DB FORCE (standing stack untouched otherwise)" \
  || step fail "D6.1 cleanup" "drop failed"

TOTAL=$((PASS+FAIL)); ELAPSED=$(( $(date +%s) - T0 ))
echo "== dogfooding round 1 summary: $PASS/$TOTAL steps passed in ${ELAPSED}s =="
SUMMARY+="total ${ELAPSED}s  $PASS/$TOTAL"$'\n'
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"; exit 1
fi
exit 0
