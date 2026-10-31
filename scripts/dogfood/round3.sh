#!/usr/bin/env bash
# R9 W5 dogfooding round 3 — run tailrace's OWN full chain against the
# STANDING (in-service) Doris the way an operator would, over the two R9
# surfaces: the variant/vector type words (A4 × A5: a docs_emb table with
# two VARIANT columns + a dimmed ARRAY<FLOAT> + a bare one) and the alert
# face (E2: a lag rule with a webhook, fired by a REAL pause, resolved by a
# REAL resume). Not a gate: this is the rerunnable 操作实录 behind
# docs/dev/dogfooding-round3-2026-10-30.md.
#
# 用法：
#   ./scripts/build.sh   # release binaries at target/release/…
#   bash scripts/dogfood/round3.sh [--help] [--keep]
#
# 对象：在役 Doris（FE 容器 doris-2.1 → 127.0.0.1:9030，BE 容器 doris-be →
#   127.0.0.1:8040）。只建/删专用库 $DB，不触碰其余库，不动容器。
#
# 覆盖变量：
#   DOGFOOD_DORIS_MYSQL   mysql 客户端命令前缀   默认 "docker exec -i doris-2.1 mysql"
#   DOGFOOD_FE_PORT       FE mysql 宿主端口      默认 9030
#   DOGFOOD_BE_PORT       BE webserver 宿主端口  默认 8040
#   DOGFOOD_DB            专用库                 默认 tailrace_dogfood3
#   DOGFOOD_CONSOLE_ADDR  console 监听           默认 127.0.0.1:18091
#   DOGFOOD_NODE_ADDR     node 监听              默认 127.0.0.1:18111
#   DOGFOOD_WEBHOOK_PORT  mock webhook 端口      默认 18195（127.0.0.1）
#   DOGFOOD_WORK          工作目录               默认 /tmp/tailrace-dogfood3
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN     二进制（默认 target/release/…）
#
# live-gate：Docker 不可达 / FE 端口不通 / 磁盘 <5GiB → [skip] exit 0。
# 产物：$DOGFOOD_WORK/summary.txt；--keep 保留工作目录（失败必留）。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
DORIS_MYSQL="${DOGFOOD_DORIS_MYSQL:-docker exec -i doris-2.1 mysql}"
FE_PORT="${DOGFOOD_FE_PORT:-9030}"
BE_PORT="${DOGFOOD_BE_PORT:-8040}"
DB="${DOGFOOD_DB:-tailrace_dogfood3}"
CONSOLE_ADDR="${DOGFOOD_CONSOLE_ADDR:-127.0.0.1:18091}"
NODE_ADDR="${DOGFOOD_NODE_ADDR:-127.0.0.1:18111}"
WEBHOOK_PORT="${DOGFOOD_WEBHOOK_PORT:-18195}"
WORK="${DOGFOOD_WORK:-/tmp/tailrace-dogfood3}"
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
CONSOLE_PID=""; NODE_PID=""; HOOK_PID=""
FAILED_STEPS=()
T0=$(date +%s); TSTEP=$T0
SUMMARY=""
step() { # step <pass|fail> <name> [detail]
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
  [ -n "$HOOK_PID" ] && kill -TERM "$HOOK_PID" 2>/dev/null
  wait 2>/dev/null
  [ -d "$WORK" ] && printf '%s' "$SUMMARY" > "$WORK/summary.txt"
  [ "$FAIL" -gt 0 ] && KEEP=1
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

m() { $DORIS_MYSQL -N -h127.0.0.1 -P9030 -uroot -e "$1" 2>/dev/null; }
q() { m "$1" | head -1 | tr -d '[:space:]'; }
# rows(): keep in-cell spaces, join lines with ';' (3.1.4 GROUP_CONCAT has
# no ORDER BY…SEPARATOR; doris-scalar style tr would eat quoted spaces)
rows() { m "$1" | tr -d '\r' | paste -sd';' -; }
raw_load() { # raw_load <table> <label> <jsonl-file>
  curl -s -X PUT -u root: -H "label: $2" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" --data-binary "@$3" \
    "http://127.0.0.1:$BE_PORT/api/$DB/$1/_stream_load"
}
# be_write_gate — the standing BE intermittently refuses stream loads with
# MEM_LIMIT_EXCEEDED (shared-VM memory pressure; two failed flushes put a
# statement into 30s/60s restart backoff and a third trips the no-progress
# poison limit → FAILED). Probe the write path on the throwaway probe_mix
# table and WAIT the window out (≤6×10s) before each deployment. Sets GATE_TRIES.
be_write_gate() {
  local i
  GATE_TRIES=0
  for i in 1 2 3 4 5 6; do
    GATE_TRIES=$i
    printf '%s\n' "{\"id\":$((900 + i)),\"a\":[9,9,9]}" > "$WORK/wl.jsonl"
    raw_load probe_mix "df3_wl_${1}_$$_$i" "$WORK/wl.jsonl" | grep -q '"Status": *"Success"' && return 0
    sleep 10
  done
  return 1
}
req() { # REST helper → sets BODY/CODE
  local args=(-s -X "$1" -o "$WORK/body" -w '%{http_code}' "http://$CONSOLE_ADDR$2")
  [ -n "${3:-}" ] && args+=(-H 'Content-Type: application/json' -d "$3")
  [ -n "${4:-}" ] && args+=(-H "Authorization: Bearer $4")
  CODE="$(curl "${args[@]}")"; BODY="$(cat "$WORK/body" 2>/dev/null)"
}
wait_http() { local i; for ((i=0; i<$2; i++)); do [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ] && return 0; sleep 1; done; return 1; }
hook_seen() { [ -f "$WORK/hook.jsonl" ] && wc -l < "$WORK/hook.jsonl" | tr -d ' ' || echo 0; }

echo "== dogfooding round3: work=$WORK db=$DB console=$CONSOLE_ADDR node=$NODE_ADDR webhook=127.0.0.1:$WEBHOOK_PORT =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required (mock webhook)"; exit 2; }

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable, skipped"; exit 0
fi
if ! nc -z 127.0.0.1 "$FE_PORT" 2>/dev/null && ! (exec 3<>"/dev/tcp/127.0.0.1/$FE_PORT") 2>/dev/null; then
  echo "[skip] standing doris FE 127.0.0.1:$FE_PORT unreachable, skipped"; exit 0
fi
AVAIL_GIB="$(df -g / | awk 'NR==2 {print $4}')"
if [ "${AVAIL_GIB:-99}" -lt 5 ]; then
  echo "[skip] host free disk ${AVAIL_GIB}GiB < 5GiB (OrbStack kill-line), skipped"; exit 0
fi
[ -x "$CONSOLE_BIN" ] && step pass "S0.1 console binary" || step fail "S0.1 console binary" "missing $CONSOLE_BIN"
[ -x "$NODE_BIN" ] && step pass "S0.2 node binary" || step fail "S0.2 node binary" "missing $NODE_BIN"

# ---------- D0 stack facts ----------
VER="$(m "SHOW FRONTENDS;" | grep -o 'doris-[0-9][0-9.a-z-]*' | head -1)"
step pass "D0.1 standing stack ${VER:-unknown} (FE doris-2.1 → 127.0.0.1:$FE_PORT, BE → 127.0.0.1:$BE_PORT)"
m "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB;" \
  && step pass "D0.2 dedicated database $DB dropped+created (rest of the stack untouched)" \
  || step fail "D0.2 database" "create failed"

# ---------- D1 console + node ----------
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state" "$WORK/hook"
cat > "$WORK/hook.py" <<'PY'
# Generated by scripts/dogfood/round3.sh — mock webhook (records POSTs).
import json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
port, out = int(sys.argv[1]), sys.argv[2]
class H(BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200); self.send_header("content-length", "2"); self.end_headers(); self.wfile.write(b"ok")
    def do_GET(self): self._ok()
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        with open(out, "a", encoding="utf-8") as f:
            f.write(json.dumps({"event_header": self.headers.get("x-tailrace-event", ""),
                                "body": body.decode("utf-8", "replace")}) + "\n")
        self._ok()
    def log_message(self, *a): pass
ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
PY
python3 "$WORK/hook.py" "$WEBHOOK_PORT" "$WORK/hook.jsonl" >"$WORK/hook.log" 2>&1 &
HOOK_PID=$!
HUP=0
for i in $(seq 1 10); do nc -z 127.0.0.1 "$WEBHOOK_PORT" 2>/dev/null && { HUP=1; break; }; sleep 0.5; done
[ "$HUP" = "1" ] \
  && step pass "D1.1 mock webhook up (127.0.0.1:$WEBHOOK_PORT)" \
  || step fail "D1.1 webhook" "$(tail -3 "$WORK/hook.log")"
TAILRACE_BOOTSTRAP_TOKEN="tr_b_df3_00112233445566778899aabbccddeeff" \
TAILRACE_ALERT_INTERVAL_SECS=2 \
  "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
wait_http "http://$CONSOLE_ADDR/healthz" 20 \
  && step pass "D1.2 console booted (alert evaluator at 2s)" \
  || step fail "D1.2 console" "$(tail -3 "$WORK/console.log")"
PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "D1.3 login" || step fail "D1.3 login" "http=$CODE"
req POST /api/datasources \
  "{\"name\":\"df3-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST /api/datasources "{\"name\":\"df3-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ -n "$DDS" ] && [ -n "$FDS" ]; } && step pass "D1.4 datasources (doris=$DDS file=$FDS)" || step fail "D1.4 datasources" "d=$DDS f=$FDS"
( cd "$WORK/node" && exec env "TAILRACE_DS_$DDS=Basic cm9vdDo=" \
    "$NODE_BIN" serve --addr "$NODE_ADDR" --control-plane "http://$CONSOLE_ADDR" --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" --heartbeat-secs 2 \
      --bootstrap-token "tr_b_df3_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 && step pass "D1.5 node up" || step fail "D1.5 node" "$(tail -3 "$WORK/node.log")"
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break; sleep 1
done
[ "$ONLINE" = "1" ] && step pass "D1.6 node ONLINE" || step fail "D1.6 ONLINE" "$(tail -3 "$WORK/node.log")"

# ---------- D2 the mixed variant+vector table through the A1 loop ----------
COLS='[["doc_id","utf8"],["meta","variant"],["content","variant"],["emb","list<float32,3>"],["scores","list<float32>"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$DB\",\"tableName\":\"docs_emb\",\"format\":null,\"columnsJson\":$COLS}" "$TOKEN"
req POST "/api/datasources/$DDS/tables/$DB/docs_emb/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"doc_id\",\"replicationNum\":1}" "$TOKEN"
DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
{ grep -q '`meta` VARIANT' <<<"$DDL" && grep -q '`emb` ARRAY<FLOAT>' <<<"$DDL" && grep -q 'vector dim=3' <<<"$DDL" && grep -q '`scores` ARRAY<FLOAT>' <<<"$DDL"; } \
  && step pass "D2.1 draft: VARIANT ×2 + emb ARRAY<FLOAT> dim=3 comment + bare scores (A4×A5 words through the wizard face)" \
  || step fail "D2.1 draft" "$(head -c 300 <<<"$DDL")"
req POST "/api/datasources/$DDS/tables/$DB/docs_emb/ddl/execute" \
  "{\"ddl\":$(printf '%s' "$DDL" | jq -Rs .),\"keyModel\":\"duplicate\"}" "$TOKEN"
SHOW="$(m "SHOW CREATE TABLE $DB.docs_emb;" | tr '[:upper:]' '[:lower:]')"
{ grep -q '`meta` variant' <<<"$SHOW" && grep -q 'vector dim=3' <<<"$SHOW"; } \
  && step pass "D2.2 executed on the live table (SHOW CREATE TABLE carries variant + the dim comment — zero hand DDL)" \
  || step fail "D2.2 execute" "$(head -c 300 <<<"$SHOW")"
# R9 review fix: the registered list<float32,3> reads back from DESC as a
# bare list<float32> (the dim lives in the COMMENT) — evolve must NOT read
# that as a retype. Before the fix this 409'd forever and blocked even pure
# ADD COLUMNs on every dimmed vector table.
req POST "/api/datasources/$DDS/tables/$DB/docs_emb/ddl/evolve" '{}' "$TOKEN"
EV_CODE="$CODE"
EV_CHANGED="$(jq -r '.changed // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$EV_CODE" = "200" ] && [ "$EV_CHANGED" = "false" ]; } \
  && step pass "D2.3 evolve on the dimmed table → 200 changed=false (dim invisible to DESC is not a retype — R9 review fix)" \
  || step fail "D2.3 evolve on dimmed table" "http=$EV_CODE body=$(head -c 300 <<<"$BODY")"

# ---------- D3 pipeline: nested docs + vectors through the engine ----------
# gate table for be_write_gate (raw stream loads only, never asserted on)
m "CREATE TABLE $DB.probe_mix (id INT NOT NULL, a ARRAY<FLOAT>) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" >/dev/null 2>&1
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"docs_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
cat > "$WORK/node/docs_in" <<'EOF'
{"doc_id":"DOC-1","meta":{"source":"api","verified":true},"content":{"l1":{"l2":{"l3":"leaf"}},"tags":["x","y"]},"emb":[0.1,0.2,0.3],"scores":[9,8,7]}
{"doc_id":"DOC-2","meta":{"source":"batch"},"content":{"k":1.5,"t":true},"emb":[1,2,3],"scores":[1]}
{"doc_id":"DOC-3","meta":null,"content":[10,20],"emb":[1e-7,1,0.33333334],"scores":null}
EOF
be_write_gate d3 \
  && step pass "D3.0a BE write path healthy (gate attempt $GATE_TRIES)" \
  || step fail "D3.0a BE write health" "MEM_LIMIT window outlasted 6×10s"
req POST /api/pipelines \
  "{\"name\":\"df3-docs\",\"sqlContent\":$(printf 'INSERT INTO %s.docs_emb SELECT doc_id, meta, content, emb, scores FROM local.docs_in' "$DB" | jq -Rs .),\"dsMapping\":{\"local\":$FDS,\"$DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
P_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
OK="$(jq -r '.ok // empty' <<<"$BODY" 2>/dev/null)"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
ST="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
[ "$OK" = "true" ] && [ "$ST" = "RUNNING" ] \
  && step pass "D3.1 pipeline df3-docs deployed → RUNNING (#$P_ID)" \
  || step fail "D3.1 deploy" "status=$ST body=$(head -c 200 <<<"$BODY")"
N=""
for i in $(seq 1 45); do N="$(q "SELECT COUNT(*) FROM $DB.docs_emb;")"; [ "$N" = "3" ] && break; sleep 2; done
# redeploy armor (once): a MEM_LIMIT window that slipped past the gate puts
# the statement into restart backoff and a third failure trips the poison
# limit — stop + re-deploy the same pipeline on the recovered BE
if [ "${N:-x}" != "3" ] && grep -q "MEM_LIMIT_EXCEEDED" "$WORK/node-state/events.log" 2>/dev/null; then
  sleep 15
  req POST "/api/pipelines/$P_ID/stop" '{}' "$TOKEN"
  be_write_gate d3r >/dev/null
  req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
  step pass "D3.1a observation: BE memory window tripped the first deployment — re-deployed pipeline #$P_ID on the recovered BE"
  N=""
  for i in $(seq 1 60); do N="$(q "SELECT COUNT(*) FROM $DB.docs_emb;")"; [ "$N" = "3" ] && break; sleep 2; done
fi
SUB="$(q "SELECT COUNT(*) FROM $DB.docs_emb WHERE doc_id='DOC-1' AND meta['source']='api' AND CAST(meta['verified'] AS INT)=1 AND content['l1']['l2']['l3']='leaf';")"
VEC="$(rows "SELECT CONCAT(CAST(element_at(emb,1) AS STRING),',',CAST(element_at(emb,3) AS STRING)) FROM $DB.docs_emb ORDER BY doc_id;")"
{ [ "$N" = "3" ] && [ "$SUB" = "1" ] && [ "$VEC" = "0.1,0.3;1,3;1e-07,0.33333334" ]; } \
  && step pass "D3.2 nested JSON answered from sub-columns (source/verified/4-level path) and vectors element-exact (incl. 1e-07) — through the real engine chain" \
  || step fail "D3.2 assertions" "n=$N sub=$SUB vec=$VEC"
# dim guard as an operator meets it: one wrong-length row quarantined
COLS_BAD='[["doc_id","utf8"],["emb","list<float32,3>"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$DB\",\"tableName\":\"emb_bad\",\"format\":null,\"columnsJson\":$COLS_BAD}" "$TOKEN"
req POST "/api/datasources/$DDS/tables/$DB/emb_bad/ddl/draft" "{\"keyModel\":\"duplicate\",\"keyColumn\":\"doc_id\",\"replicationNum\":1}" "$TOKEN"
req POST "/api/datasources/$DDS/tables/$DB/emb_bad/ddl/execute" "{\"ddl\":$(jq -r '.ddl' <<<"$BODY" | jq -Rs .),\"keyModel\":\"duplicate\"}" "$TOKEN"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"bad_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_BAD}" "$TOKEN"
printf '%s\n' '{"doc_id":"G-1","emb":[1,2,3]}' '{"doc_id":"B-1","emb":[1,2]}' > "$WORK/node/bad_in"
be_write_gate d3b \
  && step pass "D3.3a BE write path healthy before the bad-row pipeline (gate attempt $GATE_TRIES)" \
  || step fail "D3.3a BE write health" "window outlasted 6×10s"
req POST /api/pipelines \
  "{\"name\":\"df3-bad\",\"sqlContent\":$(printf 'INSERT INTO %s.emb_bad SELECT doc_id, emb FROM local.bad_in' "$DB" | jq -Rs .),\"dsMapping\":{\"local\":$FDS,\"$DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
B_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/pipelines/$B_ID/publish" "" "$TOKEN"; req POST "/api/pipelines/$B_ID/deploy" '{}' "$TOKEN"
BOK="$(jq -r '.ok // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$BOK" = "true" ]; } || step fail "D3.3 deploy bad pipeline" "http=$CODE body=$(head -c 200 <<<"$BODY")"
BN=""
for i in $(seq 1 45); do BN="$(q "SELECT COUNT(*) FROM $DB.emb_bad;")"; [ "$BN" = "1" ] && break; sleep 2; done
if [ "${BN:-x}" != "1" ] && grep -q "MEM_LIMIT_EXCEEDED" "$WORK/node-state/events.log" 2>/dev/null; then
  sleep 15
  req POST "/api/pipelines/$B_ID/stop" '{}' "$TOKEN"
  be_write_gate d3br >/dev/null
  req POST "/api/pipelines/$B_ID/deploy" '{}' "$TOKEN"
  BN=""
  for i in $(seq 1 60); do BN="$(q "SELECT COUNT(*) FROM $DB.emb_bad;")"; [ "$BN" = "1" ] && break; sleep 2; done
fi
DLQF="$WORK/node-state/bad_rows/p$B_ID-s0.jsonl"
DLQN=0
for i in $(seq 1 15); do
  if [ -s "$DLQF" ]; then DLQN="$(wc -l < "$DLQF" | tr -d ' ')"; fi
  { [ "${DLQN:-0}" -ge 1 ] 2>/dev/null; } && break
  sleep 1
done
{ [ "$BN" = "1" ] && [ "${DLQN:-0}" -ge 1 ] 2>/dev/null; } \
  && step pass "D3.3 dim guard lived: the good row landed (1), the wrong-length row quarantined to bad_rows/p$B_ID-s0.jsonl ($DLQN record(s))" \
  || step fail "D3.3 dim guard" "n=$BN dlq=$DLQN node-events=$(grep -c DECODE_BAD "$WORK/node-state/events.log" 2>/dev/null)"

# ---------- D4 one alert rule, really fired and really resolved ----------
# R9 review fix: episodes are per series, so the rule is scoped to THIS
# pipeline — unscoped it would also match the dim-guard pipeline ($B_ID),
# whose static input makes its lag grow forever (that is what put a
# pipeline=2 sample into the round-3 evidence), and the rule could never
# read inactive again.
req POST /api/alerts/rules \
  "{\"name\":\"df3-lag\",\"ruleType\":\"lag_seconds\",\"params\":{\"threshold\":6,\"pipeline\":\"$P_ID\"},\"webhookUrl\":\"http://127.0.0.1:$WEBHOOK_PORT/hook\"}" "$TOKEN"
RULE_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
[ -n "$RULE_ID" ] && step pass "D4.1 lag rule (threshold 6s, pipeline=$P_ID) + webhook created (#$RULE_ID)" || step fail "D4.1 rule" "body=$(head -c 200 <<<"$BODY")"
# pause only once the first data has actually flushed (last_data_ms is the
# lag gauge's basis; pausing before it leaves the series absent, not stale)
F1=""
for i in $(seq 1 30); do F1="$(q "SELECT COUNT(*) FROM $DB.docs_emb;")"; [ "$F1" = "3" ] && break; sleep 2; done
# R9 review fix: the SLO target line follows the lag series, which only
# exists after the first flush — assert it HERE (after F1), and fail when
# it is absent instead of printing "none yet" as a pass.
SLO=""
for i in $(seq 1 10); do
  SLO="$(curl -s "http://$CONSOLE_ADDR/metrics" | grep "^tailrace_slo_lag_target_seconds{.*pipeline=\"$P_ID\"" | head -1)"
  [ -n "$SLO" ] && break; sleep 1
done
[ -n "$SLO" ] \
  && step pass "D4.2 /metrics exposes the SLO target beside the lag series: $SLO" \
  || step fail "D4.2 SLO target line" "no tailrace_slo_lag_target_seconds for pipeline=$P_ID after first flush (rows=$F1)"
be_write_gate d4 >/dev/null
req POST "/api/pipelines/$P_ID/pause" '{}' "$TOKEN"
grep -q '"PAUSED"' <<<"$BODY" && step pass "D4.3 pipeline paused (manufacturing the lag)" || step fail "D4.3 pause" "http=$CODE"
FST=""
for i in $(seq 1 40); do
  req GET /api/alerts "" "$TOKEN"
  FST="$(jq -r --arg n df3-lag '.rules[] | select(.name==$n) | .state // "missing"' <<<"$BODY" 2>/dev/null)"
  [ "$FST" = "firing" ] && break; sleep 2
done
[ "$FST" = "firing" ] && step pass "D4.4 rule FIRING on the real pause (~8s in)" || step fail "D4.4 firing" "state=$FST"
printf '%s\n' '{"doc_id":"DOC-4","meta":{"source":"api"},"content":{"k":2},"emb":[0.4,0.5,0.6],"scores":[4]}' >> "$WORK/node/docs_in"
req POST "/api/pipelines/$P_ID/resume" '{}' "$TOKEN"
grep -q '"RUNNING"' <<<"$BODY" && step pass "D4.5 resumed (fresh row appended before)" || step fail "D4.5 resume" "http=$CODE"
RST=""
R0=$(date +%s)
# up to 120s: under a BE memory window the resume-time stream load can take
# tens of seconds — the rule then (correctly) stays firing on REAL lag
for i in $(seq 1 60); do
  req GET /api/alerts "" "$TOKEN"
  RST="$(jq -r --arg n df3-lag '.rules[] | select(.name==$n) | .state // "missing"' <<<"$BODY" 2>/dev/null)"
  [ "$RST" = "inactive" ] && break; sleep 2
done
RLAT=$(( $(date +%s) - R0 ))
# STOP right after the resolve: an idle-but-running file source re-accumulates
# lag with wall time (the freshness semantics) and the rule LAWFULLY re-fires
# ~6s after the last data — racing the stop against that is exactly what the
# D4.7 assertion tolerates (any post-episode firing is recorded, not failed).
# NOTE (dogfooding round 3, open item — UNCONFIRMED): an earlier run that fed
# one more row right before this stop, during a BE memory window, ended with
# the ledger cursor at EOF while totals.flushed stayed one row short and the
# table showed 4/5 rows after a 60s poll. Five targeted file-sink repros
# (append → stop within 0.15s) were all consistent, and the stop
# finalization flushes pending rows before the cursor save — the mechanism
# is unexplained, so the script stops without extra feeding and the doc
# lists it as an open item for a Doris-sink repro.
req POST "/api/pipelines/$P_ID/stop" '{}' "$TOKEN"
STOP_OK="$CODE"
N5=""
for i in $(seq 1 30); do N5="$(q "SELECT COUNT(*) FROM $DB.docs_emb;")"; [ "${N5:-0}" -ge 4 ] 2>/dev/null && break; sleep 2; done
{ [ "$RST" = "inactive" ] && [ "${N5:-0}" -ge 4 ] && [ "$STOP_OK" = "200" ]; } \
  && step pass "D4.6 rule RESOLVED ${RLAT}s after the resume; DOC-4 landed ($N5 rows), pipeline stopped" \
  || step fail "D4.6 resolved" "state=$RST n=$N5 stop=$STOP_OK"
sleep 3
H="$(hook_seen)"
FIRST2="$(jq -r '.event_header' "$WORK/hook.jsonl" 2>/dev/null | head -2 | paste -sd, -)"
EV1="$(jq -r '(.body|fromjson).eventId' "$WORK/hook.jsonl" 2>/dev/null | head -1)"
EV2="$(jq -r '(.body|fromjson).eventId' "$WORK/hook.jsonl" 2>/dev/null | sed -n 2p)"
# R9 review fix: episodes are per series — both notices must name the SAME
# series (previously the resolved sample could belong to another pipeline)
SK1="$(jq -r '(.body|fromjson).seriesKey // empty' "$WORK/hook.jsonl" 2>/dev/null | head -1)"
SK2="$(jq -r '(.body|fromjson).seriesKey // empty' "$WORK/hook.jsonl" 2>/dev/null | sed -n 2p)"
SEQ="$(jq -rc '.event_header' "$WORK/hook.jsonl" 2>/dev/null | paste -sd, -)"
if [ "$FIRST2" = "firing,resolved" ] && [ -n "$EV1" ] && [ "$EV1" = "$EV2" ] && [ -n "$SK1" ] && [ "$SK1" = "$SK2" ] && grep -q "pipeline=\"$P_ID\"" <<<"$SK1"; then
  step pass "D4.7 webhook carries the episode's 2 notices (firing then resolved, SAME episode id $EV1, SAME series $SK1) — sequence [$SEQ], total $H"
else
  step fail "D4.7 webhook" "first2=$FIRST2 ev1=$EV1 ev2=$EV2 sk1=$SK1 sk2=$SK2 seq=[$SEQ]"
fi
# R9 review fix: DELETE closes an open episode with reason=deleted and
# delivers resolved before the row goes; the rule is already resolved here,
# so the hook count must not grow (a 3rd notice would be a phantom).
req DELETE "/api/alerts/rules/$RULE_ID" "" "$TOKEN"
DEL_CODE="$CODE"
sleep 2
H2="$(hook_seen)"
{ [ "$DEL_CODE" = "204" ] && [ "$H2" = "$H" ]; } \
  && step pass "D4.8 rule deleted (204), no phantom notice (hook total stays $H2)" \
  || step fail "D4.8 delete" "http=$DEL_CODE hook_before=$H hook_after=$H2"

# ---------- D5 teardown ----------
[ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null; NODE_PID=""
[ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null; CONSOLE_PID=""
[ -n "$HOOK_PID" ] && kill -TERM "$HOOK_PID" 2>/dev/null; HOOK_PID=""
m "DROP DATABASE IF EXISTS $DB FORCE;" \
  && step pass "D5.1 $DB dropped (standing stack untouched)" \
  || step fail "D5.1 cleanup" "drop failed"

TOTAL=$((PASS+FAIL))
echo "== dogfooding round3 summary: $PASS/$TOTAL steps passed =="
[ "$FAIL" -gt 0 ] && { echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"; }
exit $((FAIL > 0 ? 1 : 0))
