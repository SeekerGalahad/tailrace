#!/usr/bin/env bash
# tailrace end-to-end smoke (design doc §十 P1 gate — local subset).
#
# Default run: no external dependencies — console + engine node + a file→file
# pipeline (jsonl in/out) on the loopback interface:
#   console boot → /api/auth/me 401 → login → me 200 → file datasource +
#   table objects → node serve --control-plane self-register → node ONLINE →
#   pipeline create → compile 0 diagnostics → publish → deploy → RUNNING →
#   stats totals grow → pause → stop.
#
# --full: additionally `docker compose up` Kafka+Doris (docker-compose.yml)
# and run the kafka→doris path, then the poison-data soak (T11): poison rows
# (in-field newlines/quotes/emoji/CJK) consumed continuously from kafka for
# ≥10 minutes (SMOKE_FULL_SOAK_SECS, default 600) with the pipeline kept
# RUNNING and rows conserved end to end. Requires Docker; that path is not
# part of the dependency-free gate.
#
# Artifacts: binaries from target/release/ (run ./scripts/build.sh first).
# Work dir: /tmp/tailrace-smoke (kept on failure for post-mortem).
set -uo pipefail
cd "$(dirname "$0")/.."

ROOT="$(pwd)"
CONSOLE_ADDR="${SMOKE_CONSOLE_ADDR:-127.0.0.1:18080}"
NODE_ADDR="${SMOKE_NODE_ADDR:-127.0.0.1:18100}"
BOOTSTRAP_TOKEN="${TAILRACE_BOOTSTRAP_TOKEN:-tr_b_smoke_00112233445566778899aabbccddeeff}"
WORK="${SMOKE_DIR:-/tmp/tailrace-smoke}"
KEEP="${SMOKE_KEEP:-0}"
FULL=0
[ "${1:-}" = "--full" ] && FULL=1

PASS=0; FAIL=0
CONSOLE_PID=""; NODE_PID=""
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
  wait 2>/dev/null
  [ "$FAIL" -gt 0 ] && KEEP=1
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

# HTTP helper → sets BODY (response body) and CODE (http status).
req() { # req <METHOD> <path> [json-body] [auth-token]
  local args=(-s -X "$1" -o "$WORK/body" -w '%{http_code}' "http://$CONSOLE_ADDR$2")
  [ -n "${3:-}" ] && args+=(-H 'Content-Type: application/json' -d "$3")
  [ -n "${4:-}" ] && args+=(-H "Authorization: Bearer $4")
  CODE="$(curl "${args[@]}")"
  BODY="$(cat "$WORK/body" 2>/dev/null)"
}

wait_http() { # wait_http <url> <tries> — until HTTP 200
  local i
  for ((i=0; i<$2; i++)); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ] && return 0
    sleep 1
  done
  return 1
}

wait_totals() { # wait_totals <pipeline-id> <min> <tries> — sets CONSUMED/FLUSHED
  local i
  CONSUMED=0; FLUSHED=0
  for ((i=0; i<$3; i++)); do
    req GET "/api/pipelines/$1/stats" "" "$TOKEN"
    CONSUMED="$(jq -r '.stats.totals.consumed // 0' <<<"$BODY" 2>/dev/null)"
    FLUSHED="$(jq -r '.stats.totals.flushed // 0' <<<"$BODY" 2>/dev/null)"
    { [ "${CONSUMED:-0}" -ge "$2" ] && [ "${FLUSHED:-0}" -ge "$2" ]; } && return 0
    sleep 1
  done
  return 1
}

append_rows() { # append_rows <start-index> <count> → jsonl lines into orders_in
  local i
  for ((i=$1; i<$1+$2; i++)); do printf '{"order_no":"O-%03d","qty":%d}\n' "$i" "$i"; done >> "$WORK/node/orders_in"
}

json_sql() { # embed SQL (single line) into a JSON string literal
  printf '%s' "$1" | jq -Rs .
}

echo "== tailrace smoke: work=$WORK console=$CONSOLE_ADDR node=$NODE_ADDR full=$FULL =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }

# ---------- S0 prerequisites ----------
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"
[ -x "$ROOT/target/release/tailrace-console" ] \
  && step pass "S0.1 target/release/tailrace-console binary" \
  || step fail "S0.1" "missing target/release/tailrace-console (run ./scripts/build.sh)"
[ -x "$ROOT/target/release/tailrace-node" ] \
  && step pass "S0.2 target/release/tailrace-node binary" \
  || step fail "S0.2" "missing target/release/tailrace-node"

# ---------- S1 console boot ----------
TAILRACE_BOOTSTRAP_TOKEN="$BOOTSTRAP_TOKEN" "$ROOT/target/release/tailrace-console" \
  --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
if wait_http "http://$CONSOLE_ADDR/healthz" 20; then
  step pass "S1.1 console booted (/healthz 200)"
else
  step fail "S1.1 console boot" "$(tail -3 "$WORK/console.log")"
fi
# sed, not `cut -d'：'`: GNU cut rejects the multi-byte delimiter.
ADMIN_PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
[ -n "$ADMIN_PW" ] && step pass "S1.2 admin seed password captured from first-boot log" \
  || step fail "S1.2" "no 初始密码 line in console.log"

# ---------- S2 auth ----------
req GET /api/auth/me
[ "$CODE" = "401" ] && step pass "S2.1 /api/auth/me unauthenticated → 401" \
  || step fail "S2.1" "http=$CODE body=$BODY"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "S2.2 login → 200 + token" \
  || step fail "S2.2" "http=$CODE body=$BODY"
req GET /api/auth/me "" "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.user.username // empty' <<<"$BODY")" = "admin" ]; } \
  && step pass "S2.3 /api/auth/me with session → 200 admin" \
  || step fail "S2.3" "http=$CODE body=$BODY"

# ---------- S3 file datasource + table objects ----------
req POST /api/datasources "{\"name\":\"smoke-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
DS_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$DS_ID" ]; } && step pass "S3.1 file datasource created (id=$DS_ID)" \
  || step fail "S3.1" "http=$CODE body=$BODY"

COLS='[["order_no","utf8"],["qty","int64"]]'
req POST "/api/datasources/$DS_ID/tables" \
  "{\"dbName\":\"local\",\"tableName\":\"orders_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
{ [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } && step pass "S3.2 table object orders_in (file/jsonl, FILE_POINTER)" \
  || step fail "S3.2" "http=$CODE body=$BODY"
req POST "/api/datasources/$DS_ID/tables" \
  "{\"dbName\":\"local\",\"tableName\":\"orders_out\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
{ [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } && step pass "S3.3 table object orders_out (file/jsonl)" \
  || step fail "S3.3" "http=$CODE body=$BODY"

# seed input rows (engine cwd = $WORK/node → bare file names resolve)
append_rows 1 3

# ---------- S3.9 (--full only) kafka+doris datasources BEFORE the node ----------
# The engine reads sink credentials from its own process env at deploy time
# (authEnv = TAILRACE_DS_<id>); the doris datasource must therefore exist
# (and its id be known) before the node is spawned.
KDS=""; DDS=""; DDS_ENV_NAME=""; DDS_ENV_VALUE="Basic cm9vdDo="   # root:
if [ "$FULL" = "1" ]; then
  KPORT="${TAILRACE_KAFKA_PORT:-9092}"
  FEPORT="${TAILRACE_FE_MYSQL_PORT:-39030}"
  BEPORT="${TAILRACE_BE_HTTP_PORT:-38040}"
  req POST /api/datasources "{\"name\":\"smoke-kafka\",\"type\":\"kafka\",\"connJson\":{\"bootstrap\":\"127.0.0.1:$KPORT\"}}" "$TOKEN"
  KDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
  # doris datasource contract (console api/datasources.rs): jdbcUrl AND
  # streamLoadUrl are both required; username seals a credential (authEnv
  # gate) — root has an empty password on a fresh container
  req POST /api/datasources "{\"name\":\"smoke-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FEPORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BEPORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
  DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
  { [ -n "$KDS" ] && [ -n "$DDS" ]; } && step pass "S3.9 (--full) kafka+doris datasources pre-created (k=$KDS d=$DDS)" \
    || step fail "S3.9" "kds=$KDS dds=$DDS body=$BODY"
  DDS_ENV_NAME="TAILRACE_DS_$DDS"
fi

# ---------- S4 engine node self-registration ----------
if [ -n "$DDS_ENV_NAME" ]; then
  ( cd "$WORK/node" && exec env "$DDS_ENV_NAME=$DDS_ENV_VALUE" \
      "$ROOT/target/release/tailrace-node" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "$BOOTSTRAP_TOKEN" ) >"$WORK/node.log" 2>&1 &
else
  ( cd "$WORK/node" && exec "$ROOT/target/release/tailrace-node" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "$BOOTSTRAP_TOKEN" ) >"$WORK/node.log" 2>&1 &
fi
NODE_PID=$!
if wait_http "http://$NODE_ADDR/healthz" 20; then
  step pass "S4.1 engine node up (/healthz)"
else
  step fail "S4.1 engine node boot" "$(tail -3 "$WORK/node.log")"
fi
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "S4.2 node self-registered → ONLINE in console" \
  || step fail "S4.2 node ONLINE" "nodes=$BODY; node.log=$(tail -5 "$WORK/node.log")"

# ---------- S5 pipeline: compile → publish → deploy ----------
SQL="INSERT INTO local.orders_out SELECT order_no, qty FROM local.orders_in"
req POST /api/pipelines "{\"name\":\"smoke-file-copy\",\"sqlContent\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$DS_ID},\"checkpointMs\":2000}" "$TOKEN"
P_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
[ -n "$P_ID" ] && step pass "S5.1 pipeline created (id=$P_ID)" \
  || step fail "S5.1" "http=$CODE body=$BODY"

req POST /api/pipelines/compile "{\"sql\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$DS_ID}}" "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.ok' <<<"$BODY")" = "true" ] && [ "$(jq -r '.diagnostics | length' <<<"$BODY")" = "0" ]; } \
  && step pass "S5.2 compile: ok=true, 0 diagnostics" \
  || step fail "S5.2" "http=$CODE body=$BODY"

req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.version' <<<"$BODY")" = "1" ]; } \
  && step pass "S5.3 publish v1 (spec snapshot materialized)" \
  || step fail "S5.3" "http=$CODE body=$BODY"

req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.ok' <<<"$BODY")" = "true" ]; } \
  && step pass "S5.4 deploy (plan push) accepted by engine" \
  || step fail "S5.4" "http=$CODE body=$BODY"

req GET "/api/pipelines/$P_ID" "" "$TOKEN"
[ "$(jq -r '.status' <<<"$BODY")" = "RUNNING" ] && step pass "S5.5 pipeline RUNNING" \
  || step fail "S5.5" "body=$BODY"

# ---------- S6 stats totals growth ----------
if wait_totals "$P_ID" 3 20; then step pass "S6.1 stats totals ≥ 3 (seed batch, consumed=$CONSUMED flushed=$FLUSHED)"; \
  else step fail "S6.1" "consumed=$CONSUMED flushed=$FLUSHED last=$BODY"; fi

append_rows 4 5
if wait_totals "$P_ID" 8 25; then step pass "S6.2 stats totals grew to ≥ 8 after +5 append (consumed=$CONSUMED)"; \
  else step fail "S6.2" "consumed=$CONSUMED flushed=$FLUSHED"; fi
OUT_LINES="$( [ -f "$WORK/node/orders_out" ] && wc -l < "$WORK/node/orders_out" | tr -d ' ' || echo 0 )"
[ "${OUT_LINES:-0}" -ge 8 ] && step pass "S6.3 sink file orders_out has ≥ 8 jsonl lines ($OUT_LINES)" \
  || step fail "S6.3" "orders_out lines=$OUT_LINES"

append_rows 9 4
if wait_totals "$P_ID" 12 25; then step pass "S6.4 stats totals grew to ≥ 12 after +4 more (consumed=$CONSUMED)"; \
  else step fail "S6.4" "consumed=$CONSUMED flushed=$FLUSHED"; fi

# ---------- S7 pause → stop ----------
req POST "/api/pipelines/$P_ID/pause" "" "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.status' <<<"$BODY")" = "PAUSED" ]; } \
  && step pass "S7.1 pause → engine verdict PAUSED" \
  || step fail "S7.1" "http=$CODE body=$BODY"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
[ "$(jq -r '.status' <<<"$BODY")" = "PAUSED" ] && step pass "S7.2 console pipeline status PAUSED" \
  || step fail "S7.2" "body=$BODY"

req POST "/api/pipelines/$P_ID/stop" "" "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.status' <<<"$BODY")" = "STOPPED" ]; } \
  && step pass "S7.3 stop → engine verdict STOPPED" \
  || step fail "S7.3" "http=$CODE body=$BODY"
req GET "/api/pipelines/$P_ID" "" "$TOKEN"
[ "$(jq -r '.status' <<<"$BODY")" = "PUBLISHED" ] && step pass "S7.4 console pipeline back to PUBLISHED" \
  || step fail "S7.4" "body=$BODY"

# ---------- S8 (only with --full) kafka→doris via docker compose ----------
# --full path: Kafka (ogg envelope) → Doris (duplicate-key table). Requires
# Docker + docker-compose.yml services; NOT part of the dependency-free gate.
run_full() {
  local COMPOSE="docker compose -f $ROOT/docker-compose.yml"
  # host-port overrides (must match docker-compose.yml defaults; defaults
  # sit on 39030/38040 — the standard Doris ports 9030/8040 are taken by
  # this shared dev machine's standing mdop `doris-2.1` container)
  local KPORT="${TAILRACE_KAFKA_PORT:-9092}"
  local FEPORT="${TAILRACE_FE_MYSQL_PORT:-39030}"
  local BEPORT="${TAILRACE_BE_HTTP_PORT:-38040}"
  echo "  (--full) starting docker compose kafka+doris (first pull may take minutes; kafka=127.0.0.1:$KPORT doris-be=127.0.0.1:$BEPORT)"
  if $COMPOSE up -d --wait; then
    step pass "S8.1 docker compose kafka+doris healthy"
  else
    # detached `up` swallows container stdout — and the apache/doris
    # entrypoint launders every start_be.sh failure (swap / max_map_count
    # / nofile checks) into a clean container exit(0), so without this
    # dump the failure is undiagnosable (W3-ci 2026-10-17: BE
    # "exited (0)" with zero output — real cause: the runner host's
    # default swap, T11 finding F11)
    $COMPOSE logs --tail 50 doris-be doris-fe kafka || true
    step fail "S8.1 docker compose up" "$COMPOSE up --wait failed"
    $COMPOSE down -v   # L3 discipline: teardown regardless of outcome
    return
  fi
  if $COMPOSE exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic smoke_orders_ogg; then
    step pass "S8.2 kafka topic smoke_orders_ogg"
  else
    step fail "S8.2 kafka topic" "create failed"
  fi
  # DDL retry: the FE may still be finishing bootstrap right after --wait
  doris_exec() { # doris_exec <retries> <sql...>
    local r
    for r in $(seq 1 "${1:-6}"); do
      $COMPOSE exec -T doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "${2}" && return 0
      sleep 5
    done
    return 1
  }
  # mysql -e can print a statement error yet exit 0 (T11 first-run: CREATE
  # TABLE failed with "available backend num is 0" while the step logged
  # ok) — DDL success is asserted on SHOW TABLES, not the exit code alone
  doris_wait_table() { # doris_wait_table <db> <table> <retries>
    local r out
    for r in $(seq 1 "${3:-6}"); do
      out="$($COMPOSE exec -T doris-fe mysql -N -h127.0.0.1 -P9030 -uroot -e "SHOW TABLES FROM $1 LIKE '$2';" 2>/dev/null)"
      grep -q "$2" <<<"$out" && return 0
      sleep 5
    done
    return 1
  }
  if doris_exec 6 "CREATE DATABASE IF NOT EXISTS shop; CREATE TABLE IF NOT EXISTS shop.orders (order_no VARCHAR(64) NOT NULL, qty INT NOT NULL, etl_ts BIGINT) DUPLICATE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES('replication_num' = '1');" \
     && doris_wait_table shop orders 6; then
    step pass "S8.3 doris table shop.orders"
  else
    step fail "S8.3 doris ddl" "create failed"
  fi

  { [ -n "$KDS" ] && [ -n "$DDS" ]; } && step pass "S8.4 kafka+doris datasources (k=$KDS d=$DDS, node env $DDS_ENV_NAME injected at S4)" \
    || step fail "S8.4" "kds=$KDS dds=$DDS (S3.9 pre-creation failed)"

  OGG_COLS='[["op_type","utf8"],["ogg_table","utf8"],["ogg_ts","int64"],["order_no","utf8"],["qty","int64"]]'
  DORIS_COLS='[["order_no","utf8"],["qty","int64"],["etl_ts","int64"]]'
  # SQL dialect: two-part <dsMapping-key>.<table> where the key names the
  # real database on the sink side (console compile.rs substitutes two-part
  # chains; the publish gate matches the table object's dbName to that key)
  req POST "/api/datasources/$KDS/tables" "{\"dbName\":\"kafka\",\"tableName\":\"smoke_orders_ogg\",\"format\":\"ogg\",\"columnsJson\":$OGG_COLS}" "$TOKEN"
  KC="$CODE"
  req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"shop\",\"tableName\":\"orders\",\"format\":null,\"columnsJson\":$DORIS_COLS}" "$TOKEN"
  { [ "$KC" = "201" ] || [ "$KC" = "200" ]; } && { [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } \
    && step pass "S8.5 kafka source + doris sink table objects" \
    || step fail "S8.5" "kafka tables http=$KC doris tables http=$CODE"

  FSQL="INSERT INTO shop.orders SELECT order_no, qty, ogg_ts AS etl_ts FROM kafka.smoke_orders_ogg WHERE op_type IN ('I','U')"
  req POST /api/pipelines "{\"name\":\"smoke-kafka-doris\",\"sqlContent\":$(json_sql "$FSQL"),\"dsMapping\":{\"kafka\":$KDS,\"shop\":$DDS},\"checkpointMs\":3000}" "$TOKEN"
  FP_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
  req POST "/api/pipelines/$FP_ID/publish" "" "$TOKEN"
  PC="$CODE"
  req POST "/api/pipelines/$FP_ID/deploy" '{}' "$TOKEN"
  { [ "$PC" = "200" ] && [ "$(jq -r '.ok' <<<"$BODY")" = "true" ]; } \
    && step pass "S8.6 kafka→doris pipeline published + deployed" \
    || step fail "S8.6" "publish http=$PC deploy http=$CODE body=$BODY"

  for i in $(seq 1 10); do
    printf '{"table":"SHOPDB.ORDERS","op_type":"I","ts":%d,"after":{"order_no":"K-%03d","qty":%d}}\n' "$i" "$i" "$i"
  done | $COMPOSE exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic smoke_orders_ogg
  step pass "S8.7 produced 10 ogg envelope events"

  wait_totals "$FP_ID" 10 30
  [ "${FLUSHED:-0}" -ge 10 ] && step pass "S8.8 kafka→doris stats flushed ≥ 10 (flushed=$FLUSHED)" \
    || step fail "S8.8" "flushed=$FLUSHED last=$BODY"

  DROWS="$($COMPOSE exec -T doris-fe \
    mysql -N -h127.0.0.1 -P9030 -uroot -e 'SELECT COUNT(*) FROM shop.orders;' 2>/dev/null | tr -d '[:space:]')"
  [ "${DROWS:-0}" -ge 10 ] && step pass "S8.9 doris shop.orders rows ≥ 10 ($DROWS)" \
    || step fail "S8.9" "rows=$DROWS"

  # ---------- S9 (only with --full) poison-data soak (T11) ----------
  # Poison rows (in-field newlines, doubled quotes, commas, emoji, CJK) flow
  # kafka→decode→transform→Doris CSV stream load continuously for the whole
  # soak; the pipeline must stay RUNNING and rows must be conserved exactly
  # (produced == consumed == flushed == doris rows). Roadmap T4/T11: the CSV
  # kafka-sink carrier used to bail on such rows and kill the pipeline.
  local SOAK_SECS="${SMOKE_FULL_SOAK_SECS:-600}"
  local BATCH="${SMOKE_FULL_SOAK_BATCH:-50}"
  local INTERVAL="${SMOKE_FULL_SOAK_INTERVAL:-15}"
  echo "  (--full) S9 poison soak: ${SOAK_SECS}s, batch=$BATCH every ${INTERVAL}s"
  if $COMPOSE exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic smoke_poison_ogg; then
    step pass "S9.1 kafka topic smoke_poison_ogg"
  else
    step fail "S9.1 kafka topic" "create failed"
  fi
  if doris_exec 6 "CREATE TABLE IF NOT EXISTS shop.poison_orders (order_no VARCHAR(64) NOT NULL, note VARCHAR(2048), qty INT, etl_ts BIGINT) DUPLICATE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES('replication_num' = '1');" \
     && doris_wait_table shop poison_orders 6; then
    step pass "S9.2 doris table shop.poison_orders"
  else
    step fail "S9.2 doris ddl" "create failed"
  fi

  POISON_SRC_COLS='[["op_type","utf8"],["ogg_table","utf8"],["ogg_ts","int64"],["order_no","utf8"],["note","utf8"],["qty","int64"]]'
  POISON_SNK_COLS='[["order_no","utf8"],["note","utf8"],["qty","int64"],["etl_ts","int64"]]'
  req POST "/api/datasources/$KDS/tables" "{\"dbName\":\"kafka\",\"tableName\":\"smoke_poison_ogg\",\"format\":\"ogg\",\"columnsJson\":$POISON_SRC_COLS}" "$TOKEN"
  PC1="$CODE"
  req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"shop\",\"tableName\":\"poison_orders\",\"format\":null,\"columnsJson\":$POISON_SNK_COLS}" "$TOKEN"
  { [ "$PC1" = "201" ] || [ "$PC1" = "200" ]; } && { [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } \
    && step pass "S9.3 poison kafka source + doris sink table objects" \
    || step fail "S9.3" "kafka http=$PC1 doris http=$CODE"

  PSQL="INSERT INTO shop.poison_orders SELECT order_no, note, qty, ogg_ts AS etl_ts FROM kafka.smoke_poison_ogg WHERE op_type IN ('I','U')"
  req POST /api/pipelines "{\"name\":\"smoke-poison-soak\",\"sqlContent\":$(json_sql "$PSQL"),\"dsMapping\":{\"kafka\":$KDS,\"shop\":$DDS},\"checkpointMs\":3000}" "$TOKEN"
  PP_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
  req POST "/api/pipelines/$PP_ID/publish" "" "$TOKEN"
  PC2="$CODE"
  req POST "/api/pipelines/$PP_ID/deploy" '{}' "$TOKEN"
  { [ "$PC2" = "200" ] && [ "$(jq -r '.ok' <<<"$BODY")" = "true" ]; } \
    && step pass "S9.4 poison pipeline published + deployed (id=$PP_ID)" \
    || step fail "S9.4" "publish http=$PC2 deploy http=$CODE body=$BODY"

  local produced=0 soak_running=1
  local soak_start=$(( $(date +%s) ))
  while [ $(( $(date +%s) - soak_start )) -lt "$SOAK_SECS" ]; do
    # one batch of poison rows — the JSON envelope itself is single-line per
    # message (jq escapes the in-field newline), the *field value* carries
    # newline/quotes/emoji into decode→CSV stream load
    for r in $(seq 1 "$BATCH"); do
      jq -cn --arg on "$(printf 'P-%06d' $((produced + r)))" \
         --arg note "line1
line2 \"quoted, comma\" 🎉🚀 中文毒数据 <tag>&tailrace" \
         '{table:"SHOPDB.ORDERS",op_type:"I",ts:1760000000,after:{order_no:$on,qty:7,note:$note}}'
    done | $COMPOSE exec -T kafka \
        /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic smoke_poison_ogg
    produced=$((produced + BATCH))
    req GET "/api/pipelines/$PP_ID" "" "$TOKEN"
    if [ "$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)" != "RUNNING" ]; then
      soak_running=0
      step fail "S9.5 pipeline RUNNING throughout the soak" "status left RUNNING at produced=$produced: $BODY"
      break
    fi
    sleep "$INTERVAL"
  done
  [ "$soak_running" = "1" ] \
    && step pass "S9.5 pipeline RUNNING throughout the ${SOAK_SECS}s soak (checked every batch)" \
    || true

  if wait_totals "$PP_ID" "$produced" 120; then
    step pass "S9.6 stats drained: consumed=$CONSUMED flushed=$FLUSHED (produced=$produced)"
  else
    step fail "S9.6 stats drained" "consumed=$CONSUMED flushed=$FLUSHED produced=$produced last=$BODY"
  fi
  [ "${CONSUMED:-0}" = "$produced" ] && [ "${FLUSHED:-0}" = "$produced" ] \
    && step pass "S9.7 row conservation: consumed == flushed == produced == $produced" \
    || step fail "S9.7 row conservation" "consumed=${CONSUMED:-0} flushed=${FLUSHED:-0} produced=$produced"

  local DROWS2="" i2
  for i2 in $(seq 1 30); do
    DROWS2="$($COMPOSE exec -T doris-fe \
      mysql -N -h127.0.0.1 -P9030 -uroot -e 'SELECT COUNT(*) FROM shop.poison_orders;' 2>/dev/null | tr -d '[:space:]')"
    [ "${DROWS2:-0}" = "$produced" ] && break
    sleep 2
  done
  [ "${DROWS2:-0}" = "$produced" ] \
    && step pass "S9.8 doris row count conserved ($DROWS2 == $produced)" \
    || step fail "S9.8 doris row count" "rows=${DROWS2:-0} produced=$produced"

  NOTE_V="$($COMPOSE exec -T doris-fe \
    mysql -N -h127.0.0.1 -P9030 -uroot -e "SELECT note FROM shop.poison_orders WHERE order_no='P-000001';" 2>/dev/null)"
  if grep -q 'line2' <<<"$NOTE_V" && grep -q '🎉' <<<"$NOTE_V" && grep -q '中文毒数据' <<<"$NOTE_V"; then
    step pass "S9.9 poison field value intact in doris (newline+quotes+emoji+CJK roundtrip)"
  else
    step fail "S9.9 poison value integrity" "note=$(head -c 120 <<<"$NOTE_V")"
  fi

  # memory budget evidence for the CI compose stack (roadmap T11: 超时与内存
  # 预算先行验证) — peak-ish snapshot right after the soak
  docker stats --no-stream --format '  mem {{.Name}} {{.MemUsage}} ({{.MemPerc}})' \
    tailrace-kafka tailrace-doris-fe tailrace-doris-be 2>/dev/null | tee -a "$WORK/compose-stats.txt" || true

  $COMPOSE down -v
}

[ "$FULL" = "1" ] && run_full

TOTAL=$((PASS+FAIL))
echo "== smoke summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
exit 0
