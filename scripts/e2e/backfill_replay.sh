#!/usr/bin/env bash
# R6 T-A2 backfill replay e2e (blueprint §7.3 / §7.7 A2 row) — the live proof
# of the replay-trust story on a DUPLICATE KEY table (a UNIQUE KEY table would
# fold duplicate rows at the model layer and MASK a broken label — the
# blueprint explicitly forbids that masking; every assertion below is only
# honest because DUPLICATE makes a double write visible as +N rows).
#
# 用法：
#   bash scripts/e2e/backfill_replay.sh [--help] [--keep] [--skip-up] [--wait SECS]
#
# 编排与断言（数字链，batch1=10 行 / batch2=10 行 / batch3=10 行，order_no 内
#   嵌 run-id 使每次执行的源文件字节尺寸唯一——deterministic label 的
#   fingerprint = sha256(from,to)[..8]，文件尺寸变 → 区间位点变 → label 家族
#   跨执行不撞，重放语义每次都是冷的）：
#     T1  建表走 A1 闭环（draft 确认 → execute keyModel=duplicate）——回填
#         的信任链从「表是生成的 DUPLICATE KEY 表」开始；
#     T2  主管道（file→doris，node serve 常驻）消费 batch1 → 表 10 行；
#     T3  dryRun（from=0,to=batch1 末尾字节位点）→ 200 jobId=null（不建
#         job 行）+ labelPrefix=tr-p{id}-s0-bf + 表不增 + jobs 列表空；
#     T4  回填 #1 同区间 → job done → 表 R1=20（主管道 10 行 process-scoped
#         label + 首次重放 10 行 deterministic label——回放不查重历史数据，
#         靠 label 幂等保证重放自身不叠加，这是 A2 的语义边界）；
#     T5  回填 #2 同区间 → 表 R2==R1（核心断言：同区间重放行数不变——
#         label already exists → Dedup；若 label 失效，DUPLICATE 表会 +10
#         行直接暴露）；
#     T6  回填 #3 与主管道写入并行：#3 触发后立即 append batch2，#3 区间
#         封死在 batch1 末尾（run_bounded 到 to 即停，engine lib.rs
#         cursor_reached），全 Dedup 不加行；主管道照常消费 batch2 → 表
#         R1+10；主管道全程 RUNNING（回填期间主管道写入不中断）；
#     T7  失败回填不改主管道游标：ghost sink 表（console 层已注册、Doris
#         层不存在——stream load 404）+ pipeline#2 publish 不 deploy →
#         backfill → job failed；主管道台账 offsets-*.json（node state-dir，
#         FileOffsetStore 每管道一 JSON）sha256 前后不变 + 主管道仍 RUNNING；
#     T8  主管道继续活：append batch3 → 表 R1+20。
#
# 覆盖变量（沿用 docker-compose.yml / smoke.sh 的覆盖模式）：
#   TAILRACE_FE_MYSQL_PORT      FE mysql 宿主端口（DDL/查询）  默认 39030
#   TAILRACE_BE_HTTP_PORT       BE webserver 宿主端口（stream load）默认 38040
#   TAILRACE_E2E_DB             目标库                          默认 tailrace_bf_e2e
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址                默认 127.0.0.1:18083
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址            默认 127.0.0.1:18103
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制路径
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WAIT_SECS      同 --wait（默认 300）
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-bf-e2e
#
# 标志：--help / --keep / --skip-up / --wait SECS（语义同 scripts/e2e/ddl_create.sh）
#
# live-gate（对齐 scripts/e2e/starrocks.sh 惯例）：Docker daemon 不可达、
#   --skip-up 下 FE 端口不通、或宿主可用磁盘 <5GiB 时 [skip] 空过。
#
# 幂等：全新 console data-dir + DROP DATABASE FORCE + run-id 唯一化源文件
#   （label 家族与 Doris 残留 label 不撞）；拆栈外科式（stop+rm 仅 doris-fe/
#   doris-be，严禁整体 compose down 波及本项目常驻 kafka）。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
DORIS_MYSQL="${TAILRACE_E2E_DORIS_MYSQL:-}"
# external standing Doris (smoke.sh SMOKE_DORIS_MYSQL precedent — skip the
# compose bring-up entirely and drive a standing stack, which is faster and
# lighter; ports shift to the standard ones and compose up/teardown become a
# dedicated-database DROP FORCE)
if [ -n "$DORIS_MYSQL" ]; then
  FE_PORT="${TAILRACE_FE_MYSQL_PORT:-9030}"
  BE_PORT="${TAILRACE_BE_HTTP_PORT:-8040}"
else
  FE_PORT="${TAILRACE_FE_MYSQL_PORT:-39030}"
  BE_PORT="${TAILRACE_BE_HTTP_PORT:-38040}"
fi
E2E_DB="${TAILRACE_E2E_DB:-tailrace_bf_e2e}"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18083}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18103}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
WAIT_SECS="${TAILRACE_E2E_WAIT_SECS:-300}"
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-bf-e2e}"
KEEP=0
SKIP_UP=0

for arg in "$@"; do
  case "$arg" in
    --help|-h)
      sed -n '2,/^set -uo pipefail/p' "$0" | sed -e '/^set -uo/d' -e 's/^# \{0,1\}//'
      exit 0
      ;;
    --keep)    KEEP=1 ;;
    --skip-up) SKIP_UP=1 ;;
    --wait)    : ;; # its value arrives as the next token
    *)
      if [ "${PREV_ARG:-}" = "--wait" ]; then
        WAIT_SECS="$arg"
      else
        echo "unknown flag: $arg (see --help)" >&2; exit 2
      fi
      ;;
  esac
  PREV_ARG="$arg"
done

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

# E2E-only compose override, generated into the work dir (docker-compose.yml
# itself stays pristine): BE 2.1.7's start_be.sh hard-refuses to start when
# the Docker VM has ANY active swap (T11 F11). Dev/CI boxes keep swap on for
# good reasons (a shared VM's standing stacks rely on it) and turning it off
# globally is not a test's business — the check is neutralized for THIS stack
# only by raising its threshold past any possible swap-device count before
# the original entrypoint runs. Throwaway test data; the production concern
# behind the check (BE self-managed memory + swap) does not apply here.
OVERRIDE_YML="$WORK/compose-be-swap-override.yml"
doris_compose() { docker compose -f "$ROOT/docker-compose.yml" -f "$OVERRIDE_YML" "$@"; }
# mysql-client bridge: the standing-Doris prefix when TAILRACE_E2E_DORIS_MYSQL
# is set, otherwise the compose doris-fe's in-container client
doris_mysql_exec() { # doris_mysql_exec <mysql client args...>
  if [ -n "$DORIS_MYSQL" ]; then $DORIS_MYSQL "$@"; else doris_compose exec -T doris-fe mysql "$@"; fi
}
doris_exec() { # doris_exec <retries> <sql...> — retry + assert-on-SHOW discipline
  local r
  for r in $(seq 1 "${1:-6}"); do
    doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${2}" && return 0
    sleep 5
  done
  return 1
}
doris_query() { # doris_query <sql> → single scalar
  doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${1}" 2>/dev/null | tr -d '[:space:]'
}
doris_wait_rows() { # doris_wait_rows <table> <expected> — poll COUNT(*) until equal
  local r n
  for r in $(seq 1 45); do
    n="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.$1;")"
    [ "${n:-}" = "$2" ] && { echo "$n"; return 0; }
    sleep 2
  done
  echo "${n:-}"
}
# Wait for a backend Alive=true before any DDL — the compose healthcheck only
# verifies registration, DDL needs heartbeat-driven availability (T11 F6
# family; race first seen live in the ddl_create e2e run). -F'\t': mysql -N's
# LastStartTime contains a space that would shift awk's default FS fields.
doris_wait_be_alive() {
  local r out
  for r in $(seq 1 30); do
    out="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" 2>/dev/null)"
    [ "$(awk -F'\t' '$10=="true"{n++} END{print n+0}' <<<"$out")" -ge 1 ] && return 0
    sleep 2
  done
  return 1
}

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

# trigger a backfill and poll it to a terminal state → sets JOB_STATE/JOB_ROWS/JOB_ERROR
backfill() { # backfill <pipeline-id> <from> <to> [dry]
  local dry="${4:-false}"
  req POST "/api/pipelines/$1/backfill" \
    "{\"statementIndex\":0,\"from\":{\"type\":\"offset\",\"value\":$2},\"to\":{\"type\":\"offset\",\"value\":$3},\"dryRun\":$dry}" "$TOKEN"
}
wait_job() { # wait_job <pipeline-id> <job-id> <tries> → sets JOB_STATE/JOB_ROWS/JOB_ERROR
  local i row
  JOB_STATE=""; JOB_ROWS=""; JOB_ERROR=""
  for ((i=0; i<$3; i++)); do
    req GET "/api/pipelines/$1/backfill/jobs" "" "$TOKEN"
    # tab-joined: the error chain text contains spaces (blank-separated read
    # would drop it — the first live run asserted on an ever-empty JOB_ERROR)
    row="$(jq -r --arg j "$2" \
      '[.jobs[] | select(.jobId==$j)][0] | "\(.state)\t\(.rowsWritten // 0)\t\(.error // "")"' <<<"$BODY" 2>/dev/null)"
    IFS=$'\t' read -r JOB_STATE JOB_ROWS JOB_ERROR <<<"$row"
    { [ "$JOB_STATE" = "done" ] || [ "$JOB_STATE" = "failed" ]; } && return 0
    sleep 2
  done
  return 1
}

echo "== backfill_replay e2e: work=$WORK fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT db=$E2E_DB console=$CONSOLE_ADDR =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable — backfill_replay e2e needs the compose doris stack, skipped"
  exit 0
fi
AVAIL_GIB="$(df -g / | awk 'NR==2 {print $4}')"
if [ "${AVAIL_GIB:-99}" -lt 5 ]; then
  echo "[skip] host free disk ${AVAIL_GIB}GiB < 5GiB (OrbStack kill-line; R6 risk register: defer e2e wave), skipped"
  exit 0
fi
if [ "$SKIP_UP" = "1" ]; then
  if ! nc -z 127.0.0.1 "$FE_PORT" 2>/dev/null \
     && ! (exec 3<>"/dev/tcp/127.0.0.1/$FE_PORT") 2>/dev/null; then
    echo "[skip] doris FE 127.0.0.1:$FE_PORT unreachable (--skip-up expects a running stack; or drop --skip-up to bring it up), skipped"
    exit 0
  fi
fi

[ -x "$CONSOLE_BIN" ] && step pass "S0.1 console binary $CONSOLE_BIN" \
  || step fail "S0.1 console binary" "missing $CONSOLE_BIN (run ./scripts/build.sh)"
[ -x "$NODE_BIN" ] && step pass "S0.2 engine node binary $NODE_BIN" \
  || step fail "S0.2 node binary" "missing $NODE_BIN (run ./scripts/build.sh)"

# ---------- S0.5 work dir + compose override + VM sysctl (compose mode only) ----------
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"
cat > "$OVERRIDE_YML" <<'YAML'
# Generated by scripts/e2e/backfill_replay.sh — e2e-only, never a committed
# default. Neutralizes the BE 2.1.7 start_be.sh unconditional swap refusal for
# this throwaway test stack (threshold raised past any swap-device count),
# then hands control back to the image's original entrypoint unchanged.
services:
  doris-be:
    entrypoint:
      - bash
      - -c
      - |
        sed -i 's/-gt 1 ]]; then/-gt 999999 ]]; then/' /opt/apache-doris/be/bin/start_be.sh
        exec bash entry_point.sh
YAML
step pass "S0.4 compose override generated: BE swap check neutralized for the e2e stack only ($OVERRIDE_YML)"
# BE also hard-requires vm.max_map_count >= 2000000 — an OrbStack/Docker VM
# sysctl that resets on VM restart; raising a kernel limit is harmless, so
# the e2e brings it up itself instead of failing with a laundered exit(0).
if [ -z "$DORIS_MYSQL" ]; then
  CUR_MM="$(docker run --rm --privileged --entrypoint sh apache/doris:be-2.1.7 -c 'cat /proc/sys/vm/max_map_count' 2>/dev/null)"
  if [ "${CUR_MM:-0}" -lt 2000000 ]; then
    docker run --rm --privileged --entrypoint sysctl apache/doris:be-2.1.7 -w vm.max_map_count=2000000 >/dev/null 2>&1 \
      && step pass "S0.5 VM sysctl vm.max_map_count set to 2000000 (BE hard requirement)" \
      || step fail "S0.5 sysctl" "privileged helper could not set vm.max_map_count"
  else
    step pass "S0.5 VM sysctl vm.max_map_count already ${CUR_MM} (BE requirement met)"
  fi
fi

# ---------- S1 doris stack ----------
if [ -n "$DORIS_MYSQL" ]; then
  step pass "S1.1 external standing doris via TAILRACE_E2E_DORIS_MYSQL (compose skipped; fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT)"
elif [ "$SKIP_UP" = "1" ]; then
  step pass "S1.1 doris stack reused (--skip-up; FE 127.0.0.1:$FE_PORT reachable)"
else
  echo "  starting compose doris-fe+doris-be (--wait-timeout ${WAIT_SECS}s)"
  if doris_compose up -d --wait --wait-timeout "$WAIT_SECS" doris-fe doris-be; then
    step pass "S1.1 docker compose doris-fe+doris-be up --wait healthy (kafka not started)"
  else
    doris_compose logs --tail 50 doris-be doris-fe || true
    step fail "S1.1 compose up --wait" "see logs above (image pull / BE swap check)"
  fi
fi

# ---------- S2 empty database ----------
if doris_wait_be_alive; then
  step pass "S2.0 backend Alive=true (heartbeat-driven availability, not just registered)"
else
  step fail "S2.0 backend alive" "no Alive=true backend in SHOW BACKENDS after 30×2s"
fi
if doris_exec 6 "DROP DATABASE IF EXISTS $E2E_DB FORCE; CREATE DATABASE $E2E_DB;" \
   && [ -z "$(doris_query "SHOW TABLES FROM $E2E_DB;")" ]; then
  step pass "S2.1 database $E2E_DB empty (DROP FORCE + CREATE)"
else
  step fail "S2.1 empty database" "create failed"
fi

# ---------- S3 console boot + auth + registrations ----------
# (work dir already created in S0.5, before the stack came up)
# console runs WITH THE NODE'S CWD ($WORK/node): the backfill executor runs
# in-process (backfill.rs) and re-reads the file source table, whose bare
# name "orders_in" resolves against the process cwd — a console started at
# the repo root sees no file, the bounded runner hits its idle/EOF guard
# first and the job "succeeds" with 0 rows (first live run: every backfill
# was rowsWritten=0 and the replay assertions were vacuously green)
( cd "$WORK/node" && exec env TAILRACE_BOOTSTRAP_TOKEN="tr_b_bf_e2e_00112233445566778899aabbccddeeff" \
    "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" ) >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
if wait_http "http://$CONSOLE_ADDR/healthz" 20; then
  step pass "S3.1 console booted (fresh data-dir)"
else
  step fail "S3.1 console boot" "$(tail -3 "$WORK/console.log")"
fi
ADMIN_PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "S3.2 login → 200 + token" \
  || step fail "S3.2 login" "http=$CODE body=$BODY"

req POST /api/datasources \
  "{\"name\":\"bf-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST /api/datasources "{\"name\":\"bf-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$DDS" ] && [ -n "$FDS" ]; } && step pass "S3.3 doris+file datasources (d=$DDS f=$FDS)" \
  || step fail "S3.3 datasources" "dds=$DDS fds=$FDS body=$BODY"

COLS='[["order_no","utf8"],["qty","int64"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders\",\"format\":null,\"columnsJson\":$COLS}" "$TOKEN"
T1="$CODE"
# ghost sink: registered console-side, NEVER created in Doris — the failed-
# backfill fixture (stream load 404 → job failed)
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"ghost\",\"format\":null,\"columnsJson\":$COLS}" "$TOKEN"
T2="$CODE"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"orders_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
T3="$CODE"
{ [ "$T1" = "201" ] || [ "$T1" = "200" ]; } && { [ "$T2" = "201" ] || [ "$T2" = "200" ]; } && { [ "$T3" = "201" ] || [ "$T3" = "200" ]; } \
  && step pass "S3.4 table objects: orders (doris sink) + ghost (doris sink, absent in Doris) + local.orders_in (file)" \
  || step fail "S3.4 table objects" "orders http=$T1 ghost http=$T2 file http=$T3"

# ---------- T1 create the DUPLICATE table through the A1 loop ----------
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
BF_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
KM="$(jq -r '.keyModel // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders/ddl/execute" \
  "{\"ddl\":$(json_sql "$BF_DDL"),\"keyModel\":\"duplicate\"}" "$TOKEN"
EXEC_OK="$CODE"
SHOW_T="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.orders;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
{ [ "$KM" = "duplicate" ] && [ "$EXEC_OK" = "200" ] && grep -q 'duplicate key(`order_no`)' <<<"$SHOW_T"; } \
  && step pass "T1.1 $E2E_DB.orders created via ddl draft→confirm→execute: DUPLICATE KEY(order_no) live" \
  || step fail "T1.1 DUPLICATE table" "draft http km=$KM exec=$EXEC_OK show=$(head -c 160 <<<"$SHOW_T")"

# ---------- T2 main pipeline consumes batch1 ----------
# run-id in every order_no: unique byte size per execution → the replay range
# [0, SIZE1) — and therefore its label fingerprint — never collides with a
# previous run's leftover Doris labels
RUN_ID="$(date +%s)"
for i in $(seq 0 9); do printf '{"order_no":"BF-%s-%03d","qty":%d}\n' "$RUN_ID" "$i" "$i"; done > "$WORK/node/orders_in"
SIZE1="$(wc -c < "$WORK/node/orders_in" | tr -d ' ')"
step pass "T2.1 seed batch1 (10 rows, run-id $RUN_ID, size=$SIZE1 bytes — the replay range end)"

( cd "$WORK/node" && exec env "TAILRACE_DS_$DDS=Basic cm9vdDo=" \
    "$NODE_BIN" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "tr_b_bf_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 \
  && step pass "T2.2 engine node up (serve, resident main pipeline host)" \
  || step fail "T2.2 node boot" "$(tail -3 "$WORK/node.log")"
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "T2.3 node ONLINE" || step fail "T2.3 node ONLINE" "node.log=$(tail -5 "$WORK/node.log")"

SQL="INSERT INTO $E2E_DB.orders SELECT order_no, qty FROM local.orders_in"
req POST /api/pipelines \
  "{\"name\":\"bf-main\",\"sqlContent\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$FDS,\"$E2E_DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
MAIN_PID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/pipelines/$MAIN_PID/publish" "" "$TOKEN"
PUB="$CODE"
req POST "/api/pipelines/$MAIN_PID/deploy" '{}' "$TOKEN"
req GET "/api/pipelines/$MAIN_PID" "" "$TOKEN"
MAIN_STATUS="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
{ [ -n "$MAIN_PID" ] && [ "$PUB" = "200" ] && [ "$MAIN_STATUS" = "RUNNING" ]; } \
  && step pass "T2.4 main pipeline#$MAIN_PID file→orders published+deployed → RUNNING" \
  || step fail "T2.4 main pipeline" "id=$MAIN_PID pub=$PUB status=$MAIN_STATUS body=$BODY"

ROWS="$(doris_wait_rows orders 10)"
[ "${ROWS:-x}" = "10" ] && step pass "T2.5 main pipeline wrote batch1: orders COUNT(*)=10" \
  || step fail "T2.5 batch1 rows" "rows=${ROWS:-?} node.log=$(tail -5 "$WORK/node.log")"

# ---------- T3 dryRun: zero side effects ----------
backfill "$MAIN_PID" 0 "$SIZE1" true
DRY_JOB="$(jq -r '.jobId // "null"' <<<"$BODY" 2>/dev/null)"
DRY_LP="$(jq -r '.labelPrefix // empty' <<<"$BODY" 2>/dev/null)"
req GET "/api/pipelines/$MAIN_PID/backfill/jobs" "" "$TOKEN"
JOBS_N="$(jq -r '.jobs | length' <<<"$BODY" 2>/dev/null)"
ROWS="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders;")"
{ [ "$CODE" = "200" ] && [ "$DRY_JOB" = "null" ] && [ -n "$DRY_LP" ] && grep -q -- '-s0-bf$' <<<"$DRY_LP" \
    && [ "${JOBS_N:-x}" = "0" ] && [ "${ROWS:-x}" = "10" ]; } \
  && step pass "T3.1 dryRun → jobId=null, labelPrefix=$DRY_LP, no job row, table still 10" \
  || step fail "T3.1 dryRun" "http=$CODE jobId=$DRY_JOB lp=$DRY_LP jobs=$JOBS_N rows=${ROWS:-?}"

# ---------- T4 backfill #1: first replay of [0, SIZE1) ----------
backfill "$MAIN_PID" 0 "$SIZE1"
JOB1="$(jq -r '.jobId // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "202" ] && [ -n "$JOB1" ]; } && step pass "T4.1 backfill#1 accepted (job=$JOB1, 202 pending)" \
  || step fail "T4.1 backfill#1 accept" "http=$CODE body=$BODY"
if wait_job "$MAIN_PID" "$JOB1" 60; then
  # rows>0 is asserted, not narrated: a backfill that read nothing would make
  # every later "row count unchanged" step vacuously green (first live run)
  { [ "$JOB_STATE" = "done" ] && [ "${JOB_ROWS:-0}" -gt 0 ]; } \
    && step pass "T4.2 backfill#1 job done with rowsWritten=$JOB_ROWS (the replay really moved data)" \
    || step fail "T4.2 backfill#1 terminal" "state=$JOB_STATE rows=${JOB_ROWS:-?} error=$JOB_ERROR"
else
  step fail "T4.2 backfill#1 terminal" "job never reached a terminal state (60×2s)"
fi
R1="$(doris_wait_rows orders 20)"
[ "${R1:-x}" = "20" ] \
  && step pass "T4.3 replay #1: table 10→20 (main 10 process-scoped labels + replay 10 deterministic labels)" \
  || step fail "T4.3 replay #1 rows" "rows=${R1:-?} (expected 20)"

# ---------- T5 backfill #2: SAME range — THE core assertion ----------
backfill "$MAIN_PID" 0 "$SIZE1"
JOB2="$(jq -r '.jobId // empty' <<<"$BODY" 2>/dev/null)"
wait_job "$MAIN_PID" "$JOB2" 60 \
  && step pass "T5.1 backfill#2 (same range, new job=$JOB2) terminal state=$JOB_STATE" \
  || step fail "T5.1 backfill#2 terminal" "timeout state=$JOB_STATE"
R2="$(doris_wait_rows orders 20)"
[ "${R2:-x}" = "$R1" ] \
  && step pass "T5.2 CORE: same-range replay #2 leaves the row count unchanged ($R2 == $R1 — label already exists → Dedup; a broken label would show +10 on this DUPLICATE table)" \
  || step fail "T5.2 CORE replay idempotency" "rows=${R2:-?} != ${R1:-?} (double write visible — deterministic label failed)"

# ---------- T6 backfill #3 concurrent with main-pipeline writes ----------
# #3's range is pinned to [0, SIZE1): append happens AFTER the trigger, so the
# new bytes sit beyond `to` — the bounded runner stops at SIZE1 and can never
# swallow batch2 (cursor_reached, engine lib.rs), keeping the replay set exact
backfill "$MAIN_PID" 0 "$SIZE1"
JOB3="$(jq -r '.jobId // empty' <<<"$BODY" 2>/dev/null)"
for i in $(seq 10 19); do printf '{"order_no":"BF-%s-%03d","qty":%d}\n' "$RUN_ID" "$i" "$i"; done >> "$WORK/node/orders_in"
step pass "T6.1 backfill#3 triggered (job=$JOB3) + batch2 appended while it runs"
wait_job "$MAIN_PID" "$JOB3" 60 \
  && step pass "T6.2 backfill#3 terminal state=$JOB_STATE (rowsWritten=$JOB_ROWS — ack rows ride the Dedup path; the table count below is the truth)" \
  || step fail "T6.2 backfill#3 terminal" "timeout state=$JOB_STATE"
R3="$(doris_wait_rows orders 30)"
req GET "/api/pipelines/$MAIN_PID" "" "$TOKEN"
MAIN_STATUS="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
{ [ "${R3:-x}" = "30" ] && [ "$MAIN_STATUS" = "RUNNING" ]; } \
  && step pass "T6.3 concurrent replay wrote nothing (replay set pinned to batch1) while the main pipeline consumed batch2: 30 rows, pipeline RUNNING throughout" \
  || step fail "T6.3 concurrency" "rows=${R3:-?} (expected 30) status=$MAIN_STATUS"

# ---------- T7 failed backfill never touches the main pipeline cursor ----------
# ghost pipeline: file→ghost (registered console-side, table absent in Doris).
# Published but NOT deployed — the backfill executor runs it console-side in
# its bf- ledger namespace; stream load 404s → job failed. The main pipeline's
# ledger file (node state-dir, one JSON per pipeline) must be byte-identical
# before/after, and the main pipeline must stay RUNNING.
req POST /api/pipelines \
  "{\"name\":\"bf-ghost\",\"sqlContent\":$(json_sql "INSERT INTO $E2E_DB.ghost SELECT order_no, qty FROM local.orders_in"),\"dsMapping\":{\"local\":$FDS,\"$E2E_DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
GHOST_PID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/pipelines/$GHOST_PID/publish" "" "$TOKEN"
GPUB="$CODE"
LEDGER="$(ls "$WORK/node-state"/offsets-*.json 2>/dev/null | head -1)"
if [ -n "$LEDGER" ] && [ -f "$LEDGER" ]; then
  step pass "T7.1 main ledger located: $LEDGER (pipeline#$MAIN_PID cursor)"
else
  step fail "T7.1 main ledger" "no offsets-*.json in $WORK/node-state after batch1+batch2 flushes"
fi
HASH_BEFORE="$( [ -n "$LEDGER" ] && shasum "$LEDGER" | cut -d' ' -f1 || echo none )"
backfill "$GHOST_PID" 0 "$SIZE1"
GJOB="$(jq -r '.jobId // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$GPUB" = "200" ] && [ "$CODE" = "202" ] && [ -n "$GJOB" ]; } \
  && step pass "T7.2 ghost pipeline#$GHOST_PID published (not deployed) + backfill accepted (job=$GJOB)" \
  || step fail "T7.2 ghost backfill accept" "pub=$GPUB http=$CODE body=$BODY"
if wait_job "$GHOST_PID" "$GJOB" 90; then
  [ "$JOB_STATE" = "failed" ] && [ -n "$JOB_ERROR" ] \
    && step pass "T7.3 ghost backfill job failed as designed (error=$(head -c 80 <<<"$JOB_ERROR")…)" \
    || step fail "T7.3 ghost job state" "state=$JOB_STATE error=$JOB_ERROR"
else
  step fail "T7.3 ghost job terminal" "timeout state=$JOB_STATE"
fi
HASH_AFTER="$( [ -n "$LEDGER" ] && shasum "$LEDGER" | cut -d' ' -f1 || echo none )"
req GET "/api/pipelines/$MAIN_PID" "" "$TOKEN"
MAIN_STATUS="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$HASH_AFTER" = "$HASH_BEFORE" ] && [ "$MAIN_STATUS" = "RUNNING" ]; } \
  && step pass "T7.4 CORE: failed backfill left the main ledger byte-identical (sha256 $HASH_AFTER) and the main pipeline RUNNING — bf- namespace isolation" \
  || step fail "T7.4 CORE cursor isolation" "hash $HASH_BEFORE → $HASH_AFTER status=$MAIN_STATUS"

# ---------- T8 main pipeline still alive ----------
for i in $(seq 20 29); do printf '{"order_no":"BF-%s-%03d","qty":%d}\n' "$RUN_ID" "$i" "$i"; done >> "$WORK/node/orders_in"
R4="$(doris_wait_rows orders 40)"
req GET "/api/pipelines/$MAIN_PID" "" "$TOKEN"
MAIN_STATUS="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
{ [ "${R4:-x}" = "40" ] && [ "$MAIN_STATUS" = "RUNNING" ]; } \
  && step pass "T8.1 after the failed backfill: batch3 consumed → 40 rows, main pipeline still RUNNING (cursor advanced past batch3 normally)" \
  || step fail "T8.1 main pipeline alive" "rows=${R4:-?} (expected 40) status=$MAIN_STATUS"

# ---------- teardown ----------
[ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
[ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
NODE_PID=""; CONSOLE_PID=""
# surgical compose teardown (a whole-project `compose down` is forbidden — it
# could take a resident kafka with it); external mode drops the dedicated
# e2e database only and leaves the standing stack running (smoke.sh precedent)
if [ -n "$DORIS_MYSQL" ]; then
  doris_exec 3 "DROP DATABASE IF EXISTS $E2E_DB FORCE;" \
    && step pass "S9.1 external doris cleanup: DROP DATABASE $E2E_DB FORCE (standing stack untouched)" \
    || step fail "S9.1 external cleanup" "drop failed"
else
  doris_compose stop doris-fe doris-be >/dev/null 2>&1 && doris_compose rm -f doris-fe doris-be >/dev/null 2>&1 \
    && step pass "S9.1 doris-fe+doris-be stopped+removed (surgical — compose kafka untouched)" \
    || step fail "S9.1 teardown" "docker compose stop/rm doris-fe doris-be failed"
fi

TOTAL=$((PASS+FAIL))
echo "== backfill_replay e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
