#!/usr/bin/env bash
# R8 W5 e2e: G2 type vocabulary on a live Doris (blueprint §7.7 G2 core row)
# — decimal(18,2) precision conservation through the REAL engine chain
# (file jsonl → arrow Decimal128 → stream-load JSON string → Doris DECIMAL)
# and the sequence_col ordering protection end to end (R8 incidental item 1,
# closing dogfooding #3).
#
# 用法：
#   bash scripts/e2e/type_precision.sh [--help] [--keep] [--skip-up] [--wait SECS]
#
# 两层断言：
#   P 档（raw stream load，绕过 console/engine——探针结论的服务端钉子；
#     双版本裸矩阵已归档 scripts/e2e/evidence/type-precision-probe-2026-10-30/）：
#     P1  json STRING 形态（引擎 `_` 回退臂的发射形态）写 DECIMAL(18,2)：
#         含 18,2 上限大值 99999999999999.99 逐分守恒（硬断言——引擎链路
#         精度守恒所依赖的服务端契约）；
#     P2  观察（两可皆过，细节入日志）：json NUMBER 形态大值 99999999999999.99
#         实测落库 100000000000000.00（IEEE754 double 最近值，双版本一致）；
#         若未来服务端改为精确解析，此步记录新行为而非误报失败。
#   T 档（console 全链，API 层）：
#     T1  A1 建 decimal_orders：新词汇 columnsJson
#         [order_no utf8 / amt decimal(18,2) / ts timestamp(us) / d date / note utf8]
#         × keyModel=unique —— draft DDL 断言 DECIMAL(18,2)/DATETIME(6)/DATE 映射
#         （A3），execute 后 SHOW CREATE TABLE 钉活表真实类型；
#     T2  CORE file→doris 全列管道写 10 行（amt 含 0.10 尾零 / -5.01 负数 /
#         99999999999999.99 上限大值；ts 微秒；d 日期）→ SELECT 逐行断言
#         0.01 级精度守恒 10/10 + SUM(amt) 精确 = 100000000000024.12 +
#         ts 微秒保真 + d 守恒（任务卡核心行，不可省断言）；
#     T3  sequence_col 属性链（W4 核心）：表对象声明 sequenceColumn=seq +
#         keyModel=unique → A1 draft DDL 附 "function_column.sequence_col" = "seq"
#         → execute 后 SHOW CREATE TABLE 属性在场 → 全列写 10 行（seq=10）→
#         partial 管道 (order_no,seq,qty) 混合 seq：5 行 seq=20 落盘、5 行
#         seq=5 低序被拒、未写的 note 全保留——低 seq partial 覆盖被拒，
#         定序保护真生效；
#     T4  对照（dogfooding #3 失效形态）：同形状表但不声明 sequenceColumn →
#         A1 DDL 无该属性 → 同样的低 seq partial 载荷直接覆盖（qty=555 落盘）
#         ——证明 T3 的保护来自表属性而非引擎行为差异。
#
# 覆盖变量（沿用 scripts/e2e/partial_update.sh 的模式）：
#   TAILRACE_FE_MYSQL_PORT      FE mysql 宿主端口   默认 39030（外部模式 9030）
#   TAILRACE_BE_HTTP_PORT       BE webserver 宿主端口 默认 38040（外部 8040）
#   TAILRACE_E2E_DORIS_MYSQL    外部在役 Doris 的 mysql 客户端命令前缀
#                               （设了即跳过 compose up/teardown，端口切
#                               标准口，收尾改为专库 DROP FORCE，不碰常驻
#                               栈其余库；在役栈实测 3.1.4，探针已证两版本
#                               decimal/sequence 行为逐行一致）
#   TAILRACE_E2E_DB             目标库             默认 tailrace_tp_e2e
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址   默认 127.0.0.1:18086
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址 默认 127.0.0.1:18106
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WAIT_SECS      同 --wait（默认 300）
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-tp-e2e
#
# 标志：--help / --keep / --skip-up / --wait SECS（语义同 partial_update.sh）
#
# live-gate（对齐 scripts/e2e/starrocks.sh 惯例）：Docker daemon 不可达、
#   --skip-up 下 FE 端口不通、或宿主可用磁盘 <5GiB（OrbStack 红线）时
#   [skip] 空过——不算失败。
#
# 幂等：全新 console data-dir + DROP DATABASE FORCE + 固定种子；拆栈外科式
#   （stop+rm 仅 doris-fe/doris-be 两服务，严禁整体 compose down 波及常驻
#   kafka）。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
DORIS_MYSQL="${TAILRACE_E2E_DORIS_MYSQL:-}"
if [ -n "$DORIS_MYSQL" ]; then
  FE_PORT="${TAILRACE_FE_MYSQL_PORT:-9030}"
  BE_PORT="${TAILRACE_BE_HTTP_PORT:-8040}"
else
  FE_PORT="${TAILRACE_FE_MYSQL_PORT:-39030}"
  BE_PORT="${TAILRACE_BE_HTTP_PORT:-38040}"
fi
E2E_DB="${TAILRACE_E2E_DB:-tailrace_tp_e2e}"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18086}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18106}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
WAIT_SECS="${TAILRACE_E2E_WAIT_SECS:-300}"
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-tp-e2e}"
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
# the Docker VM has ANY active swap — neutralized for THIS stack only by
# raising the threshold past any swap-device count before the original
# entrypoint runs (R6 recipe, same as partial_update.sh).
OVERRIDE_YML="$WORK/compose-be-swap-override.yml"
doris_compose() { docker compose -f "$ROOT/docker-compose.yml" -f "$OVERRIDE_YML" "$@"; }
doris_mysql_exec() { # standing-Doris prefix when TAILRACE_E2E_DORIS_MYSQL, else compose client
  if [ -n "$DORIS_MYSQL" ]; then $DORIS_MYSQL "$@"; else doris_compose exec -T doris-fe mysql "$@"; fi
}
doris_exec() { # doris_exec <retries> <sql...> — mysql client always runs INSIDE the fe container: port 9030
  local r
  for r in $(seq 1 "${1:-6}"); do
    doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${2}" && return 0
    sleep 5
  done
  return 1
}
doris_query() { # doris_query <sql> → single scalar (whitespace-stripped)
  doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${1}" 2>/dev/null | tr -d '[:space:]'
}
doris_wait_scalar() { # doris_wait_scalar <sql> <expected> — poll until the scalar equals
  local r n
  for r in $(seq 1 45); do
    n="$(doris_query "$1")"
    [ "${n:-}" = "$2" ] && { echo "$n"; return 0; }
    sleep 2
  done
  echo "${n:-}"
}
doris_wait_be_alive() { # heartbeat-driven Alive=true
  local r out
  for r in $(seq 1 30); do
    out="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" 2>/dev/null)"
    [ "$(awk -F'\t' '$10=="true"{n++} END{print n+0}' <<<"$out")" -ge 1 ] && return 0
    sleep 2
  done
  return 1
}
raw_load() { # raw_load <table> <label> <jsonl-file> [extra -H args...]
  local t="$1" l="$2" f="$3"; shift 3
  curl -s -X PUT -u root: -H "label: $l" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" "$@" --data-binary "@$f" \
    "http://127.0.0.1:$BE_PORT/api/$E2E_DB/$t/_stream_load"
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
# deploy_pipeline <name> <sink-table> <source-table> <select-cols> [insert-col-list]
deploy_pipeline() {
  local target="$E2E_DB.$2"
  [ -n "${5:-}" ] && target="$target ($5)"
  local sql="INSERT INTO $target SELECT $4 FROM local.$3"
  req POST /api/pipelines \
    "{\"name\":\"$1\",\"sqlContent\":$(json_sql "$sql"),\"dsMapping\":{\"local\":$FDS,\"$E2E_DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
  P_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
  [ -n "$P_ID" ] || { echo "  pipeline $1 create failed: http=$CODE body=$BODY" >&2; return 1; }
  req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
  [ "$CODE" = "200" ] || { echo "  pipeline $1 publish failed: http=$CODE body=$BODY" >&2; return 1; }
  req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
  [ "$CODE" = "200" ] || { echo "  pipeline $1 deploy failed: http=$CODE body=$BODY" >&2; return 1; }
  req GET "/api/pipelines/$P_ID" "" "$TOKEN"
  [ "$(jq -r '.status // empty' <<<"$BODY")" = "RUNNING" ]
}

echo "== type_precision e2e: work=$WORK fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT db=$E2E_DB console=$CONSOLE_ADDR =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable — type_precision e2e needs the compose doris stack (or TAILRACE_E2E_DORIS_MYSQL), skipped"
  exit 0
fi
AVAIL_GIB="$(df -g / | awk 'NR==2 {print $4}')"
if [ "${AVAIL_GIB:-99}" -lt 5 ]; then
  echo "[skip] host free disk ${AVAIL_GIB}GiB < 5GiB (OrbStack kill-line), skipped"
  exit 0
fi
if [ "$SKIP_UP" = "1" ]; then
  if ! nc -z 127.0.0.1 "$FE_PORT" 2>/dev/null \
     && ! (exec 3<>"/dev/tcp/127.0.0.1/$FE_PORT") 2>/dev/null; then
    echo "[skip] doris FE 127.0.0.1:$FE_PORT unreachable (--skip-up expects a running stack; or drop --skip-up), skipped"
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
# Generated by scripts/e2e/type_precision.sh — e2e-only, never a committed
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
  step pass "S2.0 backend Alive=true (heartbeat-driven availability)"
else
  step fail "S2.0 backend alive" "no Alive=true backend in SHOW BACKENDS after 30×2s"
fi
if doris_exec 6 "DROP DATABASE IF EXISTS $E2E_DB FORCE; CREATE DATABASE $E2E_DB;" \
   && [ -z "$(doris_query "SHOW TABLES FROM $E2E_DB;")" ]; then
  step pass "S2.1 database $E2E_DB empty (DROP FORCE + CREATE)"
else
  step fail "S2.1 empty database" "create failed"
fi

# ---------- P tier: raw stream-load contract (server-side, no engine) ----------
# The bare dual-version matrix lives in evidence/type-precision-probe-2026-10-30/;
# these two steps re-pin the load-bearing halves on THIS run's server.
doris_exec 3 "CREATE TABLE $E2E_DB.probe_dec (id INT NOT NULL, amt DECIMAL(18,2)) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" \
  && step pass "P0.1 probe_dec created (DECIMAL(18,2), hand-DDLed on purpose — this tier pins Doris, not the engine)" \
  || step fail "P0.1 probe_dec" "create failed"
printf '{"id":1,"amt":"12.34"}\n{"id":2,"amt":"0.10"}\n{"id":3,"amt":"-5.01"}\n{"id":4,"amt":"99999999999999.99"}\n' > "$WORK/probe_str.jsonl"
R="$(raw_load probe_dec "tp_p_str_$$" "$WORK/probe_str.jsonl")"
KEEP_STR="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.probe_dec WHERE (id=1 AND amt=12.34) OR (id=2 AND amt=0.10) OR (id=3 AND amt=-5.01) OR (id=4 AND amt=99999999999999.99);")"
{ echo "$R" | grep -q '"Status": *"Success"' && [ "$KEEP_STR" = "4" ]; } \
  && step pass "P1.1 json STRING payloads (the engine's emission shape) round-trip DECIMAL(18,2) exactly — including the 18,2-ceiling 99999999999999.99 (the server contract the engine chain rides)" \
  || step fail "P1.1 string-shape conservation" "kept=$KEEP_STR resp=$(head -c 200 <<<"$R")"
printf '{"id":11,"amt":99999999999999.99}\n' > "$WORK/probe_num.jsonl"
R="$(raw_load probe_dec "tp_p_num_$$" "$WORK/probe_num.jsonl")"
NUM_LANDED="$(doris_query "SELECT amt FROM $E2E_DB.probe_dec WHERE id=11;")"
if echo "$R" | grep -q '"Status": *"Success"'; then
  # version-stable observation (2.1.7 + 3.1.4 probed alike): json NUMBER goes
  # through IEEE754 double and the ceiling value lands +0.01 high — the exact
  # trap the engine's string emission avoids
  step pass "P2.1 observation: json NUMBER 99999999999999.99 landed as ${NUM_LANDED:-<none>} (2.1.7/3.1.4: 100000000000000.00 — the double trap; recorded, not asserted)"
else
  step pass "P2.1 observation: json NUMBER load refused by this server version: $(head -c 120 <<<"$R")"
fi

# ---------- S3 console boot + auth + datasources ----------
TAILRACE_BOOTSTRAP_TOKEN="tr_b_tp_e2e_00112233445566778899aabbccddeeff" \
  "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" >"$WORK/console.log" 2>&1 &
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
  "{\"name\":\"tp-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST /api/datasources "{\"name\":\"tp-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$DDS" ] && [ -n "$FDS" ]; } && step pass "S3.3 doris+file datasources (d=$DDS f=$FDS)" \
  || step fail "S3.3 datasources" "dds=$DDS fds=$FDS body=$BODY"

# ---------- S3.5 engine node ----------
( cd "$WORK/node" && exec env "TAILRACE_DS_$DDS=Basic cm9vdDo=" \
    "$NODE_BIN" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "tr_b_tp_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 \
  && step pass "S3.4 engine node up (serve)" \
  || step fail "S3.4 node boot" "$(tail -3 "$WORK/node.log")"
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "S3.5 node ONLINE" || step fail "S3.5 node ONLINE" "node.log=$(tail -5 "$WORK/node.log")"

# ---------- T1 A1 loop: decimal table with the NEW vocabulary ----------
# G2 words in columnsJson: decimal(18,2) / timestamp(us) / date — the A3
# mapping (DECIMAL(18,2) / DATETIME(6) / DATE) must show up in the draft.
COLS_DEC='[["order_no","utf8"],["amt","decimal(18,2)"],["ts","timestamp(us)"],["d","date"],["note","utf8"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"decimal_orders\",\"format\":null,\"columnsJson\":$COLS_DEC}" "$TOKEN"
T_DEC="$CODE"
{ [ "$T_DEC" = "201" ] || [ "$T_DEC" = "200" ]; } \
  && step pass "T1.1 sink table object decimal_orders registered with the G2 vocabulary (amt decimal(18,2), ts timestamp(us), d date)" \
  || step fail "T1.1 table object" "http=$T_DEC body=$BODY"
req POST "/api/datasources/$DDS/tables/$E2E_DB/decimal_orders/ddl/draft" \
  "{\"keyModel\":\"unique\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
DEC_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && grep -q '`amt` DECIMAL(18,2)' <<<"$DEC_DDL" && grep -q '`ts` DATETIME(6)' <<<"$DEC_DDL" && grep -q '`d` DATE' <<<"$DEC_DDL"; } \
  && step pass "T1.2 draft maps the new vocabulary: amt→DECIMAL(18,2), ts(timestamp(us))→DATETIME(6), d(date)→DATE (A3 mapping in the draft text)" \
  || step fail "T1.2 draft mapping" "http=$CODE ddl=$(head -c 300 <<<"$DEC_DDL")"
req POST "/api/datasources/$DDS/tables/$E2E_DB/decimal_orders/ddl/execute" \
  "{\"ddl\":$(json_sql "$DEC_DDL"),\"keyModel\":\"unique\"}" "$TOKEN"
SHOW_DEC="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.decimal_orders;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
{ [ "$CODE" = "200" ] && grep -q '`amt` decimal(18,2)' <<<"$SHOW_DEC" && grep -q '`ts` datetime(6)' <<<"$SHOW_DEC" && grep -q '`d` date' <<<"$SHOW_DEC"; } \
  && step pass "T1.3 execute → live SHOW CREATE TABLE carries amt decimal(18,2) / ts datetime(6) / d date (real Doris types, zero hand DDL)" \
  || step fail "T1.3 execute+show" "http=$CODE show=$(head -c 300 <<<"$SHOW_DEC")"

# ---------- T2 CORE: file→doris full-column write, 0.01-level conservation ----------
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"dec_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_DEC}" "$TOKEN"
T_F1="$CODE"
{ [ "$T_F1" = "201" ] || [ "$T_F1" = "200" ]; } && step pass "T2.1 file table local.dec_in (jsonl, same G2 vocabulary)" \
  || step fail "T2.1 file table" "http=$T_F1"
# 10 rows: amt covers trailing-zero (0.10), negative (-5.01), plain cents and
# the 18,2 ceiling (99999999999999.99); ts carries microseconds; d distinct
# days. amt cells are JSON STRINGS on purpose: the engine parses them as
# exact base-10 (probe P1), while a json NUMBER rides an f64 detour that
# already lost the last cent at this magnitude (probe P2 + first e2e run:
# 99999999999999.99 landed ...99.98 through the number path).
AMTS=(0.10 1.11 2.22 12.34 -5.01 0.05 99999999999999.99 3.33 4.44 5.55)
for i in $(seq 0 9); do
  printf '{"order_no":"TP-%03d","amt":"%s","ts":"2026-10-30 12:34:5%d.789012","d":"2026-10-2%d","note":"tp-%d"}\n' \
    "$i" "${AMTS[$i]}" "$i" "$i" "$i"
done > "$WORK/node/dec_in"
step pass "T2.2 seeds: 10 rows incl. the 18,2-ceiling amount, microsecond timestamps"
if deploy_pipeline tp-dec decimal_orders dec_in "order_no, amt, ts, d, note"; then
  step pass "T2.3 pipeline full-column file→decimal_orders deployed → RUNNING"
else
  step fail "T2.3 pipeline decimal_orders" "deploy chain failed (see lines above)"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.decimal_orders;" 10)"
S="$(doris_query "SELECT SUM(amt) FROM $E2E_DB.decimal_orders;")"
# per-row conservation: every amt exactly equals its seed (string compare on
# the decimal — Doris's own decimal arithmetic, no float in sight), ts keeps
# its microseconds, d its day, on all 10 rows at once
ROWS_OK="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.decimal_orders WHERE (order_no='TP-000' AND amt=0.10 AND ts='2026-10-30 12:34:50.789012' AND d='2026-10-20' AND note='tp-0') OR (order_no='TP-001' AND amt=1.11 AND ts='2026-10-30 12:34:51.789012' AND d='2026-10-21' AND note='tp-1') OR (order_no='TP-002' AND amt=2.22 AND ts='2026-10-30 12:34:52.789012' AND d='2026-10-22' AND note='tp-2') OR (order_no='TP-003' AND amt=12.34 AND ts='2026-10-30 12:34:53.789012' AND d='2026-10-23' AND note='tp-3') OR (order_no='TP-004' AND amt=-5.01 AND ts='2026-10-30 12:34:54.789012' AND d='2026-10-24' AND note='tp-4') OR (order_no='TP-005' AND amt=0.05 AND ts='2026-10-30 12:34:55.789012' AND d='2026-10-25' AND note='tp-5') OR (order_no='TP-006' AND amt=99999999999999.99 AND ts='2026-10-30 12:34:56.789012' AND d='2026-10-26' AND note='tp-6') OR (order_no='TP-007' AND amt=3.33 AND ts='2026-10-30 12:34:57.789012' AND d='2026-10-27' AND note='tp-7') OR (order_no='TP-008' AND amt=4.44 AND ts='2026-10-30 12:34:58.789012' AND d='2026-10-28' AND note='tp-8') OR (order_no='TP-009' AND amt=5.55 AND ts='2026-10-30 12:34:59.789012' AND d='2026-10-29' AND note='tp-9');")"
{ [ "${N:-x}" = "10" ] && [ "${S:-x}" = "100000000000024.12" ] && [ "${ROWS_OK:-x}" = "10" ]; } \
  && step pass "T2.4 CORE: 10/10 rows conserved to the cent through the engine chain — amt exact per-row (incl. 99999999999999.99), SUM(amt)=100000000000024.12 exact, ts microseconds intact, d intact (blueprint §7.7 G2 row)" \
  || step fail "T2.4 CORE conservation" "n=$N sum=$S rows=$ROWS_OK (expected 10 / 100000000000024.12 / 10) node.log=$(tail -5 "$WORK/node.log" 2>/dev/null)"

# ---------- T3 sequence_col attribute chain (W4, closing dogfooding #3) ----------
COLS_SEQ='[["order_no","utf8"],["seq","int64"],["qty","int64"],["note","utf8"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders_seq2\",\"format\":null,\"columnsJson\":$COLS_SEQ,\"sequenceColumn\":\"seq\",\"keyModel\":\"unique\"}" "$TOKEN"
T_SEQ="$CODE"
{ [ "$T_SEQ" = "201" ] || [ "$T_SEQ" = "200" ]; } \
  && step pass "T3.1 table object orders_seq2 registered WITH sequenceColumn=seq + keyModel=unique (the declarations the generator reads)" \
  || step fail "T3.1 seq table object" "http=$T_SEQ body=$BODY"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_seq2/ddl/draft" \
  "{\"keyModel\":\"unique\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
SEQ_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && grep -q '"function_column.sequence_col" = "seq"' <<<"$SEQ_DDL"; } \
  && step pass "T3.2 draft carries the table-level property \"function_column.sequence_col\" = \"seq\" (W4 generator fix — pre-R8 drafts left it a plain column and Doris silently ignored the load header)" \
  || step fail "T3.2 draft sequence_col" "http=$CODE ddl=$(head -c 300 <<<"$SEQ_DDL")"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_seq2/ddl/execute" \
  "{\"ddl\":$(json_sql "$SEQ_DDL"),\"keyModel\":\"unique\"}" "$TOKEN"
SHOW_SEQ="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.orders_seq2;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
{ [ "$CODE" = "200" ] && grep -q 'function_column.sequence_col' <<<"$SHOW_SEQ" && grep -q 'seq' <<<"$(grep 'function_column.sequence_col' <<<"$SHOW_SEQ")"; } \
  && step pass "T3.3 execute → live SHOW CREATE TABLE shows the sequence_col property on the table (the attribute exists in Doris, not just in the draft)" \
  || step fail "T3.3 seq table live" "http=$CODE show=$(head -c 300 <<<"$SHOW_SEQ")"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"seq2_full_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_SEQ}" "$TOKEN"
T_F2="$CODE"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"seq2_delta_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_SEQ}" "$TOKEN"
T_F3="$CODE"
{ [ "$T_F2" = "201" ] || [ "$T_F2" = "200" ]; } && { [ "$T_F3" = "201" ] || [ "$T_F3" = "200" ]; } \
  && step pass "T3.4 file tables seq2_full_in / seq2_delta_in" \
  || step fail "T3.4 file tables" "f2=$T_F2 f3=$T_F3"
for i in $(seq 0 9); do printf '{"order_no":"SQ-%03d","seq":10,"qty":%d,"note":"first-%03d"}\n' "$i" "$i" "$i"; done > "$WORK/node/seq2_full_in"
# even rows: seq=20 (newer, qty=200+i must land); odd rows: seq=5 (older, qty=500+i must be DISCARDED)
for i in $(seq 0 9); do
  if [ $((i % 2)) -eq 0 ]; then printf '{"order_no":"SQ-%03d","seq":20,"qty":%d}\n' "$i" "$((200 + i))"
  else printf '{"order_no":"SQ-%03d","seq":5,"qty":%d}\n' "$i" "$((500 + i))"; fi
done > "$WORK/node/seq2_delta_in"
step pass "T3.5 seeds: seq2_full_in 10×seq=10; seq2_delta_in 5×seq=20(new) + 5×seq=5(stale)"
if deploy_pipeline tp-seq2-full orders_seq2 seq2_full_in "order_no, seq, qty, note"; then
  step pass "T3.6 full-column pipeline → orders_seq2 RUNNING (sequence_column header rides every load)"
else
  step fail "T3.6 pipeline orders_seq2 full" "deploy chain failed (see lines above)"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.orders_seq2;" 10)"
S="$(doris_query "SELECT SUM(qty) FROM $E2E_DB.orders_seq2;")"
{ [ "${N:-x}" = "10" ] && [ "${S:-x}" = "45" ]; } \
  && step pass "T3.7 first round in place: 10 rows at seq=10, SUM(qty)=45" \
  || step fail "T3.7 first round" "n=$N sum=$S"
if deploy_pipeline tp-seq2-part orders_seq2 seq2_delta_in "order_no, seq, qty" "order_no, seq, qty"; then
  step pass "T3.8 partial pipeline 'INSERT INTO orders_seq2 (order_no, seq, qty)' deployed → RUNNING (partial × sequence)"
else
  step fail "T3.8 pipeline orders_seq2 partial" "deploy chain failed (see lines above)"
fi
# The acceptance row: LOW-seq partial overwrites are REFUSED because the
# table-level attribute exists. SUM: (200+202+204+206+208)+(1+3+5+7+9)=1045
S="$(doris_wait_scalar "SELECT SUM(qty) FROM $E2E_DB.orders_seq2;" 1045)"
N="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq2;")"
S20="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq2 WHERE seq=20;")"
S10="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq2 WHERE seq=10;")"
# stale qty=500+i must appear NOWHERE; note (written by neither partial load) survives
BADQ="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq2 WHERE qty >= 500;")"
KEPT="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq2 WHERE note = CONCAT('first-', SUBSTRING(order_no, 4, 3));")"
{ [ "${S:-x}" = "1045" ] && [ "${N:-x}" = "10" ] && [ "${S20:-x}" = "5" ] && [ "${S10:-x}" = "5" ] && [ "${BADQ:-x}" = "0" ] && [ "${KEPT:-x}" = "10" ]; } \
  && step pass "T3.9 CORE: low-seq partial overwrites REFUSED — 5 stale rows (seq=5, qty=500+i) discarded everywhere, 5 newer (seq=20) landed, unwritten note survived all 10 — the ordering protection the attribute buys (dogfooding #3 closed on the engine chain)" \
  || step fail "T3.9 sequence protection" "sum=$S n=$N seq20=$S20 seq10=$S10 staleqty=$BADQ kept=$KEPT (expected 1045/10/5/5/0/10)"

# ---------- T4 contrast: same loads on the attribute-LESS shape ----------
# Pre-R8 generator shape (no sequenceColumn declared → no table attribute):
# the engine still sends partial columns, Doris has nothing to order by, the
# stale value WINS. This is the failure form T3.3's attribute exists to
# prevent — proving T3's protection comes from the attribute, not from any
# engine-side behaviour change.
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders_seq_bare\",\"format\":null,\"columnsJson\":$COLS_SEQ,\"keyModel\":\"unique\"}" "$TOKEN"
T_BARE="$CODE"
{ [ "$T_BARE" = "201" ] || [ "$T_BARE" = "200" ]; } \
  && step pass "T4.1 contrast table object orders_seq_bare registered WITHOUT sequenceColumn (pre-R8 shape)" \
  || step fail "T4.1 bare table object" "http=$T_BARE body=$BODY"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_seq_bare/ddl/draft" \
  "{\"keyModel\":\"unique\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
BARE_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
BARE_LOW="$(tr '[:upper:]' '[:lower:]' <<<"$BARE_DDL")"
{ [ "$CODE" = "200" ] && grep -q 'unique key(`order_no`)' <<<"$BARE_LOW" && ! grep -q 'sequence_col' <<<"$BARE_LOW"; } \
  && step pass "T4.2 draft for the bare table has NO sequence_col property (undeclared = not emitted)" \
  || step fail "T4.2 bare draft" "http=$CODE ddl=$(head -c 300 <<<"$BARE_DDL")"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_seq_bare/ddl/execute" \
  "{\"ddl\":$(json_sql "$BARE_DDL"),\"keyModel\":\"unique\"}" "$TOKEN"
{ [ "$CODE" = "200" ]; } && step pass "T4.3 bare table created" \
  || step fail "T4.3 bare create" "http=$CODE body=$BODY"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"bare_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_SEQ}" "$TOKEN"
T_F4="$CODE"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"bare_delta\",\"format\":\"jsonl\",\"columnsJson\":$COLS_SEQ}" "$TOKEN"
T_F5="$CODE"
{ [ "$T_F4" = "201" ] || [ "$T_F4" = "200" ]; } && { [ "$T_F5" = "201" ] || [ "$T_F5" = "200" ]; } \
  && step pass "T4.4 file tables bare_in / bare_delta (separate files — never rewrite a source under a live pipeline)" \
  || step fail "T4.4 file tables" "f4=$T_F4 f5=$T_F5"
printf '{"order_no":"BR-0","seq":10,"qty":1,"note":"first"}\n' > "$WORK/node/bare_in"
printf '{"order_no":"BR-0","seq":5,"qty":555}\n' > "$WORK/node/bare_delta"
if deploy_pipeline tp-bare-full orders_seq_bare bare_in "order_no, seq, qty, note"; then
  step pass "T4.5 full-column pipeline → orders_seq_bare RUNNING (one row, seq=10)"
else
  step fail "T4.5 pipeline bare full" "deploy chain failed (see lines above)"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.orders_seq_bare;" 1)"
[ "${N:-x}" = "1" ] && step pass "T4.6 first round in place (BR-0 at seq=10)" \
  || step fail "T4.6 bare first round" "n=$N"
if deploy_pipeline tp-bare-part orders_seq_bare bare_delta "order_no, seq, qty" "order_no, seq, qty"; then
  step pass "T4.7 same stale partial (seq=5, qty=555) deployed against the attribute-less table"
else
  step fail "T4.7 pipeline bare partial" "deploy chain failed (see lines above)"
fi
OVERWRITTEN="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.orders_seq_bare WHERE seq=5 AND qty=555;" 1)"
{ [ "${OVERWRITTEN:-x}" = "1" ]; } \
  && step pass "T4.8 contrast: the SAME stale row overwrote the newer one on the attribute-less table (seq=5 qty=555 landed) — the exact dogfooding #3 failure form; T3's attribute is the only difference" \
  || step fail "T4.8 overwrite contrast" "overwritten=$OVERWRITTEN (expected 1)"

# ---------- teardown ----------
[ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
[ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
NODE_PID=""; CONSOLE_PID=""
# surgical teardown: stop+rm ONLY the two doris services (a whole-project
# compose down is forbidden — resident kafka); external mode drops the
# dedicated e2e database and leaves the standing stack running
if [ -n "$DORIS_MYSQL" ]; then
  doris_exec 3 "DROP DATABASE IF EXISTS $E2E_DB FORCE;" \
    && step pass "S4.1 external doris cleanup: DROP DATABASE $E2E_DB FORCE (standing stack untouched)" \
    || step fail "S4.1 external cleanup" "drop failed"
else
  doris_compose stop doris-fe doris-be >/dev/null 2>&1 && doris_compose rm -f doris-fe doris-be >/dev/null 2>&1 \
    && step pass "S4.1 doris-fe+doris-be stopped+removed (surgical — compose kafka untouched)" \
    || step fail "S4.1 teardown" "compose stop/rm doris-fe doris-be failed"
fi

TOTAL=$((PASS+FAIL))
echo "== type_precision e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
