#!/usr/bin/env bash
# R9 W1 e2e: VARIANT passthrough (T-A4, blueprint §7.1 note 4) — nested JSON
# through the REAL engine chain (file jsonl / debezium-json → decoder
# variant passthrough as JSON text → sink's ordinary string escaping →
# Doris VARIANT), with the server-side sub-column inference asserted from
# SQL (`payload['a']['b']`).
#
# 用法：
#   bash scripts/e2e/variant.sh [--help] [--keep] [--skip-up] [--wait SECS]
#
# 两层断言：
#   P 档（raw stream load，绕过 console/engine——探针结论的服务端钉子；
#     双版本裸矩阵已归档 scripts/e2e/evidence/variant-probe-2026-10-30/）：
#     P1  json STRING 形态（引擎 `_` 回退臂对 Utf8 列的发射形态：
#         serde_json::to_string 转义后的 JSON string literal）写 VARIANT
#         → 子列查询与直接给 JSON object 等价（硬断言——decoder 透传 +
#         sink 现状转义这条链所依赖的服务端契约）；
#     P2  UNIQUE MoW × partial_columns：只更新其他列时 variant 值保留、
#         更新 variant 列本身是整值替换而非深合并（硬断言——A7 × A4
#         交叉的服务端语义）。
#   T 档（console 全链，API 层）：
#     T1  A1 建 variant_events：columnsJson 含 ["payload","variant"] ×
#         keyModel=duplicate —— draft DDL 断言 `payload` VARIANT（A4 映射）、
#         execute 后 SHOW CREATE TABLE 钉活表真实类型、evolve 无 diff
#         （DESC 回映 variant → round-trip 稳定）；把 keyColumn 指到
#         variant 列 → 400（生成器拒绝，FE 同款语义）；
#     T2  CORE file jsonl → doris 全列管道：嵌套 JSON（嵌套 object/数组/
#         字符串/浮点/布尔叶 + 深四层 + payload:null + 键缺席）→ VARIANT
#         → Doris 侧子列查询逐叶断言（含混合类型子列）——任务卡核心行；
#     T3  UNIQUE MoW 表 variant 列 × partial_columns 管道：只写 note →
#         payload 保留；只写 payload → 整值替换、note 保留；
#     T4  debezium-json 源：after.payload 嵌套 → VARIANT 子列查询；delete
#         事件跳过。
#
# 断言红线（探针 README 第 12 条）：raw `SELECT v` 的数组回显两版本格式
#   有差（2.1.7 `[1,2,3]` vs 3.1.4 `[1, 2, 3]`）——一切断言走子列查询或
#   CAST，绝不对 raw 文本做 byte-exact 比对。
#
# 覆盖变量（沿用 scripts/e2e/type_precision.sh 的模式）：
#   TAILRACE_FE_MYSQL_PORT      FE mysql 宿主端口   默认 39030（外部模式 9030）
#   TAILRACE_BE_HTTP_PORT       BE webserver 宿主端口 默认 38040（外部 8040）
#   TAILRACE_E2E_DORIS_MYSQL    外部在役 Doris 的 mysql 客户端命令前缀
#                               （设了即跳过 compose up/teardown；在役栈实测
#                               3.1.4，探针已证两版本 VARIANT 核心行为一致）
#   TAILRACE_E2E_DB             目标库             默认 tailrace_va_e2e
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址   默认 127.0.0.1:18086
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址 默认 127.0.0.1:18106
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WAIT_SECS      同 --wait（默认 300）
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-va-e2e
#
# 标志：--help / --keep / --skip-up / --wait SECS（语义同 partial_update.sh）
#
# live-gate（对齐 scripts/e2e/starrocks.sh 惯例）：Docker daemon 不可达、
#   --skip-up 下 FE 端口不通、或宿主可用磁盘 <5GiB（OrbStack 红线）时
#   [skip] 空过——不算失败。
#
# 幂等：全新 console data-dir + DROP DATABASE FORCE；拆栈外科式
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
E2E_DB="${TAILRACE_E2E_DB:-tailrace_va_e2e}"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18086}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18106}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
# the node is exec'd from inside $WORK/node (its file-source cwd), so a
# relative binary path handed in by the caller must be anchored to the repo
# root first — first-run finding: `target/debug/tailrace-node` resolved
# against the work dir and the node never came up (all pipelines 409)
case "$CONSOLE_BIN" in /*) ;; *) CONSOLE_BIN="$ROOT/$CONSOLE_BIN" ;; esac
case "$NODE_BIN" in /*) ;; *) NODE_BIN="$ROOT/$NODE_BIN" ;; esac
WAIT_SECS="${TAILRACE_E2E_WAIT_SECS:-300}"
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-va-e2e}"
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
# the Docker VM has ANY active swap — neutralized for THIS stack only (R6
# recipe, same as partial_update.sh / type_precision.sh).
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

echo "== variant e2e: work=$WORK fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT db=$E2E_DB console=$CONSOLE_ADDR =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable — variant e2e needs the compose doris stack (or TAILRACE_E2E_DORIS_MYSQL), skipped"
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
  || step fail "S0.2 engine node binary" "missing $NODE_BIN (run ./scripts/build.sh)"

# ---------- S0.5 work dir + compose override + VM sysctl (compose mode only) ----------
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"
cat > "$OVERRIDE_YML" <<'YAML'
# Generated by scripts/e2e/variant.sh — e2e-only, never a committed default.
# Neutralizes the BE 2.1.7 start_be.sh unconditional swap refusal for this
# throwaway test stack (threshold raised past any swap-device count), then
# hands control back to the image's original entrypoint unchanged.
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
# The bare dual-version matrix lives in evidence/variant-probe-2026-10-30/;
# these steps re-pin the load-bearing halves on THIS run's server.
doris_exec 3 "CREATE TABLE $E2E_DB.probe_var (id INT NOT NULL, v VARIANT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" \
  && step pass "P0.1 probe_var created (VARIANT, hand-DDLed on purpose — this tier pins Doris, not the engine)" \
  || step fail "P0.1 probe_var" "create failed"
# P1: the engine's exact emission shape — the decoder writes the raw JSON text
# into a Utf8 cell, the sink escapes it via serde_json::to_string, so Doris
# receives a JSON *string literal*. The server parses it into the variant
# structure (probe B2) — THE contract this whole chain rides.
printf '%s\n' '{"id":1,"v":"{\"a\":{\"b\":42,\"c\":\"x\"},\"arr\":[7,8,9]}"}' > "$WORK/p_str.jsonl"
printf '%s\n' '{"id":2,"v":{"a":{"b":42,"c":"x"},"arr":[7,8,9]}}' > "$WORK/p_obj.jsonl"
R1="$(raw_load probe_var "va_p_str_$$" "$WORK/p_str.jsonl")"
R2="$(raw_load probe_var "va_p_obj_$$" "$WORK/p_obj.jsonl")"
AB1="$(doris_wait_scalar "SELECT v['a']['b'] FROM $E2E_DB.probe_var WHERE id=1;" 42)"
AB2="$(doris_query "SELECT v['a']['b'] FROM $E2E_DB.probe_var WHERE id=2;")"
{ echo "$R1" | grep -q '"Status": *"Success"' && echo "$R2" | grep -q '"Status": *"Success"' \
    && [ "${AB1:-x}" = "42" ] && [ "${AB2:-x}" = "42" ]; } \
  && step pass "P1.1 json STRING shape (the engine's sink emission) parses into the variant structure — v['a']['b']=42 on BOTH shapes, identical to the bare object (probe B2 re-pinned; the contract decoder+sink ride without any sink change)" \
  || step fail "P1.1 string-shape parse" "ab1=$AB1 ab2=$AB2 resp=$(head -c 200 <<<"$R1")"
AC="$(doris_query "SELECT v['a']['c'] FROM $E2E_DB.probe_var WHERE id=1;")"
ARR="$(doris_query "SELECT CAST(v['arr'] AS STRING) FROM $E2E_DB.probe_var WHERE id=1;" | tr -d ' ')"
{ [ "${AC:-x}" = "x" ] && [ "${ARR:-x}" = "[7,8,9]" ]; } \
  && step pass "P1.2 deeper leaves ride too: v['a']['c']='x', v['arr']=[7,8,9] (sub-column, not raw-text, assertion)" \
  || step fail "P1.2 leaves" "ac=$AC arr=$ARR"
# P2: UNIQUE MoW × partial_columns on a VARIANT column (probe C)
doris_exec 3 "CREATE TABLE $E2E_DB.probe_mow (k VARCHAR(32) NOT NULL, name VARCHAR(32), v VARIANT) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");" >/dev/null
printf '%s\n' '{"k":"K1","name":"first","v":"{\"a\":{\"b\":1},\"keep\":\"orig\"}"}' > "$WORK/p_full.jsonl"
printf '%s\n' '{"k":"K1","name":"renamed"}' > "$WORK/p_name.jsonl"
printf '%s\n' '{"k":"K1","v":{"a":{"b":2},"new":"replaced"}}' > "$WORK/p_v.jsonl"
R="$(raw_load probe_mow "va_p_mow_$$" "$WORK/p_full.jsonl")"
R="$(raw_load probe_mow "va_p_mow_n_$$" "$WORK/p_name.jsonl" -H "partial_columns: true" -H "columns: k,name")"
KEEPED="$(doris_query "SELECT v['keep'] FROM $E2E_DB.probe_mow WHERE k='K1';")"
R="$(raw_load probe_mow "va_p_mow_v_$$" "$WORK/p_v.jsonl" -H "partial_columns: true" -H "columns: k,v")"
NEWK="$(doris_query "SELECT v['new'] FROM $E2E_DB.probe_mow WHERE k='K1';")"
OLDK="$(doris_query "SELECT v['keep'] FROM $E2E_DB.probe_mow WHERE k='K1';")"
NAME="$(doris_query "SELECT name FROM $E2E_DB.probe_mow WHERE k='K1';")"
{ [ "${KEEPED:-x}" = "orig" ] && [ "${NEWK:-x}" = "replaced" ] && [ "${OLDK:-NULL}" = "NULL" ] && [ "${NAME:-x}" = "renamed" ]; } \
  && step pass "P2.1 UNIQUE MoW partial_columns: other-column update preserves the variant value (keep=orig); variant update replaces it WHOLE (keep gone, new=replaced) — no deep merge (probe C re-pinned)" \
  || step fail "P2.1 mow partial" "keeped=$KEEPED new=$NEWK oldk=$OLDK name=$NAME"

# ---------- S3 console boot + auth + datasources ----------
TAILRACE_BOOTSTRAP_TOKEN="tr_b_va_e2e_00112233445566778899aabbccddeeff" \
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
  "{\"name\":\"va-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST /api/datasources "{\"name\":\"va-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
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
      --bootstrap-token "tr_b_va_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
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

# ---------- T1 A1 loop: variant table through draft/execute/evolve ----------
COLS_VAR='[["event_id","utf8"],["payload","variant"],["note","utf8"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"variant_events\",\"format\":null,\"columnsJson\":$COLS_VAR}" "$TOKEN"
T_VAR="$CODE"
{ [ "$T_VAR" = "201" ] || [ "$T_VAR" = "200" ]; } \
  && step pass "T1.1 sink table object variant_events registered (payload variant — the A4 word through the console)" \
  || step fail "T1.1 table object" "http=$T_VAR body=$BODY"
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_events/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"event_id\",\"replicationNum\":1}" "$TOKEN"
VAR_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && grep -q '`payload` VARIANT' <<<"$VAR_DDL"; } \
  && step pass "T1.2 draft maps variant→VARIANT (A4 mapping in the draft text)" \
  || step fail "T1.2 draft mapping" "http=$CODE ddl=$(head -c 300 <<<"$VAR_DDL")"
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_events/ddl/execute" \
  "{\"ddl\":$(json_sql "$VAR_DDL"),\"keyModel\":\"duplicate\"}" "$TOKEN"
SHOW_VAR="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.variant_events;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
{ [ "$CODE" = "200" ] && grep -q '`payload` variant' <<<"$SHOW_VAR"; } \
  && step pass "T1.3 execute → live SHOW CREATE TABLE carries 'payload variant' (real Doris type, zero hand DDL)" \
  || step fail "T1.3 execute+show" "http=$CODE show=$(head -c 300 <<<"$SHOW_VAR")"
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_events/ddl/evolve" '{}' "$TOKEN"
# jq `// empty` swallows a literal false (the tp e2e's own first-run bug) —
# tostring keeps it
EV_CHANGED="$(jq -r '.changed | tostring' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ "${EV_CHANGED:-x}" = "false" ]; } \
  && step pass "T1.4 evolve reports changed=false — DESC folds the live variant column back to the 'variant' word (round-trip stable, no false retyped diff)" \
  || step fail "T1.4 evolve quiet" "http=$CODE changed=$EV_CHANGED body=$(head -c 200 <<<"$BODY")"
# variant as the KEY column: the generator must refuse (FE does; probe A3)
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_events/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"payload\",\"replicationNum\":1}" "$TOKEN"
{ [ "$CODE" = "400" ] && grep -qi 'variant' <<<"$BODY"; } \
  && step pass "T1.5 keyColumn=payload → 400 naming variant (the generator refuses a draft the FE would refuse; probe A3)" \
  || step fail "T1.5 variant key refused" "http=$CODE body=$(head -c 200 <<<"$BODY")"

# ---------- T2 CORE: file jsonl → doris VARIANT, sub-column assertions ----------
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"var_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_VAR}" "$TOKEN"
T_F1="$CODE"
{ [ "$T_F1" = "201" ] || [ "$T_F1" = "200" ]; } && step pass "T2.1 file table local.var_in (jsonl, payload variant)" \
  || step fail "T2.1 file table" "http=$T_F1"
# 8 rows: the canonical nested doc (all leaf kinds), an array root, a string
# payload, a 4-level deep doc, a null payload, an ABSENT payload, and two rows
# sharing the `payload.k` sub-column with CONFLICTING types (int vs string —
# Doris widens the sub-column to JSON, probe D).
cat > "$WORK/node/var_in" <<'EOF'
{"event_id":"E-1","payload":{"a":{"b":1,"c":"x"},"arr":[7,8,9],"s":"hello","f":1.5,"t":true,"n":null},"note":"full"}
{"event_id":"E-2","payload":[10,20,30],"note":"array-root"}
{"event_id":"E-3","payload":"plain string","note":"string-payload"}
{"event_id":"E-4","payload":{"l1":{"l2":{"l3":{"l4":"deep"}}}},"note":"deep"}
{"event_id":"E-5","payload":null,"note":"null-payload"}
{"event_id":"E-6","note":"absent-payload"}
{"event_id":"E-7","payload":{"k":100},"note":"k-int"}
{"event_id":"E-8","payload":{"k":"one hundred"},"note":"k-str"}
EOF
step pass "T2.2 seeds: 8 rows (nested all-leaf-kinds / array root / string / 4-level deep / null / absent / conflicting k types)"
if deploy_pipeline va-full variant_events var_in "event_id, payload, note"; then
  step pass "T2.3 pipeline full-column file→variant_events deployed → RUNNING"
else
  step fail "T2.3 pipeline variant_events" "deploy chain failed (see lines above)"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.variant_events;" 8)"
{ [ "${N:-x}" = "8" ]; } \
  && step pass "T2.4 COUNT=8/8 through the engine chain" \
  || step fail "T2.4 count" "n=$N node.log=$(tail -5 "$WORK/node.log" 2>/dev/null)"
# per-leaf sub-column assertions (CAST keeps string leaves comparable; the
# boolean leaf reads back as Doris 1 — probe B1; the array leaf is matched
# in BOTH version spellings, never as raw text)
LEAVES="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.variant_events WHERE event_id='E-1' AND payload['a']['b']=1 AND payload['a']['c']='x' AND CAST(payload['arr'] AS STRING) IN ('[7,8,9]','[7, 8, 9]') AND payload['s']='hello' AND payload['f']=1.5 AND payload['t']=1;")"
{ [ "${LEAVES:-x}" = "1" ]; } \
  && step pass "T2.5 CORE E-1: every leaf of the nested doc answers from the variant — a.b=1, a.c='x', arr=[7,8,9], s='hello', f=1.5, t→1 (n:null dropped server-side, probe B1); the JSON null leaf does NOT fail the row" \
  || step fail "T2.5 CORE nested leaves" "leaves=$LEAVES"
DEEP="$(doris_query "SELECT payload['l1']['l2']['l3']['l4'] FROM $E2E_DB.variant_events WHERE event_id='E-4';")"
{ [ "${DEEP:-x}" = "deep" ]; } \
  && step pass "T2.6 E-4 four-level path payload['l1']['l2']['l3']['l4']='deep'" \
  || step fail "T2.6 deep path" "deep=$DEEP"
NULLS="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.variant_events WHERE payload IS NULL;")"
STRP="$(doris_query "SELECT CAST(payload AS STRING) FROM $E2E_DB.variant_events WHERE event_id='E-3';")"
{ [ "${NULLS:-x}" = "2" ] && [ "${STRP:-x}" = '"plainstring"' ]; } \
  && step pass "T2.7 E-5/E-6 payload NULL (json null and absent alike), E-3 string payload stored as a string scalar" \
  || step fail "T2.7 nulls/scalars" "nulls=$NULLS strp=$STRP"
KINT="$(doris_query "SELECT CAST(payload['k'] AS STRING) FROM $E2E_DB.variant_events WHERE event_id='E-7';")"
KSTR="$(doris_query "SELECT CAST(payload['k'] AS STRING) FROM $E2E_DB.variant_events WHERE event_id='E-8';")"
# the widened sub-column's CAST spelling depends on how far Doris had to
# widen: int+string → STRING (bare `one hundred`, this table), int+string+
# float+object → JSON (quoted `"one hundred"`, probe D) — both spellings are
# the same fact (both rows answer), so both are accepted
{ [ "${KINT:-x}" = "100" ] && { [ "${KSTR:-x}" = "onehundred" ] || [ "${KSTR:-x}" = '"onehundred"' ]; }; } \
  && step pass "T2.8 conflicting sub-column types both answer (k=100 on E-7, k='one hundred' on E-8 — the widened sub-column; CAST spelling ${KSTR}, probe D)" \
  || step fail "T2.8 type conflict" "kint=$KINT kstr=$KSTR"

# ---------- T3 UNIQUE MoW × partial_columns through the engine ----------
COLS_UVAR='[["k","utf8"],["payload","variant"],["note","utf8"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"variant_mow\",\"format\":null,\"columnsJson\":$COLS_UVAR,\"keyModel\":\"unique\"}" "$TOKEN"
T_UVAR="$CODE"
{ [ "$T_UVAR" = "201" ] || [ "$T_UVAR" = "200" ]; } \
  && step pass "T3.1 table object variant_mow registered (UNIQUE, payload variant — probe A2 shape)" \
  || step fail "T3.1 mow table object" "http=$T_UVAR body=$BODY"
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_mow/ddl/draft" \
  "{\"keyModel\":\"unique\",\"keyColumn\":\"k\",\"replicationNum\":1}" "$TOKEN"
UVAR_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_mow/ddl/execute" \
  "{\"ddl\":$(json_sql "$UVAR_DDL"),\"keyModel\":\"unique\"}" "$TOKEN"
{ [ "$CODE" = "200" ]; } && step pass "T3.2 variant_mow created (draft → execute)" \
  || step fail "T3.2 mow create" "http=$CODE body=$BODY ddl=$(head -c 200 <<<"$UVAR_DDL")"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"uvar_full\",\"format\":\"jsonl\",\"columnsJson\":$COLS_UVAR}" "$TOKEN"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"uvar_name\",\"format\":\"jsonl\",\"columnsJson\":$COLS_UVAR}" "$TOKEN"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"uvar_payload\",\"format\":\"jsonl\",\"columnsJson\":$COLS_UVAR}" "$TOKEN"
printf '%s\n' '{"k":"K1","payload":{"a":{"b":1},"keep":"orig"},"note":"first"}' > "$WORK/node/uvar_full"
printf '%s\n' '{"k":"K1","note":"renamed"}' > "$WORK/node/uvar_name"
printf '%s\n' '{"k":"K1","payload":{"a":{"b":2},"new":"replaced"}}' > "$WORK/node/uvar_payload"
if deploy_pipeline va-mow-full variant_mow uvar_full "k, payload, note"; then
  step pass "T3.3 full-column pipeline → variant_mow RUNNING (K1 with the nested payload)"
else
  step fail "T3.3 mow full pipeline" "deploy chain failed"
fi
N="$(doris_wait_scalar "SELECT payload['keep'] FROM $E2E_DB.variant_mow WHERE k='K1';" orig)"
[ "${N:-x}" = "orig" ] && step pass "T3.4 first round in place (K1 payload.keep=orig via sub-column)" \
  || step fail "T3.4 mow first round" "keep=$N"
if deploy_pipeline va-mow-name variant_mow uvar_name "k, note" "k, note"; then
  step pass "T3.5 partial pipeline (k, note) deployed → RUNNING (variant column NOT in the write set)"
else
  step fail "T3.5 mow name partial" "deploy chain failed"
fi
KEPT="$(doris_wait_scalar "SELECT payload['keep'] FROM $E2E_DB.variant_mow WHERE k='K1' AND note='renamed';" orig)"
{ [ "${KEPT:-x}" = "orig" ]; } \
  && step pass "T3.6 CORE other-column partial update PRESERVES the variant value (keep=orig survived note=renamed)" \
  || step fail "T3.6 partial preserves variant" "kept=$KEPT"
if deploy_pipeline va-mow-payload variant_mow uvar_payload "k, payload" "k, payload"; then
  step pass "T3.7 partial pipeline (k, payload) deployed → RUNNING (variant column IS the write set)"
else
  step fail "T3.7 mow payload partial" "deploy chain failed"
fi
NEWK="$(doris_wait_scalar "SELECT payload['new'] FROM $E2E_DB.variant_mow WHERE k='K1';" replaced)"
OLDK="$(doris_query "SELECT payload['keep'] FROM $E2E_DB.variant_mow WHERE k='K1';")"
NOTE="$(doris_query "SELECT note FROM $E2E_DB.variant_mow WHERE k='K1';")"
{ [ "${NEWK:-x}" = "replaced" ] && [ "${OLDK:-NULL}" = "NULL" ] && [ "${NOTE:-x}" = "renamed" ]; } \
  && step pass "T3.8 CORE variant-column partial update replaces the value WHOLE (new=replaced, keep gone — no deep merge) and preserves note (probe C through the engine)" \
  || step fail "T3.8 variant whole replace" "new=$NEWK old=$OLDK note=$NOTE"

# ---------- T4 debezium-json source: after.payload nested → VARIANT ----------
COLS_DBZ='[["k","utf8"],["payload","variant"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"variant_dbz\",\"format\":null,\"columnsJson\":$COLS_DBZ}" "$TOKEN"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"dbz_in\",\"format\":\"debezium-json\",\"columnsJson\":$COLS_DBZ}" "$TOKEN"
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_dbz/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"k\",\"replicationNum\":1}" "$TOKEN"
DBZ_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/datasources/$DDS/tables/$E2E_DB/variant_dbz/ddl/execute" \
  "{\"ddl\":$(json_sql "$DBZ_DDL"),\"keyModel\":\"duplicate\"}" "$TOKEN"
{ [ "$CODE" = "200" ]; } && step pass "T4.1 sink variant_dbz created (draft → execute)" \
  || step fail "T4.1 dbz sink" "http=$CODE ddl=$(head -c 200 <<<"$DBZ_DDL")"
cat > "$WORK/node/dbz_in" <<'EOF'
{"before":null,"after":{"k":"D-1","payload":{"a":{"b":1},"src":"create"}},"op":"c","ts_ms":1}
{"before":null,"after":{"k":"D-2","payload":{"a":{"b":2},"src":"update"}},"op":"u","ts_ms":2}
{"before":{"k":"D-1"},"after":null,"op":"d","ts_ms":3}
EOF
step pass "T4.2 seeds: debezium c/u events with nested after.payload + one delete (skipped)"
if deploy_pipeline va-dbz variant_dbz dbz_in "k, payload"; then
  step pass "T4.3 pipeline file(debezium-json)→variant_dbz deployed → RUNNING"
else
  step fail "T4.3 dbz pipeline" "deploy chain failed"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.variant_dbz;" 2)"
B1="$(doris_query "SELECT payload['a']['b'] FROM $E2E_DB.variant_dbz WHERE k='D-1';")"
B2="$(doris_query "SELECT payload['a']['b'] FROM $E2E_DB.variant_dbz WHERE k='D-2';")"
SRC1="$(doris_query "SELECT payload['src'] FROM $E2E_DB.variant_dbz WHERE k='D-1';")"
{ [ "${N:-x}" = "2" ] && [ "${B1:-x}" = "1" ] && [ "${B2:-x}" = "2" ] && [ "${SRC1:-x}" = "create" ]; } \
  && step pass "T4.4 CORE debezium after.payload nested → VARIANT: D-1 a.b=1/src=create, D-2 a.b=2; the delete event skipped (COUNT=2) — the second decoder's passthrough" \
  || step fail "T4.4 dbz sub-columns" "n=$N b1=$B1 b2=$B2 src=$SRC1 node.log=$(tail -5 "$WORK/node.log" 2>/dev/null)"

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
echo "== variant e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
