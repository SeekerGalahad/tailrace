#!/usr/bin/env bash
# R7 T-A7 partial-column update e2e (blueprint §7.7 A7 row) — the live MoW
# proof the W2/W3 suites cannot give: "unwritten columns keep their old
# values" verified by SELECT on a real Doris, plus the Doris-side refusal
# matrix and the sequence-column round trip.
#
# 用法：
#   bash scripts/e2e/partial_update.sh [--help] [--keep] [--skip-up] [--wait SECS]
#
# 三层断言：
#   P 档（raw stream load，绕过 console/engine，钉死 Doris 服务端契约——
#     引擎形状的"为什么"在这里可复现；探针原始输出存
#     scripts/e2e/evidence/partial_update-probe-2026-10-28/）：
#     P1  UNIQUE MoW 表全列写 → partial+columns 头只写键列+1 列 → SELECT
#         逐行断言未写列保留第一轮值（正向契约，硬断言）；
#     P2  同载荷发给 DUPLICATE KEY 表 → Doris 拒绝 "Only unique key merge
#         on write support partial update"（非 MoW 拒绝路径）；
#     P3  观察（两可皆过、细节入日志）：partial 不带 columns 头 → 2.1.7
#         实测 Success 但未写列被置 NULL——引擎必须带 columns 头的原因；
#         若未来版本改语义，此步记录新行为而非误报失败。
#   T 档（console 全链，API 层）：
#     T1  A1 闭环建 orders_mow（UNIQUE+MoW，5 列）→ 全列管道写 10 行，
#         第一轮值逐行断言（note/price/flag 与 qty）；
#     T2  CORE：partial 管道 `INSERT INTO t (order_no, qty) SELECT …` 同键
#         写新 qty → SUM(qty) 45→1045，COUNT(*) 仍 10，逐行断言未写的
#         note/price/flag 仍是第一轮值（任务卡核心不可省断言）；
#     T3  evolve × partial 互斥（live）：partial 管道 RUNNING 时 evolve →
#         409 PARTIAL_UPDATE_ACTIVE（refs.pipelines 点名）；stop 后 →
#         200 changed:false + partialUpdatePipelines 重部署清单；
#     T4  非 UNIQUE 拒绝：DUPLICATE 表（A1 建、key_model=duplicate 落档）
#         上的 partial 管道 publish → 400 诊断含 keyModel=duplicate；
#         裸 INSERT 同表 publish 200（未声明=不受限，回归锚）；
#     T5  sequence 列在场一例：orders_seq 手工建（UNIQUE+MoW+
#         function_column.sequence_col=seq，注册 sequenceColumn=seq）→
#         全列管道 10 行 → partial 管道 (order_no,seq,qty) 写混合 seq：
#         偶数行 seq=20（新）落盘、奇数行 seq=5（旧）被丢弃，note 全保留
#         ——一次锁住"部分列 + 序列定序 + 旧值保留"三语义；
#     T6  引擎装配拒绝（W2 安全裕度）：subset 缺 seq 的 partial 管道
#         publish 200（console 门不管序列）→ deploy 502 ENGINE_REJECTED，
#         错误含 "sequence column seq must be inside the partial subset"
#         （晚失败摩擦记入 dogfooding 报告，反哺 R8 publish 门）。
#
# 覆盖变量（沿用 scripts/e2e/ddl_create.sh 的模式）：
#   TAILRACE_FE_MYSQL_PORT      FE mysql 宿主端口   默认 39030（外部模式 9030）
#   TAILRACE_BE_HTTP_PORT       BE webserver 宿主端口 默认 38040（外部 8040）
#   TAILRACE_E2E_DORIS_MYSQL    外部在役 Doris 的 mysql 客户端命令前缀
#                               （设了即跳过 compose up/teardown，端口切
#                               标准口，收尾改为专库 DROP FORCE，不碰常驻
#                               栈其余库；注意在役栈实测 3.1.4，P 档观察
#                               步在不同版本可能记录不同行为）
#   TAILRACE_E2E_DB             目标库             默认 tailrace_pu_e2e
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址   默认 127.0.0.1:18085
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址 默认 127.0.0.1:18105
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WAIT_SECS      同 --wait（默认 300）
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-pu-e2e
#
# 标志：--help / --keep / --skip-up / --wait SECS（语义同 ddl_create.sh）
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
E2E_DB="${TAILRACE_E2E_DB:-tailrace_pu_e2e}"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18085}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18105}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
WAIT_SECS="${TAILRACE_E2E_WAIT_SECS:-300}"
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-pu-e2e}"
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
# the Docker VM has ANY active swap (T11 F11) — neutralized for THIS stack
# only by raising the threshold past any swap-device count before the
# original entrypoint runs (R6 recipe; throwaway test data — the production
# concern behind the check does not apply here).
OVERRIDE_YML="$WORK/compose-be-swap-override.yml"
doris_compose() { docker compose -f "$ROOT/docker-compose.yml" -f "$OVERRIDE_YML" "$@"; }
doris_mysql_exec() { # standing-Doris prefix when TAILRACE_E2E_DORIS_MYSQL, else compose client
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
doris_wait_be_alive() { # heartbeat-driven Alive=true (T11 F6 family)
  local r out
  for r in $(seq 1 30); do
    out="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" 2>/dev/null)"
    [ "$(awk -F'\t' '$10=="true"{n++} END{print n+0}' <<<"$out")" -ge 1 ] && return 0
    sleep 2
  done
  return 1
}
# raw stream load straight at the BE (the P tier bypasses console/engine on
# purpose: it pins the SERVER contract the engine's request shape relies on)
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
#   → sets P_ID; the column list (5th arg) is the A7 statement-level switch
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

echo "== partial_update e2e: work=$WORK fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT db=$E2E_DB console=$CONSOLE_ADDR =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable — partial_update e2e needs the compose doris stack (or TAILRACE_E2E_DORIS_MYSQL), skipped"
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
# Generated by scripts/e2e/partial_update.sh — e2e-only, never a committed
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
# P tables are hand-DDLed ON PURPOSE: this tier pins what Doris itself does
# with these headers, independent of console/engine (the evidence files under
# scripts/e2e/evidence/partial_update-probe-2026-10-28/ are the original
# first-run outputs of this matrix).
doris_exec 3 "CREATE TABLE $E2E_DB.probe_mow (order_no VARCHAR(65533) NOT NULL, qty BIGINT, note VARCHAR(65533), price DOUBLE, flag BOOLEAN) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");" \
  && step pass "P0.1 probe_mow created (UNIQUE KEY + merge-on-write, 5 columns)" \
  || step fail "P0.1 probe_mow" "create failed"
for i in 0 1; do printf '{"order_no":"P-%d","qty":%d,"note":"first","price":%d.5,"flag":true}\n' "$i" "$i" "$i"; done > "$WORK/probe_full.jsonl"
R="$(raw_load probe_mow "pu_p_full_$$" "$WORK/probe_full.jsonl")"
echo "$R" | grep -q '"Status": *"Success"' \
  && step pass "P0.2 probe full-column load (2 rows, first-round values)" \
  || step fail "P0.2 probe full load" "$(head -c 200 <<<"$R")"
printf '{"order_no":"P-0","qty":100}\n' > "$WORK/probe_delta.jsonl"
R="$(raw_load probe_mow "pu_p_part_$$" "$WORK/probe_delta.jsonl" -H "partial_columns: true" -H "columns: order_no,qty")"
KEEPNOTE="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.probe_mow WHERE note='first' AND price=0.5 AND flag=1;")"
KEEPQTY="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.probe_mow WHERE qty=100;")"
{ echo "$R" | grep -q '"Status": *"Success"' && [ "$KEEPNOTE" = "1" ] && [ "$KEEPQTY" = "1" ]; } \
  && step pass "P1.1 partial + columns header: qty updated, note/price/flag kept first-round values (the contract the engine rides)" \
  || step fail "P1.1 positive contract" "keepnote=$KEEPNOTE keepqty=$KEEPQTY resp=$(head -c 200 <<<"$R")"
doris_exec 3 "CREATE TABLE $E2E_DB.probe_dup (order_no VARCHAR(65533) NOT NULL, qty BIGINT, note VARCHAR(65533)) DUPLICATE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" \
  && step pass "P2.0 probe_dup created (DUPLICATE KEY)" \
  || step fail "P2.0 probe_dup" "create failed"
R="$(raw_load probe_dup "pu_p_dup_$$" "$WORK/probe_delta.jsonl" -H "partial_columns: true" -H "columns: order_no,qty")"
{ echo "$R" | grep -q '"Status": *"Fail"' && echo "$R" | grep -q "Only unique key merge on write support partial update"; } \
  && step pass "P2.1 DUPLICATE KEY partial load refused: \"Only unique key merge on write support partial update\" (Doris-side non-UNIQUE rejection)" \
  || step fail "P2.1 non-MoW refusal" "$(head -c 250 <<<"$R")"
printf '{"order_no":"P-1","qty":200}\n' > "$WORK/probe_delta2.jsonl"
R="$(raw_load probe_mow "pu_p_nocol_$$" "$WORK/probe_delta2.jsonl" -H "partial_columns: true")"
KEEPNOTE="$(doris_query "SELECT note FROM $E2E_DB.probe_mow WHERE order_no='P-1';")"
if echo "$R" | grep -q '"Status": *"Success"'; then
  # version-dependent observation, not a contract: 2.1.7 resolves the written
  # set to EVERY table column and NULLs the absent ones — the exact trap the
  # engine's mandatory `columns` header exists to avoid (05f2564)
  step pass "P3.1 observation: header-less partial load accepted; unlisted note after it = ${KEEPNOTE:-<none>} (2.1.7: NULL — the trap; recorded, not asserted)"
else
  step pass "P3.1 observation: header-less partial load REFUSED by this server version: $(head -c 120 <<<"$R")"
fi

# ---------- S3 console boot + auth + datasources ----------
TAILRACE_BOOTSTRAP_TOKEN="tr_b_pu_e2e_00112233445566778899aabbccddeeff" \
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
  "{\"name\":\"pu-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST /api/datasources "{\"name\":\"pu-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$DDS" ] && [ -n "$FDS" ]; } && step pass "S3.3 doris+file datasources (d=$DDS f=$FDS)" \
  || step fail "S3.3 datasources" "dds=$DDS fds=$FDS body=$BODY"

# ---------- S4 table objects + A1 creation ----------
COLS5='[["order_no","utf8"],["qty","int64"],["note","utf8"],["price","float64"],["flag","bool"]]'
COLS2='[["order_no","utf8"],["qty","int64"]]'
COLS4SEQ='[["order_no","utf8"],["seq","int64"],["qty","int64"],["note","utf8"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders_mow\",\"format\":null,\"columnsJson\":$COLS5}" "$TOKEN"
T_MOW="$CODE"
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders_dup\",\"format\":null,\"columnsJson\":$COLS2}" "$TOKEN"
T_DUP="$CODE"
# orders_seq exists in Doris already (hand-DDLed below — sequence_col is a
# table property the A1 generator does not emit; registered here WITH the
# sequenceColumn + keyModel declarations the plan/publish gates read)
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders_seq\",\"format\":null,\"columnsJson\":$COLS4SEQ,\"sequenceColumn\":\"seq\",\"keyModel\":\"unique\"}" "$TOKEN"
T_SEQ="$CODE"
{ [ "$T_MOW" = "201" ] || [ "$T_MOW" = "200" ]; } && { [ "$T_DUP" = "201" ] || [ "$T_DUP" = "200" ]; } && { [ "$T_SEQ" = "201" ] || [ "$T_SEQ" = "200" ]; } \
  && step pass "S4.1 sink table objects: orders_mow(5列) + orders_dup + orders_seq(sequenceColumn=seq, keyModel=unique)" \
  || step fail "S4.1 sink table objects" "mow=$T_MOW dup=$T_DUP seq=$T_SEQ body=$BODY"

# A1 loop for orders_mow: draft(确认 unique) → execute (key_model auto-recorded)
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_mow/ddl/draft" \
  "{\"keyModel\":\"unique\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
MOW_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_mow/ddl/execute" \
  "{\"ddl\":$(json_sql "$MOW_DDL"),\"keyModel\":\"unique\"}" "$TOKEN"
SHOW_MOW="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.orders_mow;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
{ [ "$CODE" = "200" ] && grep -q 'unique key(`order_no`)' <<<"$SHOW_MOW" && grep -q 'enable_unique_key_merge_on_write' <<<"$SHOW_MOW"; } \
  && step pass "S4.2 A1 loop created orders_mow: UNIQUE KEY(order_no) + merge-on-write (key_model recorded by execute)" \
  || step fail "S4.2 orders_mow create" "http=$CODE show=$(head -c 160 <<<"$SHOW_MOW")"
# A1 loop for orders_dup (duplicate)
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_dup/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
DUP_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_dup/ddl/execute" \
  "{\"ddl\":$(json_sql "$DUP_DDL"),\"keyModel\":\"duplicate\"}" "$TOKEN"
SHOW_DUP="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.orders_dup;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
{ [ "$CODE" = "200" ] && grep -q 'duplicate key(`order_no`)' <<<"$SHOW_DUP"; } \
  && step pass "S4.3 A1 loop created orders_dup: DUPLICATE KEY (key_model=duplicate recorded)" \
  || step fail "S4.3 orders_dup create" "http=$CODE show=$(head -c 160 <<<"$SHOW_DUP")"
# orders_seq: hand DDL (function_column.sequence_col is a property the A1
# generator does not emit — the A1 path leaves it a plain column, in which
# case Doris silently ignores the load-time sequence header; dogfooding
# report friction #3). Registration above carries the metadata.
doris_exec 3 "CREATE TABLE $E2E_DB.orders_seq (order_no VARCHAR(65533) NOT NULL, seq BIGINT, qty BIGINT, note VARCHAR(65533)) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\",\"function_column.sequence_col\"=\"seq\");" \
  && step pass "S4.4 orders_seq hand-created: UNIQUE+MoW+function_column.sequence_col=seq (table-level sequence property)" \
  || step fail "S4.4 orders_seq create" "create failed"

# ---------- S5 file tables + deterministic seeds ----------
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"full_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS5}" "$TOKEN"
T_F1="$CODE"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"delta_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS2}" "$TOKEN"
T_F2="$CODE"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"seq_full_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS4SEQ}" "$TOKEN"
T_F3="$CODE"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"seq_delta_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS4SEQ}" "$TOKEN"
T_F4="$CODE"
{ [ "$T_F1" = "201" ] || [ "$T_F1" = "200" ]; } && { [ "$T_F2" = "201" ] || [ "$T_F2" = "200" ]; } && { [ "$T_F3" = "201" ] || [ "$T_F3" = "200" ]; } && { [ "$T_F4" = "201" ] || [ "$T_F4" = "200" ]; } \
  && step pass "S5.1 file table objects: full_in/delta_in/seq_full_in/seq_delta_in (jsonl)" \
  || step fail "S5.1 file tables" "f1=$T_F1 f2=$T_F2 f3=$T_F3 f4=$T_F4"
# full_in: 10 rows, first-round values — qty=i, note=first-i, price=i+0.5, flag=i%2==0
for i in $(seq 0 9); do printf '{"order_no":"PU-%03d","qty":%d,"note":"first-%03d","price":%d.5,"flag":%s}\n' "$i" "$i" "$i" "$i" "$([ $((i % 2)) -eq 0 ] && echo true || echo false)"; done > "$WORK/node/full_in"
# delta_in: same 10 keys, partial payload (order_no, qty) with qty=100+i
for i in $(seq 0 9); do printf '{"order_no":"PU-%03d","qty":%d}\n' "$i" "$((100 + i))"; done > "$WORK/node/delta_in"
# seq_full_in: 10 rows at seq=10
for i in $(seq 0 9); do printf '{"order_no":"SQ-%03d","seq":10,"qty":%d,"note":"first-%03d"}\n' "$i" "$i" "$i"; done > "$WORK/node/seq_full_in"
# seq_delta_in: even rows carry seq=20 (newer, qty=200+i), odd rows seq=5 (older, qty=500+i → must be discarded)
for i in $(seq 0 9); do
  if [ $((i % 2)) -eq 0 ]; then printf '{"order_no":"SQ-%03d","seq":20,"qty":%d}\n' "$i" "$((200 + i))"
  else printf '{"order_no":"SQ-%03d","seq":5,"qty":%d}\n' "$i" "$((500 + i))"; fi
done > "$WORK/node/seq_delta_in"
step pass "S5.2 seeds: full_in(10×5列) delta_in(10×2列) seq_full_in(10) seq_delta_in(10, 混合 seq)"

# ---------- S6 engine node ----------
( cd "$WORK/node" && exec env "TAILRACE_DS_$DDS=Basic cm9vdDo=" \
    "$NODE_BIN" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "tr_b_pu_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 20 \
  && step pass "S6.1 engine node up (serve)" \
  || step fail "S6.1 node boot" "$(tail -3 "$WORK/node.log")"
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "S6.2 node ONLINE" || step fail "S6.2 node ONLINE" "node.log=$(tail -5 "$WORK/node.log")"

# ---------- T1 full-column round (first-round values) ----------
if deploy_pipeline pu-full orders_mow full_in "order_no, qty, note, price, flag"; then
  step pass "T1.1 pipeline#1 full-column file→orders_mow deployed → RUNNING"
else
  step fail "T1.1 pipeline orders_mow full" "deploy chain failed (see lines above)"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.orders_mow;" 10)"
S="$(doris_query "SELECT SUM(qty) FROM $E2E_DB.orders_mow;")"
R1="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_mow WHERE qty = CAST(SUBSTRING(order_no, 4, 3) AS INT) AND note = CONCAT('first-', SUBSTRING(order_no, 4, 3)) AND ABS(price - (CAST(SUBSTRING(order_no, 4, 3) AS INT) + 0.5)) < 0.001 AND flag = (CAST(SUBSTRING(order_no, 4, 3) AS INT) % 2 = 0);")"
{ [ "${N:-x}" = "10" ] && [ "${S:-x}" = "45" ] && [ "${R1:-x}" = "10" ]; } \
  && step pass "T1.2 first round in place: 10 rows, SUM(qty)=45, per-row note/price/flag match the seed (the baseline T2 must preserve)" \
  || step fail "T1.2 first-round values" "n=$N sum=$S perrow=$R1"

# ---------- T2 CORE: partial round keeps the unwritten columns ----------
if deploy_pipeline pu-part orders_mow delta_in "order_no, qty" "order_no, qty"; then
  step pass "T2.1 pipeline#2 'INSERT INTO orders_mow (order_no, qty)' deployed → RUNNING (partial mode)"
else
  step fail "T2.1 pipeline orders_mow partial" "deploy chain failed (see lines above)"
fi
# the partial flush lands 100+i on every key: 45 → 1045
S="$(doris_wait_scalar "SELECT SUM(qty) FROM $E2E_DB.orders_mow;" 1045)"
N="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_mow;")"
NEWQ="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_mow WHERE qty = 100 + CAST(SUBSTRING(order_no, 4, 3) AS INT);")"
# THE assertion: the three unwritten columns still carry their FIRST-round
# values, row by row — "partial 语义真验而非虚账"
KEPT="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_mow WHERE note = CONCAT('first-', SUBSTRING(order_no, 4, 3)) AND ABS(price - (CAST(SUBSTRING(order_no, 4, 3) AS INT) + 0.5)) < 0.001 AND flag = (CAST(SUBSTRING(order_no, 4, 3) AS INT) % 2 = 0);")"
{ [ "${S:-x}" = "1045" ] && [ "${N:-x}" = "10" ] && [ "${NEWQ:-x}" = "10" ] && [ "${KEPT:-x}" = "10" ]; } \
  && step pass "T2.2 CORE: partial round wrote qty=100+i on all 10 keys (SUM 45→1045), COUNT(*) still 10, and all 3 unwritten columns (note/price/flag) kept their first-round values on every row" \
  || step fail "T2.2 CORE retention" "sum=$S n=$N newqty=$NEWQ kept=$KEPT (expected 1045/10/10/10)"

# ---------- T3 evolve × partial mutex (live) ----------
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_mow/ddl/evolve" "" "$TOKEN"
P_PART="$P_ID"
E409_HIT="$(jq -r --argjson p "$P_PART" '[.error.refs.pipelines[]? | select(.id==$p)] | length' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "409" ] && [ "$(jq -r '.error.code // empty' <<<"$BODY")" = "PARTIAL_UPDATE_ACTIVE" ] \
    && [ "${E409_HIT:-0}" = "1" ] \
    && [ "$(jq -r --argjson p "$P_PART" '.error.refs.pipelines[]? | select(.id==$p) | .deployed' <<<"$BODY")" = "true" ]; } \
  && step pass "T3.1 evolve on orders_mow while the partial pipeline is RUNNING → 409 PARTIAL_UPDATE_ACTIVE naming it (refs.pipelines), nothing executed" \
  || step fail "T3.1 evolve mutex 409" "http=$CODE hit=$E409_HIT body=$BODY"
req POST "/api/pipelines/$P_PART/stop" "" "$TOKEN"
STOP_OK="$CODE"
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_mow/ddl/evolve" "" "$TOKEN"
# no `// empty` on the boolean: jq's alternative operator swallows `false`,
# which is exactly the value asserted here (ddl_create.sh T1.1 lesson)
EV_CHANGED="$(jq -r '.changed' <<<"$BODY" 2>/dev/null)"
EV_HIT="$(jq -r --argjson p "$P_PART" '[.partialUpdatePipelines[]? | select(.id==$p)] | length' <<<"$BODY" 2>/dev/null)"
{ [ "$STOP_OK" = "200" ] && [ "$CODE" = "200" ] && [ "$EV_CHANGED" = "false" ] && [ "${EV_HIT:-0}" = "1" ]; } \
  && step pass "T3.2 stop the pipeline → evolve 200 changed:false (nothing to add) + partialUpdatePipelines redeploy checklist names it" \
  || step fail "T3.2 evolve after stop" "stop=$STOP_OK http=$CODE changed=$EV_CHANGED hit=$EV_HIT body=$BODY"

# ---------- T4 non-UNIQUE refusal (console publish gate + regression anchor) ----------
req POST /api/pipelines \
  "{\"name\":\"pu-dup-partial\",\"sqlContent\":$(json_sql "INSERT INTO $E2E_DB.orders_dup (order_no, qty) SELECT order_no, qty FROM local.delta_in"),\"dsMapping\":{\"local\":$FDS,\"$E2E_DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
P_DUP="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/pipelines/$P_DUP/publish" "" "$TOKEN"
GATE_TEXT="$(jq -r '.diagnostics // [] | map(.message) | join(" | ")' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "400" ] && grep -q 'keyModel=duplicate' <<<"$GATE_TEXT" && grep -q '部分列更新' <<<"$GATE_TEXT"; } \
  && step pass "T4.1 partial pipeline on the DUPLICATE table: publish → 400, diagnostic names keyModel=duplicate (console gate fires before Doris ever could)" \
  || step fail "T4.1 non-unique refusal" "http=$CODE gate=$GATE_TEXT body=$BODY"
req POST /api/pipelines \
  "{\"name\":\"pu-dup-plain\",\"sqlContent\":$(json_sql "INSERT INTO $E2E_DB.orders_dup SELECT order_no, qty FROM local.delta_in"),\"dsMapping\":{\"local\":$FDS,\"$E2E_DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
P_PLAIN="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/pipelines/$P_PLAIN/publish" "" "$TOKEN"
[ "$CODE" = "200" ] \
  && step pass "T4.2 regression anchor: bare INSERT on the same DUPLICATE table publishes untouched (no declaration = no gate)" \
  || step fail "T4.2 regression anchor" "http=$CODE body=$BODY"

# ---------- T5 sequence column in play ----------
if deploy_pipeline pu-seq-full orders_seq seq_full_in "order_no, seq, qty, note"; then
  step pass "T5.1 pipeline#5 full write → orders_seq deployed → RUNNING (sequence_column header rides every load)"
else
  step fail "T5.1 pipeline orders_seq full" "deploy chain failed (see lines above)"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.orders_seq;" 10)"
S="$(doris_query "SELECT SUM(qty) FROM $E2E_DB.orders_seq;")"
{ [ "${N:-x}" = "10" ] && [ "${S:-x}" = "45" ]; } \
  && step pass "T5.2 orders_seq first round: 10 rows at seq=10, SUM(qty)=45" \
  || step fail "T5.2 orders_seq first round" "n=$N sum=$S"
if deploy_pipeline pu-seq-part orders_seq seq_delta_in "order_no, seq, qty" "order_no, seq, qty"; then
  step pass "T5.3 pipeline#6 'INSERT INTO orders_seq (order_no, seq, qty)' deployed → RUNNING (partial × sequence)"
else
  step fail "T5.3 pipeline orders_seq partial" "deploy chain failed (see lines above)"
fi
# even rows: seq=20 wins (qty=200+i); odd rows: seq=5 loses to the stored seq=10
# (qty stays i); note — written by NEITHER partial load — must survive both.
# SUM: (200+202+204+206+208) + (1+3+5+7+9) = 1020 + 25 = 1045
S="$(doris_wait_scalar "SELECT SUM(qty) FROM $E2E_DB.orders_seq;" 1045)"
N="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq;")"
S20="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq WHERE seq=20;")"
S10="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq WHERE seq=10;")"
SEQKEPT="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.orders_seq WHERE note = CONCAT('first-', SUBSTRING(order_no, 4, 3)) AND ((CAST(SUBSTRING(order_no, 4, 3) AS INT) % 2 = 0 AND seq = 20 AND qty = 200 + CAST(SUBSTRING(order_no, 4, 3) AS INT)) OR (CAST(SUBSTRING(order_no, 4, 3) AS INT) % 2 = 1 AND seq = 10 AND qty = CAST(SUBSTRING(order_no, 4, 3) AS INT)));")"
{ [ "${S:-x}" = "1045" ] && [ "${N:-x}" = "10" ] && [ "${S20:-x}" = "5" ] && [ "${S10:-x}" = "5" ] && [ "${SEQKEPT:-x}" = "10" ]; } \
  && step pass "T5.4 CORE sequence round: 5 newer-seq rows landed (seq=20, qty=200+i), 5 older-seq rows discarded (seq=10 kept, qty unchanged), and the unwritten note survived all 10 rows — partial + sequence ordering + retention in one load" \
  || step fail "T5.4 sequence semantics" "sum=$S n=$N seq20=$S20 seq10=$S10 kept=$SEQKEPT (expected 1045/10/5/5/10)"

# ---------- T6 engine assembly refusal: subset lacking the sequence column ----------
req POST /api/pipelines \
  "{\"name\":\"pu-seq-noseq\",\"sqlContent\":$(json_sql "INSERT INTO $E2E_DB.orders_seq (order_no, qty) SELECT order_no, qty FROM local.seq_delta_in"),\"dsMapping\":{\"local\":$FDS,\"$E2E_DB\":$DDS},\"checkpointMs\":2000}" "$TOKEN"
P_NOSEQ="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST "/api/pipelines/$P_NOSEQ/publish" "" "$TOKEN"
PUB_NOSEQ="$CODE"
req POST "/api/pipelines/$P_NOSEQ/deploy" '{}' "$TOKEN"
DEPLOY_CODE="$CODE"; DEPLOY_BODY="$BODY"
req GET "/api/pipelines/$P_NOSEQ" "" "$TOKEN"
NOSEQ_STATUS="$(jq -r '.status // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$PUB_NOSEQ" = "200" ] && [ "$DEPLOY_CODE" = "502" ] && [ "$(jq -r '.error.code // empty' <<<"$DEPLOY_BODY")" = "ENGINE_REJECTED" ] \
    && grep -q 'sequence column seq must be inside the partial subset' <<<"$DEPLOY_BODY" \
    && [ "$NOSEQ_STATUS" != "RUNNING" ]; } \
  && step pass "T6.1 subset without the sequence column: publish passes (console gate does not know sequence membership) but deploy → 502 ENGINE_REJECTED carrying the engine's assembly refusal (pipeline $NOSEQ_STATUS) — the safety margin fires before any load (late-failure friction logged for R8)" \
  || step fail "T6.1 engine assembly refusal" "pub=$PUB_NOSEQ deploy=$DEPLOY_CODE status=$NOSEQ_STATUS body=$DEPLOY_BODY"

# ---------- teardown ----------
[ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
[ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
NODE_PID=""; CONSOLE_PID=""
# surgical teardown: stop+rm ONLY the two doris services (a whole-project
# compose down is forbidden — resident kafka); external mode drops the
# dedicated e2e database and leaves the standing stack running
if [ -n "$DORIS_MYSQL" ]; then
  doris_exec 3 "DROP DATABASE IF EXISTS $E2E_DB FORCE;" \
    && step pass "S7.1 external doris cleanup: DROP DATABASE $E2E_DB FORCE (standing stack untouched)" \
    || step fail "S7.1 external cleanup" "drop failed"
else
  doris_compose stop doris-fe doris-be >/dev/null 2>&1 && doris_compose rm -f doris-fe doris-be >/dev/null 2>&1 \
    && step pass "S7.1 doris-fe+doris-be stopped+removed (surgical — compose kafka untouched)" \
    || step fail "S7.1 teardown" "compose stop/rm doris-fe doris-be failed"
fi

TOTAL=$((PASS+FAIL))
echo "== partial_update e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
