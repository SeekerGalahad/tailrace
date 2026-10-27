#!/usr/bin/env bash
# R6 T-A1 DDL full-chain e2e (blueprint §7.2 / §7.7 A1 row) — the live
# complement of the datasources_ddl.rs contract tests: an EMPTY database goes
# from registered columns to rows in Doris with zero hand-written DDL, through
# the four-step console close loop (draft → human key-model confirm →
# execute → audit), then a file→doris pipeline lands data in the generated
# tables and the row counts reconcile.
#
# 用法：
#   bash scripts/e2e/ddl_create.sh [--help] [--keep] [--skip-up] [--wait SECS]
#
# 默认全流程（compose 栈 = docker-compose.yml 的 doris-fe+doris-be 两个服务，
#   服务名点名拉起，不带动 kafka——kafka 服务与本 e2e 无关）：
#     compose up -d --wait doris-fe doris-be（--wait SECS 覆盖 compose
#     --wait-timeout，默认 300s）→ console boot（全新 data-dir，首启 seed
#     admin，从 stderr 抓一次性初始密码——smoke.sh S1 先例）→ login →
#   A1 四步闭环 ×2 例：
#     例1 DUPLICATE：注册列 [["order_no","utf8"],["qty","int64"]] →
#        draft（空 body 第一轮：启发推荐 duplicate + keyColumnGuessed=true
#        「猜测」标注）→ execute 缺 keyModel 的 422 负例 → execute
#        {ddl 回传, keyModel:duplicate}（replicationNum=1——单 BE 栈，默认
#        3 副本建表会挂，调用方探测 SHOW BACKENDS 的契约由 e2e 直接给 1）
#        → SHOW CREATE TABLE 断言 DUPLICATE KEY；
#     例2 UNIQUE：同一份注册列，人工在 draft 第二轮改选 keyModel=unique
#        （「确认键模型」的人工环节——无 CDC 列时启发推荐 duplicate，用户
#        明确改选 unique 是 422 门防住的另一半语义）→ execute → SHOW CREATE
#        TABLE 断言 UNIQUE KEY + enable_unique_key_merge_on_write；
#   file→doris 管道 ×2（两表各一条 pipeline，file 源共享 orders_in）：
#     node serve 自注册 → pipeline create/publish/deploy → RUNNING →
#     10 行 jsonl 落两表 → COUNT(*) 对账（UNIQUE 表 order_no 全唯一 → 同样
#     10 行，merge-on-write 无折叠）；
#   审计断言：console.db（SQLite）audit_log 恰好 2 行 TABLE_DDL_EXECUTE，
#     keyModel 一 duplicate 一 unique，ddl 全文在 detail_json 里。
#
# 覆盖变量（沿用 docker-compose.yml / smoke.sh 的覆盖模式）：
#   TAILRACE_FE_MYSQL_PORT      FE mysql 宿主端口（DDL/查询）  默认 39030（外部模式 9030）
#   TAILRACE_BE_HTTP_PORT       BE webserver 宿主端口（stream load）默认 38040（外部 8040）
#   TAILRACE_E2E_DORIS_MYSQL    外部在役 Doris 的 mysql 客户端命令前缀（如
#                               "docker exec -i doris-2.1 mysql -h127.0.0.1
#                               -P9030 -uroot"）——smoke.sh SMOKE_DORIS_MYSQL
#                               先例：改打常驻栈（免起 compose 栈、更快更省，
#                               3.1.4 无 BE swap 硬检查）。设了即跳过 compose
#                               up/teardown，端口默认切标准口，收尾改为专库
#                               DROP FORCE（不触碰常驻栈其余库）；专用库名避免
#                               与在役数据相撞。compose 模式自身已通过 e2e 期
#                               override 中和 BE swap 硬检查（见 S0.5），两种
#                               模式等价可用
#   TAILRACE_E2E_DB             目标库（空库断言与对账范围）    默认 tailrace_ddl_e2e
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址                默认 127.0.0.1:18081
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址            默认 127.0.0.1:18101
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制路径
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WAIT_SECS      同 --wait（默认 300）
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-ddl-e2e
#
# 标志：
#   --help      本说明（无需 Docker、无需引擎二进制）
#   --keep      成功后也保留工作目录（失败必留作尸检）
#   --skip-up   复用已在跑的 doris 栈（跳过 compose up --wait；先探 FE 端口，
#               不可达按 live-gate 空过；库先 DROP FORCE 保证空库断言）
#   --wait SECS compose 健康等待预算（--wait-timeout）
#
# live-gate（对齐 scripts/e2e/starrocks.sh 惯例）：Docker daemon 不可达、
#   --skip-up 下 FE 端口不通、或宿主可用磁盘 <5GiB（AGENTS 容器红线——磁盘
#   不足会连坐 OrbStack，R6 蓝图风险表明示 <5Gi 推迟 e2e 波次）时，打印一行
#   [skip] 说明后 exit 0 空过——不算失败。
#
# 幂等：每次跑用全新 console data-dir + DROP DATABASE FORCE 重建空库 + 表名/
#   库名固定（CREATE 语义对着空库）；doris 栈拆除是外科式的（stop+rm 仅
#   doris-fe/doris-be 两个服务——本项目 compose 常驻 kafka 不被波及，整体
#   down 被本仓 e2e 纪律禁止）。
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
E2E_DB="${TAILRACE_E2E_DB:-tailrace_ddl_e2e}"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18081}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18101}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
WAIT_SECS="${TAILRACE_E2E_WAIT_SECS:-300}"
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-ddl-e2e}"
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
# the Docker VM has ANY active swap (T11 F11) — exit 1 before the process
# comes up, laundered into a clean container exit(0). Dev/CI boxes keep swap
# on for good reasons (a shared VM's standing stacks rely on it), and turning
# it off globally just for a test is not ours to do; the check is instead
# neutralized for THIS stack only, by raising its threshold past any possible
# swap-device count before the original entrypoint runs. Test-stack scope,
# throwaway data — the production concern behind the check (BE self-managed
# memory + swap) does not apply here.
OVERRIDE_YML="$WORK/compose-be-swap-override.yml"
doris_compose() { docker compose -f "$ROOT/docker-compose.yml" -f "$OVERRIDE_YML" "$@"; }
# mysql-client bridge: the standing-Doris prefix when TAILRACE_E2E_DORIS_MYSQL
# is set, otherwise the compose doris-fe's in-container client
doris_mysql_exec() { # doris_mysql_exec <mysql client args...>
  if [ -n "$DORIS_MYSQL" ]; then $DORIS_MYSQL "$@"; else doris_compose exec -T doris-fe mysql "$@"; fi
}
# DDL/query with retry — right after --wait the BE may still be finishing
# registration, and mysql -e can print a statement error yet exit 0 (T11
# first-run finding: assert on SHOW, not the exit code alone — smoke.sh
# doris_exec precedent)
doris_exec() { # doris_exec <retries> <sql...>
  local r
  for r in $(seq 1 "${1:-6}"); do
    doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${2}" && return 0
    sleep 5
  done
  return 1
}
doris_query() { # doris_query <sql> → single scalar (mysql -N, whitespace-stripped)
  doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${1}" 2>/dev/null | tr -d '[:space:]'
}
doris_wait_rows() { # doris_wait_rows <table> <expected> — poll COUNT(*) until equal
  local r n
  for r in $(seq 1 30); do
    n="$(doris_query "SELECT COUNT(*) FROM $E2E_DB.$1;")"
    [ "${n:-}" = "$2" ] && { echo "$n"; return 0; }
    sleep 2
  done
  echo "${n:-}"
}
# Wait for at least one backend Alive=true: the compose healthcheck only
# greps the backend row in `show backends` (registered), but DDL/stream load
# need "available backend num ≥ 1" — the heartbeat-driven Alive flag lags the
# healthcheck by seconds (T11 first-run finding F6 family; first live run of
# this script hit it as a race between --wait healthy and ddl/execute).
# -F'\t' is mandatory: LastStartTime carries a space and awk's default FS
# would split it, shifting Alive off $10.
doris_wait_be_alive() {
  local r out
  for r in $(seq 1 30); do
    out="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" 2>/dev/null)"
    [ "$(awk -F'\t' '$10=="true"{n++} END{print n+0}' <<<"$out")" -ge 1 ] && return 0
    sleep 2
  done
  return 1
}

# HTTP helper → sets BODY (response body) and CODE (http status) — smoke.sh req
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
json_sql() { printf '%s' "$1" | jq -Rs .; } # embed text into a JSON string literal

echo "== ddl_create e2e: work=$WORK fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT db=$E2E_DB console=$CONSOLE_ADDR =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required (audit_log assertion)"; exit 2; }

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable — ddl_create e2e needs the compose doris stack, skipped"
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
# Generated by scripts/e2e/ddl_create.sh — e2e-only, never a committed default.
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
# BE also hard-requires vm.max_map_count >= 2000000 — an OrbStack/Docker VM
# sysctl that resets on VM restart. Raising it is harmless (a kernel limit on
# memory-map count, not a resource allocation), so the e2e brings it up
# itself via a privileged helper instead of failing with a laundered exit(0).
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

# ---------- S1 doris stack (fe+be only — kafka untouched) ----------
if [ -n "$DORIS_MYSQL" ]; then
  step pass "S1.1 external standing doris via TAILRACE_E2E_DORIS_MYSQL (compose skipped; fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT)"
elif [ "$SKIP_UP" = "1" ]; then
  step pass "S1.1 doris stack reused (--skip-up; FE 127.0.0.1:$FE_PORT reachable)"
else
  echo "  starting compose doris-fe+doris-be (first pull may take minutes; --wait-timeout ${WAIT_SECS}s)"
  if doris_compose up -d --wait --wait-timeout "$WAIT_SECS" doris-fe doris-be; then
    step pass "S1.1 docker compose doris-fe+doris-be up --wait healthy (kafka not started)"
  else
    # detached up swallows container stdout; the doris entrypoint launders BE
    # start failures (swap/max_map_count/nofile) into clean exit(0) — without
    # this dump the failure is undiagnosable (smoke.sh S8.1 precedent)
    doris_compose logs --tail 50 doris-be doris-fe || true
    step fail "S1.1 compose up --wait" "see logs above (image pull / BE swap check)"
  fi
fi

# ---------- S2 empty database (idempotent: DROP FORCE + CREATE) ----------
if doris_wait_be_alive; then
  step pass "S2.0 backend Alive=true (heartbeat-driven availability, not just registered)"
else
  step fail "S2.0 backend alive" "no Alive=true backend in SHOW BACKENDS after 30×2s"
fi
if doris_exec 6 "DROP DATABASE IF EXISTS $E2E_DB FORCE; CREATE DATABASE $E2E_DB;" \
   && [ -z "$(doris_query "SHOW TABLES FROM $E2E_DB;")" ]; then
  step pass "S2.1 database $E2E_DB empty (DROP FORCE + CREATE, 0 tables)"
else
  step fail "S2.1 empty database" "SHOW TABLES non-empty or create failed"
fi

# ---------- S3 console boot + auth ----------
# (work dir already created in S0.5, before the stack came up)
TAILRACE_BOOTSTRAP_TOKEN="tr_b_ddl_e2e_00112233445566778899aabbccddeeff" \
  "$CONSOLE_BIN" --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
if wait_http "http://$CONSOLE_ADDR/healthz" 20; then
  step pass "S3.1 console booted (/healthz 200, fresh data-dir)"
else
  step fail "S3.1 console boot" "$(tail -3 "$WORK/console.log")"
fi
# sed, not `cut -d'：'`: GNU cut rejects the multi-byte delimiter (smoke.sh S1.2)
ADMIN_PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$TOKEN" ]; } && step pass "S3.2 login → 200 + token" \
  || step fail "S3.2 login" "http=$CODE body=$BODY"

# ---------- S4 datasources + table objects ----------
# doris contract (api/datasources.rs): jdbcUrl AND streamLoadUrl both required;
# root has an empty password on a fresh container — username seals the
# credential the backfill/sink path needs later
req POST /api/datasources \
  "{\"name\":\"e2e-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$DDS" ]; } && step pass "S4.1 doris datasource (id=$DDS, jdbcUrl+streamLoadUrl+root credential sealed)" \
  || step fail "S4.1 doris datasource" "http=$CODE body=$BODY"

req POST /api/datasources "{\"name\":\"e2e-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$FDS" ]; } && step pass "S4.2 file datasource (id=$FDS, path=$WORK/node)" \
  || step fail "S4.2 file datasource" "http=$CODE body=$BODY"

COLS='[["order_no","utf8"],["qty","int64"]]'
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders_dup\",\"format\":null,\"columnsJson\":$COLS}" "$TOKEN"
DUP_OK="$CODE"
req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"orders_uniq\",\"format\":null,\"columnsJson\":$COLS}" "$TOKEN"
UNIQ_OK="$CODE"
{ [ "$DUP_OK" = "201" ] || [ "$DUP_OK" = "200" ]; } && { [ "$UNIQ_OK" = "201" ] || [ "$UNIQ_OK" = "200" ]; } \
  && step pass "S4.3 sink table objects registered ($E2E_DB.orders_dup + orders_uniq, no CDC columns)" \
  || step fail "S4.3 sink table objects" "dup http=$DUP_OK uniq http=$UNIQ_OK body=$BODY"

# ---------- S5 DUPLICATE example: heuristic draft → 422 gate → execute ----------
# first round: no body → pure heuristics. No sequence/op_type column → the
# generator must recommend duplicate; key column rides the first-column
# heuristic with the 「猜测」 badge on (that is what the human confirms)
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_dup/ddl/draft" "" "$TOKEN"
KM="$(jq -r '.keyModel // empty' <<<"$BODY" 2>/dev/null)"
# no `// empty` on the boolean: jq's alternative operator swallows `false`,
# which is exactly the value round-2 asserts
GUESSED="$(jq -r '.keyColumnGuessed' <<<"$BODY" 2>/dev/null)"
KCOL="$(jq -r '.keyColumn // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ "$KM" = "duplicate" ] && [ "$GUESSED" = "true" ] && [ "$KCOL" = "order_no" ]; } \
  && step pass "T1.1 draft round-1 orders_dup: keyModel=duplicate, keyColumn=order_no (guessed=true badge)" \
  || step fail "T1.1 draft round-1" "http=$CODE keyModel=$KM guessed=$GUESSED keyColumn=$KCOL body=$BODY"

# round-2 regenerate with the human confirmation + the probed replication:
# single-BE compose stack → replication_num=1 (the caller probes SHOW
# BACKENDS per the DdlDraftBody contract; a fixed 1 IS the probe result here)
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_dup/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
DUP_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
GUESSED2="$(jq -r '.keyColumnGuessed' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ -n "$DUP_DDL" ] && [ "$GUESSED2" = "false" ] && grep -q '"replication_num" = "1"' <<<"$DUP_DDL"; } \
  && step pass "T1.2 draft round-2 (confirmed key column, replication_num=1) → keyColumnGuessed=false" \
  || step fail "T1.2 draft round-2" "http=$CODE guessed=$GUESSED2 ddl=$(head -c 120 <<<"$DUP_DDL")"

# the §7.2 gate: execute without a confirmed key model never reaches Doris
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_dup/ddl/execute" "{\"ddl\":$(json_sql "$DUP_DDL")}" "$TOKEN"
{ [ "$CODE" = "422" ] && [ "$(jq -r '.error.code // empty' <<<"$BODY")" = "KEY_MODEL_REQUIRED" ]; } \
  && step pass "T1.3 execute without keyModel → 422 KEY_MODEL_REQUIRED (nothing executed)" \
  || step fail "T1.3 422 gate" "http=$CODE body=$BODY"

req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_dup/ddl/execute" \
  "{\"ddl\":$(json_sql "$DUP_DDL"),\"keyModel\":\"duplicate\"}" "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.ok // empty' <<<"$BODY")" = "true" ]; } \
  && step pass "T1.4 execute orders_dup → ok (echoed DDL + confirmed duplicate)" \
  || step fail "T1.4 execute orders_dup" "http=$CODE body=$BODY"
# SHOW CREATE TABLE output is multi-column → lowercase the raw dump and match
# the key clause textually (the doris_query scalar helper is COUNT-shaped)
SHOW_DUP="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.orders_dup;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
if grep -q 'duplicate key(`order_no`)' <<<"$SHOW_DUP"; then
  step pass "T1.5 live table $E2E_DB.orders_dup is DUPLICATE KEY(order_no) — table created by the generated DDL, zero hand-written DDL"
else
  step fail "T1.5 SHOW CREATE TABLE orders_dup" "no DUPLICATE KEY(order_no) clause: $(head -c 200 <<<"$SHOW_DUP")"
fi

# ---------- S6 UNIQUE example: human override → execute ----------
# same registered columns, no CDC marker — the human OVERRIDES the duplicate
# recommendation to unique (the wizard's mandatory choice; the execute gate
# then requires the echoed DDL to actually carry UNIQUE KEY)
req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_uniq/ddl/draft" \
  "{\"keyModel\":\"unique\",\"keyColumn\":\"order_no\",\"replicationNum\":1}" "$TOKEN"
UNIQ_DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
KM2="$(jq -r '.keyModel // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ "$KM2" = "unique" ] && grep -qi 'unique key' <<<"$UNIQ_DDL" \
    && grep -q 'enable_unique_key_merge_on_write' <<<"$UNIQ_DDL"; } \
  && step pass "T2.1 draft orders_uniq with human keyModel=unique override → UNIQUE KEY + merge-on-write DDL" \
  || step fail "T2.1 draft orders_uniq" "http=$CODE keyModel=$KM2 ddl=$(head -c 160 <<<"$UNIQ_DDL")"

req POST "/api/datasources/$DDS/tables/$E2E_DB/orders_uniq/ddl/execute" \
  "{\"ddl\":$(json_sql "$UNIQ_DDL"),\"keyModel\":\"unique\"}" "$TOKEN"
{ [ "$CODE" = "200" ] && [ "$(jq -r '.ok // empty' <<<"$BODY")" = "true" ]; } \
  && step pass "T2.2 execute orders_uniq → ok" \
  || step fail "T2.2 execute orders_uniq" "http=$CODE body=$BODY"
SHOW_UNIQ="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.orders_uniq;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
if grep -q 'unique key(`order_no`)' <<<"$SHOW_UNIQ" && grep -q 'enable_unique_key_merge_on_write' <<<"$SHOW_UNIQ"; then
  step pass "T2.3 live table $E2E_DB.orders_uniq is UNIQUE KEY(order_no) + enable_unique_key_merge_on_write"
else
  step fail "T2.3 SHOW CREATE TABLE orders_uniq" "$(head -c 200 <<<"$SHOW_UNIQ")"
fi

# ---------- S7 file source table + seed ----------
req POST "/api/datasources/$FDS/tables" \
  "{\"dbName\":\"local\",\"tableName\":\"orders_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
{ [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } && step pass "S7.1 file table object local.orders_in (jsonl, FILE_POINTER cursor)" \
  || step fail "S7.1 file table object" "http=$CODE body=$BODY"
for i in $(seq 0 9); do printf '{"order_no":"D-%03d","qty":%d}\n' "$i" "$i"; done > "$WORK/node/orders_in"
[ "$(wc -l < "$WORK/node/orders_in" | tr -d ' ')" = "10" ] \
  && step pass "S7.2 seed 10 jsonl rows into orders_in" \
  || step fail "S7.2 seed" "orders_in not 10 lines"

# ---------- S8 engine node self-registration ----------
# sink credential rides the node env (authEnv = TAILRACE_DS_<id>; root: is
# "Basic cm9vdDo=") — smoke.sh S4 precedent; engine cwd = $WORK/node so the
# bare file name orders_in resolves
( cd "$WORK/node" && exec env "TAILRACE_DS_$DDS=Basic cm9vdDo=" \
    "$NODE_BIN" serve \
    --addr "$NODE_ADDR" \
    --control-plane "http://$CONSOLE_ADDR" \
    --advertise "$NODE_ADDR" \
    --state-dir "$WORK/node-state" \
    --heartbeat-secs 2 \
    --bootstrap-token "tr_b_ddl_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
if wait_http "http://$NODE_ADDR/healthz" 20; then
  step pass "S8.1 engine node up (/healthz)"
else
  step fail "S8.1 node boot" "$(tail -3 "$WORK/node.log")"
fi
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] && step pass "S8.2 node self-registered → ONLINE" \
  || step fail "S8.2 node ONLINE" "nodes=$BODY node.log=$(tail -5 "$WORK/node.log")"

# ---------- S9 two pipelines into the two generated tables ----------
deploy_pipeline() { # deploy_pipeline <name> <sink-table> → sets P_ID
  local sql="INSERT INTO $E2E_DB.$2 SELECT order_no, qty FROM local.orders_in"
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
if deploy_pipeline ddl-e2e-dup orders_dup; then
  step pass "S9.1 pipeline#1 file→orders_dup created/published/deployed → RUNNING"
else
  step fail "S9.1 pipeline orders_dup" "deploy chain failed (see lines above)"
fi
if deploy_pipeline ddl-e2e-uniq orders_uniq; then
  step pass "S9.2 pipeline#2 file→orders_uniq created/published/deployed → RUNNING"
else
  step fail "S9.2 pipeline orders_uniq" "deploy chain failed (see lines above)"
fi

# ---------- S10 row reconciliation on the generated tables ----------
ROWS_DUP="$(doris_wait_rows orders_dup 10)"
[ "${ROWS_DUP:-x}" = "10" ] \
  && step pass "T3.1 DUPLICATE table orders_dup: COUNT(*)=10 (file→doris pipeline into the generated table)" \
  || step fail "T3.1 orders_dup rows" "rows=${ROWS_DUP:-?} (expected 10)"
ROWS_UNIQ="$(doris_wait_rows orders_uniq 10)"
[ "${ROWS_UNIQ:-x}" = "10" ] \
  && step pass "T3.2 UNIQUE table orders_uniq: COUNT(*)=10 (10 distinct order_no, merge-on-write folds nothing)" \
  || step fail "T3.2 orders_uniq rows" "rows=${ROWS_UNIQ:-x} (expected 10)"

# ---------- S11 audit assertion (full DDL text in audit_log) ----------
# console.db is SQLite; the migration-00 audit_log carries the whole execute
# payload in detail_json (api/datasources.rs audit json!) — assert both rows,
# one per key model, ok=true, the CREATE TABLE text verbatim
AUDIT_ERR="$(python3 - "$WORK/run/console.db" "$E2E_DB" <<'PY' 2>&1
import json, sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
rows = conn.execute(
    "SELECT action, detail_json FROM audit_log WHERE action='TABLE_DDL_EXECUTE' ORDER BY id"
).fetchall()
assert len(rows) == 2, f"expected 2 TABLE_DDL_EXECUTE rows, got {len(rows)}: {rows}"
models = []
for action, detail in rows:
    d = json.loads(detail)
    assert d["ok"] is True, d
    assert d["dbName"] == sys.argv[2] and d["tableName"] in ("orders_dup", "orders_uniq"), d
    assert "CREATE TABLE" in d["ddl"] and "BUCKETS" in d["ddl"], d["ddl"]
    models.append(d["keyModel"])
assert sorted(models) == ["duplicate", "unique"], models
print("audit ok:", sorted(models))
PY
)"
[ $? -eq 0 ] && [ -n "$AUDIT_ERR" ] && step pass "T4.1 audit_log: 2×TABLE_DDL_EXECUTE (duplicate+unique), full DDL text + ok=true in detail_json" \
  || step fail "T4.1 audit assertion" "$AUDIT_ERR"

# ---------- teardown ----------
[ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
[ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
NODE_PID=""; CONSOLE_PID=""
# surgical teardown: stop+rm ONLY the two doris services — a whole-project
# `compose down` is forbidden here (a resident kafka of this project's compose
# may be running; starrocks.sh teardown discipline). External mode: drop the
# e2e database only, the standing stack keeps running (smoke.sh precedent).
if [ -n "$DORIS_MYSQL" ]; then
  doris_exec 3 "DROP DATABASE IF EXISTS $E2E_DB FORCE;" \
    && step pass "S12.1 external doris cleanup: DROP DATABASE $E2E_DB FORCE (standing stack untouched)" \
    || step fail "S12.1 external cleanup" "drop failed"
else
  doris_compose stop doris-fe doris-be >/dev/null 2>&1 && doris_compose rm -f doris-fe doris-be >/dev/null 2>&1 \
    && step pass "S12.1 doris-fe+doris-be stopped+removed (surgical — compose kafka untouched)" \
    || step fail "S12.1 teardown" "compose stop/rm doris-fe doris-be failed"
fi

TOTAL=$((PASS+FAIL))
echo "== ddl_create e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
