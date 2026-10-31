#!/usr/bin/env bash
# R9 W4 e2e: VARIANT + ARRAY<FLOAT> in ONE table through the REAL engine
# chain (T-A4 × T-A5 — W1's variant.sh and the W2 vector probe pinned each
# type alone; this pins them SIDE BY SIDE, the shape a document/embedding
# table actually takes), plus the task card's negative matrices:
#  - dim 校验负例: a declared `list<float32,3>` column receiving a
#    wrong-length / scalar / non-numeric cell — the DECODER guard makes the
#    row Bad (Doris itself silently degrades all three, vector probe
#    B7/B8/B10), so it lands in the node-state DLQ and NOWHERE in the table,
#    while the legal rows of the same batch land intact;
#  - 键列拒绝: keyColumn pointing at the variant / vector column → the
#    generator refuses the draft with the FE-equivalent wording (probe A3).
#
# 用法：
#   ./scripts/build.sh   # 先构建（release 二进制默认路径 target/release/…）
#   bash scripts/e2e/variant_vector.sh [--help] [--keep] [--skip-up] [--wait SECS]
#
# 三层断言：
#   P 档（raw stream load，绕过 console/engine——服务端契约钉子；
#     单类型裸矩阵已归档 scripts/e2e/evidence/{variant,vector}-probe-2026-10-30/）：
#     P1  同一张表 VARIANT + ARRAY<FLOAT> 共存建表，两列都用引擎的发射形态
#         （variant = JSON 字符串字面量、vector = 箭头显示串 "[0.1, 0.2, 0.3]"）
#         写入，与裸 object / 裸数组等价；
#     P2  f32 边值：1e-7 / 0.33333334 / 3.4028235e38 的字符串形态往返
#         （vector 探针 README W4 注记①：断言而非假设）。
#   T 档（console 全链，API 层）：
#     T1  A1 建 docs_emb：columnsJson [doc_id utf8 / meta variant /
#         content variant / emb list<float32,3> / scores list<float32>] ×
#         keyModel=duplicate —— draft 断言 VARIANT×2 + `emb` ARRAY<FLOAT>
#         COMMENT 'vector dim=3' + `scores` 裸 ARRAY<FLOAT>，execute 后
#         SHOW CREATE TABLE 钉活表；裸向量表 evolve 安静（changed=false）；
#         带 dim 声明的 evolve 结果记录（dim 只活在 COMMENT，DESC 带不回）；
#     T2  keyColumn=meta / keyColumn=emb → 400（生成器拒绝，FE 同款语义）；
#     T3  CORE file jsonl → doris：5 行嵌套 JSON + 向量 → 子列断言
#         （meta['source'] / content['l1']['l2']['l3'] / 标量与数组根）+
#         数组逐元素断言（CAST(element_at(...) AS STRING) 比 f32 最短显示——
#         FLOAT 与字面量的 `=` 比较恒假，探针实测）——任务卡核心行；
#     T4  dim 守卫负例：2 合法行 + 长度不符/标量/非数值元素三种坏行同批 →
#         落库恰好 2 行且值逐元素正确（守卫不误杀、不错位）、坏行进
#         node-state DLQ（bad_rows/p<id>-s0.jsonl，rawB64 去重后逐字节比对）
#         且 /metrics 的 tailrace_dlq_rows_total 与侧文件行数一致。DLQ 与
#         游标同为 at-least-once：flush 失败重放会把同一坏行再隔离一次
#         （run1 在役栈 BE 内存抖动实测 3×3，证据在 evidence/
#         variant-vector-e2e-2026-10-31/）——断言取「≥3 且去重集恰为 3 条
#         种子行」，不把重放当误报。
#   C 档（载体——「variant 透传真不解析」的引擎侧证据，评审重点）：
#     一个本地 TCP 代理把 BE 挡在第二个 doris 数据源前，录下引擎真正发出的
#     stream-load 字节：meta/content 以 JSON **字符串**形态在场且
#     fromjson 后与源子文档结构相等（整段子文档到场、无叶子展开），向量列
#     以箭头 List 显示串在场；行经代理照常落库（代理对正确性透明）。
#
# 覆盖变量（沿用 scripts/e2e/variant.sh 的模式）：
#   TAILRACE_FE_MYSQL_PORT      FE mysql 宿主端口   默认 39030（外部模式 9030）
#   TAILRACE_BE_HTTP_PORT       BE webserver 宿主端口 默认 38040（外部 8040）
#   TAILRACE_E2E_DORIS_MYSQL    外部在役 Doris 的 mysql 客户端命令前缀
#                               （设了即跳过 compose up/teardown，收尾改为
#                               专库 DROP FORCE；在役栈实测 3.1.4）
#   TAILRACE_E2E_DB             目标库             默认 tailrace_vv_e2e
#   TAILRACE_E2E_CONSOLE_ADDR   console 监听地址   默认 127.0.0.1:18087
#   TAILRACE_E2E_NODE_ADDR      engine node 监听地址 默认 127.0.0.1:18107
#   TAILRACE_E2E_PROXY_PORT     C 档载体代理端口   默认 18180（127.0.0.1）
#   TAILRACE_CONSOLE_BIN / TAILRACE_NODE_BIN  二进制
#                                 默认 target/release/tailrace-console|tailrace-node
#   TAILRACE_E2E_WAIT_SECS      同 --wait（默认 300）
#   TAILRACE_E2E_WORK           工作目录 默认 /tmp/tailrace-vv-e2e
#
# 标志：--help / --keep / --skip-up / --wait SECS（语义同 variant.sh）
#
# 依赖：jq、python3（C 档代理 + DLQ base64 还原）、curl、nc。
#
# live-gate（对齐 scripts/e2e/starrocks.sh 惯例）：Docker daemon 不可达、
#   --skip-up 下 FE 端口不通、或宿主可用磁盘 <5GiB（OrbStack 红线）时
#   [skip] 空过——不算失败。
#
# 断言红线（两份探针 README + 本 e2e 首跑）：
#   - raw `SELECT v` / 数组整体回显两版本格式有差；3.1.4 上
#     CAST(ARRAY<FLOAT> AS STRING) 直接报错，element_at(...)=<十进制字面量>
#     恒假（f32→f64 加宽）——向量一律按最短显示串逐元素比对；
#   - 3.1.4 VARIANT 数组子列的 CAST 显示用**单引号**（['x', 'y']），根值
#     CAST 用双引号（["x", "y"]）——数组断言接受三种拼写；
#   - doris_query 取标量会剥掉结果里的全部空白（含单元格内空格）——
#     多行/含空格的结果改用 doris_rows（保留单元格内空格、行以 ; 拼接）；
#   - 3.1.4 GROUP_CONCAT 无 ORDER BY…SEPARATOR 形态——多行断言逐行取回
#     由脚本端拼接。
#
# 环境装甲（在役栈首跑实测）：BE 间歇以 MEM_LIMIT_EXCEEDED 拒载（sys 内存
#   低水位），引擎把它归为不可重试 → 30s/60s 重启退避 → 第 3 次无进展判毒
#   FAILED。每次部署前 be_write_gate 先用 probe_mix 探写路径、等窗口过去；
#   T4 若仍被窗口击穿，stop+redeploy 一次（记录为 observation 步，不改引擎）。
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
E2E_DB="${TAILRACE_E2E_DB:-tailrace_vv_e2e}"
CONSOLE_ADDR="${TAILRACE_E2E_CONSOLE_ADDR:-127.0.0.1:18087}"
NODE_ADDR="${TAILRACE_E2E_NODE_ADDR:-127.0.0.1:18107}"
PROXY_PORT="${TAILRACE_E2E_PROXY_PORT:-18180}"
CONSOLE_BIN="${TAILRACE_CONSOLE_BIN:-$ROOT/target/release/tailrace-console}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
# the node is exec'd from inside $WORK/node (its file-source cwd), so a
# relative binary path handed in by the caller must be anchored to the repo
# root first (variant.sh first-run finding)
case "$CONSOLE_BIN" in /*) ;; *) CONSOLE_BIN="$ROOT/$CONSOLE_BIN" ;; esac
case "$NODE_BIN" in /*) ;; *) NODE_BIN="$ROOT/$NODE_BIN" ;; esac
WAIT_SECS="${TAILRACE_E2E_WAIT_SECS:-300}"
WORK="${TAILRACE_E2E_WORK:-/tmp/tailrace-vv-e2e}"
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
CONSOLE_PID=""; NODE_PID=""; PROXY_PID=""
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
  [ -n "$PROXY_PID" ] && kill -TERM "$PROXY_PID" 2>/dev/null
  wait 2>/dev/null
  [ "$FAIL" -gt 0 ] && KEEP=1
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

# E2E-only compose override, generated into the work dir (docker-compose.yml
# itself stays pristine): BE 2.1.7's start_be.sh hard-refuses to start when
# the Docker VM has ANY active swap — neutralized for THIS stack only (R6
# recipe, same as partial_update.sh / type_precision.sh / variant.sh).
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
# doris_query <sql> → single scalar (whitespace-stripped). A query that
# ERRORS is retried (3×2s) — first-run finding on the standing stack: the
# BE's sys-memory low-water check (MEM_ALLOC_FAILED, "sys available memory
# … less than low water mark") transiently refuses a query when the shared
# VM is tight. A failed query is not a wrong value; masking nothing — the
# value of a query that RAN is returned as is.
doris_query() {
  local r out
  for r in 1 2 3; do
    if out="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${1}" 2>/dev/null)"; then
      printf '%s' "$out" | tr -d '[:space:]'
      return 0
    fi
    sleep 2
  done
  return 1
}
doris_rows() { # doris_rows <sql> → one result row per line joined by ';' (spaces kept inside cells)
  local r out
  for r in 1 2 3; do
    if out="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "${1}" 2>/dev/null)"; then
      printf '%s' "$out" | tr -d '\r' | paste -sd';' -
      return 0
    fi
    sleep 2
  done
  return 1
}
doris_wait_scalar() { # doris_wait_scalar <sql> <expected> [tries=45] — poll (2s) until the scalar equals
  local r n
  for r in $(seq 1 "${3:-45}"); do
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
# be_write_gate <tag> — BE health gate before each pipeline deployment. The
# standing stack's BE intermittently refuses stream loads with
# MEM_LIMIT_EXCEEDED (sys-memory low-water check, "process memory used" even
# reported negative — runs 1/3): two failed flushes put a statement into its
# 30s/60s restart backoff, a third trips the engine's no-progress poison
# limit (3) → FAILED. A throwaway row into probe_mix (P tier table) probes
# the write path; the window is WAITED out (≤6×10s) so the pipeline under
# test starts on a healthy BE. Sets GATE_TRIES.
be_write_gate() {
  local i
  GATE_TRIES=0
  for i in 1 2 3 4 5 6; do
    GATE_TRIES=$i
    printf '%s\n' "{\"id\":$((900 + i)),\"a\":[9,9,9]}" > "$WORK/wl.jsonl"
    raw_load probe_mix "vv_wl_${1}_$$_$i" "$WORK/wl.jsonl" | grep -q '"Status": *"Success"' && return 0
    sleep 10
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
# deploy_pipeline <name> <sink-table> <source-table> <select-cols> [insert-col-list] [sink-ds-id]
# (sink-ds-id defaults to the direct doris datasource; the C tier passes the
# proxy-fronted one)
deploy_pipeline() {
  local target="$E2E_DB.$2"
  [ -n "${5:-}" ] && target="$target ($5)"
  local sink_ds="${6:-$DDS}"
  local sql="INSERT INTO $target SELECT $4 FROM local.$3"
  req POST /api/pipelines \
    "{\"name\":\"$1\",\"sqlContent\":$(json_sql "$sql"),\"dsMapping\":{\"local\":$FDS,\"$E2E_DB\":$sink_ds},\"checkpointMs\":2000}" "$TOKEN"
  P_ID="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
  [ -n "$P_ID" ] || { echo "  pipeline $1 create failed: http=$CODE body=$BODY" >&2; return 1; }
  req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
  [ "$CODE" = "200" ] || { echo "  pipeline $1 publish failed: http=$CODE body=$BODY" >&2; return 1; }
  req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
  [ "$CODE" = "200" ] || { echo "  pipeline $1 deploy failed: http=$CODE body=$BODY" >&2; return 1; }
  req GET "/api/pipelines/$P_ID" "" "$TOKEN"
  [ "$(jq -r '.status // empty' <<<"$BODY")" = "RUNNING" ]
}
# create_sink_table <table> <columnsJson> <keyColumn> — table object + A1
# draft + execute on the direct doris datasource; sets DDL/CODE
create_sink_table() {
  req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"$1\",\"format\":null,\"columnsJson\":$2}" "$TOKEN"
  req POST "/api/datasources/$DDS/tables/$E2E_DB/$1/ddl/draft" \
    "{\"keyModel\":\"duplicate\",\"keyColumn\":\"$3\",\"replicationNum\":1}" "$TOKEN"
  DDL="$(jq -r '.ddl // empty' <<<"$BODY" 2>/dev/null)"
  req POST "/api/datasources/$DDS/tables/$E2E_DB/$1/ddl/execute" \
    "{\"ddl\":$(json_sql "$DDL"),\"keyModel\":\"duplicate\"}" "$TOKEN"
}

echo "== variant_vector e2e: work=$WORK fe=127.0.0.1:$FE_PORT be=127.0.0.1:$BE_PORT db=$E2E_DB console=$CONSOLE_ADDR =="
command -v jq >/dev/null || { echo "jq is required"; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required (C-tier carrier proxy + DLQ base64 decode)"; exit 2; }

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable — variant_vector e2e needs the compose doris stack (or TAILRACE_E2E_DORIS_MYSQL), skipped"
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
# Generated by scripts/e2e/variant_vector.sh — e2e-only, never a committed
# default. Neutralizes the BE 2.1.7 start_be.sh unconditional swap refusal
# for this throwaway test stack (threshold raised past any swap-device
# count), then hands control back to the image's original entrypoint
# unchanged.
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
DORIS_VER="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW FRONTENDS;" 2>/dev/null | grep -o 'doris-[0-9][0-9.a-z-]*' | head -1)"
step pass "S2.2 server version under test: ${DORIS_VER:-unknown} (SHOW FRONTENDS; SELECT VERSION() only reports the mysql protocol)"

# ---------- P tier: raw stream-load contract (server-side, no engine) ----------
# Re-pins, on THIS run's server and for the MIXED table shape, the halves of
# the two single-type probes the engine chain rides.
doris_exec 3 "CREATE TABLE $E2E_DB.probe_mix (id INT NOT NULL, v VARIANT, a ARRAY<FLOAT>) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" \
  && step pass "P0.1 probe_mix created (VARIANT + ARRAY<FLOAT> side by side, hand-DDLed on purpose — this tier pins Doris, not the engine)" \
  || step fail "P0.1 probe_mix" "create failed"
# P1: the engine's exact emission shapes — a variant cell arrives as a JSON
# *string literal* (decoder serializes the sub-document, sink escapes it), a
# vector cell as the arrow display string (sink's ArrayFormatter catch-all)
printf '%s\n' '{"id":1,"v":"{\"a\":{\"b\":42}}","a":"[0.1, 0.2, 0.3]"}' > "$WORK/p_str.jsonl"
printf '%s\n' '{"id":2,"v":{"a":{"b":42}},"a":[0.1,0.2,0.3]}' > "$WORK/p_bare.jsonl"
R1="$(raw_load probe_mix "vv_p_str_$$" "$WORK/p_str.jsonl")"
R2="$(raw_load probe_mix "vv_p_bare_$$" "$WORK/p_bare.jsonl")"
B1="$(doris_wait_scalar "SELECT v['a']['b'] FROM $E2E_DB.probe_mix WHERE id=1;" 42)"
B2="$(doris_query "SELECT v['a']['b'] FROM $E2E_DB.probe_mix WHERE id=2;")"
# FLOAT vs a decimal literal never compares equal (f32→f64 widening; probed
# before this script was written) — elements compare by their shortest
# display string, which identifies the stored f32 exactly
E1="$(doris_query "SELECT CONCAT(CAST(element_at(a,1) AS STRING),'|',CAST(element_at(a,3) AS STRING),'|',array_size(a)) FROM $E2E_DB.probe_mix WHERE id=1;")"
E2="$(doris_query "SELECT CONCAT(CAST(element_at(a,1) AS STRING),'|',CAST(element_at(a,3) AS STRING),'|',array_size(a)) FROM $E2E_DB.probe_mix WHERE id=2;")"
{ echo "$R1" | grep -q '"Status": *"Success"' && echo "$R2" | grep -q '"Status": *"Success"' \
    && [ "${B1:-x}" = "42" ] && [ "${B2:-x}" = "42" ] && [ "${E1:-x}" = "0.1|0.3|3" ] && [ "${E2:-x}" = "0.1|0.3|3" ]; } \
  && step pass "P1.1 both engine emission shapes land on the MIXED table, equal to the bare forms: variant JSON-string literal → v['a']['b']=42, vector display string '[0.1, 0.2, 0.3]' → elements 0.1/0.3, size 3 (variant probe B2 + vector probe B3 re-pinned side by side)" \
  || step fail "P1.1 mixed shapes" "b1=$B1 b2=$B2 e1=$E1 e2=$E2 r1=$(head -c 200 <<<"$R1")"
# P2: f32 edges in the string form (arrow prints tiny/huge f32 in
# scientific notation — the server's string→FLOAT cast must take it)
printf '%s\n' '{"id":3,"a":"[1e-7, 0.33333334, 3.4028235e38]"}' > "$WORK/p_edge.jsonl"
R="$(raw_load probe_mix "vv_p_edge_$$" "$WORK/p_edge.jsonl")"
EDGE="$(doris_wait_scalar "SELECT CONCAT(CAST(element_at(a,1) AS STRING),'|',CAST(element_at(a,2) AS STRING),'|',CAST(element_at(a,3) AS STRING)) FROM $E2E_DB.probe_mix WHERE id=3;" '1e-07|0.33333334|3.4028235e+38')"
{ echo "$R" | grep -q '"Status": *"Success"' && [ "${EDGE:-x}" = "1e-07|0.33333334|3.4028235e+38" ]; } \
  && step pass "P2.1 f32 edges survive the string→FLOAT cast: 1e-7 → 1e-07, 0.33333334 kept, f32::MAX 3.4028235e38 → 3.4028235e+38 (vector probe README W4 note 1 — asserted, not assumed)" \
  || step fail "P2.1 f32 edges" "edge=$EDGE resp=$(head -c 200 <<<"$R")"

# ---------- S3 console boot + auth + datasources (incl. the C-tier proxy datasource) ----------
TAILRACE_BOOTSTRAP_TOKEN="tr_b_vv_e2e_00112233445566778899aabbccddeeff" \
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
  "{\"name\":\"vv-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BE_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
# the C-tier datasource: same FE (DDL gates read the real table), stream
# loads through the capture proxy — created BEFORE the node boots because
# each plan table names its credential env (auth_env TAILRACE_DS_<id>)
req POST /api/datasources \
  "{\"name\":\"vv-proxy\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FE_PORT\",\"streamLoadUrl\":\"http://127.0.0.1:$PROXY_PORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
DDS_PROXY="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
req POST /api/datasources "{\"name\":\"vv-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "201" ] && [ -n "$DDS" ] && [ -n "$DDS_PROXY" ] && [ -n "$FDS" ]; } \
  && step pass "S3.3 datasources: doris direct (d=$DDS) + doris via carrier proxy (d=$DDS_PROXY) + file (f=$FDS)" \
  || step fail "S3.3 datasources" "dds=$DDS proxy=$DDS_PROXY fds=$FDS body=$BODY"

# ---------- S3.5 engine node ----------
( cd "$WORK/node" && exec env "TAILRACE_DS_$DDS=Basic cm9vdDo=" "TAILRACE_DS_$DDS_PROXY=Basic cm9vdDo=" \
    "$NODE_BIN" serve \
      --addr "$NODE_ADDR" \
      --control-plane "http://$CONSOLE_ADDR" \
      --advertise "$NODE_ADDR" \
      --state-dir "$WORK/node-state" \
      --heartbeat-secs 2 \
      --bootstrap-token "tr_b_vv_e2e_00112233445566778899aabbccddeeff" ) >"$WORK/node.log" 2>&1 &
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

# ---------- T1 A1 loop: the mixed variant+vector table ----------
COLS_MIX='[["doc_id","utf8"],["meta","variant"],["content","variant"],["emb","list<float32,3>"],["scores","list<float32>"]]'
create_sink_table docs_emb "$COLS_MIX" doc_id
MIX_CODE="$CODE"
{ grep -q '`meta` VARIANT' <<<"$DDL" && grep -q '`content` VARIANT' <<<"$DDL" \
    && grep -q "\`emb\` ARRAY<FLOAT> COMMENT 'vector dim=3'" <<<"$DDL" && grep -q '`scores` ARRAY<FLOAT>' <<<"$DDL" \
    && ! grep -q "\`scores\` ARRAY<FLOAT> COMMENT" <<<"$DDL"; } \
  && step pass "T1.1 draft maps the mixed vocabulary: meta/content→VARIANT, emb→ARRAY<FLOAT> COMMENT 'vector dim=3' (the dim annotation), scores→bare ARRAY<FLOAT> (no dim, no comment)" \
  || step fail "T1.1 draft mapping" "ddl=$(head -c 400 <<<"$DDL")"
SHOW_MIX="$(doris_mysql_exec -N -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE $E2E_DB.docs_emb;" 2>/dev/null | tr '[:upper:]' '[:lower:]')"
{ [ "$MIX_CODE" = "200" ] && grep -q '`meta` variant' <<<"$SHOW_MIX" && grep -q '`content` variant' <<<"$SHOW_MIX" \
    && grep -q '`emb` array<float>.*vector dim=3' <<<"$SHOW_MIX" && grep -q '`scores` array<float>' <<<"$SHOW_MIX"; } \
  && step pass "T1.2 execute → live SHOW CREATE TABLE: meta/content variant, emb array<float> with the 'vector dim=3' comment, scores array<float> (real Doris types, zero hand DDL)" \
  || step fail "T1.2 execute+show" "http=$MIX_CODE show=$(head -c 400 <<<"$SHOW_MIX")"
# evolve, bare vector table: DESC `array<float>` folds back to
# `list<float32>` — the registered word — so the diff stays quiet
COLS_PLAIN='[["doc_id","utf8"],["emb","list<float32>"]]'
create_sink_table emb_plain "$COLS_PLAIN" doc_id
req POST "/api/datasources/$DDS/tables/$E2E_DB/emb_plain/ddl/evolve" '{}' "$TOKEN"
# jq `// empty` swallows a literal false — tostring keeps it (variant.sh run 1)
EV_CHANGED="$(jq -r '.changed | tostring' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ "${EV_CHANGED:-x}" = "false" ]; } \
  && step pass "T1.3 evolve on the BARE vector table reports changed=false (DESC array<float> folds back to list<float32> — round-trip stable)" \
  || step fail "T1.3 evolve bare vector" "http=$CODE changed=$EV_CHANGED body=$(head -c 300 <<<"$BODY")"
# evolve, DIMMED declaration: the dim lives only in the DDL COMMENT and DESC
# cannot carry it back — the diff must treat the registered list<float32,3>
# and the DESC-folded list<float32> as the same type (R9 review fix: before
# it, every dimmed vector table 409'd forever and even pure ADD COLUMNs were
# blocked). Asserted now, no longer "recorded".
req POST "/api/datasources/$DDS/tables/$E2E_DB/docs_emb/ddl/evolve" '{}' "$TOKEN"
EV_CHANGED="$(jq -r '.changed | tostring' <<<"$BODY" 2>/dev/null)"
EV_RETYPED="$(jq -c '.error.refs.retyped // .refs.retyped // empty' <<<"$BODY" 2>/dev/null)"
{ [ "$CODE" = "200" ] && [ "${EV_CHANGED:-x}" = "false" ]; } \
  && step pass "T1.4 evolve on the DIMMED table → 200 changed=false (dim invisible to DESC is not a retype — R9 review fix)" \
  || step fail "T1.4 evolve dimmed vector" "http=$CODE changed=$EV_CHANGED retyped=${EV_RETYPED:-none} body=$(head -c 300 <<<"$BODY")"

# ---------- T2 key-column refusals (the task card's negative examples) ----------
req POST "/api/datasources/$DDS/tables/$E2E_DB/docs_emb/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"meta\",\"replicationNum\":1}" "$TOKEN"
{ [ "$CODE" = "400" ] && grep -qi 'variant' <<<"$BODY"; } \
  && step pass "T2.1 keyColumn=meta (variant) → 400 naming VARIANT (the generator refuses a draft the FE would refuse; variant probe A3)" \
  || step fail "T2.1 variant key refused" "http=$CODE body=$(head -c 200 <<<"$BODY")"
req POST "/api/datasources/$DDS/tables/$E2E_DB/docs_emb/ddl/draft" \
  "{\"keyModel\":\"duplicate\",\"keyColumn\":\"emb\",\"replicationNum\":1}" "$TOKEN"
{ [ "$CODE" = "400" ] && grep -q 'ARRAY' <<<"$BODY"; } \
  && step pass "T2.2 keyColumn=emb (vector) → 400 naming ARRAY (FE: 'Array can only be used in the non-key column'; vector probe A3)" \
  || step fail "T2.2 vector key refused" "http=$CODE body=$(head -c 200 <<<"$BODY")"

# ---------- T3 CORE: file jsonl → doris, sub-column + element assertions ----------
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"docs_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_MIX}" "$TOKEN"
{ [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } && step pass "T3.1 file table local.docs_in (jsonl, the mixed vocabulary)" \
  || step fail "T3.1 file table" "http=$CODE"
# 5 rows. meta/content cover nested objects, a 4-level path, a leaf array,
# mixed leaf kinds, json null, {} and an array root, a string scalar; emb
# covers floats, integers (widen), negatives+zero and the f32 edges; scores
# (no dim) covers 3-elem / 1-elem / null / empty / absent.
cat > "$WORK/node/docs_in" <<'EOF'
{"doc_id":"DOC-1","meta":{"source":"api","verified":true},"content":{"l1":{"l2":{"l3":"leaf"}},"tags":["x","y"]},"emb":[0.1,0.2,0.3],"scores":[9,8,7]}
{"doc_id":"DOC-2","meta":{"source":"batch"},"content":{"l1":{"l2":"shallow"}},"emb":[1,2,3],"scores":[1]}
{"doc_id":"DOC-3","meta":{"source":"api","n":null},"content":{"k":1.5,"t":true},"emb":[-0.5,0,0.5],"scores":null}
{"doc_id":"DOC-4","meta":null,"content":{},"emb":[1e-7,1,0.33333334],"scores":[]}
{"doc_id":"DOC-5","meta":"scalar-string","content":[10,20],"emb":[0.25,0.5,0.75]}
EOF
step pass "T3.2 seeds: 5 rows (nested docs incl. null/{}/array-root/string-scalar; vectors incl. ints/negatives/zero/1e-7; scores 3/1/null/[]/absent)"
be_write_gate t3 \
  && step pass "T3.2a BE write path healthy before the deployment (gate attempt $GATE_TRIES)" \
  || step fail "T3.2a BE write health" "MEM_LIMIT window outlasted 6×10s — the standing BE is too tight for this run"
if deploy_pipeline vv-docs docs_emb docs_in "doc_id, meta, content, emb, scores"; then
  step pass "T3.3 pipeline full-column file→docs_emb deployed → RUNNING"
else
  step fail "T3.3 pipeline docs_emb" "deploy chain failed (see lines above)"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.docs_emb;" 5 75)"
{ [ "${N:-x}" = "5" ]; } \
  && step pass "T3.4 COUNT=5/5 through the engine chain" \
  || step fail "T3.4 count" "n=$N node.log=$(tail -5 "$WORK/node.log" 2>/dev/null) events=$(grep -c MEM_LIMIT "$WORK/node-state/events.log" 2>/dev/null)"
# sub-columns (CAST/sub-path only — the raw-text red line), one predicate
# group per document so a failure names the exact leaf
dcount() { doris_query "SELECT COUNT(*) FROM $E2E_DB.docs_emb WHERE doc_id='$1' AND $2;"; }
# the leaf-array cast spelling differs by context on 3.1.4 (first-run
# finding): CAST(content AS STRING) renders ["x", "y"], but
# CAST(content['tags'] AS STRING) renders ['x', 'y'] — single-quoted; all
# three spellings are the same fact
D1A="$(dcount DOC-1 "meta['source']='api' AND CAST(meta['verified'] AS INT)=1")"
D1B="$(dcount DOC-1 "content['l1']['l2']['l3']='leaf'")"
D1C="$(dcount DOC-1 "CAST(content['tags'] AS STRING) IN ('[\"x\",\"y\"]','[\"x\", \"y\"]','[''x'', ''y'']')")"
{ [ "${D1A:-x}" = "1" ] && [ "${D1B:-x}" = "1" ] && [ "${D1C:-x}" = "1" ]; } \
  && step pass "T3.5 CORE DOC-1 variant leaves: meta.source='api' + meta.verified→1 (bool stored as int, variant probe B1), content.l1.l2.l3='leaf' (4-level path), content.tags leaf array (3.1.4 spells it ['x', 'y'] under a sub-path CAST)" \
  || step fail "T3.5 nested sub-columns" "meta=$D1A path=$D1B tags=$D1C (each expected 1)"
D2="$(dcount DOC-2 "meta['source']='batch' AND content['l1']['l2']='shallow'")"
D3="$(dcount DOC-3 "meta['source']='api' AND CAST(content['k'] AS STRING)='1.5' AND CAST(content['t'] AS INT)=1")"
D4="$(dcount DOC-4 "meta IS NULL")"
D5="$(dcount DOC-5 "CAST(meta AS STRING) IN ('\"scalar-string\"','scalar-string') AND CAST(content AS STRING) IN ('[10,20]','[10, 20]')")"
# 3.1.4 rendered this variant string scalar QUOTED ("scalar-string") in the
# pre-run scratch matrix (bare and engine-form payloads alike) but UNQUOTED
# (scalar-string) after the real engine-chain loads of runs 2/3 — the cause
# (sub-column typing state of the live table) is not pinned; both spellings
# are accepted. The property under test is "meta is a string scalar,
# content an array root", not its quoting.
{ [ "${D2:-x}" = "1" ] && [ "${D3:-x}" = "1" ] && [ "${D4:-x}" = "1" ] && [ "${D5:-x}" = "1" ]; } \
  && step pass "T3.6 CORE DOC-2..5 variant shapes: shallow path beside DOC-1's deeper one (l1.l2 string vs object), mixed leaf kinds (1.5 / true→1), json null meta → NULL, string-scalar meta kept as a JSON string, array-root content answers via CAST" \
  || step fail "T3.6 variant shapes" "doc2=$D2 doc3=$D3 doc4=$D4 doc5=$D5 (each expected 1) actual-doc5=[$(doris_rows "SELECT CONCAT(IFNULL(CAST(meta AS STRING),'NULL'),' || ',IFNULL(CAST(content AS STRING),'NULL')) FROM $E2E_DB.docs_emb WHERE doc_id='DOC-5';")]"
# vectors: element-wise by shortest f32 display (FLOAT `=` literal never
# matches — probed), cardinality via array_size; one row per document
# (3.1.4's GROUP_CONCAT has no ORDER BY … SEPARATOR form — first run)
EMB="$(doris_rows "SELECT CONCAT(doc_id,'=',CAST(element_at(emb,1) AS STRING),',',CAST(element_at(emb,2) AS STRING),',',CAST(element_at(emb,3) AS STRING),'#',array_size(emb)) FROM $E2E_DB.docs_emb ORDER BY doc_id;")"
WANT_EMB='DOC-1=0.1,0.2,0.3#3;DOC-2=1,2,3#3;DOC-3=-0.5,0,0.5#3;DOC-4=1e-07,1,0.33333334#3;DOC-5=0.25,0.5,0.75#3'
{ [ "${EMB:-x}" = "$WANT_EMB" ]; } \
  && step pass "T3.7 CORE vectors element-exact on all 5 rows: floats, integers widened (1,2,3), negatives+zero, the f32 edges (1e-07 / 0.33333334) — dimension 3 everywhere" \
  || step fail "T3.7 vector elements" "got=$EMB want=$WANT_EMB"
SC="$(doris_rows "SELECT CONCAT(doc_id,'=',IFNULL(CAST(array_size(scores) AS STRING),'NULL')) FROM $E2E_DB.docs_emb ORDER BY doc_id;")"
S2="$(doris_query "SELECT CAST(element_at(scores,2) AS STRING) FROM $E2E_DB.docs_emb WHERE doc_id='DOC-1';")"
{ [ "${SC:-x}" = "DOC-1=3;DOC-2=1;DOC-3=NULL;DOC-4=0;DOC-5=NULL" ] && [ "${S2:-x}" = "8" ]; } \
  && step pass "T3.8 dim-less scores take ANY length: 3 / 1 / json null→NULL / []→0 / absent→NULL, element_at(scores,2)=8 — none killed (vector probe B5/B6)" \
  || step fail "T3.8 dimless scores" "sc=$SC s2=$S2"

# ---------- T4 dim-guard negatives: wrong-length / scalar / non-numeric → DLQ ----------
COLS_BAD='[["doc_id","utf8"],["emb","list<float32,3>"]]'
create_sink_table emb_bad "$COLS_BAD" doc_id
{ [ "$CODE" = "200" ]; } && step pass "T4.1 emb_bad created (emb declared dim=3)" \
  || step fail "T4.1 bad table" "http=$CODE ddl=$(head -c 200 <<<"$DDL")"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"bad_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_BAD}" "$TOKEN"
# the legal rows bracket the bad ones on purpose: a guard that broke the
# equal-column-lengths invariant would shift GOOD-2's vector onto the wrong
# id (review focus: dim 校验不误杀合法行)
cat > "$WORK/node/bad_in" <<'EOF'
{"doc_id":"GOOD-1","emb":[1,2,3]}
{"doc_id":"BAD-DIM","emb":[1,2]}
{"doc_id":"BAD-SCALAR","emb":42}
{"doc_id":"BAD-ELEM","emb":[1,"x",3]}
{"doc_id":"GOOD-2","emb":[0.5,0.25,0.125]}
EOF
step pass "T4.2 seeds: GOOD-1, then wrong-length [1,2] / scalar 42 / non-numeric element \"x\" (the guard's kill shapes — Doris would degrade each silently), then GOOD-2"
be_write_gate t4 \
  && step pass "T4.2a BE write path healthy before the deployment (gate attempt $GATE_TRIES)" \
  || step fail "T4.2a BE write health" "MEM_LIMIT window outlasted 6×10s — the standing BE is too tight for this run"
if deploy_pipeline vv-bad emb_bad bad_in "doc_id, emb"; then
  BAD_PID="$P_ID"
  step pass "T4.3 pipeline file→emb_bad deployed → RUNNING (default badRowPolicy=dlq, pipeline #$BAD_PID)"
else
  BAD_PID="${P_ID:-0}"
  step fail "T4.3 bad pipeline" "deploy chain failed"
fi
# redeploy armor (once): if the rows never arrive AND the node events show
# the MEM_LIMIT-driven failure chain, stop+redeploy the same pipeline after
# the window — the engine's no-progress poison limit cannot be waited out
# (3 consecutive failed flushes are judged deterministic), so a fresh
# dispatch on a recovered BE is the honest retry.
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.emb_bad;" 2 75)"
if [ "${N:-x}" != "2" ] && grep -q "MEM_LIMIT_EXCEEDED" "$WORK/node-state/events.log" 2>/dev/null; then
  sleep 15
  req POST "/api/pipelines/$BAD_PID/stop" '{}' "$TOKEN"
  be_write_gate t4r >/dev/null
  # redeploy = deploy the SAME pipeline again (stop returned it to
  # PUBLISHED); creating a new one would 409 on the name (run-4 finding)
  req POST "/api/pipelines/$BAD_PID/deploy" '{}' "$TOKEN"
  if [ "$CODE" = "200" ]; then
    step pass "T4.3a observation: BE memory window tripped the first deployment (MEM_LIMIT flush failures → restart backoff → no-progress FAILED) — stopped and re-deployed pipeline #$BAD_PID on the recovered BE (environment armor, engine untouched)"
    N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.emb_bad;" 2 75)"
  else
    step fail "T4.3a redeploy" "http=$CODE body=$(head -c 200 <<<"$BODY")"
  fi
fi
GOODS="$(doris_rows "SELECT CONCAT(doc_id,'=',CAST(element_at(emb,1) AS STRING),',',CAST(element_at(emb,2) AS STRING),',',CAST(element_at(emb,3) AS STRING)) FROM $E2E_DB.emb_bad ORDER BY doc_id;")"
{ [ "${N:-x}" = "2" ] && [ "${GOODS:-x}" = "GOOD-1=1,2,3;GOOD-2=0.5,0.25,0.125" ]; } \
  && step pass "T4.4 CORE dim guard: exactly the 2 legal rows landed, each with ITS OWN vector (no misalignment around the killed rows), all 3 kill shapes nowhere in the table" \
  || step fail "T4.4 guard filtered" "n=$N goods=$GOODS"
# DLQ semantics are at-least-once like the cursor: the side file is written
# BEFORE the flush, so a flush that fails and replays the batch quarantines
# the same bad rows again (first run: a BE MEM_LIMIT_EXCEEDED flush failure
# on the standing stack → 2 statement restarts → 3 × 3 records; evidence in
# scripts/e2e/evidence/variant-vector-e2e-2026-10-31/). The invariants are
# therefore: ≥3 records, the DISTINCT raw payloads are exactly the 3 seeds
# byte for byte, and the counter agrees with the side file.
DLQ_FILE="$WORK/node-state/bad_rows/p${BAD_PID}-s0.jsonl"
DLQ_ROWS=0
for i in $(seq 1 20); do
  [ -s "$DLQ_FILE" ] && DLQ_ROWS="$(wc -l < "$DLQ_FILE" | tr -d ' ')"
  [ "$DLQ_ROWS" -ge 3 ] 2>/dev/null && break
  sleep 1
done
{ [ "$DLQ_ROWS" -ge 3 ] 2>/dev/null; } \
  && step pass "T4.5 CORE the killed rows are quarantined, not lost: $DLQ_FILE holds $DLQ_ROWS record(s) (3 per decode of the batch — >3 means a failed flush replayed it, at-least-once)" \
  || step fail "T4.5 DLQ quarantine" "rows=$DLQ_ROWS files=$(ls "$WORK/node-state/bad_rows" 2>/dev/null | tr '\n' ' ')"
python3 - "$DLQ_FILE" > "$WORK/dlq_decode.txt" 2>&1 <<'PY'
import base64, json, sys
want = [b'{"doc_id":"BAD-DIM","emb":[1,2]}', b'{"doc_id":"BAD-SCALAR","emb":42}',
        b'{"doc_id":"BAD-ELEM","emb":[1,"x",3]}']
distinct = []
try:
    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            if line.strip():
                raw = base64.b64decode(json.loads(line)["rawB64"]).rstrip(b"\r\n")
                if raw not in distinct:
                    distinct.append(raw)
except OSError as e:
    print("NOFILE", e)
    sys.exit(0)
print("MATCH" if distinct == want else "DIFF distinct=%r" % distinct)
PY
grep -q '^MATCH$' "$WORK/dlq_decode.txt" \
  && step pass "T4.6 CORE DLQ raw bytes round-trip: the distinct base64-decoded payloads are exactly the 3 seed lines, byte for byte, in source order (recoverable for replay, not just counted)" \
  || step fail "T4.6 DLQ bytes" "$(head -c 300 "$WORK/dlq_decode.txt" 2>/dev/null)"
DLQ_METRIC="$(curl -s "http://$NODE_ADDR/metrics" | grep "^tailrace_dlq_rows_total{pipeline=\"$BAD_PID\",statement=\"0\"}" | awk '{print $2}')"
{ [ -n "$DLQ_METRIC" ] && [ "$DLQ_METRIC" = "$DLQ_ROWS" ]; } \
  && step pass "T4.7 node /metrics tailrace_dlq_rows_total{pipeline=\"$BAD_PID\",statement=\"0\"} $DLQ_METRIC = the side file's $DLQ_ROWS records (the series the E2 dlq_window rule counts agrees with what was quarantined)" \
  || step fail "T4.7 dlq metric" "metric=$DLQ_METRIC dlq_rows=$DLQ_ROWS"

# ---------- C tier: capture the wire carrier (review focus: variant passthrough does NOT parse leaves) ----------
# A plain TCP relay (byte-transparent: Expect/100-continue, keep-alive and
# the BE's answer pass untouched) tees every client→BE byte to carrier.txt.
cat > "$WORK/proxy.py" <<'PY'
# Generated by scripts/e2e/variant_vector.sh — e2e-only carrier capture.
import socket, sys, threading
port, target, cap = int(sys.argv[1]), sys.argv[2], sys.argv[3]
host, tport = target.rsplit(":", 1)
lock = threading.Lock()
def pipe(src, dst, tee):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if tee:
                with lock, open(cap, "ab") as f:
                    f.write(data)
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
def handle(client):
    try:
        upstream = socket.create_connection((host, int(tport)))
    except OSError:
        client.close()
        return
    threading.Thread(target=pipe, args=(client, upstream, True), daemon=True).start()
    threading.Thread(target=pipe, args=(upstream, client, False), daemon=True).start()
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port))
srv.listen(16)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PY
python3 "$WORK/proxy.py" "$PROXY_PORT" "127.0.0.1:$BE_PORT" "$WORK/carrier.txt" >"$WORK/proxy.log" 2>&1 &
PROXY_PID=$!
UP=0
for i in $(seq 1 10); do nc -z 127.0.0.1 "$PROXY_PORT" 2>/dev/null && { UP=1; break; }; sleep 0.5; done
[ "$UP" = "1" ] \
  && step pass "C0.1 carrier proxy up: 127.0.0.1:$PROXY_PORT → BE 127.0.0.1:$BE_PORT (tees the engine's stream-load bytes)" \
  || step fail "C0.1 carrier proxy" "$(tail -3 "$WORK/proxy.log")"
be_write_gate c0 \
  && step pass "C0.1a BE write path healthy before the carrier deployment (gate attempt $GATE_TRIES)" \
  || step fail "C0.1a BE write health" "MEM_LIMIT window outlasted 6×10s"
req POST "/api/datasources/$DDS_PROXY/tables" "{\"dbName\":\"$E2E_DB\",\"tableName\":\"docs_emb\",\"format\":null,\"columnsJson\":$COLS_MIX}" "$TOKEN"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"carr_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS_MIX}" "$TOKEN"
printf '%s\n' '{"doc_id":"CAR-1","meta":{"source":"carrier","nested":{"b":42,"arr":[1,2]}},"content":{"t":"x"},"emb":[0.1,0.2,0.3],"scores":[1,2]}' > "$WORK/node/carr_in"
if deploy_pipeline vv-carrier docs_emb carr_in "doc_id, meta, content, emb, scores" "" "$DDS_PROXY"; then
  step pass "C0.2 carrier pipeline deployed → RUNNING (sink table object on the proxy-fronted datasource)"
else
  step fail "C0.2 carrier pipeline" "deploy chain failed"
fi
N="$(doris_wait_scalar "SELECT COUNT(*) FROM $E2E_DB.docs_emb WHERE doc_id='CAR-1' AND meta['nested']['b']=42;" 1 75)"
{ [ "${N:-x}" = "1" ]; } \
  && step pass "C0.3 the carrier row landed through the proxy (meta.nested.b=42 — the relay is transparent to correctness)" \
  || step fail "C0.3 carrier row" "n=$N proxy.log=$(tail -3 "$WORK/proxy.log" 2>/dev/null) node.log=$(tail -5 "$WORK/node.log" 2>/dev/null)"
# the body line of CAR-1 as the engine emitted it
CARR="$(grep -a '"CAR-1"' "$WORK/carrier.txt" 2>/dev/null | tr -d '\r' | head -1)"
grep -a -m1 -i '^PUT ' "$WORK/carrier.txt" 2>/dev/null | tr -d '\r' > "$WORK/carrier_reqline.txt"
{ [ -n "$CARR" ] && grep -q "/api/$E2E_DB/docs_emb/_stream_load" "$WORK/carrier_reqline.txt"; } \
  && step pass "C1.0 captured: $(cat "$WORK/carrier_reqline.txt") with the CAR-1 body line" \
  || step fail "C1.0 capture" "reqline=$(cat "$WORK/carrier_reqline.txt" 2>/dev/null) bytes=$(wc -c < "$WORK/carrier.txt" 2>/dev/null)"
# Structural comparison on purpose: the decoder re-encodes the sub-document
# (simd-json `encode`) and documents key order as not guaranteed — the
# property is "the WHOLE sub-document is in the carrier as JSON text", not
# byte identity with the source line.
if jq -e '(.meta|type)=="string" and (.content|type)=="string"
          and ((.meta|fromjson)=={"source":"carrier","nested":{"b":42,"arr":[1,2]}})
          and ((.content|fromjson)=={"t":"x"})' >/dev/null 2>&1 <<<"$CARR"; then
  step pass "C1.1 CARRIER (review focus): meta/content ride as JSON STRING literals holding the WHOLE sub-document (nested object + inner array, structurally equal to the source) — no leaf parsing, no flattening; Doris does the sub-column inference server-side. Wire form: meta=$(jq -c '.meta' <<<"$CARR")"
else
  step fail "C1.1 variant carrier" "line=$(head -c 300 <<<"$CARR")"
fi
if jq -e '(.emb|type)=="string" and ((.emb|fromjson)==[0.1,0.2,0.3])
          and (.scores|type)=="string" and ((.scores|fromjson)==[1,2])' >/dev/null 2>&1 <<<"$CARR"; then
  step pass "C1.2 CARRIER vectors ride as the arrow display STRING (emb=$(jq -c '.emb' <<<"$CARR"), scores=$(jq -c '.scores' <<<"$CARR")) — the ArrayFormatter catch-all arm, the exact shape P1.1 proved the server parses"
else
  step fail "C1.2 vector carrier" "line=$(head -c 300 <<<"$CARR")"
fi

# ---------- teardown ----------
[ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
[ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
[ -n "$PROXY_PID" ] && kill -TERM "$PROXY_PID" 2>/dev/null
NODE_PID=""; CONSOLE_PID=""; PROXY_PID=""
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
echo "== variant_vector e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
