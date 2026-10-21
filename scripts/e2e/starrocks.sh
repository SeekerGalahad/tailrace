#!/usr/bin/env bash
# StarRocks sink e2e (roadmap §九 T24①) — the archived, rerunnable form of the
# T12 manual allin1 e2e.
#
# 用法：
#   bash scripts/e2e/starrocks.sh [--help] [--keep] [--skip-up] [--wait SECS]
#
# 默认全流程（对齐 T12 手工轮次）：
#   docker compose --profile starrocks up -d --wait starrocks   # 健康等待
#     （--wait SECS 覆盖 compose --wait-timeout，默认 300s；T12 实测 21s 即 Healthy，
#      冷拉镜像另计）→ 建库/建用户/GRANT → DDL → 种子数据（10 行 jsonl）→
#   file→starrocks 三档断言：
#     档1 行数      — node run consumed=10 / flushed=10，表 COUNT(*)=10 且
#                     COUNT(DISTINCT order_no)=10；
#     档2 重启幂等  — 同 --state-dir 复跑：consumed=0（FileSource 位点在 EOF），
#                     表行数不增（游标 flush 点提交语义）；
#     档3 同label去重 — 定 label 手工重放：首次 Success（11 行），同 label 再放
#                     Label Already Exists/Dedup，行数不双写（仍 11）。
#   收尾拆栈（stop+rm 仅 starrocks 服务——本项目 compose 常驻 kafka/doris 可能在跑，
#     严禁整体 down 波及；T12 手工轮的 "down 拆净" 在这里收敛为外科式拆除）。
#
# 覆盖变量（TAILRACE_SR_*，沿用 docker-compose.yml 的覆盖模式）：
#   TAILRACE_SR_PROXY_PORT     feproxy stream-load 入口宿主端口   默认 48080
#   TAILRACE_SR_FE_MYSQL_PORT  FE mysql 协议宿主端口（DDL/查询）  默认 49030
#   TAILRACE_SR_DB             目标库                            默认 tailrace_e2e
#   TAILRACE_SR_TABLE          目标表                            默认 orders
#   TAILRACE_SR_USER / TAILRACE_SR_PASSWORD   建的专用户（GRANT 用）默认 tailrace/tailrace_e2e_pw
#   TAILRACE_SR_RUN_SECS       node run --secs                   默认 15（须 > 10s flush 间隔）
#   TAILRACE_NODE_BIN          引擎二进制  默认 target/release/tailrace-node（scripts/build.sh 产物）
#   TAILRACE_SR_WAIT_SECS      同 --wait（默认 300）
#
# 标志：
#   --help      本说明（无需 Docker、无需引擎二进制）
#   --keep      成功后也保留工作目录 /tmp/tailrace-sr-e2e（失败必留作尸检）
#   --skip-up   复用已在跑的栈（跳过 compose up --wait；先探 FE 端口，不可达按 live-gate 空过）
#   --wait SECS compose 健康等待预算（--wait-timeout）
#
# live-gate（对齐 tests/jdbc_sources.rs:110 惯例）：Docker daemon 不可达，或
#   --skip-up 下 FE 端口不通时，打印一行 [skip] 说明后 exit 0 空过——不算失败。
#
# T12 提交证据链（本脚本固化的手工轮，供溯源）：
#   - roadmap §九 T12 结果行：1cd38d0 "feat: T12 StarRocks sink 完整化"——
#     e2e 实跑未回退：--wait 21s Healthy → DDL → file(jsonl 10 行)→starrocks
#     consumed=10/flushed=10、表 10 行/10 distinct；同 state-dir 重启 consumed=0
#     表不增；同 label 重放 Dedup 不双写；down 拆净。
#   - 差异矩阵第⑤项（GRANT 坑，实测）：StarRocks `GRANT ALL ON DATABASE` 不含
#     表级 INSERT（stream load 401）——必须 `GRANT INSERT,SELECT ON ALL TABLES
#     IN DATABASE`；本脚本建表在前、GRANT 在后（ALL TABLES 语义只覆盖现存表）。
#   - compose feproxy 架构注记：allin1 容器内 FE/BE 皆绑 127.0.0.1，nginx
#     feproxy(:8080) 是宿主唯一 HTTP 入口并拦掉 FE→BE 307——引擎侧 307 跟随
#     路径由 conn-doris mock 契约测试覆盖（starrocks_flavor_contract_mock），
#     本 e2e 只验 feproxy 直达链路。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
PROXY_PORT="${TAILRACE_SR_PROXY_PORT:-48080}"
FE_PORT="${TAILRACE_SR_FE_MYSQL_PORT:-49030}"
SR_DB="${TAILRACE_SR_DB:-tailrace_e2e}"
SR_TABLE="${TAILRACE_SR_TABLE:-orders}"
SR_USER="${TAILRACE_SR_USER:-tailrace}"
SR_PASS="${TAILRACE_SR_PASSWORD:-tailrace_e2e_pw}"
RUN_SECS="${TAILRACE_SR_RUN_SECS:-15}"
WAIT_SECS="${TAILRACE_SR_WAIT_SECS:-300}"
NODE_BIN="${TAILRACE_NODE_BIN:-$ROOT/target/release/tailrace-node}"
WORK="${TAILRACE_SR_WORK:-/tmp/tailrace-sr-e2e}"
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
FAILED_STEPS=()
step() { # step <pass|fail> <name> [detail]
  if [ "$1" = "pass" ]; then
    PASS=$((PASS+1)); printf '  ok    %s\n' "$2"
  else
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$2"); printf '  FAIL  %s  %s\n' "$2" "${3:-}"
  fi
}

sr_compose() { docker compose -f "$ROOT/docker-compose.yml" --profile starrocks "$@"; }
# FE DDL/query via the in-container mysql client (allin1 ships one), retry —
# right after --wait the BE may still be finishing registration (smoke.sh
# doris_exec precedent: assert on SHOW, not the exit code alone)
sr_sql() { # sr_sql <retries> <sql...>
  local r
  for r in $(seq 1 "${1:-6}"); do
    sr_compose exec -T starrocks mysql -N -h127.0.0.1 -P9030 -uroot -e "${2}" && return 0
    sleep 5
  done
  return 1
}
sr_rows_once() { # sr_rows_once <sql> → number (empty on failure)
  sr_compose exec -T starrocks mysql -N -h127.0.0.1 -P9030 -uroot -e "${1}" 2>/dev/null | tr -d '[:space:]'
}
sr_wait_rows() { # sr_wait_rows <expected> <sql> — poll until the count equals expected
  local r n
  for r in $(seq 1 30); do
    n="$(sr_rows_once "$2")"
    [ "${n:-}" = "$1" ] && { echo "$n"; return 0; }
    sleep 2
  done
  echo "${n:-}"
}

echo "== starrocks e2e: work=$WORK feproxy=127.0.0.1:$PROXY_PORT fe=127.0.0.1:$FE_PORT db=$SR_DB.$SR_TABLE =="

# ---------- live-gate ----------
if ! docker info >/dev/null 2>&1; then
  echo "[skip] docker daemon unreachable — starrocks e2e needs the allin1 stack (docker compose --profile starrocks), skipped"
  exit 0
fi
if [ "$SKIP_UP" = "1" ]; then
  if ! nc -z 127.0.0.1 "$FE_PORT" 2>/dev/null \
     && ! (exec 3<>"/dev/tcp/127.0.0.1/$FE_PORT") 2>/dev/null; then
    echo "[skip] starrocks FE 127.0.0.1:$FE_PORT unreachable (--skip-up expects a running stack; or drop --skip-up to bring it up), skipped"
    exit 0
  fi
fi

[ -x "$NODE_BIN" ] && step pass "S0.1 engine binary $NODE_BIN" \
  || step fail "S0.1 engine binary" "missing $NODE_BIN (run ./scripts/build.sh)"

# ---------- S1 stack ----------
if [ "$SKIP_UP" = "1" ]; then
  step pass "S1.1 stack reused (--skip-up; FE 127.0.0.1:$FE_PORT reachable)"
else
  echo "  starting starrocks allin1 (first pull may take minutes; --wait-timeout ${WAIT_SECS}s)"
  if sr_compose up -d --wait --wait-timeout "$WAIT_SECS" starrocks; then
    step pass "S1.1 docker compose --profile starrocks up --wait healthy"
  else
    sr_compose logs --tail 50 starrocks || true
    step fail "S1.1 compose up --wait" "see logs above (image pull / bootstrap)"
  fi
fi

# ---------- S2 database + user + GRANT (T12 difference-matrix item ⑤) ----------
# GRANT ALL ON DATABASE does NOT carry table-level INSERT on StarRocks (stream
# load got 401 in the T12 run) — the e2e user needs INSERT,SELECT on ALL TABLES.
# Table first, grant second: ALL TABLES covers existing tables only.
if sr_sql 6 "CREATE DATABASE IF NOT EXISTS $SR_DB;" \
   && sr_sql 6 "CREATE TABLE IF NOT EXISTS $SR_DB.$SR_TABLE (order_no VARCHAR(64) NOT NULL, qty INT) DUPLICATE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\" = \"1\");" \
   && sr_sql 6 "CREATE USER IF NOT EXISTS '$SR_USER'@'%' IDENTIFIED BY '$SR_PASS';" \
   && sr_sql 6 "GRANT INSERT,SELECT ON ALL TABLES IN DATABASE $SR_DB TO USER '$SR_USER'@'%';" \
   && [ -n "$(sr_rows_once "SHOW TABLES FROM $SR_DB LIKE '$SR_TABLE';")" ]; then
  step pass "S2.1 db/table/user + GRANT INSERT,SELECT ON ALL TABLES (not GRANT ALL ON DATABASE)"
else
  step fail "S2.1 ddl+grant" "see compose logs: $(sr_compose logs --tail 20 starrocks 2>&1 | tail -3)"
fi

# ---------- S3 seed + pipeline.toml ----------
rm -rf "$WORK"; mkdir -p "$WORK/state"
for i in $(seq 0 9); do printf '{"order_no":"SR-%03d","qty":%d}\n' "$i" "$i"; done > "$WORK/orders_in"
AUTH_B64="$(printf '%s:%s' "$SR_USER" "$SR_PASS" | base64 | tr -d '\n')"
cat > "$WORK/pipeline.toml" <<EOF
[pipeline]
id = 4124
name = "sr-e2e"

[[datasource]]
name = "in"
type = "file"

[[datasource]]
name = "sr"
type = "starrocks"
stream_load_url = "http://127.0.0.1:$PROXY_PORT/api/$SR_DB/$SR_TABLE/_stream_load"
auth_env = "TAILRACE_SR_AUTH"

# kind is explicit on every table — the catalog default_kind is "kafka"
[[table]]
datasource = "in"
name = "orders_in"
kind = "file"
format = "jsonl"
columns = [["order_no","utf8"],["qty","int64"]]

[[table]]
datasource = "sr"
name = "$SR_TABLE"
kind = "starrocks"
label_prefix = "sr_e2e"
columns = [["order_no","utf8"],["qty","int64"]]

[[statement]]
source = "in.orders_in"
group = "tr.sr_e2e.s0"
sink = "sr.$SR_TABLE"
select_sql = "SELECT order_no, qty FROM src"
EOF
[ -s "$WORK/orders_in" ] && [ "$(wc -l < "$WORK/orders_in" | tr -d ' ')" = "10" ] \
  && step pass "S3.1 seed 10 jsonl rows + pipeline.toml (kind=starrocks sink, label_prefix=sr_e2e)" \
  || step fail "S3.1 seed" "orders_in not 10 lines"

run_node() { # run_node → stdout of `tailrace-node run` (logs go to stderr)
  ( cd "$WORK" && TAILRACE_SR_AUTH="Basic $AUTH_B64" \
      "$NODE_BIN" run pipeline.toml --state-dir "$WORK/state" --secs "$RUN_SECS" 2>/dev/null )
}
stat_of() { # stat_of <output> <field> → first statement's value (BSD-sed safe)
  sed -n 's/^statement#0:.* '"$2"'=\([0-9]*\).*/\1/p' <<<"$1" | head -1
}

# ---------- 档1 行数 ----------
OUT1="$(run_node)"
C1="$(stat_of "$OUT1" consumed)"; F1="$(stat_of "$OUT1" flushed_rows)"
{ [ "${C1:-x}" = "10" ] && [ "${F1:-x}" = "10" ]; } \
  && step pass "T1.1 file→starrocks: consumed=10 flushed_rows=10" \
  || step fail "T1.1 row tier" "consumed=${C1:-?} flushed=${F1:-?} out=$OUT1"
ROWS="$(sr_wait_rows 10 "SELECT COUNT(*) FROM $SR_DB.$SR_TABLE;")"
DISTINCT="$(sr_rows_once "SELECT COUNT(DISTINCT order_no) FROM $SR_DB.$SR_TABLE;")"
{ [ "${ROWS:-x}" = "10" ] && [ "${DISTINCT:-x}" = "10" ]; } \
  && step pass "T1.2 starrocks table 10 rows / 10 distinct order_no" \
  || step fail "T1.2" "rows=${ROWS:-?} distinct=${DISTINCT:-?}"

# ---------- 档2 重启幂等（同 state-dir，游标在 flush 点已提交）----------
OUT2="$(run_node)"
C2="$(stat_of "$OUT2" consumed)"
ROWS2="$(sr_wait_rows 10 "SELECT COUNT(*) FROM $SR_DB.$SR_TABLE;")"
{ [ "${C2:-x}" = "0" ] && [ "${ROWS2:-x}" = "10" ]; } \
  && step pass "T2.1 same-state-dir restart: consumed=0, table still 10 rows (cursor committed at flush point)" \
  || step fail "T2.1 restart tier" "consumed=${C2:-?} rows=${ROWS2:-?} out=$OUT2"

# ---------- 档3 同 label Dedup ----------
# 手工定 label 重放：引擎侧 label 含 pid 不可外部复放，本档证明引擎 Dedup
# 分支所依赖的服务端契约（Label Already Exists → 不双写）。
replay() { # replay <label>
  # Expect: 100-continue is mandatory (StarRocks FE rejects without it —
  # "There is no 100-continue header" DdlException, found by this script's
  # first live run; the engine sends it on every load, conn-doris lib.rs)
  curl -s -X PUT -u "$SR_USER:$SR_PASS" -H "label: $1" -H "format: json" \
    -H "Expect: 100-continue" \
    --data-binary '{"order_no":"SR-REPLAY","qty":99}' \
    "http://127.0.0.1:$PROXY_PORT/api/$SR_DB/$SR_TABLE/_stream_load"
}
R1="$(replay sr_e2e_replay_v1)"
echo "$R1" | grep -Eq '"Status" *: *"Success"' \
  && step pass "T3.1 fixed-label load #1 → Success" \
  || step fail "T3.1" "resp=$(head -c 200 <<<"$R1")"
ROWS3="$(sr_wait_rows 11 "SELECT COUNT(*) FROM $SR_DB.$SR_TABLE;")"
[ "${ROWS3:-x}" = "11" ] \
  && step pass "T3.2 replay row committed once (11 rows)" \
  || step fail "T3.2" "rows=${ROWS3:-?} (expected 11)"
R2="$(replay sr_e2e_replay_v1)"
echo "$R2" | grep -qi 'already' \
  && step pass "T3.3 same label replay → Label Already Exists (Dedup)" \
  || step fail "T3.3" "resp=$(head -c 200 <<<"$R2")"
ROWS4="$(sr_wait_rows 11 "SELECT COUNT(*) FROM $SR_DB.$SR_TABLE;")"
[ "${ROWS4:-x}" = "11" ] \
  && step pass "T3.4 no double write: still 11 rows" \
  || step fail "T3.4" "rows=${ROWS4:-?} (expected 11)"

# ---------- teardown ----------
sr_compose stop starrocks >/dev/null 2>&1 && sr_compose rm -f starrocks >/dev/null 2>&1 \
  && step pass "S4.1 starrocks service stopped+removed (surgical — project kafka/doris untouched)" \
  || step fail "S4.1 teardown" "docker compose --profile starrocks stop/rm starrocks failed"

TOTAL=$((PASS+FAIL))
echo "== starrocks e2e summary: $PASS/$TOTAL steps passed =="
if [ "$FAIL" -gt 0 ]; then
  echo "failed steps:"; printf '  - %s\n' "${FAILED_STEPS[@]}"
  echo "work dir kept for post-mortem: $WORK"
  exit 1
fi
[ "$KEEP" = "1" ] || rm -rf "$WORK"
exit 0
