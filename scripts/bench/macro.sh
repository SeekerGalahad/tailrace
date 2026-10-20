#!/usr/bin/env bash
# tailrace 宏基准（roadmap §四 T1 / §三 L4）：file→file 全链吞吐。
#
# 链路与口径（与 docs/BENCHMARKS.md §方法学 保持一致）：
#   console + engine node 真进程（默认 target/debug 二进制 —— 未优化构建，
#   数字仅作纵向对比基线；--mode release 用 release 二进制）；
#   种子：--rows 行 JSON（order_no/qty/price/note 4 列，行宽 ≈95B）一次性
#   写满源文件 → deploy → 轮询 stats 至 flushed ≥ N；
#   elapsed = deploy 返回 200 → flushed≥N 的墙钟（含管道启动，不含 console/
#   node 进程启动与种子写入）；吞吐 = N / elapsed。
#   断言：sink 文件行数 ≥ N（真正落盘）。
#
# 用法：bash scripts/bench/macro.sh [--rows N] [--report FILE] [--mode debug|release]
#   --rows    种子行数（默认 100000）
#   --report  报告落盘路径（markdown，同时打印摘要到 stdout；缺省仅打印）
#   --mode    二进制档位（默认 debug；release 缺二进制时会先构建）
#   环境 MACRO_NODE_WRAP（T20 峰值 RSS 口径，默认空=行为不变）：node 进程的
#   前置包装命令，如 MACRO_NODE_WRAP="/usr/bin/time -l" MACRO_KEEP=1 ——
#   包装器的 rusage（macOS "maximum resident set size"，单位字节）落在
#   $MACRO_DIR/node.log 尾部；需 MACRO_KEEP=1 保留工作目录后自行提取。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
ROWS=100000
REPORT=""
MODE="debug"
while [ $# -gt 0 ]; do
  case "$1" in
    --rows) ROWS="$2"; shift 2 ;;
    --report) REPORT="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    *) echo "unknown arg: $1 (use --rows/--report/--mode)"; exit 2 ;;
  esac
done

CONSOLE_ADDR="${MACRO_CONSOLE_ADDR:-127.0.0.1:18280}"
NODE_ADDR="${MACRO_NODE_ADDR:-127.0.0.1:18300}"
BOOTSTRAP_TOKEN="${TAILRACE_BOOTSTRAP_TOKEN:-tr_b_macro_00112233445566778899aabbccddeeff}"
WORK="${MACRO_DIR:-/tmp/tailrace-macro-bench}"
KEEP="${MACRO_KEEP:-0}"
NODE_WRAP="${MACRO_NODE_WRAP:-}"
TIMEOUT_SECS=$(( ROWS / 500 + 120 ))   # <500 rows/s 即判失败（追赶余量 120s）

command -v jq >/dev/null || { echo "jq is required"; exit 2; }
now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }

case "$MODE" in
  debug)   BIN_DIR="$ROOT/target/debug" ;;
  release) BIN_DIR="$ROOT/target/release" ;;
  *) echo "--mode must be debug|release"; exit 2 ;;
esac
for b in tailrace-console tailrace-node; do
  if [ ! -x "$BIN_DIR/$b" ]; then
    echo "== $MODE binary $b missing — building =="
    if [ "$MODE" = "release" ]; then cargo build --release -p "$b"; else cargo build -p "$b"; fi || exit 2
  fi
done

CONSOLE_PID=""; NODE_PID=""
cleanup() {
  if [ -n "$NODE_PID" ]; then
    if [ -n "$NODE_WRAP" ]; then
      # 包装模式下 NODE_PID 是包装器（如 /usr/bin/time）：BSD time 不转发
      # SIGTERM，直接杀它会孤儿化 node 且丢失 rusage——改为给其子进程发信号，
      # node 优雅退出后包装器自行结束并打印 rusage
      pkill -TERM -P "$NODE_PID" 2>/dev/null
    else
      kill -TERM "$NODE_PID" 2>/dev/null
    fi
  fi
  [ -n "$CONSOLE_PID" ] && kill -TERM "$CONSOLE_PID" 2>/dev/null
  wait 2>/dev/null
  [ "$KEEP" = "1" ] || rm -rf "$WORK"
}
trap cleanup EXIT

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

echo "== tailrace macro bench: rows=$ROWS mode=$MODE work=$WORK =="
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"

# ---------- console ----------
TAILRACE_BOOTSTRAP_TOKEN="$BOOTSTRAP_TOKEN" "$BIN_DIR/tailrace-console" \
  --addr "$CONSOLE_ADDR" --data-dir "$WORK/run" >"$WORK/console.log" 2>&1 &
CONSOLE_PID=$!
wait_http "http://$CONSOLE_ADDR/healthz" 30 || { echo "console boot failed: $(tail -3 "$WORK/console.log")"; exit 1; }
ADMIN_PW="$(grep -o '初始密码：[0-9a-f]*' "$WORK/console.log" | head -1 | sed 's/^初始密码：//')"
[ -n "$ADMIN_PW" ] || { echo "no admin password line in console.log"; exit 1; }
req POST /api/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}"
TOKEN="$(jq -r '.token // empty' <<<"$BODY")"
[ -n "$TOKEN" ] || { echo "login failed: $CODE $BODY"; exit 1; }

# ---------- datasource + tables ----------
req POST /api/datasources "{\"name\":\"macro-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
DS_ID="$(jq -r '.id // empty' <<<"$BODY")"
[ -n "$DS_ID" ] || { echo "datasource failed: $CODE $BODY"; exit 1; }
COLS='[["order_no","utf8"],["qty","int64"],["price","float64"],["note","utf8"]]'
req POST "/api/datasources/$DS_ID/tables" "{\"dbName\":\"local\",\"tableName\":\"orders_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
req POST "/api/datasources/$DS_ID/tables" "{\"dbName\":\"local\",\"tableName\":\"orders_out\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"

# ---------- seed: N rows, one shot ----------
awk -v n="$ROWS" 'BEGIN {
  for (i=0; i<n; i++)
    printf "{\"order_no\":\"O-%07d\",\"qty\":%d,\"price\":%d.5,\"note\":\"payload-%07d-abcdefghijklmnopqrstuvwxyz\"}\n", i, i, i%1000, i
}' > "$WORK/node/orders_in"
IN_BYTES="$(wc -c < "$WORK/node/orders_in" | tr -d ' ')"

# ---------- engine node ----------
# $NODE_WRAP 故意不加引号：空时展开为无、设为 "/usr/bin/time -l" 时按词拆成命令+参数
# shellcheck disable=SC2086
( cd "$WORK/node" && exec $NODE_WRAP "$BIN_DIR/tailrace-node" serve \
    --addr "$NODE_ADDR" \
    --control-plane "http://$CONSOLE_ADDR" \
    --advertise "$NODE_ADDR" \
    --state-dir "$WORK/node-state" \
    --heartbeat-secs 1 \
    --bootstrap-token "$BOOTSTRAP_TOKEN" ) >"$WORK/node.log" 2>&1 &
NODE_PID=$!
wait_http "http://$NODE_ADDR/healthz" 30 || { echo "node boot failed: $(tail -3 "$WORK/node.log")"; exit 1; }
ONLINE=0
for i in $(seq 1 30); do
  req GET /api/nodes "" "$TOKEN"
  ONLINE="$(jq -r '[.nodes[] | select(.status=="ONLINE")] | length' <<<"$BODY" 2>/dev/null)"
  [ "$ONLINE" = "1" ] && break
  sleep 1
done
[ "$ONLINE" = "1" ] || { echo "node never ONLINE: $(tail -5 "$WORK/node.log")"; exit 1; }

# ---------- pipeline: create → publish → deploy (t0 = deploy ok) ----------
SQL="INSERT INTO local.orders_out SELECT order_no, qty, price, note FROM local.orders_in"
req POST /api/pipelines "{\"name\":\"macro-file-copy\",\"sqlContent\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$DS_ID},\"checkpointMs\":2000}" "$TOKEN"
P_ID="$(jq -r '.id // empty' <<<"$BODY")"
[ -n "$P_ID" ] || { echo "pipeline create failed: $CODE $BODY"; exit 1; }
req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
[ "$CODE" = "200" ] || { echo "publish failed: $CODE $BODY"; exit 1; }
req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
[ "$CODE" = "200" ] || { echo "deploy failed: $CODE $BODY"; exit 1; }
T0="$(now)"

# ---------- wait flushed ≥ N ----------
FLUSHED=0; CONSUMED=0; OK=0
POLL_START="$(now)"
while :; do
  req GET "/api/pipelines/$P_ID/stats" "" "$TOKEN"
  CONSUMED="$(jq -r '.stats.totals.consumed // 0' <<<"$BODY" 2>/dev/null)"
  FLUSHED="$(jq -r '.stats.totals.flushed // 0' <<<"$BODY" 2>/dev/null)"
  if [ "${FLUSHED:-0}" -ge "$ROWS" ]; then OK=1; break; fi
  awk -v a="$POLL_START" -v b="$(now)" -v lim="$TIMEOUT_SECS" 'BEGIN { exit (b-a >= lim) ? 1 : 0 }' || break
  req GET "/api/pipelines/$P_ID" "" "$TOKEN"
  [ "$(jq -r '.status // empty' <<<"$BODY")" = "FAILED" ] && { echo "pipeline FAILED: $BODY"; exit 1; }
  sleep 0.5
done
T1="$(now)"

OUT_LINES=0
[ -f "$WORK/node/orders_out" ] && OUT_LINES="$(wc -l < "$WORK/node/orders_out" | tr -d ' ')"
[ "$OK" = "1" ] || { echo "TIMEOUT after ${TIMEOUT_SECS}s: consumed=$CONSUMED flushed=$FLUSHED (see $WORK with MACRO_KEEP=1)"; KEEP=1; exit 1; }
[ "$OUT_LINES" -ge "$ROWS" ] || { echo "sink rows mismatch: orders_out lines=$OUT_LINES < $ROWS"; KEEP=1; exit 1; }

ELAPSED="$(awk -v a="$T0" -v b="$T1" 'BEGIN { printf "%.3f", b-a }')"
THROUGHPUT="$(awk -v r="$ROWS" -v e="$ELAPSED" 'BEGIN { printf "%.0f", r/e }')"
MBPS="$(awk -v b="$IN_BYTES" -v e="$ELAPSED" 'BEGIN { printf "%.2f", (b/1048576)/e }')"

# ---------- report ----------
GIT_REV="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DATE="$(date '+%Y-%m-%d %H:%M %Z')"
SUMMARY_LINES=(
  "# tailrace 宏基准报告（scripts/bench/macro.sh）"
  ""
  "- 日期: $DATE"
  "- 引擎: git@${GIT_REV}，二进制档位: **$MODE**（debug=未优化构建，数字仅作同机纵向基线）"
  "- 口径: file→file 全链追平——种子 N 行先写满源文件，t0=deploy 返回 200，t1=stats flushed≥N；吞吐=N/(t1−t0)。含管道启动与 sink 落盘（fsync），不含 console/node 进程启动与种子写入。"
  "- 行形态: JSON 4 列（order_no utf8 / qty int64 / price float64 / note utf8），行宽 ≈95B"
  ""
  "## 结果"
  ""
  "| rows | elapsed_s | throughput_rows_per_s | input_MB | input_MB_per_s | sink_lines_verified |"
  "|---|---|---|---|---|---|"
  "| $ROWS | $ELAPSED | **$THROUGHPUT** | $(awk -v b="$IN_BYTES" 'BEGIN{printf "%.1f", b/1048576}') | $MBPS | $OUT_LINES |"
  ""
  "吞吐行: throughput=$THROUGHPUT rows/s（rows=$ROWS, elapsed=${ELAPSED}s, mode=$MODE, git@${GIT_REV}）"
)
printf '%s\n' "${SUMMARY_LINES[@]}"
if [ -n "$REPORT" ]; then
  mkdir -p "$(dirname "$REPORT")"
  printf '%s\n' "${SUMMARY_LINES[@]}" > "$REPORT"
  echo "== report written: $REPORT =="
fi
