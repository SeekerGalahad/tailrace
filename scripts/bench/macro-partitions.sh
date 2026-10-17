#!/usr/bin/env bash
# tailrace 多分区宏基准（roadmap §四 T5 / §三 L4）：kafka(P 分区)→file 全链吞吐。
#
# 与 macro.sh 的差别：源是**多分区 Kafka topic**（T5 分区并行·方案 a 的验收口径）。
#   链路：compose Kafka（apache/kafka:3.9.0，127.0.0.1:9092；未起则脚本拉起 kafka 服务）
#   → 一次性播种 N 行 JSON（4 列，行宽 ≈95B，console-producer 无 key → round-robin
#   均匀铺满 P 分区）→ console+node 真进程 deploy kafka→file 管道 → 轮询 stats 至
#   flushed ≥ N；elapsed = deploy 返回 200 → 达标时刻；吞吐 = N / elapsed（追平口径，
#   与 macro.sh 一致：含管道启动与 sink 落盘 fsync，不含进程启动与播种）。
#   断言：①播种量 GetOffsetShell 求和 == N（producer 不丢）；②sink 文件行数 == N。
#
# 用法：bash scripts/bench/macro-partitions.sh [--rows N] [--partitions P]
#                                              [--report FILE] [--mode debug|release]
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
ROWS=100000
PARTITIONS=6
REPORT=""
MODE="debug"
while [ $# -gt 0 ]; do
  case "$1" in
    --rows) ROWS="$2"; shift 2 ;;
    --partitions) PARTITIONS="$2"; shift 2 ;;
    --report) REPORT="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    *) echo "unknown arg: $1 (use --rows/--partitions/--report/--mode)"; exit 2 ;;
  esac
done

CONSOLE_ADDR="${MACRO_CONSOLE_ADDR:-127.0.0.1:18290}"
NODE_ADDR="${MACRO_NODE_ADDR:-127.0.0.1:18310}"
BOOTSTRAP_TOKEN="${TAILRACE_BOOTSTRAP_TOKEN:-tr_b_macro_00112233445566778899aabbccddeeff}"
KPORT="${TAILRACE_KAFKA_PORT:-9092}"
WORK="${MACRO_DIR:-/tmp/tailrace-macro-parts-bench}"
KEEP="${MACRO_KEEP:-0}"
TIMEOUT_SECS=$(( ROWS / 500 + 180 ))   # <500 rows/s 即判失败（kafka 组加入 + 追赶余量）
TOPIC="tailrace_bench_parts_$(date +%s)"

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
    if [ "$MODE" = "release" ]; then CARGO_PROFILE_RELEASE_DEBUG=0 cargo build --release -p "$b"; else cargo build -p "$b"; fi || exit 2
  fi
done

# ---------- kafka（compose 服务；未起则拉起并等健康） ----------
kc() { docker compose exec -T kafka "$@"; }
if ! docker compose ps kafka 2>/dev/null | grep -q "running\|healthy"; then
  echo "== starting compose kafka service =="
  docker compose up -d kafka || { echo "docker compose up kafka failed"; exit 2; }
fi
for i in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$KPORT" 2>/dev/null)" != "000" ] && break
  sleep 1
done
kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 \
  || { echo "kafka CLI probe failed on 127.0.0.1:$KPORT"; exit 2; }

CONSOLE_PID=""; NODE_PID=""
cleanup() {
  [ -n "$NODE_PID" ] && kill -TERM "$NODE_PID" 2>/dev/null
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

echo "== tailrace multi-partition macro bench: rows=$ROWS partitions=$PARTITIONS topic=$TOPIC mode=$MODE =="
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"

# ---------- 播种：N 行 JSON，一次性铺满 P 分区 ----------
kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null \
  || { echo "topic create failed"; exit 2; }
awk -v n="$ROWS" 'BEGIN {
  for (i=0; i<n; i++)
    printf "{\"order_no\":\"O-%07d\",\"qty\":%d,\"price\":%d.5,\"note\":\"payload-%07d-abcdefghijklmnopqrstuvwxyz\"}\n", i, i, i%1000, i
}' > "$WORK/seed.jsonl"
SEED_T0="$(now)"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" < "$WORK/seed.jsonl" >"$WORK/produce.log" 2>&1 \
  || { echo "seed produce failed: $(tail -3 "$WORK/produce.log")"; KEEP=1; exit 1; }
SEED_T1="$(now)"
SEED_ELAPSED="$(awk -v a="$SEED_T0" -v b="$SEED_T1" 'BEGIN { printf "%.1f", b-a }')"
# kafka 3.9 移除了 kafka.tools.GetOffsetShell（scala 类），换 kafka-get-offsets.sh
PRODUCED="$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null \
  | awk -F: '{s+=$3} END {print s+0}')"
[ "$PRODUCED" = "$ROWS" ] || { echo "seed mismatch: offsets sum=$PRODUCED != $ROWS"; KEEP=1; exit 1; }
PART_DIST="$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null \
  | awk -F: '{print $2":"$3}' | sort -t: -k1,1n | paste -sd' ' -)"

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

# ---------- datasources + tables（kafka 源 + file 汇） ----------
req POST /api/datasources "{\"name\":\"bench-kafka\",\"type\":\"kafka\",\"connJson\":{\"bootstrap\":\"127.0.0.1:$KPORT\"}}" "$TOKEN"
KDS="$(jq -r '.id // empty' <<<"$BODY")"
[ -n "$KDS" ] || { echo "kafka datasource failed: $CODE $BODY"; exit 1; }
req POST /api/datasources "{\"name\":\"bench-files\",\"type\":\"file\",\"connJson\":{\"path\":\"$WORK/node\"}}" "$TOKEN"
FDS="$(jq -r '.id // empty' <<<"$BODY")"
[ -n "$FDS" ] || { echo "file datasource failed: $CODE $BODY"; exit 1; }
COLS='[["order_no","utf8"],["qty","int64"],["price","float64"],["note","utf8"]]'
req POST "/api/datasources/$KDS/tables" "{\"dbName\":\"kafka\",\"tableName\":\"$TOPIC\",\"format\":\"json\",\"columnsJson\":$COLS}" "$TOKEN"
req POST "/api/datasources/$FDS/tables" "{\"dbName\":\"local\",\"tableName\":\"orders_out\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"

# ---------- engine node ----------
( cd "$WORK/node" && exec "$BIN_DIR/tailrace-node" serve \
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

# ---------- pipeline: create → publish → deploy（t0 = deploy ok） ----------
SQL="INSERT INTO local.orders_out SELECT order_no, qty, price, note FROM kafka.$TOPIC"
req POST /api/pipelines "{\"name\":\"bench-parts-copy\",\"sqlContent\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$FDS,\"kafka\":$KDS},\"checkpointMs\":2000}" "$TOKEN"
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
  [ "$(jq -r '.status // empty' <<<"$BODY")" = "FAILED" ] && { echo "pipeline FAILED: $BODY"; KEEP=1; exit 1; }
  sleep 0.5
done
T1="$(now)"

OUT_LINES=0
[ -f "$WORK/node/orders_out" ] && OUT_LINES="$(wc -l < "$WORK/node/orders_out" | tr -d ' ')"
[ "$OK" = "1" ] || { echo "TIMEOUT after ${TIMEOUT_SECS}s: consumed=$CONSUMED flushed=$FLUSHED (see $WORK with MACRO_KEEP=1)"; KEEP=1; exit 1; }
[ "$OUT_LINES" -eq "$ROWS" ] || { echo "sink rows mismatch: orders_out lines=$OUT_LINES != $ROWS (no-loss assertion)"; KEEP=1; exit 1; }

ELAPSED="$(awk -v a="$T0" -v b="$T1" 'BEGIN { printf "%.3f", b-a }')"
THROUGHPUT="$(awk -v r="$ROWS" -v e="$ELAPSED" 'BEGIN { printf "%.0f", r/e }')"

# ---------- report ----------
GIT_REV="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DATE="$(date '+%Y-%m-%d %H:%M %Z')"
SUMMARY_LINES=(
  "# tailrace 多分区宏基准报告（scripts/bench/macro-partitions.sh）"
  ""
  "- 日期: $DATE"
  "- 引擎: git@${GIT_REV}，二进制档位: **$MODE**（debug=未优化构建，数字仅作同机纵向基线）"
  "- 口径: kafka(P=$PARTITIONS 分区)→file 全链追平——N 行先播种进 topic（不计时），t0=deploy 返回 200，t1=stats flushed≥N；吞吐=N/(t1−t0)。"
  "- 行形态: JSON 4 列（order_no utf8 / qty int64 / price float64 / note utf8），行宽 ≈95B；分区分布: $PART_DIST"
  "- 播种耗时（setup，不计入吞吐）: ${SEED_ELAPSED}s（console-producer）"
  ""
  "## 结果"
  ""
  "| rows | partitions | elapsed_s | throughput_rows_per_s | sink_lines_verified |"
  "|---|---|---|---|---|"
  "| $ROWS | $PARTITIONS | $ELAPSED | **$THROUGHPUT** | $OUT_LINES |"
  ""
  "吞吐行: throughput=$THROUGHPUT rows/s（rows=$ROWS, partitions=$PARTITIONS, elapsed=${ELAPSED}s, mode=$MODE, git@${GIT_REV}）"
)
printf '%s\n' "${SUMMARY_LINES[@]}"
if [ -n "$REPORT" ]; then
  mkdir -p "$(dirname "$REPORT")"
  printf '%s\n' "${SUMMARY_LINES[@]}" > "$REPORT"
  echo "== report written: $REPORT =="
fi
kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
