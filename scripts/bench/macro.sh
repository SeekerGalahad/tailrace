#!/usr/bin/env bash
# tailrace 宏基准（roadmap §四 T1 / §三 L4 / §九 T32）：file→file / file→doris /
# file→starrocks 全链吞吐。
#
# 链路与口径（与 docs/BENCHMARKS.md §方法学 保持一致）：
#   console + engine node 真进程（默认 target/debug 二进制 —— 未优化构建，
#   数字仅作纵向对比基线；--mode release 用 release 二进制）；
#   种子：--rows 行 JSON（order_no/qty/price/note 4 列，行宽 ≈95B）一次性
#   写满源文件 → deploy → 轮询 stats 至 flushed ≥ N；
#   elapsed = deploy 返回 200 → flushed≥N 的墙钟（含管道启动，不含 console/
#   node 进程启动与种子写入）；吞吐 = N / elapsed。
#   断言：sink 侧行数 ≥ N（file=行数；doris/starrocks=表 COUNT(*)）。
#
# 档位：
#   --sink file       （默认，历史行为不变）单管道 file→file；
#   --sink doris      docker-compose doris-fe+doris-be（kafka 不起，--slim 先例）
#                     → file→doris 单管道；--pipes P>1 起并发档：P 条管道各
#                     消费独立源文件（各 N/P 行）写入同一张 doris 表，
#                     --flush-rows R 经管道 config.tuning.flushRows 下发（引擎
#                     clamp 100..1_000_000，registry.rs）；elapsed 口径 =
#                     首条 deploy 200 → 全部管道 flushed≥各自目标；
#   --sink starrocks  compose --profile starrocks allin1。console 尚无
#                     starrocks datasource 类型（datasources.rs registry_type
#                     白名单不含），故走 tailrace-node run 直跑（T12/T24 e2e
#                     同路径）：t0 = 「pipeline run started (standalone)」日志
#                     时间戳（排除进程启动/装配），t1 = 表 COUNT(*)≥N 轮询达成
#                     —— 与 console 档口径有差（见报告注记行）。
#   环境契约（起 tailrace Doris/StarRocks 前，共享宿主机操作者配方，脚本不
#   代做）：磁盘 <5G 红线先查 df -h；be-2.1.7 检测活动 swap 拒启 → zram 用量
#   为 0 时 `docker run --rm --privileged alpine swapoff -a`；VM 8G 内存 →
#   临时 `docker stop doris-2.1 doris-be`（mdop 常驻栈）腾内存，完事回
#   `docker start`；vm.max_map_count 需 ≥2e6。正典 =
#   researchEverything/AGENTS-环境契约与工作纪律.md §3。
#   live-gate（对齐 scripts/e2e/starrocks.sh 惯例）：docker daemon 不可达或
#   compose up --wait 健康等待失败 → 打一行 [skip] 说明后 exit 0 空过（不造
#   数据）；栈成功后保留不自动拆（重跑免健康等待），收尾由操作者
#   `docker compose down -v` + 回挂 mdop 栈/swap。
#
# 用法：bash scripts/bench/macro.sh [--rows N] [--sink file|doris|starrocks]
#                              [--pipes P] [--flush-rows R] [--report FILE]
#                              [--mode debug|release]
#   --rows        种子行数（默认 100000；--pipes>1 时须被 P 整除）
#   --sink        目标 sink（默认 file）
#   --pipes       并发管道数（默认 1；>1 需 --sink doris；T32 并发档 = 4）
#   --flush-rows  攒批行阈值 flushRows（默认不传=引擎默认 10000；T32 并发档
#                 小批探针 = 500）
#   --poll-rows   poll 行粒度 pollRows（默认不传=引擎默认 8000）——pending 每次
#                 poll +poll_rows 行，批粒度地板=pollRows：小批探针须同传
#                 （如 --flush-rows 500 --poll-rows 500）
#   --report      报告落盘路径（markdown，同时打印摘要到 stdout；缺省仅打印）
#   --mode        二进制档位（默认 debug；release 缺二进制时会先构建）
#   环境变量与 MACRO_NODE_WRAP 沿用下方定义；TAILRACE_FE_MYSQL_PORT /
#   TAILRACE_BE_HTTP_PORT / TAILRACE_SR_PROXY_PORT / TAILRACE_SR_FE_MYSQL_PORT
#   沿用 docker-compose.yml 宿主端口覆盖（默认 39030/38040/48080/49030）。
#   MACRO_NODE_WRAP（T20 峰值 RSS 口径，默认空=行为不变）：node 进程的
#   前置包装命令，如 MACRO_NODE_WRAP="/usr/bin/time -l" MACRO_KEEP=1 ——
#   包装器的 rusage（macOS "maximum resident set size"，单位字节）落在
#   $MACRO_DIR/node.log 尾部；需 MACRO_KEEP=1 保留工作目录后自行提取。
set -uo pipefail
cd "$(dirname "$0")/../.."

ROOT="$(pwd)"
ROWS=100000
REPORT=""
MODE="debug"
SINK="file"
PIPES=1
FLUSH_ROWS=""
POLL_ROWS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --rows) ROWS="$2"; shift 2 ;;
    --report) REPORT="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --sink) SINK="$2"; shift 2 ;;
    --pipes) PIPES="$2"; shift 2 ;;
    --flush-rows) FLUSH_ROWS="$2"; shift 2 ;;
    --poll-rows) POLL_ROWS="$2"; shift 2 ;;
    *) echo "unknown arg: $1 (use --rows/--sink/--pipes/--flush-rows/--poll-rows/--report/--mode)"; exit 2 ;;
  esac
done

case "$SINK" in
  file|doris|starrocks) ;;
  *) echo "--sink must be file|doris|starrocks"; exit 2 ;;
esac
[ "$PIPES" -ge 1 ] 2>/dev/null || { echo "--pipes must be a positive integer"; exit 2; }
if [ "$PIPES" -gt 1 ]; then
  [ "$SINK" = "doris" ] || { echo "--pipes>1 requires --sink doris (concurrent tier is the doris group-commit probe)"; exit 2; }
  [ $(( ROWS % PIPES )) -eq 0 ] || { echo "--rows ($ROWS) must be divisible by --pipes ($PIPES)"; exit 2; }
fi
if [ -n "$FLUSH_ROWS" ]; then
  [ "$SINK" = "doris" ] || { echo "--flush-rows applies to --sink doris (engine tuning knob via console pipeline config)"; exit 2; }
  [ "$FLUSH_ROWS" -ge 100 ] 2>/dev/null || { echo "--flush-rows below the engine clamp floor 100 (registry.rs) has no effect; pass >=100"; exit 2; }
fi
if [ -n "$POLL_ROWS" ]; then
  [ "$SINK" = "doris" ] || { echo "--poll-rows applies to --sink doris (engine tuning knob via console pipeline config)"; exit 2; }
  [ "$POLL_ROWS" -ge 1 ] 2>/dev/null || { echo "--poll-rows must be >=1 (engine clamp 1..100000, registry.rs)"; exit 2; }
  # flush 行阈值按 pending 检查，而 pending 每次 poll 至少加 poll_rows 行——
  # poll_rows > flush_rows 时批粒度被 poll 托底（本卡 P=4 首跑实证 flushes=4×~6250）
  [ "$POLL_ROWS" -le "$FLUSH_ROWS" ] 2>/dev/null \
    || echo "WARN: --poll-rows ($POLL_ROWS) > --flush-rows ($FLUSH_ROWS) — actual batch size floors at poll granularity"
fi

CONSOLE_ADDR="${MACRO_CONSOLE_ADDR:-127.0.0.1:18280}"
NODE_ADDR="${MACRO_NODE_ADDR:-127.0.0.1:18300}"
BOOTSTRAP_TOKEN="${TAILRACE_BOOTSTRAP_TOKEN:-tr_b_macro_00112233445566778899aabbccddeeff}"
WORK="${MACRO_DIR:-/tmp/tailrace-macro-bench}"
KEEP="${MACRO_KEEP:-0}"
NODE_WRAP="${MACRO_NODE_WRAP:-}"
TIMEOUT_SECS=$(( ROWS / 500 + 120 ))   # <500 rows/s 即判失败（追赶余量 120s）
# tailrace compose stack ports (docker-compose.yml overrides; T32)
FEPORT="${TAILRACE_FE_MYSQL_PORT:-39030}"          # doris FE mysql (DDL/COUNT)
BPORT="${TAILRACE_BE_HTTP_PORT:-38040}"            # doris BE http (stream load)
SRPROXY="${TAILRACE_SR_PROXY_PORT:-48080}"         # starrocks feproxy (stream load)
SRFEPORT="${TAILRACE_SR_FE_MYSQL_PORT:-49030}"     # starrocks FE mysql (DDL/COUNT)
BENCH_DB="tailrace_bench"
COMPOSE_WAIT="${COMPOSE_WAIT:-420}"
SR_WAIT="${SR_WAIT:-300}"

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

# docker compose helpers (stack stays up after the run — see header; teardown is
# operator-side: docker compose down -v + mdop doris/swap restore per env contract)
tc_compose() { docker compose -f "$ROOT/docker-compose.yml" "$@"; }
compose_skip() { # compose_skip <what> <reason…> — live-gate degradation line
  echo "[skip] $1 unavailable ($2) — no numbers fabricated, tier registered as skipped"
  echo "[skip] teardown hint: docker compose -f $ROOT/docker-compose.yml down -v"
  exit 0
}
iso_to_epoch() { python3 -c 'import sys,datetime; print(f"{datetime.datetime.fromisoformat(sys.argv[1].replace("Z","+00:00")).timestamp():.3f}")' "$1"; }

# ======================= --sink starrocks（node 直跑档，T32 可选） =======================
# console 尚无 starrocks datasource 类型（registry_type 白名单，datasources.rs）——
# 本档走 tailrace-node run 直跑（scripts/e2e/starrocks.sh 同路径同 DDL/GRANT 配方）。
# 口径差异（报告注记同步）：t0 = 「pipeline run started (standalone)」日志时间戳
# （排除进程启动/装配，与 console 档的 deploy-200 t0 不同源）；t1 = FE COUNT(*)
# 轮询达成（~1s 步进，量化粗于 console 档的 0.5s stats 轮询）；攒批 = 引擎默认
# （10000 行 / 10s / 8MB，无 toml tuning 面）——与 doris 档 checkpointMs=2000
# 驱动的 flush_interval=2s 不同。仅作 sink 差异量级参考，不与 console 档直接比。
if [ "$SINK" = "starrocks" ]; then
  echo "== tailrace macro bench: rows=$ROWS mode=$MODE sink=starrocks (node-run tier) work=$WORK =="
  docker info >/dev/null 2>&1 || compose_skip "starrocks stack" "docker daemon unreachable"
  rm -rf "$WORK"; mkdir -p "$WORK/state" "$WORK/node"
  SR_USER="tailrace"; SR_PASS="tailrace_bench_pw"
  if ! tc_compose --profile starrocks up -d --wait --wait-timeout "$SR_WAIT" starrocks 2>"$WORK/compose.err"; then
    tc_compose --profile starrocks logs --tail 30 starrocks 2>/dev/null | tail -10
    compose_skip "starrocks allin1 stack" "compose up --wait failed (see logs above; $(tail -1 "$WORK/compose.err"))"
  fi
  sr_sql() { tc_compose exec -T starrocks mysql -N -h127.0.0.1 -P9030 -uroot -e "$1" 2>/dev/null; }
  # DDL 幂等（重跑净表）：DROP→CREATE→GRANT（表在前、GRANT 在后：ALL TABLES 只覆盖现存表）
  sr_sql "CREATE DATABASE IF NOT EXISTS $BENCH_DB;" >/dev/null
  sr_sql "SELECT 1;" >/dev/null   # 预热首轮 mysql 通道（--wait 后 BE 注册可能仍在收尾）
  sr_sql "DROP TABLE IF EXISTS $BENCH_DB.orders;" >/dev/null
  if ! sr_sql "CREATE TABLE $BENCH_DB.orders (order_no VARCHAR(64) NOT NULL, qty INT NOT NULL, price DOUBLE, note VARCHAR(128)) DUPLICATE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\" = \"1\");" \
     || ! sr_sql "CREATE USER IF NOT EXISTS '$SR_USER'@'%' IDENTIFIED BY '$SR_PASS';" \
     || ! sr_sql "GRANT INSERT,SELECT ON ALL TABLES IN DATABASE $BENCH_DB TO USER '$SR_USER'@'%';" \
     || [ -z "$(sr_sql "SHOW TABLES FROM $BENCH_DB LIKE 'orders';")" ]; then
    compose_skip "starrocks DDL/GRANT" "mysql DDL channel failed (FE 127.0.0.1:$SRFEPORT)"
  fi
  # 种子（同 file/doris 档行形态，一次性写满）
  awk -v n="$ROWS" 'BEGIN {
    for (i=0; i<n; i++)
      printf "{\"order_no\":\"O-%07d\",\"qty\":%d,\"price\":%d.5,\"note\":\"payload-%07d-abcdefghijklmnopqrstuvwxyz\"}\n", i, i, i%1000, i
  }' > "$WORK/node/orders_in"
  IN_BYTES="$(wc -c < "$WORK/node/orders_in" | tr -d ' ')"
  [ "$(wc -l < "$WORK/node/orders_in" | tr -d ' ')" = "$ROWS" ] || { echo "seed failed"; KEEP=1; exit 1; }
  AUTH_B64="$(printf '%s:%s' "$SR_USER" "$SR_PASS" | base64 | tr -d '\n')"
  cat > "$WORK/pipeline.toml" <<EOF
[pipeline]
id = 4321
name = "macro-sr"

[[datasource]]
name = "in"
type = "file"

[[datasource]]
name = "sr"
type = "starrocks"
stream_load_url = "http://127.0.0.1:$SRPROXY/api/$BENCH_DB/orders/_stream_load"
auth_env = "TAILRACE_SR_AUTH"

[[table]]
datasource = "in"
name = "orders_in"
kind = "file"
format = "jsonl"
columns = [["order_no","utf8"],["qty","int64"],["price","float64"],["note","utf8"]]

[[table]]
datasource = "sr"
name = "orders"
kind = "starrocks"
label_prefix = "macro_sr"
columns = [["order_no","utf8"],["qty","int64"],["price","float64"],["note","utf8"]]

[[statement]]
source = "in.orders_in"
group = "tr.macro_sr.s0"
sink = "sr.orders"
select_sql = "SELECT order_no, qty, price, note FROM src"
EOF
  echo "== running tailrace-node (secs budget $TIMEOUT_SECS) =="
  # 单层 subshell + exec（NODE_PID=node 本体；多一层外壳会孤儿化——doris 档同因）
  (
    cd "$WORK/node" || exit 1
    export TAILRACE_SR_AUTH="Basic $AUTH_B64"
    exec "$BIN_DIR/tailrace-node" run "$WORK/pipeline.toml" \
      --state-dir "$WORK/state" --secs "$TIMEOUT_SECS"
  ) >"$WORK/stdout.log" 2>"$WORK/node.log" &
  NODE_PID=$!
  # t0 = run-start 日志时间戳（等待该行出现，上限 30s；tracing 输出带 ANSI 码先剥）
  T0=""
  for i in $(seq 1 30); do
    ts="$(grep -m1 'pipeline run started (standalone)' "$WORK/node.log" 2>/dev/null | perl -pe 's/\e\[[0-9;]*m//g' | awk '{print $1}')"
    if [ -n "$ts" ]; then T0="$(iso_to_epoch "$ts")"; break; fi
    sleep 1
  done
  [ -n "$T0" ] || { echo "no run-start log line in node.log: $(tail -5 "$WORK/node.log")"; KEEP=1; exit 1; }
  # t1 = 表 COUNT(*) ≥ ROWS 轮询达成（~1s 步进，FE COUNT 即 sink 侧真相）
  SR_ROWS=0; OK=0; POLL_START="$(now)"
  while :; do
    SR_ROWS="$(sr_sql "SELECT COUNT(*) FROM $BENCH_DB.orders;" | tr -d '[:space:]')"
    [ "${SR_ROWS:-0}" -ge "$ROWS" ] && { OK=1; break; }
    awk -v a="$POLL_START" -v b="$(now)" -v lim="$TIMEOUT_SECS" 'BEGIN { exit (b-a >= lim) ? 1 : 0 }' || break
    kill -0 "$NODE_PID" 2>/dev/null || break
    sleep 1
  done
  T1="$(now)"
  kill -TERM "$NODE_PID" 2>/dev/null; wait "$NODE_PID" 2>/dev/null; NODE_PID=""
  [ "$OK" = "1" ] || { echo "TIMEOUT/incomplete after ${TIMEOUT_SECS}s: table rows=$SR_ROWS < $ROWS (see $WORK with MACRO_KEEP=1)"; KEEP=1; exit 1; }
  ELAPSED="$(awk -v a="$T0" -v b="$T1" 'BEGIN { printf "%.3f", b-a }')"
  THROUGHPUT="$(awk -v r="$ROWS" -v e="$ELAPSED" 'BEGIN { printf "%.0f", r/e }')"
  MBPS="$(awk -v b="$IN_BYTES" -v e="$ELAPSED" 'BEGIN { printf "%.2f", (b/1048576)/e }')"
  GIT_REV="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  DATE="$(date '+%Y-%m-%d %H:%M %Z')"
  SUMMARY_LINES=(
    "# tailrace 宏基准报告（scripts/bench/macro.sh --sink starrocks）"
    ""
    "- 日期: $DATE"
    "- 引擎: git@${GIT_REV}，二进制档位: **$MODE**（debug=未优化构建，数字仅作同机纵向基线）"
    "- 口径: file→starrocks node 直跑——种子 N 行先写满源文件，t0=「pipeline run started (standalone)」日志时间戳（排除进程启动/装配），t1=FE COUNT(*)≥N 轮询达成（~1s 步进）；吞吐=N/(t1−t0)。攒批=引擎默认（10000 行/10s/8MB）。与 --sink doris/file 的 console 档（t0=deploy 200、stats 0.5s 轮询、checkpointMs=2000→flush_interval 2s）口径不同源，仅作 sink 差异量级参考。"
    "- 行形态: JSON 4 列（order_no utf8 / qty int64 / price float64 / note utf8），行宽 ≈95B"
    "- 栈: starrocks/allin1-ubuntu（feproxy :$SRPROXY 拦 307，引擎 307 跟随路径不在本档——conn-doris mock 契约覆盖）；运行后栈保留（操作者收尾 down -v）"
    ""
    "## 结果"
    ""
    "| rows | elapsed_s | throughput_rows_per_s | input_MB | input_MB_per_s | table_count_verified |"
    "|---|---|---|---|---|---|"
    "| $ROWS | $ELAPSED | **$THROUGHPUT** | $(awk -v b="$IN_BYTES" 'BEGIN{printf "%.1f", b/1048576}') | $MBPS | $SR_ROWS |"
    ""
    "吞吐行: throughput=$THROUGHPUT rows/s（rows=$ROWS, elapsed=${ELAPSED}s, mode=$MODE, sink=starrocks, git@${GIT_REV}）"
  )
  printf '%s\n' "${SUMMARY_LINES[@]}"
  if [ -n "$REPORT" ]; then
    mkdir -p "$(dirname "$REPORT")"
    printf '%s\n' "${SUMMARY_LINES[@]}" > "$REPORT"
    echo "== report written: $REPORT =="
  fi
  exit 0
fi


echo "== tailrace macro bench: rows=$ROWS mode=$MODE sink=$SINK pipes=$PIPES flush_rows=${FLUSH_ROWS:-default} work=$WORK =="
rm -rf "$WORK"; mkdir -p "$WORK/run" "$WORK/node" "$WORK/node-state"

# ---------- doris 栈（--sink doris）：live-gate + compose up + DDL ----------
DORIS_MYSQL() { tc_compose exec -T doris-fe mysql -h127.0.0.1 -P9030 -uroot "$@"; }   # smoke.sh:143 同配方
DDS=""; DDS_ENV_NAME=""; DDS_ENV_VALUE="Basic cm9vdDo="   # root:（空密码，新容器正典）
if [ "$SINK" = "doris" ]; then
  docker info >/dev/null 2>&1 || compose_skip "doris stack" "docker daemon unreachable"
  # 只起 fe+be（kafka 不起——本档链路是 file→doris；smoke.sh --slim 先例）。
  # 若 BE 因活动 swap / max_map_count / 内存不足起不来，--wait 在此失败 → 降级。
  echo "== docker compose up doris-fe doris-be (kafka intentionally absent; wait ≤${COMPOSE_WAIT}s) =="
  if ! tc_compose up -d --wait --wait-timeout "$COMPOSE_WAIT" doris-fe doris-be 2>"$WORK/compose.err"; then
    tc_compose logs --tail 30 doris-fe doris-be 2>/dev/null | tail -12
    compose_skip "doris stack" "compose up --wait failed (swap/max_map_count/memory? see logs above; $(tail -1 "$WORK/compose.err"))"
  fi
  # DDL 幂等（重跑净表）；retry：--wait 健康后 DDL 通道偶发未就绪（smoke doris_exec 先例）
  doris_exec() { local r; for r in $(seq 1 6); do DORIS_MYSQL -e "$1" >/dev/null 2>&1 && return 0; sleep 5; done; return 1; }
  doris_exec "CREATE DATABASE IF NOT EXISTS $BENCH_DB;" || compose_skip "doris DDL" "FE mysql channel 127.0.0.1:$FEPORT unreachable after retries"
  doris_exec "DROP TABLE IF EXISTS $BENCH_DB.orders;"
  doris_exec "CREATE TABLE $BENCH_DB.orders (order_no VARCHAR(64) NOT NULL, qty INT NOT NULL, price DOUBLE, note VARCHAR(128)) DUPLICATE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES('replication_num' = '1');" \
    || compose_skip "doris DDL" "CREATE TABLE failed on 127.0.0.1:$FEPORT"
  [ -n "$(DORIS_MYSQL -N -e "SHOW TABLES FROM $BENCH_DB LIKE 'orders';" 2>/dev/null)" ] \
    || compose_skip "doris DDL" "table $BENCH_DB.orders not visible after create"
fi

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
if [ "$SINK" = "file" ]; then
  req POST "/api/datasources/$DS_ID/tables" "{\"dbName\":\"local\",\"tableName\":\"orders_in\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
  req POST "/api/datasources/$DS_ID/tables" "{\"dbName\":\"local\",\"tableName\":\"orders_out\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
else
  # 源表对象：每管道一张（orders_in_p0..p{P-1}），互不共享位点
  k=0
  while [ "$k" -lt "$PIPES" ]; do
    req POST "/api/datasources/$DS_ID/tables" "{\"dbName\":\"local\",\"tableName\":\"orders_in_p$k\",\"format\":\"jsonl\",\"columnsJson\":$COLS}" "$TOKEN"
    [ "$CODE" = "201" ] || [ "$CODE" = "200" ] || { echo "source table orders_in_p$k failed: $CODE $BODY"; exit 1; }
    k=$((k+1))
  done
  # doris datasource（jdbcUrl+streamLoadUrl 双必填——T11 回归口径；username 封存
  # 凭证 → authEnv 门，node 进程须在 spawn 前拿到 TAILRACE_DS_<id>）
  req POST /api/datasources "{\"name\":\"macro-doris\",\"type\":\"doris\",\"connJson\":{\"jdbcUrl\":\"jdbc:mysql://127.0.0.1:$FEPORT\",\"streamLoadUrl\":\"http://127.0.0.1:$BPORT\"},\"username\":\"root\",\"password\":\"\"}" "$TOKEN"
  DDS="$(jq -r '.id // empty' <<<"$BODY")"
  [ -n "$DDS" ] || { echo "doris datasource failed: $CODE $BODY"; exit 1; }
  DDS_ENV_NAME="TAILRACE_DS_$DDS"
  # sink 表对象（两段 dbName 须与 dsMapping 键一致，pipelines.rs plan 表装配）
  req POST "/api/datasources/$DDS/tables" "{\"dbName\":\"$BENCH_DB\",\"tableName\":\"orders\",\"format\":null,\"columnsJson\":$COLS}" "$TOKEN"
  { [ "$CODE" = "201" ] || [ "$CODE" = "200" ]; } || { echo "doris sink table object failed: $CODE $BODY"; exit 1; }
fi

# ---------- seed: N rows, one shot ----------
ROWS_PER_PIPE=$(( ROWS / PIPES ))
if [ "$SINK" = "file" ]; then
  awk -v n="$ROWS" 'BEGIN {
    for (i=0; i<n; i++)
      printf "{\"order_no\":\"O-%07d\",\"qty\":%d,\"price\":%d.5,\"note\":\"payload-%07d-abcdefghijklmnopqrstuvwxyz\"}\n", i, i, i%1000, i
  }' > "$WORK/node/orders_in"
  IN_BYTES="$(wc -c < "$WORK/node/orders_in" | tr -d ' ')"
else
  # 每管道独立源文件，各 N/P 行；order_no 前缀带管道号防跨管道重复键干扰排查
  k=0
  while [ "$k" -lt "$PIPES" ]; do
    awk -v n="$ROWS_PER_PIPE" -v p="$k" 'BEGIN {
      for (i=0; i<n; i++)
        printf "{\"order_no\":\"P%d-O-%07d\",\"qty\":%d,\"price\":%d.5,\"note\":\"payload-%07d-abcdefghijklmnopqrstuvwxyz\"}\n", p, i, i, i%1000, i
    }' > "$WORK/node/orders_in_p$k"
    [ "$(wc -l < "$WORK/node/orders_in_p$k" | tr -d ' ')" = "$ROWS_PER_PIPE" ] || { echo "seed orders_in_p$k failed"; exit 1; }
    k=$((k+1))
  done
  IN_BYTES="$(cat "$WORK"/node/orders_in_p* | wc -c | tr -d ' ')"
fi

# ---------- engine node ----------
# 单层 subshell → exec：NODE_PID 必须就是 node（或 $NODE_WRAP 包装器）本体——
# 多一层 bash 外壳会让 kill -TERM 孤儿化 node（本卡首跑实测）。
# doris 档：sink 凭证经引擎进程环境变量注入（authEnv = TAILRACE_DS_<id>，
# datasource 必须先于 node spawn 建好拿 id——smoke.sh S3.9 同因）；值含空格
# （"Basic cm9vdDo="）不能走词拆分前缀——subshell 内 export 单引号参数传入。
# $NODE_WRAP 故意不加引号：空时展开为无、设为 "/usr/bin/time -l" 时按词拆成命令+参数
# shellcheck disable=SC2086
(
  cd "$WORK/node" || exit 1
  [ -n "$DDS_ENV_NAME" ] && export "$DDS_ENV_NAME=$DDS_ENV_VALUE"
  exec $NODE_WRAP "$BIN_DIR/tailrace-node" serve \
    --addr "$NODE_ADDR" \
    --control-plane "http://$CONSOLE_ADDR" \
    --advertise "$NODE_ADDR" \
    --state-dir "$WORK/node-state" \
    --heartbeat-secs 1 \
    --bootstrap-token "$BOOTSTRAP_TOKEN"
) >"$WORK/node.log" 2>&1 &
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

# ---------- pipeline(s): create → publish → deploy (t0 = first deploy ok) ----------
# 可选 tuning（--flush-rows / --poll-rows）：经 config.tuning 下发（plan options →
# registry.rs clamp：flushRows 100..1_000_000、pollRows 1..100_000）；未传 =
# 引擎默认（flushRows 10000 / pollRows 8000）。小批探针须同时压 pollRows ≤
# flushRows——pending 每次 poll +poll_rows 行，poll 粒度是批粒度的地板。
TUNING_SEG=""
[ -n "$FLUSH_ROWS" ] && TUNING_SEG="\"flushRows\":$FLUSH_ROWS"
[ -n "$POLL_ROWS" ] && TUNING_SEG="${TUNING_SEG:+$TUNING_SEG,}\"pollRows\":$POLL_ROWS"
[ -n "$TUNING_SEG" ] && TUNING_SEG="\"config\":{\"tuning\":{$TUNING_SEG}}"
PIDS=()
if [ "$SINK" = "file" ]; then
  SQL="INSERT INTO local.orders_out SELECT order_no, qty, price, note FROM local.orders_in"
  req POST /api/pipelines "{\"name\":\"macro-file-copy\",\"sqlContent\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$DS_ID},\"checkpointMs\":2000}" "$TOKEN"
  P_ID="$(jq -r '.id // empty' <<<"$BODY")"
  [ -n "$P_ID" ] || { echo "pipeline create failed: $CODE $BODY"; exit 1; }
  req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
  [ "$CODE" = "200" ] || { echo "publish failed: $CODE $BODY"; exit 1; }
  req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
  [ "$CODE" = "200" ] || { echo "deploy failed: $CODE $BODY"; exit 1; }
  T0="$(now)"
else
  T0=""
  k=0
  while [ "$k" -lt "$PIPES" ]; do
    SQL="INSERT INTO $BENCH_DB.orders SELECT order_no, qty, price, note FROM local.orders_in_p$k"
    req POST /api/pipelines "{\"name\":\"macro-doris-p$k\",\"sqlContent\":$(json_sql "$SQL"),\"dsMapping\":{\"local\":$DS_ID,\"$BENCH_DB\":$DDS},\"checkpointMs\":2000${TUNING_SEG:+,$TUNING_SEG}}" "$TOKEN"
    P_ID="$(jq -r '.id // empty' <<<"$BODY")"
    [ -n "$P_ID" ] || { echo "pipeline p$k create failed: $CODE $BODY"; exit 1; }
    req POST "/api/pipelines/$P_ID/publish" "" "$TOKEN"
    [ "$CODE" = "200" ] || { echo "pipeline p$k publish failed: $CODE $BODY"; exit 1; }
    req POST "/api/pipelines/$P_ID/deploy" '{}' "$TOKEN"
    [ "$CODE" = "200" ] || { echo "pipeline p$k deploy failed: $CODE $BODY"; exit 1; }
    [ -z "$T0" ] && T0="$(now)"   # 首条 deploy 200 = 并发档计时起点（早启管道的消费落在窗口内）
    PIDS+=("$P_ID")
    k=$((k+1))
  done
fi

# ---------- wait flushed ≥ N (all pipes for the concurrent tier) ----------
poll_pipe() { # poll_pipe <pipeline-id> <target-rows> → sets PP_FLUSHED/PP_CONSUMED
  req GET "/api/pipelines/$1/stats" "" "$TOKEN"
  PP_CONSUMED="$(jq -r '.stats.totals.consumed // 0' <<<"$BODY" 2>/dev/null)"
  PP_FLUSHED="$(jq -r '.stats.totals.flushed // 0' <<<"$BODY" 2>/dev/null)"
  PP_FLUSHES="$(jq -r '.stats.totals.flushes // empty' <<<"$BODY" 2>/dev/null)"
}
if [ "$SINK" = "file" ]; then
  FLUSHED=0; CONSUMED=0; OK=0
  POLL_START="$(now)"
  while :; do
    poll_pipe "$P_ID" "$ROWS"
    CONSUMED="$PP_CONSUMED"; FLUSHED="$PP_FLUSHED"
    if [ "${FLUSHED:-0}" -ge "$ROWS" ]; then OK=1; break; fi
    awk -v a="$POLL_START" -v b="$(now)" -v lim="$TIMEOUT_SECS" 'BEGIN { exit (b-a >= lim) ? 1 : 0 }' || break
    req GET "/api/pipelines/$P_ID" "" "$TOKEN"
    [ "$(jq -r '.status // empty' <<<"$BODY")" = "FAILED" ] && { echo "pipeline FAILED: $BODY"; exit 1; }
    sleep 0.5
  done
  T1="$(now)"
else
  # 并发档：全部管道 flushed ≥ 各自目标才收口（T1 = 最后一根达成时刻）
  OK=0
  POLL_START="$(now)"
  ALL_PIPE_STATS=""
  while :; do
    DONE_ALL=1
    ALL_PIPE_STATS=""
    for pid in "${PIDS[@]}"; do
      poll_pipe "$pid" "$ROWS_PER_PIPE"
      ALL_PIPE_STATS+="p$pid:consumed=${PP_CONSUMED:-0},flushed=${PP_FLUSHED:-0},flushes=${PP_FLUSHES:-n/a};"
      [ "${PP_FLUSHED:-0}" -ge "$ROWS_PER_PIPE" ] || DONE_ALL=0
    done
    [ "$DONE_ALL" = "1" ] && { OK=1; break; }
    awk -v a="$POLL_START" -v b="$(now)" -v lim="$TIMEOUT_SECS" 'BEGIN { exit (b-a >= lim) ? 1 : 0 }' || break
    for pid in "${PIDS[@]}"; do
      req GET "/api/pipelines/$pid" "" "$TOKEN"
      [ "$(jq -r '.status // empty' <<<"$BODY")" = "FAILED" ] && { echo "pipeline $pid FAILED: $BODY"; exit 1; }
    done
    sleep 0.5
  done
  T1="$(now)"
fi

# ---------- sink-side row verification ----------
if [ "$SINK" = "file" ]; then
  OUT_LINES=0
  [ -f "$WORK/node/orders_out" ] && OUT_LINES="$(wc -l < "$WORK/node/orders_out" | tr -d ' ')"
else
  # doris：COUNT(*) 经 FE mysql 轮询至 == ROWS（流式 load Success 后即席可见）
  OUT_LINES=0
  for i in $(seq 1 30); do
    OUT_LINES="$(DORIS_MYSQL -N -e "SELECT COUNT(*) FROM $BENCH_DB.orders;" 2>/dev/null | tr -d '[:space:]')"
    [ "${OUT_LINES:-0}" -ge "$ROWS" ] && break
    sleep 1
  done
fi
if [ "$OK" = "1" ]; then :; else
  if [ "$SINK" = "file" ]; then
    echo "TIMEOUT after ${TIMEOUT_SECS}s: consumed=$CONSUMED flushed=$FLUSHED (see $WORK with MACRO_KEEP=1)"
  else
    echo "TIMEOUT after ${TIMEOUT_SECS}s: $ALL_PIPE_STATS (see $WORK with MACRO_KEEP=1)"
  fi
  KEEP=1; exit 1
fi
[ "$OUT_LINES" -ge "$ROWS" ] || { echo "sink rows mismatch: $SINK count=$OUT_LINES < $ROWS"; KEEP=1; exit 1; }

ELAPSED="$(awk -v a="$T0" -v b="$T1" 'BEGIN { printf "%.3f", b-a }')"
THROUGHPUT="$(awk -v r="$ROWS" -v e="$ELAPSED" 'BEGIN { printf "%.0f", r/e }')"
MBPS="$(awk -v b="$IN_BYTES" -v e="$ELAPSED" 'BEGIN { printf "%.2f", (b/1048576)/e }')"

# ---------- report ----------
GIT_REV="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DATE="$(date '+%Y-%m-%d %H:%M %Z')"
if [ "$SINK" = "file" ]; then
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
else
  SUMMARY_LINES=(
    "# tailrace 宏基准报告（scripts/bench/macro.sh --sink doris）"
    ""
    "- 日期: $DATE"
    "- 引擎: git@${GIT_REV}，二进制档位: **$MODE**（debug=未优化构建，数字仅作同机纵向基线）"
    "- 口径: file→doris 全链追平（compose doris-fe/be-2.1.7，Stream Load 直连 BE :${BPORT}）——种子 N 行先写满源文件，t0=deploy 返回 200（并发档=首条 deploy 200），t1=全部管道 stats flushed≥各自目标；吞吐=N/(t1−t0)。含管道启动与 doris 流式 load 提交，不含 console/node 进程启动与种子写入。checkpointMs=2000 → flush_interval 2s；sink 侧以 FE COUNT(*) 复核。"
    "- 行形态: JSON 4 列（order_no utf8 / qty int64 / price float64 / note utf8），行宽 ≈95B；表 $BENCH_DB.orders（DUPLICATE KEY(order_no) BUCKETS 1，DUPLICATE 模型不去重——行数守恒可验）"
    "- 并发档（T32 group-commit 收益区探针）：P=$PIPES 管道各 N/P 行独立源文件写同一表；flushRows=${FLUSH_ROWS:-10000(默认)}、pollRows=${POLL_ROWS:-8000(默认)}。注：引擎 DorisSink 不发 Doris group_commit header（conn-doris send_once 仅 label+Expect）——本档量的是小批多管道在无 group commit 下的基线（T18 ④T3 判据数据面）；批粒度地板 = pollRows（pending 每 poll +poll_rows 行）"
    "- 逐管道末次 stats: $ALL_PIPE_STATS"
    "- 栈运行后保留（重跑免健康等待）；操作者收尾：docker compose -f $ROOT/docker-compose.yml down -v（mdop 常驻 doris-2.1/doris-be 与 swap 按环境契约回挂）"
    ""
    "## 结果"
    ""
    "| sink | pipes | flush_rows | poll_rows | rows | elapsed_s | throughput_rows_per_s | input_MB | input_MB_per_s | table_count_verified |"
    "|---|---|---|---|---|---|---|---|---|---|"
    "| doris | $PIPES | ${FLUSH_ROWS:-10000} | ${POLL_ROWS:-8000} | $ROWS | $ELAPSED | **$THROUGHPUT** | $(awk -v b="$IN_BYTES" 'BEGIN{printf "%.1f", b/1048576}') | $MBPS | $OUT_LINES |"
    ""
    "吞吐行: throughput=$THROUGHPUT rows/s（rows=$ROWS, pipes=$PIPES, flushRows=${FLUSH_ROWS:-default}, pollRows=${POLL_ROWS:-default}, elapsed=${ELAPSED}s, mode=$MODE, sink=doris, git@${GIT_REV}）"
  )
fi
printf '%s\n' "${SUMMARY_LINES[@]}"
if [ -n "$REPORT" ]; then
  mkdir -p "$(dirname "$REPORT")"
  printf '%s\n' "${SUMMARY_LINES[@]}" > "$REPORT"
  echo "== report written: $REPORT =="
fi
