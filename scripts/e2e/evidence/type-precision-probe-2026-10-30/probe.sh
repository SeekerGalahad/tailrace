#!/usr/bin/env bash
# Type-precision probe (R8 W5, blueprint §7.7 G2 core row) — run BEFORE any
# e2e assertion is written (R7 workorder ③: probe first, always). Bare
# stream loads straight at the BE, no console/engine in the path: this pins
# what Doris itself does with DECIMAL(18,2) values and with the
# sequence_col property, per server version. The engine serializes
# decimal/timestamp/date columns as JSON *strings* (tailrace-conn-doris
# `_` fallback arm → ArrayFormatter → serde_json::to_string), so the string
# shapes are the load-bearing half of the matrix.
#
# Usage (one probe per server version):
#   bash probe.sh <label> <mysql-prefix...> -- <be-host-port>
# The mysql prefix runs INSIDE the fe container in both modes, so the mysql
# port is always the in-container 9030 (the host-mapped 39030 is only for
# outside callers — same lesson partial_update.sh bakes into doris_exec).
# Examples:
#   bash probe.sh compose-2.1.7 docker compose -f docker-compose.yml \
#        -f /tmp/tp-probe/compose-be-swap-override.yml exec -T doris-fe mysql -- 38040
#   bash probe.sh standing-3.1.4 docker exec -i doris-2.1 mysql -- 8040
#
# Matrix:
#   A  DECIMAL(18,2) conservation: json NUMBER payloads (12.34, 0.10,
#      -5.01, 99999999999999.99 = the 18,2 ceiling) → SELECT round trip;
#      three-decimal input 12.345 (round-vs-truncate, observation);
#      json STRING payloads ("12.34" …) — the exact shapes the engine emits.
#   B  sequence_col ordering (dogfooding #3 contrast):
#      B1 attr'd table (function_column.sequence_col=seq): full write at
#         seq=10 → partial write at LOWER seq=5 → discarded (protection on);
#      B2 attribute-less table (pre-R8 A1 generator shape): same loads →
#         the low-seq row OVERWRITES (the failure form the R8 generator fix
#         exists to prevent).
#   C  DATETIME(6)/DATE round trip with string payloads (engine shape):
#      '2026-10-30 12:34:56.789012' → DATETIME(6) keep; '2026-10-30' → DATE.
set -uo pipefail

LABEL="$1"; shift
# split the mysql prefix from the rest at "--"
MYSQL=()
while [ $# -gt 0 ]; do
  [ "$1" = "--" ] && { shift; break; }
  MYSQL+=("$1"); shift
done
BE_PORT="$1"
DB=tp_probe
M() { "${MYSQL[@]}" -h127.0.0.1 -P9030 -uroot -e "$1" 2>&1; }
load() { local t="$1" l="$2" f="$3"; shift 3
  curl -s -X PUT -u root: -H "label: $l" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" "$@" --data-binary "@$f" \
    "http://127.0.0.1:$BE_PORT/api/$DB/$t/_stream_load" \
    | grep -E '"Status"|"Message"|NumberLoadedRows' | head -3
}
sel() { sleep 1; M "SELECT $1 FROM $DB.$2 $3;" | tail -n +2; }

echo "### probe target: $LABEL (fe in-container 9030 be host 127.0.0.1:$BE_PORT)"
M "SELECT VERSION();" | tail -n +2

echo "=== A: DECIMAL(18,2) conservation (bare json stream load) ==="
M "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB;" >/dev/null
M "CREATE TABLE $DB.dec (id INT NOT NULL, amt DECIMAL(18,2)) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" >/dev/null
cat > /tmp/tp-probe-dec-num.jsonl <<'EOF'
{"id":1,"amt":12.34}
{"id":2,"amt":0.10}
{"id":3,"amt":-5.01}
{"id":4,"amt":99999999999999.99}
{"id":5,"amt":12.345}
EOF
echo "--- A1 json NUMBER payloads (12.34 / 0.10 / -5.01 / 18,2-ceiling / 12.345) ---"
load dec tp_dec_num_$RANDOM /tmp/tp-probe-dec-num.jsonl
sel "id, amt, CAST(amt AS DOUBLE)" dec "ORDER BY id"
echo "--- A2 json STRING payloads (the engine's exact emission shape) ---"
cat > /tmp/tp-probe-dec-str.jsonl <<'EOF'
{"id":11,"amt":"12.34"}
{"id":12,"amt":"0.10"}
{"id":13,"amt":"-5.01"}
{"id":14,"amt":"99999999999999.99"}
{"id":15,"amt":"12.345"}
EOF
load dec tp_dec_str_$RANDOM /tmp/tp-probe-dec-str.jsonl
sel "id, amt" dec "WHERE id >= 11 ORDER BY id"
echo "--- A3 exactness: per-row 0.01-level conservation check in SQL ---"
sel "COUNT(*) AS mismatched" dec "WHERE (id IN (1,11) AND amt <> 12.34) OR (id IN (2,12) AND amt <> 0.10) OR (id IN (3,13) AND amt <> -5.01) OR (id IN (4,14) AND amt <> 99999999999999.99)"

echo "=== B: sequence_col ordering (dogfooding #3 contrast) ==="
M "CREATE TABLE $DB.seq_attr (k VARCHAR(32) NOT NULL, seq BIGINT, qty BIGINT, note VARCHAR(32)) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\",\"function_column.sequence_col\"=\"seq\");" >/dev/null
M "CREATE TABLE $DB.seq_bare (k VARCHAR(32) NOT NULL, seq BIGINT, qty BIGINT, note VARCHAR(32)) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");" >/dev/null
cat > /tmp/tp-probe-seq-full.jsonl <<'EOF'
{"k":"K1","seq":10,"qty":1,"note":"first"}
EOF
cat > /tmp/tp-probe-seq-low.jsonl <<'EOF'
{"k":"K1","seq":5,"qty":555}
EOF
echo "--- B1 attr'd table: full write seq=10 ---"
load seq_attr tp_seq_a_full_$RANDOM /tmp/tp-probe-seq-full.jsonl
sel "*" seq_attr ""
echo "--- B1 attr'd table: partial LOWER seq=5 (with sequence_column header) ---"
load seq_attr tp_seq_a_low_$RANDOM /tmp/tp-probe-seq-low.jsonl -H "partial_columns: true" -H "columns: k,seq,qty" -H "sequence_column: seq"
sel "*" seq_attr ""
echo "--- B2 attribute-less table (pre-R8 generator shape): full write seq=10 ---"
load seq_bare tp_seq_b_full_$RANDOM /tmp/tp-probe-seq-full.jsonl -H "sequence_column: seq"
sel "*" seq_bare ""
echo "--- B2 attribute-less table: partial LOWER seq=5 → expect OVERWRITE (the failure form) ---"
load seq_bare tp_seq_b_low_$RANDOM /tmp/tp-probe-seq-low.jsonl -H "partial_columns: true" -H "columns: k,seq,qty" -H "sequence_column: seq"
sel "*" seq_bare ""

echo "=== C: DATETIME(6)/DATE round trip (engine string shape) ==="
M "CREATE TABLE $DB.dt (id INT NOT NULL, ts DATETIME(6), d DATE) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" >/dev/null
cat > /tmp/tp-probe-dt.jsonl <<'EOF'
{"id":1,"ts":"2026-10-30 12:34:56.789012","d":"2026-10-30"}
EOF
load dt tp_dt_$RANDOM /tmp/tp-probe-dt.jsonl
sel "id, ts, d" dt ""

echo "=== cleanup ==="
M "DROP DATABASE IF EXISTS $DB FORCE;" >/dev/null && echo "$LABEL: dropped $DB"
echo "### probe done: $LABEL"
