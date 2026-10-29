#!/usr/bin/env bash
# Probe 2: does the `columns` header make partial_columns retain old values?
set -uo pipefail
cd /Users/wagic/GLMProjects/tailrace
OV=/tmp/tailrace-pu-probe/compose-be-swap-override.yml
m() { docker compose -f docker-compose.yml -f "$OV" exec -T doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "$1" 2>&1; }
BE=http://127.0.0.1:38040
DB=pu_probe
load() { local t="$1" l="$2" f="$3"; shift 3
  curl -s -X PUT -u root: -H "label: $l" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" "$@" --data-binary "@$f" "$BE/api/$DB/$t/_stream_load" | grep -E '"Status"|"Message"|NumberLoadedRows'
}
echo "=== A2: UNIQUE MoW, full load then partial WITH columns header ==="
m "DROP TABLE IF EXISTS $DB.a2; CREATE TABLE $DB.a2 (order_no VARCHAR(65533) NOT NULL, qty BIGINT, note VARCHAR(65533), price DOUBLE, flag BOOLEAN) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");"
load a2 a2_full_$RANDOM /tmp/tailrace-pu-probe/a_full.jsonl
sleep 1; m "SELECT * FROM $DB.a2 ORDER BY order_no;"
echo "--- partial + columns: order_no,qty ---"
load a2 a2_part_$RANDOM /tmp/tailrace-pu-probe/a_part.jsonl -H "partial_columns: true" -H "columns: order_no,qty"
sleep 1; m "SELECT * FROM $DB.a2 ORDER BY order_no;"
echo "--- partial + columns header but JSON carries an EXTRA key (note) not in columns → ignored? ---"
printf '{"order_no":"A-1","qty":1000,"note":"should-be-ignored"}\n' > /tmp/tailrace-pu-probe/a_extra.jsonl
load a2 a2_extra_$RANDOM /tmp/tailrace-pu-probe/a_extra.jsonl -H "partial_columns: true" -H "columns: order_no,qty"
sleep 1; m "SELECT * FROM $DB.a2 ORDER BY order_no;"
echo "--- columns header WITHOUT partial_columns → full replace (others NULL) ---"
printf '{"order_no":"A-2","qty":2000}\n' > /tmp/tailrace-pu-probe/a_colsonly.jsonl
load a2 a2_colsonly_$RANDOM /tmp/tailrace-pu-probe/a_colsonly.jsonl -H "columns: order_no,qty"
sleep 1; m "SELECT * FROM $DB.a2 ORDER BY order_no;"

echo "=== C2: sequence_col table, partial WITH columns header incl seq ==="
m "DROP TABLE IF EXISTS $DB.c2; CREATE TABLE $DB.c2 (order_no VARCHAR(65533) NOT NULL, seq BIGINT, qty BIGINT, note VARCHAR(65533)) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\",\"function_column.sequence_col\"=\"seq\");"
load c2 c2_full_$RANDOM /tmp/tailrace-pu-probe/c_full.jsonl
sleep 1; m "SELECT * FROM $DB.c2;"
echo "--- partial seq=20 + columns: order_no,seq,qty + sequence_column: seq ---"
load c2 c2_hi_$RANDOM /tmp/tailrace-pu-probe/c_part_hi.jsonl -H "partial_columns: true" -H "columns: order_no,seq,qty" -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.c2;"
echo "--- partial LOWER seq=5 + columns + sequence_column → should be discarded ---"
load c2 c2_lo_$RANDOM /tmp/tailrace-pu-probe/c_part_lo.jsonl -H "partial_columns: true" -H "columns: order_no,seq,qty" -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.c2;"
echo "--- partial, columns WITHOUT seq (order_no,qty) on a sequence_col table → Doris verdict ---"
load c2 c2_noseq_$RANDOM /tmp/tailrace-pu-probe/c_noseq.jsonl -H "partial_columns: true" -H "columns: order_no,qty"
sleep 1; m "SELECT * FROM $DB.c2;"
echo "--- partial, columns incl seq, NO sequence_column header → does seq ordering still apply? (seq=5 lower) ---"
load c2 c2_nohdr_$RANDOM /tmp/tailrace-pu-probe/c_part_lo.jsonl -H "partial_columns: true" -H "columns: order_no,seq,qty"
sleep 1; m "SELECT * FROM $DB.c2;"

echo "=== D2: NO sequence_col property; A1-generator shape (seq is a plain column) + engine headers ==="
m "DROP TABLE IF EXISTS $DB.d2; CREATE TABLE $DB.d2 (order_no VARCHAR(65533) NOT NULL, seq BIGINT, qty BIGINT, note VARCHAR(65533)) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");"
load d2 d2_full_$RANDOM /tmp/tailrace-pu-probe/c_full.jsonl -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.d2;"
echo "--- partial seq=20 + columns + sequence_column header (table has no sequence_col) ---"
load d2 d2_hi_$RANDOM /tmp/tailrace-pu-probe/c_part_hi.jsonl -H "partial_columns: true" -H "columns: order_no,seq,qty" -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.d2;"
echo "--- partial LOWER seq=5 (no sequence_col property → ordering NOT enforced? expect overwrite) ---"
load d2 d2_lo_$RANDOM /tmp/tailrace-pu-probe/c_part_lo.jsonl -H "partial_columns: true" -H "columns: order_no,seq,qty" -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.d2;"
echo "=== DONE ==="
