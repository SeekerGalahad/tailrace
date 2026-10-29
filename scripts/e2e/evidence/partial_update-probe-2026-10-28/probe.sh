#!/usr/bin/env bash
# Manual semantic probe (throwaway): partial_columns × key model × sequence
# column on the compose Doris 2.1.7 stack. Output feeds the e2e assertions.
set -uo pipefail
cd /Users/wagic/GLMProjects/tailrace
OV=/tmp/tailrace-pu-probe/compose-be-swap-override.yml
m() { docker compose -f docker-compose.yml -f "$OV" exec -T doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "$1" 2>&1; }
BE=http://127.0.0.1:38040
DB=pu_probe
load() { # load <table> <label> <jsonl-file> [extra -H args...]
  local t="$1" l="$2" f="$3"; shift 3
  curl -s -X PUT -u root: -H "label: $l" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" "$@" --data-binary "@$f" "$BE/api/$DB/$t/_stream_load"
  echo
}

# wait BE alive
for i in $(seq 1 30); do
  m "SHOW BACKENDS;" | awk -F'\t' 'NR>1 && $10=="true"{f=1} END{exit !f}' && break; sleep 2
done
m "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB;"

echo "=========== A: UNIQUE MoW, no sequence col ==========="
m "CREATE TABLE $DB.a (order_no VARCHAR(65533) NOT NULL, qty BIGINT, note VARCHAR(65533), price DOUBLE, flag BOOLEAN) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");"
printf '{"order_no":"A-1","qty":1,"note":"first","price":1.5,"flag":true}\n{"order_no":"A-2","qty":2,"note":"first","price":2.5,"flag":false}\n' > /tmp/tailrace-pu-probe/a_full.jsonl
echo "--- A load1 full ---"; load a a_full_$RANDOM /tmp/tailrace-pu-probe/a_full.jsonl
sleep 1; m "SELECT * FROM $DB.a ORDER BY order_no;"
printf '{"order_no":"A-1","qty":100}\n{"order_no":"A-2","qty":200}\n' > /tmp/tailrace-pu-probe/a_part.jsonl
echo "--- A load2 partial (order_no,qty) ---"; load a a_part_$RANDOM /tmp/tailrace-pu-probe/a_part.jsonl -H "partial_columns: true"
sleep 1; m "SELECT * FROM $DB.a ORDER BY order_no;"
echo "--- A load3 partial WITHOUT partial_columns header (regression: full-replace nulls?) ---"
printf '{"order_no":"A-1","qty":111}\n' > /tmp/tailrace-pu-probe/a_part2.jsonl
load a a_nopart_$RANDOM /tmp/tailrace-pu-probe/a_part2.jsonl
sleep 1; m "SELECT * FROM $DB.a ORDER BY order_no;"
echo "--- A load4 partial missing KEY column (qty,note only) ---"
printf '{"qty":9,"note":"nokey"}\n' > /tmp/tailrace-pu-probe/a_nokey.jsonl
load a a_nokey_$RANDOM /tmp/tailrace-pu-probe/a_nokey.jsonl -H "partial_columns: true"
echo "--- A load5 partial NEW key (upsert insert path: unlisted cols → NULL/default) ---"
printf '{"order_no":"A-3","qty":3}\n' > /tmp/tailrace-pu-probe/a_new.jsonl
load a a_new_$RANDOM /tmp/tailrace-pu-probe/a_new.jsonl -H "partial_columns: true"
sleep 1; m "SELECT * FROM $DB.a ORDER BY order_no;"

echo "=========== B: DUPLICATE KEY + partial_columns ==========="
m "CREATE TABLE $DB.b (order_no VARCHAR(65533) NOT NULL, qty BIGINT, note VARCHAR(65533)) DUPLICATE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");"
echo "--- B partial load ---"; load b b_part_$RANDOM /tmp/tailrace-pu-probe/a_part.jsonl -H "partial_columns: true"

echo "=========== B2: UNIQUE MoR (merge_on_write=false) + partial_columns ==========="
m "CREATE TABLE $DB.b2 (order_no VARCHAR(65533) NOT NULL, qty BIGINT, note VARCHAR(65533)) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"false\");"
echo "--- B2 partial load ---"; load b2 b2_part_$RANDOM /tmp/tailrace-pu-probe/a_part.jsonl -H "partial_columns: true"

echo "=========== C: UNIQUE MoW + function_column.sequence_col=seq ==========="
m "CREATE TABLE $DB.c (order_no VARCHAR(65533) NOT NULL, seq BIGINT, qty BIGINT, note VARCHAR(65533)) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\",\"function_column.sequence_col\"=\"seq\");"
printf '{"order_no":"C-1","seq":10,"qty":1,"note":"first"}\n' > /tmp/tailrace-pu-probe/c_full.jsonl
echo "--- C load1 full (no header) ---"; load c c_full_$RANDOM /tmp/tailrace-pu-probe/c_full.jsonl
sleep 1; m "SELECT * FROM $DB.c;"
printf '{"order_no":"C-1","seq":20,"qty":100}\n' > /tmp/tailrace-pu-probe/c_part_hi.jsonl
echo "--- C load2 partial with seq=20 + sequence_column header ---"; load c c_hi_$RANDOM /tmp/tailrace-pu-probe/c_part_hi.jsonl -H "partial_columns: true" -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.c;"
printf '{"order_no":"C-1","seq":5,"qty":555}\n' > /tmp/tailrace-pu-probe/c_part_lo.jsonl
echo "--- C load3 partial with LOWER seq=5 (should be ignored by ordering) ---"; load c c_lo_$RANDOM /tmp/tailrace-pu-probe/c_part_lo.jsonl -H "partial_columns: true" -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.c;"
printf '{"order_no":"C-1","qty":777}\n' > /tmp/tailrace-pu-probe/c_noseq.jsonl
echo "--- C load4 partial WITHOUT seq in payload (engine refuses this shape; Doris verdict:) ---"; load c c_noseq_$RANDOM /tmp/tailrace-pu-probe/c_noseq.jsonl -H "partial_columns: true"
echo "--- C load5 partial with seq in payload but NO sequence_column header ---"; load c c_nohdr_$RANDOM /tmp/tailrace-pu-probe/c_part_hi.jsonl -H "partial_columns: true"
sleep 1; m "SELECT * FROM $DB.c;"

echo "=========== D: UNIQUE MoW WITHOUT sequence_col property, load sends sequence_column header ==========="
m "CREATE TABLE $DB.d (order_no VARCHAR(65533) NOT NULL, seq BIGINT, qty BIGINT, note VARCHAR(65533)) UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");"
echo "--- D load1 full + sequence_column header ---"; load d d_full_$RANDOM /tmp/tailrace-pu-probe/c_full.jsonl -H "sequence_column: seq"
echo "--- D load2 partial + sequence_column header ---"; load d d_part_$RANDOM /tmp/tailrace-pu-probe/c_part_hi.jsonl -H "partial_columns: true" -H "sequence_column: seq"
sleep 1; m "SELECT * FROM $DB.d;"
echo "--- D: can ALTER add sequence_col property? ---"
m "ALTER TABLE $DB.d ENABLE FEATURE \"SEQUENCE_LOAD\" WITH PROPERTIES (\"function_column.sequence_type\" = \"bigint\");"
m "SHOW CREATE TABLE $DB.d\G" | grep -i "sequence" || echo "(no sequence property after ALTER)"
echo "=========== DONE ==========="
