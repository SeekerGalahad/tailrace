#!/usr/bin/env bash
# ARRAY<FLOAT> probe (R9 W2 / T-A5, blueprint §7.1 `list<float32[,dim]>`) —
# run BEFORE the ddl mapping and the decoder's list path are written (R7
# workorder ③: probe first). Bare stream loads straight at the BE, no
# console/engine in the path: this pins what Doris itself does with an
# ARRAY<FLOAT> column — which DDL shapes create (and the FE's exact refusal
# of ARRAY as a key column, which the generator's key heuristic must step
# over like it does for VARIANT), how DESC spells the type back (the infer
# round trip), and which JSON value shapes a stream load accepts.
#
# The load-bearing question for the W4 e2e / sink side is B2/B3: the engine's
# DorisSink has no List arm — a List(Float32) cell falls through to the
# ArrayFormatter catch-all and reaches Doris as a JSON *string literal* of the
# arrow display form (`"[0.1, 0.2, 0.3]"`, spaces after commas), not as a bare
# JSON array. If Doris casts that string into the ARRAY column, the sink needs
# no list-aware emission arm; if it refuses, the sink must learn to emit list
# cells as bare arrays before the e2e can pass.
#
# Usage (one probe per server version):
#   bash probe.sh <label> <mysql-prefix...> -- <be-host-port>
# The mysql prefix runs INSIDE the fe container, so the mysql port is the
# in-container 9030 (same convention as the variant probe).
# Example (standing 3.1.4):
#   bash probe.sh standing-3.1.4 docker exec -i doris-2.1 mysql -- 8040
#
# Matrix:
#   A  DDL shapes: A1 DUPLICATE + ARRAY<FLOAT> value column WITH a column
#      COMMENT (the dim annotation the generator emits); A2 UNIQUE MoW +
#      ARRAY<FLOAT> value column; A3 ARRAY<FLOAT> as KEY column (expect
#      refusal, message captured); A4 ARRAY<FLOAT> as RANGE partition column
#      (expect refusal); A5 SHOW CREATE TABLE + DESC spelling.
#   B  load shapes into the DUPLICATE table, then SELECT + array functions:
#      B1 bare JSON array of floats — the canonical shape;
#      B2 JSON STRING form of a compact array ("[0.1,0.2,0.3]");
#      B3 JSON STRING form with spaces ("[0.1, 0.2, 0.3]") — THE ARROW DISPLAY
#         SHAPE the current sink would emit;
#      B4 integers in the array ([1,2,3]) — widen to FLOAT?;
#      B5 json null / key absent → NULL array?;
#      B6 empty array [];
#      B7 a non-numeric element (["a",2,3]) — filtered row or NULL element?;
#      B8 nested array ([[1,2],[3]]) — filtered?;
#      B9 a longer array (5 elements) — Doris has no dim, must accept;
#      B10 a scalar where the array goes (42) — filtered?
#   C  UNIQUE MoW × partial_columns: update of a sibling column preserves the
#      array value? update of the array column replaces it whole?
set -uo pipefail

LABEL="$1"; shift
MYSQL=()
while [ $# -gt 0 ]; do
  [ "$1" = "--" ] && { shift; break; }
  MYSQL+=("$1"); shift
done
BE_PORT="$1"
DB=vec_probe
M() { "${MYSQL[@]}" -h127.0.0.1 -P9030 -uroot -e "$1" 2>&1; }
load() { local t="$1" l="$2" f="$3"; shift 3
  curl -s -X PUT -u root: -H "label: $l" -H "format: json" -H "read_json_by_line: true" \
    -H "Expect: 100-continue" "$@" --data-binary "@$f" \
    "http://127.0.0.1:$BE_PORT/api/$DB/$t/_stream_load" \
    | grep -E '"Status"|"Message"|NumberLoadedRows|NumberFilteredRows|ErrorURL' | head -5
}
sel() { sleep 1; M "SELECT $1 FROM $DB.$2 $3;" | tail -n +2; }

echo "### probe target: $LABEL (fe in-container 9030 be host 127.0.0.1:$BE_PORT)"
M "SHOW FRONTENDS\G" | grep -i "^ *Version"

echo "=== A: DDL shapes ==="
M "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB;" >/dev/null
echo "--- A1 DUPLICATE KEY + ARRAY<FLOAT> value column with COMMENT ---"
M "CREATE TABLE $DB.d (id INT NOT NULL, emb ARRAY<FLOAT> COMMENT 'vector dim=3') DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" && echo "A1: created"
echo "--- A2 UNIQUE KEY MoW + ARRAY<FLOAT> value column ---"
M "CREATE TABLE $DB.u (id INT NOT NULL, name VARCHAR(32), emb ARRAY<FLOAT>) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");" && echo "A2: created"
echo "--- A3 ARRAY<FLOAT> as KEY column (expect refusal) ---"
M "CREATE TABLE $DB.k (emb ARRAY<FLOAT> NOT NULL, id INT) DUPLICATE KEY(emb) DISTRIBUTED BY HASH(emb) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");"
echo "--- A4 ARRAY<FLOAT> as RANGE partition column (expect refusal) ---"
M "CREATE TABLE $DB.p (id INT NOT NULL, emb ARRAY<FLOAT>) DUPLICATE KEY(id) PARTITION BY RANGE(emb) () DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");"
echo "--- A5 SHOW CREATE TABLE d (column lines) / DESC d ---"
M "SHOW CREATE TABLE $DB.d\G" | grep -E '^\s+`'
M "DESC $DB.d;"

echo "=== B: load shapes → SELECT (DUPLICATE table d) ==="
cat > /tmp/vec-probe-b.jsonl <<'EOF'
{"id":1,"emb":[0.1,0.2,0.3]}
{"id":2,"emb":"[0.1,0.2,0.3]"}
{"id":3,"emb":"[0.1, 0.2, 0.3]"}
{"id":4,"emb":[1,2,3]}
{"id":5,"emb":null}
{"id":6}
{"id":7,"emb":[]}
{"id":9,"emb":[0.1,0.2,0.3,0.4,0.5]}
EOF
echo "--- B load (well-formed shapes): B1 bare array / B2 json-string compact / B3 json-string spaced / B4 ints / B5 null,absent / B6 empty / B9 longer ---"
load d vec_b_$RANDOM /tmp/vec-probe-b.jsonl
echo "--- B raw: SELECT id, emb, array_size(emb), element_at(emb, 2) ---"
sel "id, emb, array_size(emb) AS n, element_at(emb, 2) AS e2" d "ORDER BY id"
echo "--- B7 non-numeric element (own load, max_filter_ratio 1) ---"
echo '{"id":10,"emb":["a",2,3]}' > /tmp/vec-probe-b7.jsonl
load d vec_b7_$RANDOM /tmp/vec-probe-b7.jsonl -H "max_filter_ratio: 1"
sel "id, emb" d "WHERE id = 10"
echo "--- B8 nested array (own load, max_filter_ratio 1) ---"
echo '{"id":11,"emb":[[1,2],[3]]}' > /tmp/vec-probe-b8.jsonl
load d vec_b8_$RANDOM /tmp/vec-probe-b8.jsonl -H "max_filter_ratio: 1"
sel "id, emb" d "WHERE id = 11"
echo "--- B10 scalar where the array goes (own load, max_filter_ratio 1) ---"
echo '{"id":12,"emb":42}' > /tmp/vec-probe-b10.jsonl
load d vec_b10_$RANDOM /tmp/vec-probe-b10.jsonl -H "max_filter_ratio: 1"
sel "id, emb" d "WHERE id = 12"

echo "=== C: UNIQUE MoW × partial_columns (table u) ==="
echo '{"id":1,"name":"first","emb":[0.5,0.6,0.7]}' > /tmp/vec-probe-c-full.jsonl
echo '{"id":1,"name":"renamed"}' > /tmp/vec-probe-c-name.jsonl
echo '{"id":1,"emb":[9.0,9.0,9.0]}' > /tmp/vec-probe-c-emb.jsonl
echo "--- C1 full write ---"
load u vec_c_full_$RANDOM /tmp/vec-probe-c-full.jsonl
sel "id, name, emb" u ""
echo "--- C1 partial: columns id,name only → emb preserved? ---"
load u vec_c_name_$RANDOM /tmp/vec-probe-c-name.jsonl -H "partial_columns: true" -H "columns: id,name"
sel "id, name, emb" u ""
echo "--- C2 partial: columns id,emb → whole replacement? name preserved? ---"
load u vec_c_emb_$RANDOM /tmp/vec-probe-c-emb.jsonl -H "partial_columns: true" -H "columns: id,emb"
sel "id, name, emb" u ""

echo "=== cleanup ==="
M "DROP DATABASE IF EXISTS $DB FORCE;" >/dev/null && echo "$LABEL: dropped $DB"
echo "### probe done: $LABEL"
