#!/usr/bin/env bash
# VARIANT probe (R9 W1 / T-A4, blueprint §7.1 note 4) — run BEFORE the ddl
# mapping, the decoder passthrough and any e2e assertion are written (R7
# workorder ③: probe first, always). Bare stream loads straight at the BE, no
# console/engine in the path: this pins what Doris itself does with a
# VARIANT column per server version — DDL shapes, which JSON value shapes
# load, what the sub-column query returns, and how VARIANT behaves under
# UNIQUE KEY merge-on-write with partial_columns.
#
# The load-bearing question for the engine design is B2: the engine's
# DorisSink serializes every Utf8 column through serde_json::to_string
# (tailrace-conn-doris `DataType::Utf8` arm), so a variant column whose
# decoder wrote the raw JSON sub-object back as a STRING reaches Doris as a
# JSON *string literal* ("{\"a\":{\"b\":1}}"), not as a JSON object. If
# Doris parses that string into the variant structure, the sink needs no
# variant-aware raw-emission arm; if it stores it as a string scalar, the
# sink must learn to emit variant cells unescaped.
#
# Usage (one probe per server version):
#   bash probe.sh <label> <mysql-prefix...> -- <be-host-port>
# The mysql prefix runs INSIDE the fe container in both modes, so the mysql
# port is always the in-container 9030 (the host-mapped 39030 is only for
# outside callers — same lesson partial_update.sh bakes into doris_exec).
# Examples:
#   bash probe.sh compose-2.1.7 docker compose -f docker-compose.yml \
#        -f scripts/e2e/evidence/variant-probe-2026-10-30/compose-be-swap-override.yml \
#        exec -T doris-fe mysql -- 38040
#   bash probe.sh standing-3.1.4 docker exec -i doris-2.1 mysql -- 8040
#
# Matrix:
#   A  DDL shapes: A1 DUPLICATE + VARIANT value column; A2 UNIQUE MoW +
#      VARIANT value column; A3 VARIANT as KEY column (expect refusal, message
#      captured); A4 SHOW CREATE TABLE + DESC spelling (the infer round trip).
#   B  load shapes into the DUPLICATE table, then raw SELECT + sub-column
#      queries (v['a']['b'] form):
#      B1 object root with nested object / array / string / float / bool /
#         null leaves (the canonical "nested JSON" row);
#      B2 json STRING form of an object — THE ENGINE SINK EMISSION SHAPE;
#      B3 json STRING form of an array;
#      B4 json STRING form of scalars ("42" / "1.5" / "true" / "null");
#      B5 non-JSON text ("plain string"), malformed JSON text ("{broken"),
#         empty string ("");
#      B6 array root / integer root / bool root;
#      B7 json null / key absent;
#      B8 empty object / nested empty object;
#      B9 four-level nesting + the full sub-column path;
#      B10 (OWN table dk) dotted single key "a.b" next to the nested path
#          a→b: v['a.b'] vs v['a']['b']. Isolated because the first pass put
#          it in table d and on 3.1.4 the dotted key MERGED into the a.b
#          sub-column path and corrupted the raw display of every other row
#          sharing that path (B1/B2 showed "b":{} while v['a']['b'] still
#          answered 1/2); 2.1.7 kept the two keys apart. A real version
#          difference, kept as its own row so B1/B2 evidence stays clean.
#   C  UNIQUE MoW × partial_columns (A7 × A4 crossing):
#      C1 full write {id,name,v} with v in json STRING form → partial update
#         of name only (columns: id,name) → is v preserved?
#      C2 partial update of v only (columns: id,v), v an object → whole-value
#         replacement (not a deep merge)? name preserved?
#      C3 partial update of v in json STRING form → parsed like the full write?
#   D  sub-column type conflict across rows (v.k integer in one row, string in
#      another) → both queryable? what type does the sub-column take?
set -uo pipefail

LABEL="$1"; shift
# split the mysql prefix from the rest at "--"
MYSQL=()
while [ $# -gt 0 ]; do
  [ "$1" = "--" ] && { shift; break; }
  MYSQL+=("$1"); shift
done
BE_PORT="$1"
DB=va_probe
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
echo "--- A1 DUPLICATE KEY + VARIANT value column ---"
M "CREATE TABLE $DB.d (id INT NOT NULL, v VARIANT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" && echo "A1: created"
echo "--- A2 UNIQUE KEY MoW + VARIANT value column ---"
M "CREATE TABLE $DB.u (id INT NOT NULL, name VARCHAR(32), v VARIANT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\");" && echo "A2: created"
echo "--- A3 VARIANT as KEY column (expect refusal) ---"
M "CREATE TABLE $DB.k (v VARIANT NOT NULL, id INT) DUPLICATE KEY(v) DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");"
echo "--- A4 SHOW CREATE TABLE d (column lines) / DESC d ---"
M "SHOW CREATE TABLE $DB.d\G" | grep -E '^\s+`'
M "DESC $DB.d;"

echo "=== B: load shapes → raw + sub-column (DUPLICATE table d) ==="
cat > /tmp/va-probe-b.jsonl <<'EOF'
{"id":1,"v":{"a":{"b":1,"c":"x"},"arr":[1,2,3],"s":"hello","f":1.5,"t":true,"n":null}}
{"id":2,"v":"{\"a\":{\"b\":2,\"c\":\"y\"},\"arr\":[4,5],\"s\":\"str-form\"}"}
{"id":3,"v":"[7,8,9]"}
{"id":4,"v":"42"}
{"id":5,"v":"1.5"}
{"id":6,"v":"true"}
{"id":7,"v":"null"}
{"id":8,"v":"plain string"}
{"id":9,"v":"{broken"}
{"id":10,"v":""}
{"id":11,"v":[10,20,30]}
{"id":12,"v":42}
{"id":13,"v":true}
{"id":14,"v":null}
{"id":15}
{"id":16,"v":{}}
{"id":17,"v":{"a":{}}}
{"id":18,"v":{"l1":{"l2":{"l3":{"l4":"deep"}}}}}
EOF
echo "--- B load: 18 rows (B1 object / B2 json-string object / B3 json-string array / B4 json-string scalars / B5 text,malformed,empty / B6 array,int,bool roots / B7 null,absent / B8 empty objects / B9 deep) ---"
load d va_b_$RANDOM /tmp/va-probe-b.jsonl
echo "--- B raw: SELECT id, v ---"
sel "id, v" d "ORDER BY id"
echo "--- B sub-columns: v['a']['b'], v['a']['c'], v['arr'], v['s'], v['f'], v['t'] ---"
sel "id, v['a']['b'] AS ab, v['a']['c'] AS ac, v['arr'] AS arr, v['s'] AS s, v['f'] AS f, v['t'] AS t" d "WHERE id <= 2 ORDER BY id"
echo "--- B2 vs B1 equivalence: CAST sub-columns to typed scalars ---"
sel "id, CAST(v['a']['b'] AS INT) AS ab_int, CAST(v['a']['c'] AS STRING) AS ac_str, CAST(v['f'] AS DOUBLE) AS f_dbl" d "WHERE id <= 2 ORDER BY id"
echo "--- B9 deep path v['l1']['l2']['l3']['l4'] ---"
sel "id, v['l1']['l2']['l3']['l4'] AS deep" d "WHERE id = 18"
echo "--- B4/B5 json-string scalars & text: how were they stored? (CAST AS STRING) ---"
sel "id, CAST(v AS STRING) AS v_str" d "WHERE id BETWEEN 3 AND 10 ORDER BY id"
echo "--- B10 (own table dk) dotted single key \"a.b\" next to nested a→b ---"
M "CREATE TABLE $DB.dk (id INT NOT NULL, v VARIANT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" >/dev/null
cat > /tmp/va-probe-b10.jsonl <<'EOF'
{"id":1,"v":{"a.b":"dotted-key","a":{"b":"nested-b"}}}
{"id":2,"v":{"a":{"b":7}}}
EOF
load dk va_b10_$RANDOM /tmp/va-probe-b10.jsonl
sel "id, v, v['a.b'] AS dotted, v['a']['b'] AS nested" dk "ORDER BY id"

echo "=== C: UNIQUE MoW × partial_columns (table u) ==="
cat > /tmp/va-probe-c-full.jsonl <<'EOF'
{"id":1,"name":"first","v":"{\"a\":{\"b\":1},\"keep\":\"orig\"}"}
EOF
cat > /tmp/va-probe-c-name.jsonl <<'EOF'
{"id":1,"name":"renamed"}
EOF
cat > /tmp/va-probe-c-vobj.jsonl <<'EOF'
{"id":1,"v":{"a":{"b":2},"new":"replaced"}}
EOF
cat > /tmp/va-probe-c-vstr.jsonl <<'EOF'
{"id":1,"v":"{\"a\":{\"b\":3},\"via\":\"string-form\"}"}
EOF
echo "--- C1 full write (v in json STRING form) ---"
load u va_c_full_$RANDOM /tmp/va-probe-c-full.jsonl
sel "id, name, v, v['a']['b'] AS ab, v['keep'] AS keep" u ""
echo "--- C1 partial: columns id,name only → v preserved? ---"
load u va_c_name_$RANDOM /tmp/va-probe-c-name.jsonl -H "partial_columns: true" -H "columns: id,name"
sel "id, name, v, v['a']['b'] AS ab, v['keep'] AS keep" u ""
echo "--- C2 partial: columns id,v with v OBJECT → whole replacement? name preserved? ---"
load u va_c_vobj_$RANDOM /tmp/va-probe-c-vobj.jsonl -H "partial_columns: true" -H "columns: id,v"
sel "id, name, v, v['a']['b'] AS ab, v['keep'] AS keep, v['new'] AS new_" u ""
echo "--- C3 partial: columns id,v with v json STRING form → parsed? ---"
load u va_c_vstr_$RANDOM /tmp/va-probe-c-vstr.jsonl -H "partial_columns: true" -H "columns: id,v"
sel "id, name, v, v['a']['b'] AS ab, v['via'] AS via" u ""

echo "=== D: sub-column type conflict across rows ==="
M "CREATE TABLE $DB.c (id INT NOT NULL, v VARIANT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" >/dev/null
cat > /tmp/va-probe-d.jsonl <<'EOF'
{"id":1,"v":{"k":100}}
{"id":2,"v":{"k":"one hundred"}}
{"id":3,"v":{"k":1.5}}
{"id":4,"v":{"k":{"nested":true}}}
EOF
load c va_d_$RANDOM /tmp/va-probe-d.jsonl
sel "id, v['k'] AS k, CAST(v['k'] AS STRING) AS k_str" c "ORDER BY id"

echo "=== cleanup ==="
M "DROP DATABASE IF EXISTS $DB FORCE;" >/dev/null && echo "$LABEL: dropped $DB"
echo "### probe done: $LABEL"
