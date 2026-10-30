#!/usr/bin/env bash
# Mini probe, appended after the dual-version matrix run: the engine's
# ArrayFormatter (tailrace-conn-doris `_` fallback arm) renders arrow
# Timestamp(_, None) in RFC3339 shape — "2026-10-30T12:34:56.789012", T
# separator. Whether Doris parses the T form into DATETIME(6) decides the
# e2e timestamp assertion shape. Bare stream load, no engine, both server
# versions; output archived in probe-t-separator.out (2026-10-30).
#
# Usage:
#   bash probe-t-separator.sh <label> <mysql-prefix...> -- <be-host-port>
set -uo pipefail
LABEL="$1"; shift
MYSQL=()
while [ $# -gt 0 ]; do
  [ "$1" = "--" ] && { shift; break; }
  MYSQL+=("$1"); shift
done
BE_PORT="$1"
DB=tp_probe2
echo "--- $LABEL: engine ArrayFormatter T-separator shape -> DATETIME(6) ---"
"${MYSQL[@]}" -h127.0.0.1 -P9030 -uroot -e "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB;
CREATE TABLE $DB.dt (id INT NOT NULL, ts DATETIME(6)) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\");" >/dev/null 2>&1
printf '{"id":1,"ts":"2026-10-30T12:34:56.789012"}\n' > /tmp/tp-probe-dt-T.jsonl
curl -s -X PUT -u root: -H "label: tp_dt_T_$RANDOM" -H "format: json" -H "read_json_by_line: true" \
  -H "Expect: 100-continue" --data-binary "@/tmp/tp-probe-dt-T.jsonl" \
  "http://127.0.0.1:$BE_PORT/api/$DB/dt/_stream_load" | grep -E '"Status"|"Message"' | head -2
sleep 1
"${MYSQL[@]}" -N -h127.0.0.1 -P9030 -uroot -e "SELECT id, ts FROM $DB.dt;" 2>&1
"${MYSQL[@]}" -h127.0.0.1 -P9030 -uroot -e "DROP DATABASE IF EXISTS $DB FORCE;" >/dev/null 2>&1
