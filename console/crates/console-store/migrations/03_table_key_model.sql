-- T-A7 partial-column update × Doris key model: a partial load (stream load
-- `partial_columns=true`) is only legal on a UNIQUE KEY merge-on-write
-- table — on a DUPLICATE KEY table Doris refuses it at load time, i.e.
-- after deploy, mid-pipeline. The console gates the declaration at publish
-- instead, which needs the table's key model on record: `unique` /
-- `duplicate`, written by the T-A1 execute step (the console created the
-- table and knows) or declared at registration / via the key-model PUT for
-- tables that pre-exist the console. NULL = not declared — a partial
-- declaration against such a sink is refused until it is.
ALTER TABLE table_object ADD COLUMN key_model TEXT;
