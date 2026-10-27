-- T-D1 dim tables (and the jdbc/doris key columns): the engine plan
-- contract's table-object metadata carries `sequence_column` (dim = the
-- PRIMARY KEY / single-column UNIQUE column used as the full-table JDBC
-- load pagination key; doris = the UNIQUE KEY out-of-order dedup basis;
-- jdbc streaming = the incremental cursor column). Console-side storage
-- counterpart; NULL = not declared.
ALTER TABLE table_object ADD COLUMN sequence_column TEXT;
