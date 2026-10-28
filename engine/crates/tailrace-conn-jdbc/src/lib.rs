//! tailrace-conn-jdbc: JDBC-style batch source (MySQL / PostgreSQL dialects).
//!
//! Semantics — keyset (seek) incremental polling, Flink JDBC-connector shape:
//! each poll runs
//! `SELECT * FROM <table> [WHERE <cursor_col> > <last>] ORDER BY <cursor_col>
//! ASC LIMIT <max_rows>` and emits **one JSON object per row per chunk**
//! (kind=jdbc pairs with format=json; the standard JsonDecoder, decode
//! policies and the Bad→DLQ channel apply unchanged downstream).
//!
//! Cursor: `Cursor::JdbcKey { key: <cursor column>, value: <last seen value> }`
//! (the tailrace-ir shape). The runtime commits it only at flush points —
//! the at-least-once anchor; a crash replays the last unflushed window.
//! Resume = the same window query with the saved value as the exclusive
//! lower bound, so a restart continues without loss or duplication **given a
//! unique cursor column** (PK / unique time column). Keyset caveat: ties on a
//! non-unique column straddling a LIMIT boundary would be skipped forever by
//! the next `>` window (silent data loss) — so the connect probe **enforces**
//! that the cursor column is unique by itself (PRIMARY KEY or a single-column
//! UNIQUE index, catalog-view lookup) and fails the source otherwise.
//!
//! Catalog mapping (TableObject reuse, no schema changes):
//! - `bootstrap` = database URL; the scheme selects the dialect
//!   (`mysql://user:pass@host:port/db`, `postgres://…`);
//! - `sequence_column` = incremental cursor column (primary key or a unique
//!   time column; NOT NULL, unique by itself — enforced at connect);
//! - `format` should be `json` for jdbc tables (the source emits JSON).
//!
//! Driver choice (roadmap T13; supply-chain gate ran FIRST, both candidates):
//! - sqlx 0.9 (core+mysql+postgres subcrates, `time`+`json`+`uuid` type
//!   features, NO tls feature — no rustls/native-tls/aws-lc enters the
//!   graph): cargo-deny advisories/bans/licenses/sources all ok, +24 lockfile
//!   crates. CHOSEN. Wired via the subcrates rather than the `sqlx` facade:
//!   the facade's optional `sqlx-sqlite` dep reserves libsqlite3-sys <0.38,
//!   which clashes at resolve time with console-store's rusqlite 0.40
//!   (links="sqlite3", feature off notwithstanding) — the subcrates are the
//!   exact same code the facade re-exports, without the sqlite edge.
//! - sync `mysql`+`postgres` pair: also deny-ok, +31 crates — rejected: the
//!   sync `postgres` crate internally vendors tokio-postgres + an embedded
//!   runtime anyway (lockfile inspection), so it pays more for the same
//!   bridging pattern.
//!
//! The engine data plane stays synchronous (conn-kafka stance): this source
//! owns a dedicated current-thread tokio runtime and bridges through
//! `block_on` — the same pattern tailrace-node's signal thread uses.
//!
//! Idle pacing: an empty poll sleeps ~250 ms in-source before returning, so
//! the runtime's 50 ms idle loop cannot hammer the database at 20 qps.
//!
//! Limitations (deliberate, first cut): TLS off (intranet/localhost scope);
//! DECIMAL columns unsupported (sqlx needs a decimal feature — poll fails
//! with a named-column error); no statement_timeout (queries are bounded by
//! LIMIT max_rows, which the runtime controls).

use std::time::Duration;

use serde_json::{Map, Value};
use sqlx_core::column::Column;
use sqlx_core::connection::Connection;
use sqlx_core::executor::Executor;
use sqlx_core::query::{query, Query};
use sqlx_core::query_scalar::query_scalar;
use sqlx_core::row::Row;
use sqlx_core::sql_str::{AssertSqlSafe, SqlSafeStr};
use sqlx_core::statement::Statement;
use sqlx_core::type_info::TypeInfo;
use sqlx_mysql::{MySql, MySqlArguments, MySqlConnection, MySqlRow};
use sqlx_postgres::{PgArguments, PgConnection, PgRow, Postgres};
use tailrace_catalog::{ReliabilityOptions, StatementSpec, TableObject};
use tailrace_connector::registry::ConnectorRegistry;
use tailrace_connector::{Cursor, Source, SourceChunk};

/// Register the jdbc source factory (kind "jdbc"; source-only — no sink).
pub fn register(reg: &mut ConnectorRegistry) {
    reg.register_source("jdbc", source_factory);
}

fn source_factory(
    obj: &TableObject,
    _spec: &StatementSpec,
    _rel: Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Source>> {
    let url = obj.bootstrap.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "jdbc source {} requires `bootstrap` = database URL",
            obj.name
        )
    })?;
    let dialect = Dialect::from_url(url)?;
    let cursor_col = obj.sequence_column.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "jdbc source {} requires `sequence_column` (incremental cursor column: PK or a UNIQUE time column — must be unique by itself)",
            obj.name
        )
    })?;
    validate_ident("table", &obj.name)?;
    validate_ident("cursor column", cursor_col)?;
    Ok(Box::new(JdbcSource::new(
        url,
        dialect,
        obj.name.clone(),
        cursor_col.to_string(),
    )))
}

/// SQL dialect, selected by URL scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    MySql,
    Postgres,
}

impl Dialect {
    fn from_url(url: &str) -> anyhow::Result<Self> {
        let scheme = url.split("://").next().unwrap_or_default();
        match scheme.to_ascii_lowercase().as_str() {
            "mysql" | "mariadb" => Ok(Dialect::MySql),
            "postgres" | "postgresql" => Ok(Dialect::Postgres),
            other => anyhow::bail!(
                "jdbc URL scheme {other:?} unsupported (mysql:// or postgres://): {url}"
            ),
        }
    }

    /// Positional placeholder for the window predicate.
    fn ph(self) -> &'static str {
        match self {
            Dialect::MySql => "?",
            Dialect::Postgres => "$1",
        }
    }
}

/// Cursor column wire kind — how the canonical string binds back as a typed
/// parameter (Postgres needs exact types; `ts > $1::text` has no operator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorKind {
    Int,
    UInt,
    Float,
    Str,
    /// naive timestamp (MySQL DATETIME / PG TIMESTAMP)
    Timestamp,
    /// tz-aware timestamp (MySQL TIMESTAMP / PG TIMESTAMPTZ)
    Timestamptz,
    Uuid,
}

/// Typed cursor bind value parsed from the canonical string.
#[derive(Debug, Clone)]
enum BindVal {
    I(i64),
    U(u64),
    F(f64),
    S(String),
    Ts(time::PrimitiveDateTime),
    Tsz(time::OffsetDateTime),
    Uid(uuid::Uuid),
}

impl BindVal {
    /// Parse the canonical cursor string back to a typed bind value.
    fn parse(kind: CursorKind, canonical: &str) -> anyhow::Result<Self> {
        Ok(match kind {
            CursorKind::Int => Self::I(canonical.parse()?),
            CursorKind::UInt => Self::U(canonical.parse()?),
            CursorKind::Float => Self::F(canonical.parse()?),
            CursorKind::Str => Self::S(canonical.to_string()),
            CursorKind::Timestamp => {
                let od = time::OffsetDateTime::parse(
                    canonical,
                    &time::format_description::well_known::Rfc3339,
                )?;
                Self::Ts(time::PrimitiveDateTime::new(od.date(), od.time()))
            }
            CursorKind::Timestamptz => Self::Tsz(time::OffsetDateTime::parse(
                canonical,
                &time::format_description::well_known::Rfc3339,
            )?),
            CursorKind::Uuid => Self::Uid(uuid::Uuid::parse_str(canonical)?),
        })
    }
}

/// A live connection of either dialect.
enum Backend {
    MySql(MySqlConnection),
    Pg(PgConnection),
}

/// JDBC batch source (keyset incremental polling, see crate docs).
pub struct JdbcSource {
    /// dedicated current-thread runtime bridging the sync Source trait to
    /// the async sqlx drivers (created once per source instance)
    rt: tokio::runtime::Runtime,
    st: State,
}

struct State {
    url: String,
    dialect: Dialect,
    table: String,
    cursor_col: String,
    backend: Option<Backend>,
    /// last seen cursor value (canonical string); committed only after a
    /// fully converted poll — a failed poll leaves it untouched
    cursor_value: Option<String>,
    /// cursor column wire kind, probed at connect (statement metadata)
    cursor_kind: Option<CursorKind>,
}

impl JdbcSource {
    pub fn new(
        url: impl Into<String>,
        dialect: Dialect,
        table: impl Into<String>,
        cursor_col: impl Into<String>,
    ) -> Self {
        Self {
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("jdbc source runtime"),
            st: State {
                url: url.into(),
                dialect,
                table: table.into(),
                cursor_col: cursor_col.into(),
                backend: None,
                cursor_value: None,
                cursor_kind: None,
            },
        }
    }
}

impl Source for JdbcSource {
    fn open(&mut self, resume: &Cursor) -> anyhow::Result<()> {
        // Cursor repositioning check (kafka pattern): a key mismatch is
        // treated as no-cursor (plan edits that change the cursor column
        // start fresh rather than misaligning).
        if let Cursor::JdbcKey { key, value } = resume {
            if *key != self.st.cursor_col {
                tracing::warn!(
                    table = %self.st.table,
                    cursor_col = %self.st.cursor_col,
                    cursor_key = %key,
                    "jdbc cursor reposition failed — treating as no-cursor"
                );
            } else if !value.is_empty() {
                self.st.cursor_value = Some(value.clone());
            }
        }
        // eager connect+probe: config errors surface now; connectivity errors
        // defer to poll (runtime backoff retries — DB may not be up yet)
        let Self { rt, st } = self;
        if let Err(e) = rt.block_on(connect_and_probe(st)) {
            tracing::warn!(
                table = %st.table,
                error = %format!("{e:#}"),
                "jdbc connect deferred to poll backoff"
            );
        }
        Ok(())
    }

    fn poll(&mut self, max_rows: usize, deadline: Duration) -> anyhow::Result<Vec<SourceChunk>> {
        if deadline.is_zero() {
            return Ok(vec![]);
        }
        let Self { rt, st } = self;
        rt.block_on(poll_async(st, max_rows, deadline))
    }

    fn cursor(&self) -> Cursor {
        Cursor::JdbcKey {
            key: self.st.cursor_col.clone(),
            value: self.st.cursor_value.clone().unwrap_or_default(),
        }
    }
}

/// Connect + probe the cursor column kind via prepared-statement metadata
/// (no data read; also validates table/SQL early), then gate the keyset
/// precondition (cursor column unique by itself — see crate docs).
async fn connect_and_probe(st: &mut State) -> anyhow::Result<()> {
    let probe = format!(
        "SELECT * FROM {} WHERE {} > {} ORDER BY {} ASC LIMIT 1",
        st.table,
        st.cursor_col,
        st.dialect.ph(),
        st.cursor_col
    );
    match st.dialect {
        Dialect::MySql => {
            let mut conn = MySqlConnection::connect(&st.url).await?;
            let stmt = conn
                .prepare(AssertSqlSafe(probe.as_str()).into_sql_str())
                .await?;
            let cols = stmt.columns();
            let col = find_col(
                cols.iter().map(|c| (c.name(), c.type_info().name())),
                &st.cursor_col,
            )?;
            st.cursor_kind = Some(mysql_cursor_kind(col.1)?);
            let mut backend = Backend::MySql(conn);
            assert_unique_cursor(&mut backend, &st.table, &st.cursor_col).await?;
            st.backend = Some(backend);
        }
        Dialect::Postgres => {
            let mut conn = PgConnection::connect(&st.url).await?;
            let stmt = conn
                .prepare(AssertSqlSafe(probe.as_str()).into_sql_str())
                .await?;
            let cols = stmt.columns();
            let col = find_col(
                cols.iter().map(|c| (c.name(), c.type_info().name())),
                &st.cursor_col,
            )?;
            st.cursor_kind = Some(pg_cursor_kind(col.1)?);
            let mut backend = Backend::Pg(conn);
            assert_unique_cursor(&mut backend, &st.table, &st.cursor_col).await?;
            st.backend = Some(backend);
        }
    }
    tracing::info!(
        table = %st.table,
        dialect = ?st.dialect,
        cursor_col = %st.cursor_col,
        kind = ?st.cursor_kind,
        "jdbc source connected"
    );
    Ok(())
}

/// MySQL: count unique indexes (PRIMARY included) whose ONLY key part is the
/// cursor column. `COALESCE(?, DATABASE())` resolves an unqualified table to
/// the connection's default schema; the NOT EXISTS clause rejects composite
/// unique keys (a leading part of `UNIQUE (ts, id)` is not unique by itself).
const MYSQL_UNIQUE_SQL: &str = "SELECT COUNT(*) FROM information_schema.STATISTICS s \
     WHERE s.TABLE_SCHEMA = COALESCE(?, DATABASE()) AND s.TABLE_NAME = ? \
       AND s.COLUMN_NAME = ? AND s.NON_UNIQUE = 0 \
       AND NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS o \
                        WHERE o.TABLE_SCHEMA = s.TABLE_SCHEMA AND o.TABLE_NAME = s.TABLE_NAME \
                          AND o.INDEX_NAME = s.INDEX_NAME AND o.SEQ_IN_INDEX <> s.SEQ_IN_INDEX)";

/// PostgreSQL: same predicate over pg_index — unique, non-partial (a partial
/// unique index proves nothing for the rows it excludes), exactly one key
/// column (`indnkeyatts`, INCLUDE columns don't count) and that column is the
/// cursor column (`indkey` is 0-based). Unquoted identifiers fold to lower
/// case in PG, so the lookup binds lower-cased names.
const PG_UNIQUE_SQL: &str = "SELECT COUNT(*) FROM pg_index i \
     JOIN pg_class t ON t.oid = i.indrelid \
     JOIN pg_namespace n ON n.oid = t.relnamespace \
     JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname::text = $3::text \
     WHERE n.nspname::text = COALESCE($1::text, current_schema()::text) \
       AND t.relname::text = $2::text \
       AND i.indisunique AND i.indpred IS NULL AND i.indnkeyatts = 1 \
       AND i.indkey[0] = a.attnum";

/// Keyset safety gate (R3-W1 review): `WHERE col > ? ORDER BY col LIMIT n`
/// has no tie-breaker, so a cursor column with duplicate values straddling a
/// LIMIT boundary would have the remaining ties skipped forever by the next
/// window — silent data loss. Refuse to run unless the column is unique by
/// itself (PRIMARY KEY or single-column UNIQUE index), checked once per
/// connect via the catalog views.
async fn assert_unique_cursor(
    backend: &mut Backend,
    table: &str,
    cursor_col: &str,
) -> anyhow::Result<()> {
    let (schema, name) = match table.rsplit_once('.') {
        Some((s, n)) => (Some(s), n),
        None => (None, table),
    };
    let unique_indexes: i64 = match backend {
        Backend::MySql(conn) => query_scalar::<MySql, i64>(AssertSqlSafe(MYSQL_UNIQUE_SQL))
            .bind(schema)
            .bind(name)
            .bind(cursor_col)
            .fetch_one(conn)
            .await
            .map_err(|e| anyhow::anyhow!("mysql cursor uniqueness probe failed: {e}"))?,
        Backend::Pg(conn) => query_scalar::<Postgres, i64>(AssertSqlSafe(PG_UNIQUE_SQL))
            .bind(schema.map(str::to_ascii_lowercase))
            .bind(name.to_ascii_lowercase())
            .bind(cursor_col.to_ascii_lowercase())
            .fetch_one(conn)
            .await
            .map_err(|e| anyhow::anyhow!("postgres cursor uniqueness probe failed: {e}"))?,
    };
    if unique_indexes == 0 {
        anyhow::bail!(
            "jdbc source {table}: cursor column {cursor_col} has no PRIMARY KEY / single-column UNIQUE index — \
             keyset polling (WHERE {cursor_col} > ?) would silently skip duplicate values across a LIMIT boundary; \
             use the primary key or a UNIQUE time column as `sequence_column`"
        );
    }
    Ok(())
}

/// One poll: window query → per-row JSON chunks; cursor advances only after
/// every row converted (in-batch staging — kafka poll pattern).
async fn poll_async(
    st: &mut State,
    max_rows: usize,
    deadline: Duration,
) -> anyhow::Result<Vec<SourceChunk>> {
    if st.backend.is_none() {
        connect_and_probe(st).await?;
    }
    let sql = match &st.cursor_value {
        Some(_) => format!(
            "SELECT * FROM {} WHERE {} > {} ORDER BY {} ASC LIMIT {}",
            st.table,
            st.cursor_col,
            st.dialect.ph(),
            st.cursor_col,
            max_rows
        ),
        None => format!(
            "SELECT * FROM {} ORDER BY {} ASC LIMIT {}",
            st.table, st.cursor_col, max_rows
        ),
    };
    let (rows, converted) = match st.dialect {
        Dialect::MySql => {
            let Backend::MySql(conn) = st.backend.as_mut().expect("backend ensured") else {
                unreachable!("dialect/backend mismatch")
            };
            let mut q = query::<MySql>(AssertSqlSafe(sql.as_str()));
            if let Some(v) = &st.cursor_value {
                q = bind_mysql(q, v, st.cursor_kind.expect("probed with backend"))?;
            }
            let rows: Vec<MySqlRow> = match q.fetch_all(conn).await {
                Ok(r) => r,
                Err(e) => {
                    st.backend = None; // force reconnect next poll
                    return Err(anyhow::anyhow!("mysql window query failed: {e}"));
                }
            };
            let converted: Vec<anyhow::Result<(Vec<u8>, Option<String>)>> = rows
                .iter()
                .map(|r| mysql_row_to_chunk(r, &st.cursor_col))
                .collect();
            (rows.len(), converted)
        }
        Dialect::Postgres => {
            let Backend::Pg(conn) = st.backend.as_mut().expect("backend ensured") else {
                unreachable!("dialect/backend mismatch")
            };
            let mut q = query::<Postgres>(AssertSqlSafe(sql.as_str()));
            if let Some(v) = &st.cursor_value {
                q = bind_pg(q, v, st.cursor_kind.expect("probed with backend"))?;
            }
            let rows: Vec<PgRow> = match q.fetch_all(conn).await {
                Ok(r) => r,
                Err(e) => {
                    st.backend = None;
                    return Err(anyhow::anyhow!("postgres window query failed: {e}"));
                }
            };
            let converted: Vec<anyhow::Result<(Vec<u8>, Option<String>)>> = rows
                .iter()
                .map(|r| pg_row_to_chunk(r, &st.cursor_col))
                .collect();
            (rows.len(), converted)
        }
    };
    let mut out = Vec::with_capacity(rows);
    let mut last_canonical: Option<String> = None;
    for r in converted {
        let (bytes, canonical) = r?;
        if canonical.is_none() {
            anyhow::bail!(
                "cursor column {} is NULL in a row — declare a NOT NULL cursor column",
                st.cursor_col
            );
        }
        last_canonical = canonical;
        out.push(SourceChunk::plain(bytes));
    }
    if out.is_empty() {
        // idle pacing: keep the runtime's 50ms idle loop off the database
        tokio::time::sleep(deadline.min(Duration::from_millis(250))).await;
        return Ok(out);
    }
    // whole poll converted — the cursor can advance (staged per-batch: any
    // conversion error above returned before this line)
    st.cursor_value = last_canonical;
    Ok(out)
}

fn bind_mysql<'q>(
    q: Query<'q, MySql, MySqlArguments>,
    canonical: &str,
    kind: CursorKind,
) -> anyhow::Result<Query<'q, MySql, MySqlArguments>> {
    Ok(match BindVal::parse(kind, canonical)? {
        BindVal::I(v) => q.bind(v),
        BindVal::U(v) => q.bind(v),
        BindVal::F(v) => q.bind(v),
        BindVal::S(v) => q.bind(v),
        BindVal::Ts(v) => q.bind(v),
        BindVal::Tsz(v) => q.bind(v),
        BindVal::Uid(v) => q.bind(v),
    })
}

fn bind_pg<'q>(
    q: Query<'q, Postgres, PgArguments>,
    canonical: &str,
    kind: CursorKind,
) -> anyhow::Result<Query<'q, Postgres, PgArguments>> {
    Ok(match BindVal::parse(kind, canonical)? {
        BindVal::I(v) => q.bind(v),
        // postgres integers are all signed; the cursor value came from an
        // int column so the narrowing is lossless
        BindVal::U(v) => q.bind(i64::try_from(v)?),
        BindVal::F(v) => q.bind(v),
        BindVal::S(v) => q.bind(v),
        BindVal::Ts(v) => q.bind(v),
        BindVal::Tsz(v) => q.bind(v),
        BindVal::Uid(v) => q.bind(v),
    })
}

/// Locate the cursor column by name in prepared-statement metadata.
fn find_col<'a>(
    mut cols: impl Iterator<Item = (&'a str, &'a str)>,
    cursor_col: &str,
) -> anyhow::Result<(&'a str, &'a str)> {
    cols.find(|(n, _)| *n == cursor_col)
        .ok_or_else(|| anyhow::anyhow!("cursor column {cursor_col} not found in table columns"))
}

// ==================== mysql row → JSON ====================

fn mysql_cursor_kind(type_name: &str) -> anyhow::Result<CursorKind> {
    Ok(match type_name {
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => CursorKind::Int,
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" => CursorKind::UInt,
        "FLOAT" | "DOUBLE" => CursorKind::Float,
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET" => {
            CursorKind::Str
        }
        "DATETIME" => CursorKind::Timestamp,
        "TIMESTAMP" => CursorKind::Timestamptz,
        other => anyhow::bail!("mysql cursor column type {other} unsupported (use an integer, string or timestamp column)"),
    })
}

/// One MySQL row → (JSON object bytes, canonical cursor value if cursor col).
fn mysql_row_to_chunk(
    row: &MySqlRow,
    cursor_col: &str,
) -> anyhow::Result<(Vec<u8>, Option<String>)> {
    let mut obj = Map::new();
    let mut canonical = None;
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name();
        let ty = col.type_info().name();
        let (v, canon) = mysql_cell(row, i, name, ty, name == cursor_col)?;
        if name == cursor_col {
            canonical = canon;
        }
        obj.insert(name.to_string(), v);
    }
    Ok((serde_json::to_vec(&Value::Object(obj))?, canonical))
}

fn mysql_cell(
    row: &MySqlRow,
    i: usize,
    name: &str,
    ty: &str,
    is_cursor: bool,
) -> anyhow::Result<(Value, Option<String>)> {
    // cursor arms return early with (json, canonical); plain arms produce
    // Option<Value> (SQL NULL → JSON null)
    let v: Option<Value> = match ty {
        "BOOLEAN" => row.try_get::<Option<bool>, _>(i)?.map(Value::from),
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            let v = row.try_get::<Option<i64>, _>(i)?;
            if is_cursor {
                return Ok((val(v.map(Value::from)), v.map(|x| x.to_string())));
            }
            v.map(Value::from)
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" => {
            let v = row.try_get::<Option<u64>, _>(i)?;
            if is_cursor {
                return Ok((val(v.map(Value::from)), v.map(|x| x.to_string())));
            }
            v.map(Value::from)
        }
        "FLOAT" | "DOUBLE" => {
            let v = row.try_get::<Option<f64>, _>(i)?;
            if is_cursor {
                return Ok((num(v), v.map(|x| format!("{x}")))); // shortest round-trip
            }
            Some(num(v))
        }
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET" => {
            let v = row.try_get::<Option<String>, _>(i)?;
            if is_cursor {
                return Ok((val(v.clone().map(Value::from)), v));
            }
            v.map(Value::from)
        }
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => row
            .try_get::<Option<Vec<u8>>, _>(i)?
            .map(|b| Value::String(hex_encode(&b))),
        "DATETIME" => {
            let v = row.try_get::<Option<time::PrimitiveDateTime>, _>(i)?;
            let canon = v.map(rfc3339_naive);
            if is_cursor {
                return Ok((val(canon.clone().map(Value::from)), canon));
            }
            canon.map(Value::from)
        }
        "TIMESTAMP" => {
            let v = row.try_get::<Option<time::OffsetDateTime>, _>(i)?;
            let canon = v.map(rfc3339_utc);
            if is_cursor {
                return Ok((val(canon.clone().map(Value::from)), canon));
            }
            canon.map(Value::from)
        }
        "DATE" => row.try_get::<Option<time::Date>, _>(i)?.map(|d| Value::String(d.to_string())),
        "TIME" => row.try_get::<Option<time::Time>, _>(i)?.map(|t| Value::String(t.to_string())),
        "YEAR" => row.try_get::<Option<i16>, _>(i)?.map(Value::from),
        "JSON" => row
            .try_get::<Option<sqlx_core::types::Json<Value>>, _>(i)?
            .map(|j| j.0),
        other => anyhow::bail!(
            "mysql column {name} has unsupported type {other} (DECIMAL/BIT/GEOMETRY need a wider type map)",
        ),
    };
    Ok((val(v), None))
}

// ==================== postgres row → JSON ====================

fn pg_cursor_kind(type_name: &str) -> anyhow::Result<CursorKind> {
    // names come from PgTypeInfo's TypeInfo::name() (= display_name(),
    // UPPERCASE; bpchar displays as "CHAR")
    Ok(match type_name {
        "INT2" | "INT4" | "INT8" => CursorKind::Int,
        "FLOAT4" | "FLOAT8" => CursorKind::Float,
        "TEXT" | "VARCHAR" | "CHAR" | "NAME" => CursorKind::Str,
        "TIMESTAMP" => CursorKind::Timestamp,
        "TIMESTAMPTZ" => CursorKind::Timestamptz,
        "UUID" => CursorKind::Uuid,
        other => anyhow::bail!("postgres cursor column type {other} unsupported (use an integer, text or timestamp column)"),
    })
}

fn pg_row_to_chunk(row: &PgRow, cursor_col: &str) -> anyhow::Result<(Vec<u8>, Option<String>)> {
    let mut obj = Map::new();
    let mut canonical = None;
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name();
        let (v, canon) = pg_cell(row, i, name, col.type_info().name(), name == cursor_col)?;
        if name == cursor_col {
            canonical = canon;
        }
        obj.insert(name.to_string(), v);
    }
    Ok((serde_json::to_vec(&Value::Object(obj))?, canonical))
}

fn pg_cell(
    row: &PgRow,
    i: usize,
    name: &str,
    ty: &str,
    is_cursor: bool,
) -> anyhow::Result<(Value, Option<String>)> {
    let v: Option<Value> = match ty {
        "BOOL" => row.try_get::<Option<bool>, _>(i)?.map(Value::from),
        "INT2" => {
            let v = row.try_get::<Option<i16>, _>(i)?;
            if is_cursor {
                return Ok((val(v.map(Value::from)), v.map(|x| x.to_string())));
            }
            v.map(Value::from)
        }
        "INT4" => {
            let v = row.try_get::<Option<i32>, _>(i)?;
            if is_cursor {
                return Ok((val(v.map(Value::from)), v.map(|x| x.to_string())));
            }
            v.map(Value::from)
        }
        "INT8" => {
            let v = row.try_get::<Option<i64>, _>(i)?;
            if is_cursor {
                return Ok((val(v.map(Value::from)), v.map(|x| x.to_string())));
            }
            v.map(Value::from)
        }
        "FLOAT4" => {
            let v = row.try_get::<Option<f32>, _>(i)?;
            if is_cursor {
                // mirror FLOAT8 (R3-W1 review low#1): the canonical widens
                // to f64 BEFORE formatting — see f32_cursor_canonical
                return Ok((num(v.map(|x| x as f64)), v.map(f32_cursor_canonical)));
            }
            v.map(|x| Value::from(x as f64))
        }
        "FLOAT8" => {
            let v = row.try_get::<Option<f64>, _>(i)?;
            if is_cursor {
                return Ok((num(v), v.map(|x| format!("{x}"))));
            }
            Some(num(v))
        }
        "TEXT" | "VARCHAR" | "CHAR" | "NAME" => {
            let v = row.try_get::<Option<String>, _>(i)?;
            if is_cursor {
                return Ok((val(v.clone().map(Value::from)), v));
            }
            v.map(Value::from)
        }
        "BYTEA" => row
            .try_get::<Option<Vec<u8>>, _>(i)?
            .map(|b| Value::String(hex_encode(&b))),
        "JSON" | "JSONB" => row
            .try_get::<Option<sqlx_core::types::Json<Value>>, _>(i)?
            .map(|j| j.0),
        "UUID" => {
            let v = row.try_get::<Option<uuid::Uuid>, _>(i)?;
            if is_cursor {
                return Ok((val(v.map(|x| Value::from(x.to_string()))), v.map(|x| x.to_string())));
            }
            v.map(|x| Value::from(x.to_string()))
        }
        "TIMESTAMP" => {
            let v = row.try_get::<Option<time::PrimitiveDateTime>, _>(i)?;
            let canon = v.map(rfc3339_naive);
            if is_cursor {
                return Ok((val(canon.clone().map(Value::from)), canon));
            }
            canon.map(Value::from)
        }
        "TIMESTAMPTZ" => {
            let v = row.try_get::<Option<time::OffsetDateTime>, _>(i)?;
            let canon = v.map(rfc3339_utc);
            if is_cursor {
                return Ok((val(canon.clone().map(Value::from)), canon));
            }
            canon.map(Value::from)
        }
        "DATE" => row.try_get::<Option<time::Date>, _>(i)?.map(|d| Value::String(d.to_string())),
        "TIME" => row.try_get::<Option<time::Time>, _>(i)?.map(|t| Value::String(t.to_string())),
        other => anyhow::bail!(
            "postgres column {name} has unsupported type {other} (numeric/arrays/exotics need a wider type map)",
        ),
    };
    Ok((val(v), None))
}

// ==================== shared helpers ====================

/// FLOAT4 cursor canonical (R3-W1 review low#1): widen to f64 first, THEN
/// format. f32→f64 widening is exact, and Postgres compares `float4 > float8`
/// by promoting the column (exact), so the shortest round-trip f64 string
/// binds back to the very value the emitted row promoted to — `col > bind`
/// excludes it and keyset polling progresses. Formatting the f32 directly
/// would emit "0.3", which parses to an f64 strictly BELOW the promoted
/// `0.3f32` (= 0.30000001192092896…), and the same row would re-match every
/// poll (permanent re-emission, no progress).
///
/// Why an arm instead of rejecting FLOAT4 in `pg_cursor_kind`: MySQL FLOAT
/// is an accepted, working cursor kind (`mysql_cell` handles is_cursor), so
/// rejecting FLOAT4 would break dialect symmetry and drop a legal (if
/// unusual) single-column UNIQUE/PK shape the uniqueness gate otherwise
/// allows — and the exactness argument above shows no correctness hole
/// remains once the canonical widens first.
fn f32_cursor_canonical(x: f32) -> String {
    format!("{}", x as f64)
}

fn rfc3339_naive(dt: time::PrimitiveDateTime) -> String {
    dt.assume_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn rfc3339_utc(dt: time::OffsetDateTime) -> String {
    use time::format_description::well_known::Rfc3339;
    dt.to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Option<Value> → Value with SQL NULL mapping to JSON null.
fn val(v: Option<Value>) -> Value {
    v.unwrap_or(Value::Null)
}

/// f64 → JSON number (NaN/Infinity are not representable in JSON → null).
fn num(v: Option<f64>) -> Value {
    v.and_then(serde_json::Number::from_f64)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Lowercase hex (no prefix) — the JSON carrier shape for binary columns.
fn hex_encode(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push(HEX[(x >> 4) as usize] as char);
        s.push(HEX[(x & 0xf) as usize] as char);
    }
    s
}

/// Identifier gate: the table/cursor names are interpolated into SQL, so
/// only `[A-Za-z_][A-Za-z0-9_$]*` dot-segments pass (catalog values are
/// operator-controlled, but a typo'd `;DROP TABLE` must still never reach
/// the wire).
fn validate_ident(what: &str, s: &str) -> anyhow::Result<()> {
    for seg in s.split('.') {
        let mut chars = seg.chars();
        let head_ok = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_');
        let body_ok = chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
        if !head_ok || !body_ok {
            anyhow::bail!("invalid SQL identifier for {what}: {s:?} (allowed: [A-Za-z_][A-Za-z0-9_$]* segments)");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect_from_url_schemes() {
        assert_eq!(
            Dialect::from_url("mysql://root@h/db").unwrap(),
            Dialect::MySql
        );
        assert_eq!(Dialect::from_url("MariaDB://h/db").unwrap(), Dialect::MySql);
        assert_eq!(
            Dialect::from_url("postgres://u:p@h:5432/db").unwrap(),
            Dialect::Postgres
        );
        assert!(Dialect::from_url("oracle://h/db").is_err());
        assert!(Dialect::from_url("no-scheme").is_err());
    }

    #[test]
    fn ident_gate() {
        assert!(validate_ident("t", "orders").is_ok());
        assert!(validate_ident("t", "shop.orders").is_ok());
        assert!(validate_ident("t", "t_1$c").is_ok());
        for bad in [
            "orders; DROP TABLE x",
            "1orders",
            "",
            "a..b",
            "o rders",
            "'q'",
        ] {
            assert!(
                validate_ident("t", bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn cursor_bind_round_trip() {
        // int / uint / float
        assert!(matches!(
            BindVal::parse(CursorKind::Int, "-42").unwrap(),
            BindVal::I(-42)
        ));
        assert!(matches!(
            BindVal::parse(CursorKind::UInt, "18446744073709551615").unwrap(),
            BindVal::U(18446744073709551615)
        ));
        assert!(matches!(
            BindVal::parse(CursorKind::Float, "0.5").unwrap(),
            BindVal::F(f) if f == 0.5
        ));
        // string
        assert!(matches!(
            BindVal::parse(CursorKind::Str, "abc").unwrap(),
            BindVal::S(s) if s == "abc"
        ));
        // timestamps: canonical RFC3339 → typed, same instant back
        let ts = BindVal::parse(CursorKind::Timestamp, "2026-10-20T01:02:03Z").unwrap();
        match ts {
            BindVal::Ts(dt) => assert_eq!(rfc3339_naive(dt), "2026-10-20T01:02:03Z"),
            other => panic!("{other:?}"),
        }
        // +08:00 wall clock canonicalizes to the same instant in UTC
        let tsz = BindVal::parse(CursorKind::Timestamptz, "2026-10-20T01:02:03+08:00").unwrap();
        match tsz {
            BindVal::Tsz(od) => assert_eq!(rfc3339_utc(od), "2026-10-19T17:02:03Z"),
            other => panic!("{other:?}"),
        }
        assert!(BindVal::parse(CursorKind::Timestamptz, "not-a-time").is_err());
        // uuid
        assert!(matches!(
            BindVal::parse(CursorKind::Uuid, "0f14d0ab-9605-4a62-a9e4-5ed26688389b").unwrap(),
            BindVal::Uid(_)
        ));
    }

    #[test]
    fn hex_shape() {
        assert_eq!(hex_encode(&[0x0f, 0xa1]), "0fa1");
        assert_eq!(hex_encode(&[]), "");
    }

    /// R3-W1 review low#1 regression: a FLOAT4 cursor column must produce a
    /// canonical that binds back EXACTLY to the promoted f64 — otherwise
    /// `WHERE col > bind` re-matches the emitted row every poll (permanent
    /// re-emission, no keyset progress; previously the FLOAT4 arm had no
    /// is_cursor branch at all and every poll died on a bogus "cursor
    /// column is NULL").
    #[test]
    fn float4_cursor_canonical_binds_exact() {
        // the probe accepts FLOAT4 (deliberate: mirror of MySQL FLOAT)
        assert_eq!(pg_cursor_kind("FLOAT4").unwrap(), CursorKind::Float);
        assert_eq!(pg_cursor_kind("FLOAT8").unwrap(), CursorKind::Float);
        for x in [0.3f32, 0.1, 1.0e20, -2.25, 123456.78] {
            let canonical = f32_cursor_canonical(x);
            let bound: f64 = canonical.parse().unwrap();
            assert_eq!(
                bound, x as f64,
                "{x}: canonical {canonical} must round-trip to the exact promoted value"
            );
        }
        // counterexample locking WHY widening must precede formatting: the
        // f32 shortest repr is NOT the promoted value — it parses strictly
        // below it, so `col > bind` would re-match the row forever
        assert_eq!(format!("{}", 0.3f32), "0.3");
        assert!(0.3f32 as f64 > "0.3".parse::<f64>().unwrap());
        assert_eq!(f32_cursor_canonical(0.3f32), "0.30000001192092896");
    }

    #[test]
    fn factory_requires_url_and_cursor_column() {
        let obj = |bootstrap: Option<&str>, seq: Option<&str>| TableObject {
            datasource: "ds".into(),
            name: "orders".into(),
            kind: "jdbc".into(),
            format: Some("json".into()),
            columns: vec![],
            bootstrap: bootstrap.map(str::to_string),
            stream_load_url: None,
            auth_env: None,
            label_prefix: None,
            sequence_column: seq.map(str::to_string),
            partial_columns: None,
        };
        let spec = StatementSpec {
            source: "ds.orders".into(),
            group: "tr.p1.s0".into(),
            sink: "ds.orders".into(),
            pushdown: vec![],
            select_sql: "SELECT * FROM src".into(),
            start_offset: None,
            dims: Vec::new(),
        };
        let err = |b: Option<&str>, q: Option<&str>| match source_factory(&obj(b, q), &spec, None) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("factory must reject ({b:?}, {q:?})"),
        };
        assert!(err(None, Some("id")).contains("bootstrap"));
        assert!(err(Some("mysql://root@localhost/test"), None).contains("sequence_column"));
        assert!(err(Some("oracle://localhost/test"), Some("id")).contains("scheme"));
        assert!(err(Some("mysql://root@localhost/test"), Some("id; DROP"))
            .contains("invalid SQL identifier"));
    }
}
