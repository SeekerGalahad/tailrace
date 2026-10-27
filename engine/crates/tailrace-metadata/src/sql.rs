//! SQL metadata backends.
//!
//! MySQL wire protocol (Doris FE 9030 / MySQL servers): SHOW DATABASES →
//! SHOW TABLES → DESC, all on the **text** protocol (`raw_sql`) — no
//! COM_STMT_PREPARE, whose support on Doris FE is version-dependent; SHOW /
//! DESC is the lowest common denominator the 9030 port guarantees.
//! User-supplied db/table names only ever appear backtick-quoted with
//! backticks doubled and NUL rejected, so no identifier can break out.
//!
//! PostgreSQL: `information_schema` over the extended protocol with bound
//! parameters (`::text` casts — sql_identifier domains decode as text only
//! then); nothing user-supplied is interpolated at all. The spec's
//! `database` is the connection database (PG scopes catalogs per database;
//! the two-part asset name is schema.table inside it).

use sqlx_core::connection::Connection;
use sqlx_core::executor::Executor;
use sqlx_core::query::query;
use sqlx_core::raw_sql::raw_sql;
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_mysql::{MySqlConnectOptions, MySqlConnection, MySqlRow, MySqlSslMode};
use sqlx_postgres::{PgConnectOptions, PgConnection, PgRow, Postgres};

use crate::{ConnSpec, ExecOutcome, StatementOutcome};

/// System schemas hidden from discovery (MySQL's four + Doris's
/// `__internal_schema`; Postgres filters its own set in SQL).
const MYSQL_SYSTEM_DBS: [&str; 5] = [
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
    "__internal_schema",
];

const PG_LIST_SQL: &str =
    "SELECT table_schema::text, table_name::text FROM information_schema.tables \
     WHERE table_type = 'BASE TABLE' \
       AND table_schema NOT IN ('pg_catalog', 'information_schema') \
       AND table_schema NOT LIKE 'pg\\_toast%' \
     ORDER BY table_schema, table_name";
const PG_DESCRIBE_SQL: &str =
    "SELECT column_name::text, data_type::text FROM information_schema.columns \
     WHERE table_schema = $1 AND table_name = $2 \
     ORDER BY ordinal_position";

async fn mysql_connect(spec: &ConnSpec) -> anyhow::Result<(MySqlConnection, Option<String>)> {
    let ConnSpec::MySql {
        host,
        port,
        database,
        username,
        password,
    } = spec
    else {
        unreachable!("mysql_connect on a non-MySql spec")
    };
    let mut opts =
        MySqlConnectOptions::new()
            .host(host)
            .port(*port)
            .username(if username.is_empty() {
                "root"
            } else {
                username
            });
    // empty password → leave unset (Option::None, same as a URL without a
    // password): Some("") would engage caching_sha2 full auth → the RSA
    // key exchange, which the no-tls subcrate build cannot perform
    if !password.is_empty() {
        opts = opts.password(password);
    }
    // TLS off by configuration too, mirroring the conn-jdbc no-tls stance
    let mut opts = opts.ssl_mode(MySqlSslMode::Disabled);
    if let Some(db) = database {
        opts = opts.database(db);
    }
    let conn = MySqlConnection::connect_with(&opts)
        .await
        .map_err(|e| anyhow::anyhow!("mysql 协议连接失败（{host}:{port}）: {e}"))?;
    Ok((conn, database.clone()))
}

/// MySQL protocol: non-system (or the configured) database(s) × their tables.
pub(crate) async fn mysql_list_tables(spec: &ConnSpec) -> anyhow::Result<Vec<(String, String)>> {
    let (mut conn, database) = mysql_connect(spec).await?;
    let dbs: Vec<String> = match database {
        Some(db) => vec![db],
        None => {
            let rows: Vec<MySqlRow> = conn
                .fetch_all(raw_sql("SHOW DATABASES"))
                .await
                .map_err(|e| anyhow::anyhow!("SHOW DATABASES 失败: {e}"))?;
            rows.iter()
                .map(|r| r.try_get::<String, _>(0))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| anyhow::anyhow!("SHOW DATABASES 解码失败: {e}"))?
                .into_iter()
                .filter(|db| !MYSQL_SYSTEM_DBS.contains(&db.to_ascii_lowercase().as_str()))
                .collect()
        }
    };
    let mut out = Vec::new();
    for db in dbs {
        let sql = format!("SHOW TABLES FROM `{}`", quote_ident(&db)?);
        let rows: Vec<MySqlRow> = conn
            .fetch_all(raw_sql(AssertSqlSafe(sql)))
            .await
            .map_err(|e| anyhow::anyhow!("SHOW TABLES 失败（{db}）: {e}"))?;
        for r in rows {
            let table = r
                .try_get::<String, _>(0)
                .map_err(|e| anyhow::anyhow!("SHOW TABLES 解码失败（{db}）: {e}"))?;
            out.push((db.clone(), table));
        }
    }
    Ok(out)
}

/// MySQL protocol: `DESC db.table` → (column, DDL type) in declaration
/// order. Zero rows = unknown table.
pub(crate) async fn mysql_describe(
    spec: &ConnSpec,
    db: &str,
    table: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let (mut conn, _) = mysql_connect(spec).await?;
    let sql = format!("DESC `{}`.`{}`", quote_ident(db)?, quote_ident(table)?);
    let rows: Vec<MySqlRow> = conn
        .fetch_all(raw_sql(AssertSqlSafe(sql)))
        .await
        .map_err(|e| anyhow::anyhow!("DESC 失败（{db}.{table}）: {e}"))?;
    if rows.is_empty() {
        anyhow::bail!("表不存在或无列: {db}.{table}");
    }
    rows.iter()
        .map(|r| {
            let name = r
                .try_get::<String, _>(0)
                .map_err(|e| anyhow::anyhow!("DESC 解码失败（字段名）: {e}"))?;
            let ty = r
                .try_get::<String, _>(1)
                .map_err(|e| anyhow::anyhow!("DESC 解码失败（{name} 类型）: {e}"))?;
            Ok((name, ty))
        })
        .collect()
}

/// Backtick-quote body for one identifier: backticks doubled, NUL (which a
/// JSON string can smuggle) rejected.
fn quote_ident(name: &str) -> anyhow::Result<String> {
    if name.contains('\0') {
        anyhow::bail!("非法标识符（含 NUL）");
    }
    Ok(name.replace('`', "``"))
}

/// Execute statements sequentially on one connection (T-A1 step ④). The
/// first error aborts the batch and is reported with its 1-based position
/// plus the server's message verbatim; nothing is rolled back (MySQL DDL is
/// auto-committed — the console surfaces the partial state via the error).
pub(crate) async fn mysql_exec(spec: &ConnSpec, sqls: &[&str]) -> anyhow::Result<ExecOutcome> {
    let (mut conn, _) = mysql_connect(spec).await?;
    let mut statements = Vec::with_capacity(sqls.len());
    for (i, sql) in sqls.iter().enumerate() {
        let started = std::time::Instant::now();
        let res = conn
            .execute(raw_sql(AssertSqlSafe((*sql).to_string())))
            .await
            .map_err(|e| anyhow::anyhow!("第 {} 条执行失败: {e}", i + 1))?;
        statements.push(StatementOutcome {
            sql: (*sql).to_string(),
            elapsed: started.elapsed(),
            rows_affected: res.rows_affected(),
        });
    }
    Ok(ExecOutcome { statements })
}

async fn pg_connect(spec: &ConnSpec) -> anyhow::Result<PgConnection> {
    let ConnSpec::Postgres {
        host,
        port,
        database,
        username,
        password,
    } = spec
    else {
        unreachable!("pg_connect on a non-Postgres spec")
    };
    let mut opts =
        PgConnectOptions::new()
            .host(host)
            .port(*port)
            .username(if username.is_empty() {
                "postgres"
            } else {
                username
            });
    if !password.is_empty() {
        opts = opts.password(password);
    }
    if let Some(db) = database {
        opts = opts.database(db);
    }
    PgConnection::connect_with(&opts)
        .await
        .map_err(|e| anyhow::anyhow!("postgres 连接失败（{host}:{port}）: {e}"))
}

/// PostgreSQL: non-system schemas × their base tables (within the
/// connection database).
pub(crate) async fn pg_list_tables(spec: &ConnSpec) -> anyhow::Result<Vec<(String, String)>> {
    let mut conn = pg_connect(spec).await?;
    let rows: Vec<PgRow> = query::<Postgres>(PG_LIST_SQL)
        .fetch_all(&mut conn)
        .await
        .map_err(|e| anyhow::anyhow!("postgres 表清单查询失败: {e}"))?;
    rows.iter().map(pg_pair).collect()
}

/// PostgreSQL: column names + data types of one table.
pub(crate) async fn pg_describe(
    spec: &ConnSpec,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut conn = pg_connect(spec).await?;
    let rows: Vec<PgRow> = query::<Postgres>(PG_DESCRIBE_SQL)
        .bind(schema)
        .bind(table)
        .fetch_all(&mut conn)
        .await
        .map_err(|e| anyhow::anyhow!("postgres 列查询失败（{schema}.{table}）: {e}"))?;
    if rows.is_empty() {
        anyhow::bail!("表不存在或无列: {schema}.{table}");
    }
    rows.iter().map(pg_pair).collect()
}

fn pg_pair(r: &PgRow) -> anyhow::Result<(String, String)> {
    Ok((
        r.try_get::<String, _>(0)
            .map_err(|e| anyhow::anyhow!("postgres 行解码失败: {e}"))?,
        r.try_get::<String, _>(1)
            .map_err(|e| anyhow::anyhow!("postgres 行解码失败: {e}"))?,
    ))
}
