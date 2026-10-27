//! T13 integration tests: real MySQL/PostgreSQL via testcontainers.
//!
//! Gating: `TAILRACE_TESTCONTAINERS=1` boots ephemeral mysql:8.0 /
//! postgres:15 (tags pinned to the locally preheated images — no pull);
//! the tests are `#[ignore = "live: …"]` so the default skip shows as an
//! ignored count (T25 honest ledger); `--include-ignored` runs execute
//! them, and without the flag the in-body gate still skips (conn-kafka's
//! live-gate convention).
//! Assertions per the task card: restart continuation neither loses nor
//! duplicates rows (keyset cursor), cumulative totals stay monotonic, and
//! each dialect performs at least one real read+write round trip through
//! the standard decode chain (including a bad row → `Decoded::Bad`, the
//! runtime's existing DLQ route).

use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use tailrace_catalog::TableObject;
use tailrace_conn_jdbc::register;
use tailrace_connector::registry::ConnectorRegistry;
use tailrace_connector::{Cursor, Source, SourceChunk};
use tailrace_decode::{ColBatch, Decoded, Decoder, JsonDecoder, Pushdown};
use testcontainers::core::ImageExt;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::{mysql, postgres};

fn gated() -> bool {
    std::env::var("TAILRACE_TESTCONTAINERS").ok().as_deref() == Some("1")
}

fn jdbc_obj(url: &str, table: &str, cursor_col: &str) -> TableObject {
    TableObject {
        datasource: "db".into(),
        name: table.into(),
        kind: "jdbc".into(),
        format: Some("json".into()),
        columns: vec![],
        bootstrap: Some(url.to_string()),
        stream_load_url: None,
        auth_env: None,
        label_prefix: None,
        sequence_column: Some(cursor_col.into()),
    }
}

fn build_source(url: &str, table: &str, cursor_col: &str) -> Box<dyn Source> {
    let mut reg = ConnectorRegistry::new();
    register(&mut reg);
    let obj = jdbc_obj(url, table, cursor_col);
    let spec = tailrace_catalog::StatementSpec {
        source: format!("db.{table}"),
        group: "tr.p9901.s0".into(),
        sink: format!("db.{table}"),
        pushdown: vec![],
        select_sql: "SELECT * FROM src".into(),
        start_offset: None,
        dims: Vec::new(),
    };
    reg.build_source(&obj, &spec, None)
        .expect("jdbc source builds")
}

/// Drain a source to exhaustion in ≤max-per-poll slices, collecting chunk
/// payloads; returns (raw json objects, final cursor).
fn drain(src: &mut Box<dyn Source>, max_rows: usize) -> (Vec<Vec<u8>>, Cursor) {
    let mut all = Vec::new();
    loop {
        let chunks = src.poll(max_rows, Duration::from_secs(5)).expect("poll ok");
        if chunks.is_empty() {
            break;
        }
        for SourceChunk { raw, .. } in chunks {
            all.push(raw);
        }
    }
    (all, src.cursor())
}

fn ids_of(raws: &[Vec<u8>]) -> Vec<i64> {
    raws.iter()
        .map(|b| {
            serde_json::from_slice::<serde_json::Value>(b)
                .expect("chunk is valid JSON")
                .get("id")
                .and_then(|v| v.as_i64())
                .expect("id is a JSON number")
        })
        .collect()
}

/// Decode every chunk through the STANDARD JsonDecoder against the declared
/// schema; returns the number of Appended rows and the first Bad payload if
/// any (the DLQ channel proof).
fn decode_all(raws: &[Vec<u8>], schema: Arc<Schema>) -> (usize, Option<Vec<u8>>) {
    let dec = JsonDecoder;
    let mut appended = 0usize;
    let mut bad: Option<Vec<u8>> = None;
    for raw in raws {
        let mut batch = ColBatch::new(schema.clone(), 1);
        let out = dec.decode(raw.clone(), &mut batch, &Pushdown::default());
        match out {
            Decoded::Append => appended += 1,
            Decoded::Skip => {}
            Decoded::Bad(payload) => bad = Some(payload),
        }
    }
    (appended, bad)
}

#[ignore = "live: TAILRACE_TESTCONTAINERS=1 (ephemeral mysql:8.0) or CI chaos job with --include-ignored"]
#[test]
fn mysql_keyset_cursor_restart_no_loss_no_dup() -> anyhow::Result<()> {
    if !gated() {
        eprintln!("[skip] TAILRACE_TESTCONTAINERS!=1 — jdbc live tests skipped");
        return Ok(());
    }
    let node = mysql::Mysql::default().with_tag("8.0").start()?;
    let port = node.get_host_port_ipv4(3306)?;
    let url = format!("mysql://root@127.0.0.1:{port}/test");

    // schema + seed (real write path through the same driver stack)
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_mysql::MySqlConnection;
        let mut conn = MySqlConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64), ratio DOUBLE, born DATETIME)",
        ))
        .await?;
        for i in 1i64..=5 {
            let sql = format!(
                "INSERT INTO orders VALUES ({i}, 'n{i}', {i}.5, '2026-01-0{i} 10:00:00')"
            );
            conn.execute(AssertSqlSafe(sql)).await?;
        }
        anyhow::Ok(())
    })?;

    // ① first run: read a 2-row slice, cursor = JdbcKey{id, "2"}
    let mut src = build_source(&url, "orders", "id");
    src.open(&Cursor::None)?;
    let first = src.poll(2, Duration::from_secs(5))?;
    assert_eq!(first.len(), 2, "max_rows bounds the poll");
    assert_eq!(
        ids_of(&first.iter().map(|c| c.raw.clone()).collect::<Vec<_>>()),
        vec![1, 2]
    );
    let cur = src.cursor();
    let Cursor::JdbcKey { key, value } = &cur else {
        panic!("cursor kind must be JdbcKey, got {cur:?}")
    };
    assert_eq!((key.as_str(), value.as_str()), ("id", "2"));

    // ② restart: a NEW source instance resumes from the committed cursor —
    // continues with 3..=5, never re-reading 1..=2 (no dup) nor skipping
    let mut src2 = build_source(&url, "orders", "id");
    src2.open(&cur)?;
    let (rest_raw, cur2) = drain(&mut src2, 10);
    assert_eq!(ids_of(&rest_raw), vec![3, 4, 5], "resume: no loss, no dup");
    let Cursor::JdbcKey { value, .. } = &cur2 else {
        panic!()
    };
    assert_eq!(value, "5");

    // ③ late inserts: the SAME resumed instance picks up only the new rows
    rt.block_on(async {
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_mysql::MySqlConnection;
        let mut conn = MySqlConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "INSERT INTO orders VALUES (6, 'n6', 6.5, '2026-01-06 10:00:00'), (7, 'n7', 7.5, '2026-01-07 10:00:00')",
        ))
        .await?;
        anyhow::Ok(())
    })?;
    let (late_raw, cur3) = drain(&mut src2, 10);
    assert_eq!(
        ids_of(&late_raw),
        vec![6, 7],
        "cursor advance → only new rows"
    );
    let Cursor::JdbcKey { value, .. } = &cur3 else {
        panic!()
    };
    assert_eq!(value, "7");

    // monotonic totals across the whole lifecycle (restart included)
    let total = 2 + rest_raw.len() + late_raw.len();
    assert_eq!(total, 7, "7 rows seen exactly once overall");

    // ④ real read-write through the standard decode chain
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ratio", DataType::Float64, true),
        Field::new("born", DataType::Utf8, true),
    ]));
    let all_raw: Vec<Vec<u8>> = first
        .iter()
        .map(|c| c.raw.clone())
        .chain(rest_raw)
        .chain(late_raw)
        .collect();
    let (appended, bad) = decode_all(&all_raw, schema.clone());
    assert_eq!(
        appended, 7,
        "all rows decode Append against the matching schema"
    );
    assert!(bad.is_none());

    // ⑤ bad rows ride the EXISTING channel (no jdbc bypass): a malformed
    // chunk (e.g. corrupt bytes from a driver bug) surfaces as Decoded::Bad
    // through the same JsonDecoder the runtime routes to DLQ. Note the
    // JsonDecoder's tolerant typed chain maps a *valid* JSON type mismatch
    // (string value, int64 column) to null — existing decode semantics,
    // unchanged by this crate.
    let mut poisoned = all_raw[0].clone();
    poisoned.truncate(poisoned.len() - 3); // break the JSON
    let mut batch = ColBatch::new(schema, 1);
    let out = JsonDecoder.decode(poisoned.clone(), &mut batch, &Pushdown::default());
    match out {
        Decoded::Bad(b) => assert_eq!(b, poisoned, "bad payload round-trips to the DLQ writer"),
        other => panic!("malformed chunk must be Decoded::Bad (DLQ route), got {other:?}"),
    }

    // ⑥ JSON shape: typed values survive (number/string), datetime RFC3339
    let v: serde_json::Value = serde_json::from_slice(&all_raw[0])?;
    assert_eq!(v["id"], serde_json::json!(1));
    assert_eq!(v["name"], serde_json::json!("n1"));
    assert_eq!(v["ratio"], serde_json::json!(1.5)); // id=1 → ratio = 1.5
    assert_eq!(v["born"], serde_json::json!("2026-01-01T10:00:00Z"));

    // ⑦ keyset precondition (R3-W1 review fix): a cursor column with only a
    // NON-unique index is refused — duplicate values straddling a LIMIT
    // boundary would otherwise be skipped forever by the next `>` window.
    // The probe failure is deferred from open() to poll() (connectivity
    // convention), so the refusal surfaces as the poll error.
    rt.block_on(async {
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_mysql::MySqlConnection;
        let mut conn = MySqlConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "CREATE TABLE ticks (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL, KEY ticks_ts (ts))",
        ))
        .await?;
        conn.execute(AssertSqlSafe(
            "INSERT INTO ticks VALUES (1, '2026-03-01 00:00:00'), (2, '2026-03-01 00:00:00'), (3, '2026-03-01 00:00:00')",
        ))
        .await?;
        anyhow::Ok(())
    })?;
    let mut non_unique = build_source(&url, "ticks", "ts");
    non_unique.open(&Cursor::None)?;
    let err = non_unique
        .poll(2, Duration::from_secs(5))
        .expect_err("non-unique cursor column must be refused");
    assert!(
        format!("{err:#}").contains("has no PRIMARY KEY / single-column UNIQUE index"),
        "refusal names the missing UNIQUE index: {err:#}"
    );
    // ...the PK of the same table passes the gate, and so does a
    // schema-qualified table name (COALESCE(?, DATABASE()) path)
    let mut by_pk = build_source(&url, "ticks", "id");
    by_pk.open(&Cursor::None)?;
    let (tick_raw, _) = drain(&mut by_pk, 2);
    assert_eq!(
        tick_raw.len(),
        3,
        "PK cursor column passes the uniqueness gate"
    );
    let mut qualified = build_source(&url, "test.orders", "id");
    qualified.open(&Cursor::None)?;
    let (q_raw, _) = drain(&mut qualified, 10);
    assert_eq!(
        q_raw.len(),
        7,
        "schema-qualified table passes the uniqueness gate"
    );

    node.rm()?; // cleanup
    Ok(())
}

#[ignore = "live: TAILRACE_TESTCONTAINERS=1 (ephemeral postgres:15) or CI chaos job with --include-ignored"]
#[test]
fn postgres_keyset_cursor_int_and_timestamptz() -> anyhow::Result<()> {
    if !gated() {
        eprintln!("[skip] TAILRACE_TESTCONTAINERS!=1 — jdbc live tests skipped");
        return Ok(());
    }
    let node = postgres::Postgres::default().with_tag("15").start()?;
    let port = node.get_host_port_ipv4(5432)?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_postgres::PgConnection;
        let mut conn = PgConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, name TEXT, ratio DOUBLE PRECISION, born TIMESTAMPTZ)",
        ))
        .await?;
        for i in 1i64..=5 {
            let sql = format!(
                "INSERT INTO orders VALUES ({i}, 'n{i}', {i}.5, '2026-01-0{i} 10:00:00+00')"
            );
            conn.execute(AssertSqlSafe(sql)).await?;
        }
        anyhow::Ok(())
    })?;

    // ① int cursor: slice + restart continuation, no loss no dup
    let mut src = build_source(&url, "orders", "id");
    src.open(&Cursor::None)?;
    let first = src.poll(3, Duration::from_secs(5))?;
    assert_eq!(first.len(), 3);
    let cur = src.cursor();
    let Cursor::JdbcKey { key, value } = &cur else {
        panic!()
    };
    assert_eq!((key.as_str(), value.as_str()), ("id", "3"));
    let mut src2 = build_source(&url, "orders", "id");
    src2.open(&cur)?;
    let (rest_raw, cur2) = drain(&mut src2, 10);
    assert_eq!(ids_of(&rest_raw), vec![4, 5]);
    let Cursor::JdbcKey { value, .. } = &cur2 else {
        panic!()
    };
    assert_eq!(value, "5");
    assert_eq!(3 + rest_raw.len(), 5, "totals: 5 rows exactly once");

    // ② standard decode chain (same JsonDecoder contract)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ratio", DataType::Float64, true),
        Field::new("born", DataType::Utf8, true),
    ]));
    let all_raw: Vec<Vec<u8>> = first
        .iter()
        .map(|c| c.raw.clone())
        .chain(rest_raw)
        .collect();
    let (appended, bad) = decode_all(&all_raw, schema);
    assert_eq!(appended, 5);
    assert!(bad.is_none(), "no bad rows against the matching schema");

    // ③ timestamptz cursor: the time-column increment variant
    rt.block_on(async {
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_postgres::PgConnection;
        let mut conn = PgConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "CREATE TABLE events (id BIGINT PRIMARY KEY, payload TEXT, created_at TIMESTAMPTZ NOT NULL UNIQUE)",
        ))
        .await?;
        conn.execute(AssertSqlSafe(
            "INSERT INTO events VALUES (1, 'e1', '2026-02-01 00:00:00+00'), (2, 'e2', '2026-02-02 00:00:00+00'), (3, 'e3', '2026-02-03 00:00:00+00')",
        ))
        .await?;
        anyhow::Ok(())
    })?;
    let mut tsrc = build_source(&url, "events", "created_at");
    tsrc.open(&Cursor::None)?;
    let (first_t_raw, tcur) = drain(&mut tsrc, 2);
    assert_eq!(first_t_raw.len(), 3);
    let Cursor::JdbcKey { key, value } = &tcur else {
        panic!()
    };
    assert_eq!(key, "created_at");
    assert_eq!(
        value, "2026-02-03T00:00:00Z",
        "timestamptz cursor canonicalizes to RFC3339 UTC"
    );

    // late event strictly after the cursor → only it flows
    rt.block_on(async {
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_postgres::PgConnection;
        let mut conn = PgConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "INSERT INTO events VALUES (4, 'e4', '2026-02-04 00:00:00+00')",
        ))
        .await?;
        anyhow::Ok(())
    })?;
    let mut tsrc2 = build_source(&url, "events", "created_at");
    tsrc2.open(&tcur)?;
    let (late_t_raw, _) = drain(&mut tsrc2, 2);
    assert_eq!(late_t_raw.len(), 1);
    let v: serde_json::Value = serde_json::from_slice(&late_t_raw[0])?;
    assert_eq!(v["payload"], serde_json::json!("e4"));

    // ④ keyset precondition (R3-W1 review fix): the leading column of a
    // composite PRIMARY KEY is not unique by itself → refused (pg_index
    // indnkeyatts = 1 predicate); a schema-qualified name resolves through
    // the pg_namespace join and passes for the real PK.
    rt.block_on(async {
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_postgres::PgConnection;
        let mut conn = PgConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "CREATE TABLE pairs (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
        ))
        .await?;
        conn.execute(AssertSqlSafe(
            "INSERT INTO pairs VALUES (1, 1), (1, 2), (2, 1)",
        ))
        .await?;
        anyhow::Ok(())
    })?;
    let mut composite = build_source(&url, "pairs", "a");
    composite.open(&Cursor::None)?;
    let err = composite
        .poll(2, Duration::from_secs(5))
        .expect_err("composite-PK leading column must be refused");
    assert!(
        format!("{err:#}").contains("has no PRIMARY KEY / single-column UNIQUE index"),
        "refusal names the missing UNIQUE index: {err:#}"
    );
    let mut qualified = build_source(&url, "public.orders", "id");
    qualified.open(&Cursor::None)?;
    let (q_raw, _) = drain(&mut qualified, 10);
    assert_eq!(
        q_raw.len(),
        5,
        "schema-qualified table passes the uniqueness gate"
    );

    node.rm()?;
    Ok(())
}
