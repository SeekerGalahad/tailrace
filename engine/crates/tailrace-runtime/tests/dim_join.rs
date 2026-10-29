//! T-D1 dimension-table broadcast join e2e (blueprint D narrow path).
//!
//! Two layers:
//! - a no-docker **wiring** test: file src → scripted "jdbc" dim (through the
//!   connector-registry bridge, the same hole the real JDBC connector plugs
//!   into) → file sink — assemble + run, LEFT JOIN correctness including the
//!   NULL side for unmatched keys;
//! - **live MySQL** tests (`TAILRACE_TESTCONTAINERS=1`, `#[ignore]` per the
//!   conn-jdbc live-gate convention): the real JDBC full-table load with the
//!   unique-key gate — a dim table whose declared key column has no PK /
//!   single-column UNIQUE index refuses at assembly, a proper PK dim loads
//!   and the same LEFT JOIN e2e holds.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tailrace_catalog::{DimJoin, PlanCatalog, PlanTableObject, StatementSpec};
use tailrace_connector::registry::ConnectorRegistry;
use tailrace_connector::{Cursor, Source, SourceChunk};
use tailrace_runtime::dlq::BadRowPolicy;
use tailrace_runtime::{PipelineRunner, RunOptions};

// ==================== cwd-relative file fixtures (the t5/t6 pattern) ====================

static CWD_LOCK: Mutex<()> = Mutex::new(());

struct CwdGuard {
    prev: PathBuf,
    _lock: MutexGuard<'static, ()>,
}

impl CwdGuard {
    fn enter(dir: &Path) -> Self {
        let lock = CWD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::current_dir().expect("current_dir");
        std::env::set_current_dir(dir).unwrap_or_else(|e| {
            panic!("chdir into {}: {e}", dir.display());
        });
        Self { prev, _lock: lock }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.prev);
    }
}

fn tmp_workdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tailrace-dim-join-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).expect("create workdir");
    d
}

// ==================== scripted "jdbc" dim source (registry bridge) ====================

/// Scripted source standing in for the JDBC connector: hands out JSON rows
/// `page` at a time until exhausted (the drain shape the real JdbcSource's
/// full-table first round + keyset windows produce).
struct RowsSource {
    rows: Vec<Vec<u8>>,
    given: usize,
}

impl Source for RowsSource {
    fn open(&mut self, _: &Cursor) -> anyhow::Result<()> {
        Ok(())
    }
    fn poll(&mut self, max_rows: usize, _: Duration) -> anyhow::Result<Vec<SourceChunk>> {
        if self.given >= self.rows.len() {
            return Ok(vec![]);
        }
        let take = max_rows.min(self.rows.len() - self.given);
        let out: Vec<SourceChunk> = self.rows[self.given..self.given + take]
            .iter()
            .map(|r| SourceChunk::plain(r.clone()))
            .collect();
        self.given += take;
        Ok(out)
    }
    fn cursor(&self) -> Cursor {
        Cursor::None
    }
}

// The scripted dim rows ride a thread_local side channel (SourceFactory is a
// plain fn pointer; the factory fires on the assembling thread inside
// `PipelineRunner::assemble`).
thread_local! {
    static DIM_ROWS: RefCell<Option<Vec<Vec<u8>>>> = const { RefCell::new(None) };
}

fn script_dim_rows(rows: Vec<Vec<u8>>) {
    DIM_ROWS.with(|p| *p.borrow_mut() = Some(rows));
}

fn scripted_jdbc_factory(
    _o: &tailrace_catalog::TableObject,
    _s: &StatementSpec,
    _r: Option<&tailrace_catalog::ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Source>> {
    let rows = DIM_ROWS.with(|p| p.borrow_mut().take()).unwrap_or_default();
    Ok(Box::new(RowsSource { rows, given: 0 }))
}

/// The runtime's default registry (file connector in) + the scripted "jdbc"
/// factory — the same registry bridge the real tailrace-conn-jdbc plugs into.
fn mock_registry() -> ConnectorRegistry {
    let mut reg = tailrace_runtime::default_registry();
    reg.register_source("jdbc", scripted_jdbc_factory);
    reg
}

/// The live registry: default + the real JDBC connector.
fn live_registry() -> ConnectorRegistry {
    let mut reg = tailrace_runtime::default_registry();
    tailrace_conn_jdbc::register(&mut reg);
    reg
}

// ==================== plan fixtures ====================

const PIPELINE_ID: u64 = 5410;

/// One dim-join statement: file src (order_no, uid) LEFT JOIN dim_d (id,
/// name) on uid = id → file sink.
fn dim_statements(dim_reference: &str) -> Vec<StatementSpec> {
    vec![StatementSpec {
        source: "1.local.input".into(),
        group: "tr.p5410.s0".into(),
        sink: "1.local.orders_wide".into(),
        pushdown: vec![],
        select_sql: "SELECT s.order_no, d.name FROM src s LEFT JOIN dim_d d ON s.uid = d.id".into(),
        start_offset: None,
        dims: vec![DimJoin {
            reference: dim_reference.into(),
            name: "dim_d".into(),
        }],
        partial_columns: None,
    }]
}

fn plan_tables(dim: PlanTableObject) -> Vec<PlanTableObject> {
    vec![
        PlanTableObject::new("1.local.input", "file")
            .with_format("json")
            .with_columns(vec![
                ("order_no".into(), "utf8".into()),
                ("uid".into(), "int64".into()),
            ]),
        PlanTableObject::new("1.local.orders_wide", "file")
            .with_format("jsonl")
            .with_columns(vec![
                ("order_no".into(), "utf8".into()),
                ("name".into(), "utf8".into()),
            ]),
        dim,
    ]
}

fn dim_table_obj(bootstrap: Option<String>, table: &str, key: &str) -> PlanTableObject {
    let obj = PlanTableObject::new(format!("2.db.{table}"), "dim")
        .with_format("json")
        .with_columns(vec![
            ("id".into(), "int64".into()),
            ("name".into(), "utf8".into()),
        ])
        .with_option(tailrace_catalog::option_keys::SEQUENCE_COLUMN, key);
    match bootstrap {
        Some(b) => obj.with_option(tailrace_catalog::option_keys::BOOTSTRAP, b),
        None => obj,
    }
}

/// Seed the file source: uid 1 (alice), 9 (NO dim side), 2 (bob).
fn seed_input() -> std::io::Result<()> {
    std::fs::write(
        "input",
        "{\"order_no\":\"O-1\",\"uid\":1}\n{\"order_no\":\"O-9\",\"uid\":9}\n{\"order_no\":\"O-2\",\"uid\":2}\n",
    )
}

/// Read the sink output: order_no → name (None = JSON null — the LEFT JOIN
/// NULL side).
fn wide_out() -> anyhow::Result<Vec<(String, Option<String>)>> {
    let text = std::fs::read_to_string("orders_wide")?;
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l)?;
            let name = v["name"].as_str().map(str::to_string);
            Ok((v["order_no"].as_str().expect("order_no").to_string(), name))
        })
        .collect()
}

fn run_opts() -> RunOptions {
    RunOptions {
        duration: Duration::from_millis(1500),
        flush_interval: Duration::from_millis(300),
        flush_rows: 1,
        ..Default::default()
    }
}

/// The shared e2e body: assemble + run + LEFT JOIN assertions (matches, NULL
/// side, all rows flowing).
fn assert_left_join_result(stats: &[tailrace_runtime::StatementStats]) -> anyhow::Result<()> {
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].consumed, 3, "3 source rows consumed");
    assert!(stats[0].bad_rows == 0, "no bad rows: {:?}", stats[0]);
    let mut rows = wide_out()?;
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        rows,
        vec![
            ("O-1".to_string(), Some("alice".to_string())),
            ("O-2".to_string(), Some("bob".to_string())),
            ("O-9".to_string(), None),
        ],
        "LEFT JOIN: matched dim names + NULL side for the unmatched uid"
    );
    Ok(())
}

// ==================== tests ====================

/// Wiring e2e (no docker): file src → scripted "jdbc" dim (the registry
/// bridge the real connector plugs into) → file sink. Proves the T-D1
/// assemble path end to end: dims resolved → full-table load → build_dim_plan
/// → per-micro-batch transform (inject_src untouched) → sink.
#[test]
fn dim_join_left_join_wiring_e2e() -> anyhow::Result<()> {
    let work = tmp_workdir("wiring");
    let _cwd = CwdGuard::enter(&work);
    seed_input()?;
    script_dim_rows(vec![
        br#"{"id":1,"name":"alice"}"#.to_vec(),
        br#"{"id":2,"name":"bob"}"#.to_vec(),
    ]);
    let cat = PlanCatalog::new(
        PIPELINE_ID,
        dim_statements("2.db.dim_user"),
        plan_tables(dim_table_obj(
            Some("mysql://root@localhost/test".into()),
            "dim_user",
            "id",
        )),
    );
    let reg = mock_registry();
    let mut runner = PipelineRunner::assemble(
        &cat,
        &work.join("state"),
        run_opts(),
        None,
        BadRowPolicy::default(),
        &reg,
    )?;
    let stats = runner.run()?;
    assert_left_join_result(&stats)?;
    std::fs::remove_dir_all(&work).ok();
    Ok(())
}

/// Expect-assembly-error helper (PipelineRunner is not Debug — match instead
/// of expect_err).
fn assemble_err(
    cat: &PlanCatalog,
    state: &Path,
    opts: RunOptions,
    reg: &ConnectorRegistry,
) -> anyhow::Error {
    match PipelineRunner::assemble(cat, state, opts, None, BadRowPolicy::default(), reg) {
        Err(e) => e,
        Ok(_) => panic!("assembly must be refused"),
    }
}

/// Assembly-time refusals (no docker, scripted source): a dim reference that
/// does not resolve, and one resolving to a non-dim kind, reject the whole
/// assembly — no half-assembled pipeline.
#[test]
fn dim_join_assembly_refuses_unresolvable_and_wrong_kind() -> anyhow::Result<()> {
    let work = tmp_workdir("refuse");
    let _cwd = CwdGuard::enter(&work);
    seed_input()?;
    let reg = mock_registry();
    // unresolvable dim reference
    let cat = PlanCatalog::new(
        PIPELINE_ID,
        dim_statements("2.db.no_such"),
        plan_tables(dim_table_obj(
            Some("mysql://root@localhost/test".into()),
            "dim_user",
            "id",
        )),
    );
    let err = assemble_err(&cat, &work.join("state"), run_opts(), &reg);
    assert!(format!("{err:#}").contains("no_such"), "{err:#}");
    // dim reference resolving to a kind=file object (not dim)
    let mut tables = plan_tables(dim_table_obj(None, "dim_user", "id"));
    tables[2] = PlanTableObject::new("2.db.dim_user", "file")
        .with_format("json")
        .with_columns(vec![("id".into(), "int64".into())]);
    let cat = PlanCatalog::new(PIPELINE_ID, dim_statements("2.db.dim_user"), tables);
    let err = assemble_err(&cat, &work.join("state"), run_opts(), &reg);
    assert!(
        format!("{err:#}").contains("must be a kind=dim table object"),
        "{err:#}"
    );
    std::fs::remove_dir_all(&work).ok();
    Ok(())
}

fn gated() -> bool {
    std::env::var("TAILRACE_TESTCONTAINERS").ok().as_deref() == Some("1")
}

/// Live e2e (ephemeral mysql:8.0): the real JDBC full-table dim load.
/// ① a dim table whose declared key column has NO PK / single-column UNIQUE
///    index refuses at assembly (the keyset uniqueness gate reused from
///    conn-jdbc — cartesian-amplification guard);
/// ② a proper PK dim loads full-table and the LEFT JOIN output is exact
///    (matches + NULL side), plus the row cap refuses a load that crosses it.
#[ignore = "live: TAILRACE_TESTCONTAINERS=1 (ephemeral mysql:8.0) or CI with --include-ignored"]
#[test]
fn mysql_dim_join_e2e_and_unique_key_gate() -> anyhow::Result<()> {
    use testcontainers::core::ImageExt;
    use testcontainers::runners::SyncRunner;
    if !gated() {
        eprintln!("[skip] TAILRACE_TESTCONTAINERS!=1 — dim join live tests skipped");
        return Ok(());
    }
    let node = testcontainers_modules::mysql::Mysql::default()
        .with_tag("8.0")
        .start()?;
    let port = node.get_host_port_ipv4(3306)?;
    let url = format!("mysql://root@127.0.0.1:{port}/test");

    // schema + seed (real write path through the same driver stack)
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_core::sql_str::AssertSqlSafe;
        use sqlx_mysql::MySqlConnection;
        let mut conn = MySqlConnection::connect(&url).await?;
        conn.execute(AssertSqlSafe(
            "CREATE TABLE dim_user (id BIGINT PRIMARY KEY, name VARCHAR(64))",
        ))
        .await?;
        conn.execute(AssertSqlSafe(
            "INSERT INTO dim_user VALUES (1, 'alice'), (2, 'bob')",
        ))
        .await?;
        // no PK/UNIQUE on ts — only a plain index (the refusal case)
        conn.execute(AssertSqlSafe(
            "CREATE TABLE bad_dim (id BIGINT PRIMARY KEY, name VARCHAR(64), ts DATETIME NOT NULL, KEY bad_ts (ts))",
        ))
        .await?;
        conn.execute(AssertSqlSafe(
            "INSERT INTO bad_dim VALUES (1, 'x', '2026-03-01 00:00:00'), (2, 'y', '2026-03-01 00:00:00')",
        ))
        .await?;
        anyhow::Ok(())
    })?;

    let work = tmp_workdir("live");
    let _cwd = CwdGuard::enter(&work);
    seed_input()?;
    let reg = live_registry();

    // ① no unique key → assembly refused with the gate's message
    let cat = PlanCatalog::new(
        PIPELINE_ID,
        dim_statements("2.db.bad_dim"),
        plan_tables(dim_table_obj(Some(url.clone()), "bad_dim", "ts")),
    );
    let err = assemble_err(&cat, &work.join("state"), run_opts(), &reg);
    let msg = format!("{err:#}");
    assert!(
        msg.contains("dim table 2.db.bad_dim"),
        "error carries the dim context: {msg}"
    );
    assert!(
        msg.contains("has no PRIMARY KEY / single-column UNIQUE index"),
        "the keyset uniqueness gate (reused from conn-jdbc) names the missing key: {msg}"
    );

    // ② proper PK dim → full e2e
    let cat = PlanCatalog::new(
        PIPELINE_ID,
        dim_statements("2.db.dim_user"),
        plan_tables(dim_table_obj(Some(url.clone()), "dim_user", "id")),
    );
    let mut runner = PipelineRunner::assemble(
        &cat,
        &work.join("state"),
        run_opts(),
        None,
        BadRowPolicy::default(),
        &reg,
    )?;
    let stats = runner.run()?;
    assert_left_join_result(&stats)?;

    // ③ row cap: a 2-row dim under a 1-row cap refuses at assembly
    let cat = PlanCatalog::new(
        PIPELINE_ID,
        dim_statements("2.db.dim_user"),
        plan_tables(dim_table_obj(Some(url), "dim_user", "id")),
    );
    let opts = RunOptions {
        dim_max_rows: 1,
        ..run_opts()
    };
    let err = assemble_err(&cat, &work.join("state2"), opts, &reg);
    assert!(
        format!("{err:#}").contains("exceeds the row cap"),
        "{err:#}"
    );

    std::fs::remove_dir_all(&work).ok();
    node.rm()?;
    Ok(())
}
