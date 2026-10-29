//! A7 × G4 (R7 review): `SinkCapabilities::partial_update` is a load-bearing
//! branch point, not a decoration. A plan table that declares
//! `partial_columns` on a sink whose capabilities do not flip the flag
//! (file, kafka — every non-doris sink) is refused at assembly, before any
//! source is opened or any row moves. Without the gate the runtime would
//! narrow the projection to the subset and the sink would silently write
//! subset rows under full-row semantics (rows missing columns; nothing is
//! "preserved" anywhere) — exactly the shape the doris factory refuses for
//! starrocks. The console publish gate catches this earlier for its own
//! pipelines; the engine's standalone paths (TOML FileCatalog, a hand-built
//! plan body) only have this check.
//!
//! No external dependency: the file source is never opened (assembly fails
//! first), the kafka producer is constructed lazily (no broker contact —
//! the conn-kafka unit-test convention), and the undeclared control case
//! runs against a scratch dir.

use std::path::PathBuf;

use tailrace_catalog::{PlanCatalog, PlanTableObject, StatementSpec};
use tailrace_runtime::dlq::BadRowPolicy;
use tailrace_runtime::{default_registry, PipelineRunner, RunOptions};

/// Unique scratch dir per (pid, nanos) — the offsets ledger root.
fn tmp_state_dir() -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tailrace-capgate-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).expect("create state dir");
    d
}

fn cols() -> Vec<(String, String)> {
    vec![
        ("order_no".to_string(), "utf8".to_string()),
        ("qty".to_string(), "int64".to_string()),
    ]
}

/// file source `1.local.orders_in` → one sink table of `kind`, optionally
/// declaring a partial subset. The statement itself carries no declaration:
/// the runtime reads the TABLE entry (the console projects the INSERT
/// column list there; a TOML catalog declares it on the table).
fn plan(kind: &str, bootstrap: Option<&str>, partial: Option<Vec<&str>>) -> PlanCatalog {
    let statements = vec![StatementSpec {
        source: "1.local.orders_in".into(),
        group: "capgate.s0".into(),
        sink: "1.local.orders_out".into(),
        pushdown: vec![],
        select_sql: "SELECT order_no, qty FROM src".into(),
        start_offset: None,
        dims: Vec::new(),
        partial_columns: None,
    }];
    let mut sink = PlanTableObject::new("1.local.orders_out", kind)
        .with_format("jsonl")
        .with_columns(cols());
    if let Some(b) = bootstrap {
        sink = sink.with_option(tailrace_catalog::option_keys::BOOTSTRAP, b);
    }
    if let Some(p) = partial {
        sink = sink.with_option(
            tailrace_catalog::option_keys::PARTIAL_COLUMNS,
            serde_json::json!(p),
        );
    }
    let tables = vec![
        PlanTableObject::new("1.local.orders_in", "file")
            .with_format("json")
            .with_columns(cols()),
        sink,
    ];
    PlanCatalog::new(7301, statements, tables)
}

fn assemble(cat: &PlanCatalog) -> anyhow::Result<PipelineRunner> {
    PipelineRunner::assemble(
        cat,
        &tmp_state_dir(),
        RunOptions::default(),
        None,
        BadRowPolicy::default(),
        &default_registry(),
    )
}

/// A file sink cannot preserve unlisted columns — a declaration on it is
/// an assembly error naming the statement, the sink, its kind and the
/// capability that is missing.
#[test]
fn file_sink_with_partial_columns_is_refused_at_assembly() {
    let err = assemble(&plan("file", None, Some(vec!["order_no"])))
        .err()
        .expect("file sink + partial_columns must not assemble");
    let msg = format!("{err:#}");
    assert!(msg.contains("statement #0"), "{msg}");
    assert!(msg.contains("1.local.orders_out"), "{msg}");
    assert!(msg.contains("kind=file"), "{msg}");
    assert!(
        msg.contains("capabilities().partial_update=false"),
        "the refusal must name the capability, not the concrete type: {msg}"
    );
}

/// kafka declares the honest floor too (no broker-side column semantics
/// at all) — same refusal, and the producer is never asked to connect.
#[test]
fn kafka_sink_with_partial_columns_is_refused_at_assembly() {
    let err = assemble(&plan("kafka", Some("localhost:1"), Some(vec!["order_no"])))
        .err()
        .expect("kafka sink + partial_columns must not assemble");
    let msg = format!("{err:#}");
    assert!(msg.contains("kind=kafka"), "{msg}");
    assert!(msg.contains("does not support partial update"), "{msg}");
}

/// The control: the same file sink WITHOUT a declaration assembles exactly
/// as before — the gate costs undeclared plans nothing (the pre-A7 shape is
/// untouched).
#[test]
fn undeclared_file_sink_still_assembles() {
    let runner = assemble(&plan("file", None, None));
    assert!(
        runner.is_ok(),
        "no declaration = no capability demand: {:?}",
        runner.err()
    );
}
