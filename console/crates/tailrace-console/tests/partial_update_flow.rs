//! T-A7 partial-column update, control-plane contract (R7 W3): the
//! statement-level switch (INSERT column list) through compile → publish
//! gate → plan projection → the evolve × deploy mutex, against the mock
//! engine / a header-capturing mock BE. No live Doris: the FE side is the
//! MockFe below — a real MySQL-wire server answering SHOW CREATE TABLE from
//! a script (LOW-5, R8: the publish gate asks the live table once the
//! registration gate passes) and refusing every other query, which keeps
//! the deploy-side precheck (SHOW ALTER TABLE COLUMN) on its 502 path; the
//! in-flight 409 and the MoW "unwrapped column keeps its value" semantics
//! are the W4 e2e's.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use axum::routing::put;
use axum::{Json, Router};
use common::*;
use console_auth::BootstrapAuthority;
use serde_json::{json, Value};
use tailrace_console::state::AppState;

const PARTIAL_SQL: &str =
    "INSERT INTO ods.orders (id, payload) SELECT id, payload FROM shop.orders WHERE op_type IN ('I','U')";

/// Live SHOW CREATE TABLE answers, the shapes e2e S4.2/S4.3 asserted on
/// real Doris 2.1.7 / 3.1.4 (scripts/e2e/partial_update.sh): UNIQUE MoW
/// carries the explicit property, DUPLICATE has no MoW story, MoR carries
/// the explicit false.
const LIVE_MOW: &str = "CREATE TABLE `orders` (\n  `order_no` varchar(65533) NOT NULL,\n  `id` bigint NULL,\n  `payload` varchar(65533) NULL\n) ENGINE=OLAP\nUNIQUE KEY(`order_no`)\nCOMMENT 'OLAP'\nDISTRIBUTED BY HASH(`order_no`) BUCKETS 1\nPROPERTIES (\n\"replication_num\" = \"1\",\n\"enable_unique_key_merge_on_write\" = \"true\"\n);";
const LIVE_DUP: &str = "CREATE TABLE `orders` (\n  `order_no` varchar(65533) NOT NULL,\n  `id` bigint NULL,\n  `payload` varchar(65533) NULL\n) ENGINE=OLAP\nDUPLICATE KEY(`order_no`, `id`)\nDISTRIBUTED BY HASH(`order_no`) BUCKETS 1\nPROPERTIES (\n\"replication_num\" = \"1\"\n);";
const LIVE_MOR: &str = "CREATE TABLE `orders` (\n  `order_no` varchar(65533) NOT NULL,\n  `id` bigint NULL\n) ENGINE=OLAP\nUNIQUE KEY(`order_no`)\nDISTRIBUTED BY HASH(`order_no`) BUCKETS 1\nPROPERTIES (\n\"replication_allocation\" = \"tag.location.default: 1\",\n\"enable_unique_key_merge_on_write\" = \"false\"\n);";

/// Create a pipeline over the seeded datasources (not published).
async fn pipeline(app: &TestApp, name: &str, sql: &str, kafka: i64, doris: i64) -> i64 {
    let (s, b) = req(
        app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": name, "sqlContent": sql,
            "dsMapping": {"shop": kafka, "ods": doris},
            "checkpointMs": 10000
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    b["id"].as_i64().unwrap()
}

async fn publish(app: &TestApp, pid: i64) -> (StatusCode, Value) {
    req(app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await
}

fn diag_text(b: &Value) -> String {
    b["diagnostics"]
        .as_array()
        .map(|d| {
            d.iter()
                .map(|x| x["message"].as_str().unwrap_or("").to_string())
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .unwrap_or_default()
}

async fn set_key_model(app: &TestApp, table: i64, model: &str) -> (StatusCode, Value) {
    req(
        app,
        "PUT",
        &format!("/api/datasources/tables/{table}"),
        Some(json!({"keyModel": model})),
    )
    .await
}

/// A fresh doris datasource + ods.orders table (keyModel=unique at
/// registration) with its jdbcUrl at `jdbc_port` — 19030 is nothing
/// listening, i.e. the unreachable-FE path; a MockFe port is the live one.
async fn doris_ds_at(app: &TestApp, jdbc_port: u16, name: &str) -> (i64, i64) {
    let (s, b) = req(
        app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": name, "type": "doris",
            "connJson": {
                "jdbcUrl": format!("jdbc:mysql://127.0.0.1:{jdbc_port}/shop"),
                "streamLoadUrl": "http://127.0.0.1:18040"
            },
            "username": "loader", "password": "pw"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let ds = b["id"].as_i64().unwrap();
    let (s, b) = req(
        app,
        "POST",
        &format!("/api/datasources/{ds}/tables"),
        Some(json!({
            "dbName": "ods", "tableName": "orders", "format": null,
            "columnsJson": [["id", "int64"], ["payload", "utf8"]],
            "keyModel": "unique"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    (ds, b["id"].as_i64().unwrap())
}

/// The switch through the console compile endpoint, both states: no column
/// list → `partialColumns: null`, a column list → the names in declaration
/// order; a duplicate entry is a compile diagnostic (the parser's refusal,
/// surfaced in the editor shape).
#[tokio::test]
async fn compile_exposes_the_partial_columns_switch() {
    let app = app().await;
    let (kafka, doris, _, _) = seed_datasources(&app).await;
    let mapping = json!({"shop": kafka, "ods": doris});

    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines/compile",
        Some(json!({"sql": PIPELINE_SQL, "dsMapping": mapping, "pipelineId": 7})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(b["ok"].as_bool().unwrap());
    assert!(
        b["statements"][0]["partialColumns"].is_null(),
        "no INSERT column list = null (full-row writes): {b}"
    );

    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines/compile",
        Some(json!({"sql": PARTIAL_SQL, "dsMapping": mapping, "pipelineId": 7})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["statements"][0]["partialColumns"],
        json!(["id", "payload"]),
        "the list rides verbatim: {b}"
    );
    // the rest of the statement shape is unchanged by the list
    assert_eq!(b["statements"][0]["sink"], format!("{doris}.ods.orders"));
    assert_eq!(b["statements"][0]["pushdown"], json!(["I", "U"]));

    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines/compile",
        Some(json!({
            "sql": "INSERT INTO ods.orders (id, id) SELECT id, payload FROM shop.orders",
            "dsMapping": mapping
        })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(!b["ok"].as_bool().unwrap());
    assert!(
        diag_text(&b).contains("duplicate column id"),
        "duplicate entry diagnosed: {b}"
    );
}

/// Publish gate, the Doris semantic constraint: a partial declaration needs
/// a doris sink whose key model is on record as `unique` (partial update is
/// UNIQUE KEY merge-on-write only) and whose registered columns cover the
/// list; every statement on one sink must declare the same thing. The
/// refusals are compile-shaped diagnostics; the undeclared pipeline (the
/// regression anchor) publishes untouched.
#[tokio::test]
async fn publish_gate_enforces_doris_partial_update_semantics() {
    let app = app().await;
    // LOW-5: the gate's second half asks the live FE once registration
    // passes — a MockFe answering SHOW CREATE TABLE with a MoW UNIQUE table
    // keeps the ③ "gate opens" leg meaningful; the refusals ①②④⑤⑥ never
    // reach the FE
    let fe = MockFe::default();
    let fe_port = fe.clone().serve(LIVE_MOW).await;
    let (kafka, doris, kafka_table, doris_table) = seed_datasources_at_fe(&app, fe_port).await;

    // the regression anchor: no declaration, no key model needed
    let plain = pipeline(&app, "plain", PIPELINE_SQL, kafka, doris).await;
    let (s, b) = publish(&app, plain).await;
    assert_eq!(
        s,
        StatusCode::OK,
        "undeclared pipeline publishes as before: {b}"
    );

    // ① key model not on record → refused, pointing at the PUT
    let pid = pipeline(&app, "partial", PARTIAL_SQL, kafka, doris).await;
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    let text = diag_text(&b);
    assert!(
        text.contains("未登记键模型") && text.contains("keyModel=unique"),
        "{text}"
    );

    // ② DUPLICATE KEY on record → refused (Doris rejects the partial load)
    let (s, b) = set_key_model(&app, doris_table, "duplicate").await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["keyModel"], "duplicate");
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        diag_text(&b).contains("keyModel=duplicate"),
        "{}",
        diag_text(&b)
    );
    // the list endpoint shows the record
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/datasources/tables?datasourceId={doris}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["items"][0]["keyModel"], "duplicate", "{b}");

    // ③ UNIQUE on record → the registration gate passes and the live gate
    // (MockFe: MoW UNIQUE) agrees → publish
    let (s, _) = set_key_model(&app, doris_table, "Unique").await;
    assert_eq!(s, StatusCode::OK, "case-insensitive input");
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(
        b["specJson"]
            .as_str()
            .unwrap()
            .contains(&format!("${{ds:{doris}.ods.orders}} (id, payload)")),
        "the materialized spec keeps the column list: {b}"
    );

    // ④ a declared column the sink does not register → refused
    let bad_col = pipeline(
        &app,
        "bad-col",
        "INSERT INTO ods.orders (id, nope) SELECT id, payload FROM shop.orders",
        kafka,
        doris,
    )
    .await;
    let (s, b) = publish(&app, bad_col).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(diag_text(&b).contains("未登记列 nope"), "{}", diag_text(&b));

    // ⑤ two statements on one sink, one declaring and one not → refused
    let mixed = pipeline(
        &app,
        "mixed",
        &format!("{PARTIAL_SQL}; {PIPELINE_SQL}"),
        kafka,
        doris,
    )
    .await;
    let (s, b) = publish(&app, mixed).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        diag_text(&b).contains("部分列声明不一致"),
        "{}",
        diag_text(&b)
    );

    // ⑥ a non-doris sink has no partial mode: kafka (source table as sink)
    let kafka_sink = pipeline(
        &app,
        "kafka-sink",
        "INSERT INTO shop.orders (id) SELECT id FROM shop.orders",
        kafka,
        doris,
    )
    .await;
    let (s, b) = publish(&app, kafka_sink).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(diag_text(&b).contains("kind=kafka"), "{}", diag_text(&b));
    // …and starrocks is refused at publish too (its partial update is a
    // different load contract the engine does not wire)
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "sr", "type": "starrocks",
            "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:19031/shop", "streamLoadUrl": "http://127.0.0.1:18041"},
            "username": "u", "password": "p"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let sr = b["id"].as_i64().unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{sr}/tables"),
        Some(json!({
            "dbName": "ods", "tableName": "orders",
            "columnsJson": [["id", "int64"], ["payload", "utf8"]],
            "keyModel": "unique"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "keyModel accepted at registration: {b}");
    let sr_pipe = pipeline(&app, "sr", PARTIAL_SQL, kafka, sr).await;
    let (s, b) = publish(&app, sr_pipe).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        diag_text(&b).contains("kind=starrocks"),
        "{}",
        diag_text(&b)
    );

    // key-model PUT input gate: not a Doris concept on a kafka table, only
    // unique/duplicate, the field is required, unknown id → 404
    let (s, b) = set_key_model(&app, kafka_table, "unique").await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(
        b["error"]["message_key"],
        "err.table_object.key_model_datasource"
    );
    let (s, b) = set_key_model(&app, doris_table, "aggregate").await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.key_model_unknown");
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/datasources/tables/{doris_table}"),
        Some(json!({})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.key_model_required");
    let (s, _) = set_key_model(&app, 999_999, "unique").await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// LOW-5 (R8): the key model on record is not the last word — once the
/// registration gate passes (kind=doris, keyModel=unique on record),
/// publish asks the LIVE table: `SHOW CREATE TABLE` over the MySQL wire
/// (the MockFe speaks the protocol the embedded sqlx client needs).
/// Registered unique but live DUPLICATE / MoR → 400 naming the live
/// verdict; the FE unreachable → fail-closed 502 (the DDL endpoints' /
/// deploy precheck's stance), nothing published; live MoW → the gate
/// opens; an undeclared pipeline never asks the FE (regression anchor).
#[tokio::test]
async fn publish_gate_verifies_the_live_key_model() {
    let app = app().await;
    let fe = MockFe::default();
    let fe_port = fe.clone().serve(LIVE_DUP).await;
    let (kafka, doris, _, doris_table) = seed_datasources_at_fe(&app, fe_port).await;
    set_key_model(&app, doris_table, "unique").await;
    let pid = pipeline(&app, "live-dup", PARTIAL_SQL, kafka, doris).await;

    // registered unique, live DUPLICATE → 400 with the live verdict named
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        diag_text(&b).contains("活表 SHOW CREATE TABLE 核实为 DUPLICATE KEY"),
        "{}",
        diag_text(&b)
    );
    // the wire shape: one fully-qualified backticked query per sink
    assert_eq!(
        fe.queries.lock().unwrap().last().cloned(),
        Some("SHOW CREATE TABLE `ods`.`orders`".to_string()),
        "{:?}",
        fe.queries.lock().unwrap()
    );
    let (_, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(b["status"], "DRAFT", "nothing published on a refusal");

    // live UNIQUE but merge-on-read → same refusal, the MoR verdict
    fe.set_ddl(LIVE_MOR);
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(diag_text(&b).contains("merge-on-read"), "{}", diag_text(&b));

    // live UNIQUE merge-on-write → the gate opens
    fe.set_ddl(LIVE_MOW);
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::OK, "{b}");

    // FE unreachable (nothing listens on 19030) → fail-closed 502 naming
    // the check, nothing published — the record says unique, but a 3am
    // load failure is not an acceptable substitute for an answer
    let (doris_off, _) = doris_ds_at(&app, 19030, "doris-off").await;
    let off = pipeline(&app, "fe-off", PARTIAL_SQL, kafka, doris_off).await;
    let (s, b) = publish(&app, off).await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "UNAVAILABLE", "{b}");
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("SHOW CREATE TABLE"),
        "{b}"
    );
    let (_, b) = req(&app, "GET", &format!("/api/pipelines/{off}"), None).await;
    assert_eq!(b["status"], "DRAFT", "{b}");

    // regression anchor: no declaration → no FE round trip, unreachable or
    // not — the pre-LOW-5 publish shape
    let plain = pipeline(&app, "plain-off", PIPELINE_SQL, kafka, doris_off).await;
    let (s, b) = publish(&app, plain).await;
    assert_eq!(s, StatusCode::OK, "{b}");
}

/// Deploy side of the mutex: a plan with a partial-column sink is prechecked
/// live (`SHOW ALTER TABLE COLUMN`) BEFORE anything reaches the engine —
/// here the FE is unreachable, so the answer is 502 UNAVAILABLE naming the
/// check, and the mock engine never sees a plan. recover and a backfill
/// accept ride the same gate (no job row is created). The undeclared
/// pipeline deploys as before, its plan entries carrying no
/// `partial_columns` key at all (byte-shape anchor).
#[tokio::test]
async fn deploy_prechecks_schema_change_before_the_engine_push() {
    let app = app().await;
    let (st, port) = spawn_mock_engine().await;
    // the FE is a MockFe serving a MoW UNIQUE SHOW CREATE (publish's LOW-5
    // live gate needs an answerable FE) that refuses everything else — the
    // deploy-side SHOW ALTER precheck keeps its 502 path
    let fe = MockFe::default();
    let fe_port = fe.clone().serve(LIVE_MOW).await;
    let (kafka, doris, _, doris_table) = seed_datasources_at_fe(&app, fe_port).await;
    online_node(&app, port, "n1").await;

    // undeclared pipeline: deploys, no partial_columns anywhere in the plan
    let plain = published_pipeline(&app, kafka, doris).await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{plain}/deploy"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let plan = st.plans.lock().unwrap()[0].clone();
    for t in plan["tables"].as_array().unwrap() {
        assert!(
            t.get("partial_columns").is_none(),
            "undeclared entries must not carry the key: {t}"
        );
    }

    // declared pipeline: the precheck fires first
    set_key_model(&app, doris_table, "unique").await;
    let pid = pipeline(&app, "partial", PARTIAL_SQL, kafka, doris).await;
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/deploy"), None).await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "UNAVAILABLE", "{b}");
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("SHOW ALTER TABLE COLUMN"),
        "{b}"
    );
    assert_eq!(
        st.plans.lock().unwrap().len(),
        1,
        "the engine must not receive the partial plan when the precheck cannot answer"
    );
    // the pipeline row is not marked DEPLOY_FAILED by a precheck refusal
    // (nothing was attempted against the engine)
    let (_, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(b["status"], "PUBLISHED", "{b}");

    // recover: same path, same gate
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/recover"),
        Some(json!({})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "UNAVAILABLE");
}

/// Push-side re-gate (R7 review): what gets pushed is the spec snapshot, and
/// a snapshot's declaration can be invalid without publish ever saying so —
/// ① a snapshot published before T-A7 (the parser then ignored the INSERT
/// column list = full-row writes; no gate ran) now carries a list on a table
/// with no key model on record; ② the key model is re-registered after a
/// valid publish. Both must be refused at deploy / recover with the publish
/// gate's 400 diagnostics — before the FE precheck, before the mock engine
/// sees a plan, pipeline status untouched — instead of silently flipping the
/// sink into partial mode (a DUPLICATE KEY table would then fail every
/// load). A backfill accept refuses the same way (422
/// PARTIAL_DECLARATION_INVALID, no job row). A record set back to `unique`
/// reopens the path: the precheck's 502 is the next stop.
#[tokio::test]
async fn push_regates_the_declaration_against_the_registered_sink() {
    let app = app().await;
    let (st, port) = spawn_mock_engine().await;
    // MockFe (MoW UNIQUE) so the valid publish below passes LOW-5's live
    // gate; its refusal of non-SHOW-CREATE queries keeps the final deploy
    // leg on the precheck's 502
    let fe = MockFe::default();
    let fe_port = fe.clone().serve(LIVE_MOW).await;
    let (kafka, doris, _, doris_table) = seed_datasources_at_fe(&app, fe_port).await;
    online_node(&app, port, "n1").await;

    // ① the pre-A7 snapshot: published bare (no list, no gate), then the
    // stored spec is what an old console left behind for a list-bearing
    // SQL — materialized form, column list in place, key model never asked
    let legacy = published_pipeline(&app, kafka, doris).await;
    let legacy_spec = format!(
        "INSERT INTO ${{ds:{doris}.ods.orders}} (id, payload) SELECT id, payload \
         FROM ${{ds:{kafka}.shop.orders}} WHERE op_type IN ('I','U')"
    );
    app.store
        .set_pipeline_published(legacy, &legacy_spec, PARTIAL_SQL, "legacy")
        .unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{legacy}/deploy"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["ok"], false);
    assert!(
        diag_text(&b).contains("未登记键模型"),
        "the list the gate never saw is gated at push: {}",
        diag_text(&b)
    );
    assert!(
        st.plans.lock().unwrap().is_empty(),
        "the engine never sees a plan the gate refused"
    );

    // ② a valid publish whose premise moves afterwards
    set_key_model(&app, doris_table, "unique").await;
    let pid = pipeline(&app, "partial", PARTIAL_SQL, kafka, doris).await;
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, _) = set_key_model(&app, doris_table, "duplicate").await;
    assert_eq!(s, StatusCode::OK);
    for action in ["deploy", "recover"] {
        let (s, b) = req(
            &app,
            "POST",
            &format!("/api/pipelines/{pid}/{action}"),
            Some(json!({})),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{action}: {b}");
        assert_eq!(b["ok"], false, "{action}: compile-shaped refusal");
        assert!(
            diag_text(&b).contains("keyModel=duplicate"),
            "{action}: {}",
            diag_text(&b)
        );
    }
    assert!(st.plans.lock().unwrap().is_empty(), "nothing pushed");
    let (_, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(
        b["status"], "PUBLISHED",
        "a gate refusal is not DEPLOY_FAILED: {b}"
    );

    // backfill accept: same gate, the envelope shape of the other capability
    // gates, refused before any job row (the source gate — kafka, 422 — would
    // otherwise be the answer; the sink's declaration is judged first)
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/backfill"),
        Some(json!({
            "statementIndex": 0,
            "from": {"type": "offset", "value": 0},
            "to": {"type": "offset", "value": 10}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "PARTIAL_DECLARATION_INVALID", "{b}");
    assert_eq!(
        b["error"]["message_key"],
        "err.backfill.partial_declaration"
    );
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("keyModel=duplicate"),
        "the publish diagnostic rides inside: {b}"
    );
    let (_, b) = req(
        &app,
        "GET",
        &format!("/api/pipelines/{pid}/backfill/jobs"),
        None,
    )
    .await;
    assert_eq!(b["jobs"].as_array().unwrap().len(), 0, "no job row");

    // the record back to unique → the gate reopens and the FE precheck is
    // the next stop (unreachable here: 502, as the test above pins)
    set_key_model(&app, doris_table, "unique").await;
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/deploy"), None).await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "UNAVAILABLE", "{b}");
}

/// Evolve side of the mutex: while a partial-update pipeline is deployed
/// (RUNNING or PAUSED) on the table, evolve refuses up front with 409
/// PARTIAL_UPDATE_ACTIVE naming it — before the live DESC, so the refusal
/// needs no Doris. Not deployed → the gate is silent and evolve proceeds
/// to the DESC (502 here, FE unreachable), listing the pipeline in the
/// `partialUpdatePipelines` checklist would be the 200's job.
#[tokio::test]
async fn evolve_refuses_while_a_partial_update_pipeline_is_deployed() {
    let app = app().await;
    let (_, port) = spawn_mock_engine().await;
    // MockFe (MoW UNIQUE) for the partial pipeline's publish (LOW-5); the
    // evolve legs below expect the DESC failure — the MockFe refuses it
    let fe = MockFe::default();
    let fe_port = fe.clone().serve(LIVE_MOW).await;
    let (kafka, doris, _, doris_table) = seed_datasources_at_fe(&app, fe_port).await;
    let (node_id, _, _) = online_node(&app, port, "n1").await;
    set_key_model(&app, doris_table, "unique").await;
    let pid = pipeline(&app, "partial", PARTIAL_SQL, kafka, doris).await;
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let evolve = format!("/api/datasources/{doris}/tables/ods/orders/ddl/evolve");

    // published, not deployed: no refusal — evolve reaches the DESC
    let (s, b) = req(&app, "POST", &evolve, None).await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(
        b["error"]["code"], "UNAVAILABLE",
        "the DESC, not the gate: {b}"
    );

    // deployed (the row a successful push leaves) → 409 naming the pipeline
    app.store.set_pipeline_deployed(pid, node_id).unwrap();
    let (s, b) = req(&app, "POST", &evolve, None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "PARTIAL_UPDATE_ACTIVE", "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.partial_update_active");
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&format!("#{pid} partial")),
        "{b}"
    );
    let refs = &b["error"]["refs"];
    assert_eq!(refs["pipelines"][0]["id"], pid, "{refs}");
    assert_eq!(refs["pipelines"][0]["deployed"], true);
    assert_eq!(refs["pipelines"][0]["status"], "RUNNING");
    assert_eq!(refs["pipelines"][0]["columns"], json!(["id", "payload"]));
    assert!(
        refs["partialUpdateNotice"]
            .as_str()
            .unwrap()
            .contains("PARTIAL_UPDATE_ACTIVE"),
        "the notice text names both directions: {refs}"
    );

    // PAUSED still holds the assignment → still refused
    app.store.set_pipeline_status(pid, "PAUSED", false).unwrap();
    let (s, b) = req(&app, "POST", &evolve, None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["refs"]["pipelines"][0]["status"], "PAUSED");

    // stopped (assignment cleared) → the gate lifts, evolve proceeds again
    app.store
        .set_pipeline_status(pid, "PUBLISHED", true)
        .unwrap();
    let (s, b) = req(&app, "POST", &evolve, None).await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "UNAVAILABLE");

    // an undeclared pipeline deployed on the same table never trips the gate
    let plain = published_pipeline(&app, kafka, doris).await;
    app.store.set_pipeline_deployed(plain, node_id).unwrap();
    let (s, b) = req(&app, "POST", &evolve, None).await;
    assert_eq!(
        s,
        StatusCode::BAD_GATEWAY,
        "full-row pipelines are not partial: {b}"
    );
}

// ---------------- the partial write path, through the backfill executor ----------------

/// One captured stream-load PUT: (request headers, body).
type LoadHit = (HashMap<String, String>, String);

/// Header-capturing mock stream-load BE: every PUT is recorded as (headers,
/// body) and answered Success.
#[derive(Clone, Default)]
struct CaptureBe {
    hits: Arc<Mutex<Vec<LoadHit>>>,
}

impl CaptureBe {
    async fn serve(self) -> u16 {
        let be = self.clone();
        let app = Router::new().route(
            "/api/{db}/{table}/_stream_load",
            put(move |headers: axum::http::HeaderMap, body: String| {
                let be = be.clone();
                async move {
                    let map: HashMap<String, String> = headers
                        .iter()
                        .map(|(k, v)| {
                            (k.as_str().to_string(), v.to_str().unwrap_or("").to_string())
                        })
                        .collect();
                    be.hits.lock().unwrap().push((map, body));
                    Json(json!({"Status": "Success", "Label": "x"}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        port
    }
}

/// The declaration reaches the wire: a file→doris statement with an INSERT
/// column list, replayed by the console-side backfill executor (driven
/// directly — the HTTP accept path is behind the FE precheck, which this
/// environment cannot pass), lands on the BE as a stream load with
/// `partial_columns: true` whose JSON rows carry exactly the declared keys.
/// That is `bounded_plan` projecting the list onto the sink entry and
/// `backfill_doris_sink` honouring it, the same two hops a deploy push and
/// the engine's sink factory make.
#[tokio::test]
async fn backfill_executor_writes_the_declared_subset_as_a_partial_load() {
    let app = app().await;
    let be = CaptureBe::default();
    let be_port = be.clone().serve().await;
    // MockFe (MoW UNIQUE): the publish below needs the LOW-5 live answer,
    // and the backfill accept's precheck (SHOW ALTER) keeps its 502
    let fe = MockFe::default();
    let fe_port = fe.clone().serve(LIVE_MOW).await;

    // file source: three columns in the file, two declared for the write
    let dir = std::env::temp_dir().join(format!(
        "tailrace-console-pu-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let text = "{\"order_no\":\"ORD-1\",\"qty\":7,\"note\":\"keep-me\"}\n\
                {\"order_no\":\"ORD-2\",\"qty\":9,\"note\":\"keep-me\"}\n";
    std::fs::write(dir.join("pu_src"), text).unwrap();
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({"name": "pu-files", "type": "file", "connJson": {"path": dir}})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let file_ds = b["id"].as_i64().unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{file_ds}/tables"),
        Some(json!({
            "dbName": "src", "tableName": "pu_src", "format": "json",
            "columnsJson": [["order_no", "utf8"], ["qty", "int64"], ["note", "utf8"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "pu-doris", "type": "doris",
            "connJson": {
                "jdbcUrl": format!("jdbc:mysql://127.0.0.1:{fe_port}/shop"),
                "streamLoadUrl": format!("http://127.0.0.1:{be_port}")
            },
            "username": "loader", "password": "pw"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let doris_ds = b["id"].as_i64().unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{doris_ds}/tables"),
        Some(json!({
            "dbName": "ods", "tableName": "orders",
            "columnsJson": [["order_no", "utf8"], ["qty", "int64"], ["note", "utf8"]],
            "keyModel": "unique"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": "pu-pipe",
            "sqlContent": "INSERT INTO ods.orders (order_no, qty) SELECT order_no, qty FROM src.pu_src",
            "dsMapping": {"src": file_ds, "ods": doris_ds},
            "checkpointMs": 10000
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pid = b["id"].as_i64().unwrap();
    let (s, b) = publish(&app, pid).await;
    assert_eq!(s, StatusCode::OK, "{b}");

    // the HTTP accept path is behind the FE precheck (502 here, no job row)
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/backfill"),
        Some(json!({
            "statementIndex": 0,
            "from": {"type": "offset", "value": 0},
            "to": {"type": "offset", "value": text.len()}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "UNAVAILABLE");
    let (_, b) = req(
        &app,
        "GET",
        &format!("/api/pipelines/{pid}/backfill/jobs"),
        None,
    )
    .await;
    assert_eq!(
        b["jobs"].as_array().unwrap().len(),
        0,
        "no job row behind a refused precheck"
    );
    // dryRun skips the precheck (zero side effects, nothing to protect)
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/backfill"),
        Some(json!({
            "statementIndex": 0, "dryRun": true,
            "from": {"type": "offset", "value": 0},
            "to": {"type": "offset", "value": text.len()}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");

    // drive the executor directly on a job row (what the accept path would
    // do once the precheck answered)
    let state = AppState::new_full(
        app.store.clone(),
        BootstrapAuthority::new("tok-secret"),
        app.artifact_dir.clone(),
        app.cipher.clone(),
    );
    let job = app
        .store
        .create_backfill_job(
            pid,
            0,
            r#"{"type":"offset","value":0}"#,
            &format!(r#"{{"type":"offset","value":{}}}"#, text.len()),
        )
        .unwrap();
    let (st, j, to) = (state, job.clone(), text.len() as i64);
    tokio::task::spawn_blocking(move || tailrace_console::backfill::run_job(st, j, 0, 0, to))
        .await
        .unwrap();
    let (_, b) = req(
        &app,
        "GET",
        &format!("/api/pipelines/{pid}/backfill/jobs"),
        None,
    )
    .await;
    let row = b["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["jobId"] == job)
        .cloned()
        .unwrap();
    assert_eq!(row["state"], "done", "{row}");
    assert_eq!(row["rowsWritten"], 2, "{row}");

    let hits = be.hits.lock().unwrap().clone();
    assert_eq!(hits.len(), 1, "one bounded flush");
    let (headers, body) = &hits[0];
    assert_eq!(
        headers.get("partial_columns").map(String::as_str),
        Some("true"),
        "the declared subset rides as a partial load: {headers:?}"
    );
    // the second header of the pair is NOT optional: without `columns`
    // naming the subset Doris 2.1.7 resolves a json partial load to every
    // table column and silently NULLs the unlisted ones (Status Success) —
    // live-probed in W4 (scripts/e2e/evidence/partial_update-probe-2026-10-28)
    assert_eq!(
        headers.get("columns").map(String::as_str),
        Some("order_no,qty"),
        "the subset must be named on the wire: {headers:?}"
    );
    let first: Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
    assert_eq!(first["order_no"], "ORD-1");
    assert_eq!(first["qty"], 7);
    assert!(
        first.get("note").is_none(),
        "the unlisted column is not written (Doris keeps its row value): {body}"
    );
    std::fs::remove_dir_all(&dir).ok();
}
