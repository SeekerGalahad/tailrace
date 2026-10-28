//! T-A1 create-table closed loop (blueprint §7.2): draft annotations, the
//! key-model 422 gate in front of execute, the audit trail (full DDL text +
//! outcome, success or refusal alike), and evolve's guard rails. The happy
//! paths that need a live Doris (CREATE TABLE landing, ADD COLUMN surviving
//! a pipeline restart) are the W6 testcontainers e2e suite's job; what is
//! pinned here is the console contract — everything the console decides
//! before (and records after) touching the FE.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::*;
use console_store::Store;
use serde_json::{json, Value};
use tower::ServiceExt;

/// Doris datasource pointing at a port nothing listens on (loopback →
/// instant refusal, no 60s EXEC_TIMEOUT wait) plus its registered table
/// object; returns (app, datasource id, table object id).
async fn ddl_app() -> (TestApp, i64, i64) {
    let app = app().await;
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "shop-doris", "type": "doris",
            "connJson": {
                "jdbcUrl": "jdbc:mysql://127.0.0.1:1/shop",
                "streamLoadUrl": "http://127.0.0.1:1"
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let ds = b["id"].as_i64().unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables"),
        Some(json!({
            "dbName": "shop", "tableName": "orders", "format": null,
            // op_type → UNIQUE recommendation; order_id → *_id key guess;
            // event_ts → time-shaped but utf8 (partition candidate skipped,
            // note emitted); seq → declared sequence column
            "columnsJson": [
                ["op_type", "utf8"], ["order_id", "utf8"], ["amount", "float64"],
                ["event_ts", "utf8"], ["seq", "int64"]
            ],
            "sequenceColumn": "seq"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    (app, ds, b["id"].as_i64().unwrap())
}

/// A kafka datasource with a `shop.orders` table object (columns registered)
/// — the wrong-kind target for every DDL endpoint; returns its id.
async fn kafka_with_table(app: &TestApp) -> i64 {
    let (s, b) = req(
        app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "edge-kafka", "type": "kafka",
            "connJson": {"bootstrap": "127.0.0.1:19092"}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let kafka = b["id"].as_i64().unwrap();
    let (s, b) = req(
        app,
        "POST",
        &format!("/api/datasources/{kafka}/tables"),
        Some(json!({
            "dbName": "shop", "tableName": "orders", "format": "ogg",
            "columnsJson": [["id", "int64"], ["payload", "utf8"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    kafka
}

/// Audit rows for one action: (actor, entity_id, parsed detail) in
/// insertion order (same shape as the mcp_server suite's helper).
fn audit_rows(store: &Store, action: &str) -> Vec<(String, Option<String>, Value)> {
    store
        .with_conn(|c| {
            let mut stmt = c
                .prepare("SELECT actor, entity_id, detail_json FROM audit_log WHERE action = ?1 ORDER BY id")
                .unwrap();
            let rows = stmt
                .query_map([action], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                })
                .unwrap();
            let mut out = Vec::new();
            for row in rows {
                let (actor, entity, detail) = row.unwrap();
                let detail = detail
                    .and_then(|d| serde_json::from_str(&d).ok())
                    .unwrap_or(Value::Null);
                out.push((actor, entity, detail));
            }
            Ok(out)
        })
        .unwrap()
}

#[tokio::test]
async fn ddl_draft_annotates_every_guess() {
    let (app, ds, _tid) = ddl_app().await;

    // session-gated like the rest of /api
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/datasources/{ds}/tables/shop/orders/ddl/draft"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // first round: no body at all — pure heuristics
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/orders/ddl/draft"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    // CDC evidence (op_type) recommends UNIQUE; *_id heuristic picks the key
    assert_eq!(b["keyModel"], "unique");
    assert_eq!(b["keyColumn"], "order_id");
    assert!(
        b["keyColumnGuessed"].as_bool().unwrap(),
        "heuristic key carries the 「猜测」 badge: {b}"
    );
    assert!(b["keyModelReason"].as_str().unwrap().contains("CDC"));
    // event_ts is time-shaped but utf8 → VARCHAR, which Doris refuses as a
    // RANGE partition key: no candidate, and the draft says why in notes
    // (annotated, never silent) instead of emitting an unexecutable clause
    assert!(b["partition"].is_null(), "{b}");
    assert!(
        b["notes"].as_array().unwrap().iter().any(|n| n
            .as_str()
            .is_some_and(|s| s.contains("event_ts") && s.contains("RANGE"))),
        "skip reason surfaces as a note: {b}"
    );
    assert_eq!(b["sequenceColumn"], "seq");
    assert_eq!(b["columns"].as_array().unwrap().len(), 5);
    let ddl = b["ddl"].as_str().unwrap();
    for fragment in [
        "CREATE TABLE IF NOT EXISTS `shop`.`orders`",
        "UNIQUE KEY(`order_id`)",
        "DISTRIBUTED BY HASH(`order_id`) BUCKETS AUTO",
        "\"enable_unique_key_merge_on_write\" = \"true\"",
    ] {
        assert!(ddl.contains(fragment), "draft missing {fragment}:\n{ddl}");
    }
    assert!(
        !ddl.contains("PARTITION BY"),
        "no unexecutable partition clause:\n{ddl}"
    );
    // read-only step: no audit row, nothing executed
    assert!(audit_rows(&app.store, "TABLE_DDL_EXECUTE").is_empty());
    assert!(audit_rows(&app.store, "TABLE_DDL_EVOLVE").is_empty());

    // round two: the human confirmed the key column → badge clears, text
    // regenerates without touching any backend
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/orders/ddl/draft"),
        Some(json!({"keyColumn": "order_id", "keyModel": "duplicate"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["keyModel"], "duplicate");
    assert!(!b["keyColumnGuessed"].as_bool().unwrap());
    // DUPLICATE override keeps the sequence column as a plain column, with
    // the note saying so (§7.2 ②)
    let notes = b["notes"].as_array().unwrap();
    assert!(
        notes
            .iter()
            .any(|n| n.as_str().is_some_and(|s| s.contains("seq"))),
        "{b}"
    );
    assert!(!b["ddl"].as_str().unwrap().contains("merge_on_write"));
}

/// The partition candidate over HTTP: offered (guessed) when executable,
/// dropped by `partition:false`, confirmed by `partition:true`, and skipped
/// with a note when the confirmed key model makes it unexecutable (MoW
/// partition column must be the key column).
#[tokio::test]
async fn ddl_draft_partition_candidate_confirm_and_drop() {
    let (app, ds, _tid) = ddl_app().await;
    // int64 `dt` IS a RANGE-able time column; event_id → *_id key guess
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables"),
        Some(json!({
            "dbName": "shop", "tableName": "events", "format": null,
            "columnsJson": [["event_id", "int64"], ["dt", "int64"], ["v", "float64"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let path = format!("/api/datasources/{ds}/tables/shop/events/ddl/draft");

    // round one: DUPLICATE recommended (no CDC evidence) → candidate offered
    // as a guess, clause + dynamic_partition properties in the text, caveat
    // about yyyyMMdd integer bounds in notes
    let (s, b) = req(&app, "POST", &path, None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["keyModel"], "duplicate");
    assert_eq!(b["partition"]["column"], "dt");
    assert!(b["partition"]["guessed"].as_bool().unwrap(), "{b}");
    let ddl = b["ddl"].as_str().unwrap();
    assert!(ddl.contains("PARTITION BY RANGE(`dt`) ()"), "{ddl}");
    assert!(
        ddl.contains("\"dynamic_partition.enable\" = \"true\""),
        "{ddl}"
    );
    assert!(
        b["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n.as_str().is_some_and(|s| s.contains("yyyyMMdd"))),
        "{b}"
    );

    // the human drops it → no clause, no properties, no partition in the
    // response (the wizard's 取消 control)
    let (s, b) = req(
        &app,
        "POST",
        &path,
        Some(json!({"keyModel": "duplicate", "keyColumn": "event_id", "partition": false})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(b["partition"].is_null(), "{b}");
    let ddl = b["ddl"].as_str().unwrap();
    assert!(!ddl.contains("PARTITION BY"), "{ddl}");
    assert!(!ddl.contains("dynamic_partition"), "{ddl}");

    // the human confirms it → same clause, guess flag cleared
    let (s, b) = req(
        &app,
        "POST",
        &path,
        Some(json!({"keyModel": "duplicate", "keyColumn": "event_id", "partition": true})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["partition"]["column"], "dt");
    assert!(!b["partition"]["guessed"].as_bool().unwrap(), "{b}");
    assert!(b["ddl"]
        .as_str()
        .unwrap()
        .contains("PARTITION BY RANGE(`dt`) ()"));

    // UNIQUE override with event_id as the key: a merge-on-write table's
    // partition column must be a KEY column, so the candidate is skipped —
    // even with partition:true — and the note says why
    let (s, b) = req(
        &app,
        "POST",
        &path,
        Some(json!({"keyModel": "unique", "keyColumn": "event_id", "partition": true})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["keyModel"], "unique");
    assert!(b["partition"].is_null(), "{b}");
    assert!(!b["ddl"].as_str().unwrap().contains("PARTITION BY"));
    assert!(
        b["notes"].as_array().unwrap().iter().any(|n| n
            .as_str()
            .is_some_and(|s| s.contains("`dt`") && s.contains("必须是键列"))),
        "{b}"
    );
    // nothing above touched a backend
    assert!(audit_rows(&app.store, "TABLE_DDL_EXECUTE").is_empty());
}

#[tokio::test]
async fn ddl_draft_guards() {
    let (app, ds, _tid) = ddl_app().await;

    // kafka datasource: the draft is Doris-dialect DDL, a kafka row cannot be
    // its target even with columns registered
    let kafka = kafka_with_table(&app).await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{kafka}/tables/shop/orders/ddl/draft"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.datasource_kind");

    // unknown key-model hint
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/orders/ddl/draft"),
        Some(json!({"keyModel": "aggregate"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.key_model_unknown");

    // unregistered db.table
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/ghost/ddl/draft"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.table_unregistered");

    // registered without columns — nothing to draft from
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables"),
        Some(json!({"dbName": "shop", "tableName": "bare"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/bare/ddl/draft"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.columns_missing");
}

#[tokio::test]
async fn ddl_execute_rejects_unconfirmed_key_model() {
    let (app, ds, _tid) = ddl_app().await;
    let path = format!("/api/datasources/{ds}/tables/shop/orders/ddl/execute");

    // §7.2 gate: no key model → 422, explicitly, before anything else
    let (s, b) = req(
        &app,
        "POST",
        &path,
        Some(json!({"ddl": "CREATE TABLE `shop`.`orders` (…) DUPLICATE KEY(`order_id`)"})),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "KEY_MODEL_REQUIRED");
    assert_eq!(b["error"]["message_key"], "err.ddl.key_model_required");

    // unknown value → same gate
    let (s, b) = req(
        &app,
        "POST",
        &path,
        Some(json!({"ddl": "…", "keyModel": "aggregate"})),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "KEY_MODEL_REQUIRED");

    // ddl itself missing → the other half of the confirmation
    let (s, b) = req(&app, "POST", &path, Some(json!({"keyModel": "unique"}))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "DDL_REQUIRED");

    // confirmed model contradicts the echoed DDL → 422 (a stale draft would
    // otherwise audit a lie)
    let (s, b) = req(
        &app,
        "POST",
        &path,
        Some(json!({
            "ddl": "CREATE TABLE `shop`.`orders` (…) DUPLICATE KEY(`order_id`)",
            "keyModel": "unique"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "KEY_MODEL_MISMATCH");

    // a text carrying BOTH key-clause keywords (the other one smuggled in a
    // comment) is not a draft this console produced — refused even though the
    // confirmed keyword is present somewhere in it
    let (s, b) = req(
        &app,
        "POST",
        &path,
        Some(json!({
            "ddl": "-- UNIQUE KEY considered\nCREATE TABLE `shop`.`orders` (…) DUPLICATE KEY(`order_id`)",
            "keyModel": "duplicate"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "KEY_MODEL_MISMATCH");

    // rejections happen before any connection: no audit rows either
    assert!(
        audit_rows(&app.store, "TABLE_DDL_EXECUTE").is_empty(),
        "422 gate must not leave audit evidence of an execution"
    );
}

#[tokio::test]
async fn ddl_execute_failure_is_verbatim_and_audited() {
    let (app, ds, tid) = ddl_app().await;
    let ddl = "CREATE TABLE IF NOT EXISTS `shop`.`orders` (\n    `order_id` VARCHAR(65533)\n)\nUNIQUE KEY(`order_id`)\nDISTRIBUTED BY HASH(`order_id`) BUCKETS AUTO;";
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/orders/ddl/execute"),
        Some(json!({"ddl": ddl, "keyModel": "unique"})),
    )
    .await;
    // FE unreachable → 502 carrying the backend error verbatim
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "DDL_FAILED");
    assert!(
        !b["error"]["message"].as_str().unwrap().is_empty(),
        "the Doris/driver message rides the response: {b}"
    );

    // the attempt is audit evidence: full DDL text, the refusal, the target
    let rows = audit_rows(&app.store, "TABLE_DDL_EXECUTE");
    assert_eq!(rows.len(), 1, "{rows:?}");
    let (actor, entity, detail) = &rows[0];
    assert_eq!(actor, "admin");
    assert_eq!(entity.as_deref(), Some(tid.to_string().as_str()));
    assert_eq!(detail["dbName"], "shop");
    assert_eq!(detail["tableName"], "orders");
    assert_eq!(detail["keyModel"], "unique");
    assert_eq!(detail["ok"], false);
    assert!(
        detail["error"].as_str().is_some_and(|e| !e.is_empty()),
        "refusal message audited: {detail}"
    );
    // the audit row carries the DDL verbatim — byte for byte what was sent
    assert_eq!(detail["ddl"].as_str(), Some(ddl));
}

#[tokio::test]
async fn ddl_evolve_guards() {
    let (app, ds, _tid) = ddl_app().await;

    // kafka datasource → 400 (same Doris-dialect target rule as draft)
    let kafka = kafka_with_table(&app).await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{kafka}/tables/shop/orders/ddl/evolve"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.datasource_kind");

    // no registered columns → 400 before any DESC
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables"),
        Some(json!({"dbName": "shop", "tableName": "bare"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/bare/ddl/evolve"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.columns_missing");

    // a registered column outside the vocabulary is refused, not folded
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables"),
        Some(json!({
            "dbName": "shop", "tableName": "odd",
            "columnsJson": [["id", "int64"], ["blob", "binary"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/odd/ddl/evolve"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.ddl.column_unknown_type");

    // FE unreachable → DESC fails → 502, nothing executed, nothing audited
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{ds}/tables/shop/orders/ddl/evolve"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "UNAVAILABLE");
    assert!(audit_rows(&app.store, "TABLE_DDL_EVOLVE").is_empty());
}
