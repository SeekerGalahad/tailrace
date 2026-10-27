//! T-D1 dim-join pipeline through the management plane (the E4/D1
//! integration gap regression): register a kind=dim table → create a
//! pipeline whose SQL joins `${ds:dim.<dsId>.<table>}` → publish (gate:
//! dim refs must resolve to kind=dim WITH a key column) → deploy against
//! the mock engine and verify the plan carries the dim table's metadata
//! (kind=dim + bootstrap from jdbcUrl + sequence_column) — the entries the
//! engine preflight counts and load_dim refuses to run without.

mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::json;

/// Register a mysql datasource + a kind=dim table with key column `id`;
/// returns (mysql_ds_id, dim_table_id).
async fn seed_dim_datasource(app: &TestApp) -> (i64, i64) {
    let (s, b) = req(
        app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "shop-mysql", "type": "mysql",
            "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:13306/shop"}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let mysql = b["id"].as_i64().unwrap();
    let (s, b) = req(
        app,
        "POST",
        &format!("/api/datasources/{mysql}/tables"),
        Some(json!({
            "dbName": "shop", "tableName": "dim_user", "kind": "dim",
            "sequenceColumn": "id",
            "columnsJson": [["id", "int64"], ["name", "utf8"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let dim_table = b["id"].as_i64().unwrap();
    (mysql, dim_table)
}

/// Register the widened sink table (`ods.orders_wide`) on the doris
/// datasource seeded by [`seed_datasources`].
async fn register_wide_sink(app: &TestApp, doris: i64) {
    let (s, b) = req(
        app,
        "POST",
        &format!("/api/datasources/{doris}/tables"),
        Some(json!({
            "dbName": "ods", "tableName": "orders_wide",
            "columnsJson": [["id", "int64"], ["name", "utf8"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
}

/// The list endpoint surfaces the dim registration (kind + sequenceColumn).
#[tokio::test]
async fn dim_table_registration_surfaces_sequence_column() {
    let app = app().await;
    let (mysql, _dim) = seed_dim_datasource(&app).await;
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/datasources/tables?datasourceId={mysql}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let row = &b["items"][0];
    assert_eq!(row["kind"], "dim");
    assert_eq!(row["sequenceColumn"], "id");

    // kind=dim without a key column → 400 (the broadcast-load pagination key)
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{mysql}/tables"),
        Some(
            json!({"dbName": "shop", "tableName": "dim_nokey", "kind": "dim",
                    "columnsJson": [["id", "int64"]]}),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("sequenceColumn"),
        "{b}"
    );
    // dim on a non-JDBC datasource → 400
    let (_s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({"name": "k", "type": "kafka", "connJson": {"bootstrap": "127.0.0.1:19092"}})),
    )
    .await;
    let kafka = b["id"].as_i64().unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{kafka}/tables"),
        Some(json!({"dbName": "shop", "tableName": "dim_x", "kind": "dim"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mysql/postgresql"),
        "{b}"
    );
}

/// The full management-plane path: compile reports dims → publish gates the
/// dim references → deploy pushes a plan whose tables[] carries the dim
/// entry (kind=dim, engine-dialect bootstrap, sequence_column) — without
/// which the engine preflight rejects the whole plan ("plan table object
/// metadata missing").
#[tokio::test]
async fn dim_join_pipeline_publishes_and_deploys() {
    let app = app().await;
    let (st, port) = spawn_mock_engine().await;
    let (_node_id, _key, _token) = online_node(&app, port, "edge-dim").await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let (mysql, dim_table) = seed_dim_datasource(&app).await;
    register_wide_sink(&app, doris).await;

    let sql = format!(
        "INSERT INTO ods.orders_wide SELECT s.id, d.name FROM shop.orders s \
         LEFT JOIN ${{ds:dim.{mysql}.dim_user}} d ON s.id = d.id"
    );

    // ---------- compile: the dim join rides StatementOut.dims ----------
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines/compile",
        Some(json!({
            "sql": sql,
            "dsMapping": {"shop": kafka, "ods": doris},
            "pipelineId": 7
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["ok"], true);
    let stmt = &b["statements"][0];
    assert_eq!(
        stmt["dims"],
        json!([{ "reference": format!("{mysql}.dim_user"), "name": "dim_d" }]),
        "dim join extracts to dims with the dsId.table reference: {stmt}"
    );
    assert!(
        stmt["selectSql"]
            .as_str()
            .unwrap()
            .contains("LEFT JOIN dim_d d"),
        "{stmt}"
    );

    // ---------- publish gate: unregistered dim reference refuses ----------
    let broken = format!(
        "INSERT INTO ods.orders_wide SELECT s.id, d.name FROM shop.orders s \
         LEFT JOIN ${{ds:dim.{mysql}.no_such_dim}} d ON s.id = d.id"
    );
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": "dim-join", "sqlContent": broken,
            "dsMapping": {"shop": kafka, "ods": doris}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pid = b["id"].as_i64().unwrap();
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "unregistered dim ref must gate: {b}"
    );
    assert!(
        b["diagnostics"][0]["message"]
            .as_str()
            .unwrap()
            .contains(&format!("{mysql}.no_such_dim")),
        "{b}"
    );

    // a registered-but-not-dim object in the dim position → kind diagnostic
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/pipelines/{pid}"),
        Some(json!({
            "name": "dim-join",
            "sqlContent": format!(
                "INSERT INTO ods.orders_wide SELECT s.id, d.name FROM shop.orders s \
                 LEFT JOIN ${{ds:dim.{kafka}.orders}} d ON s.id = d.id"
            ),
            "dsMapping": {"shop": kafka, "ods": doris}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["diagnostics"][0]["message"]
            .as_str()
            .unwrap()
            .contains("kind=kafka"),
        "wrong-kind dim object names its kind: {b}"
    );

    // ---------- happy path: publish + deploy ----------
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/pipelines/{pid}"),
        Some(json!({
            "name": "dim-join", "sqlContent": sql,
            "dsMapping": {"shop": kafka, "ods": doris}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(s, StatusCode::OK, "publish with a proper dim ref: {b}");
    // the published spec guards the dim table too (§4.2 delete guard, dim needle)
    let (s, b) = req(
        &app,
        "DELETE",
        &format!("/api/datasources/tables/{dim_table}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "IN_USE");

    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": null})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["ok"], true);

    let plans = st.plans.lock().unwrap().clone();
    let plan = plans.last().expect("one plan pushed");
    let tables = plan["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 3, "source + sink + the dim entry: {tables:?}");
    let dim = tables
        .iter()
        .find(|t| t["reference"] == format!("{mysql}.dim_user"))
        .expect("dim table entry rides the plan");
    assert_eq!(dim["kind"], "dim");
    assert_eq!(
        dim["bootstrap"], "mysql://127.0.0.1:13306/shop",
        "engine JDBC dialect URL (jdbc: prefix stripped from connJson): {dim}"
    );
    assert_eq!(
        dim["sequence_column"], "id",
        "the key column rides the plan"
    );
    assert_eq!(dim["columns"][0], json!(["id", "int64"]));
    assert!(
        plan["sqlContent"]
            .as_str()
            .unwrap()
            .contains(&format!("dim.{mysql}.dim_user")),
        "the engine recompiles the same dim reference: {plan}"
    );
}

/// Gate branch the registration API cannot produce anymore (it requires
/// sequenceColumn for kind=dim): a legacy/store-level dim row without a key
/// column still refuses at publish with the sequenceColumn diagnostic.
#[tokio::test]
async fn dim_without_sequence_column_refuses_at_publish() {
    let app = app().await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    register_wide_sink(&app, doris).await;
    let mysql = {
        let (s, b) = req(
            &app,
            "POST",
            "/api/datasources",
            Some(json!({
                "name": "shop-mysql", "type": "mysql",
                "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:13306/shop"}
            })),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{b}");
        b["id"].as_i64().unwrap()
    };
    // direct store insert: kind=dim, no sequence_column (pre-migration shape)
    app.store
        .insert_table_object(
            mysql,
            "shop",
            "dim_legacy",
            "dim",
            None,
            Some(r#"[["id","int64"],["name","utf8"]]"#),
            None,
        )
        .unwrap();
    let sql = format!(
        "INSERT INTO ods.orders_wide SELECT s.id, d.name FROM shop.orders s \
         LEFT JOIN ${{ds:dim.{mysql}.dim_legacy}} d ON s.id = d.id"
    );
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": "dim-legacy", "sqlContent": sql,
            "dsMapping": {"shop": kafka, "ods": doris}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pid = b["id"].as_i64().unwrap();
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["diagnostics"][0]["message"]
            .as_str()
            .unwrap()
            .contains("sequenceColumn"),
        "{b}"
    );
}
