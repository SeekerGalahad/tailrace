//! Datasource resource lifecycle (§4.2): types registry, create → list/get →
//! CAS update → probes → table registration → guarded deletes.

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use common::*;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn datasource_types_registry_shape() {
    let app = app().await;
    // no session -> 401
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/datasource-types")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let (s, b) = req(&app, "GET", "/api/datasource-types", None).await;
    assert_eq!(s, StatusCode::OK);
    let types = b["types"].as_array().unwrap();
    assert_eq!(types.len(), 3, "{b}");
    assert_eq!(types[0]["type"], "kafka");
    assert_eq!(types[0]["fields"][0]["name"], "bootstrap");
    assert_eq!(types[0]["fields"][0]["required"], true);
    assert_eq!(types[1]["type"], "doris");
    assert_eq!(types[1]["credentialLabel"], "数据库账号");
    assert_eq!(types[1]["fields"].as_array().unwrap().len(), 3);
    assert_eq!(types[2]["type"], "file");
}

#[tokio::test]
async fn datasource_crud_probe_and_tables() {
    let app = app_with_cipher(console_store::CredentialCipher::ephemeral()).await;

    // --- create: unknown type 400, missing connJson keys 400, dup name 409
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({"name": "x", "type": "mysql", "connJson": {}})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["code"], "INVALID_ARGUMENT");
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({"name": "x", "type": "kafka", "connJson": {}})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("bootstrap"),
        "{b}"
    );

    // --- create ok (credentials appear exactly once)
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "edge-kafka", "type": "kafka",
            "connJson": {"bootstrap": "127.0.0.1:19092,127.0.0.1:19093"}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let id = b["id"].as_i64().unwrap();
    assert_eq!(b["generation"], 1);

    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({"name": "edge-kafka", "type": "kafka", "connJson": {"bootstrap": "h:1"}})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");

    // --- list: credentials never returned, only hasPassword
    let (s, b) = req(&app, "GET", "/api/datasources", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["total"], 1);
    let item = &b["items"][0];
    assert_eq!(item["id"], id);
    assert_eq!(item["type"], "kafka");
    assert_eq!(item["status"], "CREATED");
    assert_eq!(item["hasPassword"], false);
    assert_eq!(
        item["connJson"]["bootstrap"],
        "127.0.0.1:19092,127.0.0.1:19093"
    );
    assert!(item["updatedAt"].as_str().unwrap().ends_with('Z'), "{item}");
    assert!(item.get("password").is_none() && item.get("username").is_none());

    // type filter
    let (s, b) = req(&app, "GET", "/api/datasources?type=doris", None).await;
    assert_eq!((s, b["total"].as_i64().unwrap()), (StatusCode::OK, 0));

    // --- detail (with a credential stored)
    let doris_dir = std::env::temp_dir().join("tailrace-ds-probe-dir");
    std::fs::create_dir_all(&doris_dir).unwrap();
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "shop-doris", "type": "doris",
            "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:19030/shop", "streamLoadUrl": "http://127.0.0.1:18040"},
            "username": "loader", "password": "secret"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let doris_id = b["id"].as_i64().unwrap();
    let (s, b) = req(&app, "GET", &format!("/api/datasources/{doris_id}"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["hasPassword"], true);
    assert_eq!(b["tableCount"], 0);

    // §5.3 at-rest: the stored halves are AES-256-GCM sealed (12B nonce b64
    // + ciphertext b64), never plaintext, and reopen with the app's key.
    {
        let cred = app
            .store
            .credential(doris_id)
            .unwrap()
            .expect("credential row");
        assert_ne!(
            cred.nonce, "pending-aes-gcm",
            "legacy plaintext marker must be gone"
        );
        assert!(
            !cred.ciphertext.contains("secret"),
            "ciphertext leaks plaintext"
        );
        assert!(
            !cred.ciphertext.contains("loader"),
            "ciphertext leaks username"
        );
        let plain = app
            .cipher
            .open(&cred.nonce, &cred.ciphertext)
            .expect("reopen sealed credential");
        assert!(
            plain.contains("loader") && plain.contains("secret"),
            "{plain}"
        );
    }

    // --- update: wrong generation -> GENERATION_CONFLICT; right one bumps
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/datasources/{doris_id}"),
        Some(json!({
            "name": "shop-doris", "type": "doris",
            "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:19031/shop", "streamLoadUrl": "http://127.0.0.1:18041"},
            "username": "loader", "password": "secret", "generation": 42
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "GENERATION_CONFLICT");
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/datasources/{doris_id}"),
        Some(json!({
            "name": "shop-doris", "type": "doris",
            "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:19031/shop", "streamLoadUrl": "http://127.0.0.1:18041"},
            "generation": 1
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["generation"], 2);
    // blank password kept the stored credential
    let (s, b) = req(&app, "GET", &format!("/api/datasources/{doris_id}"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["hasPassword"], true);

    // --- probes: unsaved (file type, real dir vs missing path)
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources/test",
        Some(json!({"type": "file", "connJson": {"path": doris_dir.to_string_lossy()}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["status"], "CONN_OK");
    assert!(b["latencyMs"].as_i64().unwrap() >= 0);
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources/test",
        Some(json!({"type": "file", "connJson": {"path": "/nonexistent-tailrace-probe"}})),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::OK,
        "probe outcome rides a 200 even on failure: {b}"
    );
    assert_eq!(b["status"], "CONN_FAIL");

    // saved probe writes status back
    let (s, b) = req(&app, "POST", &format!("/api/datasources/{id}/test"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["status"], "CONN_FAIL",
        "no broker on 19092/19093 in test env"
    );
    let (_s, b) = req(&app, "GET", &format!("/api/datasources/{id}"), None).await;
    assert_eq!(b["status"], "CONN_FAIL");

    // --- tables: register, list, duplicate 409, IN_USE guards
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{id}/tables"),
        Some(
            json!({"dbName": "shop", "tableName": "orders", "format": "ogg",
                    "columnsJson": [["id", "int64"], ["payload", "utf8"]]}),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let tid = b["id"].as_i64().unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{id}/tables"),
        Some(json!({"dbName": "shop", "tableName": "orders", "format": "ogg"})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");

    let (s, b) = req(
        &app,
        "GET",
        "/api/datasources/tables?datasourceId=&kind=",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["total"], 1);
    let t = &b["items"][0];
    assert_eq!(t["datasourceId"], id);
    assert_eq!(t["kind"], "kafka");
    assert_eq!(t["cursorSemantics"], "KAFKA_OFFSET");
    assert_eq!(t["source"], "manual");
    let (_s, b) = req(
        &app,
        "GET",
        &format!("/api/datasources/tables?datasourceId={id}&kind=doris"),
        None,
    )
    .await;
    assert_eq!(b["total"], 0);

    // datasource delete blocked while tables exist (IN_USE refs)
    let (s, b) = req(&app, "DELETE", &format!("/api/datasources/{id}"), None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "IN_USE");

    // table delete frees it; datasource delete then succeeds
    let (s, b) = req(
        &app,
        "DELETE",
        &format!("/api/datasources/tables/{tid}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT, "{b}");
    let (s, b) = req(&app, "DELETE", &format!("/api/datasources/{id}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT, "{b}");
    let (s, _b) = req(&app, "GET", &format!("/api/datasources/{id}"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // discover / infer-schema: honest 501 markers
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{doris_id}/discover"),
        Some(json!({"sampleRows": 20})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_IMPLEMENTED, "{b}");
    assert_eq!(b["error"]["code"], "NOT_IMPLEMENTED");
    let (s, _) = req(
        &app,
        "POST",
        &format!("/api/datasources/{doris_id}/infer-schema"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn datasource_delete_blocked_by_pipeline_mapping() {
    let app = app().await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    // pipeline maps shop -> kafka: datasource delete must 409 with refs.pipelines
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({"name": "p1", "sqlContent": "SELECT 1", "dsMapping": {"shop": kafka, "ods": doris}})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pid = b["id"].as_i64().unwrap();
    let (s, b) = req(&app, "DELETE", &format!("/api/datasources/{kafka}"), None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "IN_USE");
    assert_eq!(
        b["error"]["refs"]["pipelines"][0], pid,
        "refs.pipelines per §4.2: {b}"
    );
    // release and retry
    let (s, _) = req(&app, "DELETE", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    // tables still block
    let (s, b) = req(&app, "DELETE", &format!("/api/datasources/{kafka}"), None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
}

#[tokio::test]
async fn sse_requires_session_and_rejects_unknown_api() {
    // sanity for the auth boundary on the new surface (one representative path)
    let app = app().await;
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/pipelines/1/logs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let _ = to_bytes(resp.into_body(), usize::MAX).await;
}
