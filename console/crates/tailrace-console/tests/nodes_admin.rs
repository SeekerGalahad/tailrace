//! Node admin surface lifecycle (§4.3): manual registration behind the
//! healthz probe → list/detail → pipelines-runtime proxy → events/metrics →
//! revoke → guarded delete; §4.4 deploy/action real handlers (migrate 501).

mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::json;

#[tokio::test]
async fn manual_register_probe_gate() {
    let app = app().await;
    // nothing listens on this port -> probe fails -> 502 (§4.3)
    let (s, b) = req(
        &app,
        "POST",
        "/api/nodes/manual",
        Some(json!({"name": "dead-node", "host": "127.0.0.1", "enginePort": 1})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "ENGINE_UNREACHABLE");
    // nothing was registered
    let (_s, b) = req(&app, "GET", "/api/nodes", None).await;
    assert_eq!(b["summary"]["total"], 0);
}

#[tokio::test]
async fn node_admin_lifecycle() {
    let app = app().await;
    let (_st, port) = spawn_mock_engine().await;
    let (id, node_key, _token) = online_node(&app, port, "edge-01").await;

    // --- list: ONLINE + summary + capacity from heartbeat capacityJson
    let (s, b) = req(&app, "GET", "/api/nodes", None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["summary"]["total"], 1);
    assert_eq!(b["summary"]["online"], 1);
    let n = &b["nodes"][0];
    assert_eq!(n["id"], id);
    assert_eq!(n["nodeKey"], node_key.as_str());
    assert_eq!(n["name"], "edge-01");
    assert_eq!(n["status"], "ONLINE");
    assert_eq!(n["engineVersion"], "1.0.0");
    assert_eq!(n["apiUrl"], format!("http://127.0.0.1:{port}"));
    assert_eq!(n["capacity"]["pipelines"], 8);
    assert_eq!(n["capacity"]["memoryMb"], 2048);
    assert_eq!(n["errors24h"], 0);
    assert!(n["lastHeartbeat"].as_str().unwrap().ends_with('Z'));

    // --- detail: + host/ssh/enginePort/installDir/registerMode
    let (s, b) = req(&app, "GET", &format!("/api/nodes/{id}"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["host"], "127.0.0.1");
    assert_eq!(b["enginePort"], port);
    assert_eq!(b["registerMode"], "manual");
    assert_eq!(b["installDir"], "/opt/tailrace/engine");
    let (s, _) = req(&app, "GET", "/api/nodes/999", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // --- pipelines-runtime proxy: engine body passed through verbatim
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/nodes/{id}/pipelines-runtime"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["version"], "0.1.0-mock",
        "engine /v1/pipelines passthrough"
    );
    // engine gone -> 502
    let (s2, b2) = req(&app, "GET", "/api/nodes/999/pipelines-runtime", None).await;
    assert_eq!(s2, StatusCode::NOT_FOUND, "{b2}");

    // --- events: empty, then a heartbeat carrying events populates it
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/nodes/{id}/events?hours=168"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["items"].as_array().unwrap().len(), 0);
    // all-events endpoint with level filter
    let (s, b) = req(&app, "GET", "/api/nodes/events?hours=24&level=ERROR", None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["items"].as_array().unwrap().len(), 0);

    // --- metrics: 1-minute buckets (empty window is a valid empty series)
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/nodes/{id}/metrics?hours=6"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(b["points"].as_array().unwrap().is_empty(), "{b}");

    // --- deploy-log: empty until the SSH chain writes entries
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/nodes/{id}/deploy-log?limit=100"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["items"].as_array().unwrap().len(), 0);

    // --- §4.4 SSH chain endpoints: deploy/action are real (T35), migrate stays 501
    // deploy: unknown artifact -> 404 before any node is created
    let (s, b) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"hosts": [{"host": "10.1.1.6", "name": "edge-batch", "sshUser": "root"}], "artifactId": 999})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{b}");
    // action on a manual node (no SSH identity) -> 400
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/nodes/{id}/action"),
        Some(json!({"action": "stop"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["error"]["message"].as_str().unwrap().contains("SSH 托管"),
        "{b}"
    );
    // migrate remains the honest P2 501
    let (s, _) = req(
        &app,
        "POST",
        &format!("/api/nodes/{id}/migrate"),
        Some(json!({"toNodeId": 3})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_IMPLEMENTED);

    // --- revoke: 204; heartbeats with the old token now fail 401
    let (s, _) = req(&app, "POST", &format!("/api/nodes/{id}/revoke"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    // --- delete: manual node -> skip-with-warning branch (remote not SSH-managed) + cascade
    let (s, b) = req(&app, "DELETE", &format!("/api/nodes/{id}"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["remoteCleaned"], false);
    assert!(b["warning"].is_string());
    let (_s, b) = req(&app, "GET", "/api/nodes", None).await;
    assert_eq!(b["summary"]["total"], 0, "node row + observations removed");
}

#[tokio::test]
async fn node_delete_blocked_while_pipeline_assigned() {
    let app = app().await;
    let (st, port) = spawn_mock_engine().await;
    let (node_id, _key, _token) = online_node(&app, port, "edge-02").await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let pid = published_pipeline(&app, kafka, doris).await;
    // deploy assigns the node
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": node_id})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    // node delete now 409 IN_USE with refs.pipelines
    let (s, b) = req(&app, "DELETE", &format!("/api/nodes/{node_id}"), None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "IN_USE");
    assert_eq!(b["error"]["refs"]["pipelines"][0], pid);
    assert_eq!(
        st.plans.lock().unwrap().len(),
        1,
        "one plan captured before the blocked delete"
    );
}
