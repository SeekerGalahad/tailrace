//! §4.4 deploy-chain surface without a live SSH target: validation gates,
//! background-chain acceptance (unreachable host → PREFLIGHT FAIL → node
//! FAILED with deploy_log rows), and the non-SSH-managed action guard.
//!
//! The real eight-step chain against a docker sshd target (including the
//! mid-failure rollback case) lives in `nodes_deploy_docker_e2e.rs` — gated
//! behind `TAILRACE_TESTCONTAINERS=1`.

mod common;

use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::*;
use serde_json::json;

/// Wait until the node row's deploy_status reaches `want` (the chain runs in
/// a background task; store writes are its only observable here).
async fn wait_deploy_status(app: &TestApp, node_id: i64, want: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(Some(node)) = app.store.node(node_id) {
            if node.deploy_status.as_deref() == Some(want) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn deploy_validates_body_and_artifact() {
    let app = app().await;
    // a real artifact row so validation below the artifact gate is reachable
    let artifact_id = match app
        .store
        .insert_artifact(
            "tailrace-node",
            "1.0.0",
            "f.tar.gz",
            "deadbeef",
            10,
            "admin",
        )
        .unwrap()
    {
        console_store::ArtifactInsert::Created(id) => id,
        console_store::ArtifactInsert::Duplicate => panic!("unique artifact"),
    };
    // missing artifactId -> 400
    let (s, _) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"hosts": [{"host": "10.1.1.6", "name": "n1", "sshUser": "root"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // unknown artifact -> 404 (before any node is created)
    let (s, _) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"hosts": [{"host": "10.1.1.6", "name": "n1", "sshUser": "root"}], "artifactId": artifact_id + 999})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    // neither hosts nor nodeId -> 400
    let (s, _) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"artifactId": artifact_id})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // both -> 400
    let (s, _) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"hosts": [{"host": "10.1.1.6", "name": "n1", "sshUser": "root"}], "nodeId": 1, "artifactId": artifact_id})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // empty hosts -> 400
    let (s, _) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"hosts": [], "artifactId": artifact_id})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // nodeId mode on a nonexistent node -> 404
    let (s, _) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"nodeId": 999, "artifactId": artifact_id})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    // nothing was created
    let (_s, b) = req(&app, "GET", "/api/nodes", None).await;
    assert_eq!(b["summary"]["total"], 0);
}

/// Batch hosts[] mode with an unreachable SSH target: the request is
/// accepted (nodes created, `deploys[].status=PENDING`), then the background
/// chain fails PREFLIGHT (no TAILRACE_SSH_KEY configured / connect refused)
/// and leaves FAILED nodes with an honest deploy_log trail.
#[tokio::test]
async fn deploy_unreachable_target_fails_preflight_with_log() {
    let app = app().await;
    // a real artifact row (bytes never read — PREFLIGHT fails first)
    let artifact_id = match app
        .store
        .insert_artifact(
            "tailrace-node",
            "1.0.0",
            "f.tar.gz",
            "deadbeef",
            10,
            "admin",
        )
        .unwrap()
    {
        console_store::ArtifactInsert::Created(id) => id,
        console_store::ArtifactInsert::Duplicate => panic!("unique artifact"),
    };

    let (s, b) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({
            "hosts": [{"host": "127.0.0.1", "name": "dead-target", "sshUser": "root", "sshPort": 1, "enginePort": 18199}],
            "artifactId": artifact_id
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let deploys = b["deploys"].as_array().unwrap();
    assert_eq!(deploys.len(), 1);
    assert_eq!(deploys[0]["status"], "PENDING");
    let node_id = deploys[0]["nodeId"].as_i64().unwrap();

    // the chain fails fast (connect refused on port 1) -> FAILED
    assert!(
        wait_deploy_status(&app, node_id, "FAILED", Duration::from_secs(30)).await,
        "chain should reach FAILED; last: {:?}",
        app.store
            .node(node_id)
            .unwrap()
            .and_then(|n| n.deploy_status)
    );

    // node row: register_mode=deployed, PENDING → FAILED (no REGISTERED lie)
    let node = app.store.node(node_id).unwrap().unwrap();
    assert_eq!(node.register_mode, "deployed");
    assert_eq!(node.deploy_status.as_deref(), Some("FAILED"));
    assert_eq!(node.artifact_id, None, "artifactId only pinned on success");

    // deploy_log carries the PREFLIGHT FAIL row (ascending, via the API)
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/nodes/{node_id}/deploy-log?limit=10"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let items = b["items"].as_array().unwrap();
    assert!(
        !items.is_empty(),
        "deploy_log must not be empty after a run"
    );
    let first = &items[0];
    assert_eq!(first["step"], "PREFLIGHT");
    assert_eq!(first["status"], "FAIL");
    assert!(
        first["output"].as_str().unwrap().contains("SSH"),
        "output names the SSH failure: {first}"
    );
}

/// nodeId redeploy of a manual node (no SSH identity) is a 400, and the
/// process-action endpoint keeps its non-SSH-managed guard.
#[tokio::test]
async fn redeploy_and_actions_reject_non_ssh_managed_nodes() {
    let app = app().await;
    let (_st, port) = spawn_mock_engine().await;
    let (id, _key, _token) = online_node(&app, port, "manual-edge").await;
    let artifact_id = match app
        .store
        .insert_artifact(
            "tailrace-node",
            "1.0.0",
            "f.tar.gz",
            "deadbeef",
            10,
            "admin",
        )
        .unwrap()
    {
        console_store::ArtifactInsert::Created(id) => id,
        console_store::ArtifactInsert::Duplicate => panic!("unique artifact"),
    };
    let (s, b) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"nodeId": id, "artifactId": artifact_id})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    for action in ["stop", "restart", "rollback", "uninstall"] {
        let (s, b) = req(
            &app,
            "POST",
            &format!("/api/nodes/{id}/action"),
            Some(json!({"action": action})),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{action}: {b}");
    }
}

/// upgrade without artifactId, and unknown action names, are 400s.
#[tokio::test]
async fn action_validation() {
    let app = app().await;
    // deploy a (dead) target so a deployed node exists for the action surface
    let artifact_id = match app
        .store
        .insert_artifact(
            "tailrace-node",
            "1.0.0",
            "f.tar.gz",
            "deadbeef",
            10,
            "admin",
        )
        .unwrap()
    {
        console_store::ArtifactInsert::Created(id) => id,
        console_store::ArtifactInsert::Duplicate => panic!("unique artifact"),
    };
    let (_s, b) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({"hosts": [{"host": "127.0.0.1", "name": "dead-action", "sshUser": "root", "sshPort": 1}], "artifactId": artifact_id})),
    )
    .await;
    let id = b["deploys"][0]["nodeId"].as_i64().unwrap();
    assert!(
        wait_deploy_status(&app, id, "FAILED", Duration::from_secs(30)).await,
        "dead target"
    );
    // upgrade without artifactId -> 400
    let (s, _) = req(
        &app,
        "POST",
        &format!("/api/nodes/{id}/action"),
        Some(json!({"action": "upgrade"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // unknown action -> 400 (dictionary copy)
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/nodes/{id}/action"),
        Some(json!({"action": "explode"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["error"]["message_key"], "err.node.action_unknown");
    // stop on the failed deployed node: SSH still refused -> 502 SSH_FAILED
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/nodes/{id}/action"),
        Some(json!({"action": "stop"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "SSH_FAILED");
}
