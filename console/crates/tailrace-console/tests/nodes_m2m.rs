//! m2m register/heartbeat integration tests (§4.3 + audit H1 protocol)
//! against the real router. The full §4.3 heartbeat example body is used
//! verbatim.
//!
//! Post-H1 contract: the console-global bootstrap token admits an unknown
//! node_key exactly once; register responds with a server-generated per-node
//! token (plaintext echoed once, only sha256 at rest) that the node must
//! present on every later register/heartbeat. Re-registering rotates the
//! token again.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::Router;
use console_auth::{token_hash, BootstrapAuthority};
use console_store::Store;
use serde_json::{json, Value};
use tailrace_console::{build_router, state::AppState};
use tower::ServiceExt;

const GLOBAL_TOKEN: &str = "tok-secret";
const NODE_KEY: &str = "sn-0123456789abcdef";

fn app() -> (Router, Store) {
    let store = Store::open_in_memory().unwrap();
    let state = AppState::new(store.clone(), BootstrapAuthority::new(GLOBAL_TOKEN));
    (build_router(state), store)
}

async fn json_body(resp: Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn post(app: &Router, path: &str, body: Value) -> Response {
    app.clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn register_rejects_bad_token() {
    let (app, _) = app();
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({
            "nodeKey": NODE_KEY, "apiUrl": "http://10.1.1.5:18100", "bootstrapToken": "wrong"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(resp).await["error"]["code"], "BAD_NODE_TOKEN");
    // missing token entirely
    let resp = post(&app, "/api/nodes/register", json!({ "nodeKey": NODE_KEY })).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn register_issues_per_node_token_once() {
    let (app, store) = app();
    // first registration: the global bootstrap token admits the unknown node
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({
            "nodeKey": NODE_KEY,
            "apiUrl": "http://10.1.1.5:18100",
            "capacityJson": "{\"pipelines\":8,\"statements\":24,\"memory_mb\":2048}",
            "bootstrapToken": GLOBAL_TOKEN
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    let id1 = body["node"]["id"].as_i64().unwrap();
    assert_eq!(body["node"]["nodeKey"], NODE_KEY);
    assert_eq!(body["node"]["name"], "sn-01234567"); // derived name per §4.3

    // the response carries a fresh server-generated per-node token
    let node_token = body["bootstrapToken"].as_str().unwrap().to_string();
    assert!(node_token.starts_with("tr_b_"), "{node_token}");
    assert_ne!(node_token, GLOBAL_TOKEN, "must not echo the global token");
    // only the sha256 digest is at rest — neither plaintext nor global token
    let node = store.node_by_key(NODE_KEY).unwrap().unwrap();
    assert_eq!(
        node.bootstrap_token.as_deref(),
        Some(token_hash(&node_token).as_str())
    );
    assert_ne!(node.bootstrap_token.as_deref(), Some(GLOBAL_TOKEN));
    assert_eq!(node.register_mode, "self");

    // the global token never admits this node again (first registration only)
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({
            "nodeKey": NODE_KEY, "apiUrl": "http://10.1.1.9:18100", "bootstrapToken": GLOBAL_TOKEN
        }),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "global token is first-registration-only"
    );

    // re-register with the per-node token: same identity, drift refreshed,
    // token rotated and echoed once again
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({
            "nodeKey": NODE_KEY, "apiUrl": "http://10.1.1.9:18100", "bootstrapToken": node_token
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["node"]["id"].as_i64().unwrap(), id1);
    let rotated = body["bootstrapToken"].as_str().unwrap().to_string();
    assert_ne!(rotated, node_token, "register rotates the token");
    let node = store.node_by_key(NODE_KEY).unwrap().unwrap();
    assert_eq!(node.api_url.as_deref(), Some("http://10.1.1.9:18100"));
    assert_eq!(
        node.bootstrap_token.as_deref(),
        Some(token_hash(&rotated).as_str())
    );

    // the rotated-away token is dead
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({ "nodeKey": NODE_KEY, "bootstrapToken": node_token }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // register audited (one row per successful register, none for 401s)
    store
        .with_conn(|c| {
            let n: i64 = c.query_row(
                "SELECT COUNT(*) FROM audit_log WHERE action='NODE_REGISTER'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(n, 2);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn heartbeat_full_example_persists_observations() {
    let (app, store) = app();
    // register first (heartbeat never admits unknown nodes); keep the issued
    // per-node token for the heartbeats
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({
            "nodeKey": NODE_KEY, "apiUrl": "http://10.1.1.5:18100", "bootstrapToken": GLOBAL_TOKEN
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let node_token = json_body(resp).await["bootstrapToken"]
        .as_str()
        .unwrap()
        .to_string();

    // the §4.3 heartbeat example, plus one garbage event row that must be skipped
    let mut hb = json!({
        "nodeKey": NODE_KEY,
        "name": "edge-01",
        "apiUrl": "http://10.1.1.5:18100",
        "engineVersion": "1.0.0",
        "capacityJson": "{\"pipelines\":8,\"statements\":24,\"memory_mb\":2048,\"running_pipelines\":2,\"running_statements\":5}",
        "pipelineStates": [
            { "id": 42, "state": "RUNNING", "statements": 2,
              "totals": { "in": 128000, "out": 127996, "flushes": 12 } }
        ],
        "events": [
            { "ts": 1760160000, "level": "ERROR", "kind": "STATEMENT_FAILED", "pipelineId": 42, "message": "boom" },
            { "level": "INFO" }
        ],
        "bootstrapToken": node_token,
        "resources": { "rssBytes": 91226112, "memTotalBytes": 4294967296i64, "cpuPercent": 7.2, "rxBps": 20480, "txBps": 15360 }
    });
    // the global token must NOT pass a heartbeat of a registered node
    let mut with_global = hb.clone();
    with_global["bootstrapToken"] = json!(GLOBAL_TOKEN);
    let resp = post(&app, "/api/nodes/heartbeat", with_global).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = post(&app, "/api/nodes/heartbeat", hb.clone()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["node"]["nodeKey"], NODE_KEY);

    let node = store.node_by_key(NODE_KEY).unwrap().unwrap();
    assert_eq!(node.status, "ONLINE");
    assert_eq!(node.name, "edge-01"); // name adopted from heartbeat
    assert_eq!(node.engine_version.as_deref(), Some("1.0.0"));
    assert!(node.last_heartbeat.is_some());

    store
        .with_conn(|c| {
            // one valid event persisted; garbage row skipped
            let ev: i64 = c.query_row("SELECT COUNT(*) FROM node_event", [], |r| r.get(0))?;
            // pipeline metrics row with totals
            let pm: (i64, i64, i64) = c.query_row(
                "SELECT consumed, flushed, flushes FROM pipeline_metrics WHERE pipeline_id = 42",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            // node metrics from resources
            let nm: i64 = c.query_row("SELECT COUNT(*) FROM node_metrics", [], |r| r.get(0))?;
            assert_eq!((ev, nm), (1, 1));
            assert_eq!(pm, (128000, 127996, 12));
            Ok(())
        })
        .unwrap();

    // field-tolerance: a minimal heartbeat (only identity + token) is accepted
    hb["pipelineStates"] = Value::Null;
    hb["events"] = Value::Null;
    hb["resources"] = Value::Null;
    let resp = post(&app, "/api/nodes/heartbeat", hb).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn heartbeat_rejects_unknown_and_revoked() {
    let (app, store) = app();
    // unknown node + global token -> 401 (heartbeat requires prior register)
    let resp = post(
        &app,
        "/api/nodes/heartbeat",
        json!({
            "nodeKey": "sn-unknown00000000", "bootstrapToken": GLOBAL_TOKEN
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // register, then the global token no longer works for heartbeats either
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({
            "nodeKey": "sn-cccccccccccccccc", "bootstrapToken": GLOBAL_TOKEN
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let c_token = json_body(resp).await["bootstrapToken"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = post(
        &app,
        "/api/nodes/heartbeat",
        json!({
            "nodeKey": "sn-cccccccccccccccc", "bootstrapToken": GLOBAL_TOKEN
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // revoked (NULL digest) rejects even the correct token (§4.3)
    store.revoke_bootstrap_token("sn-cccccccccccccccc").unwrap();
    let resp = post(
        &app,
        "/api/nodes/heartbeat",
        json!({
            "nodeKey": "sn-cccccccccccccccc", "bootstrapToken": c_token
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(resp).await["error"]["code"], "BAD_NODE_TOKEN");

    // wrong token on a live node -> 401
    let resp = post(
        &app,
        "/api/nodes/register",
        json!({
            "nodeKey": "sn-dddddddddddddddd", "bootstrapToken": GLOBAL_TOKEN
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = post(
        &app,
        "/api/nodes/heartbeat",
        json!({
            "nodeKey": "sn-dddddddddddddddd", "bootstrapToken": "nope"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn session_cannot_call_m2m_and_vice_versa() {
    let (app, _) = app();
    // no session cookie needed for m2m (they are the free-of-session list)
    // and a session token must NOT authorize m2m endpoints
    let resp = post(&app, "/api/nodes/register", json!({
        "nodeKey": NODE_KEY, "bootstrapToken": "tr_s_000000000000000000000000000000000000000000000000"
    }))
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
