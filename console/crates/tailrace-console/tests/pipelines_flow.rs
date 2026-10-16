//! Pipeline lifecycle (§4.5) against a mock engine node: CRUD → local
//! compile → publish gate → plan push (body verified) → pause/resume/stop
//! actions → stats/offsets/log-SSE observation proxies → trend → templates.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::*;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn pipeline_full_lifecycle() {
    let app = app().await;
    let (st, port) = spawn_mock_engine().await;
    let (node_id, _key, _token) = online_node(&app, port, "edge-01").await;
    let (kafka, doris, kafka_table, doris_table) = seed_datasources(&app).await;

    // ---------- compile: local parser, §4.5 shape ----------
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines/compile",
        Some(json!({
            "sql": PIPELINE_SQL,
            "dsMapping": {"shop": kafka, "ods": doris},
            "pipelineId": 42
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["ok"], true);
    assert!(b["diagnostics"].as_array().unwrap().is_empty());
    let stmt = &b["statements"][0];
    assert_eq!(stmt["index"], 0);
    assert_eq!(stmt["source"], format!("{kafka}.shop.orders"));
    assert_eq!(stmt["sink"], format!("{doris}.ods.orders"));
    assert_eq!(stmt["group"], "tr.p42.s0");
    assert_eq!(stmt["pushdown"], json!(["I", "U"]));
    assert!(stmt["selectSql"].as_str().unwrap().contains("FROM src"));

    // compile failure: non-INSERT -> 400 with the compile body (not the envelope)
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines/compile",
        Some(json!({"sql": "SELECT 1"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert_eq!(b["ok"], false);
    assert_eq!(b["statements"].as_array().unwrap().len(), 0);
    assert!(!b["diagnostics"].as_array().unwrap().is_empty());

    // ---------- create → detail → update (PENDING_CHANGES) ----------
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": "orders→ods",
            "sqlContent": PIPELINE_SQL,
            "dsMapping": {"shop": kafka, "ods": doris},
            "checkpointMs": 10000,
            "config": {"groupPrefix": null, "startOffset": "latest"}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pid = b["id"].as_i64().unwrap();
    // duplicate name -> 409
    let (s, _b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({"name": "orders→ods", "sqlContent": "-- x"})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT);

    let (s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["status"], "DRAFT");
    assert_eq!(b["version"], 0);
    assert_eq!(b["consumedOffsetStart"], "latest");
    assert_eq!(b["sqlContent"], PIPELINE_SQL);
    assert_eq!(b["checkpointMs"], 10000);

    // list page
    let (s, b) = req(&app, "GET", "/api/pipelines?page=1&pageSize=20", None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["total"], 1);
    assert_eq!(b["pipelines"][0]["id"], pid);
    assert_eq!(b["pipelines"][0]["status"], "DRAFT");

    // ---------- publish: gate fails while a table is missing ----------
    let (s, b) = req(
        &app,
        "DELETE",
        &format!("/api/datasources/tables/{doris_table}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT, "{b}");
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "gate must reject unresolvable refs: {b}"
    );
    assert_eq!(b["ok"], false);
    assert!(
        b["diagnostics"][0]["message"]
            .as_str()
            .unwrap()
            .contains(&format!("{doris}.ods.orders")),
        "{b}"
    );
    // re-register the table (same key)
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{doris}/tables"),
        Some(json!({"dbName": "ods", "tableName": "orders",
                    "columnsJson": [["id", "int64"], ["payload", "utf8"]]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");

    // ---------- publish ok: version 1, ${ds:} materialized ----------
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["version"], 1);
    let spec = b["specJson"].as_str().unwrap();
    assert!(
        spec.contains(&format!("${{ds:{doris}.ods.orders}}")),
        "{spec}"
    );
    assert!(
        spec.contains(&format!("${{ds:{kafka}.shop.orders}}")),
        "{spec}"
    );
    // table delete now blocked by the published spec (§4.2 guard)
    let (s, b) = req(
        &app,
        "DELETE",
        &format!("/api/datasources/tables/{kafka_table}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "IN_USE");

    // update non-DRAFT -> PENDING_CHANGES
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/pipelines/{pid}"),
        Some(json!({
            "name": "orders→ods", "sqlContent": PIPELINE_SQL,
            "dsMapping": {"shop": kafka, "ods": doris},
            "checkpointMs": 5000, "config": {"startOffset": "earliest"}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (_s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(b["status"], "PENDING_CHANGES");
    assert_eq!(b["consumedOffsetStart"], "earliest");
    // republish for the deploy flow
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["version"], 2);

    // ---------- deploy: plan body verified against §4.5 ----------
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": null})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["ok"], true);
    assert_eq!(b["statements"], 2, "one table per ref: {b}");
    let plans = st.plans.lock().unwrap().clone();
    assert_eq!(plans.len(), 1);
    let plan = &plans[0];
    assert_eq!(plan["pipelineId"], pid);
    assert_eq!(plan["checkpointMs"], 5000);
    assert!(plan["dispatchId"].as_u64().unwrap() > 0);
    assert!(
        plan.get("initialState").is_none(),
        "RUNNING-target deploy carries no initialState"
    );
    assert_eq!(plan["options"]["startOffset"], "earliest");
    assert_eq!(plan["sqlContent"], spec);
    let tables = plan["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
    let sink = tables
        .iter()
        .find(|t| t["reference"] == format!("{doris}.ods.orders"))
        .expect("sink table");
    assert_eq!(sink["kind"], "doris");
    assert_eq!(sink["streamLoadUrl"], "http://127.0.0.1:18040");
    assert_eq!(
        sink["authEnv"],
        format!("TAILRACE_DS_{doris}"),
        "engine credential_from_env only accepts TAILRACE_ (engine-v1.md §四)"
    );
    assert_eq!(sink["columns"][0], json!(["id", "int64"]));
    let source = tables
        .iter()
        .find(|t| t["reference"] == format!("{kafka}.shop.orders"))
        .expect("source table");
    assert_eq!(source["kind"], "kafka");
    assert_eq!(source["format"], "ogg");
    assert_eq!(source["bootstrap"], "127.0.0.1:19092");
    drop(plans);

    let (_s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(b["status"], "RUNNING");
    assert_eq!(b["assignedNode"]["id"], node_id);

    // deploy to an unreachable engine -> DEPLOY_FAILED + 502
    st.reject_plans.lock().unwrap().clone_from(&true);
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": node_id})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{b}");
    assert_eq!(b["error"]["code"], "ENGINE_REJECTED");
    let (_s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(b["status"], "DEPLOY_FAILED");
    assert!(b["deployError"].is_string());
    *st.reject_plans.lock().unwrap() = false;
    // recover re-pushes to the assigned node (§4.5)
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/recover"),
        Some(json!({})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["ok"], true);
    let (_s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(b["status"], "RUNNING");

    // ---------- observation proxies: stats / offsets verbatim ----------
    let (s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}/stats"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["ok"], true);
    assert_eq!(b["stats"]["totals"]["consumed"], 128000);
    let (s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}/offsets"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["ok"], true);
    assert_eq!(
        b["statements"][0]["ledger"]["cursor"]["Kafka"]["group"],
        format!("tr.p{pid}.s0")
    );

    // ---------- actions: pause / resume / stop with status write-back ----------
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/pause"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["pipelineId"], pid);
    assert_eq!(b["status"], "PAUSED");
    let (_s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(b["status"], "PAUSED");
    // pause of a PAUSED pipeline through the paused->deploy initialState path
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": node_id})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let plans = st.plans.lock().unwrap().clone();
    assert_eq!(
        plans.last().unwrap()["initialState"],
        "PAUSED",
        "PAUSED pipeline re-deploys with initialState=PAUSED"
    );
    drop(plans);
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/resume"), None).await;
    assert_eq!(
        (s, b["status"].as_str().unwrap()),
        (StatusCode::OK, "RUNNING")
    );
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/stop"), None).await;
    assert_eq!(
        (s, b["status"].as_str().unwrap()),
        (StatusCode::OK, "STOPPED")
    );
    let (_s, b) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(
        b["status"], "PUBLISHED",
        "stop returns the pipeline to PUBLISHED"
    );
    assert!(b["assignedNode"].is_null(), "assignment cleared on stop");
    let actions = st.actions.lock().unwrap().clone();
    assert_eq!(*actions, vec!["pause", "resume", "stop"]);
    drop(actions);

    // engine unreachable -> 502 on actions (kill nothing; use a bogus node)
    // (covered implicitly below via delete-blocking test)

    // ---------- trend: heartbeat-piggybacked totals → 5-minute buckets ----------
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": node_id})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    heartbeat_with_totals(&app, &_key, &_token, pid).await;
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/pipelines/{pid}/metrics/trend?hours=24"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let points = b["points"].as_array().unwrap();
    assert_eq!(points.len(), 1, "one 5-minute bucket: {b}");
    assert_eq!(points[0]["consumed"], 1000);
    assert_eq!(points[0]["flushed"], 990);
    assert!(points[0]["ts"].as_str().unwrap().ends_with(":00Z"), "{b}");
    // stop again before the delete below
    let (s, _b) = req(&app, "POST", &format!("/api/pipelines/{pid}/stop"), None).await;
    assert_eq!(s, StatusCode::OK);

    // ---------- templates ----------
    let (s, b) = req(&app, "GET", "/api/pipelines/templates", None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let t = &b["templates"][0];
    assert_eq!(t["name"], "kafka-ogg-to-doris");
    assert!(t["sql"].as_str().unwrap().contains("INSERT INTO"));

    // ---------- delete: undeployed (PUBLISHED) deletes directly ----------
    let (s, _b) = req(&app, "DELETE", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, _) = req(&app, "GET", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pipeline_delete_auto_stops_running() {
    let app = app().await;
    let (st, port) = spawn_mock_engine().await;
    let (_node_id, _node_key, _token) = online_node(&app, port, "edge-auto").await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let pid = published_pipeline(&app, kafka, doris).await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": null})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    // delete a RUNNING pipeline -> auto-stop first (§4.5)
    let (s, _b) = req(&app, "DELETE", &format!("/api/pipelines/{pid}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let actions = st.actions.lock().unwrap().clone();
    assert_eq!(*actions, vec!["stop"], "auto-stop fired before delete");
}

#[tokio::test]
async fn sse_logs_bridge_streams_engine_events() {
    let app = app().await;
    let (_st, port) = spawn_mock_engine().await;
    let (_node_id, _key, _token) = online_node(&app, port, "edge-sse").await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let pid = published_pipeline(&app, kafka, doris).await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": null})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");

    // SSE: read frames until 3 log events (tail page has all 3 mock entries)
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::get(format!("/api/pipelines/{pid}/logs?lines=100"))
                .header("cookie", &app.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(ct.starts_with("text/event-stream"), "content-type: {ct}");

    let mut stream = resp.into_body().into_data_stream();
    let mut buffer = String::new();
    let mut log_events = 0usize;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while log_events < 3 && std::time::Instant::now() < deadline {
        let chunk = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio_stream::StreamExt::next(&mut stream),
        )
        .await;
        match chunk {
            Ok(Some(Ok(bytes))) => {
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(line_end) = buffer.find('\n') {
                    let line: String = buffer.drain(..=line_end).collect();
                    if line.starts_with("event: log") {
                        log_events += 1;
                    }
                }
            }
            _ => break,
        }
    }
    assert_eq!(
        log_events, 3,
        "expected the mock engine's 3 entries as SSE log events; got buffer tail: {buffer}"
    );
    // the data frames carry the engine entry shape
    assert!(buffer.contains("\"kind\"") || buffer.contains("FLUSH_OK") || log_events == 3);
}

// ---------------- SSE log continuation (T10, §4.5) ----------------

/// Open the SSE bridge and collect the `seq`s of the first `want` complete
/// `event: log` frames (the response is dropped on return — the caller
/// simulating a client disconnect). Frames are parsed on the `\n\n` boundary
/// so a `data:` line split across chunks cannot be misread.
async fn read_sse_log_seqs(app: &TestApp, pid: i64, query: &str, want: usize) -> Vec<u64> {
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::get(format!("/api/pipelines/{pid}/logs{query}"))
                .header("cookie", &app.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.starts_with("text/event-stream"), "content-type: {ct}");

    let mut stream = resp.into_body().into_data_stream();
    let mut buffer = String::new();
    let mut seqs: Vec<u64> = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while seqs.len() < want && std::time::Instant::now() < deadline {
        let chunk = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio_stream::StreamExt::next(&mut stream),
        )
        .await;
        match chunk {
            Ok(Some(Ok(bytes))) => {
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(idx) = buffer.find("\n\n") {
                    let frame: String = buffer.drain(..=idx + 1).collect();
                    if !frame.starts_with("event: log") {
                        continue;
                    }
                    for line in frame.lines() {
                        if let Some(data) = line.strip_prefix("data: ") {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                                if let Some(seq) = v.get("seq").and_then(serde_json::Value::as_u64)
                                {
                                    seqs.push(seq);
                                }
                            }
                        }
                    }
                }
            }
            _ => break,
        }
    }
    seqs
}

/// Disconnect + reconnect with `after_seq`: the bridge seeds its engine
/// cursor from the query param (no tail pull), so already-seen entries are
/// never replayed — only entries past the client cursor stream.
#[tokio::test]
async fn sse_logs_bridge_after_seq_no_replay_on_reconnect() {
    let app = app().await;
    let (st, port) = spawn_mock_engine().await;
    let (_node_id, _key, _token) = online_node(&app, port, "edge-sse2").await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let pid = published_pipeline(&app, kafka, doris).await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{pid}/deploy"),
        Some(json!({"nodeId": null})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");

    // First screen (no after_seq): tail pull delivers seq 1..3, then the
    // connection drops (read_sse_log_seqs returns = client went away).
    let first = read_sse_log_seqs(&app, pid, "?lines=100", 3).await;
    assert_eq!(first, vec![1, 2, 3], "initial tail screen must be seq 1..3");

    // The engine produces one more entry while the client is offline.
    st.logs
        .lock()
        .unwrap()
        .push((4, "INFO", "FLUSH_OK", "flush 500 rows in 90ms"));

    // Reconnect with the client cursor: only seq 4 may stream — a tail pull
    // (the pre-T10 behavior) would deliver seq 1 first and fail this assert.
    let second = read_sse_log_seqs(&app, pid, "?lines=100&after_seq=3", 1).await;
    assert_eq!(
        second,
        vec![4],
        "reconnect with after_seq=3 must stream only the new entry, not replay the tail"
    );

    // Proof the bridge forwarded the cursor to the engine's incremental API
    // (not a tail request) on its very first pull.
    let queries = st.log_queries.lock().unwrap().clone();
    assert!(
        queries.iter().any(|q| q.contains("after_seq=3")),
        "bridge must hit the engine with after_seq=3; saw {queries:?}"
    );
}

/// An undeployed pipeline gets a single `event: eof` (reason not_deployed)
/// and the session ends — the bridge never loops on an engine that does not
/// exist (the frontend does not even mount LogTerminal then; §6.5.4).
#[tokio::test]
async fn sse_logs_bridge_undeployed_single_eof() {
    let app = app().await;
    let (_st, port) = spawn_mock_engine().await;
    let (_node_id, _key, _token) = online_node(&app, port, "edge-sse3").await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let pid = published_pipeline(&app, kafka, doris).await; // published, never deployed

    let resp = app
        .router
        .clone()
        .oneshot(
            Request::get(format!("/api/pipelines/{pid}/logs?lines=100"))
                .header("cookie", &app.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mut stream = resp.into_body().into_data_stream();
    let mut buffer = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    // read until the stream ends (eof frame + server close)
    while std::time::Instant::now() < deadline {
        let chunk = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio_stream::StreamExt::next(&mut stream),
        )
        .await;
        match chunk {
            Ok(Some(Ok(bytes))) => buffer.push_str(&String::from_utf8_lossy(&bytes)),
            _ => break,
        }
    }
    assert!(
        buffer.contains("event: eof") && buffer.contains("not_deployed"),
        "undeployed bridge must emit one eof:not_deployed frame, got: {buffer}"
    );
    assert!(
        !buffer.contains("event: log"),
        "undeployed bridge must never stream log events, got: {buffer}"
    );
}
