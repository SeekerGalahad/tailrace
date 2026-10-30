//! T-E2 alerting contract tests: rules CRUD + audit, the evaluator state
//! machine driven by mock snapshots (threshold boundary, event rows only on
//! transitions, resolved always reachable, disable mid-flight), window
//! rules over mock counter history, webhook delivery (received / unreachable
//! never fails the pass), the SSE hub, and the `/metrics` SLO target riding
//! the lag series' label set end to end through a mock node.

mod common;

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use common::{app, req, TestApp};
use serde_json::{json, Value};
use tailrace_console::alerting::{parse_samples, Evaluator, Sample};
use tower::ServiceExt;

/// Realistic epoch-ms origin (a 0 origin would let u64 saturation fabricate
/// a window base at the cutoff).
const T0: u64 = 1_700_000_000_000;

fn lag_snapshot(node: &str, pipeline: &str, lag: f64) -> Vec<Sample> {
    parse_samples(
        &format!(
            "# TYPE tailrace_pipeline_lag_seconds gauge\n\
             tailrace_pipeline_lag_seconds{{pipeline=\"{pipeline}\",statement=\"0\"}} {lag}\n"
        ),
        Some(node),
    )
}

async fn create_rule(app: &TestApp, body: Value) -> i64 {
    let (s, b) = req(app, "POST", "/api/alerts/rules", Some(body)).await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    b["id"].as_i64().unwrap()
}

fn audit_actions(app: &TestApp) -> Vec<String> {
    app.store
        .with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT action FROM audit_log WHERE entity_type = 'alert_rule' ORDER BY id",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .unwrap()
}

/// Rules CRUD: every 400 gate, 201 → 409 on duplicate name, the combined
/// read shape, PUT (incl. rename conflict / 404), DELETE (204 then 404),
/// and the audit trail CREATE/UPDATE/DELETE.
#[tokio::test]
async fn rules_crud_contract_and_audit() {
    let app = app().await;
    // 400 gates
    for (body, key) in [
        (
            json!({"name": " ", "ruleType": "lag_seconds", "params": {"threshold": 30}}),
            "err.alert.name_missing",
        ),
        (
            json!({"name": "x", "ruleType": "p99", "params": {}}),
            "err.alert.type_unknown",
        ),
        (
            json!({"name": "x", "ruleType": "lag_seconds", "params": {}}),
            "err.alert.params_invalid",
        ),
        (
            json!({"name": "x", "ruleType": "dlq_window", "params": {"threshold": 5, "windowSecs": 0}}),
            "err.alert.params_invalid",
        ),
        (
            json!({"name": "x", "ruleType": "lag_seconds", "params": {"threshold": 30}, "webhookUrl": "ftp://x"}),
            "err.alert.webhook_url_invalid",
        ),
    ] {
        let (s, b) = req(&app, "POST", "/api/alerts/rules", Some(body)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["code"], "INVALID_ARGUMENT");
        assert_eq!(b["error"]["message_key"], key, "{b}");
    }
    // malformed JSON body
    let (s, b, _) = common::raw_request(
        &app,
        "POST",
        "/api/alerts/rules",
        [("content-type", "application/json".to_string())]
            .into_iter()
            .collect(),
        b"{not json".to_vec(),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(b["error"]["message_key"], "err.bad_body");

    let id = create_rule(
        &app,
        json!({"name": "orders lag", "ruleType": "lag_seconds",
               "params": {"threshold": 30, "pipeline": "3"}}),
    )
    .await;
    let (s, b) = req(
        &app,
        "POST",
        "/api/alerts/rules",
        Some(json!({"name": "orders lag", "ruleType": "state_failed", "params": {}})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    assert_eq!(b["error"]["code"], "CONFLICT");
    assert_eq!(b["error"]["message_key"], "err.alert.name_taken");

    // combined read
    let (s, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["ruleTypes"],
        json!([
            "lag_seconds",
            "state_failed",
            "heartbeat_age",
            "dlq_window",
            "flush_errors_window"
        ])
    );
    assert_eq!(b["intervalSecs"], 15);
    assert_eq!(b["rules"].as_array().unwrap().len(), 1);
    let r = &b["rules"][0];
    assert_eq!(r["id"], id);
    assert_eq!(r["ruleType"], "lag_seconds");
    assert_eq!(r["params"], json!({"threshold": 30, "pipeline": "3"}));
    assert_eq!(r["enabled"], true);
    assert!(r["webhookUrl"].is_null());
    assert_eq!(r["state"], "inactive");
    assert!(r["openEvent"].is_null());
    assert!(r["createdAt"].as_str().unwrap().ends_with('Z'));
    assert_eq!(b["events"], json!([]));

    // PUT: full replace, state kept
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/alerts/rules/{id}"),
        Some(json!({"name": "orders lag v2", "ruleType": "lag_seconds",
                    "params": {"threshold": 45}, "enabled": false,
                    "webhookUrl": "http://127.0.0.1:1/hook"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["name"], "orders lag v2");
    assert_eq!(b["params"], json!({"threshold": 45}));
    assert_eq!(b["enabled"], false);
    assert_eq!(b["webhookUrl"], "http://127.0.0.1:1/hook");
    assert_eq!(b["state"], "inactive");
    // rename onto another rule's name → 409; unknown id → 404
    let other = create_rule(&app, json!({"name": "failed", "ruleType": "state_failed"})).await;
    let (s, b) = req(
        &app,
        "PUT",
        &format!("/api/alerts/rules/{other}"),
        Some(json!({"name": "orders lag v2", "ruleType": "state_failed", "params": {}})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");
    let (s, b) = req(
        &app,
        "PUT",
        "/api/alerts/rules/999",
        Some(json!({"name": "ghost", "ruleType": "state_failed", "params": {}})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{b}");
    assert_eq!(b["error"]["message_key"], "err.alert.not_found");

    // DELETE
    let (s, _) = req(&app, "DELETE", &format!("/api/alerts/rules/{id}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, b) = req(&app, "DELETE", &format!("/api/alerts/rules/{id}"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{b}");
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"].as_array().unwrap().len(), 1);

    assert_eq!(
        audit_actions(&app),
        vec![
            "ALERT_RULE_CREATE",
            "ALERT_RULE_UPDATE",
            "ALERT_RULE_CREATE",
            "ALERT_RULE_DELETE"
        ],
        "every mutation audited (rejected writes are not)"
    );
}

/// The evaluator state machine against mock lag snapshots: the boundary
/// value does not fire, the first violation opens ONE episode row, five
/// more violating passes add no rows (sample refreshed), clearing resolves
/// the same row (resolved sample recorded, rule inactive), a quiet pass
/// after that emits nothing, a new violation opens a second row, and
/// disabling a firing rule resolves it on the next pass.
#[tokio::test]
async fn evaluator_episodes_land_only_on_transitions_and_resolve() {
    let app = app().await;
    let id = create_rule(
        &app,
        json!({"name": "lag", "ruleType": "lag_seconds", "params": {"threshold": 30, "pipeline": "3"}}),
    )
    .await;
    let mut ev = Evaluator::new();
    let mut t = T0;
    let mut tick = |lag: f64| {
        t += 15_000;
        (t, lag_snapshot("edge-1", "3", lag))
    };

    // boundary: 30 > 30 is false
    let (now, snap) = tick(30.0);
    let notices = ev.run_pass(&app.state, &snap, now).await.unwrap();
    assert!(notices.is_empty(), "value == threshold must not fire");
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "inactive");
    assert_eq!(b["events"].as_array().unwrap().len(), 0);

    // first violation → one firing notice, one episode row
    let (now, snap) = tick(30.5);
    let notices = ev.run_pass(&app.state, &snap, now).await.unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "firing");
    assert_eq!(notices[0].rule_id, id);
    assert_eq!(notices[0].sample["value"], 30.5);
    assert_eq!(notices[0].sample["labels"]["node"], "edge-1");
    assert_eq!(notices[0].ts_ms, now);
    let firing_event = notices[0].event_id;
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "firing");
    assert_eq!(b["rules"][0]["openEvent"]["id"], firing_event);
    assert_eq!(b["rules"][0]["openEvent"]["state"], "firing");
    assert_eq!(b["events"].as_array().unwrap().len(), 1);

    // five more violating passes: no notices, still one row, sample moves
    for lag in [40.0, 50.0, 60.0, 70.0, 80.0] {
        let (now, snap) = tick(lag);
        assert!(ev
            .run_pass(&app.state, &snap, now)
            .await
            .unwrap()
            .is_empty());
    }
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    let events = b["events"].as_array().unwrap();
    assert_eq!(events.len(), 1, "firing ticks never add rows: {events:?}");
    assert_eq!(events[0]["id"], firing_event);
    assert_eq!(events[0]["state"], "firing");
    assert_eq!(
        events[0]["sample"]["value"], 80.0,
        "latest sample on the open row"
    );
    assert!(events[0]["resolvedAt"].is_null());

    // clears → resolved on the SAME row, resolving sample kept, rule inactive
    let (now, snap) = tick(3.0);
    let notices = ev.run_pass(&app.state, &snap, now).await.unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "resolved");
    assert_eq!(notices[0].event_id, firing_event);
    assert_eq!(notices[0].sample["value"], 3.0);
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "inactive");
    assert!(b["rules"][0]["openEvent"].is_null());
    let events = b["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["state"], "resolved");
    assert!(events[0]["resolvedAt"].as_str().unwrap().ends_with('Z'));
    assert_eq!(events[0]["resolvedSample"]["value"], 3.0);
    assert_eq!(
        events[0]["sample"]["value"], 80.0,
        "firing-side sample preserved"
    );

    // quiet pass: nothing
    let (now, snap) = tick(3.0);
    assert!(ev
        .run_pass(&app.state, &snap, now)
        .await
        .unwrap()
        .is_empty());

    // new violation: a second row
    let (now, snap) = tick(99.0);
    let notices = ev.run_pass(&app.state, &snap, now).await.unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "firing");
    assert_ne!(notices[0].event_id, firing_event);
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    let events = b["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["state"], "firing", "newest first: {events:?}");
    assert_eq!(events[1]["state"], "resolved");

    // absent series (node stopped exposing) is not a violation → resolves
    let (now, _) = tick(0.0);
    let notices = ev.run_pass(&app.state, &[], now).await.unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "resolved");
    assert!(notices[0].sample["value"].is_null());

    // disable while firing → next pass resolves with the reason
    let (now, snap) = tick(99.0);
    assert_eq!(ev.run_pass(&app.state, &snap, now).await.unwrap().len(), 1);
    let (s, _) = req(
        &app,
        "PUT",
        &format!("/api/alerts/rules/{id}"),
        Some(json!({"name": "lag", "ruleType": "lag_seconds",
                    "params": {"threshold": 30, "pipeline": "3"}, "enabled": false})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "firing", "edit keeps the state");
    let (now, snap) = tick(99.0);
    let notices = ev.run_pass(&app.state, &snap, now).await.unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "resolved");
    assert_eq!(notices[0].sample["reason"], "disabled");
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "inactive");
    // disabled rules stay quiet even when the condition holds
    let (now, snap) = tick(999.0);
    assert!(ev
        .run_pass(&app.state, &snap, now)
        .await
        .unwrap()
        .is_empty());
}

/// Window rules over mock counter snapshots: no base inside the window →
/// not evaluated (no restart false-fires); base at the cutoff → fires on
/// the increment; the base slides so the episode resolves once the burst
/// leaves the window. state_failed and heartbeat_age ride the same pass.
#[tokio::test]
async fn window_and_state_rules_from_mock_snapshots() {
    let app = app().await;
    let dlq = create_rule(
        &app,
        json!({"name": "dlq burst", "ruleType": "dlq_window", "params": {"threshold": 5, "windowSecs": 60}}),
    )
    .await;
    let failed = create_rule(&app, json!({"name": "failed", "ruleType": "state_failed"})).await;
    let hb = create_rule(
        &app,
        json!({"name": "heartbeat", "ruleType": "heartbeat_age", "params": {"threshold": 90}}),
    )
    .await;
    let snap = |dlq_total: f64, state: f64, age: f64| {
        parse_samples(
            &format!(
                "tailrace_dlq_rows_total{{pipeline=\"1\",statement=\"0\"}} {dlq_total}\n\
                 tailrace_pipeline_state{{pipeline=\"1\"}} {state}\n\
                 tailrace_node_heartbeat_age_seconds{{node_id=\"sn-1\"}} {age}\n"
            ),
            Some("edge-1"),
        )
    };
    let mut ev = Evaluator::new();
    // t=0: baseline only; FAILED + stale heartbeat fire immediately
    let notices = ev
        .run_pass(&app.state, &snap(100.0, 3.0, 120.0), T0)
        .await
        .unwrap();
    let mut fired: Vec<(i64, &str)> = notices
        .iter()
        .map(|n| (n.rule_id, n.event.as_str()))
        .collect();
    fired.sort();
    assert_eq!(fired, vec![(failed, "firing"), (hb, "firing")]);
    // t=30s: +2 rows but no base at/before t=−30s → dlq not evaluated;
    // pipeline back to RUNNING + fresh heartbeat → both resolve
    let notices = ev
        .run_pass(&app.state, &snap(102.0, 0.0, 5.0), T0 + 30_000)
        .await
        .unwrap();
    let mut res: Vec<(i64, &str)> = notices
        .iter()
        .map(|n| (n.rule_id, n.event.as_str()))
        .collect();
    res.sort();
    assert_eq!(res, vec![(failed, "resolved"), (hb, "resolved")]);
    // t=60s: base t=0 (100) → increment 10 > 5 → dlq fires
    let notices = ev
        .run_pass(&app.state, &snap(110.0, 0.0, 5.0), T0 + 60_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(
        (notices[0].rule_id, notices[0].event.as_str()),
        (dlq, "firing")
    );
    assert_eq!(notices[0].sample["increment"], 10.0);
    assert_eq!(notices[0].sample["windowSecs"], 60);
    // t=90s: base = newest <= t=30s → 102 → increment 9 → still firing, no notice
    assert!(ev
        .run_pass(&app.state, &snap(111.0, 0.0, 5.0), T0 + 90_000)
        .await
        .unwrap()
        .is_empty());
    // t=130s: base = newest <= t=70s → t=60s (110) → increment 2 → resolved
    let notices = ev
        .run_pass(&app.state, &snap(112.0, 0.0, 5.0), T0 + 130_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(
        (notices[0].rule_id, notices[0].event.as_str()),
        (dlq, "resolved")
    );
    assert_eq!(notices[0].sample["increment"], 2.0);
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    let events = b["events"].as_array().unwrap();
    assert_eq!(events.len(), 3, "one episode per rule: {events:?}");
    assert!(events.iter().all(|e| e["state"] == "resolved"));
}

/// Webhook delivery: the mock receiver gets one POST per transition with
/// the notice JSON (+ `x-tailrace-event`), firing then resolved; an
/// unreachable webhook on another rule is logged and never fails the pass
/// (the transition still lands and is still returned).
#[tokio::test]
async fn webhook_receives_firing_and_resolved() {
    let received: Arc<Mutex<Vec<(String, Value)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = received.clone();
    let hook = Router::new().route(
        "/hook",
        post(
            move |headers: axum::http::HeaderMap, Json(body): Json<Value>| {
                let sink = sink.clone();
                async move {
                    let ev = headers
                        .get("x-tailrace-event")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    sink.lock().unwrap().push((ev, body));
                    StatusCode::NO_CONTENT
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, hook).await.unwrap() });

    let app = app().await;
    let id = create_rule(
        &app,
        json!({"name": "failed→hook", "ruleType": "state_failed",
               "webhookUrl": format!("http://127.0.0.1:{port}/hook")}),
    )
    .await;
    // unreachable webhook (nothing listens on port 1)
    let dead = create_rule(
        &app,
        json!({"name": "failed→dead", "ruleType": "state_failed",
               "webhookUrl": "http://127.0.0.1:1/hook"}),
    )
    .await;
    let state = |v: f64| {
        parse_samples(
            &format!("tailrace_pipeline_state{{pipeline=\"7\"}} {v}\n"),
            Some("edge-1"),
        )
    };
    let mut ev = Evaluator::new();
    let notices = ev.run_pass(&app.state, &state(3.0), T0).await.unwrap();
    assert_eq!(
        notices.len(),
        2,
        "both rules transition despite the dead webhook"
    );
    assert!(notices
        .iter()
        .any(|n| n.rule_id == dead && n.event == "firing"));
    let notices = ev
        .run_pass(&app.state, &state(0.0), T0 + 15_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 2);

    let got = received.lock().unwrap().clone();
    assert_eq!(got.len(), 2, "one POST per transition: {got:?}");
    assert_eq!(got[0].0, "firing");
    assert_eq!(got[0].1["event"], "firing");
    assert_eq!(got[0].1["ruleId"], id);
    assert_eq!(got[0].1["ruleName"], "failed→hook");
    assert_eq!(got[0].1["ruleType"], "state_failed");
    assert_eq!(got[0].1["sample"]["value"], 3.0);
    assert_eq!(got[0].1["sample"]["labels"]["pipeline"], "7");
    assert_eq!(got[0].1["tsMs"], T0);
    assert!(got[0].1["eventId"].as_i64().unwrap() > 0);
    assert_eq!(got[1].0, "resolved");
    assert_eq!(got[1].1["event"], "resolved");
    assert_eq!(got[1].1["eventId"], got[0].1["eventId"], "same episode");
    // the dead-webhook rule's episode landed exactly like the live one
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    let dead_events: Vec<&Value> = b["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["ruleId"] == dead)
        .collect();
    assert_eq!(dead_events.len(), 1);
    assert_eq!(dead_events[0]["state"], "resolved");
}

/// SSE hub: a subscriber receives each transition notice; the stream
/// endpoint answers as an event stream under the session cookie.
#[tokio::test]
async fn sse_hub_broadcasts_transitions() {
    let app = app().await;
    create_rule(
        &app,
        json!({"name": "hb", "ruleType": "heartbeat_age", "params": {"threshold": 90}}),
    )
    .await;
    let mut rx = app.state.subscribe_alerts();
    let snap = |age: f64| {
        parse_samples(
            &format!("tailrace_node_heartbeat_age_seconds{{node_id=\"sn-1\"}} {age}\n"),
            Some("edge-1"),
        )
    };
    let mut ev = Evaluator::new();
    ev.run_pass(&app.state, &snap(91.0), T0).await.unwrap();
    let n = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("hub delivers within 2s")
        .unwrap();
    assert_eq!(n.event, "firing");
    assert_eq!(n.rule_name, "hb");
    assert_eq!(n.sample["value"], 91.0);
    ev.run_pass(&app.state, &snap(1.0), T0 + 15_000)
        .await
        .unwrap();
    let n = rx.try_recv().unwrap();
    assert_eq!(n.event, "resolved");
    // no transition → nothing broadcast
    ev.run_pass(&app.state, &snap(1.0), T0 + 30_000)
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());

    // the SSE endpoint: session-gated event stream
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/alerts/stream")
                .header("cookie", &app.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .starts_with("text/event-stream"));
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/alerts/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "no session → 401");
}

/// `/metrics` SLO v1 end to end: with a mock node exposing a lag series and
/// an enabled lag rule, the console scrape carries
/// `tailrace_slo_lag_target_seconds` under exactly the lag series' label
/// set; a disabled rule contributes no target line; the evaluator sees the
/// same snapshot (node label injected) and fires on it.
#[tokio::test]
async fn metrics_exposes_slo_target_next_to_lag_and_evaluator_sees_same_snapshot() {
    let node = Router::new()
        .route("/healthz", get(|| async { Json(json!({"ok": true})) }))
        .route(
            "/metrics",
            get(|| async {
                (
                    [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                    "# TYPE tailrace_pipeline_lag_seconds gauge\n\
                     tailrace_pipeline_lag_seconds{pipeline=\"3\",statement=\"0\"} 42\n\
                     tailrace_pipeline_lag_seconds{pipeline=\"4\",statement=\"0\"} 1\n",
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, node).await.unwrap() });

    let app = app().await;
    let (s, b) = req(
        &app,
        "POST",
        "/api/nodes/manual",
        Some(json!({"name": "edge-1", "host": "127.0.0.1", "enginePort": port})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let scrape = || async {
        let resp = app
            .router
            .clone()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        String::from_utf8(
            axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    };
    // no rules: lag series proxied, no SLO family at all
    let text = scrape().await;
    assert!(text.contains(
        "tailrace_pipeline_lag_seconds{node=\"edge-1\",pipeline=\"3\",statement=\"0\"} 42\n"
    ));
    assert!(!text.contains("tailrace_slo_lag_target_seconds"));

    let id = create_rule(
        &app,
        json!({"name": "lag p3", "ruleType": "lag_seconds", "params": {"threshold": 30, "pipeline": "3"}}),
    )
    .await;
    let text = scrape().await;
    assert_eq!(
        text.matches("# TYPE tailrace_slo_lag_target_seconds gauge")
            .count(),
        1
    );
    assert!(
        text.contains(
            "tailrace_slo_lag_target_seconds{node=\"edge-1\",pipeline=\"3\",statement=\"0\"} 30\n"
        ),
        "target under the lag series' exact label set: {text}"
    );
    assert!(
        !text.contains("tailrace_slo_lag_target_seconds{node=\"edge-1\",pipeline=\"4\""),
        "series no rule watches gets no target"
    );

    // the evaluator consumes the same gathered snapshot and fires (42 > 30)
    let gathered = tailrace_console::metrics::gather(&app.state).await;
    let samples = tailrace_console::metrics::node_samples(&gathered);
    assert_eq!(samples.len(), 2);
    let notices = Evaluator::new()
        .run_pass(&app.state, &samples, T0)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "firing");
    assert_eq!(notices[0].sample["value"], 42.0);

    // disabling the rule removes its target line
    let (s, _) = req(
        &app,
        "PUT",
        &format!("/api/alerts/rules/{id}"),
        Some(json!({"name": "lag p3", "ruleType": "lag_seconds",
                    "params": {"threshold": 30, "pipeline": "3"}, "enabled": false})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(!scrape().await.contains("tailrace_slo_lag_target_seconds"));
}
