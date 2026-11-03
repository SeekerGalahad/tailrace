//! T-E2 alerting contract tests: rules CRUD + audit, the evaluator state
//! machine driven by mock snapshots (threshold boundary, event rows only on
//! transitions, resolved always reachable, disable mid-flight), window
//! rules over mock counter history (incl. restart/gap holds — R9 review
//! fix), per-series episodes and node-unreachable holds, the T-V1 idle hold
//! (lag episodes neither fired nor resolved while the source reports idle),
//! webhook delivery (received / unreachable never fails the pass), the SSE
//! hub, and the `/metrics` SLO target riding the lag series' label set end
//! to end through a mock node. R11 T-V4 adds the SLO history contract:
//! evaluator passes feed the 1h lag sample ring, whose quantiles surface on
//! `GET /api/slo` and `/metrics` recomputable from the raw ring rows, with
//! sliding windows and ring eviction pinned on a deterministic clock.

mod common;

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use common::{app, req, TestApp};
use serde_json::{json, Value};
use tailrace_console::alerting::{parse_samples, Evaluator, Sample, Snapshot};
use tower::ServiceExt;

/// Realistic epoch-ms origin (a 0 origin would let u64 saturation fabricate
/// a window base at the cutoff).
const T0: u64 = 1_700_000_000_000;

/// Poll a condition for up to ~4s (webhook deliveries are spawned — the
/// evaluation pass never blocks on them — so a test observes them
/// eventually rather than inline).
async fn eventually(what: &str, f: impl Fn() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(f(), "condition never met: {what}");
}

fn lag_snapshot(node: &str, pipeline: &str, lag: f64) -> Vec<Sample> {
    parse_samples(
        &format!(
            "# TYPE tailrace_pipeline_lag_seconds gauge\n\
             tailrace_pipeline_lag_seconds{{pipeline=\"{pipeline}\",statement=\"0\"}} {lag}\n"
        ),
        Some(node),
    )
}

/// Lag plus the T-V1 idle gauge for one statement, as a node renders them
/// (same label set); `idle` None omits the idle series (no verdict).
fn lag_idle_snapshot(node: &str, pipeline: &str, lag: f64, idle: Option<u8>) -> Vec<Sample> {
    let mut text =
        format!("tailrace_pipeline_lag_seconds{{pipeline=\"{pipeline}\",statement=\"0\"}} {lag}\n");
    if let Some(i) = idle {
        text.push_str(&format!(
            "tailrace_pipeline_source_idle{{pipeline=\"{pipeline}\",statement=\"0\"}} {i}\n"
        ));
    }
    parse_samples(&text, Some(node))
}

async fn create_rule(app: &TestApp, body: Value) -> i64 {
    let (s, b) = req(app, "POST", "/api/alerts/rules", Some(body)).await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    b["id"].as_i64().unwrap()
}

/// A local webhook receiver recording `x-tailrace-event` + notice JSON per
/// POST. Returns its port and the received list.
async fn spawn_hook() -> (u16, Arc<Mutex<Vec<(String, Value)>>>) {
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
    (port, received)
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

    // series absent while its node still answers (empty snapshot, nothing
    // unreachable) resolves with the reason — a pipeline that stopped
    // exposing lag is not lagging anymore
    let (now, _) = tick(0.0);
    let notices = ev.run_pass(&app.state, &[], now).await.unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "resolved");
    assert_eq!(notices[0].sample["reason"], "series_absent");

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

/// T-V1 idle hold through the driver: an episode opened under real backlog
/// is neither re-fired nor resolved while the source reports idle (the lag
/// gauge keeps climbing on the clock — the positional verdict says nothing
/// is behind the cursor); it resolves on the first Clear once rows flow
/// again. A pipeline idle from the start never opens an episode, however
/// long the silence. Episode rows and the rule's aggregate state are read
/// back through the API to pin the store side of "hold".
#[tokio::test]
async fn evaluator_holds_lag_episodes_while_source_idle() {
    let app = app().await;
    create_rule(
        &app,
        json!({"name": "lag", "ruleType": "lag_seconds", "params": {"threshold": 30}}),
    )
    .await;
    let mut ev = Evaluator::new();
    let mut t = T0;

    // real backlog (idle 0) and lag over threshold → firing
    t += 15_000;
    let notices = ev
        .run_pass(
            &app.state,
            &lag_idle_snapshot("edge-1", "3", 45.0, Some(0)),
            t,
        )
        .await
        .unwrap();
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].event, "firing");
    let episode = notices[0].event_id;

    // caught up: idle 1 while the lag climbs through an hour of silence →
    // no notice of either kind, the one episode stays open
    for lag in [60.0, 300.0, 900.0, 3600.0] {
        t += 15_000;
        let n = ev
            .run_pass(
                &app.state,
                &lag_idle_snapshot("edge-1", "3", lag, Some(1)),
                t,
            )
            .await
            .unwrap();
        assert!(
            n.is_empty(),
            "idle hold must emit nothing at lag {lag}: {n:?}"
        );
    }
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "firing", "held open, not resolved");
    assert_eq!(b["rules"][0]["openEvent"]["id"], episode);
    let events = b["events"].as_array().unwrap();
    assert_eq!(
        events.len(),
        1,
        "no duplicate episode under hold: {events:?}"
    );
    assert_eq!(events[0]["state"], "firing");
    assert_eq!(
        events[0]["sample"]["value"], 45.0,
        "a held tick does not touch the open row's sample"
    );

    // rows flow again (idle 0) and the flush refreshed the lag → resolved
    // on the same row
    t += 15_000;
    let notices = ev
        .run_pass(
            &app.state,
            &lag_idle_snapshot("edge-1", "3", 2.0, Some(0)),
            t,
        )
        .await
        .unwrap();
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].event, "resolved");
    assert_eq!(notices[0].event_id, episode);

    // a pipeline that is idle from its first sample never fires — the
    // "running but no data" pipeline that used to page on lag forever
    for lag in [100.0, 10_000.0, 86_400.0] {
        t += 15_000;
        let n = ev
            .run_pass(
                &app.state,
                &lag_idle_snapshot("edge-1", "4", lag, Some(1)),
                t,
            )
            .await
            .unwrap();
        assert!(
            n.is_empty(),
            "idle-from-start never opens an episode: {n:?}"
        );
    }
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "inactive");
    assert_eq!(
        b["events"].as_array().unwrap().len(),
        1,
        "still only pipeline 3's (resolved) episode"
    );
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
    let (port, received) = spawn_hook().await;

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

    // deliveries are spawned (a black-holed webhook must not stall the
    // pass) — the receiver observes them shortly after
    eventually("webhook receiver got both POSTs", || {
        received.lock().unwrap().len() >= 2
    })
    .await;
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

/// R9 review fix (high): a node whose /metrics could not be fetched does
/// not turn into a false resolved. The firing episode is held while the
/// node is unreachable, continues (same episode) when it returns still
/// violating, and only resolves once the node answers AND the condition
/// cleared.
#[tokio::test]
async fn unreachable_node_holds_open_episodes_instead_of_false_resolving() {
    let app = app().await;
    create_rule(
        &app,
        json!({"name": "lag", "ruleType": "lag_seconds", "params": {"threshold": 30}}),
    )
    .await;
    let mut ev = Evaluator::new();
    let snap = Snapshot {
        samples: lag_snapshot("edge-1", "3", 45.0),
        unreachable_nodes: vec![],
    };
    let notices = ev.run_pass_snapshot(&app.state, &snap, T0).await.unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "firing");
    let firing_event = notices[0].event_id;

    // the node goes dark: its series vanish BECAUSE the scrape failed —
    // the episode holds (no resolved notice, no state flip, same row)
    let dark = Snapshot {
        samples: vec![],
        unreachable_nodes: vec!["edge-1".to_string()],
    };
    let notices = ev
        .run_pass_snapshot(&app.state, &dark, T0 + 15_000)
        .await
        .unwrap();
    assert!(
        notices.is_empty(),
        "a down node is not a recovery: {notices:?}"
    );
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(
        b["rules"][0]["state"], "firing",
        "episode held while unreachable"
    );
    assert_eq!(b["rules"][0]["openEvent"]["id"], firing_event);

    // the node comes back still violating: the SAME episode continues —
    // no duplicate firing, no firing/resolved churn
    let notices = ev
        .run_pass_snapshot(&app.state, &snap, T0 + 30_000)
        .await
        .unwrap();
    assert!(notices.is_empty(), "same episode continues: {notices:?}");
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "firing");
    assert_eq!(b["rules"][0]["openEvent"]["id"], firing_event);

    // back and recovered: resolved on the same row
    let recovered = Snapshot {
        samples: lag_snapshot("edge-1", "3", 3.0),
        unreachable_nodes: vec![],
    };
    let notices = ev
        .run_pass_snapshot(&app.state, &recovered, T0 + 45_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "resolved");
    assert_eq!(notices[0].event_id, firing_event);
    assert_eq!(notices[0].sample["value"], 3.0);
}

/// R9 review fix (medium): deleting a firing rule first closes its open
/// episodes (reason `deleted`) and DELIVERS the resolved notice — the
/// webhook receiver never keeps a firing alert for a rule that no longer
/// exists. The episode rows then go with the rule (CASCADE).
#[tokio::test]
async fn deleting_a_firing_rule_delivers_resolved_notices() {
    let (port, received) = spawn_hook().await;
    let app = app().await;
    let id = create_rule(
        &app,
        json!({"name": "failed", "ruleType": "state_failed",
               "webhookUrl": format!("http://127.0.0.1:{port}/hook")}),
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
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "firing");
    let firing_event = notices[0].event_id;
    eventually("firing POST observed", || {
        !received.lock().unwrap().is_empty()
    })
    .await;

    // DELETE while firing: 204, and the receiver learns the alert is over
    let (s, _) = req(&app, "DELETE", &format!("/api/alerts/rules/{id}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    eventually("resolved-after-delete POST observed", || {
        received
            .lock()
            .unwrap()
            .iter()
            .any(|(h, b)| h == "resolved" && b["eventId"] == firing_event)
    })
    .await;
    let resolved: Vec<(String, Value)> = received
        .lock()
        .unwrap()
        .iter()
        .filter(|(h, _)| h == "resolved")
        .cloned()
        .collect();
    assert_eq!(resolved.len(), 1, "exactly one resolved notice");
    assert_eq!(resolved[0].1["sample"]["reason"], "deleted");
    assert!(
        resolved[0].1["seriesKey"]
            .as_str()
            .unwrap_or_default()
            .contains("pipeline=\"7\""),
        "resolved notice names the series: {:?}",
        resolved[0].1["seriesKey"]
    );
    // the rule and its episode rows are gone together
    let (s, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["rules"].as_array().unwrap().len(), 0);
    assert_eq!(b["events"].as_array().unwrap().len(), 0);
}

/// R9 review fix (medium): a window rule with no usable base sample (a
/// console restart wipes the in-memory history; a sample gap pushes the
/// only base candidate far before the cutoff) HOLDS the current state — a
/// firing episode is not falsely resolved, and a gap-spanning difference
/// does not fire as a window increment.
#[tokio::test]
async fn window_rules_hold_state_across_restart_and_sample_gaps() {
    let app = app().await;
    create_rule(
        &app,
        json!({"name": "dlq", "ruleType": "dlq_window",
               "params": {"threshold": 10, "windowSecs": 60}}),
    )
    .await;
    let dlq = |v: f64| {
        parse_samples(
            &format!("tailrace_dlq_rows_total{{pipeline=\"1\",statement=\"0\"}} {v}\n"),
            Some("edge-1"),
        )
    };

    // fire honestly: base at t=0 (100), latest t=60 (112) → inc 12
    let mut ev = Evaluator::new();
    ev.run_pass(&app.state, &dlq(100.0), T0).await.unwrap();
    let notices = ev
        .run_pass(&app.state, &dlq(112.0), T0 + 60_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "firing");
    assert_eq!(notices[0].sample["increment"], 12.0);
    let firing_event = notices[0].event_id;

    // console restarts (fresh evaluator → no history): the open episode
    // must NOT get a false resolved — no base, verdict Unknown, held
    let mut ev2 = Evaluator::new();
    let notices = ev2
        .run_pass(&app.state, &dlq(112.0), T0 + 75_000)
        .await
        .unwrap();
    assert!(notices.is_empty(), "restart tick holds: {notices:?}");
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "firing", "held across the restart");
    assert_eq!(b["rules"][0]["openEvent"]["id"], firing_event);

    // a 10-minute outage follows (the counter kept growing upstream): the
    // first tick back has no base within the slack — still held, and the
    // gap-spanning difference (55 rows) must NOT fire as a 60s increment
    let notices = ev2
        .run_pass(&app.state, &dlq(167.0), T0 + 675_000)
        .await
        .unwrap();
    assert!(
        notices.is_empty(),
        "gap-spanning increment is not a window increment: {notices:?}"
    );
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "firing");

    // an honest base re-enters the window and the increment is small →
    // the held episode resolves once the rule can truly evaluate again
    let notices = ev2
        .run_pass(&app.state, &dlq(170.0), T0 + 735_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "resolved");
    assert_eq!(notices[0].event_id, firing_event);
    assert_eq!(notices[0].sample["increment"], 3.0);
}

/// R9 review fix (medium): episodes are PER SERIES. One unfiltered rule
/// matching two pipelines: both fire (two notices, two episodes), the
/// first recovery resolves ONLY its own episode (the rule stays firing),
/// the second resolves the rule — and every notice carries the series it
/// belongs to.
#[tokio::test]
async fn episodes_and_notices_are_per_series() {
    let app = app().await;
    create_rule(&app, json!({"name": "failed", "ruleType": "state_failed"})).await;
    let two = |p1: f64, p2: f64| {
        parse_samples(
            &format!(
                "tailrace_pipeline_state{{pipeline=\"1\"}} {p1}\n\
                 tailrace_pipeline_state{{pipeline=\"2\"}} {p2}\n"
            ),
            Some("edge-1"),
        )
    };
    let mut ev = Evaluator::new();
    let notices = ev.run_pass(&app.state, &two(3.0, 3.0), T0).await.unwrap();
    assert_eq!(
        notices.len(),
        2,
        "one firing per violating series: {notices:?}"
    );
    let key = |n: &tailrace_console::alerting::AlertNotice| n.series_key.clone();
    assert!(
        key(&notices[0]).contains("pipeline=\"1\"") || key(&notices[0]).contains("pipeline=\"2\"")
    );
    assert_ne!(key(&notices[0]), key(&notices[1]), "distinct series");
    assert_ne!(
        notices[0].event_id, notices[1].event_id,
        "distinct episodes"
    );
    let ep = |n: &tailrace_console::alerting::AlertNotice| n.event_id;
    let ep1 = if key(&notices[0]).contains("pipeline=\"1\"") {
        ep(&notices[0])
    } else {
        ep(&notices[1])
    };
    let ep2 = if ep1 == ep(&notices[0]) {
        ep(&notices[1])
    } else {
        ep(&notices[0])
    };
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(
        b["rules"][0]["openEvents"].as_array().unwrap().len(),
        2,
        "one open episode per series"
    );

    // pipeline 1 recovers: exactly one resolved — for pipeline 1's series,
    // with pipeline 1's episode id — and the rule stays firing
    let notices = ev
        .run_pass(&app.state, &two(0.0, 3.0), T0 + 15_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].event, "resolved");
    assert_eq!(notices[0].event_id, ep1);
    assert!(notices[0].series_key.contains("pipeline=\"1\""));
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(
        b["rules"][0]["state"], "firing",
        "pipeline 2 still violating"
    );
    assert_eq!(b["rules"][0]["openEvents"].as_array().unwrap().len(), 1);

    // pipeline 2 recovers: the second episode resolves, rule inactive
    let notices = ev
        .run_pass(&app.state, &two(0.0, 0.0), T0 + 30_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event_id, ep2);
    assert!(notices[0].series_key.contains("pipeline=\"2\""));
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"][0]["state"], "inactive");
    let events = b["events"].as_array().unwrap();
    assert_eq!(events.len(), 2, "one episode per series, both resolved");
    assert!(events.iter().all(|e| e["state"] == "resolved"));
}

/// R9 review fix (medium): window thresholds accept 0 — an integer counter
/// with `threshold: 0` fires on ANY single row in the window ("alert the
/// first failed flush", matching the Prometheus example's `> 0`).
#[tokio::test]
async fn window_threshold_zero_fires_on_a_single_row() {
    let app = app().await;
    create_rule(
        &app,
        json!({"name": "flush", "ruleType": "flush_errors_window",
               "params": {"threshold": 0, "windowSecs": 60}}),
    )
    .await;
    let err = |v: f64| {
        parse_samples(
            &format!("tailrace_flush_total{{pipeline=\"1\",result=\"error\"}} {v}\n"),
            Some("edge-1"),
        )
    };
    let mut ev = Evaluator::new();
    ev.run_pass(&app.state, &err(0.0), T0).await.unwrap();
    let notices = ev
        .run_pass(&app.state, &err(1.0), T0 + 60_000)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1, "one row in the window fires: {notices:?}");
    assert_eq!(notices[0].event, "firing");
    assert_eq!(notices[0].sample["increment"], 1.0);
}

/// R9 review fix (low): filters the kind's series can never carry are
/// rejected with the params-invalid envelope instead of silently accepted
/// (a heartbeat rule scoped to a pipeline would never match anything).
#[tokio::test]
async fn impossible_filters_are_rejected() {
    let app = app().await;
    for body in [
        json!({"name": "hb", "ruleType": "heartbeat_age",
               "params": {"threshold": 90, "pipeline": "1"}}),
        json!({"name": "sf", "ruleType": "state_failed",
               "params": {"statement": "0"}}),
    ] {
        let (s, b) = req(&app, "POST", "/api/alerts/rules", Some(body)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
        assert_eq!(b["error"]["message_key"], "err.alert.params_invalid", "{b}");
    }
    let (_, b) = req(&app, "GET", "/api/alerts", None).await;
    assert_eq!(b["rules"].as_array().unwrap().len(), 0);
}

/// R9 review fix (high): the evaluator's snapshot derives heartbeat age
/// from the console's own receive clock (`node.last_heartbeat`), drops the
/// node's self-report, and names the nodes it could not scrape — so a
/// heartbeat_age rule rises exactly when a node goes quiet (a down node
/// cannot report its own silence), and the evaluator can tell "series
/// gone" from "node unreachable".
#[tokio::test]
async fn snapshot_derives_heartbeat_age_from_console_clock_and_flags_unreachable() {
    let app = app().await;
    // two nodes on record: edge-1 last beat 10 minutes ago, edge-2 just now
    app.store
        .with_conn(|c| {
            c.execute(
                "INSERT INTO node (node_key, name, register_mode, status) \
                 VALUES ('sn-1', 'edge-1', 'manual', 'ONLINE')",
                [],
            )?;
            c.execute(
                "UPDATE node SET last_heartbeat = datetime('now', '-10 minutes') \
                 WHERE node_key = 'sn-1'",
                [],
            )?;
            c.execute(
                "INSERT INTO node (node_key, name, register_mode, status) \
                 VALUES ('sn-2', 'edge-2', 'manual', 'ONLINE')",
                [],
            )?;
            c.execute(
                "UPDATE node SET last_heartbeat = datetime('now') WHERE node_key = 'sn-2'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    // edge-1 answers /metrics but self-reports a fresh age (a stuck
    // reporter must not mask a silent node); edge-2 is unreachable
    let gathered = tailrace_console::metrics::Gathered {
        console_text: String::new(),
        blocks: vec![
            (
                "edge-1".to_string(),
                Ok("tailrace_node_heartbeat_age_seconds{node_id=\"sn-1\"} 3\n".to_string()),
            ),
            ("edge-2".to_string(), Err("unreachable: boom".to_string())),
        ],
    };
    let snap = tailrace_console::metrics::snapshot(&gathered, &app.state);
    assert_eq!(snap.unreachable_nodes, vec!["edge-2".to_string()]);
    let hb: Vec<&Sample> = snap
        .samples
        .iter()
        .filter(|s| s.metric == "tailrace_node_heartbeat_age_seconds")
        .collect();
    assert_eq!(
        hb.len(),
        2,
        "one synthesized age per registered node: {hb:?}"
    );
    let by_node = |n: &str| {
        hb.iter()
            .find(|s| s.labels.get("node").map(String::as_str) == Some(n))
            .unwrap()
    };
    let e1 = by_node("edge-1");
    assert_eq!(e1.labels.get("node_id").map(String::as_str), Some("sn-1"));
    assert!(
        (500.0..700.0).contains(&e1.value),
        "console clock (~600s), not the self-report (3): {}",
        e1.value
    );
    let e2 = by_node("edge-2");
    assert!(e2.value < 90.0, "just-beat node is fresh: {}", e2.value);

    // a heartbeat_age rule fires for the silent node on this snapshot
    create_rule(
        &app,
        json!({"name": "hb", "ruleType": "heartbeat_age", "params": {"threshold": 90}}),
    )
    .await;
    let notices = Evaluator::new()
        .run_pass_snapshot(&app.state, &snap, T0)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].event, "firing");
    assert!(notices[0].series_key.contains("node=\"edge-1\""));
}

// ---------------- R11 T-V4: SLO history (lag sample ring) ----------------

/// The console `/metrics` body (open route).
async fn scrape_metrics(app: &TestApp) -> String {
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
}

/// The raw ring rows of one pipeline/statement inside the half-open window
/// (from, to] — read straight from the table, independent of the code
/// under test.
fn ring_values(app: &TestApp, pipeline: &str, statement: &str, from: i64, to: i64) -> Vec<f64> {
    app.store
        .with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT lag_secs FROM slo_lag_sample WHERE pipeline = ?1 AND statement = ?2 \
                 AND ts_ms > ?3 AND ts_ms <= ?4",
            )?;
            let rows = stmt.query_map((pipeline, statement, from, to), |r| r.get::<_, f64>(0))?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .unwrap()
}

/// Independent nearest-rank (percent form, integer ceil): sorted[⌈n·pct/100⌉ − 1].
fn recompute(values: &[f64], pct: usize) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let rank = (v.len() * pct).div_ceil(100).max(1);
    v[rank - 1]
}

/// R11 T-V4 acceptance «p95 有样本支撑可复算»: 20 evaluator passes 15s apart
/// write the ring as a side effect of evaluation; `GET /api/slo` answers
/// p50/p95/p99 + sample count + window, and every quantile is recomputed
/// here from the raw table rows of the reported window (an independent
/// nearest-rank) — equal, and each one a stored sample. Idle passes (T-V1
/// gauge at 1) leave no sample; two nodes reporting one pair in a pass keep
/// the worse lag; `/metrics` serves the same numbers under
/// `{pipeline,statement}` and nothing at all while the ring is empty.
#[tokio::test]
async fn slo_quantiles_recompute_from_the_ring_over_api_and_metrics() {
    let app = app().await;
    let text = scrape_metrics(&app).await;
    assert!(
        !text.contains("tailrace_slo_lag_p95_seconds")
            && !text.contains("tailrace_slo_lag_samples"),
        "empty ring: no SLO quantile family at all (absent ≠ 0): {text}"
    );

    // the HTTP surfaces read the console wall clock: end the passes 1s
    // before it so every sample sits inside the default (1h) window
    let now = tailrace_console::slo::now_ms();
    // pipeline 3: the values 1..=20 in scrambled order (one per pass)
    let lags = [
        7.0, 19.0, 3.0, 12.0, 20.0, 1.0, 15.0, 9.0, 4.0, 18.0, 11.0, 2.0, 16.0, 6.0, 13.0, 8.0,
        17.0, 5.0, 14.0, 10.0,
    ];
    let pass_ts = |i: usize| now - 1_000 - (lags.len() - 1 - i) as u64 * 15_000;
    let mut evaluator = Evaluator::new();
    for (i, lag) in lags.iter().enumerate() {
        let mut samples = lag_idle_snapshot("edge-1", "3", *lag, Some(0));
        // pipeline 4: idle (gauge 1) on even passes → never sampled there
        samples.extend(lag_idle_snapshot(
            "edge-2",
            "4",
            1000.0 + i as f64,
            Some(if i % 2 == 0 { 1 } else { 0 }),
        ));
        if i == lags.len() - 1 {
            // pipeline 5 reported by two nodes in one pass → one sample, max
            samples.extend(lag_snapshot("edge-1", "5", 2.0));
            samples.extend(lag_snapshot("edge-2", "5", 6.0));
        }
        evaluator
            .run_pass(&app.state, &samples, pass_ts(i))
            .await
            .unwrap();
    }

    let (s, b) = req(&app, "GET", "/api/slo", None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["windowSecs"], 3600, "default window = the whole ring");
    assert_eq!(b["retentionSecs"], 3600);
    assert_eq!(
        b["intervalSecs"],
        tailrace_console::alerting::interval_from_env().as_secs()
    );
    let (from, to) = (b["fromMs"].as_i64().unwrap(), b["toMs"].as_i64().unwrap());
    assert_eq!(to - from, 3_600_000);
    assert!(to as u64 >= now, "window ends at the request clock");
    let series = b["series"].as_array().unwrap();
    assert_eq!(
        series
            .iter()
            .map(|s| s["pipeline"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["3", "4", "5"]
    );
    for s in series {
        let (p, st) = (
            s["pipeline"].as_str().unwrap(),
            s["statement"].as_str().unwrap(),
        );
        let values = ring_values(&app, p, st, from, to);
        assert_eq!(
            s["samples"],
            values.len(),
            "sample count = ring rows ({p}/{st})"
        );
        for (key, pct) in [("p50", 50), ("p95", 95), ("p99", 99)] {
            let reported = s[key].as_f64().unwrap();
            assert_eq!(
                reported,
                recompute(&values, pct),
                "{key} of {p}/{st} recomputes"
            );
            assert!(values.contains(&reported), "{key} is a stored sample");
        }
    }
    // the hand-checkable expectations behind the recomputation
    let p3 = &series[0];
    assert_eq!(
        (
            p3["samples"].as_u64(),
            p3["p50"].as_f64(),
            p3["p95"].as_f64(),
            p3["p99"].as_f64()
        ),
        (Some(20), Some(10.0), Some(19.0), Some(20.0)),
        "1..=20 → nearest-rank p50 10 / p95 19 / p99 20"
    );
    assert_eq!(p3["node"], "edge-1");
    assert_eq!(p3["oldestMs"], pass_ts(0));
    assert_eq!(p3["newestMs"], pass_ts(lags.len() - 1));
    let p4 = &series[1];
    assert_eq!(p4["samples"], 10, "the 10 idle passes left no sample");
    assert_eq!(
        (p4["p50"].as_f64(), p4["p95"].as_f64(), p4["p99"].as_f64()),
        (Some(1009.0), Some(1019.0), Some(1019.0))
    );
    let p5 = &series[2];
    assert_eq!(
        (
            p5["samples"].as_u64(),
            p5["p95"].as_f64(),
            p5["node"].as_str()
        ),
        (Some(1), Some(6.0), Some("edge-2")),
        "two nodes, one pass → the worse lag, one sample"
    );

    // filters narrow by label value; the window query is clamped 1..=3600
    let (_, b) = req(&app, "GET", "/api/slo?pipeline=3", None).await;
    assert_eq!(b["series"].as_array().unwrap().len(), 1);
    let (_, b) = req(&app, "GET", "/api/slo?pipeline=3&statement=1", None).await;
    assert!(b["series"].as_array().unwrap().is_empty());
    let (_, b) = req(&app, "GET", "/api/slo?windowSecs=99999", None).await;
    assert_eq!(b["windowSecs"], 3600);
    let (_, b) = req(&app, "GET", "/api/slo?windowSecs=abc", None).await;
    assert_eq!(b["windowSecs"], 3600);
    let (_, b) = req(&app, "GET", "/api/slo?windowSecs=0", None).await;
    assert_eq!(b["windowSecs"], 1);
    assert!(
        b["series"].as_array().unwrap().is_empty(),
        "the newest pass sits 1s back — outside a 1s window ending now"
    );

    // /metrics: the same numbers, labelled {pipeline,statement} (no node)
    let text = scrape_metrics(&app).await;
    for family in [
        "tailrace_slo_lag_p50_seconds",
        "tailrace_slo_lag_p95_seconds",
        "tailrace_slo_lag_p99_seconds",
        "tailrace_slo_lag_samples",
    ] {
        assert_eq!(
            text.matches(&format!("# TYPE {family} gauge\n")).count(),
            1,
            "{family} declared once"
        );
    }
    for s in series {
        let labels = format!(
            "{{pipeline=\"{}\",statement=\"{}\"}}",
            s["pipeline"].as_str().unwrap(),
            s["statement"].as_str().unwrap()
        );
        for (family, key) in [
            ("tailrace_slo_lag_p50_seconds", "p50"),
            ("tailrace_slo_lag_p95_seconds", "p95"),
            ("tailrace_slo_lag_p99_seconds", "p99"),
        ] {
            let line = format!("{family}{labels} {}\n", s[key].as_f64().unwrap());
            assert!(text.contains(&line), "missing {line:?} in {text}");
        }
        let line = format!("tailrace_slo_lag_samples{labels} {}\n", s["samples"]);
        assert!(text.contains(&line), "missing {line:?} in {text}");
    }
}

/// R11 T-V4 acceptance «窗口滑动 + 裁剪生效» (deterministic clock): the
/// window is (now − windowSecs, now] — as `now` advances, the oldest
/// samples leave it (a large early lag stops deciding p95 without any
/// prune); each pass evicts the ring tail older than 1h in its own batch,
/// a pass that reports no lag at all still evicts, and the boot sweep drops
/// what a downtime left behind (idempotent).
#[tokio::test]
async fn slo_window_slides_and_ring_prunes_on_write_and_at_startup() {
    use tailrace_console::slo::{startup_prune, window_summaries, RETENTION_SECS};
    let app = app().await;
    let mut evaluator = Evaluator::new();
    let pass = |lag: f64| lag_snapshot("edge-1", "3", lag);
    let rows = |app: &TestApp| -> Vec<(i64, f64)> {
        app.store
            .with_conn(|c| {
                let mut stmt =
                    c.prepare("SELECT ts_ms, lag_secs FROM slo_lag_sample ORDER BY ts_ms")?;
                let r = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
                Ok(r.collect::<Result<Vec<_>, _>>()?)
            })
            .unwrap()
    };
    let t = |offset: i64| (T0 as i64 + offset) as u64;

    // 58m20s back: a 500s lag, still inside the 1h ring
    evaluator
        .run_pass(&app.state, &pass(500.0), t(-3_500_000))
        .await
        .unwrap();
    for (offset, lag) in [(-45_000, 4.0), (-30_000, 1.0), (-15_000, 3.0), (0, 2.0)] {
        evaluator
            .run_pass(&app.state, &pass(lag), t(offset))
            .await
            .unwrap();
    }
    assert_eq!(rows(&app).len(), 5, "nothing older than 1h yet");

    let at = |now: u64, window: u64| {
        let v = window_summaries(&app.store, now, window, Some("3"), Some("0")).unwrap();
        assert!(v.len() <= 1);
        v.into_iter()
            .next()
            .map(|s| (s.samples, s.p50, s.p95, s.p99, s.oldest_ms))
    };
    // last minute: [4,1,3,2] → p50 2, p95/p99 4
    assert_eq!(at(t(0), 60), Some((4, 2.0, 4.0, 4.0, T0 as i64 - 45_000)));
    // whole ring: the 500 joins and decides p95/p99
    assert_eq!(
        at(t(0), RETENTION_SECS),
        Some((5, 3.0, 500.0, 500.0, T0 as i64 - 3_500_000))
    );
    // slide 30s: (T0−30s, T0+30s] — the samples at −45s and −30s (the
    // boundary is exclusive) leave the window
    assert_eq!(
        at(t(30_000), 60),
        Some((2, 2.0, 3.0, 3.0, T0 as i64 - 15_000))
    );
    // slide the hour 200s: the 500 leaves the window before any prune
    assert_eq!(
        at(t(200_000), RETENTION_SECS),
        Some((4, 2.0, 4.0, 4.0, T0 as i64 - 45_000))
    );
    assert!(
        rows(&app).iter().any(|(_, lag)| *lag == 500.0),
        "not pruned yet"
    );
    // a window with nothing in it summarizes to nothing
    assert_eq!(at(t(600_000), 60), None);

    // ring eviction rides the next pass: cutoff T0+200s−1h removes the 500
    evaluator
        .run_pass(&app.state, &pass(6.0), t(200_000))
        .await
        .unwrap();
    let left = rows(&app);
    assert_eq!(left.len(), 5);
    assert!(
        left.iter()
            .all(|(ts, _)| *ts > T0 as i64 + 200_000 - 3_600_000),
        "every surviving row is inside the ring: {left:?}"
    );
    assert!(!left.iter().any(|(_, lag)| *lag == 500.0));

    // a pass that reports no lag series still evicts the tail
    evaluator
        .run_pass(&app.state, &[], t(30_000 + 3_600_000))
        .await
        .unwrap();
    assert_eq!(
        rows(&app),
        vec![(T0 as i64 + 200_000, 6.0)],
        "only the sample younger than the empty pass' cutoff survives"
    );

    // console restarts 2h after its last write: the boot sweep drops the tail
    let boot = t(200_000 + 3_600_000 + 7_200_000);
    assert_eq!(startup_prune(&app.store, boot).unwrap(), 1);
    assert!(rows(&app).is_empty());
    assert_eq!(startup_prune(&app.store, boot).unwrap(), 0, "idempotent");
    assert_eq!(at(boot, RETENTION_SECS), None);
}
