//! T-A2 backfill control plane (blueprint §7.3 / R6 W4): the console
//! contract — dry-run preview with zero side effects, the capability 422
//! gates (non doris/starrocks sink, non-file source, bounds, statement
//! index), the pending → running → done|failed state machine, deterministic
//! labels across job replays (the F1 fix made observable at the console
//! boundary) and the cursor-isolation assertion (main pipeline row
//! untouched, bf- ledger namespace only).
//!
//! The happy path runs the REAL executor end to end against a stateful mock
//! stream-load BE (first load of a label → Success, replay → the Doris
//! "label already exists" refusal, exactly like the W2 mock); live-Doris
//! e2e (DUPLICATE KEY row-count invariance) is the W6 suite's job.

mod common;

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::put;
use axum::{Json, Router};
use common::*;
use serde_json::{json, Value};
use tower::ServiceExt;

// ---------------- shared fixtures ----------------

/// One per-process directory holding every test's source file (dotless,
/// per-test unique names). The file DATASOURCE is registered with this
/// absolute path, and the process cwd is never changed: the executor must
/// find the file through the registered directory (plan `bootstrap` → the
/// engine file factory joins it with the table name), not through the cwd
/// — the R6 review's silent-zero-rows backfill was exactly a console whose
/// cwd happened to hold no such file.
static SOURCE_DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

fn source_dir() -> &'static std::path::Path {
    SOURCE_DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!(
            "tailrace-console-bf-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ));
        std::fs::create_dir_all(&d).expect("create shared source dir");
        d
    })
}

/// One captured stream-load attempt: (label, Authorization, body row count).
#[derive(Clone)]
struct LoadHit {
    label: String,
    auth: String,
    rows: usize,
}

/// Stateful mock stream-load BE: the first PUT of a label answers Success;
/// every replay of the same label answers the Doris "label already exists"
/// refusal (which the W2 sink maps to FlushOutcome::Dedup).
#[derive(Clone, Default)]
struct MockBe {
    seen: Arc<Mutex<Vec<String>>>,
    hits: Arc<Mutex<Vec<LoadHit>>>,
}

impl MockBe {
    async fn serve(self) -> u16 {
        // (self is cloned into the router; the caller keeps its handle)
        let be = self.clone();
        let app = Router::new().route(
            "/api/{db}/{table}/_stream_load",
            put(move |headers: axum::http::HeaderMap, body: String| {
                let be = be.clone();
                async move {
                    let label = headers
                        .get("label")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    let rows = body.lines().filter(|l| !l.trim().is_empty()).count();
                    be.hits.lock().unwrap().push(LoadHit {
                        label: label.clone(),
                        auth,
                        rows,
                    });
                    let replay = be.seen.lock().unwrap().contains(&label);
                    be.seen.lock().unwrap().push(label.clone());
                    if replay {
                        Json(json!({
                            "Status": "Fail",
                            "Reason": format!("label already exists: {label}")
                        }))
                    } else {
                        Json(json!({"Status": "Success", "Label": label}))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        port
    }
}

/// Seed `<source_dir>/<name>` with `n` equal json lines; returns (file name,
/// total byte length = an exact-EOF `to` bound).
fn seed_source(name: &str, n: usize) -> (String, i64) {
    let mut text = String::new();
    for i in 0..n {
        text.push_str(&format!("{{\"order_no\":\"ORD-{i:04}\",\"qty\":7}}\n"));
    }
    let len = text.len() as i64;
    std::fs::write(source_dir().join(name), text).expect("seed source file");
    (name.to_string(), len)
}

/// file source + doris sink datasources with table objects and a published
/// file→doris pipeline; returns (app, pipeline id, file byte length).
async fn file_to_doris_app(tag: &str, rows: usize, be_port: u16) -> (TestApp, i64, i64) {
    let app = app().await;
    let src_name = format!("bf_src_{tag}");
    let (_, to) = seed_source(&src_name, rows);

    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": format!("bf-files-{tag}"), "type": "file",
            "connJson": {"path": source_dir()}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let file_ds = b["id"].as_i64().unwrap();
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{file_ds}/tables"),
        Some(json!({
            "dbName": "src", "tableName": src_name, "format": "json",
            "columnsJson": [["order_no", "utf8"], ["qty", "int64"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");

    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": format!("bf-doris-{tag}"), "type": "doris",
            "connJson": {
                "jdbcUrl": "jdbc:mysql://127.0.0.1:1/shop",
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
            "columnsJson": [["order_no", "utf8"], ["qty", "int64"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");

    let sql = format!("INSERT INTO ods.orders SELECT order_no, qty FROM src.{src_name}");
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": format!("bf-pipe-{tag}"),
            "sqlContent": sql,
            "dsMapping": {"src": file_ds, "ods": doris_ds},
            "checkpointMs": 10000
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pid = b["id"].as_i64().unwrap();
    let (s, b) = req(&app, "POST", &format!("/api/pipelines/{pid}/publish"), None).await;
    assert_eq!(s, StatusCode::OK, "publish: {b}");
    (app, pid, to)
}

fn bounds(from: i64, to: i64) -> Value {
    json!({
        "statementIndex": 0,
        "from": {"type": "offset", "value": from},
        "to": {"type": "offset", "value": to},
        "dryRun": true
    })
}

async fn post_backfill(app: &TestApp, pid: i64, mut body: Value) -> (StatusCode, Value) {
    body["dryRun"] = Value::Bool(body["dryRun"].as_bool().unwrap_or(false));
    req(
        app,
        "POST",
        &format!("/api/pipelines/{pid}/backfill"),
        Some(body),
    )
    .await
}

/// Poll the jobs endpoint until the job reaches a terminal state (5s cap).
async fn wait_terminal(app: &TestApp, pid: i64, job: &str) -> Value {
    for _ in 0..200 {
        let (s, b) = req(
            app,
            "GET",
            &format!("/api/pipelines/{pid}/backfill/jobs"),
            None,
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{b}");
        if let Some(j) = b["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|j| j["jobId"] == job)
        {
            let st = j["state"].as_str().unwrap_or("");
            if st == "done" || st == "failed" {
                return j.clone();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("job {job} did not reach a terminal state in 5s");
}

fn job_count(app: &TestApp) -> i64 {
    app.store
        .with_conn(|c| Ok(c.query_row("SELECT COUNT(*) FROM backfill_job", [], |r| r.get(0))?))
        .unwrap()
}

fn audit_backfills(app: &TestApp) -> Vec<Value> {
    app.store
        .with_conn(|c| {
            let mut stmt = c
                .prepare("SELECT detail_json FROM audit_log WHERE action = 'PIPELINE_BACKFILL'")
                .unwrap();
            let rows = stmt
                .query_map([], |r| r.get::<_, Option<String>>(0))
                .unwrap();
            let mut out = Vec::new();
            for r in rows {
                let d = r.unwrap();
                out.push(
                    d.and_then(|d| serde_json::from_str(&d).ok())
                        .unwrap_or(Value::Null),
                );
            }
            Ok(out)
        })
        .unwrap()
}

// ---------------- the contract ----------------

/// dryRun: exactly the three preview fields, and NOTHING created — no job
/// row, no audit, no execution.
#[tokio::test]
async fn dryrun_previews_with_zero_side_effects() {
    let be = MockBe::default();
    let port = be.clone().serve().await;
    let (app, pid, to) = file_to_doris_app("dry", 3, port).await;

    let (s, b) = post_backfill(&app, pid, bounds(0, to)).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(b["jobId"].is_null(), "dry run creates nothing: {b}");
    assert!(b["estimatedRows"].is_null(), "file bounds are bytes: {b}");
    assert_eq!(b["labelPrefix"], format!("tr-p{pid}-s0-bf"));
    assert_eq!(
        b.as_object().unwrap().len(),
        3,
        "only the three fields: {b}"
    );

    assert_eq!(job_count(&app), 0, "no job row");
    assert!(audit_backfills(&app).is_empty(), "no audit row");
    assert!(
        be.hits.lock().unwrap().is_empty(),
        "no stream load happened"
    );
    // the preview is repeatable and byte-stable (the label family is
    // job-independent by design — that is the replay-dedup invariant)
    let (_, b2) = post_backfill(&app, pid, bounds(0, to)).await;
    assert_eq!(b["labelPrefix"], b2["labelPrefix"]);
}

/// The task-card 回退门: only doris/starrocks sinks are backfillable (label
/// idempotency lives there); a file sink and a kafka source are both 422'd
/// with explicit dictionary envelopes, and nothing is created.
#[tokio::test]
async fn capability_gates_answer_422() {
    let app = app().await;
    // kafka source + doris sink: the source gate (scalar from cannot address
    // the partition map — an honest refusal, not a silent superset replay)
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let pid = published_pipeline(&app, kafka, doris).await;
    let (s, b) = post_backfill(&app, pid, bounds(0, 100)).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "BACKFILL_SOURCE_UNSUPPORTED");
    assert_eq!(b["error"]["message_key"], "err.backfill.source_unsupported");
    assert!(
        b["error"]["message"].as_str().unwrap().contains("file 源"),
        "explicit copy: {b}"
    );

    // file → file pipeline: the sink gate
    let (_, to) = seed_source("bf_src_gate", 2);
    let (s, bb) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({"name": "bf-files-gate", "type": "file", "connJson": {"path": source_dir()}})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{bb}");
    let fds = bb["id"].as_i64().unwrap();
    for (db, tbl) in [("src", "bf_src_gate"), ("out", "bf_out_gate")] {
        let (s, bb) = req(
            &app,
            "POST",
            &format!("/api/datasources/{fds}/tables"),
            Some(json!({
                "dbName": db, "tableName": tbl, "format": "json",
                "columnsJson": [["order_no", "utf8"], ["qty", "int64"]]
            })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{bb}");
    }
    let (s, bb) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": "bf-pipe-gate",
            "sqlContent": "INSERT INTO out.bf_out_gate SELECT order_no, qty FROM src.bf_src_gate",
            "dsMapping": {"src": fds, "out": fds},
            "checkpointMs": 10000
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{bb}");
    let fpid = bb["id"].as_i64().unwrap();
    let (s, _bb) = req(
        &app,
        "POST",
        &format!("/api/pipelines/{fpid}/publish"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, b) = post_backfill(&app, fpid, bounds(0, to)).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "BACKFILL_SINK_UNSUPPORTED");
    assert_eq!(b["error"]["message_key"], "err.backfill.sink_unsupported");
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("label 幂等"),
        "explicit copy names the mechanism: {b}"
    );
    assert_eq!(job_count(&app), 0);
}

/// Bounds and statement-index gates: unordered bounds, timestamp-typed
/// bounds and an out-of-range index all 422; session auth still applies.
#[tokio::test]
async fn bounds_and_index_gates() {
    let be = MockBe::default();
    let port = be.clone().serve().await;
    let (app, pid, to) = file_to_doris_app("bounds", 2, port).await;

    // session-gated like the rest of /api
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::post(format!("/api/pipelines/{pid}/backfill"))
                .body(Body::from(bounds(0, to).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let mut body = bounds(10, 10);
    let (s, b) = post_backfill(&app, pid, body.clone()).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "BOUNDS_INVALID");

    body = bounds(20, 10);
    let (s, b) = post_backfill(&app, pid, body).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(b["error"]["code"], "BOUNDS_INVALID");

    body = bounds(0, to);
    body["from"] = json!({"type": "timestamp", "value": "2026-10-01T00:00:00Z"});
    let (s, b) = post_backfill(&app, pid, body).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "BOUNDS_TYPE_UNSUPPORTED");
    assert_eq!(b["error"]["message_key"], "err.backfill.bounds_type");

    body = bounds(0, to);
    body["statementIndex"] = json!(5);
    let (s, b) = post_backfill(&app, pid, body).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["error"]["code"], "STATEMENT_INDEX_RANGE");

    // 404 for an unknown pipeline
    let (s, b) = post_backfill(&app, 999, bounds(0, to)).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{b}");
    assert_eq!(job_count(&app), 0);
}

/// The core: execute → pending → running → done with the exact row count;
/// replay the SAME range under a NEW job → byte-identical labels → the mock
/// BE answers "label already exists" → Dedup, no new committed loads. And
/// the console-side cursor-isolation assertion: the pipeline row is
/// untouched and the replay ledger lives only under bf-{jobId}/.
#[tokio::test]
async fn execute_state_machine_replay_labels_and_cursor_isolation() {
    let be = MockBe::default();
    let port = be.clone().serve().await;
    let (app, pid, to) = file_to_doris_app("exec", 5, port).await;

    let before = app.store.pipeline_full(pid).unwrap().unwrap();
    let before_row = format!(
        "{:?}",
        (
            before.base.status.clone(),
            before.base.version,
            before.base.updated_at.clone(),
            before.base.checkpoint_ms,
        )
    );

    let mut body = bounds(0, to);
    body["dryRun"] = json!(false);
    let (s, b) = post_backfill(&app, pid, body).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{b}");
    assert_eq!(b["state"], "pending");
    let job1 = b["jobId"].as_str().unwrap().to_string();
    assert_eq!(b["labelPrefix"], format!("tr-p{pid}-s0-bf"));

    // acceptance is audited (detail carries job + label family)
    let audits = audit_backfills(&app);
    assert_eq!(audits.len(), 1, "{audits:?}");
    assert_eq!(audits[0]["jobId"], job1.as_str());
    assert_eq!(audits[0]["dryRun"], false);

    let done = wait_terminal(&app, pid, &job1).await;
    assert_eq!(done["state"], "done", "{done}");
    assert_eq!(
        done["rowsWritten"], 5,
        "exactly the rows below `to`: {done}"
    );
    assert!(done["finishedAt"].as_str().is_some());

    // the load carried the sealed credential as the Basic header
    {
        let hits = be.hits.lock().unwrap().clone();
        assert!(!hits.is_empty(), "the executor really stream-loaded");
        use base64::Engine as _;
        let want = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("loader:pw")
        );
        assert_eq!(hits[0].auth, want, "sealed credential became the BE auth");
        assert_eq!(hits[0].rows, 5);
    }
    let labels1: Vec<String> = be
        .hits
        .lock()
        .unwrap()
        .iter()
        .map(|h| h.label.clone())
        .collect();

    // ---------- replay: a NEW job of the SAME range ----------
    let mut replay = bounds(0, to);
    replay["dryRun"] = json!(false);
    let (s, b) = post_backfill(&app, pid, replay).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{b}");
    let job2 = b["jobId"].as_str().unwrap().to_string();
    assert_ne!(job1, job2);
    let done2 = wait_terminal(&app, pid, &job2).await;
    assert_eq!(done2["state"], "done", "{done2}");

    // every replay label is byte-identical to the first run's (the F1 fix):
    // the mock answered "label already exists" for each — Dedup, zero new
    // committed loads, while the job still reports the replayed row count
    let all = be.hits.lock().unwrap().clone();
    let labels2: Vec<String> = all[hits_after(&all, &job1)..]
        .iter()
        .map(|h| h.label.clone())
        .collect();
    assert!(!labels2.is_empty());
    assert_eq!(
        labels1.iter().map(String::as_str).collect::<Vec<_>>(),
        labels2.iter().map(String::as_str).collect::<Vec<_>>(),
        "same range across jobs → same labels (job-id-independent family)"
    );
    assert_eq!(
        all.iter().filter(|h| labels2.contains(&h.label)).count(),
        labels1.len() * 2,
        "each label loaded exactly twice (original + replay)"
    );

    // ---------- cursor isolation ----------
    // (a) the pipeline row is byte-stable: a backfill — success or failure —
    //     never writes the main pipeline's console state
    let after = app.store.pipeline_full(pid).unwrap().unwrap();
    let after_row = format!(
        "{:?}",
        (
            after.base.status.clone(),
            after.base.version,
            after.base.updated_at.clone(),
            after.base.checkpoint_ms,
        )
    );
    assert_eq!(before_row, after_row, "main pipeline row untouched");

    // (b) the replay ledger lives ONLY under <data>/backfill/bf-{jobId}/ —
    //     one namespace per job, nothing at the root, and the engine-node
    //     main ledger dir is a different filesystem entirely (by design)
    let bf_root = app.artifact_dir.parent().unwrap().join("backfill");
    let entries: Vec<String> = std::fs::read_dir(&bf_root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries.len(), 2, "one bf- dir per job: {entries:?}");
    for job in [&job1, &job2] {
        let dir = bf_root.join(format!("bf-{job}"));
        let ledger = dir.join(format!("offsets-{pid}.json"));
        assert!(ledger.exists(), "bf ledger at {}: ", ledger.display());
    }
    assert!(
        !bf_root.join(format!("offsets-{pid}.json")).exists(),
        "no main-shaped ledger at the bf root"
    );

    // the jobs endpoint lists both, newest first
    let (s, b) = req(
        &app,
        "GET",
        &format!("/api/pipelines/{pid}/backfill/jobs"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let jobs = b["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0]["jobId"], job2.as_str());
    assert_eq!(jobs.iter().filter(|j| j["state"] == "done").count(), 2);
}

/// First load index of the second job's hits: everything after the first
/// job's labels were seen again (mock is strictly append-ordered).
fn hits_after(all: &[LoadHit], _job1: &str) -> usize {
    // first job's hits are the leading labels1 block; its length = half the
    // total (each label appears exactly twice)
    all.len() / 2
}

/// A failing flush (the BE answers a terminal client error) lands the job
/// in `failed` with the engine text — and STILL leaves the pipeline row
/// untouched (失败也不动主管道游标).
#[tokio::test]
async fn execute_failure_marks_job_failed_and_isolates_cursors() {
    // a BE that always answers a deterministic client error → the sink
    // classifies it fatal (no retry wait)
    let app_fail = MockFailBe::serve().await;
    let (app, pid, to) = file_to_doris_app("fail", 3, app_fail).await;

    let before = app
        .store
        .pipeline_full(pid)
        .unwrap()
        .unwrap()
        .base
        .status
        .clone();
    let mut body = bounds(0, to);
    body["dryRun"] = json!(false);
    let (s, b) = post_backfill(&app, pid, body).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{b}");
    let job = b["jobId"].as_str().unwrap().to_string();
    let done = wait_terminal(&app, pid, &job).await;
    assert_eq!(done["state"], "failed", "{done}");
    let err = done["error"].as_str().unwrap_or_default();
    assert!(!err.is_empty(), "failure reason recorded: {done}");
    assert_eq!(done["rowsWritten"], 0);

    // main pipeline row byte-stable through the failure as well
    let after = app.store.pipeline_full(pid).unwrap().unwrap();
    assert_eq!(before, after.base.status);
    assert_eq!(after.base.version, 1, "no version bump");
}

/// Always-400 stream-load BE (client error = fatal, no backoff wait).
struct MockFailBe;

impl MockFailBe {
    async fn serve() -> u16 {
        let app = Router::new().route(
            "/api/{db}/{table}/_stream_load",
            put(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"Status": "Fail", "Reason": "mock BE refusal"})),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        port
    }
}
