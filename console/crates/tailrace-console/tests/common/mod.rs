//! Shared integration-test plumbing: app assembly (temp artifact dir),
//! session login, JSON request helpers, and a mock engine node serving the
//! /v1 surface the console proxies against.
//!
//! This module is compiled into every test binary; not all of them use the
//! mock engine, hence the blanket dead_code allowance.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use console_auth::{hash_password, BootstrapAuthority};
use console_store::{CredentialCipher, Store};
use serde_json::{json, Value};
use tailrace_console::mcp::McpState;
use tailrace_console::{build_router, build_router_with_mcp, state::AppState};
use tower::ServiceExt;

pub const PASSWORD: &str = "pw-123";

pub struct TestApp {
    pub router: Router,
    pub cookie: String,
    /// Store handle for at-rest assertions (e.g. credential ciphertext).
    pub store: Store,
    /// The app's credential cipher (§5.3) — same instance the handlers use.
    pub cipher: CredentialCipher,
    /// The app's artifact dir (`<data>/artifacts`); T-A2 tests derive the
    /// sibling `<data>/backfill` ledger root from its parent.
    pub artifact_dir: std::path::PathBuf,
}

pub async fn app() -> TestApp {
    app_with_cipher(CredentialCipher::ephemeral()).await
}

/// Same as [`app`] but with a caller-known cipher (lets tests verify the
/// §5.3 seal/open roundtrip against the stored ciphertext).
pub async fn app_with_cipher(cipher: CredentialCipher) -> TestApp {
    build(cipher, None).await
}

/// Same as [`app`] but with the MCP write switch pinned (T-E4 §7.5) —
/// independent of whatever `MCP_ALLOW_WRITE` the developer's shell carries.
pub async fn app_with_mcp(allow_write: bool) -> TestApp {
    app_with_mcp_state(McpState::new(allow_write)).await
}

/// Same as [`app`] with a fully caller-built MCP state (rate-limit budget,
/// write switch).
pub async fn app_with_mcp_state(mcp: McpState) -> TestApp {
    build(CredentialCipher::ephemeral(), Some(mcp)).await
}

async fn build(cipher: CredentialCipher, mcp: Option<McpState>) -> TestApp {
    let store = Store::open_in_memory().unwrap();
    store
        .create_user(
            "admin",
            &hash_password(PASSWORD).unwrap(),
            Some("管理员"),
            "admin",
        )
        .unwrap();
    // Production layout: <data-dir>/artifacts (so the T-A2 backfill root
    // derives as the sibling <data-dir>/backfill). Nesting one level is not
    // cosmetic — a bare temp-dir artifact_dir would collapse EVERY app's
    // backfill root onto the same <temp>/backfill and same-day job ids
    // (separate stores, identical "{yyyymmdd}-01" slugs) would share one
    // bf- ledger dir.
    let artifact_dir = std::env::temp_dir().join(format!(
        "tailrace-console-test-{}-{}/artifacts",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ));
    let state = AppState::new_full(
        store.clone(),
        BootstrapAuthority::new("tok-secret"),
        artifact_dir.clone(),
        cipher.clone(),
    );
    let router = match mcp {
        Some(m) => build_router_with_mcp(state, m),
        None => build_router(state),
    };
    let cookie = login(&router).await;
    TestApp {
        router,
        cookie,
        store,
        cipher,
        artifact_dir: artifact_dir.clone(),
    }
}

pub async fn login(router: &Router) -> String {
    let resp = router
        .clone()
        .oneshot(
            Request::post("/api/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"username": "admin", "password": PASSWORD}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

pub async fn req(
    app: &TestApp,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let builder = match method {
        "GET" => Request::get(path),
        "POST" => Request::post(path),
        "PUT" => Request::put(path),
        "DELETE" => Request::delete(path),
        other => panic!("method {other}"),
    };
    let request = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .header("cookie", &app.cookie)
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => builder
            .header("cookie", &app.cookie)
            .body(Body::empty())
            .unwrap(),
    };
    let resp = app.router.clone().oneshot(request).await.unwrap();
    let status = resp.status();
    (status, json_body(resp).await)
}

pub async fn raw_request(
    app: &TestApp,
    method: &str,
    path: &str,
    headers: HashMap<&str, String>,
    body: Vec<u8>,
) -> (StatusCode, Value, String) {
    let builder = match method {
        "GET" => Request::get(path),
        "POST" => Request::post(path),
        other => panic!("method {other}"),
    };
    let mut b = builder.header("cookie", &app.cookie);
    for (k, v) in headers {
        b = b.header(k, v);
    }
    let resp = app
        .router
        .clone()
        .oneshot(b.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    (status, json_body(resp).await, ct)
}

pub async fn json_body(resp: Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&bytes)
        .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
}

// ---------------- mock engine node ----------------

/// (seq, level, kind, message)
type LogRow = (u64, &'static str, &'static str, &'static str);

#[derive(Clone, Default)]
pub struct MockEngineState {
    pub plans: Arc<Mutex<Vec<Value>>>,
    pub actions: Arc<Mutex<Vec<String>>>,
    /// logs: (seq, level, kind, message)
    pub logs: Arc<Mutex<Vec<LogRow>>>,
    pub reject_plans: Arc<Mutex<bool>>,
    /// Canonical query string ("k=v&…" sorted) of every /v1/pipelines/{id}/logs
    /// hit — lets tests assert how the console bridge drives the cursor API.
    pub log_queries: Arc<Mutex<Vec<String>>>,
}

/// Spawn a mock engine on an ephemeral loopback port; returns its state
/// handle and port.
pub async fn spawn_mock_engine() -> (MockEngineState, u16) {
    let st = MockEngineState::default();
    *st.logs.lock().unwrap() = vec![
        (1, "INFO", "PIPELINE_STARTED", "assembled 1 statement"),
        (2, "INFO", "FLUSH_OK", "flush 10000 rows in 812ms"),
        (3, "WARN", "LAG_HIGH", "consumer lag 12000"),
    ];
    let app = mock_router(st.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (st, port)
}

fn mock_router(st: MockEngineState) -> Router {
    let st_list = st.clone();
    let st_plan = st.clone();
    let st_logs = st.clone();
    let st_action = st.clone();
    let _ = (&st_list, &st_plan, &st_logs, &st_action);
    Router::new()
        .route("/healthz", get(|| async { Json(json!({"ok": true})) }))
        .route(
            "/v1/pipelines",
            get(move || {
                let st = st_list.clone();
                async move {
                    let n = st.plans.lock().unwrap().len();
                    Json(json!({
                        "version": "0.1.0-mock",
                        "pipelines": (0..n).map(|_| json!({
                            "id": 42, "status": "RUNNING", "statementCount": 1,
                            "statements": [], "error": null
                        })).collect::<Vec<_>>()
                    }))
                }
            }),
        )
        .route(
            "/v1/pipelines/{id}/plan",
            put(move |axum::extract::Path(id): axum::extract::Path<u64>, body: String| {
                let st = st_plan.clone();
                async move {
                    let v: Value = serde_json::from_str(&body).unwrap();
                    if *st.reject_plans.lock().unwrap() {
                        return Json(json!({"ok": false, "error": "mock rejection: table missing"}));
                    }
                    let n = v.get("tables").and_then(|t| t.as_array()).map(|a| a.len()).unwrap_or(0);
                    st.plans.lock().unwrap().push(v);
                    Json(json!({"ok": true, "pipelineId": id, "statements": n, "mode": "sql-compiled+serve-executing"}))
                }
            }),
        )
        .route(
            "/v1/pipelines/{id}/stats",
            get(move |axum::extract::Path(id): axum::extract::Path<u64>| async move {
                Json(json!({
                    "ok": true,
                    "stats": {"pipelineId": id, "status": "RUNNING",
                              "totals": {"consumed": 128000, "flushed": 127996, "skipped": 0, "bad": 0, "flushes": 12, "lastFlushMs": 812},
                              "statements": []}
                }))
            }),
        )
        .route(
            "/v1/pipelines/{id}/logs",
            get(move |axum::extract::Path(id): axum::extract::Path<u64>,
                  axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>| {
                let st = st_logs.clone();
                async move {
                    let after: u64 = q.get("after_seq").and_then(|v| v.parse().ok()).unwrap_or(0);
                    let limit = q.get("limit").and_then(|v| v.parse::<usize>().ok()).unwrap_or(200);
                    let is_tail = q.get("tail").map(|v| v == "1").unwrap_or(false);
                    {
                        let mut qs: Vec<String> =
                            q.iter().map(|(k, v)| format!("{k}={v}")).collect();
                        qs.sort();
                        st.log_queries.lock().unwrap().push(qs.join("&"));
                    }
                    let all = st.logs.lock().unwrap().clone();
                    let (entries, last_seq) = if is_tail && after == 0 {
                        let n = limit.min(all.len());
                        let take = &all[all.len() - n..];
                        (take.to_vec(), take.last().map(|e| e.0).unwrap_or(0))
                    } else {
                        let take: Vec<_> = all.iter().filter(|e| e.0 > after).take(limit).cloned().collect();
                        let last = take.last().map(|e| e.0).unwrap_or(after);
                        (take, last)
                    };
                    let items: Vec<Value> = entries
                        .into_iter()
                        .map(|(seq, level, kind, message)| {
                            json!({"seq": seq, "tsMs": seq * 1000, "level": level, "kind": kind, "message": message, "statementIdx": 0})
                        })
                        .collect();
                    let _ = id;
                    Json(json!({"ok": true, "entries": items, "lastSeq": last_seq}))
                }
            }),
        )
        .route(
            "/v1/pipelines/{id}/offsets",
            get(move |axum::extract::Path(id): axum::extract::Path<u64>| async move {
                Json(json!({
                    "ok": true, "pipelineId": id, "deployed": true,
                    "statements": [{"statement": "0", "ledger": {
                        "cursor": {"Kafka": {"topic": "orders_ogg", "group": format!("tr.p{id}.s0"), "offsets": {"0": 7}}},
                        "lastAck": format!("p{id}-r1_0"),
                        "totals": {"consumed": 10, "flushed": 9, "flushes": 2}}}]
                }))
            }),
        )
        .route(
            "/v1/pipelines/{id}/{action}",
            post(move |axum::extract::Path((id, action)): axum::extract::Path<(u64, String)>| {
                let st = st_action.clone();
                async move {
                    st.actions.lock().unwrap().push(action.clone());
                    let status = match action.as_str() {
                        "pause" => "PAUSED",
                        "resume" => "RUNNING",
                        "stop" => "STOPPED",
                        _ => "RUNNING",
                    };
                    Json(json!({"ok": true, "pipelineId": id, "status": status}))
                }
            }),
        )
}

/// Manually register a mock engine node through the console API and flip it
/// ONLINE via one m2m heartbeat; returns (node_id, node_key, node_token).
pub async fn online_node(app: &TestApp, port: u16, name: &str) -> (i64, String, String) {
    let (status, body) = req(
        app,
        "POST",
        "/api/nodes/manual",
        Some(json!({"name": name, "host": "127.0.0.1", "enginePort": port})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "manual register failed: {body}");
    let id = body["id"].as_i64().unwrap();
    let node_key = body["nodeKey"].as_str().unwrap().to_string();
    let token = body["bootstrapToken"].as_str().unwrap().to_string();
    // heartbeat -> ONLINE
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::post("/api/nodes/heartbeat")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "nodeKey": node_key, "bootstrapToken": token, "name": name,
                        "apiUrl": format!("http://127.0.0.1:{port}"), "engineVersion": "1.0.0",
                        "capacityJson": "{\"pipelines\":8,\"statements\":24,\"memory_mb\":2048}"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    (id, node_key, token)
}

/// Send one m2m heartbeat with pipeline totals piggybacked (§4.3
/// pipelineStates → pipeline_metrics).
pub async fn heartbeat_with_totals(app: &TestApp, node_key: &str, token: &str, pid: i64) {
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::post("/api/nodes/heartbeat")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "nodeKey": node_key, "bootstrapToken": token,
                        "pipelineStates": [ {"id": pid, "state": "RUNNING",
                                             "totals": {"in": 1000, "out": 990, "flushes": 7}} ],
                        "resources": {"rssBytes": 91226112, "cpuPercent": 7.2, "rxBps": 20480, "txBps": 15360}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ---------------- mock Doris FE (MySQL wire protocol) ----------------

/// A mock Doris FE speaking just enough of the MySQL wire protocol for the
/// sqlx client tailrace-metadata embeds: protocol-41 greeting with
/// mysql_native_password and no TLS (the console connects with
/// ssl_mode=Disabled, and this server advertises no extra capabilities so
/// the negotiated set drops DEPRECATE_EOF — classic EOF-framed text
/// resultsets). `SHOW CREATE TABLE` is answered from the scripted DDL text
/// (the LOW-5 live key-model gate's dependency); every other query gets an
/// ERR packet naming the refusal, so FE-side failure paths (the deploy
/// precheck's SHOW ALTER TABLE COLUMN, evolve's DESC, ddl execute) keep
/// their 502s against this server instead of an unreachable port. Queries
/// are recorded for assertions.
#[derive(Clone, Default)]
pub struct MockFe {
    pub queries: Arc<Mutex<Vec<String>>>,
    ddl: Arc<Mutex<String>>,
}

impl MockFe {
    /// Swap the SHOW CREATE TABLE answer of a running server (one verdict
    /// per publish-gate scenario without re-wiring datasources).
    pub fn set_ddl(&self, ddl: &str) {
        *self.ddl.lock().unwrap() = ddl.to_string();
    }

    /// Serve on an ephemeral loopback port; `ddl` is what SHOW CREATE TABLE
    /// returns for any db.table. Returns the port (jdbcUrl
    /// `jdbc:mysql://127.0.0.1:<port>/shop`).
    pub async fn serve(self, ddl: &str) -> u16 {
        *self.ddl.lock().unwrap() = ddl.to_string();
        let fe = self.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let fe = fe.clone();
                tokio::spawn(async move { fe.session(socket).await });
            }
        });
        port
    }

    /// One connection: greeting → (any) handshake response → OK → COM_QUERY
    /// loop. Sequence numbers follow the wire rule: replies continue from
    /// the last received packet's id + 1.
    async fn session(self, mut socket: tokio::net::TcpStream) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // greeting: protocol 10, version, 20-byte seed split across the two
        // auth-plugin-data fields, capabilities = PROTOCOL_41 |
        // SECURE_CONNECTION | PLUGIN_AUTH only (no SSL, no DEPRECATE_EOF),
        // mysql_native_password
        let mut greeting = Vec::new();
        greeting.push(0x0a);
        greeting.extend_from_slice(b"8.0.32-mock\0");
        greeting.extend_from_slice(&1u32.to_le_bytes());
        greeting.extend_from_slice(b"12345678");
        greeting.push(0x00);
        greeting.extend_from_slice(&0x8200u16.to_le_bytes()); // caps low
        greeting.push(0x21); // charset
        greeting.extend_from_slice(&0x0002u16.to_le_bytes()); // AUTOCOMMIT
        greeting.extend_from_slice(&0x0008u16.to_le_bytes()); // caps high
        greeting.push(21); // auth-plugin-data length
        greeting.extend_from_slice(&[0u8; 10]);
        greeting.extend_from_slice(b"123456789012\0");
        greeting.extend_from_slice(b"mysql_native_password\0");
        let mut seq = 0u8;
        write_frame(&mut socket, seq, &greeting).await;
        // handshake response (auth scramble whatever) → accept
        let Some((_payload, s)) = read_frame(&mut socket).await else {
            return;
        };
        seq = s + 1;
        write_frame(
            &mut socket,
            seq,
            &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00],
        )
        .await; // OK
        loop {
            let Some((payload, s)) = read_frame(&mut socket).await else {
                return;
            };
            if payload.first() != Some(&0x03) {
                // not COM_QUERY (ping etc.) → OK
                write_frame(
                    &mut socket,
                    s + 1,
                    &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00],
                )
                .await;
                continue;
            }
            let sql = String::from_utf8_lossy(&payload[1..]).to_string();
            self.queries.lock().unwrap().push(sql.clone());
            let mut seq = s + 1;
            let upper = sql.to_ascii_uppercase();
            if upper.starts_with("SHOW CREATE TABLE") {
                // "… `db`.`table`" → the segment before the trailing backtick
                let table = sql
                    .trim()
                    .strip_suffix('`')
                    .and_then(|rest| rest.rsplit('`').next())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("t")
                    .to_string();
                let ddl = self.ddl.lock().unwrap().clone();
                // resultset: 2 columns, 1 row, EOF-framed (no DEPRECATE_EOF)
                write_frame(&mut socket, seq, &[0x02]).await;
                seq += 1;
                write_frame(&mut socket, seq, &column_def("Table")).await;
                seq += 1;
                write_frame(&mut socket, seq, &column_def("Create Table")).await;
                seq += 1;
                write_frame(&mut socket, seq, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;
                seq += 1;
                let mut row = Vec::new();
                row.extend(lenenc_str(&table));
                row.extend(lenenc_str(&ddl));
                write_frame(&mut socket, seq, &row).await;
                seq += 1;
                write_frame(&mut socket, seq, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;
            } else if upper.starts_with("SET ") {
                // sqlx's connect-time session setup
                // (`SET time_zone='+00:00',NAMES utf8mb4`) — accept silently
                write_frame(
                    &mut socket,
                    seq,
                    &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00],
                )
                .await;
            } else {
                let msg = format!("mock FE: only SHOW CREATE TABLE is scripted, got: {sql}");
                let mut err = vec![0xff];
                err.extend_from_slice(&1064u16.to_le_bytes()); // ER_PARSE_ERROR
                err.push(b'#');
                err.extend_from_slice(b"HY000");
                err.extend_from_slice(msg.as_bytes());
                write_frame(&mut socket, seq, &err).await;
            }
        }

        async fn write_frame(socket: &mut tokio::net::TcpStream, seq: u8, payload: &[u8]) {
            let mut frame = Vec::with_capacity(payload.len() + 4);
            frame.extend_from_slice(&(payload.len() as u32).to_le_bytes()[..3]);
            frame.push(seq);
            frame.extend_from_slice(payload);
            let _ = socket.write_all(&frame).await;
            let _ = socket.flush().await;
        }

        async fn read_frame(socket: &mut tokio::net::TcpStream) -> Option<(Vec<u8>, u8)> {
            let mut header = [0u8; 4];
            socket.read_exact(&mut header).await.ok()?;
            let len =
                (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
            let mut payload = vec![0u8; len];
            socket.read_exact(&mut payload).await.ok()?;
            Some((payload, header[3]))
        }

        /// lenenc string bytes (handles the ≥251-byte two-byte length form
        /// a real SHOW CREATE text needs).
        fn lenenc_str(s: &str) -> Vec<u8> {
            let b = s.as_bytes();
            let mut v = Vec::with_capacity(b.len() + 3);
            if b.len() < 251 {
                v.push(b.len() as u8);
            } else {
                v.push(0xfc);
                v.extend_from_slice(&(b.len() as u16).to_le_bytes());
            }
            v.extend_from_slice(b);
            v
        }

        /// One column-definition payload (protocol 41): the six lenenc
        /// strings (catalog/schema/table_alias/table/alias/org_name — the
        /// result column rides the ALIAS slot), fixed-field length 0x0c,
        /// utf8 charset, VAR_STRING type, no flags, 0 decimals.
        fn column_def(name: &str) -> Vec<u8> {
            let mut p = Vec::new();
            for field in ["def", "", "", "", name, ""] {
                p.extend(lenenc_str(field));
            }
            p.push(0x0c);
            p.extend_from_slice(&33u16.to_le_bytes());
            p.extend_from_slice(&65535u32.to_le_bytes());
            p.push(0xfd); // MYSQL_TYPE_VAR_STRING
            p.extend_from_slice(&0u16.to_le_bytes());
            p.push(0);
            p.extend_from_slice(&[0u8; 2]);
            p
        }
    }
}

/// `seed_datasources` with the doris datasource's jdbcUrl pointed at a mock
/// FE port (LOW-5: publish's live SHOW CREATE TABLE gate needs an
/// answerable FE); 19030 in [`seed_datasources`] stays unreachable by
/// construction.
pub async fn seed_datasources_at_fe(app: &TestApp, fe_port: u16) -> (i64, i64, i64, i64) {
    seed_datasources_with_jdbc(app, fe_port).await
}

/// Register a kafka (source) + doris (sink) datasource pair with their table
/// objects; returns (kafka_ds_id, doris_ds_id, kafka_table_id, doris_table_id).
pub async fn seed_datasources(app: &TestApp) -> (i64, i64, i64, i64) {
    seed_datasources_with_jdbc(app, 19030).await
}

async fn seed_datasources_with_jdbc(app: &TestApp, jdbc_port: u16) -> (i64, i64, i64, i64) {
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
        "/api/datasources",
        Some(json!({
            "name": "shop-doris", "type": "doris",
            "connJson": {"jdbcUrl": format!("jdbc:mysql://127.0.0.1:{jdbc_port}/shop"), "streamLoadUrl": "http://127.0.0.1:18040"},
            "username": "loader", "password": "pw"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let doris = b["id"].as_i64().unwrap();

    let (s, b) = req(
        app,
        "POST",
        &format!("/api/datasources/{kafka}/tables"),
        Some(json!({
            "dbName": "shop", "tableName": "orders", "format": "ogg",
            "columnsJson": [["id", "int64"], ["payload", "utf8"], ["op_type", "utf8"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let kafka_table = b["id"].as_i64().unwrap();
    let (s, b) = req(
        app,
        "POST",
        &format!("/api/datasources/{doris}/tables"),
        Some(json!({
            "dbName": "ods", "tableName": "orders", "format": null,
            "columnsJson": [["id", "int64"], ["payload", "utf8"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let doris_table = b["id"].as_i64().unwrap();
    (kafka, doris, kafka_table, doris_table)
}

pub const PIPELINE_SQL: &str =
    "INSERT INTO ods.orders SELECT id, payload FROM shop.orders WHERE op_type IN ('I','U')";

/// Create + publish a pipeline over the seeded datasources; returns its id.
pub async fn published_pipeline(app: &TestApp, kafka: i64, doris: i64) -> i64 {
    let (s, b) = req(
        app,
        "POST",
        "/api/pipelines",
        Some(json!({
            "name": "orders→ods",
            "sqlContent": PIPELINE_SQL,
            "dsMapping": {"shop": kafka, "ods": doris},
            "checkpointMs": 10000,
            "preferredNodeId": null,
            "config": {"reliability": null, "recovery": null, "tuning": null, "groupPrefix": null, "startOffset": "earliest"}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let id = b["id"].as_i64().unwrap();
    let (s, b) = req(app, "POST", &format!("/api/pipelines/{id}/publish"), None).await;
    assert_eq!(s, StatusCode::OK, "publish failed: {b}");
    id
}
