//! tailrace-api: axum REST (the console's only entry into a node) —
//! /healthz + the /v1 surface: compile, pipelines (list/plan/stats/logs/
//! offsets/actions). Every /v1 route sits behind the shared node-token
//! middleware (auth — upgrade plan §二 engine H1).
//! The heartbeat module: identity/events/graceful-shutdown when running with
//! --control-plane.

pub mod auth;
pub mod heartbeat;

use std::sync::{Arc, RwLock};

/// Node runtime snapshot (written by the node process at pipeline start).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct NodeState {
    pub version: String,
    pub pipelines: Vec<PipelineSnapshot>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PipelineSnapshot {
    pub id: u64,
    pub statements: Vec<String>,
    pub status: &'static str,
}

/// Shared state handle (node writes / api reads).
#[derive(Debug, Clone, Default)]
pub struct StateHandle(Arc<RwLock<NodeState>>);

impl StateHandle {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(NodeState {
            version: env!("CARGO_PKG_VERSION").to_string(),
            pipelines: Vec::new(),
        })))
    }

    pub fn upsert_pipeline(&self, snap: PipelineSnapshot) {
        let mut st = self.0.write().unwrap();
        st.pipelines.retain(|p| p.id != snap.id);
        st.pipelines.push(snap);
    }

    pub fn snapshot(&self) -> NodeState {
        self.0.read().unwrap().clone()
    }

    pub fn snapshot_mut(&self) -> std::sync::RwLockWriteGuard<'_, NodeState> {
        self.0.write().unwrap()
    }
}

/// A statement pushed in a plan body (an item of the embedded specJson array).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PlanStatement {
    pub source: String,
    pub sink: String,
    pub group: String,
    #[serde(default)]
    pub pushdown: Vec<String>,
    pub select_sql: String,
    /// Consume start (earliest/latest; default earliest — effective only when
    /// no committed offset exists)
    #[serde(default)]
    pub start_offset: Option<String>,
}

/// Start the axum server on its own thread (returns the JoinHandle; the
/// thread builds its own tokio runtime — the node main thread stays
/// synchronous for the data plane). `api_token` guards the whole /v1 surface
/// (see [`auth`]); /healthz stays open for liveness probing.
pub fn start_server(
    addr: &str,
    state: StateHandle,
    registry: std::sync::Arc<tailrace_runtime::registry::PipelineRegistry>,
    api_token: auth::NodeApiToken,
) -> anyhow::Result<std::thread::JoinHandle<()>> {
    use axum::routing::get;
    let addr: std::net::SocketAddr = addr.parse()?;
    let st_compile = state.clone();
    let st_plan = state.clone();
    let st_action = state.clone();
    let handle = std::thread::spawn(move || {
        let reg_get = std::sync::Arc::clone(&registry);
        let reg_plan = std::sync::Arc::clone(&registry);
        let reg_action = std::sync::Arc::clone(&registry);
        let reg_stats = std::sync::Arc::clone(&registry);
        let reg_offsets = std::sync::Arc::clone(&registry);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("api runtime");
        rt.block_on(async move {
            let v1 = axum::Router::new()
                .route(
                    "/v1/compile",
                    axum::routing::post(move |body: String| {
                        let st2 = st_compile;
                        async move {
                            let _ = &st2;
                            // body: {"sql": "...", "pipelineId": 42} → statement
                            // set + diagnostics (editor inline errors)
                            let req: Result<serde_json::Value, _> = serde_json::from_str(&body);
                            let resp = match req {
                                Ok(v) => {
                                    let sql = v.get("sql").and_then(|x| x.as_str()).unwrap_or("");
                                    let pid = v.get("pipelineId").and_then(|x| x.as_u64()).unwrap_or(1);
                                    match tailrace_parser::compile(sql, pid) {
                                        Ok((c, diags)) => serde_json::json!({
                                            "ok": diags.is_empty(),
                                            "statements": c.statements,
                                            "diagnostics": diags,
                                        }),
                                        Err(e) => serde_json::json!({ "ok": false, "error": format!("{e:#}") }),
                                    }
                                }
                                Err(e) => serde_json::json!({ "ok": false, "error": format!("{e}") }),
                            };
                            let _ = &st2;
                            axum::Json(resp)
                        }
                    }),
                )
                .route(
                    "/v1/pipelines",
                    get({
                        let reg_get = reg_get.clone();
                        move || {
                            let snaps = reg_get.snapshot();
                            async move {
                                let pipelines: Vec<serde_json::Value> = snaps
                                    .into_iter()
                                    .map(|(id, n, st, err)| {
                                        serde_json::json!({
                                            "id": id,
                                            "status": st,
                                            "statementCount": n,
                                            "statements": Vec::<String>::new(),
                                            "error": err,
                                        })
                                    })
                                    .collect();
                                axum::Json(serde_json::json!({
                                    "version": env!("CARGO_PKG_VERSION"),
                                    "pipelines": pipelines,
                                }))
                            }
                        }
                    }),
                )
                // Console plan push (the SQL-first canon:
                // body={"pipelineId","sqlContent","checkpointMs"}; the legacy
                // body={"pipelineId","specJson"} path is retired)
                .route(
                    "/v1/pipelines/{id}/plan",
                    axum::routing::put(move |axum::extract::Path(id): axum::extract::Path<u64>, body: String| {
                        let st = st_plan.clone();
                        let reg_plan = reg_plan.clone();
                        async move {
                            let resp = match serde_json::from_str::<serde_json::Value>(&body) {
                                Ok(v) => {
                                    // ① raw SQL path (canon): compile SQL →
                                    // statement set
                                    let sql_content = v.get("sqlContent").and_then(|x| x.as_str());
                                    if let Some(sql) = sql_content.filter(|s| !s.is_empty()) {
                                        // plan-level config pass-through (all
                                        // defaults when absent — old consoles
                                        // need zero migration); parsed before
                                        // compiling: groupPrefix/startOffset
                                        // take part in statement compilation
                                        // (group naming / consume start)
                                        let plan_opts: Option<tailrace_catalog::PlanOptions> = v
                                            .get("options")
                                            .and_then(|x| serde_json::from_value(x.clone()).ok());
                                        let (gp, so) = match &plan_opts {
                                            Some(o) => (
                                                o.group_prefix().map(str::to_owned),
                                                Some(o.start_offset().to_string()),
                                            ),
                                            None => (None, None),
                                        };
                                        match tailrace_parser::compile_with_opts(sql, id, gp.as_deref(), so.as_deref()) {
                                            Ok((compiled, diags)) if diags.is_empty() => {
                                                let stmts: Vec<tailrace_catalog::StatementSpec> = compiled.statements.clone();
                                                let ckpt = v.get("checkpointMs").and_then(|x| x.as_u64());
                                                let tables: Vec<tailrace_catalog::PlanTableObject> = v
                                                    .get("tables")
                                                    .and_then(|x| serde_json::from_value(x.clone()).ok())
                                                    .unwrap_or_default();
                                                let st_cb = st.clone();
                    let on_fail: std::sync::Arc<dyn Fn(u64) + Send + Sync> = std::sync::Arc::new(
                        move |pid: u64| {
                            let mut snap = st_cb.snapshot_mut();
                            for p in snap.pipelines.iter_mut() {
                                if p.id == pid { p.status = "FAILED"; }
                            }
                        },
                    );
                                                let dispatch_id = v.get("dispatchId").and_then(|x| x.as_u64()).unwrap_or(0);
                                                // initialState=PAUSED: a PAUSED
                                                // pipeline from upgrade/migration/
                                                // reconciliation recovery pauses
                                                // partitions right at assembly —
                                                // the console's follow-up pause
                                                // degrades to a double guard
                                                let initial_paused = v.get("initialState").and_then(|x| x.as_str()) == Some("PAUSED");
                                                match reg_plan.deploy(id, dispatch_id, stmts, tables, ckpt, plan_opts, Some(on_fail), initial_paused) {
                                                    Ok(n) => {
                                                        let list: Vec<PlanStatement> = compiled
                                                            .statements
                                                            .iter()
                                                            .map(|s| PlanStatement {
                                                                source: s.source.clone(),
                                                                sink: s.sink.clone(),
                                                                group: s.group.clone(),
                                                                pushdown: s.pushdown.clone(),
                                                                select_sql: s.select_sql.clone(),
                                                                start_offset: s.start_offset.clone(),
                                                            })
                                                            .collect();
                                                        let mut snap = st.snapshot_mut();
                                                        snap.pipelines.retain(|p| p.id != id);
                                                        snap.pipelines.push(PipelineSnapshot {
                                                            id,
                                                            statements: list.iter().map(|s| format!("{} → {}", s.source, s.sink)).collect(),
                                                            status: "RUNNING",
                                                        });
                                                        drop(snap);
                                                        serde_json::json!({ "ok": true, "pipelineId": id, "statements": n, "mode": "sql-compiled+serve-executing" })
                                                    }
                                                    Err(e) => serde_json::json!({ "ok": false, "error": format!("pipeline assembly/start failed: {e:#}") })
                                                }
                                            }
                                            Ok((_, diags)) => serde_json::json!({
                                                "ok": false,
                                                "error": "SQL compile diagnostics",
                                                "diagnostics": diags,
                                            }),
                                            Err(e) => serde_json::json!({ "ok": false, "error": format!("SQL compile failed: {e:#}") }),
                                        }
                                    } else {
                                        // ② retired legacy path: old specJson
                                        // pipelines must re-publish SQL-first
                                        serde_json::json!({ "ok": false, "error": "the specJson compatibility path is retired — re-publish SQL-first (pipeline page edit + publish)" })
                                    }
                                }
                                Err(e) => serde_json::json!({ "ok": false, "error": format!("{e}") }),
                            };
                            axum::Json(resp)
                        }
                    }),
                )
                // action state machine (pause/resume/stop)
                // observability trio: stats (live in/out volumes) + logs
                // (seq-cursor incremental log) + offsets (ledger pass-through)
                .route(
                    "/v1/pipelines/{id}/stats",
                    get({
                        let reg_stats = reg_stats.clone();
                        move |axum::extract::Path(id): axum::extract::Path<u64>| {
                            let reg = reg_stats.clone();
                            async move {
                                match reg.stats(id) {
                                    Some(s) => axum::Json(serde_json::json!({
                                        "ok": true,
                                        "stats": s,
                                    })),
                                    None => axum::Json(serde_json::json!({
                                        "ok": false,
                                        "error": format!("pipeline #{id} not registered (undeployed/stopped) — logs may still be queryable via /logs"),
                                    })),
                                }
                            }
                        }
                    }),
                )
                .route(
                    "/v1/pipelines/{id}/logs",
                    get({
                        let reg_logs = reg_stats.clone();
                        move |axum::extract::Path(id): axum::extract::Path<u64>,
                              axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>| {
                            let reg = reg_logs.clone();
                            async move {
                                let after: u64 = q.get("after_seq").and_then(|v| v.parse().ok()).unwrap_or(0);
                                let limit: usize = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(200).min(500);
                                // tail=1: the trailing limit entries (the SSE
                                // bridge's initial pull); otherwise the
                                // after_seq cursor increment
                                let (entries, last_seq) = if after == 0
                                    && q.get("tail").map(|v| v == "1").unwrap_or(false)
                                {
                                    reg.log_hub().last(id, limit)
                                } else {
                                    reg.logs(id, after, limit)
                                };
                                axum::Json(serde_json::json!({
                                    "ok": true, "entries": entries, "lastSeq": last_seq,
                                }))
                            }
                        }
                    }),
                )
                .route(
                    "/v1/pipelines/{id}/offsets",
                    get({
                        let reg_offsets = reg_offsets.clone();
                        move |axum::extract::Path(id): axum::extract::Path<u64>| {
                            let reg = reg_offsets.clone();
                            async move {
                                match reg.offsets(id) {
                                    Ok(v) => axum::Json(v),
                                    Err(e) => axum::Json(serde_json::json!({
                                        "ok": false,
                                        "error": format!("{e:#}"),
                                    })),
                                }
                            }
                        }
                    }),
                )
                .route(
                    "/v1/pipelines/{id}/{action}",
                    axum::routing::post(
                        move |axum::extract::Path((id, action)): axum::extract::Path<(u64, String)>| {
                            let st = st_action.clone();
                            let reg_action = reg_action.clone();
                            async move {
                                let (new_status, hit) = match action.as_str() {
                                    "pause" => ("PAUSED", reg_action.pause(id)),
                                    "resume" => ("RUNNING", reg_action.resume(id)),
                                    "stop" => {
                                        reg_action.stop_pipeline(id);
                                        ("STOPPED", true)
                                    }
                                    _ => ("RUNNING", false),
                                };
                                let mut snap = st.snapshot_mut();
                                if action == "stop" {
                                    snap.pipelines.retain(|p| p.id != id);
                                } else {
                                    for p in snap.pipelines.iter_mut() {
                                        if p.id == id {
                                            p.status = new_status;
                                        }
                                    }
                                }
                                drop(snap);
                                axum::Json(serde_json::json!({ "ok": hit, "pipelineId": id, "status": new_status }))
                            }
                        },
                    ),
                );
            // engine H1: the whole /v1 surface sits behind the shared node
            // token; only /healthz stays open (liveness probing)
            let app = axum::Router::new()
                .route(
                    "/healthz",
                    get(|| async { axum::Json(serde_json::json!({ "ok": true })) }),
                )
                .merge(v1.route_layer(axum::middleware::from_fn_with_state(
                    api_token,
                    auth::require_node_token,
                )));
            let listener = tokio::net::TcpListener::bind(addr).await.expect("api bind");
            axum::serve(listener, app).await.expect("api serve");
        });
    });
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "live test: requires Kafka/Doris + real-environment verification"]
    fn plan_push_and_lifecycle_live() -> anyhow::Result<()> {
        if std::net::TcpStream::connect("localhost:9092").is_err() {
            return Ok(()); // local-liveness only
        }
        let state = StateHandle::new();
        let addr = format!("127.0.0.1:{}", 18200 + (std::process::id() % 1000));
        let reg = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
            &std::env::temp_dir().join(format!("tailrace-test-reg-{}", std::process::id())),
        ));
        let tok = auth::NodeApiToken::new("tr_b_live_test");
        start_server(&addr, state, reg, tok)?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        let c = reqwest::blocking::Client::new();
        // offsets endpoint answers even before any deploy (empty ledger)
        let resp = c
            .get(format!("http://{addr}/v1/pipelines/4242/offsets"))
            .header("Authorization", "Bearer tr_b_live_test")
            .send()?
            .text()?;
        assert!(
            resp.contains("\"ok\":true") || resp.contains("\"ok\": true"),
            "{resp}"
        );
        assert!(resp.contains("4242"), "{resp}");
        // plan push
        let plan_body = serde_json::json!({
            "pipelineId": 4242,
            "sqlContent": "INSERT INTO ${ds:3.shop.orders} SELECT * FROM ${ds:12.default.orders_ogg}",
            "tables": [{"reference":"3.shop.orders","kind":"doris_table","columns":[],"stream_load_url":"http://localhost:1","auth_env":"TAILRACE_TEST"},
                       {"reference":"12.default.orders_ogg","kind":"kafka","columns":[],"bootstrap":"localhost:9092"}],
            "checkpointMs": 10000
        });
        let http_resp = c
            .put(format!("http://{addr}/v1/pipelines/4242/plan"))
            .header("Content-Type", "application/json")
            .header("X-Node-Token", "tr_b_live_test")
            .body(serde_json::to_string(&plan_body)?)
            .send()?;
        let status = http_resp.status();
        let resp = http_resp.text()?;
        println!("PLAN http={status}");
        assert!(
            resp.contains("\"ok\": true") || resp.contains("\"ok\":true"),
            "resp={resp}"
        );
        // pause → PAUSED
        let resp = c
            .post(format!("http://{addr}/v1/pipelines/4242/pause"))
            .header("Authorization", "Bearer tr_b_live_test")
            .send()?
            .text()?;
        assert!(resp.contains("PAUSED"), "{resp}");
        // resume → RUNNING
        let resp = c
            .post(format!("http://{addr}/v1/pipelines/4242/resume"))
            .header("Authorization", "Bearer tr_b_live_test")
            .send()?
            .text()?;
        assert!(resp.contains("RUNNING"), "{resp}");
        // visible in the list
        let resp = c
            .get(format!("http://{addr}/v1/pipelines"))
            .header("Authorization", "Bearer tr_b_live_test")
            .send()?
            .text()?;
        assert!(resp.contains("4242") && resp.contains("RUNNING"), "{resp}");
        Ok(())
    }

    #[test]
    fn healthz_and_pipelines_live() -> anyhow::Result<()> {
        if std::net::TcpStream::connect("localhost:9092").is_err() {
            return Ok(()); // local-liveness only (kafka presence = the dev env)
        }
        let state = StateHandle::new();
        state.upsert_pipeline(PipelineSnapshot {
            id: 42,
            statements: vec!["k.t1→d.t2".into()],
            status: "RUNNING",
        });
        let addr = format!("127.0.0.1:{}", 18000 + (std::process::id() % 1000));
        let reg = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
            &std::env::temp_dir().join(format!("tailrace-test-reg2-{}", std::process::id())),
        ));
        start_server(
            &addr,
            state.clone(),
            reg,
            auth::NodeApiToken::new("tr_b_live_test2"),
        )?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        let body = reqwest::blocking::Client::new()
            .get(format!("http://{addr}/healthz"))
            .send()?
            .text()?;
        assert!(body.contains("\"ok\":true"), "{body}");
        let body = reqwest::blocking::Client::new()
            .get(format!("http://{addr}/v1/pipelines"))
            .header("X-Node-Token", "tr_b_live_test2")
            .send()?
            .text()?;
        assert!(
            body.contains("pipelines"),
            "GET /v1/pipelines defers to the Registry (an empty registry shows no NodeState leftovers): {body}"
        );
        Ok(())
    }

    /// engine H1 regression lock: tokenless /v1 is rejected with HTTP 401,
    /// the wrong token is rejected, the right token passes (either header
    /// shape), and /healthz stays open. No external dependency — always runs.
    #[test]
    fn v1_requires_node_token_healthz_open() -> anyhow::Result<()> {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            let p = l.local_addr()?.port();
            drop(l);
            p
        };
        let addr = format!("127.0.0.1:{port}");
        let state = StateHandle::new();
        let reg = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
            &std::env::temp_dir().join(format!("tailrace-test-auth-{}", std::process::id())),
        ));
        let _h = start_server(
            &addr,
            state,
            reg,
            auth::NodeApiToken::new("tr_b_unit_token"),
        )?;
        std::thread::sleep(std::time::Duration::from_millis(400));
        let c = reqwest::blocking::Client::new();
        assert_eq!(
            c.get(format!("http://{addr}/healthz")).send()?.status(),
            reqwest::StatusCode::OK,
            "healthz stays open"
        );
        assert_eq!(
            c.get(format!("http://{addr}/v1/pipelines"))
                .send()?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "tokenless /v1 rejected"
        );
        assert_eq!(
            c.get(format!("http://{addr}/v1/pipelines"))
                .header("Authorization", "Bearer wrong-token")
                .send()?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "wrong token rejected"
        );
        assert_eq!(
            c.get(format!("http://{addr}/v1/pipelines"))
                .header("Authorization", "Bearer tr_b_unit_token")
                .send()?
                .status(),
            reqwest::StatusCode::OK,
            "bearer accepted"
        );
        assert_eq!(
            c.get(format!("http://{addr}/v1/pipelines"))
                .header("X-Node-Token", "tr_b_unit_token")
                .send()?
                .status(),
            reqwest::StatusCode::OK,
            "x-node-token accepted"
        );
        Ok(())
    }
}
