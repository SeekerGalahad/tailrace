//! Engine-node admin endpoints (§4.3 管理端点): list/detail with the 90s
//! OFFLINE judgement, manual registration behind a healthz probe, revocation,
//! guarded delete, events / metrics feeds, the pipelines-runtime proxy, and
//! the §4.4 SSH deploy chain (eight-step state machine + process actions;
//! migrate stays a 501 — P2).

use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{ApiError, AuthContext, M2mState};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::api::{audit, iso, not_implemented, operator_of};
use crate::state::AppState;

/// Heartbeat silence window before a node reads as OFFLINE (§3.2: 90s).
const OFFLINE_AFTER_SECS: i64 = 90;

// ---------------- DTOs ----------------

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeCapacity {
    pub pipelines: Option<i64>,
    pub statements: Option<i64>,
    pub memory_mb: Option<i64>,
    pub running_pipelines: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeListItem {
    pub id: i64,
    pub node_key: String,
    pub name: String,
    pub api_url: Option<String>,
    pub status: String,
    pub engine_version: Option<String>,
    pub deploy_status: Option<String>,
    pub capacity: NodeCapacity,
    pub artifact_id: Option<i64>,
    pub errors24h: i64,
    pub last_heartbeat: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodesSummary {
    pub online: i64,
    pub total: i64,
    pub running_pipelines: i64,
    pub events24h: i64,
    pub errors24h: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NodesListResponse {
    pub nodes: Vec<NodeListItem>,
    pub summary: NodesSummary,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeDetail {
    #[serde(flatten)]
    pub list_item: NodeListItem,
    pub host: Option<String>,
    pub ssh_user: Option<String>,
    pub ssh_port: Option<i64>,
    pub engine_port: i64,
    pub install_dir: String,
    pub register_mode: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManualRegisterBody {
    pub name: String,
    pub host: String,
    pub engine_port: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManualRegistered {
    pub id: i64,
    pub node_key: String,
    /// Per-node token shown exactly once — the operator needs it to point the
    /// engine's --bootstrap-token at this node's m2m identity.
    pub bootstrap_token: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeDeleteResponse {
    pub remote_cleaned: bool,
    pub warning: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeEventItem {
    pub ts: String,
    pub node_key: String,
    pub level: String,
    pub kind: String,
    pub pipeline_id: Option<i64>,
    pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct EventsResponse {
    pub items: Vec<NodeEventItem>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeMetricPointOut {
    pub ts: String,
    pub rss_bytes: Option<i64>,
    pub cpu_percent: Option<f64>,
    pub rx_bps: Option<f64>,
    pub tx_bps: Option<f64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MetricsResponse {
    pub points: Vec<NodeMetricPointOut>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeployLogItem {
    pub step: String,
    pub status: String,
    pub output: Option<String>,
    pub operator: Option<String>,
    pub ts: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeployLogResponse {
    pub items: Vec<DeployLogItem>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployBody {
    /// Fresh batch: one node per entry (nodes are created by this call).
    pub hosts: Option<Vec<DeployHost>>,
    /// Redeploy an existing SSH-managed node.
    pub node_id: Option<i64>,
    pub artifact_id: Option<i64>,
    pub clean: Option<bool>,
    pub wipe_state: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeActionBody {
    pub action: String,
    pub artifact_id: Option<i64>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigrateBody {
    pub to_node_id: i64,
}

// ---------------- helpers ----------------

fn parse_capacity(raw: Option<&str>) -> NodeCapacity {
    let v = raw
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or(Value::Null);
    let get = |k: &str| v.get(k).and_then(Value::as_i64);
    NodeCapacity {
        pipelines: get("pipelines"),
        statements: get("statements"),
        memory_mb: get("memory_mb"),
        running_pipelines: get("running_pipelines"),
    }
}

fn load_node(state: &AppState, id: i64) -> Result<console_store::Node, Response> {
    state.store().node(id).ok().flatten().ok_or_else(|| {
        super::messages::not_found("err.node.not_found", &[&id.to_string()]).into_response()
    })
}

// ---------------- handlers ----------------

/// `GET /api/nodes` — list + summary (30s polling consumer, §4.3).
#[utoipa::path(
    get,
    path = "/api/nodes",
    responses((status = 200, body = NodesListResponse))
)]
pub async fn list(State(state): State<AppState>) -> Response {
    let store = state.store();
    let nodes = match store.list_nodes() {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let errors: std::collections::HashMap<String, i64> = store
        .errors_24h_by_node()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let running: std::collections::HashMap<i64, i64> = store
        .running_pipeline_counts()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let now = unix_now();

    let mut online = 0i64;
    let mut running_pipelines = 0i64;
    let items: Vec<NodeListItem> = nodes
        .iter()
        .map(|n| {
            let status = display_status(n, now);
            if status == "ONLINE" {
                online += 1;
            }
            running_pipelines += running.get(&n.id).copied().unwrap_or(0);
            NodeListItem {
                id: n.id,
                node_key: n.node_key.clone(),
                name: n.name.clone(),
                api_url: n.api_url.clone(),
                status,
                engine_version: n.engine_version.clone(),
                deploy_status: n.deploy_status.clone(),
                capacity: parse_capacity(n.capacity_json.as_deref()),
                artifact_id: n.artifact_id,
                errors24h: errors.get(&n.node_key).copied().unwrap_or(0),
                last_heartbeat: iso(n.last_heartbeat.as_deref()),
            }
        })
        .collect();
    let events24h = store.events_24h_total().unwrap_or(0);
    let errors24h = errors.values().sum();
    Json(NodesListResponse {
        nodes: items,
        summary: NodesSummary {
            online,
            total: nodes.len() as i64,
            running_pipelines,
            events24h,
            errors24h,
        },
    })
    .into_response()
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Parse "YYYY-MM-DD HH:MM:SS" (UTC, SQLite format) → unix seconds.
pub(crate) fn sqlite_ts_to_epoch(ts: &str) -> Option<i64> {
    // SQLite datetime('now') is UTC; parse manually to avoid a chrono dep.
    let parts: Vec<&str> = ts.split(['-', ' ', ':']).collect();
    if parts.len() != 6 {
        return None;
    }
    let nums: Option<Vec<i64>> = parts.iter().map(|p| p.parse().ok()).collect();
    let n = nums?;
    // days since epoch via civil-date algorithm (Howard Hinnant's days_from_civil)
    let (y, m, d) = (n[0], n[1], n[2]);
    let yy = if m <= 2 { y - 1 } else { y };
    let era = if yy >= 0 { yy } else { yy - 399 } / 400;
    let yoe = yy - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + n[3] * 3600 + n[4] * 60 + n[5])
}

fn display_status(node: &console_store::Node, now_epoch: i64) -> String {
    if node.status == "ONLINE" {
        if let Some(last) = node.last_heartbeat.as_deref().and_then(sqlite_ts_to_epoch) {
            if now_epoch - last > OFFLINE_AFTER_SECS {
                return "OFFLINE".into();
            }
        }
    }
    node.status.clone()
}

/// `GET /api/nodes/{id}` — detail (+ host/ssh/enginePort/installDir/registerMode).
#[utoipa::path(
    get,
    path = "/api/nodes/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = NodeDetail),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn detail(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let node = match load_node(&state, id) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let errors: std::collections::HashMap<String, i64> = state
        .store()
        .errors_24h_by_node()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let running = state
        .store()
        .running_pipeline_counts()
        .unwrap_or_default()
        .into_iter()
        .collect::<std::collections::HashMap<i64, i64>>();
    let mut capacity = parse_capacity(node.capacity_json.as_deref());
    // running_pipelines reflects the console-side truth when the node does
    // not self-report it
    if capacity.running_pipelines.is_none() {
        capacity.running_pipelines = running.get(&node.id).copied();
    }
    Json(NodeDetail {
        list_item: NodeListItem {
            id: node.id,
            node_key: node.node_key.clone(),
            name: node.name.clone(),
            api_url: node.api_url.clone(),
            status: display_status(&node, unix_now()),
            engine_version: node.engine_version.clone(),
            deploy_status: node.deploy_status.clone(),
            capacity,
            artifact_id: node.artifact_id,
            errors24h: errors.get(&node.node_key).copied().unwrap_or(0),
            last_heartbeat: iso(node.last_heartbeat.as_deref()),
        },
        host: node.host.clone(),
        ssh_user: node.ssh_user.clone(),
        ssh_port: node.ssh_port,
        engine_port: node.engine_port,
        install_dir: node.install_dir.clone(),
        register_mode: node.register_mode.clone(),
    })
    .into_response()
}

/// `POST /api/nodes/manual` — probe `GET http://host:port/healthz` first; a
/// 200 answer registers the node with a console-generated node_key (§4.3
/// channel A). Probe failure → 502.
#[utoipa::path(
    post,
    path = "/api/nodes/manual",
    request_body = ManualRegisterBody,
    responses(
        (status = 200, body = ManualRegistered),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse, description = "healthz probe failed"),
    )
)]
pub async fn register_manual(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let body: ManualRegisterBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return super::messages::bad_request("err.bad_body", &[&e.to_string()]).into_response()
        }
    };
    if body.name.trim().is_empty() || body.host.trim().is_empty() {
        return ApiError::bad_request("缺少 name / host").into_response();
    }
    let engine_port = body.engine_port.unwrap_or(18100);
    let api_url = format!("http://{}:{}", body.host.trim(), engine_port);
    let endpoint = console_proxy::NodeEndpoint::new(&api_url);
    match endpoint.healthz_ok(state.node_client()).await {
        Ok(true) => {}
        Ok(false) => {
            return super::messages::respond(
                axum::http::StatusCode::BAD_GATEWAY,
                "ENGINE_UNREACHABLE",
                "err.node.probe_failed",
                &[&api_url],
            )
            .into_response()
        }
        Err(e) => {
            return ApiError::new(
                axum::http::StatusCode::BAD_GATEWAY,
                "ENGINE_UNREACHABLE",
                e.to_string(),
            )
            .into_response()
        }
    }
    let node_key = format!("sn-{}", hex16());
    let token = console_auth::generate_token(console_auth::BOOTSTRAP_TOKEN_PREFIX);
    let token_hash = console_auth::token_hash(&token); // digest at rest (audit H1)
    let store = state.store();
    let id = match store.insert_manual_node(
        &node_key,
        body.name.trim(),
        body.host.trim(),
        engine_port,
        &api_url,
        &token_hash,
    ) {
        Ok(id) => id,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("UNIQUE constraint failed") {
                return ApiError::new(
                    axum::http::StatusCode::CONFLICT,
                    "CONFLICT",
                    "节点名称或 node_key 已存在",
                )
                .into_response();
            }
            return ApiError::internal(&e).into_response();
        }
    };
    audit(
        store,
        &operator_of(&ctx, &headers),
        "NODE_MANUAL_REGISTER",
        "node",
        Some(node_key.clone()),
        Some(
            json!({"name": body.name.trim(), "host": body.host.trim(), "enginePort": engine_port})
                .to_string(),
        ),
    );
    Json(ManualRegistered {
        id,
        node_key,
        bootstrap_token: token,
    })
    .into_response()
}

/// 16 hex chars of randomness (node_key suffix, §4.3 sn-<16hex>).
fn hex16() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 8];
    rand::rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// `POST /api/nodes/{id}/revoke` — revoke the node token; the in-memory
/// ticket ledger is empty by definition until the ticket endpoint ships,
/// so revocation clears what exists.
#[utoipa::path(
    post,
    path = "/api/nodes/{id}/revoke",
    params(("id" = i64, Path)),
    responses(
        (status = 204),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn revoke(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let node = match load_node(&state, id) {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(e) = state.store().revoke_bootstrap_token(&node.node_key) {
        return ApiError::internal(&e).into_response();
    }
    audit(
        state.store(),
        &operator_of(&ctx, &headers),
        "NODE_REVOKE",
        "node",
        Some(node.node_key.clone()),
        None,
    );
    axum::http::StatusCode::NO_CONTENT.into_response()
}

/// `DELETE /api/nodes/{id}` — 409 while pipelines are assigned; the remote
/// cleanup rides the §4.4 SSH chain: SSH-managed (deployed) nodes get a
/// best-effort uninstall (stop + `rm -rf install_dir`) first, and an
/// unreachable remote degrades to the documented skip-with-warning branch.
#[utoipa::path(
    delete,
    path = "/api/nodes/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = NodeDeleteResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "pipelines still assigned"),
    )
)]
pub async fn remove(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let node = match load_node(&state, id) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let assigned = state.store().pipelines_assigned_to(id).unwrap_or_default();
    if !assigned.is_empty() {
        return super::messages::respond_with_refs(
            axum::http::StatusCode::CONFLICT,
            "IN_USE",
            "err.node.in_use",
            &[&format!("{assigned:?}")],
            json!({"pipelines": assigned})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
    }
    // best-effort remote cleanup on SSH-managed nodes (§4.4 chain, T35)
    let mut remote_cleaned = false;
    let mut warning = None;
    if ssh_managed(&node) {
        let placeholder = console_store::Artifact {
            id: 0,
            name: String::new(),
            version: String::new(),
            filename: String::new(),
            sha256: String::new(),
            size_bytes: 0,
            uploaded_by: String::new(),
            created_at: String::new(),
            ref_count: 0,
        };
        let spec = spec_from_node(
            &node,
            &placeholder,
            None,
            false,
            false,
            crate::deploy::ACTION_UNINSTALL,
            operator_of(&ctx, &headers),
        );
        match crate::deploy::run_process_action(
            &state,
            &spec,
            crate::deploy::ProcessAction::Uninstall,
        )
        .await
        {
            Ok(()) => remote_cleaned = true,
            Err(e) => {
                warning = Some(super::messages::t(
                    "err.node.remote_cleanup_failed",
                    &[&format!("{e:#}")],
                ));
            }
        }
    } else {
        warning =
            Some("节点非 SSH 托管（manual/self 注册），远端清理跳过；已仅清理控制面元数据".into());
    }
    if let Err(e) = state.store().delete_node_cascade(id, &node.node_key) {
        return ApiError::internal(&e).into_response();
    }
    audit(
        state.store(),
        &operator_of(&ctx, &headers),
        "NODE_DELETE",
        "node",
        Some(node.node_key.clone()),
        None,
    );
    Json(NodeDeleteResponse {
        remote_cleaned,
        warning,
    })
    .into_response()
}

/// `GET /api/nodes/{id}/events?hours=168` — one node's event feed.
#[utoipa::path(
    get,
    path = "/api/nodes/{id}/events",
    params(("id" = i64, Path), ("hours" = Option<i64>, Query, description = "window, clamped 1..168")),
    responses(
        (status = 200, body = EventsResponse),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn events(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let node = match load_node(&state, id) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let hours = params
        .get("hours")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(24)
        .clamp(1, 168);
    let rows = match state.store().node_events(Some(&node.node_key), hours, None) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    Json(EventsResponse {
        items: rows.into_iter().map(event_item).collect(),
    })
    .into_response()
}

/// `GET /api/nodes/events?hours=24&level=ERROR` — cluster-wide feed.
#[utoipa::path(
    get,
    path = "/api/nodes/events",
    params(("hours" = Option<i64>, Query), ("level" = Option<String>, Query)),
    responses((status = 200, body = EventsResponse))
)]
pub async fn all_events(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let hours = params
        .get("hours")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(24)
        .clamp(1, 168);
    let level = params.get("level").map(String::as_str);
    let rows = match state.store().node_events(None, hours, level) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    Json(EventsResponse {
        items: rows.into_iter().map(event_item).collect(),
    })
    .into_response()
}

fn event_item(r: console_store::NodeEventRow) -> NodeEventItem {
    NodeEventItem {
        ts: iso(Some(&r.ts)).unwrap_or(r.ts),
        node_key: r.node_key,
        level: r.level,
        kind: r.kind,
        pipeline_id: r.pipeline_id,
        message: r.message,
    }
}

/// `GET /api/nodes/{id}/metrics?hours=6` — 1-minute buckets, window clamped
/// 1..24h (§4.3).
#[utoipa::path(
    get,
    path = "/api/nodes/{id}/metrics",
    params(("id" = i64, Path), ("hours" = Option<i64>, Query, description = "window, clamped 1..24")),
    responses(
        (status = 200, body = MetricsResponse),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn metrics(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    if load_node(&state, id).is_err() {
        return super::messages::not_found("err.node.not_found", &[&id.to_string()])
            .into_response();
    }
    let hours = params
        .get("hours")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(6)
        .clamp(1, 24);
    let points = match state.store().node_metrics_series(id, hours) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    Json(MetricsResponse {
        points: points
            .into_iter()
            .map(|p| NodeMetricPointOut {
                ts: format!("{}:00Z", p.ts),
                rss_bytes: p.rss_bytes,
                cpu_percent: p.cpu_pct,
                rx_bps: p.rx_bps,
                tx_bps: p.tx_bps,
            })
            .collect(),
    })
    .into_response()
}

/// `GET /api/nodes/{id}/pipelines-runtime` — proxy of the node's
/// `GET /v1/pipelines` (statement-level snapshot verbatim, §4.3/A2 #29).
#[utoipa::path(
    get,
    path = "/api/nodes/{id}/pipelines-runtime",
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine /v1/pipelines body, passed through verbatim"),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse, description = "engine unreachable"),
    )
)]
pub async fn pipelines_runtime(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let node = match load_node(&state, id) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let Some(api_url) = node.api_url else {
        return ApiError::new(
            axum::http::StatusCode::BAD_GATEWAY,
            "ENGINE_UNREACHABLE",
            "节点未上报 api_url（等待首次心跳）",
        )
        .into_response();
    };
    // engine H1: /v1 requires the shared node token — carry it
    let endpoint =
        console_proxy::NodeEndpoint::with_token(api_url, state.bootstrap().global_token());
    match endpoint
        .get_json(state.node_client(), "/v1/pipelines")
        .await
    {
        Ok(v) => Json(v).into_response(),
        Err(e) => ApiError::new(
            axum::http::StatusCode::BAD_GATEWAY,
            "ENGINE_UNREACHABLE",
            e.to_string(),
        )
        .into_response(),
    }
}

/// `GET /api/nodes/{id}/deploy-log?limit=100` — ascending deploy log (§4.4;
/// empty until the SSH chain writes entries).
#[utoipa::path(
    get,
    path = "/api/nodes/{id}/deploy-log",
    params(("id" = i64, Path), ("limit" = Option<i64>, Query)),
    responses(
        (status = 200, body = DeployLogResponse),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn deploy_log(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    if load_node(&state, id).is_err() {
        return super::messages::not_found("err.node.not_found", &[&id.to_string()])
            .into_response();
    }
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    let rows = match state.store().deploy_log_items(id, limit) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    Json(DeployLogResponse {
        items: rows
            .into_iter()
            .map(|r| DeployLogItem {
                step: r.step,
                status: r.status,
                output: r.output,
                operator: r.operator,
                ts: iso(Some(&r.ts)).unwrap_or(r.ts),
            })
            .collect(),
    })
    .into_response()
}

// ---------------- §4.4 SSH deploy chain (T35) ----------------

/// One new-host entry of a batch deploy (`hosts[]` mode).
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployHost {
    pub host: String,
    pub name: String,
    pub ssh_user: String,
    pub ssh_port: Option<i64>,
    pub engine_port: Option<i64>,
    pub install_dir: Option<String>,
}

/// Per-node acceptance echo of `POST /api/nodes/deploy`.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployAccepted {
    pub node_id: i64,
    /// Initial machine state — the chain advances asynchronously
    /// (watch `GET /api/nodes/{id}/deploy-log`).
    pub status: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeployResponse {
    pub deploys: Vec<DeployAccepted>,
}

/// `202 {accepted:true}` echo of `POST /api/nodes/{id}/action`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ActionAccepted {
    pub accepted: bool,
}

/// True when the node row carries enough SSH identity for the chain to own
/// the remote process (`register_mode='deployed'` + host + install dir).
fn ssh_managed(node: &console_store::Node) -> bool {
    node.register_mode == "deployed" && node.host.as_deref().is_some_and(|h| !h.is_empty())
}

/// Build the chain spec from an existing node row (redeploy / upgrade /
/// actions). Prefers the row's own ssh/install fields.
fn spec_from_node(
    node: &console_store::Node,
    artifact: &console_store::Artifact,
    fresh_token: Option<String>,
    clean: bool,
    wipe_state: bool,
    action: &'static str,
    operator: String,
) -> crate::deploy::ChainSpec {
    crate::deploy::ChainSpec {
        node_id: node.id,
        node_key: node.node_key.clone(),
        name: node.name.clone(),
        host: node.host.clone().unwrap_or_default(),
        ssh_port: node.ssh_port.unwrap_or(22),
        ssh_user: node.ssh_user.clone().unwrap_or_else(|| "root".into()),
        engine_port: node.engine_port,
        install_dir: node.install_dir.clone(),
        artifact_id: artifact.id,
        artifact_name: artifact.name.clone(),
        artifact_version: artifact.version.clone(),
        artifact_sha256: artifact.sha256.clone(),
        fresh_token,
        clean,
        wipe_state,
        action,
        operator,
    }
}

/// `POST /api/nodes/deploy` — the §4.4 eight-step SSH deploy state machine.
/// Two modes: `hosts[]` (fresh batch — nodes are created with
/// register_mode='deployed' + generated node_key/token) or `nodeId`
/// (redeploy an existing SSH-managed node). The chain itself runs in a
/// background task; every step (and the failure rollback path) lands in
/// `deploy_log` and `node.deploy_status`.
#[utoipa::path(
    post,
    path = "/api/nodes/deploy",
    request_body = DeployBody,
    responses(
        (status = 200, body = DeployResponse, description = "chain accepted per node (PENDING); progress via deploy-log"),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse, description = "artifact / node not found"),
        (status = 409, body = crate::api::ErrorResponse),
    )
)]
pub async fn deploy(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let body: DeployBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return super::messages::bad_request("err.bad_body", &[&e.to_string()]).into_response()
        }
    };
    let Some(artifact_id) = body.artifact_id else {
        return ApiError::bad_request("缺少 artifactId（制品未指定）").into_response();
    };
    let artifact = match state.store().artifact(artifact_id) {
        Ok(Some(a)) => a,
        Ok(None) => {
            return super::messages::not_found(
                "err.node.artifact_not_found",
                &[&artifact_id.to_string()],
            )
            .into_response();
        }
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let operator = operator_of(&ctx, &headers);
    let clean = body.clean.unwrap_or(false);
    let wipe_state = body.wipe_state.unwrap_or(false);

    // Resolve targets → (node row, fresh token echo-or-None) pairs.
    let mut targets: Vec<(console_store::Node, Option<String>)> = Vec::new();
    if let Some(hosts) = &body.hosts {
        if hosts.is_empty() {
            return ApiError::bad_request("hosts 不能为空（或改用 nodeId 重新部署既有节点）")
                .into_response();
        }
        if body.node_id.is_some() {
            return ApiError::bad_request("hosts 与 nodeId 二选一").into_response();
        }
        for h in hosts {
            if h.host.trim().is_empty() || h.name.trim().is_empty() {
                return ApiError::bad_request("hosts[].host / hosts[].name 不能为空")
                    .into_response();
            }
            let node_key = format!("sn-{}", hex16());
            let token = console_auth::generate_token(console_auth::BOOTSTRAP_TOKEN_PREFIX);
            let token_hash = console_auth::token_hash(&token);
            let id = match state
                .store()
                .insert_deployed_node(&console_store::NewDeployedNode {
                    node_key: &node_key,
                    name: h.name.trim(),
                    host: h.host.trim(),
                    ssh_port: h.ssh_port.unwrap_or(22),
                    ssh_user: h.ssh_user.trim(),
                    engine_port: h.engine_port.unwrap_or(18100),
                    install_dir: h.install_dir.as_deref().unwrap_or("/opt/tailrace/engine"),
                    bootstrap_token: &token_hash,
                }) {
                Ok(id) => id,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("UNIQUE constraint failed") {
                        return super::messages::respond(
                            axum::http::StatusCode::CONFLICT,
                            "CONFLICT",
                            "err.node.deploy_name_taken",
                            &[h.name.trim()],
                        )
                        .into_response();
                    }
                    return ApiError::internal(&e).into_response();
                }
            };
            let node = match state.store().node(id) {
                Ok(Some(n)) => n,
                _ => {
                    return ApiError::internal(&anyhow::anyhow!("deployed node row lost"))
                        .into_response()
                }
            };
            targets.push((node, Some(token)));
        }
    } else if let Some(node_id) = body.node_id {
        let node = match load_node(&state, node_id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        if !ssh_managed(&node) {
            return ApiError::bad_request(
                "节点非 SSH 托管（manual/self 注册无部署链身份），仅 deployed 节点可重新部署",
            )
            .into_response();
        }
        targets.push((node, None));
    } else {
        return ApiError::bad_request("缺少 hosts 或 nodeId").into_response();
    }

    // Audit once for the request; per-step rows live in deploy_log.
    audit(
        state.store(),
        &operator,
        "NODE_DEPLOY",
        "node",
        Some(
            targets
                .iter()
                .map(|(n, _)| n.node_key.clone())
                .collect::<Vec<_>>()
                .join(","),
        ),
        Some(
            json!({
                "artifactId": artifact.id, "artifactVersion": artifact.version,
                "hosts": targets.len(),
                "clean": clean, "wipeState": wipe_state
            })
            .to_string(),
        ),
    );

    // Spawn the chain per node (sequential over the batch — one SSH lane,
    // honest deploy_log ordering under the same operator).
    let mut accepted = Vec::new();
    let mut first = true;
    for (node, token) in targets {
        let spec = spec_from_node(
            &node,
            &artifact,
            token,
            clean,
            wipe_state,
            crate::deploy::ACTION_DEPLOY,
            operator.clone(),
        );
        accepted.push(DeployAccepted {
            node_id: spec.node_id,
            status: "PENDING".into(),
        });
        let st = state.clone();
        let run = async move {
            let outcome = crate::deploy::run_deploy_chain(st, spec).await;
            tracing::info!(outcome = ?outcome, "deploy chain finished");
        };
        if first {
            first = false;
            tokio::spawn(run);
        } else {
            // stagger batch starts so remote `releases/<v>` writes and
            // health windows don't interleave on shared hosts
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            tokio::spawn(run);
        }
    }
    Json(DeployResponse { deploys: accepted }).into_response()
}

/// `POST /api/nodes/{id}/action` — process-level actions on an SSH-managed
/// node. `start/stop/restart/rollback/uninstall` run synchronously over SSH
/// (each phase into deploy_log); `upgrade` requires `artifactId` and rides
/// the full eight-step chain in the background.
#[utoipa::path(
    post,
    path = "/api/nodes/{id}/action",
    request_body = NodeActionBody,
    params(("id" = i64, Path)),
    responses(
        (status = 202, body = ActionAccepted),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse, description = "SSH side failed"),
    )
)]
pub async fn action(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    let body: NodeActionBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return super::messages::bad_request("err.bad_body", &[&e.to_string()]).into_response()
        }
    };
    let node = match load_node(&state, id) {
        Ok(n) => n,
        Err(r) => return r,
    };
    if !ssh_managed(&node) {
        return ApiError::bad_request(
            "节点非 SSH 托管（manual/self 注册无部署链身份），进程动作不可用",
        )
        .into_response();
    }
    let operator = operator_of(&ctx, &headers);

    // upgrade: full eight-step chain in the background (§4.4)
    if body.action == "upgrade" {
        let Some(artifact_id) = body.artifact_id else {
            return ApiError::bad_request("upgrade 需要 artifactId").into_response();
        };
        let artifact = match state.store().artifact(artifact_id) {
            Ok(Some(a)) => a,
            Ok(None) => {
                return super::messages::not_found(
                    "err.node.artifact_not_found",
                    &[&artifact_id.to_string()],
                )
                .into_response();
            }
            Err(e) => return ApiError::internal(&e).into_response(),
        };
        audit(
            state.store(),
            &operator,
            "NODE_ACTION",
            "node",
            Some(node.node_key.clone()),
            Some(json!({"action": "upgrade", "artifactId": artifact.id}).to_string()),
        );
        let spec = spec_from_node(
            &node,
            &artifact,
            None,
            true,
            false,
            crate::deploy::ACTION_UPGRADE,
            operator,
        );
        let st = state.clone();
        tokio::spawn(async move {
            let outcome = crate::deploy::run_deploy_chain(st, spec).await;
            tracing::info!(outcome = ?outcome, "upgrade chain finished");
        });
        return (
            axum::http::StatusCode::ACCEPTED,
            Json(ActionAccepted { accepted: true }),
        )
            .into_response();
    }

    let Some(process_action) = crate::deploy::ProcessAction::parse(&body.action) else {
        return super::messages::bad_request("err.node.action_unknown", &[&body.action])
            .into_response();
    };
    // actions operate on the *current* release; a placeholder artifact block
    // keeps the spec self-contained (unused fields for sync actions)
    let placeholder = console_store::Artifact {
        id: node.artifact_id.unwrap_or(0),
        name: String::new(),
        version: String::new(),
        filename: String::new(),
        sha256: String::new(),
        size_bytes: 0,
        uploaded_by: String::new(),
        created_at: String::new(),
        ref_count: 0,
    };
    let spec = spec_from_node(
        &node,
        &placeholder,
        None,
        false,
        false,
        process_action.label(),
        operator.clone(),
    );
    audit(
        state.store(),
        &operator,
        "NODE_ACTION",
        "node",
        Some(node.node_key.clone()),
        Some(json!({"action": body.action}).to_string()),
    );
    match crate::deploy::run_process_action(&state, &spec, process_action).await {
        Ok(()) => (
            axum::http::StatusCode::ACCEPTED,
            Json(ActionAccepted { accepted: true }),
        )
            .into_response(),
        Err(e) => ApiError::new(
            axum::http::StatusCode::BAD_GATEWAY,
            "SSH_FAILED",
            format!("{e:#}"),
        )
        .into_response(),
    }
}

/// `POST /api/nodes/{id}/migrate` — P2 deliverable (§4.4), still a 501.
#[utoipa::path(
    post,
    path = "/api/nodes/{id}/migrate",
    request_body = MigrateBody,
    params(("id" = i64, Path)),
    responses((status = 501, body = crate::api::ErrorResponse))
)]
pub async fn migrate(
    State(_state): State<AppState>,
    Path(id): Path<i64>,
    _body: axum::body::Bytes,
) -> Response {
    let _ = id;
    not_implemented("节点迁移（三步编排）", "P2 交付").into_response()
}
