//! Engine-node m2m endpoints (§4.3): `POST /api/nodes/register` and
//! `POST /api/nodes/heartbeat`. Both bypass sessions and self-authenticate via
//! bootstrap/node token inside the JSON body (middleware `require_node_*`).

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{ApiError, NodePrincipal};
use console_store::{NewNodeEvent, NewNodeMetrics, NewPipelineMetrics};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::api::ErrorResponse;
use crate::state::AppState;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RegisterBody {
    /// Validated by the m2m middleware; here for schema completeness.
    pub node_key: Option<String>,
    pub api_url: Option<String>,
    pub capacity_json: Option<String>,
    pub bootstrap_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeCreated {
    pub id: i64,
    pub node_key: String,
    pub name: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RegisterResponse {
    pub node: NodeCreated,
    /// Server-generated per-node token (`tr_b_<48hex>`), echoed exactly once
    /// in this response; only its sha256 digest is stored (audit H1). The
    /// node must persist it and present it on every later register/heartbeat.
    #[serde(rename = "bootstrapToken")]
    pub bootstrap_token: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeEcho {
    pub node_key: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HeartbeatResponse {
    pub node: NodeEcho,
}

/// Heartbeat body — §4.3 "消费侧逐字段容忍（null/缺字段全容忍）". Loose
/// arrays (`pipelineStates` / `events` / `resources`) stay `serde_json::Value`
/// and are validated row-by-row.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatBody {
    pub node_key: Option<String>,
    pub name: Option<String>,
    pub api_url: Option<String>,
    pub engine_version: Option<String>,
    pub capacity_json: Option<String>,
    pub pipeline_states: Option<Vec<Value>>,
    pub events: Option<Vec<Value>>,
    pub resources: Option<Value>,
}

/// `POST /api/nodes/register` — idempotent upsert by node_key; 200 echoes the
/// authoritative identity (§4.3). The global bootstrap token admits an
/// unknown node_key exactly once; every successful register issues a fresh
/// server-generated per-node token (mirroring the manual-register
/// `generate_token` precedent), stores only its sha256 digest and echoes the
/// plaintext exactly once (audit H1).
#[utoipa::path(
    post,
    path = "/api/nodes/register",
    request_body = RegisterBody,
    responses(
        (status = 200, body = RegisterResponse, description = "node registered; echo nodeKey is authoritative; bootstrapToken is the new per-node token, echoed exactly once"),
        (status = 401, body = ErrorResponse, description = "bootstrap token invalid/revoked"),
    )
)]
pub async fn register(
    State(state): State<AppState>,
    Extension(principal): Extension<NodePrincipal>,
    body: Bytes,
) -> Response {
    let body: RegisterBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return ApiError::bad_request(format!("非法注册请求: {e}")).into_response(),
    };
    let token = console_auth::generate_token(console_auth::BOOTSTRAP_TOKEN_PREFIX);
    let token_hash = console_auth::token_hash(&token);
    let store = state.store();
    let result = store.upsert_self_registered(
        &principal.node_key,
        body.api_url.as_deref(),
        body.capacity_json.as_deref(),
        &token_hash,
    );
    let (id, name) = match result {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let _ = store.insert_audit(&console_store::AuditEntry {
        actor: format!("node:{}", principal.node_key),
        action: "NODE_REGISTER".into(),
        entity_type: "node".into(),
        entity_id: Some(principal.node_key.clone()),
        detail_json: None,
    });
    Json(RegisterResponse {
        node: NodeCreated {
            id,
            node_key: principal.node_key,
            name,
        },
        bootstrap_token: token,
    })
    .into_response()
}

/// `POST /api/nodes/heartbeat` — marks ONLINE, persists events + resource
/// metrics + pipeline totals, acknowledges with the nodeKey echo (§4.3).
#[utoipa::path(
    post,
    path = "/api/nodes/heartbeat",
    request_body = HeartbeatBody,
    responses(
        (status = 200, body = HeartbeatResponse, description = "events acknowledged"),
        (status = 401, body = ErrorResponse, description = "node token invalid or node unknown"),
    )
)]
pub async fn heartbeat(
    State(state): State<AppState>,
    Extension(principal): Extension<NodePrincipal>,
    body: Bytes,
) -> Response {
    let body: HeartbeatBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return ApiError::bad_request(format!("非法心跳请求: {e}")).into_response(),
    };
    let store = state.store();
    let node_key = principal.node_key.clone();
    let applied = store.apply_heartbeat(
        &node_key,
        body.name.as_deref(),
        body.api_url.as_deref(),
        body.engine_version.as_deref(),
        body.capacity_json.as_deref(),
    );
    match applied {
        Ok(true) => {}
        Ok(false) => return ApiError::not_found("节点不存在").into_response(),
        Err(e) => return ApiError::internal(&e).into_response(),
    }

    // Events: row-level tolerance — rows missing level/kind/message are
    // skipped; missing/garbage ts falls back to now.
    let events: Vec<NewNodeEvent> = body
        .events
        .unwrap_or_default()
        .iter()
        .filter_map(|v| parse_event(&node_key, v))
        .collect();
    // Pipeline totals -> pipeline_metrics (only rows carrying full totals).
    let node = store.node_by_key(&node_key).ok().flatten();
    if let Some(node) = &node {
        let rows: Vec<NewPipelineMetrics> = body
            .pipeline_states
            .unwrap_or_default()
            .iter()
            .filter_map(parse_pipeline_metrics)
            .collect();
        if let Err(e) = store.insert_pipeline_metrics(node.id, &rows) {
            return ApiError::internal(&e).into_response();
        }
        if let Some(resources) = &body.resources {
            if let Some(m) = parse_resources(resources) {
                if let Err(e) = store.insert_node_metrics(node.id, &m) {
                    return ApiError::internal(&e).into_response();
                }
            }
        }
    }
    if let Err(e) = store.insert_node_events(&events) {
        return ApiError::internal(&e).into_response();
    }
    Json(HeartbeatResponse {
        node: NodeEcho { node_key },
    })
    .into_response()
}

// ---------------- lenient row parsing ----------------

fn parse_event(node_key: &str, v: &Value) -> Option<NewNodeEvent> {
    let level = v.get("level")?.as_str()?.to_string();
    let kind = v.get("kind")?.as_str()?.to_string();
    let message = v
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    let pipeline_id = v.get("pipelineId").and_then(json_int);
    let ts_unix = v.get("ts").and_then(json_int);
    Some(NewNodeEvent {
        node_key: node_key.to_string(),
        pipeline_id,
        level,
        kind,
        message,
        ts_unix,
    })
}

fn parse_pipeline_metrics(v: &Value) -> Option<NewPipelineMetrics> {
    let pipeline_id = v.get("id").and_then(json_int)?;
    let status = v.get("state").and_then(|s| s.as_str()).map(str::to_string);
    let totals = v.get("totals")?;
    Some(NewPipelineMetrics {
        pipeline_id,
        status,
        consumed: totals.get("in").and_then(json_int)?,
        flushed: totals.get("out").and_then(json_int)?,
        flushes: totals.get("flushes").and_then(json_int)?,
    })
}

fn parse_resources(v: &Value) -> Option<NewNodeMetrics> {
    if !v.is_object() {
        return None;
    }
    Some(NewNodeMetrics {
        rss_bytes: v.get("rssBytes").and_then(json_int),
        mem_total_bytes: v.get("memTotalBytes").and_then(json_int),
        cpu_pct: v.get("cpuPercent").and_then(|x| x.as_f64()),
        rx_bps: v.get("rxBps").and_then(|x| x.as_f64()),
        tx_bps: v.get("txBps").and_then(|x| x.as_f64()),
    })
}

/// Accept numbers, or numeric strings, everywhere (逐字段容忍).
fn json_int(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}
