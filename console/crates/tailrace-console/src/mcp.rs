//! MCP server (T-E4, blueprint §7.5): JSON-RPC 2.0 over `POST /mcp`.
//!
//! Transport: Streamable HTTP, request/response only (no server→client SSE
//! stream — every method here is a plain request). One JSON-RPC message per
//! POST; batches are rejected (removed in MCP 2025-06-18). `initialize`
//! hands out an `Mcp-Session-Id` so later calls can be attributed to the
//! client's declared `clientInfo.name`; clients that do not echo it fall
//! back to `X-MCP-Client` / `User-Agent` labelling.
//!
//! Tools: fourteen entries (the §7.5 ten plus the R8 four — DDL
//! draft/execute/evolve and backfill status; `trigger_backfill` already
//! covered create-with-dryRun), each a thin adapter over the REST handler
//! it maps to — same DTOs, same error envelopes, invoked in-process (no
//! HTTP hop, no session middleware; `/mcp` follows the MCP convention of no
//! console auth, only a global fixed-window rate limit). Read tools are
//! always on. The six write tools need BOTH the process-level switch
//! (`--mcp-allow-write` / `MCP_ALLOW_WRITE=1`) AND `"confirm": true` in the
//! call arguments.
//!
//! Tool descriptions are GENERATED from the OpenAPI document (R8): each
//! tool names its `(method, path)` and the description text is the
//! operation's — the same utoipa annotations that serve /swagger-ui. There
//! is no second hand-written description to drift; the unit tests pin that
//! every tool's route resolves in the document.
//!
//! Audit (§7.5): every tool call — ok, failed or rejected — lands in
//! `audit_log` as `MCP_TOOL_CALL` with `actor = mcp:<client>`, including
//! the attempts that never reach a tool (unknown tool name, missing /
//! non-string name, non-object `arguments` — audited as rejected with
//! `UNKNOWN_TOOL` / `INVALID_PARAMS`). Write tools that pass the gates
//! additionally produce the REST handler's own domain
//! audit row (PIPELINE_CREATE / PIPELINE_DEPLOY / PIPELINE_ACTION) under
//! the same actor: the handler's operator resolves to the MCP identity.
//!
//! `trigger_backfill` (T-A2, §7.3): maps to POST /api/pipelines/{id}/backfill
//! with `dryRun` defaulting to TRUE (blueprint §7.5 — an MCP caller gets the
//! zero-side-effect preview unless it explicitly asks for execution).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Json, Router};
use console_auth::{AuthContext, AuthUser, M2mState};
use serde_json::{json, Map, Value};
use utoipa::OpenApi;

use crate::api::{self, audit};
use crate::state::AppState;

// ---------------- constants ----------------

/// Newest protocol revision we speak; older known ones are echoed back.
pub const PROTOCOL_VERSION: &str = "2025-06-18";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];
pub const SERVER_NAME: &str = "tailrace-console";
/// Env twin of `--mcp-allow-write`: `1|true|yes|on` (case-insensitive).
pub const ALLOW_WRITE_ENV: &str = "MCP_ALLOW_WRITE";
pub const SESSION_HEADER: &str = "mcp-session-id";
pub const CLIENT_HEADER: &str = "x-mcp-client";
/// Global request budget per second across all clients — the trivial brake
/// for an unauthenticated endpoint, not a per-client quota.
pub const RATE_LIMIT_PER_SEC: u32 = 100;
const MAX_SESSIONS: usize = 1024;
const MAX_HANDLER_BODY: usize = 16 * 1024 * 1024;
const MAX_LABEL_CHARS: usize = 64;
const MAX_AUDIT_STRING_CHARS: usize = 256;

// JSON-RPC 2.0 error codes (+ implementation-defined -3200x).
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
/// Implementation-defined: the process-level write switch is off.
pub const WRITE_DISABLED: i64 = -32001;
/// Implementation-defined: global rate limit exceeded.
pub const RATE_LIMITED: i64 = -32002;

/// The tool table, in `tools/list` order: `(name, is_write)`. The §7.5 ten
/// (pause and resume routes share one tool — `action` selects) plus the R8
/// four: the DDL closed loop (`ddl_draft` read — it composes text only;
/// `ddl_execute` / `ddl_evolve` write — they run DDL on Doris) and
/// `backfill_status` (read — the job progress list; `trigger_backfill`
/// already covers create, dryRun included). Fourteen of the ≤16 budget.
pub const TOOLS: &[(&str, bool)] = &[
    ("list_datasources", false),
    ("list_pipelines", false),
    ("get_pipeline", false),
    ("get_pipeline_stats", false),
    ("get_pipeline_logs", false),
    ("compile_sql", false),
    ("create_pipeline", true),
    ("deploy_pipeline", true),
    ("pause_resume_pipeline", true),
    ("trigger_backfill", true),
    ("ddl_draft", false),
    ("ddl_execute", true),
    ("ddl_evolve", true),
    ("backfill_status", false),
];

// ---------------- state / router ----------------

/// MCP wiring: the write switch plus the two pieces of volatile state the
/// transport needs (session labels, rate window). Cloned handles share the
/// underlying maps, so `router()` layers can hand it to every request.
#[derive(Clone)]
pub struct McpState {
    allow_write: bool,
    sessions: Arc<Mutex<HashMap<String, String>>>,
    limiter: Arc<RateLimiter>,
}

impl McpState {
    pub fn new(allow_write: bool) -> Self {
        Self::with_rate_limit(allow_write, RATE_LIMIT_PER_SEC)
    }

    /// [`Self::new`] with an explicit per-second budget (tests pin a tiny
    /// one to exercise the 429 path without wall-clock games).
    pub fn with_rate_limit(allow_write: bool, max_per_sec: u32) -> Self {
        Self {
            allow_write,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            limiter: Arc::new(RateLimiter::new(max_per_sec)),
        }
    }

    /// `MCP_ALLOW_WRITE` ∈ {1,true,yes,on} (case-insensitive) enables the
    /// write tools; anything else — including unset — keeps them off.
    pub fn from_env() -> Self {
        Self::new(env_truthy(std::env::var(ALLOW_WRITE_ENV).ok().as_deref()))
    }

    pub fn allow_write(&self) -> bool {
        self.allow_write
    }

    /// Audit label for this request: a known `Mcp-Session-Id` (recorded at
    /// `initialize` from `clientInfo.name`) wins; otherwise
    /// `X-MCP-Client`, then `User-Agent`, then `anonymous`.
    fn client_label(&self, headers: &HeaderMap) -> String {
        if let Some(sid) = header_str(headers, SESSION_HEADER) {
            let sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(name) = sessions.get(sid) {
                return name.clone();
            }
        }
        header_str(headers, CLIENT_HEADER)
            .or_else(|| header_str(headers, "user-agent"))
            .map(sanitize_label)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "anonymous".to_string())
    }

    fn register_session(&self, label: &str) -> String {
        let sid = new_session_id();
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        // Bounded: a flood of initializes at worst drops labels back to the
        // header fallback (audit stays truthful, just less specific).
        if sessions.len() >= MAX_SESSIONS {
            sessions.clear();
        }
        sessions.insert(sid.clone(), label.to_string());
        sid
    }
}

/// `/mcp` routes with the given config; mount via `build_router_with_mcp`.
pub fn router(mcp: McpState) -> Router<AppState> {
    Router::new()
        .route("/mcp", post(handle))
        .layer(Extension(mcp))
}

// ---------------- HTTP surface ----------------

/// `POST /mcp` — one JSON-RPC request per body. JSON-RPC responses (errors
/// included) ride HTTP 200; notifications get 202 with no body; parse
/// failures get 400 and rate-limit rejections 429.
async fn handle(
    State(state): State<AppState>,
    Extension(mcp): Extension<McpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !mcp.limiter.allow() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(rpc_error(
                Value::Null,
                RATE_LIMITED,
                "rate limit exceeded; retry later",
                None,
            )),
        )
            .into_response();
    }
    let request: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(rpc_error(
                    Value::Null,
                    PARSE_ERROR,
                    &format!("parse error: {e}"),
                    None,
                )),
            )
                .into_response()
        }
    };
    let client = mcp.client_label(&headers);
    let reply = dispatch(&state, &mcp, &client, request).await;
    let mut response = match reply.body {
        Some(v) => Json(v).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    };
    if let Some(sid) = reply.session_id {
        if let Ok(v) = HeaderValue::from_str(&sid) {
            response
                .headers_mut()
                .insert(HeaderName::from_static(SESSION_HEADER), v);
        }
    }
    response
}

// ---------------- JSON-RPC dispatch ----------------

/// Result of one dispatched message: the JSON-RPC response (None for
/// notifications) plus, when `initialize` ran, the session id to advertise.
pub struct Reply {
    pub body: Option<Value>,
    pub session_id: Option<String>,
}

/// Handle one JSON-RPC message against the app state. Split out from
/// [`handle`] so tests can drive the protocol without HTTP.
pub async fn dispatch(state: &AppState, mcp: &McpState, client: &str, request: Value) -> Reply {
    let Some(obj) = request.as_object() else {
        return Reply {
            body: Some(rpc_error(
                Value::Null,
                INVALID_REQUEST,
                "expected a single JSON-RPC request object (batching is not supported)",
                None,
            )),
            session_id: None,
        };
    };
    let is_request = obj.contains_key("id");
    let id = obj.get("id").cloned().unwrap_or(Value::Null);
    if !is_request {
        // notifications (initialized/cancelled/progress): nothing to do.
        return Reply {
            body: None,
            session_id: None,
        };
    }
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Reply {
            body: Some(rpc_error(
                id,
                INVALID_REQUEST,
                "jsonrpc must be \"2.0\"",
                None,
            )),
            session_id: None,
        };
    }
    let Some(method) = obj.get("method").and_then(Value::as_str) else {
        return Reply {
            body: Some(rpc_error(id, INVALID_REQUEST, "missing method", None)),
            session_id: None,
        };
    };
    let params = obj.get("params").cloned().unwrap_or(Value::Null);

    let mut session_id = None;
    let outcome: Result<Value, RpcErr> = match method {
        "initialize" => {
            let (result, sid) = initialize(state, mcp, client, &params);
            session_id = Some(sid);
            Ok(result)
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_definitions() })),
        "tools/call" => tools_call(state, mcp, client, &params).await,
        other => Err(RpcErr::new(
            METHOD_NOT_FOUND,
            format!("method not found: {other}"),
            None,
        )),
    };
    let body = match outcome {
        Ok(result) => rpc_result(id, result),
        Err(e) => rpc_error(id, e.code, &e.message, e.data),
    };
    Reply {
        body: Some(body),
        session_id,
    }
}

fn initialize(
    state: &AppState,
    mcp: &McpState,
    header_client: &str,
    params: &Value,
) -> (Value, String) {
    let client_name = params
        .pointer("/clientInfo/name")
        .and_then(Value::as_str)
        .map(sanitize_label)
        .filter(|s| !s.is_empty());
    let label = client_name.unwrap_or_else(|| header_client.to_string());
    let requested = params.get("protocolVersion").and_then(Value::as_str);
    let negotiated = requested
        .filter(|v| SUPPORTED_PROTOCOL_VERSIONS.contains(v))
        .unwrap_or(PROTOCOL_VERSION);
    let session_id = mcp.register_session(&label);
    audit(
        state.store(),
        &format!("mcp:{label}"),
        "MCP_INITIALIZE",
        "mcp_session",
        Some(session_id.clone()),
        Some(
            json!({
                "clientInfo": params.get("clientInfo").cloned().unwrap_or(Value::Null),
                "requestedProtocolVersion": requested,
                "protocolVersion": negotiated,
            })
            .to_string(),
        ),
    );
    let result = json!({
        "protocolVersion": negotiated,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION") },
        "instructions": instructions(mcp.allow_write),
    });
    (result, session_id)
}

fn instructions(allow_write: bool) -> String {
    let writes = if allow_write {
        "ENABLED"
    } else {
        "DISABLED (start tailrace-console with --mcp-allow-write or MCP_ALLOW_WRITE=1 to enable them)"
    };
    format!(
        "Tailrace console management plane. Read tools (list/get/compile, ddl_draft, \
         backfill_status) are always available. Write tools (create_pipeline, \
         deploy_pipeline, pause_resume_pipeline, trigger_backfill, ddl_execute, ddl_evolve) \
         are {writes}; every write call must also pass \"confirm\": true. All calls are \
         audited as actor mcp:<client>."
    )
}

// ---------------- tools ----------------

async fn tools_call(
    state: &AppState,
    mcp: &McpState,
    client: &str,
    params: &Value,
) -> Result<Value, RpcErr> {
    // §7.5: every tool call — ok, failed or rejected — is audit evidence, so
    // the actor is resolved up front and each early rejection below audits
    // before returning (the three classes that never reached `execute`
    // previously left no row: unknown tool, missing/non-string tool name,
    // non-object arguments).
    let actor = format!("mcp:{client}");
    let name = match params.get("name").and_then(Value::as_str) {
        Some(n) => n,
        None => {
            audit_tool_call(
                state,
                &actor,
                &tool_label(params),
                &Map::new(),
                "rejected",
                json!({ "code": "INVALID_PARAMS", "reason": "missing or non-string tool name" }),
            );
            return Err(RpcErr::invalid_params("missing tool name"));
        }
    };
    let args = match params.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(m)) => m.clone(),
        Some(other) => {
            audit_tool_call(
                state,
                &actor,
                name,
                &Map::new(),
                "rejected",
                json!({
                    "code": "INVALID_PARAMS",
                    "reason": "arguments must be a JSON object of tool arguments",
                    "arguments": brief_value(other),
                }),
            );
            return Err(RpcErr::invalid_params(
                "arguments must be a JSON object of tool arguments",
            ));
        }
    };
    let Some((_, write)) = TOOLS.iter().find(|(n, _)| *n == name) else {
        audit_tool_call(
            state,
            &actor,
            name,
            &args,
            "rejected",
            json!({ "code": "UNKNOWN_TOOL", "tool": name }),
        );
        return Err(RpcErr::new(
            INVALID_PARAMS,
            format!("unknown tool: {name}"),
            Some(json!({ "code": "UNKNOWN_TOOL", "tool": name })),
        ));
    };
    if *write {
        if !mcp.allow_write {
            audit_tool_call(
                state,
                &actor,
                name,
                &args,
                "rejected",
                json!({ "code": "WRITE_DISABLED" }),
            );
            return Err(RpcErr::new(
                WRITE_DISABLED,
                format!(
                    "write tool {name} is disabled: start tailrace-console with \
                     --mcp-allow-write (or set MCP_ALLOW_WRITE=1)"
                ),
                Some(json!({ "code": "WRITE_DISABLED", "tool": name })),
            ));
        }
        if args.get("confirm") != Some(&Value::Bool(true)) {
            audit_tool_call(
                state,
                &actor,
                name,
                &args,
                "rejected",
                json!({ "code": "CONFIRM_REQUIRED" }),
            );
            return Err(RpcErr::new(
                INVALID_PARAMS,
                format!("write tool {name} requires \"confirm\": true"),
                Some(json!({ "code": "CONFIRM_REQUIRED", "tool": name })),
            ));
        }
    }
    let response = match execute(state, &actor, name, &args).await {
        Ok(r) => r,
        Err(e) => {
            audit_tool_call(
                state,
                &actor,
                name,
                &args,
                "rejected",
                json!({ "code": "INVALID_ARGUMENTS" }),
            );
            return Err(e);
        }
    };
    let (status, body) = response_json(response).await;
    let outcome = if status.is_success() { "ok" } else { "error" };
    audit_tool_call(
        state,
        &actor,
        name,
        &args,
        outcome,
        json!({ "httpStatus": status.as_u16() }),
    );
    Ok(tool_result(status, body))
}

/// Invoke the REST handler the tool maps to (§7.5), in-process. The
/// `AuthContext` carries the MCP actor as its username so the handler's own
/// audit rows (`operator_of` fallback) record `mcp:<client>` too.
async fn execute(
    state: &AppState,
    actor: &str,
    name: &str,
    args: &Map<String, Value>,
) -> Result<Response, RpcErr> {
    let st = State(state.clone());
    let identity = || Extension(mcp_identity(actor));
    let response = match name {
        "list_datasources" => {
            let mut q = HashMap::new();
            if let Some(t) = opt_str(args, "type")? {
                q.insert("type".to_string(), t.to_string());
            }
            api::datasources::list(st, Query(q)).await
        }
        "list_pipelines" => {
            let mut q = HashMap::new();
            for key in ["page", "pageSize"] {
                if let Some(n) = opt_i64(args, key)? {
                    q.insert(key.to_string(), n.to_string());
                }
            }
            api::pipelines::list(st, Query(q)).await
        }
        "get_pipeline" => api::pipelines::detail(st, Path(req_i64(args, "id")?)).await,
        "get_pipeline_stats" => api::pipelines::stats(st, Path(req_i64(args, "id")?)).await,
        "get_pipeline_logs" => {
            let id = req_i64(args, "id")?;
            let lines = opt_i64(args, "lines")?.unwrap_or(200);
            logs_snapshot(state, id, lines).await
        }
        "compile_sql" => {
            req_str(args, "sql")?;
            api::pipelines::compile(st, json_bytes(args)).await
        }
        "create_pipeline" => {
            req_str(args, "name")?;
            req_str(args, "sqlContent")?;
            let mut body = args.clone();
            body.remove("confirm");
            api::pipelines::create(st, identity(), HeaderMap::new(), json_bytes(&body)).await
        }
        "deploy_pipeline" => {
            let id = req_i64(args, "id")?;
            let node_id = opt_i64(args, "nodeId")?;
            let body = Bytes::from(json!({ "nodeId": node_id }).to_string());
            api::pipelines::deploy(st, identity(), HeaderMap::new(), Path(id), body).await
        }
        "pause_resume_pipeline" => {
            let id = req_i64(args, "id")?;
            match req_str(args, "action")? {
                "pause" => api::pipelines::pause(st, identity(), HeaderMap::new(), Path(id)).await,
                "resume" => {
                    api::pipelines::resume(st, identity(), HeaderMap::new(), Path(id)).await
                }
                other => {
                    return Err(RpcErr::invalid_params(format!(
                        "action must be \"pause\" or \"resume\", got {other:?}"
                    )))
                }
            }
        }
        "trigger_backfill" => {
            let id = req_i64(args, "id")?;
            // bounds are the §7.3 objects verbatim ({"type":"offset","value":N})
            let bound = |key: &str| -> Result<Value, RpcErr> {
                args.get(key)
                    .filter(|v| v.is_object())
                    .cloned()
                    .ok_or_else(|| {
                        RpcErr::invalid_params(format!(
                            "argument {key:?} must be an object like {{\"type\":\"offset\",\"value\":0}}"
                        ))
                    })
            };
            let from = bound("from")?;
            let to = bound("to")?;
            let dry_run = match args.get("dryRun") {
                None | Some(Value::Null) => true, // §7.5: MCP defaults to plan-only
                Some(Value::Bool(b)) => *b,
                Some(v) => {
                    return Err(RpcErr::invalid_params(format!(
                        "argument \"dryRun\" must be a boolean, got {v}"
                    )))
                }
            };
            let statement_index = opt_i64(args, "statementIndex")?;
            let body = Bytes::from(
                json!({
                    "statementIndex": statement_index,
                    "from": from,
                    "to": to,
                    "dryRun": dry_run
                })
                .to_string(),
            );
            api::pipelines::backfill(st, identity(), HeaderMap::new(), Path(id), body).await
        }
        // R8: the DDL closed loop (T-A1) and the backfill job surface
        // (T-A2 ⑤) as tools. ddl_draft is read-only (text composition);
        // execute / evolve run DDL on Doris and ride the write gates; the
        // body args pass through verbatim after the path triple and
        // `confirm` are lifted out.
        "ddl_draft" => {
            let target = ddl_target(args)?;
            let body = ddl_body(args);
            api::datasources::ddl_draft(st, Path(target), body).await
        }
        "ddl_execute" => {
            let target = ddl_target(args)?;
            req_str(args, "ddl")?;
            req_str(args, "keyModel")?;
            let body = ddl_body(args);
            api::datasources::ddl_execute(st, identity(), HeaderMap::new(), Path(target), body)
                .await
        }
        "ddl_evolve" => {
            let target = ddl_target(args)?;
            api::datasources::ddl_evolve(st, identity(), HeaderMap::new(), Path(target)).await
        }
        "backfill_status" => api::pipelines::backfill_jobs(st, Path(req_i64(args, "id")?)).await,
        other => return Err(RpcErr::invalid_params(format!("unknown tool: {other}"))),
    };
    Ok(response)
}

/// The DDL tools' path triple: `(datasourceId, db, table)` — required, typed.
fn ddl_target(args: &Map<String, Value>) -> Result<(i64, String, String), RpcErr> {
    Ok((
        req_i64(args, "datasourceId")?,
        req_str(args, "db")?.to_string(),
        req_str(args, "table")?.to_string(),
    ))
}

/// DDL request body: the args minus the path triple and `confirm` — the
/// DdlDraftBody / DdlExecuteBody fields (keyModel, keyColumn, replicationNum,
/// partition, columnTypes, ddl) pass through verbatim.
fn ddl_body(args: &Map<String, Value>) -> Bytes {
    let mut body = args.clone();
    body.remove("confirm");
    for key in ["datasourceId", "db", "table"] {
        body.remove(key);
    }
    json_bytes(&body)
}

/// `get_pipeline_logs` for a request/response tool: the REST route is an
/// SSE bridge (§4.5), which cannot fold into one JSON-RPC result — this is
/// the same engine tail pull the bridge does for its first screen
/// (`tail=1&limit`, clamped 50..500), as a single snapshot.
async fn logs_snapshot(state: &AppState, id: i64, lines: i64) -> Response {
    let lines = lines.clamp(50, 500);
    let Some(full) = state.store().pipeline_full(id).ok().flatten() else {
        return api::messages::not_found("err.pipeline.not_found", &[&id.to_string()]);
    };
    let Some(node_id) = full.base.assigned_node else {
        return Json(json!({
            "ok": false,
            "error": api::messages::t("err.pipeline.undeployed_plain", &[&id.to_string()]),
        }))
        .into_response();
    };
    let Some(node) = state.store().node(node_id).ok().flatten() else {
        return api::messages::not_found(
            "err.pipeline.assigned_node_missing",
            &[&node_id.to_string()],
        );
    };
    let Some(api_url) = node.api_url else {
        return console_auth::ApiError::new(
            StatusCode::BAD_GATEWAY,
            "ENGINE_UNREACHABLE",
            "node has not reported an api_url yet",
        )
        .into_response();
    };
    let endpoint =
        console_proxy::NodeEndpoint::with_token(api_url, state.bootstrap().global_token());
    match endpoint
        .get_json(
            state.node_client(),
            &format!("/v1/pipelines/{id}/logs?tail=1&limit={lines}"),
        )
        .await
    {
        Ok(v) => Json(v).into_response(),
        Err(e) => console_auth::ApiError::new(
            StatusCode::BAD_GATEWAY,
            "ENGINE_UNREACHABLE",
            e.to_string(),
        )
        .into_response(),
    }
}

// ---------------- tool descriptions / schemas ----------------

fn tool_definitions() -> Vec<Value> {
    TOOLS
        .iter()
        .map(|(name, write)| tool_definition(name, *write))
        .collect()
}

/// The `(method, path)` of the REST operation each tool maps to — exactly
/// the routes registered in the OpenAPI document (the same utoipa
/// annotations that serve /swagger-ui). The pause/resume pair shares the
/// pause route (its `action` argument selects); descriptions name the route
/// they came from.
fn tool_route(name: &str) -> (&'static str, &'static str) {
    match name {
        "list_datasources" => ("GET", "/api/datasources"),
        "list_pipelines" => ("GET", "/api/pipelines"),
        "get_pipeline" => ("GET", "/api/pipelines/{id}"),
        "get_pipeline_stats" => ("GET", "/api/pipelines/{id}/stats"),
        "get_pipeline_logs" => ("GET", "/api/pipelines/{id}/logs"),
        "compile_sql" => ("POST", "/api/pipelines/compile"),
        "create_pipeline" => ("POST", "/api/pipelines"),
        "deploy_pipeline" => ("POST", "/api/pipelines/{id}/deploy"),
        "pause_resume_pipeline" => ("POST", "/api/pipelines/{id}/pause"),
        "trigger_backfill" => ("POST", "/api/pipelines/{id}/backfill"),
        "ddl_draft" => (
            "POST",
            "/api/datasources/{id}/tables/{db}/{table}/ddl/draft",
        ),
        "ddl_execute" => (
            "POST",
            "/api/datasources/{id}/tables/{db}/{table}/ddl/execute",
        ),
        "ddl_evolve" => (
            "POST",
            "/api/datasources/{id}/tables/{db}/{table}/ddl/evolve",
        ),
        "backfill_status" => ("GET", "/api/pipelines/{id}/backfill/jobs"),
        other => unreachable!("tool_route called with unregistered tool {other:?}"),
    }
}

/// The OpenAPI document the tool descriptions are generated from — the
/// same derive that serves /swagger-ui, built once.
static OPENAPI: std::sync::LazyLock<utoipa::openapi::OpenApi> =
    std::sync::LazyLock::new(crate::openapi::ApiDoc::openapi);

/// One tool's description, generated from the OpenAPI document (R8: one
/// source — dropping a handler's doc comment breaks the unit test, not a
/// silently stale copy here). The text is the operation's description (the
/// handler doc comment; its first paragraph when there is only one),
/// prefixed with the route so the tool's path-shaped arguments are readable
/// in context.
fn openapi_tool_description(name: &str) -> String {
    let (method, path) = tool_route(name);
    let item = OPENAPI
        .paths
        .paths
        .get(path)
        .unwrap_or_else(|| panic!("tool route {path} is missing from the OpenAPI document"));
    let operation = match method {
        "GET" => item.get.as_ref(),
        "POST" => item.post.as_ref(),
        other => unreachable!("tool_route produced method {other:?}"),
    }
    .unwrap_or_else(|| panic!("{method} {path} is missing from the OpenAPI document"));
    let text = operation
        .description
        .clone()
        .or_else(|| operation.summary.clone())
        .unwrap_or_default();
    format!("{method} {path} — {text}")
}

fn tool_definition(name: &str, write: bool) -> Value {
    let id_prop = json!({ "type": "integer", "description": "Pipeline id" });
    let ds_mapping = json!({
        "type": "object",
        "description": "Database alias -> datasource id, e.g. {\"shop\": 3}; resolves db.table references",
        "additionalProperties": { "type": ["integer", "string"] }
    });
    let ddl_target_props = json!({
        "datasourceId": { "type": "integer", "description": "Doris/StarRocks datasource id" },
        "db": { "type": "string" },
        "table": { "type": "string" }
    });
    let description = openapi_tool_description(name);
    let mut schema = match name {
        "list_datasources" => json!({
            "type": "object",
            "properties": { "type": { "type": "string", "description": "Registry type filter; omit for all" } },
            "additionalProperties": false
        }),
        "list_pipelines" => json!({
            "type": "object",
            "properties": {
                "page": { "type": "integer", "minimum": 1, "default": 1 },
                "pageSize": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }
            },
            "additionalProperties": false
        }),
        "get_pipeline" => json!({
            "type": "object",
            "properties": { "id": id_prop },
            "required": ["id"],
            "additionalProperties": false
        }),
        "get_pipeline_stats" => json!({
            "type": "object",
            "properties": { "id": id_prop },
            "required": ["id"],
            "additionalProperties": false
        }),
        "get_pipeline_logs" => json!({
            "type": "object",
            "properties": {
                "id": id_prop,
                "lines": { "type": "integer", "minimum": 50, "maximum": 500, "default": 200 }
            },
            "required": ["id"],
            "additionalProperties": false
        }),
        "compile_sql" => json!({
            "type": "object",
            "properties": {
                "sql": { "type": "string", "description": "One or more INSERT INTO ... SELECT ... statements" },
                "dsMapping": ds_mapping,
                "pipelineId": { "type": "integer", "description": "Group naming uses tr.p{id}.s{n}; 0/omitted for unsaved drafts" }
            },
            "required": ["sql"],
            "additionalProperties": false
        }),
        "create_pipeline" => json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "sqlContent": { "type": "string" },
                "dsMapping": ds_mapping,
                "checkpointMs": { "type": "integer", "default": 10000 },
                "preferredNodeId": { "type": "integer" },
                "config": { "type": "object", "description": "Advanced options: reliability/recovery/tuning/groupPrefix/startOffset" }
            },
            "required": ["name", "sqlContent"],
            "additionalProperties": false
        }),
        "deploy_pipeline" => json!({
            "type": "object",
            "properties": {
                "id": id_prop,
                "nodeId": { "type": "integer", "description": "Target node; omit for auto-assignment" }
            },
            "required": ["id"],
            "additionalProperties": false
        }),
        "pause_resume_pipeline" => json!({
            "type": "object",
            "properties": {
                "id": id_prop,
                "action": { "type": "string", "enum": ["pause", "resume"] }
            },
            "required": ["id", "action"],
            "additionalProperties": false
        }),
        "trigger_backfill" => json!({
            "type": "object",
            "properties": {
                "id": id_prop,
                "statementIndex": { "type": "integer", "minimum": 0, "default": 0 },
                "from": { "type": "object", "description": "{\"type\":\"offset\",\"value\":N}" },
                "to": { "type": "object", "description": "{\"type\":\"offset\",\"value\":N}" },
                "dryRun": { "type": "boolean", "default": true, "description": "Plan only; defaults to true" }
            },
            "required": ["id", "from", "to"],
            "additionalProperties": false
        }),
        "ddl_draft" => json!({
            "type": "object",
            "properties": {
                "datasourceId": ddl_target_props["datasourceId"].clone(),
                "db": ddl_target_props["db"].clone(),
                "table": ddl_target_props["table"].clone(),
                "keyColumn": { "type": "string", "description": "Confirmed key column; clears keyColumnGuessed" },
                "keyModel": { "type": "string", "enum": ["unique", "duplicate"], "description": "Key-model override; the recommendation stands when absent" },
                "replicationNum": { "type": "integer", "minimum": 1, "description": "\"replication_num\" property; 1 on a single-BE cluster" },
                "partition": { "type": "boolean", "description": "true = confirmed candidate; false = dropped (no PARTITION BY)" },
                "columnTypes": {
                    "type": "array",
                    "description": "Per-column type overrides [name, vocabulary type]; steers this draft only",
                    "items": { "type": "array", "minItems": 2, "maxItems": 2, "items": { "type": "string" } }
                }
            },
            "required": ["datasourceId", "db", "table"],
            "additionalProperties": false
        }),
        "ddl_execute" => json!({
            "type": "object",
            "properties": {
                "datasourceId": ddl_target_props["datasourceId"].clone(),
                "db": ddl_target_props["db"].clone(),
                "table": ddl_target_props["table"].clone(),
                "ddl": { "type": "string", "description": "The draft text echoed back verbatim" },
                "keyModel": { "type": "string", "enum": ["unique", "duplicate"] }
            },
            "required": ["datasourceId", "db", "table", "ddl", "keyModel"],
            "additionalProperties": false
        }),
        "ddl_evolve" => json!({
            "type": "object",
            "properties": {
                "datasourceId": ddl_target_props["datasourceId"].clone(),
                "db": ddl_target_props["db"].clone(),
                "table": ddl_target_props["table"].clone()
            },
            "required": ["datasourceId", "db", "table"],
            "additionalProperties": false
        }),
        "backfill_status" => json!({
            "type": "object",
            "properties": { "id": id_prop },
            "required": ["id"],
            "additionalProperties": false
        }),
        other => unreachable!("tool_definition called with unregistered tool {other:?}"),
    };
    if write {
        let props = schema
            .get_mut("properties")
            .and_then(Value::as_object_mut)
            .expect("write tool schema has properties");
        props.insert(
            "confirm".to_string(),
            json!({
                "type": "boolean",
                "const": true,
                "description": "Explicit acknowledgement of a state-changing call; rejected unless exactly true."
            }),
        );
        let mut required = schema
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        required.push(Value::String("confirm".to_string()));
        schema["required"] = Value::Array(required);
    }
    json!({
        "name": name,
        "description": description,
        "inputSchema": schema,
        "annotations": { "readOnlyHint": !write }
    })
}

// ---------------- helpers ----------------

/// JSON-RPC error carried as a value (code/message/data triple).
struct RpcErr {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl RpcErr {
    fn new(code: i64, message: impl Into<String>, data: Option<Value>) -> Self {
        Self {
            code,
            message: message.into(),
            data,
        }
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS, message, None)
    }
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = json!({ "code": code, "message": message });
    if let Some(d) = data {
        error["data"] = d;
    }
    json!({ "jsonrpc": "2.0", "id": id, "error": error })
}

/// MCP `tools/call` result envelope: the handler's JSON as text content
/// (LLM-readable), the structured pair (httpStatus, body), and `isError`
/// mirroring the REST status.
fn tool_result(status: StatusCode, body: Value) -> Value {
    let text = match &body {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => serde_json::to_string_pretty(other).unwrap_or_default(),
    };
    json!({
        "content": [ { "type": "text", "text": text } ],
        "structuredContent": { "httpStatus": status.as_u16(), "body": body },
        "isError": !status.is_success(),
    })
}

/// Drain an axum `Response` into its status + parsed JSON body (non-JSON
/// bodies pass through as strings).
async fn response_json(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = match axum::body::to_bytes(response.into_body(), MAX_HANDLER_BODY).await {
        Ok(b) if b.is_empty() => Value::Null,
        Ok(b) => serde_json::from_slice(&b)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into_owned())),
        Err(e) => json!({ "error": { "code": "BODY_READ_FAILED", "message": e.to_string() } }),
    };
    (status, body)
}

/// The session identity write tools run under: the audit actor rides the
/// username (`operator_of` has no X-Operator header to prefer here).
fn mcp_identity(actor: &str) -> AuthContext {
    AuthContext {
        user: AuthUser {
            id: 0,
            username: actor.to_string(),
            display_name: Some("MCP client".to_string()),
            role: "mcp".to_string(),
        },
        token_hash: String::new(),
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Keep the label audit-safe and bounded: drop control characters, trim,
/// truncate to [`MAX_LABEL_CHARS`] on a char boundary.
fn sanitize_label(raw: &str) -> String {
    let cleaned: String = raw.chars().filter(|c| !c.is_control()).collect();
    let cleaned = cleaned.trim();
    if cleaned.chars().count() > MAX_LABEL_CHARS {
        cleaned.chars().take(MAX_LABEL_CHARS).collect()
    } else {
        cleaned.to_string()
    }
}

fn new_session_id() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 16];
    rand::rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn env_truthy(v: Option<&str>) -> bool {
    matches!(
        v.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

// ---- argument marshalling (tool args -> handler extractors) ----

fn req_i64(args: &Map<String, Value>, key: &str) -> Result<i64, RpcErr> {
    opt_i64(args, key)?
        .ok_or_else(|| RpcErr::invalid_params(format!("missing required argument {key:?}")))
}

fn opt_i64(args: &Map<String, Value>, key: &str) -> Result<Option<i64>, RpcErr> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
            .map(Some)
            .ok_or_else(|| RpcErr::invalid_params(format!("argument {key:?} must be an integer"))),
    }
}

fn req_str<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, RpcErr> {
    opt_str(args, key)?
        .ok_or_else(|| RpcErr::invalid_params(format!("missing required argument {key:?}")))
}

fn opt_str<'a>(args: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>, RpcErr> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.as_str())),
        Some(_) => Err(RpcErr::invalid_params(format!(
            "argument {key:?} must be a string"
        ))),
    }
}

fn json_bytes(map: &Map<String, Value>) -> Bytes {
    Bytes::from(Value::Object(map.clone()).to_string())
}

// ---- audit ----

/// Fire-and-forget `MCP_TOOL_CALL` audit row for every tool invocation,
/// whatever the outcome (§7.5: rejected attempts are evidence too).
fn audit_tool_call(
    state: &AppState,
    actor: &str,
    tool: &str,
    args: &Map<String, Value>,
    outcome: &str,
    extra: Value,
) {
    let mut detail = json!({ "tool": tool, "outcome": outcome, "args": abbreviate_args(args) });
    if let (Some(d), Some(x)) = (detail.as_object_mut(), extra.as_object()) {
        for (k, v) in x {
            d.insert(k.clone(), v.clone());
        }
    }
    audit(
        state.store(),
        actor,
        "MCP_TOOL_CALL",
        "mcp_tool",
        Some(tool.to_string()),
        Some(detail.to_string()),
    );
}

/// Audit-safe tool label for calls that did not resolve to a name: the
/// name verbatim when present, a bounded marker otherwise (the raw JSON
/// value never rides the audit row unbounded).
fn tool_label(params: &Value) -> String {
    match params.get("name") {
        Some(Value::String(s)) => s.clone(),
        Some(other) => brief_value(other).to_string(),
        None => "<missing>".to_string(),
    }
}

/// One audit-bounded JSON rendering: serialized then head-truncated to
/// [`MAX_AUDIT_STRING_CHARS`] chars (for values that cannot ride the
/// args map, e.g. a non-object `arguments` payload).
fn brief_value(v: &Value) -> Value {
    let s = serde_json::to_string(v).unwrap_or_default();
    if s.chars().count() > MAX_AUDIT_STRING_CHARS {
        let head: String = s.chars().take(MAX_AUDIT_STRING_CHARS).collect();
        Value::String(format!("{head}...({} chars)", s.chars().count()))
    } else {
        Value::String(s)
    }
}

/// Bound audit noise: string values longer than [`MAX_AUDIT_STRING_CHARS`]
/// are head-truncated with their original length noted (SQL bodies etc.).
fn abbreviate_args(args: &Map<String, Value>) -> Value {
    Value::Object(
        args.iter()
            .map(|(k, v)| {
                let v = match v {
                    Value::String(s) if s.chars().count() > MAX_AUDIT_STRING_CHARS => {
                        let head: String = s.chars().take(MAX_AUDIT_STRING_CHARS).collect();
                        Value::String(format!("{head}...({} chars)", s.chars().count()))
                    }
                    other => other.clone(),
                };
                (k.clone(), v)
            })
            .collect(),
    )
}

// ---------------- rate limit ----------------

/// Global fixed-window limiter (per second). Deliberately trivial: a single
/// shared window, no queues, no per-client tracking.
struct RateLimiter {
    max_per_sec: u32,
    window: Mutex<(Instant, u32)>,
}

impl RateLimiter {
    fn new(max_per_sec: u32) -> Self {
        Self {
            max_per_sec,
            window: Mutex::new((Instant::now(), 0)),
        }
    }

    fn allow(&self) -> bool {
        self.allow_at(Instant::now())
    }

    fn allow_at(&self, now: Instant) -> bool {
        let mut w = self.window.lock().unwrap_or_else(PoisonError::into_inner);
        if now.duration_since(w.0).as_secs() >= 1 {
            *w = (now, 0);
        }
        if w.1 >= self.max_per_sec {
            return false;
        }
        w.1 += 1;
        true
    }
}

// ---------------- tests ----------------

#[cfg(test)]
mod tests {
    use super::*;

    /// §7.5 + R8 surface guard: exactly fourteen tools (the blueprint ten
    /// plus the DDL closed loop and backfill status — within the ≤16
    /// budget), eight read + six write, unique names, every definition
    /// well-formed, `confirm` required on writes, and every description
    /// GENERATED from the OpenAPI document (route prefix + the operation's
    /// text — the same-source rule; a handler losing its doc comment turns
    /// this red, not a silently stale hand-written copy).
    #[test]
    fn tool_table_matches_the_registered_surface() {
        assert_eq!(TOOLS.len(), 14, "fourteen tools (≤16 budget)");
        assert!(TOOLS.len() <= 16, "the R8 tool-face budget");
        let reads = TOOLS.iter().filter(|(_, w)| !w).count();
        let writes = TOOLS.iter().filter(|(_, w)| *w).count();
        assert_eq!((reads, writes), (8, 6));
        let names: Vec<&str> = TOOLS.iter().map(|(n, _)| *n).collect();
        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(names.len(), unique.len(), "tool names must be unique");
        for (name, write) in TOOLS {
            let def = tool_definition(name, *write);
            assert_eq!(def["name"], *name);
            let description = def["description"].as_str().unwrap_or("");
            assert!(
                description.len() > 20,
                "{name}: description must be non-trivial"
            );
            let (method, path) = tool_route(name);
            assert!(
                description.starts_with(&format!("{method} {path}")),
                "{name}: description is route-prefixed: {description}"
            );
            // same-source: the text after the route prefix IS the OpenAPI
            // operation's, not a second hand-written copy
            assert_eq!(
                description,
                openapi_tool_description(name),
                "{name}: description must be the generated one"
            );
            assert!(
                description.contains('\u{4e00}') || description.len() > 40,
                "{name}: the OpenAPI doc comments carry the handlers' text"
            );
            assert_eq!(def["inputSchema"]["type"], "object");
            assert_eq!(def["annotations"]["readOnlyHint"], !*write);
            let required = def["inputSchema"]["required"].as_array().cloned();
            if *write {
                assert!(
                    required.is_some_and(|r| r.contains(&json!("confirm"))),
                    "{name}: confirm must be required"
                );
                assert_eq!(
                    def["inputSchema"]["properties"]["confirm"]["const"], true,
                    "{name}: confirm must be const true"
                );
            } else {
                assert!(
                    !required.unwrap_or_default().contains(&json!("confirm")),
                    "{name}: read tools take no confirm"
                );
            }
        }
    }

    #[test]
    fn env_truthy_accepts_common_spellings() {
        assert!(env_truthy(Some("1")));
        assert!(env_truthy(Some("true")));
        assert!(env_truthy(Some("TRUE")));
        assert!(env_truthy(Some(" yes ")));
        assert!(env_truthy(Some("on")));
        assert!(!env_truthy(None));
        assert!(!env_truthy(Some("")));
        assert!(!env_truthy(Some("0")));
        assert!(!env_truthy(Some("false")));
        assert!(!env_truthy(Some("maybe")));
    }

    #[test]
    fn sanitize_label_bounds_the_actor() {
        assert_eq!(sanitize_label("zcode/1.0"), "zcode/1.0");
        let long = "x".repeat(200);
        assert_eq!(sanitize_label(&long).chars().count(), MAX_LABEL_CHARS);
        assert_eq!(sanitize_label(""), "");
        // multi-byte chars are not split mid-codepoint
        let cjk: String = "汉".repeat(100);
        assert_eq!(sanitize_label(&cjk).chars().count(), MAX_LABEL_CHARS);
        let ctl = "bad\u{0007}label";
        assert_eq!(sanitize_label(ctl), "badlabel");
    }

    #[test]
    fn rate_limiter_blocks_past_budget_and_resets_next_window() {
        let rl = RateLimiter::new(3);
        let t0 = Instant::now();
        assert!(rl.allow_at(t0));
        assert!(rl.allow_at(t0 + std::time::Duration::from_millis(100)));
        assert!(rl.allow_at(t0 + std::time::Duration::from_millis(200)));
        assert!(!rl.allow_at(t0 + std::time::Duration::from_millis(300)));
        assert!(!rl.allow_at(t0 + std::time::Duration::from_millis(900)));
        // new window
        assert!(rl.allow_at(t0 + std::time::Duration::from_secs(1)));
    }
}
