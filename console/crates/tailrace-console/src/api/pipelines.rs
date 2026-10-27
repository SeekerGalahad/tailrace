//! Pipeline endpoints (§4.5): CRUD with the DRAFT/PENDING_CHANGES flow,
//! local SQL compilation (embedded tailrace-parser, §4.6), publish gating +
//! version snapshots, plan push-down with node auto-assignment, the
//! pause/resume/stop action proxies, stats/offsets observation proxies, the
//! SSE log bridge and the metrics trend.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{ApiError, AuthContext, M2mState};
use console_store::PipelineWrite;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use utoipa::ToSchema;

use crate::api::{audit, iso, operator_of};
use crate::compile::{compile_sql, substitute_db_refs, CompileDiag, StatementOut};
use crate::state::AppState;

/// Dispatch ids: epoch-millis-based, unique per push (the engine rejects a
/// repeated dispatchId idempotently, §4.5).
static DISPATCH_SEQ: AtomicU64 = AtomicU64::new(0);

fn next_dispatch_id() -> u64 {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    ms * 1000 + DISPATCH_SEQ.fetch_add(1, Ordering::Relaxed) % 1000
}

// ---------------- DTOs ----------------

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineConfig {
    pub reliability: Option<Value>,
    pub recovery: Option<Value>,
    pub tuning: Option<Value>,
    pub group_prefix: Option<String>,
    pub start_offset: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineUpsertBody {
    pub name: String,
    pub sql_content: String,
    pub ds_mapping: Option<Map<String, Value>>,
    pub checkpoint_ms: Option<i64>,
    pub preferred_node_id: Option<i64>,
    pub config: Option<PipelineConfig>,
    /// PUT only: CAS generation of the datasource edit is not required for
    /// pipelines (single-writer console); accepted and ignored.
    pub generation: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PipelineCreated {
    pub id: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineListItem {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub version: i64,
    pub group_prefix: Option<String>,
    pub consumed_offset_start: String,
    pub assigned_node: Option<Value>,
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PipelinesListResponse {
    pub pipelines: Vec<PipelineListItem>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineDetailOut {
    #[serde(flatten)]
    pub list_item: PipelineListItem,
    pub sql_content: String,
    pub spec_json: Option<String>,
    pub ds_mapping: Option<Map<String, Value>>,
    pub checkpoint_ms: i64,
    pub preferred_node_id: Option<i64>,
    pub deploy_error: Option<String>,
    pub created_at: String,
    /// Advanced config echo (§4.5): the editor re-sends it verbatim on PUT so
    /// a plain save must not wipe the stored five-section options.
    pub config: Option<PipelineConfig>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CompileBody {
    pub sql: String,
    pub ds_mapping: Option<Map<String, Value>>,
    /// Group naming uses tr.p{pipelineId}.s{n}; 0 for unsaved drafts.
    pub pipeline_id: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CompileResponse {
    pub ok: bool,
    pub statements: Vec<StatementOut>,
    pub diagnostics: Vec<CompileDiag>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PublishResponse {
    pub id: i64,
    pub version: i64,
    pub spec_json: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployBody {
    pub node_id: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrendPoint {
    pub ts: String,
    pub consumed: i64,
    pub flushed: i64,
    pub rate_in_rps: Option<f64>,
    pub rate_out_rps: Option<f64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TrendResponse {
    pub points: Vec<TrendPoint>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SqlTemplate {
    pub name: String,
    pub label: String,
    pub sql: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TemplatesResponse {
    pub templates: Vec<SqlTemplate>,
}

// ---------------- shared plumbing ----------------

fn parse_json_body<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, Response> {
    serde_json::from_slice(body).map_err(|e| {
        super::messages::bad_request("err.bad_body", &[&e.to_string()]).into_response()
    })
}

fn load_pipeline(state: &AppState, id: i64) -> Result<console_store::PipelineFull, Response> {
    state
        .store()
        .pipeline_full(id)
        .ok()
        .flatten()
        .ok_or_else(|| {
            super::messages::not_found("err.pipeline.not_found", &[&id.to_string()]).into_response()
        })
}

/// Normalize the dsMapping values to integers ("3" and 3 both accepted).
fn normalize_mapping(mapping: &Map<String, Value>) -> Map<String, Value> {
    mapping
        .iter()
        .map(|(k, v)| {
            let id = v
                .as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()));
            (k.clone(), Value::from(id))
        })
        .collect()
}

fn mapping_json(mapping: Option<&Map<String, Value>>) -> Option<String> {
    mapping.map(|m| serde_json::to_string(&normalize_mapping(m)).unwrap_or_else(|_| "{}".into()))
}

/// consumedOffsetStart = config.startOffset ?? "earliest" (§4.5).
fn consumed_offset_start(config_json: Option<&str>) -> String {
    config_json
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|v| {
            v.get("startOffset")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "earliest".into())
}

fn group_prefix_of(config_json: Option<&str>) -> Option<String> {
    config_json
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|v| {
            v.get("groupPrefix")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn assigned_node_value(state: &AppState, node_id: Option<i64>) -> Option<Value> {
    let id = node_id?;
    state
        .store()
        .node(id)
        .ok()
        .flatten()
        .map(|n| json!({ "id": n.id, "name": n.name }))
}

// ---------------- CRUD ----------------

/// `GET /api/pipelines?page=1&pageSize=20` (§4.5).
#[utoipa::path(
    get,
    path = "/api/pipelines",
    params(("page" = Option<i64>, Query), ("pageSize" = Option<i64>, Query)),
    responses((status = 200, body = PipelinesListResponse))
)]
pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let page = params
        .get("page")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1)
        .max(1);
    let page_size = params
        .get("pageSize")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(20)
        .clamp(1, 100);
    let store = state.store();
    let (rows, total) = match store.list_pipelines_paged(page, page_size) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let config_jsons: HashMap<i64, Option<String>> = rows
        .iter()
        .filter_map(|p| {
            store
                .pipeline_full(p.id)
                .ok()
                .flatten()
                .map(|f| (p.id, f.config_json))
        })
        .collect();
    let pipelines = rows
        .iter()
        .map(|p| PipelineListItem {
            id: p.id,
            name: p.name.clone(),
            status: p.status.clone(),
            version: p.version,
            group_prefix: group_prefix_of(config_jsons.get(&p.id).and_then(|o| o.as_deref())),
            consumed_offset_start: consumed_offset_start(
                config_jsons.get(&p.id).and_then(|o| o.as_deref()),
            ),
            assigned_node: assigned_node_value(&state, p.assigned_node),
            updated_at: iso(Some(&p.updated_at)).unwrap_or_default(),
        })
        .collect();
    Json(PipelinesListResponse {
        pipelines,
        total,
        page,
        page_size,
    })
    .into_response()
}

/// `POST /api/pipelines` — create a draft (§4.5).
#[utoipa::path(
    post,
    path = "/api/pipelines",
    request_body = PipelineUpsertBody,
    responses(
        (status = 201, body = PipelineCreated),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "name taken"),
    )
)]
pub async fn create(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let body: PipelineUpsertBody = match parse_json_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.name.trim().is_empty() {
        return ApiError::bad_request("缺少管道名称").into_response();
    }
    let config = body
        .config
        .as_ref()
        .map(|c| serde_json::to_string(c).unwrap_or_else(|_| "{}".into()));
    let store = state.store();
    let id = match store.create_pipeline(
        body.name.trim(),
        &body.sql_content,
        mapping_json(body.ds_mapping.as_ref()).as_deref(),
        body.checkpoint_ms.unwrap_or(10000),
        body.preferred_node_id,
        config.as_deref(),
    ) {
        Ok(PipelineWrite::Created(id)) => id,
        Ok(PipelineWrite::NameConflict) => {
            return ApiError::new(
                axum::http::StatusCode::CONFLICT,
                "CONFLICT",
                "管道名称已存在",
            )
            .into_response()
        }
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    audit(
        store,
        &operator_of(&ctx, &headers),
        "PIPELINE_CREATE",
        "pipeline",
        Some(id.to_string()),
        Some(json!({"name": body.name.trim()}).to_string()),
    );
    (
        axum::http::StatusCode::CREATED,
        Json(PipelineCreated { id }),
    )
        .into_response()
}

/// `GET /api/pipelines/{id}` — detail (§4.5).
#[utoipa::path(
    get,
    path = "/api/pipelines/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = PipelineDetailOut),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn detail(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let full = match load_pipeline(&state, id) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let p = &full.base;
    Json(PipelineDetailOut {
        list_item: PipelineListItem {
            id: p.id,
            name: p.name.clone(),
            status: p.status.clone(),
            version: p.version,
            group_prefix: group_prefix_of(full.config_json.as_deref()),
            consumed_offset_start: consumed_offset_start(full.config_json.as_deref()),
            assigned_node: assigned_node_value(&state, p.assigned_node),
            updated_at: iso(Some(&p.updated_at)).unwrap_or_default(),
        },
        sql_content: p.sql_content.clone(),
        spec_json: p.spec_json.clone(),
        ds_mapping: p
            .ds_mapping_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<Map<String, Value>>(s).ok()),
        checkpoint_ms: p.checkpoint_ms,
        preferred_node_id: p.preferred_node,
        deploy_error: full.deploy_error.clone(),
        created_at: iso(Some(&p.created_at)).unwrap_or_default(),
        config: full
            .config_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<PipelineConfig>(s).ok()),
    })
    .into_response()
}

/// `PUT /api/pipelines/{id}` — non-DRAFT saves move to PENDING_CHANGES
/// (「已保存为待发布变更，需发布后部署才生效」, §4.5).
#[utoipa::path(
    put,
    path = "/api/pipelines/{id}",
    request_body = PipelineUpsertBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = PipelineCreated),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse),
    )
)]
pub async fn update(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    let body: PipelineUpsertBody = match parse_json_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if load_pipeline(&state, id).is_err() {
        return super::messages::not_found("err.pipeline.not_found", &[&id.to_string()])
            .into_response();
    }
    let config = body
        .config
        .as_ref()
        .map(|c| serde_json::to_string(c).unwrap_or_else(|_| "{}".into()));
    let store = state.store();
    match store.update_pipeline(
        id,
        body.name.trim(),
        &body.sql_content,
        mapping_json(body.ds_mapping.as_ref()).as_deref(),
        body.checkpoint_ms.unwrap_or(10000),
        body.preferred_node_id,
        config.as_deref(),
    ) {
        Ok(PipelineWrite::Created(_)) => {}
        Ok(PipelineWrite::NameConflict) => {
            return ApiError::new(
                axum::http::StatusCode::CONFLICT,
                "CONFLICT",
                "管道名称已存在",
            )
            .into_response()
        }
        Err(e) => return ApiError::internal(&e).into_response(),
    }
    audit(
        store,
        &operator_of(&ctx, &headers),
        "PIPELINE_UPDATE",
        "pipeline",
        Some(id.to_string()),
        None,
    );
    Json(PipelineCreated { id }).into_response()
}

/// `DELETE /api/pipelines/{id}` — RUNNING pipelines are stopped first
/// (graceful stop via the node proxy; an unreachable engine blocks the
/// delete, §4.5).
#[utoipa::path(
    delete,
    path = "/api/pipelines/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 204),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse, description = "engine unreachable during auto-stop"),
    )
)]
pub async fn remove(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let full = match load_pipeline(&state, id) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let store = state.store();
    if full.base.status == "RUNNING" {
        if let Err(e) = proxy_action(&state, id, "stop").await {
            return e;
        }
    }
    if let Err(e) = store.delete_pipeline(id) {
        return ApiError::internal(&e).into_response();
    }
    audit(
        store,
        &operator_of(&ctx, &headers),
        "PIPELINE_DELETE",
        "pipeline",
        Some(id.to_string()),
        None,
    );
    axum::http::StatusCode::NO_CONTENT.into_response()
}

// ---------------- compile / publish ----------------

/// `POST /api/pipelines/compile` — local compile via the embedded
/// tailrace-parser (§4.6: offline editor diagnostics). Diagnostics ride a
/// 400 whose body is the compile shape (not the generic envelope — §4.5's
/// own 400 example).
#[utoipa::path(
    post,
    path = "/api/pipelines/compile",
    request_body = CompileBody,
    responses(
        (status = 200, body = CompileResponse, description = "ok:true with the statement set"),
        (status = 400, body = CompileResponse, description = "ok:false with diagnostics (§4.5 compile 400 shape)"),
    )
)]
pub async fn compile(State(_state): State<AppState>, body: Bytes) -> Response {
    let body: CompileBody = match parse_json_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let mapping = body.ds_mapping.unwrap_or_default();
    let substituted = substitute_db_refs(&body.sql, &normalize_mapping(&mapping));
    let (statements, diagnostics) = compile_sql(&substituted, body.pipeline_id.unwrap_or(0));
    let ok = diagnostics.is_empty();
    let resp = CompileResponse {
        ok,
        statements,
        diagnostics,
    };
    if ok {
        Json(resp).into_response()
    } else {
        (axum::http::StatusCode::BAD_REQUEST, Json(resp)).into_response()
    }
}

/// `POST /api/pipelines/{id}/publish` — gate: every referenced table object
/// resolvable → materialize `${ds:id.db.table}` spec → version+1 → snapshot
/// (§4.5).
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/publish",
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = PublishResponse),
        (status = 400, body = CompileResponse, description = "gate failed: diagnostics"),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn publish(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let full = match load_pipeline(&state, id) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let p = &full.base;
    let mapping: Map<String, Value> = p
        .ds_mapping_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<Map<String, Value>>(s).ok())
        .unwrap_or_default();
    let spec = substitute_db_refs(&p.sql_content, &normalize_mapping(&mapping));
    let (statements, diagnostics) = compile_sql(&spec, id as u64);
    if !diagnostics.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(CompileResponse {
                ok: false,
                statements,
                diagnostics,
            }),
        )
            .into_response();
    }
    // publish gate: every source/sink resolves to a registered table object,
    // and every dim-join reference resolves to a registered kind=dim table
    // object WITH its key column (T-D1 — the engine preflight rejects a plan
    // whose dim metadata did not ride along, and load_dim refuses a dim
    // object without sequence_column; catching both here fails the publish,
    // not the deploy)
    let store = state.store();
    let mut gate: Vec<CompileDiag> = Vec::new();
    for (idx, st) in statements.iter().enumerate() {
        for role_ref in [(&st.source, "源"), (&st.sink, "汇")] {
            if store
                .table_object_for_reference(role_ref.0)
                .ok()
                .flatten()
                .is_none()
            {
                gate.push(CompileDiag {
                    line: idx as i64 + 1,
                    message: super::messages::t(
                        "err.pipeline.statement_table_missing",
                        &[&(idx + 1).to_string(), role_ref.1, role_ref.0],
                    ),
                });
            }
        }
        for d in &st.dims {
            match store.table_object_for_dim_reference(&d.reference) {
                Ok(Some(t)) => {
                    if t.kind != "dim" {
                        gate.push(CompileDiag {
                            line: idx as i64 + 1,
                            message: super::messages::t(
                                "err.pipeline.dim_kind_wrong",
                                &[&(idx + 1).to_string(), &d.reference, &t.kind],
                            ),
                        });
                    } else if t
                        .sequence_column
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .is_none()
                    {
                        gate.push(CompileDiag {
                            line: idx as i64 + 1,
                            message: super::messages::t(
                                "err.pipeline.dim_no_sequence",
                                &[&(idx + 1).to_string(), &d.reference],
                            ),
                        });
                    }
                }
                Ok(None) => gate.push(CompileDiag {
                    line: idx as i64 + 1,
                    message: super::messages::t(
                        "err.pipeline.dim_table_missing",
                        &[&(idx + 1).to_string(), &d.reference],
                    ),
                }),
                Err(e) => return ApiError::internal(&e).into_response(),
            }
        }
    }
    if !gate.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(CompileResponse {
                ok: false,
                statements: Vec::new(),
                diagnostics: gate,
            }),
        )
            .into_response();
    }
    let version = match store.set_pipeline_published(id, &spec, &p.sql_content, &ctx.user.username)
    {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    audit(
        store,
        &operator_of(&ctx, &headers),
        "PIPELINE_PUBLISH",
        "pipeline",
        Some(id.to_string()),
        Some(json!({"version": version}).to_string()),
    );
    Json(PublishResponse {
        id,
        version,
        spec_json: spec,
    })
    .into_response()
}

// ---------------- deploy / recover (plan push) ----------------

/// `POST /api/pipelines/{id}/deploy` — plan push-down with auto-assignment
/// (preferred eligible → fewest running → lowest id, §4.5) and the full plan
/// body per §4.5/engine B.2. Engine `ok:false` records DEPLOY_FAILED and
/// returns 502.
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/deploy",
    request_body = DeployBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine verdict JSON (ok:true + statement count)"),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "no eligible node"),
        (status = 502, body = crate::api::ErrorResponse, description = "engine unreachable or ok:false"),
    )
)]
pub async fn deploy(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    // {"nodeId": null} and an empty body both mean auto-assign
    let node_id: Option<i64> = serde_json::from_slice::<DeployBody>(&body)
        .ok()
        .and_then(|b| b.node_id);
    push_plan(
        &state,
        id,
        node_id,
        "PIPELINE_DEPLOY",
        &operator_of(&ctx, &headers),
    )
    .await
}

/// `POST /api/pipelines/{id}/recover` — re-push the current plan to the
/// target node (default: the currently assigned node); the engine resumes
/// from its offsets ledger (§4.5).
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/recover",
    request_body = DeployBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine verdict JSON"),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse),
    )
)]
pub async fn recover(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    let node_id: Option<i64> = serde_json::from_slice::<DeployBody>(&body)
        .ok()
        .and_then(|b| b.node_id)
        .or_else(|| {
            state
                .store()
                .pipeline(id)
                .ok()
                .flatten()
                .and_then(|p| p.assigned_node)
        });
    push_plan(
        &state,
        id,
        node_id,
        "PIPELINE_RECOVER",
        &operator_of(&ctx, &headers),
    )
    .await
}

// ---------------- plan construction + push ----------------

/// Build and push the plan (§4.5 deploy / recover shared path). Steps:
/// compile the materialized spec → resolve every ref against registered
/// table objects (+ datasource connection params) → pick the node → PUT the
/// plan → record the outcome.
async fn push_plan(
    state: &AppState,
    id: i64,
    requested_node: Option<i64>,
    audit_action: &str,
    operator: &str,
) -> Response {
    let full = match load_pipeline(state, id) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let p = &full.base;
    let Some(spec) = p.spec_json.clone() else {
        return ApiError::bad_request("管道尚未发布（无 spec 快照），先执行发布").into_response();
    };
    let (statements, diagnostics) = compile_sql(&spec, id as u64);
    if !diagnostics.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(CompileResponse {
                ok: false,
                statements,
                diagnostics,
            }),
        )
            .into_response();
    }

    // tables: one plan entry per unique reference — source/sink (`dsId.db.
    // table`) AND dim joins (`dsId.table`, T-D1; the engine preflight counts
    // dim references — their metadata must ride with the plan or the whole
    // plan is rejected)
    let store = state.store();
    let mut refs: Vec<String> = Vec::new();
    for st in &statements {
        for r in [&st.source, &st.sink] {
            if !refs.contains(r) {
                refs.push(r.clone());
            }
        }
        for d in &st.dims {
            if !refs.contains(&d.reference) {
                refs.push(d.reference.clone());
            }
        }
    }
    let mut tables: Vec<Value> = Vec::new();
    let mut missing: Vec<String> = Vec::new();
    for reference in &refs {
        match build_plan_table(store, reference) {
            Ok(Some(t)) => tables.push(t),
            Ok(None) => missing.push(reference.clone()),
            Err(e) => return ApiError::internal(&e).into_response(),
        }
    }
    if !missing.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(CompileResponse {
                ok: false,
                statements: Vec::new(),
                diagnostics: vec![CompileDiag {
                    line: 1,
                    message: super::messages::t(
                        "err.pipeline.table_objects_missing",
                        &[&missing.join(", ")],
                    ),
                }],
            }),
        )
            .into_response();
    }

    // node selection: explicit → preferred-eligible → fewest running → id
    let node = match pick_node(state, requested_node.or(p.preferred_node)).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    let Some(api_url) = node.api_url.clone() else {
        return super::messages::respond(
            axum::http::StatusCode::CONFLICT,
            "NO_ELIGIBLE_NODE",
            "err.pipeline.node_no_api_url",
            &[&node.name],
        )
        .into_response();
    };

    // plan body per §4.5 (fields map 1:1 onto the engine's PUT /v1/.../plan)
    let mut plan = json!({
        "pipelineId": id,
        "checkpointMs": p.checkpoint_ms,
        "dispatchId": next_dispatch_id(),
        "tables": tables,
        "sqlContent": spec,
    });
    if p.status == "PAUSED" {
        plan["initialState"] = json!("PAUSED");
    }
    if let Some(config) = full
        .config_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
    {
        if config.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
            plan["options"] = config;
        }
    }

    let endpoint =
        console_proxy::NodeEndpoint::with_token(api_url, state.bootstrap().global_token());
    match endpoint.put_plan(state.node_client(), id, &plan).await {
        Ok(verdict) if verdict.get("ok").and_then(Value::as_bool).unwrap_or(false) => {
            if let Err(e) = store.set_pipeline_deployed(id, node.id) {
                return ApiError::internal(&e).into_response();
            }
            audit(
                store,
                operator,
                audit_action,
                "pipeline",
                Some(id.to_string()),
                Some(json!({"nodeId": node.id, "nodeName": node.name, "dispatchId": plan["dispatchId"]}).to_string()),
            );
            Json(verdict).into_response()
        }
        Ok(verdict) => {
            let error = verdict
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| verdict.to_string());
            let _ = store.set_pipeline_deploy_failed(id, &error);
            super::messages::respond(
                axum::http::StatusCode::BAD_GATEWAY,
                "ENGINE_REJECTED",
                "err.pipeline.engine_assembly_failed",
                &[&error],
            )
            .into_response()
        }
        Err(e) => {
            let _ = store.set_pipeline_deploy_failed(id, &e.to_string());
            ApiError::new(
                axum::http::StatusCode::BAD_GATEWAY,
                "ENGINE_UNREACHABLE",
                e.to_string(),
            )
            .into_response()
        }
    }
}

/// Resolve one `"dsId.db.table"` (source/sink) or `"dsId.table"` (dim join,
/// T-D1) reference into the plan `tables[]` entry (§4.5: kind/format/columns
/// from the table object, bootstrap / streamLoadUrl from the datasource
/// conn, authEnv bound per datasource — values never enter the plan).
/// Ok(None) = not registered.
fn build_plan_table(
    store: &console_store::Store,
    reference: &str,
) -> anyhow::Result<Option<Value>> {
    let table = match store.table_object_for_reference(reference)? {
        Some(t) => Some(t),
        None => store.table_object_for_dim_reference(reference)?,
    };
    let Some(table) = table else {
        return Ok(None);
    };
    let Some(ds) = store.datasource(table.datasource_id)? else {
        return Ok(None);
    };
    let conn: Value = serde_json::from_str(&ds.conn_json).unwrap_or(Value::Null);
    let columns: Vec<(String, String)> = store
        .table_object_columns_json(table.id)?
        .flatten()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let kind = tailrace_catalog::normalize_kind(&table.kind);
    let has_credential = store.credential(ds.id)?.is_some();
    // authEnv: the engine's credential_from_env only accepts TAILRACE_-prefixed
    // names (engine-v1.md §四); the design doc §4.5 example ("TR_DS_4") predates
    // that constraint — see docs/protocol/console-api.md deviations.
    let auth_env = matches!(kind.as_str(), "doris" | "starrocks")
        .then(|| format!("TAILRACE_DS_{}", ds.id))
        .filter(|_| has_credential);
    // stream_load_url: the engine's DorisSink PUTs this verbatim, and the
    // /v1 contract (docs/protocol/engine-v1.md example) is the FULL endpoint
    // `http://be:8040/api/{db}/{table}/_stream_load` — compose it from the
    // datasource's BE base + this table's db/table.
    let stream_load_url = conn
        .get("streamLoadUrl")
        .and_then(Value::as_str)
        .filter(|_| matches!(kind.as_str(), "doris" | "starrocks"))
        .map(|base| {
            format!(
                "{}/api/{}/{}/_stream_load",
                base.trim_end_matches('/'),
                table.db_name,
                table.table_name
            )
        });
    // bootstrap: kafka keeps its broker list from connJson; dim and jdbc
    // tables are served by the JDBC factory, which takes a `mysql://` /
    // `postgres://` URL (engine dialect selection) — the console's
    // mysql/postgresql datasources store `jdbc:mysql://…` / `jdbc:postgresql:
    // //…`, so the `jdbc:` prefix is stripped here.
    let bootstrap = match kind.as_str() {
        "dim" | "jdbc" => conn.get("jdbcUrl").and_then(Value::as_str).map(|url| {
            url.trim()
                .strip_prefix("jdbc:")
                .unwrap_or(url.trim())
                .to_string()
        }),
        _ => conn
            .get("bootstrap")
            .and_then(Value::as_str)
            .map(str::to_string),
    };
    // sequence_column (T-D1): the declared key column rides the plan's table
    // metadata verbatim — dim full-table load pagination key (load_dim
    // refuses without it), doris UNIQUE KEY dedup basis, jdbc cursor column.
    let sequence_column = table
        .sequence_column
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    Ok(Some(json!({
        "reference": reference,
        "kind": kind,
        "format": table.format,
        "columns": columns,
        "bootstrap": bootstrap,
        // table-entry keys follow the engine /v1 plan contract (snake_case —
        // tailrace-catalog PlanTableObject has no serde renames; the camelCase
        // keys here were silently dropped, killing every doris deploy with
        // "must declare auth_env" — T11 first-run finding)
        "stream_load_url": stream_load_url,
        "auth_env": auth_env,
        "sequence_column": sequence_column,
    })))
}

/// Auto-assignment (§4.5): explicit id → preferred (if ONLINE) → eligible
/// node with fewest RUNNING pipelines → lowest id. Eligible = ONLINE by
/// heartbeat within 90s and advertising an api_url.
async fn pick_node(
    state: &AppState,
    preferred: Option<i64>,
) -> Result<console_store::Node, Response> {
    let store = state.store();
    let nodes = store
        .list_nodes()
        .map_err(|e| ApiError::internal(&e).into_response())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let eligible = |n: &console_store::Node| -> bool {
        n.status == "ONLINE"
            && n.api_url.is_some()
            && n.last_heartbeat
                .as_deref()
                .and_then(crate::api::nodes_admin::sqlite_ts_to_epoch)
                .map(|ts| now - ts <= 90)
                .unwrap_or(false)
    };
    if let Some(id) = preferred {
        if let Some(n) = nodes.iter().find(|n| n.id == id) {
            if eligible(n) {
                return Ok(n.clone());
            }
        }
    }
    let running: HashMap<i64, i64> = store
        .running_pipeline_counts()
        .unwrap_or_default()
        .into_iter()
        .collect();
    nodes
        .iter()
        .filter(|n| eligible(n))
        .min_by_key(|n| (running.get(&n.id).copied().unwrap_or(0), n.id))
        .cloned()
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::CONFLICT,
                "NO_ELIGIBLE_NODE",
                "没有可指派的在线节点（需心跳 90s 内在线且已上报 api_url）",
            )
            .into_response()
        })
}

// ---------------- actions + observation proxies ----------------

/// Shared pause/resume/stop proxy (§4.5): engine POST
/// `/v1/pipelines/{id}/{action}` → console DB status write-back.
async fn proxy_action(state: &AppState, id: i64, action: &str) -> Result<Value, Response> {
    let full = load_pipeline(state, id)?;
    let store = state.store();
    let Some(node_id) = full.base.assigned_node else {
        if action == "stop" {
            // stop of an undeployed pipeline: idempotent return-to-PUBLISHED
            let _ = store.set_pipeline_status(id, "PUBLISHED", true);
            return Ok(json!({"pipelineId": id, "status": "PUBLISHED"}));
        }
        return Err(
            super::messages::bad_request("err.pipeline.undeployed", &[&id.to_string()])
                .into_response(),
        );
    };
    let node = store.node(node_id).ok().flatten().ok_or_else(|| {
        super::messages::not_found(
            "err.pipeline.assigned_node_missing",
            &[&node_id.to_string()],
        )
        .into_response()
    })?;
    let Some(api_url) = node.api_url else {
        return Err(ApiError::new(
            axum::http::StatusCode::BAD_GATEWAY,
            "ENGINE_UNREACHABLE",
            "节点未上报 api_url",
        )
        .into_response());
    };
    let endpoint =
        console_proxy::NodeEndpoint::with_token(api_url, state.bootstrap().global_token());
    match endpoint
        .pipeline_action(state.node_client(), id, action)
        .await
    {
        Ok(verdict) => {
            let ok = verdict.get("ok").and_then(Value::as_bool).unwrap_or(false);
            let status = verdict.get("status").and_then(Value::as_str).unwrap_or("");
            if ok {
                let (db_status, clear) = match action {
                    "pause" => ("PAUSED", false),
                    "resume" => ("RUNNING", false),
                    "stop" => ("PUBLISHED", true), // 回已发布态，可再部署
                    _ => (status, false),
                };
                let _ = store.set_pipeline_status(id, db_status, clear);
            }
            Ok(verdict)
        }
        Err(e) => Err(ApiError::new(
            axum::http::StatusCode::BAD_GATEWAY,
            "ENGINE_UNREACHABLE",
            e.to_string(),
        )
        .into_response()),
    }
}

/// `POST /api/pipelines/{id}/pause` — §4.5 action proxy.
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/pause",
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine verdict: {\"pipelineId\":42,\"status\":\"PAUSED\"}"),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse, description = "engine unreachable"),
    )
)]
pub async fn pause(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    run_action(state, ctx, headers, id, "pause").await
}

/// `POST /api/pipelines/{id}/resume` — §4.5 action proxy.
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/resume",
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine verdict: {\"pipelineId\":42,\"status\":\"RUNNING\"}"),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse),
    )
)]
pub async fn resume(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    run_action(state, ctx, headers, id, "resume").await
}

/// `POST /api/pipelines/{id}/stop` — §4.5 action proxy (graceful stop; the
/// pipeline returns to PUBLISHED and can be re-deployed).
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/stop",
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine verdict: {\"pipelineId\":42,\"status\":\"STOPPED\"}"),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 502, body = crate::api::ErrorResponse),
    )
)]
pub async fn stop(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    run_action(state, ctx, headers, id, "stop").await
}

async fn run_action(
    state: AppState,
    ctx: AuthContext,
    headers: axum::http::HeaderMap,
    id: i64,
    action: &'static str,
) -> Response {
    match proxy_action(&state, id, action).await {
        Ok(v) => {
            audit(
                state.store(),
                &operator_of(&ctx, &headers),
                "PIPELINE_ACTION",
                "pipeline",
                Some(id.to_string()),
                Some(json!({"action": action}).to_string()),
            );
            Json(v).into_response()
        }
        Err(r) => r,
    }
}

/// `GET /api/pipelines/{id}/stats` — engine stats proxy, body verbatim; the
/// engine's own ok:false ("not registered") passes through on 200 for the
/// frontend's degrade branch (§4.5).
#[utoipa::path(
    get,
    path = "/api/pipelines/{id}/stats",
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine stats JSON verbatim (ok:false = not registered)"),
        (status = 502, body = crate::api::ErrorResponse, description = "engine unreachable"),
    )
)]
pub async fn stats(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    proxy_get(&state, id, "stats").await
}

/// `GET /api/pipelines/{id}/offsets` — engine offsets proxy (§4.5/§4.6: the
/// one planned engine-side addition; ledger truth, works for stopped
/// pipelines).
#[utoipa::path(
    get,
    path = "/api/pipelines/{id}/offsets",
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine offsets JSON verbatim"),
        (status = 502, body = crate::api::ErrorResponse),
    )
)]
pub async fn offsets(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    proxy_get(&state, id, "offsets").await
}

async fn proxy_get(state: &AppState, id: i64, resource: &str) -> Response {
    let full = match load_pipeline(state, id) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let Some(node_id) = full.base.assigned_node else {
        return Json(json!({
            "ok": false,
            "error": super::messages::t(
                "err.pipeline.undeployed_plain",
                &[&id.to_string()],
            ),
        }))
        .into_response();
    };
    let node = match state.store().node(node_id).ok().flatten() {
        Some(n) => n,
        None => {
            return super::messages::not_found(
                "err.pipeline.assigned_node_missing",
                &[&node_id.to_string()],
            )
            .into_response()
        }
    };
    let Some(api_url) = node.api_url else {
        return ApiError::new(
            axum::http::StatusCode::BAD_GATEWAY,
            "ENGINE_UNREACHABLE",
            "节点未上报 api_url",
        )
        .into_response();
    };
    let endpoint =
        console_proxy::NodeEndpoint::with_token(api_url, state.bootstrap().global_token());
    match endpoint
        .get_json(
            state.node_client(),
            &format!("/v1/pipelines/{id}/{resource}"),
        )
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

// ---------------- SSE log bridge (§4.5 / A5) ----------------

/// `GET /api/pipelines/{id}/logs?lines=200&after_seq=N` — SSE bridge over
/// the engine's cursor API: initial `tail=1&limit` (clamped 50..500), then
/// 1.5s `after_seq` polling; keepalive every 15s; session ends at 300s or
/// after 5 consecutive engine failures (eof event, §4.5). A client-supplied
/// `after_seq>0` seeds the cursor (log continuation on reconnect — no tail
/// replay; the engine only returns entries past that global seq).
#[utoipa::path(
    get,
    path = "/api/pipelines/{id}/logs",
    params(
        ("id" = i64, Path),
        ("lines" = Option<usize>, Query, description = "first-screen tail, clamped 50..500"),
        ("after_seq" = Option<u64>, Query, description = "resume cursor: skip the tail pull, stream only entries past this global seq"),
    ),
    responses((status = 200, description = "text/event-stream: events log / eof, : keepalive"))
)]
pub async fn logs(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let lines = params
        .get("lines")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200)
        .clamp(50, 500);
    // Reconnect cursor (§4.5 log continuation): 0/absent = fresh first
    // screen (tail pull); >0 = resume past that seq (incremental only).
    let after_seq = params
        .get("after_seq")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let full = match load_pipeline(&state, id) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let api_url = full.base.assigned_node.and_then(|node_id| {
        state
            .store()
            .node(node_id)
            .ok()
            .flatten()
            .and_then(|n| n.api_url)
    });

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, std::convert::Infallible>>(64);
    let stream_client = state.stream_client().clone();
    // engine H1: the /v1 proxy calls carry the console's shared node token
    // (captured before the spawn — the engine middleware requires it)
    let node_token = state.bootstrap().global_token().to_string();
    tokio::spawn(async move {
        let mut consecutive_failures = 0u32;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(300);
        let Some(api_url) = api_url else {
            let _ = send_event(&tx, "eof", &json!({"reason": "not_deployed"})).await;
            return;
        };
        let endpoint = console_proxy::NodeEndpoint::with_token(api_url, node_token);
        // Client cursor seed: after_seq>0 skips the tail pull entirely (the
        // `cursor == 0` branch below) so a reconnect never replays the tail.
        let mut cursor: u64 = after_seq;
        loop {
            let page = if cursor == 0 {
                endpoint
                    .get_json(
                        &stream_client,
                        &format!("/v1/pipelines/{id}/logs?tail=1&limit={lines}"),
                    )
                    .await
            } else {
                endpoint
                    .get_json(
                        &stream_client,
                        &format!("/v1/pipelines/{id}/logs?after_seq={cursor}&limit=500"),
                    )
                    .await
            };
            match page {
                Ok(v) => {
                    consecutive_failures = 0;
                    cursor = v.get("lastSeq").and_then(Value::as_u64).unwrap_or(cursor);
                    if let Some(entries) = v.get("entries").and_then(Value::as_array) {
                        let mut dropped = false;
                        for e in entries {
                            if send_event(&tx, "log", e).await.is_err() {
                                dropped = true; // client went away
                                break;
                            }
                        }
                        if dropped {
                            return;
                        }
                    }
                }
                Err(_) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= 5 {
                        let _ =
                            send_event(&tx, "eof", &json!({"reason": "engine_unreachable"})).await;
                        return;
                    }
                }
            }
            // poll cadence 1.5s; hard session end at 300s (§4.5). Keepalive
            // frames come from the SSE layer's KeepAlive (15s, ": keepalive").
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(1500)) => {}
                _ = tokio::time::sleep_until(deadline) => {
                    let _ = send_event(&tx, "eof", &json!({"reason": "timeout"})).await;
                    return;
                }
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
}

async fn send_event(
    tx: &tokio::sync::mpsc::Sender<Result<Event, std::convert::Infallible>>,
    name: &str,
    data: &Value,
) -> Result<(), tokio::sync::mpsc::error::SendError<Result<Event, std::convert::Infallible>>> {
    tx.send(Ok(Event::default().event(name).data(data.to_string())))
        .await
}

// ---------------- trend + templates ----------------

/// `GET /api/pipelines/{id}/metrics/trend?hours=24` — console-side 5-minute
/// buckets from heartbeat-piggybacked pipeline_metrics; rates derived from
/// consecutive bucket deltas (§4.5).
#[utoipa::path(
    get,
    path = "/api/pipelines/{id}/metrics/trend",
    params(("id" = i64, Path), ("hours" = Option<i64>, Query, description = "window, clamped 1..168")),
    responses(
        (status = 200, body = TrendResponse),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn metrics_trend(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if load_pipeline(&state, id).is_err() {
        return super::messages::not_found("err.pipeline.not_found", &[&id.to_string()])
            .into_response();
    }
    let hours = params
        .get("hours")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(24)
        .clamp(1, 168);
    let buckets = match state.store().pipeline_metrics_trend(id, hours) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let mut points: Vec<TrendPoint> = Vec::with_capacity(buckets.len());
    let mut prev: Option<(i64, i64)> = None;
    for b in buckets {
        let rate_in = prev.map(|(c, _)| (b.consumed - c) as f64 / 300.0);
        let rate_out = prev.map(|(_, f)| (b.flushed - f) as f64 / 300.0);
        points.push(TrendPoint {
            ts: format!("{}:00Z", b.ts),
            consumed: b.consumed,
            flushed: b.flushed,
            rate_in_rps: rate_in,
            rate_out_rps: rate_out,
        });
        prev = Some((b.consumed, b.flushed));
    }
    Json(TrendResponse { points }).into_response()
}

/// `GET /api/pipelines/templates` — SQL templates embedded in the binary
/// (§4.5; the db.table form — dsMapping applies at compile time).
#[utoipa::path(get, path = "/api/pipelines/templates", responses((status = 200, body = TemplatesResponse)))]
pub async fn templates() -> Response {
    Json(TemplatesResponse {
        templates: vec![SqlTemplate {
            name: "kafka-ogg-to-doris".into(),
            label: "OGG 单表直通".into(),
            sql: "-- 模板：OGG 单表直通（kafka ogg → doris；dsMapping 映射 shop/ods 两库）\n\
                  INSERT INTO ods.orders SELECT id, payload, op_type FROM shop.orders WHERE op_type IN ('I','U')"
                .into(),
        }],
    })
    .into_response()
}
