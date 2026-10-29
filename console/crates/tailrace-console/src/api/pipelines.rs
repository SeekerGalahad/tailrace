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

/// One pipeline whose published SQL declares a partial-column write (INSERT
/// column list) on a given table (T-A7 mutex, evolve side). `deployed` =
/// assigned to a node and RUNNING/PAUSED — the state in which its loads
/// would collide with a schema change.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartialUpdatePipeline {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub deployed: bool,
    /// The declared column subset (the INSERT column list).
    pub columns: Vec<String>,
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

// ---------------- T-A7 partial-column declarations ----------------

/// Per-sink partial-column declarations of a compiled statement set, as the
/// plan carries them: one entry per sink that declares a list. A sink is
/// ONE plan table entry whatever the number of statements writing it, so
/// every statement on the same sink must declare the same thing — the same
/// list, or no list at all; a mismatch (two lists, or list vs. bare) is a
/// compile diagnostic, not a silent pick.
fn sink_partial_declarations(
    statements: &[StatementOut],
) -> Result<HashMap<String, Vec<String>>, CompileDiag> {
    let mut seen: HashMap<&str, (usize, Option<&Vec<String>>)> = HashMap::new();
    for (i, st) in statements.iter().enumerate() {
        match seen.get(st.sink.as_str()) {
            Some((j, prev)) if *prev != st.partial_columns.as_ref() => {
                return Err(CompileDiag {
                    line: i as i64 + 1,
                    message: super::messages::t(
                        "err.pipeline.partial_sink_conflict",
                        &[&(i + 1).to_string(), &(j + 1).to_string(), &st.sink],
                    ),
                });
            }
            Some(_) => {}
            None => {
                seen.insert(&st.sink, (i, st.partial_columns.as_ref()));
            }
        }
    }
    Ok(seen
        .into_iter()
        .filter_map(|(sink, (_, cols))| cols.map(|c| (sink.to_string(), c.clone())))
        .collect())
}

/// Publish-gate checks of one statement's partial-column declaration
/// against its registered sink (Doris semantics, fail-fast here instead of
/// at the first load): the sink is kind=doris (starrocks partial update is
/// a different load contract the engine refuses; kafka/file have no such
/// mode), its key model is on record and `unique` (partial update is
/// UNIQUE KEY merge-on-write only — a DUPLICATE KEY table fails the load),
/// and every declared column is a registered column (the engine's sink
/// builder checks the same subset — same names, exact match).
fn partial_declaration_diags(
    store: &console_store::Store,
    idx: usize,
    st: &StatementOut,
    sink: &console_store::TableObject,
) -> anyhow::Result<Vec<CompileDiag>> {
    let Some(declared) = st.partial_columns.as_ref() else {
        return Ok(Vec::new());
    };
    let line = idx as i64 + 1;
    let n = (idx + 1).to_string();
    let mut out = Vec::new();
    let kind = tailrace_catalog::normalize_kind(&sink.kind);
    if kind != "doris" {
        out.push(CompileDiag {
            line,
            message: super::messages::t("err.pipeline.partial_sink_kind", &[&n, &st.sink, &kind]),
        });
        // the rest is Doris-specific — one diagnostic says enough
        return Ok(out);
    }
    match sink.key_model.as_deref() {
        None => out.push(CompileDiag {
            line,
            message: super::messages::t("err.pipeline.partial_key_model_missing", &[&n, &st.sink]),
        }),
        Some("unique") => {}
        Some(other) => out.push(CompileDiag {
            line,
            message: super::messages::t(
                "err.pipeline.partial_key_model_wrong",
                &[&n, &st.sink, other],
            ),
        }),
    }
    let registered: Vec<String> = store
        .table_object_columns_json(sink.id)?
        .flatten()
        .and_then(|s| serde_json::from_str::<Vec<(String, String)>>(&s).ok())
        .unwrap_or_default()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    for col in declared {
        if !registered.contains(col) {
            out.push(CompileDiag {
                line,
                message: super::messages::t(
                    "err.pipeline.partial_column_unknown",
                    &[&n, &st.sink, col],
                ),
            });
        }
    }
    Ok(out)
}

/// The partial-declaration gate over a whole compiled statement set: every
/// statement carrying an INSERT column list is checked against its
/// registered sink (`partial_declaration_diags`). Run at publish AND again
/// at every plan push (deploy / recover) and backfill accept — the spec
/// snapshot is what gets pushed, and two things can make a snapshot's
/// declaration invalid after it was published: the sink's record moved
/// (key model re-registered), or the snapshot predates T-A7, when the
/// parser ignored the column list (full-row writes) and no gate ran at all.
/// Re-gating keeps such a snapshot from silently flipping into partial mode
/// on its first post-upgrade push (a DUPLICATE KEY table would then fail
/// every stream load — "Only unique key merge on write support partial
/// update"). Unregistered sinks are skipped here: the callers report them
/// as missing table objects. Undeclared statements cost nothing.
fn partial_declarations_gate(
    store: &console_store::Store,
    statements: &[StatementOut],
) -> anyhow::Result<Vec<CompileDiag>> {
    let mut out = Vec::new();
    for (idx, st) in statements.iter().enumerate() {
        if st.partial_columns.is_none() {
            continue;
        }
        let Some(sink) = store.table_object_for_reference(&st.sink)? else {
            continue;
        };
        out.extend(partial_declaration_diags(store, idx, st, &sink)?);
    }
    Ok(out)
}

/// Pipelines whose published SQL declares a partial-column write on
/// `table` (T-A7 mutex, evolve side — console knowledge only, no Doris
/// round trip): every pipeline referencing the table, recompiled from its
/// spec snapshot, kept when a statement targets this sink WITH an INSERT
/// column list. `deployed` marks the ones whose loads are live.
pub(crate) fn partial_update_pipelines_on(
    store: &console_store::Store,
    table: &console_store::TableObject,
) -> anyhow::Result<Vec<PartialUpdatePipeline>> {
    let sink_ref = format!(
        "{}.{}.{}",
        table.datasource_id, table.db_name, table.table_name
    );
    let mut out = Vec::new();
    for pid in store.pipelines_referencing_table(table)? {
        let Some(p) = store.pipeline(pid)? else {
            continue;
        };
        let Some(spec) = p.spec_json.as_deref() else {
            continue;
        };
        let (statements, diags) = compile_sql(spec, pid as u64);
        if !diags.is_empty() {
            // a snapshot that no longer compiles cannot be deployed either
            continue;
        }
        let Some(columns) = statements
            .iter()
            .find(|s| s.sink == sink_ref && s.partial_columns.is_some())
            .and_then(|s| s.partial_columns.clone())
        else {
            continue;
        };
        out.push(PartialUpdatePipeline {
            id: p.id,
            name: p.name,
            deployed: p.assigned_node.is_some()
                && matches!(p.status.as_str(), "RUNNING" | "PAUSED"),
            status: p.status,
            columns,
        });
    }
    Ok(out)
}

/// The schema-change jobs still in flight among what `SHOW ALTER TABLE
/// COLUMN` listed: anything not FINISHED / CANCELLED (PENDING, WAITING_TXN,
/// RUNNING). Doris refuses partial loads on such a table (T-A7 mutex,
/// deploy side).
fn alter_jobs_in_flight(
    jobs: &[tailrace_metadata::AlterColumnJob],
) -> Vec<&tailrace_metadata::AlterColumnJob> {
    jobs.iter()
        .filter(|j| {
            !matches!(
                j.state.to_ascii_uppercase().as_str(),
                "FINISHED" | "CANCELLED"
            )
        })
        .collect()
}

/// The 409 SCHEMA_CHANGE_IN_PROGRESS refusal for one partial-column sink
/// whose table has schema-change jobs in flight: names the table and the
/// jobs in the message, carries `refs.{sink,table,jobs[{jobId,state}]}`
/// (the shape the deploy / recover / backfill OpenAPI descriptions promise).
/// Pure — the live `SHOW ALTER TABLE COLUMN` answer is the caller's; CI has
/// no Doris to put a job in flight, so this is where the response shape is
/// locked (unit test below).
fn schema_change_refusal(
    sink_ref: &str,
    table: &str,
    in_flight: &[&tailrace_metadata::AlterColumnJob],
) -> Response {
    let states: Vec<String> = in_flight
        .iter()
        .map(|j| format!("{}={}", j.job_id, j.state))
        .collect();
    let mut refs = Map::new();
    refs.insert("sink".into(), json!(sink_ref));
    refs.insert("table".into(), json!(table));
    refs.insert(
        "jobs".into(),
        json!(in_flight
            .iter()
            .map(|j| json!({"jobId": j.job_id, "state": j.state}))
            .collect::<Vec<_>>()),
    );
    super::messages::respond_with_refs(
        axum::http::StatusCode::CONFLICT,
        "SCHEMA_CHANGE_IN_PROGRESS",
        "err.pipeline.partial_schema_change",
        &[table, &states.join("、")],
        refs,
    )
}

/// T-A7 mutex, deploy side: before a plan with partial-column sinks is
/// pushed (deploy / recover — and a backfill accepted), every such sink is
/// checked live for a schema-change job in flight — Doris would refuse the
/// partial loads, so the console refuses the push. `Err(response)` is the
/// refusal: 409 SCHEMA_CHANGE_IN_PROGRESS naming the table and the jobs, or
/// 502 UNAVAILABLE when the FE could not be asked (the DDL endpoints' stance
/// for a failed DESC — a partial-update deploy needs the answer, not a
/// guess). Sinks without a declaration cost nothing: no FE round trip.
async fn schema_change_gate(
    state: &AppState,
    partials: &HashMap<String, Vec<String>>,
) -> Result<(), Response> {
    let store = state.store();
    let mut sinks: Vec<&String> = partials.keys().collect();
    sinks.sort(); // deterministic order (and error) across runs
    for sink_ref in sinks {
        let Some(t) = store
            .table_object_for_reference(sink_ref)
            .map_err(|e| ApiError::internal(&e).into_response())?
        else {
            continue; // unregistered sinks are reported by the caller
        };
        let Some(ds) = store
            .datasource(t.datasource_id)
            .map_err(|e| ApiError::internal(&e).into_response())?
        else {
            continue;
        };
        let spec = crate::api::datasources::conn_spec(state, &ds)?;
        let table = format!("{}.{}", t.db_name, t.table_name);
        let jobs = tailrace_metadata::doris_alter_column_jobs(&spec, &t.db_name, &t.table_name)
            .await
            .map_err(|e| {
                crate::api::datasources::unavailable(
                    &super::messages::t("err.pipeline.partial_precheck_failed", &[&table]),
                    e,
                )
            })?;
        let in_flight = alter_jobs_in_flight(&jobs);
        if !in_flight.is_empty() {
            return Err(schema_change_refusal(sink_ref, &table, &in_flight));
        }
    }
    Ok(())
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
    // not the deploy). T-A7: a partial-column declaration must fit its
    // registered sink (doris, keyModel=unique, declared ⊆ registered) and be
    // the same for every statement on that sink.
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
    // partial declarations against their registered sinks (unregistered
    // sinks were reported as missing above)
    match partial_declarations_gate(store, &statements) {
        Ok(d) => gate.extend(d),
        Err(e) => return ApiError::internal(&e).into_response(),
    }
    // one partial declaration per sink across statements (the plan carries
    // one table entry per sink)
    if let Err(d) = sink_partial_declarations(&statements) {
        gate.push(d);
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
/// returns 502. T-A7: the snapshot's partial-column declarations are
/// re-gated against their registered sinks (400 diagnostics, same as
/// publish — a pre-A7 snapshot or a moved key model never flips into partial
/// mode unchecked), then a plan with partial-column sinks is checked live
/// for a schema change in flight on each such sink (409
/// SCHEMA_CHANGE_IN_PROGRESS; FE unreachable → 502 UNAVAILABLE).
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/deploy",
    request_body = DeployBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine verdict JSON (ok:true + statement count)"),
        (status = 400, body = crate::api::ErrorResponse, description = "compile diagnostics of the spec snapshot — including the T-A7 partial-declaration gate re-run at push (kind=doris, keyModel=unique, declared ⊆ registered)"),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "NO_ELIGIBLE_NODE — no eligible node; SCHEMA_CHANGE_IN_PROGRESS — a partial-column sink has an ADD COLUMN job in flight (refs.table / jobs), nothing pushed"),
        (status = 502, body = crate::api::ErrorResponse, description = "ENGINE_UNREACHABLE / ENGINE_REJECTED (ok:false); UNAVAILABLE — the partial-update precheck could not ask the Doris FE"),
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
/// from its offsets ledger (§4.5). Same T-A7 schema-change precheck as
/// deploy (partial-column sinks only).
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/recover",
    request_body = DeployBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, description = "engine verdict JSON"),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "NO_ELIGIBLE_NODE / SCHEMA_CHANGE_IN_PROGRESS (partial-column sink, see deploy)"),
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
/// compile the materialized spec → re-run the T-A7 partial-declaration gate
/// against the registered sinks → resolve every ref against registered
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
    // plan is rejected). T-A7: a sink's partial-column declaration (the
    // INSERT column list) rides its entry as `partial_columns` — the switch
    // the engine's DorisSink reads; entries without one are byte-identical
    // to the pre-A7 shape.
    let store = state.store();
    let partials = match sink_partial_declarations(&statements) {
        Ok(m) => m,
        Err(d) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(CompileResponse {
                    ok: false,
                    statements: Vec::new(),
                    diagnostics: vec![d],
                }),
            )
                .into_response()
        }
    };
    // T-A7 re-gate: the snapshot's declarations must still fit their
    // registered sinks at push time — the publish gate's diagnostics, so a
    // snapshot published before the gate existed (pre-A7: column list
    // ignored, full-row writes) or whose sink record moved since cannot
    // flip into partial mode unchecked
    match partial_declarations_gate(store, &statements) {
        Ok(d) if d.is_empty() => {}
        Ok(d) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(CompileResponse {
                    ok: false,
                    statements: Vec::new(),
                    diagnostics: d,
                }),
            )
                .into_response()
        }
        Err(e) => return ApiError::internal(&e).into_response(),
    }
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
        match build_plan_table(store, reference, partials.get(reference).map(Vec::as_slice)) {
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

    // T-A7 mutex (deploy side): a partial-column sink mid-schema-change
    // refuses the push — checked live, before a node is even picked
    if let Err(r) = schema_change_gate(state, &partials).await {
        return r;
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
/// `partial_columns` (T-A7): the sink's INSERT column list when the
/// statement set declares one — emitted as the entry's `partial_columns`
/// key; `None` emits no key at all, so an undeclared entry is byte-identical
/// to the pre-A7 shape. Ok(None) = not registered. Shared with the T-A2
/// backfill executor (crate-visible) — a bounded replay resolves plan tables
/// exactly like a deploy push.
pub(crate) fn build_plan_table(
    store: &console_store::Store,
    reference: &str,
    partial_columns: Option<&[String]>,
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
    // //…`, so the `jdbc:` prefix is stripped here. File tables carry the
    // datasource's registered directory (connJson `path`): the engine's file
    // factory resolves the table name against it, so a resident node and the
    // console-side backfill executor open the same file whatever either
    // process's cwd is (R6 review: with the bare name resolved against the
    // console cwd, a backfill read nothing and "succeeded" with 0 rows).
    let bootstrap = match kind.as_str() {
        "dim" | "jdbc" => conn.get("jdbcUrl").and_then(Value::as_str).map(|url| {
            url.trim()
                .strip_prefix("jdbc:")
                .unwrap_or(url.trim())
                .to_string()
        }),
        "file" => conn
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string),
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
    let mut entry = json!({
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
    });
    // T-A7: the declared subset rides only when declared (serde-default on
    // the engine side; absent key = full-row writes)
    if let Some(cols) = partial_columns {
        entry["partial_columns"] = json!(cols);
    }
    Ok(Some(entry))
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

// ---------------- T-A2 backfill (blueprint §7.3) ----------------

/// One replay bound: `{type:"offset", value:N}`. Only `offset` bounds exist
/// this wave — timestamp bounds need console-side timestamp→offset
/// resolution (blueprint §7.3 leaves it to the console; not yet built).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BackfillBound {
    /// Bound addressing: "offset" only (kafka offset / file byte position,
    /// per the source's cursor semantics).
    #[serde(rename = "type")]
    pub bound_type: String,
    /// The bound value (integer for offset bounds).
    pub value: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackfillBody {
    /// Which statement of the pipeline to replay (default 0).
    pub statement_index: Option<u32>,
    pub from: BackfillBound,
    pub to: BackfillBound,
    /// true = preview only: no job row, no execution, no writes.
    pub dry_run: Option<bool>,
}

/// Dry-run preview (blueprint §7.3 response shape). `jobId` is null on
/// purpose: a dry run creates nothing.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackfillDryRunOut {
    pub job_id: Option<String>,
    /// Exact only for row-shaped cursors; null when the bound pair does not
    /// translate to a row count (file bounds are byte positions).
    pub estimated_rows: Option<i64>,
    /// The deterministic label family a real run of this range would use
    /// (full label = `{labelPrefix}-{statementIndex}-{sha256(from,to)[..8]}_{seq}`).
    pub label_prefix: String,
}

/// Execute acceptance: the job exists (pending) and the executor has been
/// spawned; poll `GET /{id}/backfill/jobs` for the state machine.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackfillAccepted {
    pub job_id: String,
    pub state: String,
    pub estimated_rows: Option<i64>,
    pub label_prefix: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackfillJobItem {
    pub job_id: String,
    pub statement_index: i64,
    pub from: Value,
    pub to: Value,
    pub state: String,
    pub rows_written: i64,
    pub error: Option<String>,
    pub created_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BackfillJobsResponse {
    pub jobs: Vec<BackfillJobItem>,
}

/// Everything the 422 gate chain + dry-run preview need, resolved once from
/// the CURRENT published spec (the executor re-resolves at claim time).
struct BackfillTarget {
    statement_index: u32,
    from: i64,
    to: i64,
    label_prefix: String,
    /// T-A7: the statement's partial-column declaration keyed by its sink
    /// reference (the plan entry the executor resolves) — the accept path
    /// runs the same schema-change precheck as deploy on it.
    partial: Option<(String, Vec<String>)>,
}

/// Shared request validation (blueprint §7.3 + the task card's capability
/// gate): pipeline exists → published → compiles → statement index in range
/// → offset bounds ordered → source/sink table objects resolve → sink is
/// doris/starrocks (label idempotency lives there — the 回退门), source is
/// file (scalar bounds address the byte cursor exactly; kafka needs
/// partition-level seeding, follow-up batch), no dim joins. Every rejection
/// is a 422/400 envelope from the dictionary; nothing is created.
fn backfill_target(
    state: &AppState,
    id: i64,
    body: &BackfillBody,
) -> Result<BackfillTarget, Response> {
    let full = load_pipeline(state, id)?;
    let Some(spec) = full.base.spec_json.clone() else {
        return Err(super::messages::bad_request(
            "err.backfill.not_published",
            &[&id.to_string()],
        ));
    };
    let (statements, diagnostics) = compile_sql(&spec, id as u64);
    if !diagnostics.is_empty() {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(CompileResponse {
                ok: false,
                statements,
                diagnostics,
            }),
        )
            .into_response());
    }
    let idx = body.statement_index.unwrap_or(0);
    let Some(st) = statements.get(idx as usize) else {
        return Err(super::messages::respond(
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "STATEMENT_INDEX_RANGE",
            "err.backfill.statement_index",
            &[&idx.to_string(), &statements.len().to_string()],
        ));
    };
    if !st.dims.is_empty() {
        let refs = st
            .dims
            .iter()
            .map(|d| d.reference.clone())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(super::messages::respond(
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "BACKFILL_DIM_UNSUPPORTED",
            "err.backfill.dim_unsupported",
            &[&(idx + 1).to_string(), &refs],
        ));
    }
    // bounds: offset-typed and ordered (the bounded runner takes plain i64s)
    let bound = |b: &BackfillBound, what: &'static str| -> Result<i64, Response> {
        if b.bound_type != "offset" {
            return Err(super::messages::respond(
                axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                "BOUNDS_TYPE_UNSUPPORTED",
                "err.backfill.bounds_type",
                &[what, &b.bound_type],
            ));
        }
        b.value.as_i64().ok_or_else(|| {
            super::messages::respond(
                axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                "BOUNDS_INVALID",
                "err.backfill.bounds_value",
                &[what, &b.value.to_string()],
            )
        })
    };
    let from = bound(&body.from, "from")?;
    let to = bound(&body.to, "to")?;
    if from >= to {
        return Err(super::messages::respond(
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "BOUNDS_INVALID",
            "err.backfill.bounds_order",
            &[&from.to_string(), &to.to_string()],
        ));
    }
    // capability gate (task card 回退门): the sink must be doris/starrocks —
    // label idempotency (the replay-dedup mechanism) lives in the stream-load
    // label space; file/kafka sinks would double-write every replay.
    let store = state.store();
    let resolve = |reference: &str| -> Result<console_store::TableObject, Response> {
        store
            .table_object_for_reference(reference)
            .ok()
            .flatten()
            .ok_or_else(|| {
                super::messages::respond(
                    axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                    "STATEMENT_TABLE_MISSING",
                    "err.pipeline.statement_table_missing",
                    &[&(idx + 1).to_string(), "源/汇", reference],
                )
            })
    };
    let source_kind = tailrace_catalog::normalize_kind(&resolve(&st.source)?.kind);
    let sink_table = resolve(&st.sink)?;
    let sink_kind = tailrace_catalog::normalize_kind(&sink_table.kind);
    let sink_ds = sink_table.datasource_id;
    if !matches!(sink_kind.as_str(), "doris" | "starrocks") {
        return Err(super::messages::respond(
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "BACKFILL_SINK_UNSUPPORTED",
            "err.backfill.sink_unsupported",
            &[&(idx + 1).to_string(), &st.sink, &sink_kind],
        ));
    }
    // T-A7 re-gate (backfill flavour): a bounded replay writes the same
    // partial loads a deploy would, so the declaration must still fit the
    // registered sink — the publish gate's diagnostics, before any job row
    // exists (a pre-A7 snapshot or a moved key model is refused here, not by
    // Doris mid-replay)
    if st.partial_columns.is_some() {
        let diags = partial_declaration_diags(store, idx as usize, st, &sink_table)
            .map_err(|e| ApiError::internal(&e).into_response())?;
        if let Some(d) = diags.first() {
            return Err(super::messages::respond(
                axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                "PARTIAL_DECLARATION_INVALID",
                "err.backfill.partial_declaration",
                &[&d.message],
            ));
        }
    }
    // source gate: file only this wave. A scalar `from` cannot address a
    // kafka partition map (engine W2 warns and would silently replay from the
    // group's start position — more data than asked, permanently duplicated
    // on a DUPLICATE KEY table); per-partition console-side seeding is the
    // follow-up. Honest refusal beats a silent superset replay.
    if source_kind != "file" {
        return Err(super::messages::respond(
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "BACKFILL_SOURCE_UNSUPPORTED",
            "err.backfill.source_unsupported",
            &[&(idx + 1).to_string(), &st.source, &source_kind],
        ));
    }
    if store.credential(sink_ds).ok().flatten().is_none() {
        return Err(super::messages::respond(
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "BACKFILL_NO_CREDENTIAL",
            "err.backfill.no_credential",
            &[&sink_ds.to_string()],
        ));
    }
    Ok(BackfillTarget {
        statement_index: idx,
        from,
        to,
        label_prefix: crate::backfill::label_prefix(id, idx),
        partial: st
            .partial_columns
            .clone()
            .map(|cols| (st.sink.clone(), cols)),
    })
}

/// `POST /api/pipelines/{id}/backfill` (blueprint §7.3): preview or enqueue
/// a bounded replay. `dryRun:true` answers the job-shape preview
/// (jobId/estimatedRows/labelPrefix) with ZERO side effects — no job row,
/// no audit, no execution. `dryRun:false` creates a pending job row (the
/// console-store migration 02 table), audits the acceptance and spawns the
/// console-side executor: bf-{jobId} ledger namespace + deterministic
/// labels (W2), main-pipeline cursors untouched in every outcome.
#[utoipa::path(
    post,
    path = "/api/pipelines/{id}/backfill",
    request_body = BackfillBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = BackfillDryRunOut, description = "dryRun preview — nothing created"),
        (status = 202, body = BackfillAccepted, description = "job row created (pending), executor spawned"),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "SCHEMA_CHANGE_IN_PROGRESS — the statement declares a partial-column write and its sink has an ADD COLUMN job in flight (T-A7; dryRun skips the precheck)"),
        (status = 422, body = crate::api::ErrorResponse, description = "capability gate: BACKFILL_SINK_UNSUPPORTED (non doris/starrocks sink), BACKFILL_SOURCE_UNSUPPORTED, BACKFILL_DIM_UNSUPPORTED, BACKFILL_NO_CREDENTIAL, BOUNDS_TYPE_UNSUPPORTED, BOUNDS_INVALID, STATEMENT_INDEX_RANGE; PARTIAL_DECLARATION_INVALID — the statement's INSERT column list no longer passes the T-A7 publish gate against its registered sink (re-run here)"),
        (status = 502, body = crate::api::ErrorResponse, description = "UNAVAILABLE — the partial-update precheck could not ask the Doris FE"),
    )
)]
pub async fn backfill(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    let body: BackfillBody = match parse_json_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let target = match backfill_target(&state, id, &body) {
        Ok(t) => t,
        Err(r) => return r,
    };
    // file bounds are byte positions and kafka needs watermark queries for a
    // row count — no source kind yields an honest number this wave, so the
    // preview carries null (the executed job's rowsWritten is the truth)
    let estimated_rows: Option<i64> = None;
    if body.dry_run.unwrap_or(false) {
        return Json(BackfillDryRunOut {
            job_id: None,
            estimated_rows,
            label_prefix: target.label_prefix,
        })
        .into_response();
    }
    // T-A7 mutex (deploy side, backfill flavour): a bounded replay writes the
    // same partial loads a deploy would — same live precheck, before the
    // job row exists
    if let Some((sink, cols)) = &target.partial {
        let partials: HashMap<String, Vec<String>> = HashMap::from([(sink.clone(), cols.clone())]);
        if let Err(r) = schema_change_gate(&state, &partials).await {
            return r;
        }
    }
    let from_json = serde_json::to_string(&body.from).unwrap_or_default();
    let to_json = serde_json::to_string(&body.to).unwrap_or_default();
    let job_id = match state.store().create_backfill_job(
        id,
        target.statement_index as i64,
        &from_json,
        &to_json,
    ) {
        Ok(j) => j,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    audit(
        state.store(),
        &operator_of(&ctx, &headers),
        "PIPELINE_BACKFILL",
        "pipeline",
        Some(id.to_string()),
        Some(
            json!({
                "jobId": job_id,
                "statementIndex": target.statement_index,
                "from": body.from.value,
                "to": body.to.value,
                "labelPrefix": target.label_prefix,
                "dryRun": false
            })
            .to_string(),
        ),
    );
    // the executor owns pending → running → done|failed from here; request
    // handling never blocks on data movement
    let exec_state = state.clone();
    let exec_job = job_id.clone();
    let (idx, from, to) = (target.statement_index, target.from, target.to);
    tokio::task::spawn_blocking(move || {
        crate::backfill::run_job(exec_state, exec_job, idx, from, to)
    });
    (
        axum::http::StatusCode::ACCEPTED,
        Json(BackfillAccepted {
            job_id,
            state: "pending".into(),
            estimated_rows,
            label_prefix: target.label_prefix,
        }),
    )
        .into_response()
}

/// `GET /api/pipelines/{id}/backfill/jobs` — the job progress list the
/// detail page polls (T-A2 ⑤ job 进度行; newest first).
#[utoipa::path(
    get,
    path = "/api/pipelines/{id}/backfill/jobs",
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = BackfillJobsResponse),
        (status = 404, body = crate::api::ErrorResponse),
    )
)]
pub async fn backfill_jobs(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    if load_pipeline(&state, id).is_err() {
        return super::messages::not_found("err.pipeline.not_found", &[&id.to_string()])
            .into_response();
    }
    let jobs = match state.store().list_backfill_jobs(id) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let jobs = jobs
        .into_iter()
        .map(|j| BackfillJobItem {
            job_id: j.id,
            statement_index: j.statement_index,
            from: serde_json::from_str(&j.from_json).unwrap_or(Value::Null),
            to: serde_json::from_str(&j.to_json).unwrap_or(Value::Null),
            state: j.state,
            rows_written: j.rows_written,
            error: j.error,
            created_at: iso(Some(&j.created_at)).unwrap_or_default(),
            finished_at: j.finished_at.and_then(|f| iso(Some(&f))),
        })
        .collect();
    Json(BackfillJobsResponse { jobs }).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stmt(sink: &str, partial: Option<&[&str]>) -> StatementOut {
        StatementOut {
            index: 0,
            source: "1.shop.orders".into(),
            group: "tr.p1.s0".into(),
            sink: sink.into(),
            pushdown: vec![],
            select_sql: "SELECT id FROM src".into(),
            start_offset: None,
            dims: vec![],
            partial_columns: partial.map(|p| p.iter().map(|s| s.to_string()).collect()),
        }
    }

    /// T-A7: one declaration per sink across statements — equal lists (or
    /// equal absence) pass; a different list, or list vs. bare, on the same
    /// sink is the conflict diagnostic naming both statements.
    #[test]
    fn sink_partial_declarations_one_per_sink() {
        let ok = sink_partial_declarations(&[
            stmt("4.ods.a", Some(&["k", "a"])),
            stmt("4.ods.b", None),
            stmt("4.ods.a", Some(&["k", "a"])),
            stmt("4.ods.b", None),
        ])
        .unwrap();
        assert_eq!(ok.len(), 1);
        assert_eq!(ok["4.ods.a"], vec!["k", "a"]);

        let e =
            sink_partial_declarations(&[stmt("4.ods.a", Some(&["k", "a"])), stmt("4.ods.a", None)])
                .unwrap_err();
        assert_eq!(e.line, 2);
        assert!(
            e.message.contains("语句 2 与语句 1") && e.message.contains("4.ods.a"),
            "{}",
            e.message
        );
        let e = sink_partial_declarations(&[
            stmt("4.ods.a", Some(&["k", "a"])),
            stmt("4.ods.a", Some(&["k"])),
        ])
        .unwrap_err();
        assert!(e.message.contains("部分列声明不一致"), "{}", e.message);
    }

    /// T-A7 deploy-side mutex: FINISHED / CANCELLED (any case) are done;
    /// everything else Doris lists (PENDING / WAITING_TXN / RUNNING) is in
    /// flight and blocks a partial-update push.
    #[test]
    fn alter_jobs_in_flight_filters_terminal_states() {
        let job = |id: &str, st: &str| tailrace_metadata::AlterColumnJob {
            job_id: id.into(),
            state: st.into(),
        };
        let jobs = vec![
            job("1", "FINISHED"),
            job("2", "cancelled"),
            job("3", "RUNNING"),
            job("4", "WAITING_TXN"),
            job("5", "PENDING"),
        ];
        let live: Vec<&str> = alter_jobs_in_flight(&jobs)
            .iter()
            .map(|j| j.job_id.as_str())
            .collect();
        assert_eq!(live, vec!["3", "4", "5"]);
        assert!(
            alter_jobs_in_flight(&[]).is_empty(),
            "no job on record = nothing in flight"
        );
    }

    /// T-A7 plan projection: the declared subset becomes the sink entry's
    /// `partial_columns` key (snake_case — the engine's PlanTableObject
    /// field); without a declaration the key is ABSENT, not null, so an
    /// undeclared entry is byte-identical to the pre-A7 shape.
    #[test]
    fn build_plan_table_projects_the_declaration_only_when_declared() {
        let store = console_store::Store::open_in_memory().unwrap();
        let ds = store
            .insert_datasource(
                "d",
                "doris",
                r#"{"jdbcUrl":"jdbc:mysql://127.0.0.1:1/x","streamLoadUrl":"http://127.0.0.1:1"}"#,
            )
            .unwrap();
        store
            .insert_table_object(
                ds,
                "ods",
                "orders",
                "doris",
                None,
                Some(r#"[["k","utf8"],["a","utf8"],["b","utf8"]]"#),
                None,
                Some("unique"),
            )
            .unwrap();
        let reference = format!("{ds}.ods.orders");
        let declared = vec!["k".to_string(), "a".to_string()];
        let on = build_plan_table(&store, &reference, Some(&declared))
            .unwrap()
            .expect("registered");
        assert_eq!(on["partial_columns"], json!(["k", "a"]));
        assert_eq!(on["kind"], "doris");
        assert_eq!(
            on["columns"],
            json!([["k", "utf8"], ["a", "utf8"], ["b", "utf8"]])
        );
        assert_eq!(
            on["stream_load_url"],
            "http://127.0.0.1:1/api/ods/orders/_stream_load"
        );

        let off = build_plan_table(&store, &reference, None)
            .unwrap()
            .expect("registered");
        assert!(
            off.get("partial_columns").is_none(),
            "no declaration = no key: {off}"
        );
        // everything else is identical between the two shapes
        let mut on_sans = on.clone();
        on_sans.as_object_mut().unwrap().remove("partial_columns");
        assert_eq!(on_sans, off);
    }

    /// T-A7 re-gate (R7 review): the gate over a statement set follows the
    /// sink's CURRENT record — a declaration that passed at publish is
    /// refused once the key model on record is no longer `unique` (the
    /// pre-A7 snapshot case is the same shape: a list the gate never saw).
    /// Undeclared statements and unregistered sinks contribute nothing.
    #[test]
    fn partial_declarations_gate_follows_the_sink_record() {
        let store = console_store::Store::open_in_memory().unwrap();
        let ds = store
            .insert_datasource(
                "d",
                "doris",
                r#"{"jdbcUrl":"jdbc:mysql://127.0.0.1:1/x","streamLoadUrl":"http://127.0.0.1:1"}"#,
            )
            .unwrap();
        let table = store
            .insert_table_object(
                ds,
                "ods",
                "orders",
                "doris",
                None,
                Some(r#"[["k","utf8"],["a","utf8"],["b","utf8"]]"#),
                None,
                Some("unique"),
            )
            .unwrap();
        let sink = format!("{ds}.ods.orders");
        let declared = [stmt(&sink, Some(&["k", "a"])), stmt(&sink, None)];
        assert!(
            partial_declarations_gate(&store, &declared)
                .unwrap()
                .is_empty(),
            "unique on record + declared ⊆ registered passes"
        );
        // the record moves under the snapshot → the same statements are refused
        store.set_table_key_model(table, "duplicate").unwrap();
        let diags = partial_declarations_gate(&store, &declared).unwrap();
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert_eq!(diags[0].line, 1);
        assert!(
            diags[0].message.contains("keyModel=duplicate"),
            "{}",
            diags[0].message
        );
        // an unregistered sink is not this gate's business (reported as a
        // missing table object by the callers)
        assert!(
            partial_declarations_gate(&store, &[stmt("9.nowhere.t", Some(&["k"]))])
                .unwrap()
                .is_empty()
        );
        // no declaration anywhere = no lookups, no diagnostics
        assert!(partial_declarations_gate(&store, &[stmt(&sink, None)])
            .unwrap()
            .is_empty());
    }

    /// T-A7 deploy-side mutex, the 409 shape (R7 review: CI has no Doris
    /// to put an ALTER job in flight, so the refusal is locked here):
    /// SCHEMA_CHANGE_IN_PROGRESS, message naming the table and every
    /// in-flight job as `id=STATE`, `refs.{sink,table,jobs[{jobId,state}]}`.
    #[tokio::test]
    async fn schema_change_refusal_carries_table_and_jobs_refs() {
        let job = |id: &str, st: &str| tailrace_metadata::AlterColumnJob {
            job_id: id.into(),
            state: st.into(),
        };
        let jobs = [job("1001", "RUNNING"), job("1002", "WAITING_TXN")];
        let in_flight: Vec<&tailrace_metadata::AlterColumnJob> = jobs.iter().collect();
        let resp = schema_change_refusal("4.ods.orders", "ods.orders", &in_flight);
        assert_eq!(resp.status(), axum::http::StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let b: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(b["error"]["code"], "SCHEMA_CHANGE_IN_PROGRESS", "{b}");
        assert_eq!(
            b["error"]["message_key"],
            "err.pipeline.partial_schema_change"
        );
        let msg = b["error"]["message"].as_str().unwrap();
        assert!(
            msg.contains("ods.orders") && msg.contains("1001=RUNNING、1002=WAITING_TXN"),
            "{msg}"
        );
        let refs = &b["error"]["refs"];
        assert_eq!(refs["sink"], "4.ods.orders", "{refs}");
        assert_eq!(refs["table"], "ods.orders");
        assert_eq!(
            refs["jobs"],
            json!([
                {"jobId": "1001", "state": "RUNNING"},
                {"jobId": "1002", "state": "WAITING_TXN"}
            ]),
            "{refs}"
        );
    }
}
