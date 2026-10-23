//! Datasource endpoints (§4.2): type registry (dynamic-form single fact
//! source), CRUD with generation CAS + credential split, connectivity
//! probes, table-object registration.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{ApiError, AuthContext};
use console_store::DatasourceUpdate;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use utoipa::ToSchema;

use crate::api::{audit, iso, operator_of, ErrorResponse};
use crate::state::AppState;

// ---------------- static type registry (§4.2 GET /api/datasource-types) ----------------

/// One form field of a datasource type self-description.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TypeField {
    pub name: String,
    pub label: String,
    #[serde(rename = "inputType")]
    pub input_type: String,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DatasourceType {
    #[serde(rename = "type")]
    pub ty: String,
    pub label: String,
    pub family: String,
    #[serde(rename = "credentialLabel")]
    pub credential_label: Option<String>,
    pub fields: Vec<TypeField>,
}

fn registry() -> Vec<DatasourceType> {
    vec![
        DatasourceType {
            ty: "kafka".into(),
            label: "Kafka".into(),
            family: "provider".into(),
            credential_label: None,
            fields: vec![TypeField {
                name: "bootstrap".into(),
                label: "Bootstrap Servers".into(),
                input_type: "text".into(),
                required: true,
                placeholder: Some("10.1.1.5:9092".into()),
            }],
        },
        DatasourceType {
            ty: "doris".into(),
            label: "Doris".into(),
            family: "jdbc".into(),
            credential_label: Some("数据库账号".into()),
            fields: vec![
                TypeField {
                    name: "jdbcUrl".into(),
                    label: "JDBC URL（FE）".into(),
                    input_type: "text".into(),
                    required: true,
                    placeholder: None,
                },
                TypeField {
                    name: "streamLoadUrl".into(),
                    label: "Stream Load URL（BE 直连）".into(),
                    input_type: "text".into(),
                    required: true,
                    placeholder: Some("http://be-1:8040".into()),
                },
                TypeField {
                    name: "fenodes".into(),
                    label: "FE 节点列表".into(),
                    input_type: "text".into(),
                    required: false,
                    placeholder: None,
                },
            ],
        },
        // starrocks (T39 ⑥ ← T32 caveat): its own registry member so a
        // StarRocks target can be registered through the console instead of
        // the `node run` bypass the T32 bench used. connJson reuses the doris
        // shape verbatim (jdbcUrl FE 9030 / streamLoadUrl / fenodes) — the
        // probe, discover (MySQL-protocol FE) and the sink URL composition
        // in pipelines.rs are shared; the engine picks the StarRocks Stream
        // Load flavor off the table-object kind (see `table_kind_for`).
        DatasourceType {
            ty: "starrocks".into(),
            label: "StarRocks".into(),
            family: "jdbc".into(),
            credential_label: Some("数据库账号".into()),
            fields: vec![
                TypeField {
                    name: "jdbcUrl".into(),
                    label: "JDBC URL（FE）".into(),
                    input_type: "text".into(),
                    required: true,
                    placeholder: None,
                },
                TypeField {
                    name: "streamLoadUrl".into(),
                    label: "Stream Load URL（FE/BE）".into(),
                    input_type: "text".into(),
                    required: true,
                    placeholder: Some("http://fe-1:8030".into()),
                },
                TypeField {
                    name: "fenodes".into(),
                    label: "FE 节点列表".into(),
                    input_type: "text".into(),
                    required: false,
                    placeholder: None,
                },
            ],
        },
        DatasourceType {
            ty: "file".into(),
            label: "本地文件/目录".into(),
            family: "provider".into(),
            credential_label: None,
            fields: vec![TypeField {
                name: "path".into(),
                label: "路径".into(),
                input_type: "text".into(),
                required: true,
                placeholder: Some("/data/inbox/orders".into()),
            }],
        },
        // jdbc family (T14 ← R3-W1 review low#4): the MySQL/PostgreSQL batch
        // sources conn-jdbc serves (kind "jdbc", cursor JDBC_KEY). Same
        // jdbcUrl field shape as doris so the probe / discover parsers are
        // shared; credentials ride the sealed credential split.
        DatasourceType {
            ty: "mysql".into(),
            label: "MySQL（JDBC 批源）".into(),
            family: "jdbc".into(),
            credential_label: Some("数据库账号".into()),
            fields: vec![TypeField {
                name: "jdbcUrl".into(),
                label: "JDBC URL".into(),
                input_type: "text".into(),
                required: true,
                placeholder: Some("jdbc:mysql://10.1.1.7:3306/shop".into()),
            }],
        },
        DatasourceType {
            ty: "postgresql".into(),
            label: "PostgreSQL（JDBC 批源）".into(),
            family: "jdbc".into(),
            credential_label: Some("数据库账号".into()),
            fields: vec![TypeField {
                name: "jdbcUrl".into(),
                label: "JDBC URL".into(),
                input_type: "text".into(),
                required: true,
                placeholder: Some("jdbc:postgresql://10.1.1.8:5432/shop".into()),
            }],
        },
    ]
}

/// Registry order gate for write paths (§4.2: 400 when the type is unknown).
fn registry_type(ty: &str) -> Option<&'static str> {
    match ty {
        "kafka" => Some("kafka"),
        "doris" => Some("doris"),
        "starrocks" => Some("starrocks"),
        "file" => Some("file"),
        "mysql" => Some("mysql"),
        "postgresql" => Some("postgresql"),
        _ => None,
    }
}

/// Required connJson keys per type (form-completeness precheck).
fn required_conn_keys(ty: &str) -> &'static [&'static str] {
    match ty {
        "kafka" => &["bootstrap"],
        "doris" | "starrocks" => &["jdbcUrl", "streamLoadUrl"],
        "file" => &["path"],
        "mysql" | "postgresql" => &["jdbcUrl"],
        _ => &[],
    }
}

/// Table-object kind for a datasource type (the console-store
/// `cursor_semantics` derivation keys off this: kafka → KAFKA_OFFSET, file →
/// FILE_POINTER, jdbc → JDBC_KEY, else NONE). The mysql/postgresql types
/// both register `jdbc` objects — the conn-jdbc kind (IR `Cursor::JdbcKey`).
/// doris and starrocks stay DISTINCT kinds (T39 ⑥): the conn-doris factory
/// selects the StarRocks Stream Load flavor (no `read_json_by_line` /
/// `sequence_column`, FE redirect following — the T12 difference matrix) off
/// `kind == "starrocks"`, so folding starrocks into "doris" here would make
/// the engine send Doris-only headers to a StarRocks FE. Both kinds derive
/// cursor NONE (sink-side).
fn table_kind_for(ds_type: &str) -> &str {
    match ds_type {
        "mysql" | "postgresql" => "jdbc",
        other => other,
    }
}

/// `GET /api/datasource-types` — dynamic form source (§4.2).
#[utoipa::path(get, path = "/api/datasource-types", responses((status = 200, body = DatasourceTypesResponse)))]
pub async fn list_types() -> Response {
    Json(DatasourceTypesResponse { types: registry() }).into_response()
}

// ---------------- DTOs ----------------

#[derive(Debug, Serialize, ToSchema)]
pub struct DatasourceTypesResponse {
    pub types: Vec<DatasourceType>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasourceUpsertBody {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub conn_json: Option<Value>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// PUT only: CAS guard (§4.2 A.1 #7).
    pub generation: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasourceItem {
    pub id: i64,
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub status: String,
    pub generation: i64,
    pub has_password: bool,
    pub conn_json: Value,
    pub updated_at: String,
    /// Detail view only (§4.2: 同列表项字段 + 表对象计数).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_count: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DatasourceListResponse {
    pub items: Vec<DatasourceItem>,
    pub total: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasourceCreated {
    pub id: i64,
    pub generation: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TestBody {
    #[serde(rename = "type")]
    pub ty: String,
    pub conn_json: Option<Value>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TestResponse {
    pub status: String,
    pub message: String,
    pub latency_ms: u64,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableRegisterBody {
    pub db_name: String,
    pub table_name: String,
    pub format: Option<String>,
    pub columns_json: Option<Vec<(String, String)>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TableRegistered {
    pub id: i64,
}

/// List-tables row. `columns_json` (wire `columnsJson`) carries the
/// `[name, type]` pairs registered manually or via T14 infer-schema, `null`
/// when the table was registered without columns — it feeds the web editor's
/// `db.table.` column completion (T39 ⑤: the console hop the T22 chain was
/// missing; additive wire change).
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableObjectItem {
    pub id: i64,
    pub datasource_id: i64,
    pub db_name: String,
    pub table_name: String,
    pub kind: String,
    pub format: Option<String>,
    pub cursor_semantics: String,
    pub source: String,
    pub columns_json: Option<Vec<(String, String)>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TableListResponse {
    pub items: Vec<TableObjectItem>,
    pub total: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverBody {
    pub sample_rows: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredAssetDto {
    pub db_name: String,
    pub table_name: String,
    /// Table-object kind the register step derives (kafka/doris/jdbc).
    pub kind: String,
    /// Decode format hint (null: not applicable / to be inferred).
    pub format: Option<String>,
    /// Kafka topic partition count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DiscoverResponse {
    pub assets: Vec<DiscoveredAssetDto>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferSchemaBody {
    pub db_name: String,
    pub table_name: String,
    pub format: Option<String>,
    /// Kafka only: how many tail messages to sample (default 20, clamped
    /// 1..=100).
    pub sample_rows: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferSchemaResponse {
    /// (column, arrow-type-name) pairs — the runtime's `arrow_type`
    /// vocabulary: int64 / float64 / bool / utf8.
    pub columns: Vec<(String, String)>,
    /// Kafka only: the decode format the samples suggest ("ogg" / "json");
    /// null for SQL backends (no decode layer).
    pub format: Option<String>,
}

// ---------------- discover cache (T14: console 转发并缓存) ----------------

/// Successes only, invalidated by a TTL and by key change; failures are
/// never cached so a retry after fixing conn params always re-probes.
const DISCOVER_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// Process-wide cache keyed by the full identity of what was probed —
/// datasource id + generation + the exact connJson — so an entry is only
/// reused for a byte-identical query (a pure-function cache: cross-request
/// sharing of identical answers is the point; any edit changes the key).
/// Kept here rather than AppState so the module stays self-contained
/// (T14 file domain: this file only).
type DiscoverCacheMap = std::collections::HashMap<(i64, i64, String), CachedDiscover>;
static DISCOVER_CACHE: std::sync::OnceLock<std::sync::Mutex<DiscoverCacheMap>> =
    std::sync::OnceLock::new();

struct CachedDiscover {
    at: std::time::Instant,
    assets: std::sync::Arc<Vec<DiscoveredAssetDto>>,
}

fn cache_get(
    id: i64,
    generation: i64,
    conn_json: &str,
) -> Option<std::sync::Arc<Vec<DiscoveredAssetDto>>> {
    let cache = DISCOVER_CACHE.get_or_init(Default::default);
    let mut map = cache.lock().ok()?;
    let key = (id, generation, conn_json.to_string());
    match map.get_mut(&key) {
        Some(c) if c.at.elapsed() < DISCOVER_TTL => Some(c.assets.clone()),
        // stale (TTL) → drop so the miss path refetches
        Some(_) => {
            map.remove(&key);
            None
        }
        None => None,
    }
}

fn cache_put(
    id: i64,
    generation: i64,
    conn_json: &str,
    assets: std::sync::Arc<Vec<DiscoveredAssetDto>>,
) {
    let cache = DISCOVER_CACHE.get_or_init(Default::default);
    if let Ok(mut map) = cache.lock() {
        map.insert(
            (id, generation, conn_json.to_string()),
            CachedDiscover {
                at: std::time::Instant::now(),
                assets,
            },
        );
    }
}

// ---------------- handlers ----------------

fn to_item(
    ds: &console_store::Datasource,
    has_password: bool,
    table_count: Option<i64>,
) -> DatasourceItem {
    DatasourceItem {
        id: ds.id,
        name: ds.name.clone(),
        ty: ds.ds_type.clone(),
        status: ds.status.clone(),
        generation: ds.generation,
        has_password,
        conn_json: serde_json::from_str(&ds.conn_json).unwrap_or(Value::Null),
        updated_at: iso(Some(&ds.updated_at)).unwrap_or_default(),
        table_count,
    }
}

fn parse_body<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, Response> {
    serde_json::from_slice(body).map_err(|e| {
        super::messages::bad_request("err.bad_body", &[&e.to_string()]).into_response()
    })
}

/// Validate name/type/connJson shape; returns the canonical conn_json string.
fn validate_upsert(body: &DatasourceUpsertBody) -> Result<String, Response> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("缺少数据源名称").into_response());
    }
    let Some(ty) = registry_type(&body.ty) else {
        return Err(super::messages::bad_request(
            "err.datasource.unknown_type_registry",
            &[&body.ty],
        ));
    };
    let conn = body.conn_json.clone().unwrap_or(Value::Null);
    if !conn.is_object() {
        return Err(ApiError::bad_request("connJson 必须是对象").into_response());
    }
    let missing: Vec<&str> = required_conn_keys(ty)
        .iter()
        .filter(|k| {
            conn.get(**k)
                .and_then(Value::as_str)
                .map(|v| v.trim())
                .map(str::is_empty)
                .unwrap_or(true)
        })
        .copied()
        .collect();
    if !missing.is_empty() {
        return Err(super::messages::bad_request(
            "err.datasource.missing_required_fields",
            &[&missing.join(", ")],
        ));
    }
    serde_json::to_string(&conn).map_err(|e| ApiError::internal(&e.into()).into_response())
}

/// Credential halves (§5.3): JSON {"username","password"} sealed with
/// AES-256-GCM → (nonce_b64, ciphertext_b64) for the two
/// `datasource_credential` columns. Plaintext never reaches storage or any
/// response, and credentials are never returned by any endpoint.
fn credential_halves(
    body: &DatasourceUpsertBody,
    cipher: &console_store::CredentialCipher,
) -> Option<anyhow::Result<(String, String)>> {
    let has_user = body
        .username
        .as_deref()
        .map(|u| !u.trim().is_empty())
        .unwrap_or(false);
    let has_pass = body
        .password
        .as_deref()
        .map(|p| !p.is_empty())
        .unwrap_or(false);
    if !has_user && !has_pass {
        return None;
    }
    let payload = json!({
        "username": body.username.as_deref().unwrap_or(""),
        "password": body.password.as_deref().unwrap_or(""),
    });
    Some(cipher.seal(&payload.to_string()))
}

/// `GET /api/datasources?type=&layer=` (§4.2; `layer` accepted for form
/// compatibility — the v1 model has no layer dimension).
#[utoipa::path(
    get,
    path = "/api/datasources",
    params(("type" = Option<String>, Query, description = "filter by registry type")),
    responses((status = 200, body = DatasourceListResponse))
)]
pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let ty = params
        .get("type")
        .map(String::as_str)
        .filter(|v| !v.is_empty());
    let store = state.store();
    let items = match store.list_datasources(ty) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let items: Vec<DatasourceItem> = items
        .iter()
        .map(|ds| {
            let has_password = store
                .credential(ds.id)
                .map(|c| c.is_some())
                .unwrap_or(false);
            to_item(ds, has_password, None)
        })
        .collect();
    Json(DatasourceListResponse {
        total: items.len() as i64,
        items,
    })
    .into_response()
}

/// `POST /api/datasources` — create; plaintext credentials appear exactly
/// once (§4.2), 409 on duplicate name.
#[utoipa::path(
    post,
    path = "/api/datasources",
    request_body = DatasourceUpsertBody,
    responses(
        (status = 201, body = DatasourceCreated),
        (status = 400, body = ErrorResponse),
        (status = 409, body = ErrorResponse, description = "name already exists"),
    )
)]
pub async fn create(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let body: DatasourceUpsertBody = match parse_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let conn_json = match validate_upsert(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let store = state.store();
    let id = match store.insert_datasource(body.name.trim(), &body.ty, &conn_json) {
        Ok(id) => id,
        Err(e) => return conflict_or_internal(&e, "数据源名称已存在"),
    };
    match credential_halves(&body, state.cipher()) {
        Some(Ok((nonce, ciphertext))) => {
            if let Err(e) = store.upsert_credential(id, &nonce, &ciphertext) {
                return ApiError::internal(&e).into_response();
            }
        }
        Some(Err(e)) => return ApiError::internal(&e).into_response(),
        None => {}
    }
    audit(
        store,
        &operator_of(&ctx, &headers),
        "DATASOURCE_CREATE",
        "datasource",
        Some(id.to_string()),
        Some(json!({"name": body.name.trim(), "type": body.ty}).to_string()),
    );
    (
        axum::http::StatusCode::CREATED,
        Json(DatasourceCreated { id, generation: 1 }),
    )
        .into_response()
}

fn conflict_or_internal(e: &anyhow::Error, conflict_message: &str) -> Response {
    let is_constraint = e
        .chain()
        .any(|c| c.to_string().contains("UNIQUE constraint failed"));
    if is_constraint {
        ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "CONFLICT",
            conflict_message,
        )
        .into_response()
    } else {
        ApiError::internal(e).into_response()
    }
}

/// `GET /api/datasources/{id}` — detail (+ table count, never credentials).
#[utoipa::path(
    get,
    path = "/api/datasources/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = DatasourceItem),
        (status = 404, body = ErrorResponse),
    )
)]
pub async fn detail(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let store = state.store();
    let Some(ds) = store.datasource(id).ok().flatten() else {
        return super::messages::not_found("err.datasource.not_found", &[&id.to_string()])
            .into_response();
    };
    let has_password = store.credential(id).map(|c| c.is_some()).unwrap_or(false);
    let table_count = store.table_count(id).unwrap_or(0);
    Json(to_item(&ds, has_password, Some(table_count))).into_response()
}

/// `PUT /api/datasources/{id}` — generation CAS; blank password keeps the
/// stored credential (§4.2 A.1 #7).
#[utoipa::path(
    put,
    path = "/api/datasources/{id}",
    request_body = DatasourceUpsertBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = DatasourceCreated, description = "updated; carries the new generation"),
        (status = 400, body = ErrorResponse),
        (status = 404, body = ErrorResponse),
        (status = 409, body = ErrorResponse, description = "GENERATION_CONFLICT / name taken"),
    )
)]
pub async fn update(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    let body: DatasourceUpsertBody = match parse_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let conn_json = match validate_upsert(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let Some(expected) = body.generation else {
        return ApiError::bad_request("缺少 generation（CAS 乐观锁必填）").into_response();
    };
    let sealed = match credential_halves(&body, state.cipher()) {
        Some(Ok(halves)) => Some(halves),
        Some(Err(e)) => return ApiError::internal(&e).into_response(),
        None => None,
    };
    let store = state.store();
    match store.update_datasource(
        id,
        body.name.trim(),
        &body.ty,
        &conn_json,
        expected,
        sealed.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
    ) {
        Ok(DatasourceUpdate::Applied(gen)) => {
            audit(
                store,
                &operator_of(&ctx, &headers),
                "DATASOURCE_UPDATE",
                "datasource",
                Some(id.to_string()),
                Some(json!({"generation": gen}).to_string()),
            );
            Json(DatasourceCreated {
                id,
                generation: gen,
            })
            .into_response()
        }
        Ok(DatasourceUpdate::GenerationConflict) => ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "GENERATION_CONFLICT",
            "generation 不匹配，数据源已被其他人修改",
        )
        .into_response(),
        Ok(DatasourceUpdate::NameConflict) => ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "CONFLICT",
            "数据源名称已被占用",
        )
        .into_response(),
        Ok(DatasourceUpdate::NotFound) => {
            super::messages::not_found("err.datasource.not_found", &[&id.to_string()])
                .into_response()
        }
        Err(e) => ApiError::internal(&e).into_response(),
    }
}

/// `DELETE /api/datasources/{id}` — guarded by table objects / pipeline
/// mappings (§4.2 IN_USE with refs).
#[utoipa::path(
    delete,
    path = "/api/datasources/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 204),
        (status = 404, body = ErrorResponse),
        (status = 409, body = ErrorResponse, description = "IN_USE with refs.pipelines"),
    )
)]
pub async fn remove(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let store = state.store();
    let Some(ds) = store.datasource(id).ok().flatten() else {
        return super::messages::not_found("err.datasource.not_found", &[&id.to_string()])
            .into_response();
    };
    let pipelines = store
        .pipelines_referencing_datasource(id)
        .unwrap_or_default();
    let tables = store.table_count(id).unwrap_or(0);
    if !pipelines.is_empty() || tables > 0 {
        let mut refs = Map::new();
        if !pipelines.is_empty() {
            refs.insert("pipelines".into(), json!(pipelines));
        }
        if tables > 0 {
            refs.insert("tables".into(), json!(tables));
        }
        return super::messages::respond_with_refs(
            axum::http::StatusCode::CONFLICT,
            "IN_USE",
            "err.datasource.in_use",
            &[&pipelines.len().to_string(), &tables.to_string()],
            refs,
        );
    }
    if let Err(e) = store.delete_datasource(id) {
        return ApiError::internal(&e).into_response();
    }
    audit(
        store,
        &operator_of(&ctx, &headers),
        "DATASOURCE_DELETE",
        "datasource",
        Some(id.to_string()),
        Some(json!({"name": ds.name}).to_string()),
    );
    axum::http::StatusCode::NO_CONTENT.into_response()
}

/// `POST /api/datasources/test` — unsaved probe (registration form); the
/// probe itself always 200s and reports its outcome honestly (§4.2).
#[utoipa::path(
    post,
    path = "/api/datasources/test",
    request_body = TestBody,
    responses((status = 200, body = TestResponse, description = "CONN_OK / CONN_FAIL both ride 200"))
)]
pub async fn test_unsaved(State(_state): State<AppState>, body: Bytes) -> Response {
    let body: TestBody = match parse_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    run_probe(&body.ty, body.conn_json.as_ref().unwrap_or(&Value::Null))
        .await
        .1
}

/// `POST /api/datasources/{id}/test` — persisted probe: writes status + audit.
#[utoipa::path(
    post,
    path = "/api/datasources/{id}/test",
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = TestResponse),
        (status = 404, body = ErrorResponse),
    )
)]
pub async fn test_by_id(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let store = state.store();
    let Some(ds) = store.datasource(id).ok().flatten() else {
        return super::messages::not_found("err.datasource.not_found", &[&id.to_string()])
            .into_response();
    };
    let conn: Value = serde_json::from_str(&ds.conn_json).unwrap_or(Value::Null);
    let result = run_probe(&ds.ds_type, &conn).await;
    let status = if result.0 { "CONN_OK" } else { "CONN_FAIL" };
    let _ = store.set_datasource_status(id, status);
    audit(
        store,
        &operator_of(&ctx, &headers),
        "DATASOURCE_TEST",
        "datasource",
        Some(id.to_string()),
        Some(json!({"status": status}).to_string()),
    );
    result.1
}

async fn run_probe(ty: &str, conn_json: &Value) -> (bool, Response) {
    if registry_type(ty).is_none() {
        return (
            false,
            super::messages::bad_request("err.datasource.unknown_type", &[ty]).into_response(),
        );
    }
    let r = match ty {
        // jdbc family (T14 registry addition): the shared probe crate's
        // registry has no mysql/postgresql arms yet — same semantics as its
        // TCP probes, run console-side (one endpoint, 3s ceiling)
        "mysql" | "postgresql" => {
            let o = probe_jdbc_url(conn_json).await;
            (o.ok, o.message, o.latency_ms)
        }
        _ => {
            let p = console_proxy::probe_datasource(ty, conn_json).await;
            (p.ok, p.message, p.latency_ms)
        }
    };
    let resp = Json(TestResponse {
        status: if r.0 {
            "CONN_OK".into()
        } else {
            "CONN_FAIL".into()
        },
        message: r.1,
        latency_ms: r.2,
    });
    (r.0, resp.into_response())
}

/// Local probe outcome (console_proxy::ProbeResult is not re-exported).
struct ProbeOutcome {
    ok: bool,
    message: String,
    latency_ms: u64,
}

/// TCP reachability of the jdbcUrl's host:port for mysql/postgresql
/// datasources (mirrors the doris FE arm's semantics: 200 with CONN_OK /
/// CONN_FAIL, never an error status).
async fn probe_jdbc_url(conn_json: &Value) -> ProbeOutcome {
    let started = std::time::Instant::now();
    let url = conn_json
        .get("jdbcUrl")
        .and_then(Value::as_str)
        .map(str::trim);
    let Some(url) = url else {
        return ProbeOutcome {
            ok: false,
            message: "connJson 缺少 jdbcUrl".into(),
            latency_ms: started.elapsed().as_millis() as u64,
        };
    };
    let Some((host, port, _db)) = jdbc_endpoint(url) else {
        return ProbeOutcome {
            ok: false,
            message: super::messages::t("err.datasource.jdbc_url_unparsable", &[url]),
            latency_ms: started.elapsed().as_millis() as u64,
        };
    };
    let addr = format!("{host}:{port}");
    let outcome = match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("{addr}: {e}")),
        Err(_) => Err(super::messages::t("probe.jdbc.timeout", &[&addr])),
    };
    ProbeOutcome {
        ok: outcome.is_ok(),
        message: match outcome {
            Ok(()) => super::messages::t("probe.jdbc.reachable", &[&addr]),
            Err(msg) => super::messages::t("probe.jdbc.unreachable", &[&msg]),
        },
        latency_ms: started.elapsed().as_millis() as u64,
    }
}

/// Unsealed credential halves for a stored datasource (§5.3: opened in
/// memory for one call, never logged, never returned).
struct CredentialPair {
    username: String,
    password: String,
}

fn unsealed_credential(state: &AppState, id: i64) -> Result<Option<CredentialPair>, Response> {
    let Some(c) = state.store().credential(id).ok().flatten() else {
        return Ok(None);
    };
    let plain = state
        .cipher()
        .open(&c.nonce, &c.ciphertext)
        .map_err(|e| ApiError::internal(&e).into_response())?;
    let v: Value = serde_json::from_str(&plain)
        .map_err(|e| ApiError::internal(&(e.into())).into_response())?;
    Ok(Some(CredentialPair {
        username: v
            .get("username")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        password: v
            .get("password")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    }))
}

/// "jdbc:mysql://fe-1:9030/shop?x=1" → ("fe-1", 9030, Some("shop")). Port is
/// mandatory (the probe parser takes the same stance).
fn jdbc_endpoint(url: &str) -> Option<(String, u16, Option<String>)> {
    let rest = url.split_once("://")?.1;
    let (hostport, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let hostport = hostport.split('?').next()?;
    let (host, port) = hostport.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    if host.is_empty() {
        return None;
    }
    let database = tail.split('?').next().filter(|s| !s.is_empty());
    Some((host.to_string(), port, database.map(str::to_string)))
}

/// Datasource row → metadata-client ConnSpec (T14): connJson shape plus the
/// unsealed credential. Err(Response) = 400-shaped shape problems.
fn conn_spec(
    state: &AppState,
    ds: &console_store::Datasource,
) -> Result<tailrace_metadata::ConnSpec, Response> {
    let conn: Value = serde_json::from_str(&ds.conn_json).unwrap_or(Value::Null);
    let cred = unsealed_credential(state, ds.id)?;
    let str_field = |k: &str| {
        conn.get(k)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    };
    match ds.ds_type.as_str() {
        "kafka" => match str_field("bootstrap") {
            Some(bootstrap) => Ok(tailrace_metadata::ConnSpec::Kafka {
                bootstrap: bootstrap.to_string(),
            }),
            None => Err(ApiError::bad_request("connJson 缺少 bootstrap").into_response()),
        },
        // doris/starrocks FEs both speak the MySQL protocol on the jdbcUrl
        // port (9030): discover/infer-schema share the mysql metadata client
        "doris" | "starrocks" | "mysql" => {
            let url = str_field("jdbcUrl")
                .ok_or_else(|| ApiError::bad_request("connJson 缺少 jdbcUrl").into_response())?;
            let (host, port, database) = jdbc_endpoint(url).ok_or_else(|| {
                super::messages::bad_request("err.datasource.jdbc_mysql_unparsable", &[url])
            })?;
            Ok(tailrace_metadata::ConnSpec::MySql {
                host,
                port,
                database,
                username: cred
                    .as_ref()
                    .map(|c| c.username.clone())
                    .unwrap_or_default(),
                password: cred
                    .as_ref()
                    .map(|c| c.password.clone())
                    .unwrap_or_default(),
            })
        }
        "postgresql" => {
            let url = str_field("jdbcUrl")
                .ok_or_else(|| ApiError::bad_request("connJson 缺少 jdbcUrl").into_response())?;
            let (host, port, database) = jdbc_endpoint(url).ok_or_else(|| {
                super::messages::bad_request("err.datasource.jdbc_pg_unparsable", &[url])
            })?;
            Ok(tailrace_metadata::ConnSpec::Postgres {
                host,
                port,
                database,
                username: cred
                    .as_ref()
                    .map(|c| c.username.clone())
                    .unwrap_or_default(),
                password: cred
                    .as_ref()
                    .map(|c| c.password.clone())
                    .unwrap_or_default(),
            })
        }
        "file" => Err(ApiError::bad_request(
            "文件数据源不支持发现（路径下的文件请直接登记表对象）",
        )
        .into_response()),
        other => Err(
            super::messages::bad_request("err.datasource.unknown_type", &[other]).into_response(),
        ),
    }
}

/// 502 for backend-side failures (unreachable broker / auth refused) — the
/// request was well-formed, the datasource is not answering.
fn unavailable(context: &str, e: anyhow::Error) -> Response {
    ApiError::new(
        axum::http::StatusCode::BAD_GATEWAY,
        "UNAVAILABLE",
        format!("{context}: {e}"),
    )
    .into_response()
}

/// `POST /api/datasources/{id}/discover` (T14): forward to the engine
/// metadata client (embedded, same pattern as the local compile parser) and
/// cache successes for `DISCOVER_TTL` keyed by (id, generation). Kafka
/// topics map to assets via the first-dot split; SQL backends list tables
/// per (schema-restricted) database.
#[utoipa::path(
    post,
    path = "/api/datasources/{id}/discover",
    request_body = DiscoverBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = DiscoverResponse),
        (status = 400, body = ErrorResponse, description = "type/connJson not discoverable"),
        (status = 404, body = ErrorResponse),
        (status = 502, body = ErrorResponse, description = "UNAVAILABLE — datasource reachable-check failed"),
    )
)]
pub async fn discover(State(state): State<AppState>, Path(id): Path<i64>, body: Bytes) -> Response {
    // sampleRows is accepted (wire shape the web sends) but listing itself
    // samples nothing — only infer-schema does; keep the parse so a
    // malformed body still 400s.
    let _body: DiscoverBody = match parse_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let store = state.store();
    let Some(ds) = store.datasource(id).ok().flatten() else {
        return super::messages::not_found("err.datasource.not_found", &[&id.to_string()])
            .into_response();
    };
    if let Some(assets) = cache_get(id, ds.generation, &ds.conn_json) {
        return Json(DiscoverResponse {
            assets: (*assets).clone(),
        })
        .into_response();
    }
    let spec = match conn_spec(&state, &ds) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let kind = table_kind_for(&ds.ds_type).to_string();
    match tailrace_metadata::discover(&spec).await {
        Ok(assets) => {
            let assets: Vec<DiscoveredAssetDto> = assets
                .into_iter()
                .map(|a| DiscoveredAssetDto {
                    db_name: a.db_name,
                    table_name: a.table_name,
                    kind: kind.clone(),
                    format: None,
                    partitions: a.partitions,
                })
                .collect();
            cache_put(
                id,
                ds.generation,
                &ds.conn_json,
                std::sync::Arc::new(assets.clone()),
            );
            Json(DiscoverResponse { assets }).into_response()
        }
        Err(e) => unavailable("数据源探测失败", e),
    }
}

/// `POST /api/datasources/{id}/infer-schema` (T14): Kafka samples the topic
/// tail and infers columns through the decode chain's typed lattice (plus a
/// detected decode format); SQL backends read DDL column types. Nothing is
/// persisted — the register step (POST /tables) does that.
#[utoipa::path(
    post,
    path = "/api/datasources/{id}/infer-schema",
    request_body = InferSchemaBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = InferSchemaResponse),
        (status = 400, body = ErrorResponse, description = "bad db/table/format"),
        (status = 404, body = ErrorResponse),
        (status = 502, body = ErrorResponse, description = "UNAVAILABLE — backend query failed"),
    )
)]
pub async fn infer_schema(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    let body: InferSchemaBody = match parse_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.db_name.trim().is_empty() || body.table_name.trim().is_empty() {
        return ApiError::bad_request("缺少 dbName / tableName").into_response();
    }
    let store = state.store();
    let Some(ds) = store.datasource(id).ok().flatten() else {
        return super::messages::not_found("err.datasource.not_found", &[&id.to_string()])
            .into_response();
    };
    let spec = match conn_spec(&state, &ds) {
        Ok(s) => s,
        Err(r) => return r,
    };
    if matches!(spec, tailrace_metadata::ConnSpec::Kafka { .. }) {
        // client-input format problems 400 before any backend is touched
        // (csv/parquet have no self-describing samples; unknown names too)
        match body.format.as_deref() {
            Some("ogg") | Some("json") | Some("jsonl") | None => {}
            Some(other) => {
                return super::messages::bad_request(
                    "err.datasource.kafka_format_unsupported",
                    &[&format!("{other:?}")],
                )
            }
        }
    }
    // sample-row clamp (kafka sampling breadth; ignored by SQL backends)
    let sample_rows = body.sample_rows.unwrap_or(20).clamp(1, 100) as usize;
    match tailrace_metadata::infer_schema(
        &spec,
        body.db_name.trim(),
        body.table_name.trim(),
        body.format.as_deref(),
        sample_rows,
    )
    .await
    {
        Ok(schema) => Json(InferSchemaResponse {
            columns: schema.columns,
            format: schema.format,
        })
        .into_response(),
        Err(e) => unavailable("模式推断失败", e),
    }
}

/// `POST /api/datasources/{id}/tables` — register a table object (§4.2 v1
/// minimal set; the structural prerequisite for pipeline publish).
#[utoipa::path(
    post,
    path = "/api/datasources/{id}/tables",
    request_body = TableRegisterBody,
    params(("id" = i64, Path)),
    responses(
        (status = 200, body = TableRegistered),
        (status = 400, body = ErrorResponse),
        (status = 404, body = ErrorResponse),
        (status = 409, body = ErrorResponse, description = "(datasource, db, table) already registered"),
    )
)]
pub async fn register_table(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    let body: TableRegisterBody = match parse_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.db_name.trim().is_empty() || body.table_name.trim().is_empty() {
        return ApiError::bad_request("缺少 dbName / tableName").into_response();
    }
    let store = state.store();
    let Some(ds) = store.datasource(id).ok().flatten() else {
        return super::messages::not_found("err.datasource.not_found", &[&id.to_string()])
            .into_response();
    };
    // kind derives from the datasource type (kafka ds → kafka table …,
    // mysql/postgresql ds → jdbc objects — see table_kind_for)
    let kind = table_kind_for(&ds.ds_type).to_string();
    let columns_json = body
        .columns_json
        .as_ref()
        .map(|cols| serde_json::to_string(cols).unwrap_or_else(|_| "[]".into()));
    let tid = match store.insert_table_object(
        id,
        body.db_name.trim(),
        body.table_name.trim(),
        &kind,
        body.format.as_deref(),
        columns_json.as_deref(),
    ) {
        Ok(tid) => tid,
        Err(e) => return conflict_or_internal(&e, "该库表已登记（datasource+db+table 唯一）"),
    };
    audit(
        store,
        &operator_of(&ctx, &headers),
        "TABLE_CREATE",
        "table_object",
        Some(tid.to_string()),
        Some(json!({"datasourceId": id, "dbName": body.db_name, "tableName": body.table_name, "kind": kind}).to_string()),
    );
    Json(TableRegistered { id: tid }).into_response()
}

/// `GET /api/datasources/tables?datasourceId=&kind=` (§4.2).
#[utoipa::path(
    get,
    path = "/api/datasources/tables",
    params(
        ("datasourceId" = Option<i64>, Query),
        ("kind" = Option<String>, Query),
    ),
    responses((status = 200, body = TableListResponse))
)]
pub async fn list_tables(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let datasource_id = params
        .get("datasourceId")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse().ok());
    let kind = params
        .get("kind")
        .map(String::as_str)
        .filter(|v| !v.is_empty());
    let store = state.store();
    let items = match store.list_table_objects(datasource_id, kind) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let items: Vec<TableObjectItem> = match items
        .into_iter()
        .map(|t| {
            // columns ride the existing per-id accessor (the same one the
            // pipeline assembly in pipelines.rs reads): the store's list row
            // model carries no columns_json, and the table-object catalog is
            // small enough that a per-row lookup on the in-process SQLite
            // handle beats widening the shared row model for one DTO (T39 ⑤).
            // Store errors propagate to 500 like list_table_objects above;
            // only an unparseable blob degrades to null (build_plan_table
            // semantics, pipelines.rs).
            let columns_json = store
                .table_object_columns_json(t.id)?
                .flatten()
                .and_then(|s| serde_json::from_str::<Vec<(String, String)>>(&s).ok());
            Ok(TableObjectItem {
                id: t.id,
                datasource_id: t.datasource_id,
                db_name: t.db_name,
                table_name: t.table_name,
                kind: t.kind,
                format: t.format,
                cursor_semantics: t.cursor_semantics,
                source: t.source,
                columns_json,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
    {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    Json(TableListResponse {
        total: items.len() as i64,
        items,
    })
    .into_response()
}

/// `DELETE /api/datasources/tables/{id}` — 409 while any published pipeline
/// still references the table (§4.2).
#[utoipa::path(
    delete,
    path = "/api/datasources/tables/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 204),
        (status = 404, body = ErrorResponse),
        (status = 409, body = ErrorResponse, description = "referenced by published pipelines"),
    )
)]
pub async fn remove_table(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let store = state.store();
    let Some(t) = store.table_object(id).ok().flatten() else {
        return super::messages::not_found("err.table_object.not_found", &[&id.to_string()])
            .into_response();
    };
    let pipelines = store.pipelines_referencing_table(&t).unwrap_or_default();
    if !pipelines.is_empty() {
        return super::messages::respond_with_refs(
            axum::http::StatusCode::CONFLICT,
            "IN_USE",
            "err.table_object.in_use",
            &[&format!("{pipelines:?}")],
            serde_json::json!({"pipelines": pipelines})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
    }
    if let Err(e) = store.delete_table_object(id) {
        return ApiError::internal(&e).into_response();
    }
    audit(
        store,
        &operator_of(&ctx, &headers),
        "TABLE_DELETE",
        "table_object",
        Some(id.to_string()),
        Some(json!({"datasourceId": t.datasource_id, "dbName": t.db_name, "tableName": t.table_name}).to_string()),
    );
    axum::http::StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(name: &str) -> DiscoveredAssetDto {
        DiscoveredAssetDto {
            db_name: "shop".into(),
            table_name: name.into(),
            kind: "kafka".into(),
            format: None,
            partitions: Some(1),
        }
    }

    /// T14 cache contract: a fresh hit returns the stored assets; any key
    /// change (generation bump = config edit, or a different connJson / id)
    /// misses. Failures are never inserted (the handler only caches Ok).
    #[test]
    fn discover_cache_key_invalidation() {
        cache_put(
            7,
            3,
            r#"{"bootstrap":"a:9092"}"#,
            std::sync::Arc::new(vec![asset("orders")]),
        );
        let hit = cache_get(7, 3, r#"{"bootstrap":"a:9092"}"#).expect("fresh entry");
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].table_name, "orders");
        // generation bump (config edit) misses
        assert!(
            cache_get(7, 4, r#"{"bootstrap":"a:9092"}"#).is_none(),
            "generation bump busts the entry"
        );
        // same generation but edited connJson misses
        assert!(
            cache_get(7, 3, r#"{"bootstrap":"b:9092"}"#).is_none(),
            "connJson change busts the entry"
        );
        cache_put(
            7,
            4,
            r#"{"bootstrap":"a:9092"}"#,
            std::sync::Arc::new(vec![asset("orders"), asset("bills")]),
        );
        let hit = cache_get(7, 4, r#"{"bootstrap":"a:9092"}"#).expect("re-populated");
        assert_eq!(hit.len(), 2);
        assert!(
            cache_get(8, 3, r#"{"bootstrap":"a:9092"}"#).is_none(),
            "other id misses"
        );
    }
}
