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
            label: "Doris / StarRocks".into(),
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
    ]
}

/// Registry order gate for write paths (§4.2: 400 when the type is unknown).
fn registry_type(ty: &str) -> Option<&'static str> {
    match ty {
        "kafka" => Some("kafka"),
        "doris" => Some("doris"),
        "file" => Some("file"),
        _ => None,
    }
}

/// Required connJson keys per type (form-completeness precheck).
fn required_conn_keys(ty: &str) -> &'static [&'static str] {
    match ty {
        "kafka" => &["bootstrap"],
        "doris" => &["jdbcUrl", "streamLoadUrl"],
        "file" => &["path"],
        _ => &[],
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
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TableListResponse {
    pub items: Vec<TableObjectItem>,
    pub total: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct DiscoverBody {
    pub sample_rows: Option<i64>,
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
    serde_json::from_slice(body)
        .map_err(|e| ApiError::bad_request(format!("非法请求体: {e}")).into_response())
}

/// Validate name/type/connJson shape; returns the canonical conn_json string.
fn validate_upsert(body: &DatasourceUpsertBody) -> Result<String, Response> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("缺少数据源名称").into_response());
    }
    let Some(ty) = registry_type(&body.ty) else {
        return Err(ApiError::bad_request(format!(
            "未知数据源类型 {}（注册表：kafka/doris/file）",
            body.ty
        ))
        .into_response());
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
        return Err(ApiError::bad_request(format!(
            "connJson 缺少必填字段: {}",
            missing.join(", ")
        ))
        .into_response());
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
        return ApiError::not_found(format!("数据源 #{id} 不存在")).into_response();
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
            ApiError::not_found(format!("数据源 #{id} 不存在")).into_response()
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
        return ApiError::not_found(format!("数据源 #{id} 不存在")).into_response();
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
        return ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "IN_USE",
            format!(
                "数据源仍被引用（管道 {} 个，表对象 {} 个）",
                pipelines.len(),
                tables
            ),
        )
        .with_refs(refs)
        .into_response();
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
        return ApiError::not_found(format!("数据源 #{id} 不存在")).into_response();
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
            ApiError::bad_request(format!("未知数据源类型 {ty}")).into_response(),
        );
    }
    let r = console_proxy::probe_datasource(ty, conn_json).await;
    let resp = Json(TestResponse {
        status: if r.ok {
            "CONN_OK".into()
        } else {
            "CONN_FAIL".into()
        },
        message: r.message,
        latency_ms: r.latency_ms,
    });
    (r.ok, resp.into_response())
}

/// `POST /api/datasources/{id}/discover` — TODO(§4.2 v1 minimal set, not yet
/// implemented): real discovery needs live metadata clients (Kafka topic
/// listing / Doris SHOW TABLES) the console does not embed yet. Note: what
/// §4.2 actually defers to P2 is the catalog snapshot browser / sample
/// preview / impact-analysis family — discover/infer-schema themselves are
/// v1 scope and land once the metadata clients do.
#[utoipa::path(
    post,
    path = "/api/datasources/{id}/discover",
    request_body = DiscoverBody,
    params(("id" = i64, Path)),
    responses((status = 501, body = ErrorResponse, description = "v1 minimal-set member, metadata clients not yet embedded"))
)]
pub async fn discover(
    State(_state): State<AppState>,
    Path(id): Path<i64>,
    _body: Bytes,
) -> Response {
    let _ = id;
    crate::api::not_implemented(
        "数据源发现（discover）",
        "v1 最小集成员：需要 Kafka/Doris 元数据客户端，尚未交付",
    )
    .into_response()
}

/// `POST /api/datasources/{id}/infer-schema` — TODO(§4.2 v1 minimal set):
/// same metadata-client dependency as discover (see the note there — v1
/// scope, not part of the P2 deferral).
#[utoipa::path(
    post,
    path = "/api/datasources/{id}/infer-schema",
    params(("id" = i64, Path)),
    responses((status = 501, body = ErrorResponse))
)]
pub async fn infer_schema(State(_state): State<AppState>, Path(id): Path<i64>) -> Response {
    let _ = id;
    crate::api::not_implemented(
        "模式推断（infer-schema）",
        "v1 最小集成员：与 discover 同依赖（Kafka/Doris 元数据客户端），尚未交付",
    )
    .into_response()
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
        return ApiError::not_found(format!("数据源 #{id} 不存在")).into_response();
    };
    // kind derives from the datasource type (kafka ds → kafka table …)
    let kind = match ds.ds_type.as_str() {
        "doris" | "starrocks" => "doris",
        other => other,
    };
    let columns_json = body
        .columns_json
        .as_ref()
        .map(|cols| serde_json::to_string(cols).unwrap_or_else(|_| "[]".into()));
    let tid = match store.insert_table_object(
        id,
        body.db_name.trim(),
        body.table_name.trim(),
        kind,
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
    let items = match state.store().list_table_objects(datasource_id, kind) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let items: Vec<TableObjectItem> = items
        .into_iter()
        .map(|t| TableObjectItem {
            id: t.id,
            datasource_id: t.datasource_id,
            db_name: t.db_name,
            table_name: t.table_name,
            kind: t.kind,
            format: t.format,
            cursor_semantics: t.cursor_semantics,
            source: t.source,
        })
        .collect();
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
        return ApiError::not_found(format!("表对象 #{id} 不存在")).into_response();
    };
    let pipelines = store.pipelines_referencing_table(&t).unwrap_or_default();
    if !pipelines.is_empty() {
        return ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "IN_USE",
            format!("表对象被已发布管道引用: {pipelines:?}"),
        )
        .with_refs(
            serde_json::json!({"pipelines": pipelines})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        )
        .into_response();
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
