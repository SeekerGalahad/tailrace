//! Artifact endpoints (§4.4): multipart upload with server-side sha256,
//! storage under `data/artifacts/`, refCount listing, guarded delete.

use axum::extract::multipart::Multipart;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{ApiError, AuthContext};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use crate::api::{audit, iso, operator_of};
use crate::state::AppState;

/// §4.0: uploads beyond 512MB are 413.
const MAX_ARTIFACT_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactItem {
    pub id: i64,
    pub name: String,
    pub version: String,
    pub size_bytes: i64,
    pub sha256: String,
    pub ref_count: i64,
    pub source: String,
    pub uploaded_by: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ArtifactsResponse {
    pub artifacts: Vec<ArtifactItem>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ArtifactUploaded {
    pub id: i64,
    pub sha256: String,
}

/// `GET /api/artifacts` (§4.4).
#[utoipa::path(
    get,
    path = "/api/artifacts",
    responses((status = 200, body = ArtifactsResponse))
)]
pub async fn list(State(state): State<AppState>) -> Response {
    let rows = match state.store().list_artifacts() {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    Json(ArtifactsResponse {
        artifacts: rows
            .into_iter()
            .map(|a| ArtifactItem {
                id: a.id,
                name: a.name,
                version: a.version,
                size_bytes: a.size_bytes,
                sha256: a.sha256,
                ref_count: a.ref_count,
                source: "upload".into(),
                uploaded_by: a.uploaded_by,
                created_at: iso(Some(&a.created_at)).unwrap_or(a.created_at),
            })
            .collect(),
    })
    .into_response()
}

/// `POST /api/artifacts` — multipart (`file` + `name` + `version`); sha256 is
/// computed server-side while streaming to disk; `name+version` duplicates
/// are 409 (§4.4).
#[utoipa::path(
    post,
    path = "/api/artifacts",
    request_body(content = String, content_type = "multipart/form-data"),
    responses(
        (status = 201, body = ArtifactUploaded),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "name+version already exists"),
        (status = 413, body = crate::api::ErrorResponse, description = "> 512MB"),
    )
)]
pub async fn upload(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    mut multipart: Multipart,
) -> Response {
    let mut name: Option<String> = None;
    let mut version: Option<String> = None;
    let mut filename: Option<String> = None;
    let mut hasher = Sha256::new();
    let mut size: usize = 0;
    let mut staged: Option<std::path::PathBuf> = None;
    let mut file_out: Option<tokio::fs::File> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        let field_name = field.name().unwrap_or_default().to_string();
        match field_name.as_str() {
            "file" => {
                let mut field = field;
                let Some(fname) = field.file_name().map(str::to_string) else {
                    return ApiError::bad_request("multipart 缺少 file 字段的文件名")
                        .into_response();
                };
                if !fname.ends_with(".tar.gz") {
                    return ApiError::bad_request("制品必须是 .tar.gz 包").into_response();
                }
                if staged.is_none() {
                    let dir = state.artifact_dir();
                    if let Err(e) = tokio::fs::create_dir_all(dir).await {
                        return ApiError::internal(&anyhow::anyhow!("创建制品目录失败: {e}"))
                            .into_response();
                    }
                    let path = dir.join(format!(".upload-{}", std::process::id()));
                    match tokio::fs::File::create(&path).await {
                        Ok(f) => {
                            staged = Some(path);
                            file_out = Some(f);
                        }
                        Err(e) => {
                            return ApiError::internal(&anyhow::anyhow!("暂存文件创建失败: {e}"))
                                .into_response()
                        }
                    }
                }
                filename = Some(fname);
                let Some(out) = file_out.as_mut() else {
                    unreachable!("staged file open")
                };
                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => {
                            size += chunk.len();
                            if size > MAX_ARTIFACT_BYTES {
                                let _ =
                                    tokio::fs::remove_file(staged.as_ref().expect("staged exists"))
                                        .await;
                                return ApiError::new(
                                    axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                                    "PAYLOAD_TOO_LARGE",
                                    "制品超过 512MB 上限",
                                )
                                .into_response();
                            }
                            hasher.update(&chunk);
                            if let Err(e) = tokio::io::AsyncWriteExt::write_all(out, &chunk).await {
                                return ApiError::internal(&anyhow::anyhow!("写入暂存失败: {e}"))
                                    .into_response();
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let _ = tokio::fs::remove_file(staged.as_ref().expect("staged exists"))
                                .await;
                            return ApiError::bad_request(format!("multipart 读取失败: {e}"))
                                .into_response();
                        }
                    }
                }
            }
            other => {
                let Ok(text) = field.text().await else {
                    return ApiError::bad_request(format!("字段 {other} 读取失败")).into_response();
                };
                match other {
                    "name" => name = Some(text),
                    "version" => version = Some(text),
                    _ => {}
                }
            }
        }
    }

    let Some(staged_path) = staged else {
        return ApiError::bad_request("multipart 缺少 file 字段").into_response();
    };
    let name = name.unwrap_or_else(|| "tailrace-node".into());
    let Some(version) = version.filter(|v| !v.trim().is_empty()) else {
        let _ = tokio::fs::remove_file(&staged_path).await;
        return ApiError::bad_request("缺少 version 字段").into_response();
    };
    let version = version.trim().to_string();
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        let _ = tokio::fs::remove_file(&staged_path).await;
        return ApiError::bad_request("name 仅允许 [A-Za-z0-9._-]").into_response();
    }
    let filename = filename.unwrap_or_else(|| format!("{name}-{version}.tar.gz"));

    let sha256 = hex(&hasher.finalize());
    let store = state.store();
    let inserted = match store.insert_artifact(
        &name,
        &version,
        &filename,
        &sha256,
        size as i64,
        &ctx.user.username,
    ) {
        Ok(v) => v,
        Err(e) => {
            let _ = tokio::fs::remove_file(&staged_path).await;
            return ApiError::internal(&e).into_response();
        }
    };
    let id = match inserted {
        console_store::ArtifactInsert::Created(id) => id,
        console_store::ArtifactInsert::Duplicate => {
            let _ = tokio::fs::remove_file(&staged_path).await;
            return ApiError::new(
                axum::http::StatusCode::CONFLICT,
                "CONFLICT",
                format!("制品 {name} {version} 已存在"),
            )
            .into_response();
        }
    };

    let final_path = state
        .artifact_dir()
        .join(format!("{name}-{version}.tar.gz"));
    if let Err(e) = tokio::fs::rename(&staged_path, &final_path).await {
        let _ = store.delete_artifact(id);
        let _ = tokio::fs::remove_file(&staged_path).await;
        return ApiError::internal(&anyhow::anyhow!("制品落盘失败: {e}")).into_response();
    }

    audit(
        store,
        &operator_of(&ctx, &headers),
        "ARTIFACT_UPLOAD",
        "artifact",
        Some(id.to_string()),
        Some(
            json!({"name": name, "version": version, "sha256": sha256, "sizeBytes": size})
                .to_string(),
        ),
    );
    (
        axum::http::StatusCode::CREATED,
        Json(ArtifactUploaded { id, sha256 }),
    )
        .into_response()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `DELETE /api/artifacts/{id}` — 409 while nodes reference it (§4.4).
#[utoipa::path(
    delete,
    path = "/api/artifacts/{id}",
    params(("id" = i64, Path)),
    responses(
        (status = 204),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "referenced by nodes"),
    )
)]
pub async fn remove(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let store = state.store();
    let Some(a) = store.artifact(id).ok().flatten() else {
        return ApiError::not_found(format!("制品 #{id} 不存在")).into_response();
    };
    let nodes = store.nodes_referencing_artifact(id).unwrap_or_default();
    if !nodes.is_empty() {
        return ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "IN_USE",
            format!("制品被节点引用: {nodes:?}"),
        )
        .with_refs(
            json!({"nodes": nodes})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        )
        .into_response();
    }
    if let Err(e) = store.delete_artifact(id) {
        return ApiError::internal(&e).into_response();
    }
    // best-effort file removal (metadata row is the truth; a stale file is
    // reaped by the same name on re-upload)
    let path = state
        .artifact_dir()
        .join(format!("{}-{}.tar.gz", a.name, a.version));
    let _ = tokio::fs::remove_file(path).await;
    audit(
        store,
        &operator_of(&ctx, &headers),
        "ARTIFACT_DELETE",
        "artifact",
        Some(id.to_string()),
        Some(json!({"name": a.name, "version": a.version}).to_string()),
    );
    axum::http::StatusCode::NO_CONTENT.into_response()
}
