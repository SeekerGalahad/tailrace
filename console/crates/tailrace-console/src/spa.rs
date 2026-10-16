//! Embedded SPA (§3.1/A2): rust-embed serves `web/dist`; unknown non-API
//! paths fall back to index.html (client-side routing).
//!
//! Folder is three levels up from this crate: tailrace/web/dist (the naive
//! "../../web/dist" from the crate resolves one level short).

use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

use console_auth::ApiError;

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/../../../web/dist"]
struct Assets;

const INDEX: &str = "index.html";

pub async fn fallback(uri: Uri) -> Response {
    let path = uri.path();
    if path.starts_with("/api/") || path.starts_with("/api-docs") || path == "/api" {
        return ApiError::not_found("接口不存在").into_response();
    }
    serve_path(path).unwrap_or_else(index_response)
}

fn serve_path(path: &str) -> Option<Response> {
    let name = path.trim_start_matches('/');
    let name = if name.is_empty() { INDEX } else { name };
    let asset = Assets::get(name)?;
    Some(
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, content_type(name))],
            asset.data,
        )
            .into_response(),
    )
}

fn index_response() -> Response {
    match Assets::get(INDEX) {
        Some(asset) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            asset.data,
        )
            .into_response(),
        // web/dist missing at build time (release builds require it first).
        None => ApiError::new(
            StatusCode::NOT_FOUND,
            "SPA_MISSING",
            "前端资源未内嵌（先构建 web/dist）",
        )
        .into_response(),
    }
}

fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "json" | "map" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "webp" => "image/webp",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}
