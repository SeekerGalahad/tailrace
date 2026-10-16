//! `GET /healthz` — liveness probe (topology §3.2: console serves it at :8080).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[utoipa::path(
    get,
    path = "/healthz",
    responses((status = 200, description = "console is alive", body = Object))
)]
pub async fn healthz() -> Response {
    (StatusCode::OK, axum::Json(json!({ "status": "ok" }))).into_response()
}
