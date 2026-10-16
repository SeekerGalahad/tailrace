//! Error envelope (§4.0): failures return HTTP 4xx/5xx +
//! `{"error":{"code":"…","message":"…"}}`. Success returns the resource JSON
//! directly (no 200+error-field wrapper).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    /// Extra structured payload merged into the envelope (IN_USE refs).
    pub refs: Option<serde_json::Map<String, serde_json::Value>>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.to_string(),
            message: message.into(),
            refs: None,
        }
    }

    /// 401 — no/invalid session (§4.0 error table).
    pub fn unauthenticated() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHENTICATED",
            "未登录或会话已过期",
        )
    }

    /// 400 — invalid payload (§4.0).
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "INVALID_ARGUMENT", message)
    }

    /// 401 — m2m bootstrap/node token mismatch (§4.0/§4.3).
    pub fn bad_node_token() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "BAD_NODE_TOKEN",
            "节点 bootstrap token 无效或已吊销",
        )
    }

    /// 404.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message)
    }

    /// 500 — unexpected storage failure.
    pub fn internal(err: &anyhow::Error) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            format!("内部错误: {err}"),
        )
    }

    /// Attach a `refs` object for IN_USE conflicts (§4.2 delete guards:
    /// `409 {"error":{"code":"IN_USE","refs":{"pipelines":[7]}}}`).
    pub fn with_refs(mut self, refs: serde_json::Map<String, serde_json::Value>) -> Self {
        self.refs = Some(refs);
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut error = json!({ "code": self.code, "message": self.message });
        if let Some(refs) = &self.refs {
            error["refs"] = Value::Object(refs.clone());
        }
        let body = json!({ "error": error });
        (self.status, Json(body)).into_response()
    }
}

/// Constant-time equality for secret material (session/node token hashes).
pub fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
