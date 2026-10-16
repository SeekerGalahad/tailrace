//! REST handlers (§4).

// Local helpers return unboxed `Response` errors; boxing would churn every
// Err site for no runtime gain (cold-path parse/load helpers).
#![allow(clippy::result_large_err)]

pub mod artifacts;
pub mod auth;
pub mod datasources;
pub mod health;
pub mod nodes;
pub mod nodes_admin;
pub mod pipelines;

use axum::http::HeaderMap;
use console_auth::{ApiError, AuthContext};
use console_store::{AuditEntry, Store};
use utoipa::ToSchema;

/// `{"error":{"code":"…","message":"…"}}` (§4.0 envelope).
#[derive(Debug, serde::Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, serde::Serialize, ToSchema)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

/// SQLite `datetime('now')` ("YYYY-MM-DD HH:MM:SS", UTC) → ISO-8601 Z, the
/// wire shape every §4 example uses ("2026-10-11T08:00:00Z").
pub fn iso(ts: Option<&str>) -> Option<String> {
    ts.map(|s| format!("{}Z", s.replace(' ', "T")))
}

/// Audit actor (§4.0): `X-Operator` header overrides the session username.
pub fn operator_of(ctx: &AuthContext, headers: &HeaderMap) -> String {
    headers
        .get("x-operator")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| ctx.user.username.clone())
}

/// Fire-and-forget audit write (§4.0 audit runs through every management
/// mutation): failures are logged, never fail the request.
pub fn audit(
    store: &Store,
    actor: &str,
    action: &str,
    entity_type: &str,
    entity_id: Option<String>,
    detail_json: Option<String>,
) {
    if let Err(e) = store.insert_audit(&AuditEntry {
        actor: actor.to_string(),
        action: action.to_string(),
        entity_type: entity_type.to_string(),
        entity_id,
        detail_json,
    }) {
        tracing::warn!(action, error = %e, "audit write failed");
    }
}

/// 501 for endpoints whose machinery ships in a later stage (§4.4 SSH deploy
/// chain and friends) — explicit TODO marker, not a silent stub.
pub fn not_implemented(what: &str, doc_ref: &str) -> ApiError {
    ApiError::new(
        axum::http::StatusCode::NOT_IMPLEMENTED,
        "NOT_IMPLEMENTED",
        format!("{what} 尚未实现（{doc_ref}）；本端点按计划推迟到后续阶段交付"),
    )
}
