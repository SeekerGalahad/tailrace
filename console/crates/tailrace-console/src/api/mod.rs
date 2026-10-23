//! REST handlers (§4).

// Local helpers return unboxed `Response` errors; boxing would churn every
// Err site for no runtime gain (cold-path parse/load helpers).
#![allow(clippy::result_large_err)]

pub mod artifacts;
pub mod auth;
pub mod datasources;
pub mod health;
pub mod messages;
pub mod nodes;
pub mod nodes_admin;
pub mod pipelines;

use axum::http::HeaderMap;
use console_auth::AuthContext;
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
    /// Reserved dictionary key for i18n message lookup (roadmap §九 T30):
    /// unset today, so it is absent from the wire (`skip_serializing_if`)
    /// and default responses serialize exactly as before; v1.x may populate
    /// it alongside `message`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_key: Option<String>,
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
/// chain and friends) — explicit TODO marker, not a silent stub. Returns the
/// §4.0 envelope directly (messages module: zh dictionary + message_key).
pub fn not_implemented(what: &str, doc_ref: &str) -> axum::response::Response {
    messages::respond(
        axum::http::StatusCode::NOT_IMPLEMENTED,
        "NOT_IMPLEMENTED",
        "err.not_implemented",
        &[what, doc_ref],
    )
}

#[cfg(test)]
mod tests {
    use super::{ErrorBody, ErrorResponse};

    /// Wire-compat (T30): the reserved `message_key` channel must stay
    /// invisible in today's error shape — a default body serializes to the
    /// exact §4.0 envelope with no extra key.
    #[test]
    fn error_body_default_has_no_message_key() {
        let body = ErrorResponse {
            error: ErrorBody {
                code: "UNAUTHENTICATED".to_string(),
                message: "未登录或会话已过期".to_string(),
                message_key: None,
            },
        };
        let json = serde_json::to_string(&body).expect("serialize ErrorResponse");
        assert!(
            !json.contains("message_key"),
            "message_key leaked into default wire shape: {json}"
        );
        assert_eq!(
            json,
            r#"{"error":{"code":"UNAUTHENTICATED","message":"未登录或会话已过期"}}"#
        );
    }

    /// The reservation itself is functional: a populated key serializes.
    #[test]
    fn error_body_message_key_serializes_when_set() {
        let body = ErrorBody {
            code: "NOT_FOUND".to_string(),
            message: "资源不存在".to_string(),
            message_key: Some("err.not_found".to_string()),
        };
        assert_eq!(
            serde_json::to_string(&body).expect("serialize ErrorBody"),
            r#"{"code":"NOT_FOUND","message":"资源不存在","message_key":"err.not_found"}"#
        );
    }
}
