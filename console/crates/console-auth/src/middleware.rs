//! axum middlewares: session auth (cookie/bearer) and m2m node auth (§4.0).
//!
//! Both are generic over the app state via small traits so the router owner
//! (tailrace-console) keeps its own `AppState`.

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, COOKIE};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use console_store::Store;
use serde_json::Value;

use crate::bootstrap::{BootstrapAuthority, VerifyError};
use crate::error::ApiError;
use crate::session::{SessionService, SESSION_COOKIE};

/// Max buffered m2m body (heartbeat payloads are a few KB).
const M2M_BODY_LIMIT: usize = 4 * 1024 * 1024;

// ---------------- extensions inserted on success ----------------

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user: crate::session::AuthUser,
    /// sha256 of the presented token — logout revokes by this key.
    pub token_hash: String,
}

#[derive(Debug, Clone)]
pub struct NodePrincipal {
    pub node_key: String,
}

// ---------------- state access traits ----------------

pub trait SessionState: Send + Sync + 'static {
    fn sessions(&self) -> &SessionService;
}

pub trait M2mState: Send + Sync + 'static {
    fn store(&self) -> &Store;
    fn bootstrap(&self) -> &BootstrapAuthority;
}

// ---------------- token extraction ----------------

/// Extract the session token from `Cookie: tr_session=…` or
/// `Authorization: Bearer …` (§4.0).
pub fn extract_session_token(req: &Request) -> Option<String> {
    if let Some(auth) = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = auth.strip_prefix("Bearer ").filter(|t| !t.is_empty()) {
            return Some(token.to_string());
        }
    }
    for header in req.headers().get_all(COOKIE) {
        let Ok(raw) = header.to_str() else { continue };
        for pair in raw.split(';') {
            let pair = pair.trim();
            if let Some(token) = pair.strip_prefix(&format!("{SESSION_COOKIE}=")) {
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
        }
    }
    None
}

// ---------------- session middleware ----------------

/// 401 envelope without a valid session; inserts [`AuthContext`] otherwise.
pub async fn require_session<S: SessionState + Clone>(
    State(state): State<S>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(token) = extract_session_token(&req) else {
        return ApiError::unauthenticated().into_response();
    };
    let Some((user, token_hash)) = state.sessions().authenticate(&token) else {
        return ApiError::unauthenticated().into_response();
    };
    req.extensions_mut()
        .insert(AuthContext { user, token_hash });
    next.run(req).await
}

// ---------------- m2m middleware ----------------

/// Node self-authentication for `POST /api/nodes/register` (§4.3): identity
/// and token ride in the JSON body (`nodeKey` / `bootstrapToken`); unknown
/// node_keys are admitted once with the console-global bootstrap token (they
/// are created by the handler, which issues a fresh server-generated
/// per-node token and stores only its sha256 digest).
pub async fn require_node_register<S: M2mState + Clone>(
    State(state): State<S>,
    req: Request,
    next: Next,
) -> Response {
    node_gate(state, req, true, next).await
}

/// Same as [`require_node_register`] but only already-registered nodes pass
/// (heartbeat must follow a successful register).
pub async fn require_node_heartbeat<S: M2mState + Clone>(
    State(state): State<S>,
    req: Request,
    next: Next,
) -> Response {
    node_gate(state, req, false, next).await
}

async fn node_gate<S: M2mState>(
    state: S,
    req: Request,
    allow_unknown: bool,
    next: Next,
) -> Response {
    // Buffer the body: nodeKey/bootstrapToken live inside the JSON payload.
    let (mut parts, body) = req.into_parts();
    let bytes = match to_bytes(body, M2M_BODY_LIMIT).await {
        Ok(b) => b,
        Err(_) => return ApiError::bad_request("请求体读取失败").into_response(),
    };
    let payload: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return ApiError::bad_request(format!("非法 JSON 请求体: {e}")).into_response(),
    };
    let node_key = payload.get("nodeKey").and_then(|v| v.as_str());
    let token = payload.get("bootstrapToken").and_then(|v| v.as_str());
    match state
        .bootstrap()
        .verify_node(state.store(), node_key, token, allow_unknown)
    {
        Ok(Some(node_key)) => {
            parts.extensions.insert(NodePrincipal { node_key });
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
        Ok(None) | Err(VerifyError::Missing) | Err(VerifyError::BadToken) => {
            ApiError::bad_node_token().into_response()
        }
        Err(VerifyError::Store) => ApiError::unauthenticated().into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request as HttpRequest;

    #[test]
    fn token_extraction_cookie_and_bearer() {
        let req = HttpRequest::get("/")
            .header(COOKIE, "other=1; tr_session=tr_s_abc; x=2")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_session_token(&req).as_deref(), Some("tr_s_abc"));
        let req = HttpRequest::get("/")
            .header(AUTHORIZATION, "Bearer tr_s_def")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_session_token(&req).as_deref(), Some("tr_s_def"));
        // bearer wins over cookie when both present
        let req = HttpRequest::get("/")
            .header(COOKIE, "tr_session=cookie-tok")
            .header(AUTHORIZATION, "Bearer bearer-tok")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_session_token(&req).as_deref(), Some("bearer-tok"));
        let req = HttpRequest::get("/").body(Body::empty()).unwrap();
        assert_eq!(extract_session_token(&req), None);
    }
}
