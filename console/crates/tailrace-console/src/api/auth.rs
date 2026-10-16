//! Auth endpoints (§4.1): login / logout / me.

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{header::SET_COOKIE, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{clear_session_cookie, session_cookie, ApiError, AuthContext, LoginError};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::ErrorResponse;
use crate::state::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub username: String,
    pub display_name: Option<String>,
    pub role: String,
}

impl UserInfo {
    fn from_auth(u: &console_auth::AuthUser) -> Self {
        Self {
            username: u.username.clone(),
            display_name: u.display_name.clone(),
            role: u.role.clone(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    pub token: String,
    pub expires_in_sec: i64,
    pub user: UserInfo,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MeResponse {
    pub user: UserInfo,
}

/// `POST /api/auth/login` — 200 + `Set-Cookie: tr_session=…` or 401 envelope.
#[utoipa::path(
    post,
    path = "/api/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, body = LoginResponse, description = "session issued; token also set as HttpOnly cookie"),
        (status = 400, body = ErrorResponse, description = "missing username/password"),
        (status = 401, body = ErrorResponse, description = "BAD_CREDENTIALS / ACCOUNT_LOCKED"),
    )
)]
pub async fn login(State(state): State<AppState>, body: Bytes) -> Response {
    let req: LoginRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return ApiError::bad_request(format!("非法登录请求: {e}")).into_response(),
    };
    if req.username.trim().is_empty() || req.password.is_empty() {
        return ApiError::bad_request("缺少用户名或密码").into_response();
    }
    match state.session_service().login(&req.username, &req.password) {
        Ok(ok) => {
            let _ = state.store().insert_audit(&console_store::AuditEntry {
                actor: ok.user.username.clone(),
                action: "LOGIN".into(),
                entity_type: "user".into(),
                entity_id: Some(ok.user.username.clone()),
                detail_json: None,
            });
            let resp = LoginResponse {
                token: ok.token.clone(),
                expires_in_sec: ok.expires_in_secs,
                user: UserInfo::from_auth(&ok.user),
            };
            (
                StatusCode::OK,
                [(SET_COOKIE, session_cookie(&ok.token))],
                Json(resp),
            )
                .into_response()
        }
        Err(LoginError::BadCredentials) => ApiError::new(
            StatusCode::UNAUTHORIZED,
            "BAD_CREDENTIALS",
            "用户名或密码错误",
        )
        .into_response(),
        Err(LoginError::Locked { retry_in_secs }) => ApiError::new(
            StatusCode::UNAUTHORIZED,
            "ACCOUNT_LOCKED",
            format!("尝试过于频繁，请 {retry_in_secs} 秒后再试"),
        )
        .into_response(),
    }
}

/// `GET /api/auth/me` — 200 current user or 401 (frontend jumps to /login).
#[utoipa::path(
    get,
    path = "/api/auth/me",
    responses(
        (status = 200, body = MeResponse),
        (status = 401, body = ErrorResponse),
    )
)]
pub async fn me(Extension(ctx): Extension<AuthContext>) -> Response {
    Json(MeResponse {
        user: UserInfo::from_auth(&ctx.user),
    })
    .into_response()
}

/// `POST /api/auth/logout` — 204, revokes the current session and clears the
/// cookie.
#[utoipa::path(
    post,
    path = "/api/auth/logout",
    responses(
        (status = 204, description = "session revoked"),
        (status = 401, body = ErrorResponse),
    )
)]
pub async fn logout(
    State(state): State<AppState>,
    Extension(ctx): Extension<AuthContext>,
) -> Response {
    state.session_service().logout(&ctx.token_hash);
    (
        StatusCode::NO_CONTENT,
        [(SET_COOKIE, clear_session_cookie())],
    )
        .into_response()
}
