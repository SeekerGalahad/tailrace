//! Login/logout/me integration tests (§4.1) against the real router assembly.

use axum::body::{to_bytes, Body};
use axum::http::{header::SET_COOKIE, Request, StatusCode};
use axum::response::Response;
use axum::Router;
use console_auth::{hash_password, BootstrapAuthority};
use console_store::Store;
use serde_json::Value;
use tailrace_console::{build_router, state::AppState};
use tower::ServiceExt;

const PASSWORD: &str = "pw-123";

fn app() -> Router {
    let store = Store::open_in_memory().unwrap();
    store
        .create_user(
            "admin",
            &hash_password(PASSWORD).unwrap(),
            Some("管理员"),
            "admin",
        )
        .unwrap();
    let state = AppState::new(store, BootstrapAuthority::new("tok-secret"));
    build_router(state)
}

async fn json_body(resp: Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn login(app: &Router, username: &str, password: &str) -> Response {
    app.clone()
        .oneshot(
            Request::post("/api/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "username": username, "password": password }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn me_without_session_is_401() {
    let resp = app()
        .oneshot(Request::get("/api/auth/me").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = json_body(resp).await;
    assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
}

#[tokio::test]
async fn login_flow_cookie_and_bearer_and_logout() {
    let app = app();

    // wrong password -> 401 + envelope
    let resp = login(&app, "admin", "wrong").await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = json_body(resp).await;
    assert_eq!(body["error"]["code"], "BAD_CREDENTIALS");
    assert_eq!(body["error"]["message"], "用户名或密码错误");

    // correct login -> 200, token, cookie, user block (§4.1 shape)
    let resp = login(&app, "admin", PASSWORD).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cookie = resp
        .headers()
        .get(SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .expect("Set-Cookie present")
        .to_string();
    assert!(cookie.starts_with("tr_session=tr_s_"), "cookie: {cookie}");
    assert!(cookie.contains("HttpOnly"));
    assert!(cookie.contains("SameSite=Lax"));
    assert!(cookie.contains("Path=/"));
    let body = json_body(resp).await;
    let token = body["token"].as_str().unwrap().to_string();
    assert!(token.starts_with("tr_s_"));
    assert_eq!(body["expiresInSec"], 86400);
    assert_eq!(body["user"]["username"], "admin");
    assert_eq!(body["user"]["displayName"], "管理员");
    assert_eq!(body["user"]["role"], "admin");

    // /me without session -> 401
    let resp = app
        .clone()
        .oneshot(Request::get("/api/auth/me").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // /me with cookie -> 200
    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/auth/me")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["user"]["username"], "admin");

    // /me with Authorization: Bearer -> 200
    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/auth/me")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // logout -> 204 + cookie cleared
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/auth/logout")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let clear = resp
        .headers()
        .get(SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();
    assert!(clear.starts_with("tr_session=;"), "clear cookie: {clear}");

    // revoked session -> 401 on both channels
    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/auth/me")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = app
        .oneshot(
            Request::get("/api/auth/me")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_input_validation() {
    let app = app();
    // missing password -> 400 envelope
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"username":"admin"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = json_body(resp).await;
    assert_eq!(body["error"]["code"], "INVALID_ARGUMENT");

    // garbage json -> 400
    let resp = app
        .oneshot(
            Request::post("/api/auth/login")
                .header("content-type", "application/json")
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn healthz_404_and_spa_index() {
    let app = app();
    // healthz
    let resp = app
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["status"], "ok");

    // unknown API path -> 404 envelope (not SPA html)
    let resp = app
        .clone()
        .oneshot(Request::get("/api/nope").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_body(resp).await["error"]["code"], "NOT_FOUND");

    // SPA index served at /
    let resp = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        content_type.starts_with("text/html"),
        "content-type: {content_type}"
    );
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("<"), "looks like html");

    // SPA client route falls back to index.html
    let resp = app
        .oneshot(Request::get("/pipelines").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/html"));
}
