//! Alert endpoints (T-E2): the combined rules + current state + recent
//! episodes read, audited rule CRUD, and the SSE transition stream fed by
//! the evaluator's broadcast hub ([`crate::alerting`]).

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{ApiError, AuthContext};
use console_store::{AlertEvent, AlertRule, AlertRuleWrite};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::alerting::{parse_params, RuleKind, RULE_TYPES};
use crate::api::{audit, iso, operator_of};
use crate::state::AppState;

/// Recent-episodes window of `GET /api/alerts`.
const RECENT_EVENTS: usize = 50;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertRuleBody {
    pub name: String,
    /// lag_seconds | state_failed | heartbeat_age | dlq_window | flush_errors_window
    pub rule_type: String,
    /// Type-dependent: `threshold` (lag/heartbeat seconds; window kinds
    /// row count), `windowSecs` (window kinds, 1..=3600), optional series
    /// filters `pipeline` / `statement` / `node`.
    #[serde(default)]
    pub params: Value,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Optional http(s) URL POSTed with the transition notice JSON.
    #[serde(default)]
    pub webhook_url: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertEventItem {
    pub id: i64,
    pub rule_id: i64,
    pub rule_name: String,
    /// firing | resolved
    pub state: String,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub resolved_at: Option<String>,
    /// Observed values at the firing transition (updated while firing).
    pub sample: Value,
    /// Observed values at the resolving transition.
    pub resolved_sample: Option<Value>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertRuleItem {
    pub id: i64,
    pub name: String,
    pub rule_type: String,
    pub params: Value,
    pub enabled: bool,
    pub webhook_url: Option<String>,
    /// inactive | firing (evaluator-owned current state)
    pub state: String,
    /// The open episode while firing.
    pub open_event: Option<AlertEventItem>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertsResponse {
    /// The five v1 rule kinds (client dropdown source).
    pub rule_types: Vec<String>,
    /// Evaluator period in seconds.
    pub interval_secs: u64,
    pub rules: Vec<AlertRuleItem>,
    /// Newest first, across rules.
    pub events: Vec<AlertEventItem>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AlertRuleCreated {
    pub id: i64,
}

fn parse_json_body<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, Response> {
    serde_json::from_slice(body).map_err(|e| {
        super::messages::bad_request("err.bad_body", &[&e.to_string()]).into_response()
    })
}

fn event_item(e: AlertEvent, rule_name: &str) -> AlertEventItem {
    AlertEventItem {
        id: e.id,
        rule_id: e.rule_id,
        rule_name: rule_name.to_string(),
        state: e.state,
        first_seen_at: iso(Some(&e.first_seen_at)).unwrap_or(e.first_seen_at),
        last_seen_at: iso(Some(&e.last_seen_at)).unwrap_or(e.last_seen_at),
        resolved_at: iso(e.resolved_at.as_deref()),
        sample: serde_json::from_str(&e.sample_json).unwrap_or(Value::Null),
        resolved_sample: e
            .resolved_sample_json
            .as_deref()
            .map(|s| serde_json::from_str(s).unwrap_or(Value::Null)),
    }
}

fn rule_item(r: AlertRule, open: Option<AlertEvent>) -> AlertRuleItem {
    let open_event = open.map(|e| event_item(e, &r.name));
    AlertRuleItem {
        id: r.id,
        name: r.name,
        rule_type: r.rule_type,
        params: serde_json::from_str(&r.params_json).unwrap_or(Value::Null),
        enabled: r.enabled,
        webhook_url: r.webhook_url,
        state: r.state,
        open_event,
        created_at: iso(Some(&r.created_at)).unwrap_or(r.created_at),
        updated_at: iso(Some(&r.updated_at)).unwrap_or(r.updated_at),
    }
}

/// Validate a body into (name, type, params JSON text, webhook) — the
/// §4.0 400 envelopes for every rejection.
fn validate(body: &AlertRuleBody) -> Result<(String, RuleKind, String, Option<String>), Response> {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err(super::messages::bad_request("err.alert.name_missing", &[]));
    }
    let Some(kind) = RuleKind::parse(body.rule_type.trim()) else {
        return Err(super::messages::bad_request(
            "err.alert.type_unknown",
            &[body.rule_type.trim()],
        ));
    };
    let params = if body.params.is_null() {
        json!({})
    } else {
        body.params.clone()
    };
    if let Err(reason) = parse_params(kind, &params) {
        return Err(super::messages::bad_request(
            "err.alert.params_invalid",
            &[&reason],
        ));
    }
    let webhook = body
        .webhook_url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    if let Some(u) = webhook.as_deref() {
        if !(u.starts_with("http://") || u.starts_with("https://")) {
            return Err(super::messages::bad_request(
                "err.alert.webhook_url_invalid",
                &[u],
            ));
        }
    }
    Ok((name, kind, params.to_string(), webhook))
}

/// `GET /api/alerts` — rules with their current state (+ open episode),
/// plus the most recent episodes across rules.
#[utoipa::path(
    get,
    path = "/api/alerts",
    responses((status = 200, body = AlertsResponse))
)]
pub async fn list(State(state): State<AppState>) -> Response {
    let store = state.store();
    let rules = match store.list_alert_rules() {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let names: std::collections::HashMap<i64, String> =
        rules.iter().map(|r| (r.id, r.name.clone())).collect();
    let mut items = Vec::with_capacity(rules.len());
    for r in rules {
        let open = if r.state == "firing" {
            store.open_alert_episode(r.id).ok().flatten()
        } else {
            None
        };
        items.push(rule_item(r, open));
    }
    let events = match store.list_alert_events(RECENT_EVENTS) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    Json(AlertsResponse {
        rule_types: RULE_TYPES.iter().map(|s| s.to_string()).collect(),
        interval_secs: crate::alerting::interval_from_env().as_secs(),
        rules: items,
        events: events
            .into_iter()
            .map(|e| {
                let name = names.get(&e.rule_id).cloned().unwrap_or_default();
                event_item(e, &name)
            })
            .collect(),
    })
    .into_response()
}

/// `POST /api/alerts/rules` — create a rule (state starts `inactive`;
/// the next evaluator pass picks it up). Audited ALERT_RULE_CREATE.
#[utoipa::path(
    post,
    path = "/api/alerts/rules",
    request_body = AlertRuleBody,
    responses(
        (status = 201, body = AlertRuleCreated),
        (status = 400, body = crate::api::ErrorResponse, description = "name missing / unknown ruleType / params invalid / webhookUrl not http(s)"),
        (status = 409, body = crate::api::ErrorResponse, description = "name already exists"),
    )
)]
pub async fn create(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let body: AlertRuleBody = match parse_json_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let (name, kind, params, webhook) = match validate(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let store = state.store();
    let id = match store.create_alert_rule(
        &name,
        kind.as_str(),
        &params,
        body.enabled,
        webhook.as_deref(),
    ) {
        Ok(AlertRuleWrite::Written(id)) => id,
        Ok(AlertRuleWrite::NameConflict) => {
            return super::messages::respond(
                axum::http::StatusCode::CONFLICT,
                "CONFLICT",
                "err.alert.name_taken",
                &[&name],
            )
        }
        Ok(AlertRuleWrite::NotFound) => unreachable!("create never reports NotFound"),
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    audit(
        store,
        &operator_of(&ctx, &headers),
        "ALERT_RULE_CREATE",
        "alert_rule",
        Some(id.to_string()),
        Some(
            json!({"name": name, "ruleType": kind.as_str(), "params": serde_json::from_str::<Value>(&params).unwrap_or(Value::Null),
                   "enabled": body.enabled, "webhookUrl": webhook})
            .to_string(),
        ),
    );
    (
        axum::http::StatusCode::CREATED,
        Json(AlertRuleCreated { id }),
    )
        .into_response()
}

/// `PUT /api/alerts/rules/{id}` — replace every user-editable field. The
/// evaluator state is kept (an edit never drops a firing alert; the next
/// pass re-evaluates under the new parameters). Audited ALERT_RULE_UPDATE.
#[utoipa::path(
    put,
    path = "/api/alerts/rules/{id}",
    params(("id" = i64, Path)),
    request_body = AlertRuleBody,
    responses(
        (status = 200, body = AlertRuleItem),
        (status = 400, body = crate::api::ErrorResponse),
        (status = 404, body = crate::api::ErrorResponse),
        (status = 409, body = crate::api::ErrorResponse, description = "name already exists"),
    )
)]
pub async fn update(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
    body: Bytes,
) -> Response {
    let body: AlertRuleBody = match parse_json_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let (name, kind, params, webhook) = match validate(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let store = state.store();
    match store.update_alert_rule(
        id,
        &name,
        kind.as_str(),
        &params,
        body.enabled,
        webhook.as_deref(),
    ) {
        Ok(AlertRuleWrite::Written(_)) => {}
        Ok(AlertRuleWrite::NotFound) => {
            return super::messages::not_found("err.alert.not_found", &[&id.to_string()])
        }
        Ok(AlertRuleWrite::NameConflict) => {
            return super::messages::respond(
                axum::http::StatusCode::CONFLICT,
                "CONFLICT",
                "err.alert.name_taken",
                &[&name],
            )
        }
        Err(e) => return ApiError::internal(&e).into_response(),
    }
    audit(
        store,
        &operator_of(&ctx, &headers),
        "ALERT_RULE_UPDATE",
        "alert_rule",
        Some(id.to_string()),
        Some(
            json!({"name": name, "ruleType": kind.as_str(), "params": serde_json::from_str::<Value>(&params).unwrap_or(Value::Null),
                   "enabled": body.enabled, "webhookUrl": webhook})
            .to_string(),
        ),
    );
    let Some(rule) = store.alert_rule(id).ok().flatten() else {
        return super::messages::not_found("err.alert.not_found", &[&id.to_string()]);
    };
    let open = if rule.state == "firing" {
        store.open_alert_episode(id).ok().flatten()
    } else {
        None
    };
    Json(rule_item(rule, open)).into_response()
}

/// `DELETE /api/alerts/rules/{id}` — 204; episodes cascade. Audited
/// ALERT_RULE_DELETE.
#[utoipa::path(
    delete,
    path = "/api/alerts/rules/{id}",
    params(("id" = i64, Path)),
    responses((status = 204), (status = 404, body = crate::api::ErrorResponse))
)]
pub async fn remove(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let store = state.store();
    let Some(rule) = store.alert_rule(id).ok().flatten() else {
        return super::messages::not_found("err.alert.not_found", &[&id.to_string()]);
    };
    match store.delete_alert_rule(id) {
        Ok(true) => {}
        Ok(false) => return super::messages::not_found("err.alert.not_found", &[&id.to_string()]),
        Err(e) => return ApiError::internal(&e).into_response(),
    }
    audit(
        store,
        &operator_of(&ctx, &headers),
        "ALERT_RULE_DELETE",
        "alert_rule",
        Some(id.to_string()),
        Some(json!({"name": rule.name, "ruleType": rule.rule_type}).to_string()),
    );
    axum::http::StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/alerts/stream` — SSE: one `alert` event per firing/resolved
/// transition (the notice JSON the webhook receives), `: keepalive` every
/// 15s. Subscribes to the evaluator's broadcast hub; a subscriber that
/// falls behind the bounded buffer gets a `lagged` event carrying the
/// number of skipped notices instead of a silent gap.
#[utoipa::path(
    get,
    path = "/api/alerts/stream",
    responses((status = 200, description = "text/event-stream: events alert / lagged, : keepalive"))
)]
pub async fn stream(State(state): State<AppState>) -> Response {
    use tokio_stream::StreamExt;
    let rx = state.subscribe_alerts();
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).map(|item| {
        Ok::<Event, std::convert::Infallible>(match item {
            Ok(notice) => Event::default()
                .event("alert")
                .data(serde_json::to_string(&notice).unwrap_or_else(|_| "{}".into())),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                Event::default()
                    .event("lagged")
                    .data(json!({"skipped": n}).to_string())
            }
        })
    });
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
}
