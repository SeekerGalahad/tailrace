//! SLO history endpoint (R11 T-V4): `GET /api/slo` — lag p50/p95/p99 per
//! (pipeline, statement) over a window of the console's 1h sample ring
//! ([`crate::slo`]), with the sample count and the window bounds, so every
//! number can be judged by its support and recomputed from the ring rows.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::ApiError;
use serde::Serialize;
use utoipa::ToSchema;

use crate::slo::{self, SeriesSummary};
use crate::state::AppState;

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SloResponse {
    /// Effective window length in seconds: the `windowSecs` query clamped
    /// to 1..=retentionSecs (absent or unparsable = retentionSecs).
    pub window_secs: u64,
    /// Window start, epoch ms, EXCLUSIVE (window = (fromMs, toMs]).
    pub from_ms: i64,
    /// Window end, epoch ms, inclusive (the console clock at the request).
    pub to_ms: i64,
    /// Sample ring retention in seconds — the longest window available.
    pub retention_secs: u64,
    /// Sampling period in seconds (= the alert evaluator interval; one
    /// sample per series per pass, so a fully covered window holds about
    /// windowSecs / intervalSecs samples).
    pub interval_secs: u64,
    /// One entry per (pipeline, statement) with at least one sample in the
    /// window, ordered by pipeline id then statement index.
    pub series: Vec<SloSeries>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SloSeries {
    /// `pipeline` label value (console pipeline id).
    pub pipeline: String,
    /// `statement` label value (statement index).
    pub statement: String,
    /// Node of the newest sample in the window (provenance only — the SLO
    /// identity is pipeline + statement).
    pub node: String,
    /// Samples in the window: the quantiles' support.
    pub samples: u64,
    /// Nearest-rank quantiles of the window's lag samples, seconds: the
    /// value at 1-based rank ceil(samples × q) of the ascending samples (no
    /// interpolation — each is one of the stored samples).
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    /// Oldest / newest sample time in the window, epoch ms (coverage: a
    /// console that started 5 minutes ago holds 5 minutes of history).
    pub oldest_ms: i64,
    pub newest_ms: i64,
}

impl From<SeriesSummary> for SloSeries {
    fn from(s: SeriesSummary) -> Self {
        SloSeries {
            pipeline: s.pipeline,
            statement: s.statement,
            node: s.node,
            samples: s.samples as u64,
            p50: s.p50,
            p95: s.p95,
            p99: s.p99,
            oldest_ms: s.oldest_ms,
            newest_ms: s.newest_ms,
        }
    }
}

/// `GET /api/slo` — lag quantiles + sample count per pipeline/statement
/// over the window (`windowSecs` seconds ending now), optionally narrowed
/// to one `pipeline` / `statement` label value. Series idle at sampling
/// time (T-V1) are not in the ring.
#[utoipa::path(
    get,
    path = "/api/slo",
    params(
        ("windowSecs" = Option<u64>, Query, description = "window length in seconds, clamped 1..=3600 (default 3600 = the whole ring)"),
        ("pipeline" = Option<String>, Query, description = "only this pipeline (label value = console pipeline id)"),
        ("statement" = Option<String>, Query, description = "only this statement index"),
    ),
    responses((status = 200, body = SloResponse))
)]
pub async fn summary(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let window_secs = params
        .get("windowSecs")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(slo::RETENTION_SECS)
        .clamp(1, slo::RETENTION_SECS);
    let filter = |k: &str| params.get(k).map(|v| v.trim()).filter(|v| !v.is_empty());
    let now = slo::now_ms();
    let series = match slo::window_summaries(
        state.store(),
        now,
        window_secs,
        filter("pipeline"),
        filter("statement"),
    ) {
        Ok(v) => v,
        Err(e) => return ApiError::internal(&e).into_response(),
    };
    let (from_ms, to_ms) = slo::window_bounds(now, window_secs);
    Json(SloResponse {
        window_secs,
        from_ms,
        to_ms,
        retention_secs: slo::RETENTION_SECS,
        interval_secs: crate::alerting::interval_from_env().as_secs(),
        series: series.into_iter().map(SloSeries::from).collect(),
    })
    .into_response()
}
