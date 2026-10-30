//! T-E2 alerting: the console-side rules engine (R9 task card §一).
//!
//! The console already proxies and re-labels every registered node's
//! `/metrics` (A6), so the evaluator lives here — one periodic pass pulls
//! that aggregated snapshot ([`crate::metrics::gather`]), evaluates the five
//! v1 rule kinds against it and drives the episode state machine in
//! `alert_rule` / `alert_event`:
//!
//! - `lag_seconds` — `tailrace_pipeline_lag_seconds > threshold`
//! - `state_failed` — `tailrace_pipeline_state == 3` (ControlFlags FAILED)
//! - `heartbeat_age` — `tailrace_node_heartbeat_age_seconds > threshold`
//! - `dlq_window` — increment of `tailrace_dlq_rows_total` over
//!   `windowSecs` > threshold
//! - `flush_errors_window` — increment of
//!   `tailrace_flush_total{result="error"}` over `windowSecs` > threshold.
//!   The engine currently emits only `result="success"` (A6: the
//!   Dedup/Retry/Error split lands with the A2 deterministic labels), so
//!   the series is absent and the rule stays dormant until that lands —
//!   the evaluation contract is pinned by tests against mock snapshots.
//!
//! Anti-flood / anti-false-alarm decisions (review focus of the task card):
//!
//! - **Event rows land on transitions only**: an inactive→firing tick
//!   inserts one episode row, further firing ticks touch it, firing→inactive
//!   flips it to `resolved`. A resolved notice is therefore always reachable
//!   for every firing (also when the rule is disabled mid-flight).
//! - **Strict comparison** (`value > threshold`): a value exactly at the
//!   threshold does not fire (boundary pinned by tests).
//! - **Window arithmetic**: increment = latest − newest sample at or before
//!   `now − window`. History lives in memory ([`CounterWindows`], capped at
//!   [`MAX_WINDOW_SECS`]); a series with no base sample inside the window is
//!   NOT evaluated (a freshly restarted console never false-fires), and a
//!   counter reset (latest < base) credits the post-reset value — a true
//!   lower bound of what happened inside the window.
//! - **Absent series never fire**: a node that stops exposing a series is
//!   not a violation (heartbeat_age covers node-down separately).

use std::collections::{BTreeMap, HashMap, VecDeque};

use console_store::AlertRule;
use serde::Serialize;
use serde_json::{json, Value};

use crate::state::AppState;

/// The five v1 rule kinds (task card: `规则 v1 五类`).
pub const RULE_TYPES: &[&str] = &[
    "lag_seconds",
    "state_failed",
    "heartbeat_age",
    "dlq_window",
    "flush_errors_window",
];

/// Upper bound for a rule's `windowSecs` — also the retention of the
/// in-memory counter history (bounded memory; a longer window would need
/// the sample store v1 deliberately does not have).
pub const MAX_WINDOW_SECS: u64 = 3600;

/// Default evaluator period (task card: 15s 默认). Override with
/// `TAILRACE_ALERT_INTERVAL_SECS` (min 1).
pub const DEFAULT_INTERVAL_SECS: u64 = 15;

// ---------------- snapshot parsing ----------------

/// One exposition sample (`name{labels} value`).
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub metric: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Parse Prometheus text-format lines into samples; comments, HELP/TYPE and
/// blank lines are skipped. `node` (when given) is injected as a `node`
/// label — the same re-labelling the /metrics proxy applies during
/// assembly, so evaluator and scrape see identical series identities.
pub fn parse_samples(text: &str, node: Option<&str>) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (metric, labels, rest) = match line.find('{') {
            Some(open) if line.find(' ').is_none_or(|sp| open < sp) => {
                let Some(rel) = line[open..].find('}') else {
                    continue;
                };
                let close = open + rel;
                let body = &line[open + 1..close];
                // a label set that does not parse drops the whole line —
                // keeping it label-less would fold distinct series together
                let Some(mut labels) = parse_label_set(body) else {
                    continue;
                };
                if let Some(n) = node {
                    labels.insert("node".to_string(), n.to_string());
                }
                (&line[..open], labels, &line[close + 1..])
            }
            _ => {
                let Some(sp) = line.find(' ') else { continue };
                let mut labels = BTreeMap::new();
                if let Some(n) = node {
                    labels.insert("node".to_string(), n.to_string());
                }
                (&line[..sp], labels, &line[sp + 1..])
            }
        };
        let Some(value_tok) = rest.split_whitespace().next() else {
            continue;
        };
        let Some(value) = parse_f64(value_tok) else {
            continue;
        };
        out.push(Sample {
            metric: metric.to_string(),
            labels,
            value,
        });
    }
    out
}

/// `k="v", k2="v2"` → map (label values unescaped). `None` when the body is
/// malformed (unterminated quote, missing `=`) — the caller drops the line.
fn parse_label_set(body: &str) -> Option<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // skip separators / whitespace
        while i < bytes.len() && (bytes[i] == b',' || bytes[i] == b' ') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' {
            i += 1;
        }
        if i >= bytes.len() {
            return None;
        }
        let key = body[key_start..i].trim().to_string();
        i += 1; // '='
        if i >= bytes.len() || bytes[i] != b'"' {
            return None;
        }
        i += 1;
        let mut val = String::new();
        let mut closed = false;
        while i < bytes.len() {
            match bytes[i] {
                b'\\' => {
                    if i + 1 < bytes.len() {
                        match bytes[i + 1] {
                            b'\\' => val.push('\\'),
                            b'"' => val.push('"'),
                            b'n' => val.push('\n'),
                            other => val.push(other as char),
                        }
                        i += 2;
                        continue;
                    }
                    i += 1;
                }
                b'"' => {
                    closed = true;
                    i += 1;
                    break;
                }
                _ => {
                    // copy one UTF-8 char (multi-byte sequences pass through)
                    let ch_len = utf8_len(bytes[i]);
                    val.push_str(&body[i..(i + ch_len).min(body.len())]);
                    i += ch_len;
                }
            }
        }
        if !closed || key.is_empty() {
            return None;
        }
        out.insert(key, val);
    }
    Some(out)
}

/// UTF-8 sequence length from the leading byte (defaults to 1 — a
/// continuation byte here means the slice boundary is mid-sequence).
fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1,
    }
}

/// Exposition float: `NaN`/`±Inf` spelled the way the format spells them,
/// else Rust's own parser.
fn parse_f64(tok: &str) -> Option<f64> {
    match tok {
        "NaN" => Some(f64::NAN),
        "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        _ => tok.parse::<f64>().ok(),
    }
}

// ---------------- rules ----------------

/// The five v1 kinds, parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleKind {
    LagSeconds,
    StateFailed,
    HeartbeatAge,
    DlqWindow,
    FlushErrorsWindow,
}

impl RuleKind {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "lag_seconds" => RuleKind::LagSeconds,
            "state_failed" => RuleKind::StateFailed,
            "heartbeat_age" => RuleKind::HeartbeatAge,
            "dlq_window" => RuleKind::DlqWindow,
            "flush_errors_window" => RuleKind::FlushErrorsWindow,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RuleKind::LagSeconds => "lag_seconds",
            RuleKind::StateFailed => "state_failed",
            RuleKind::HeartbeatAge => "heartbeat_age",
            RuleKind::DlqWindow => "dlq_window",
            RuleKind::FlushErrorsWindow => "flush_errors_window",
        }
    }
}

/// Parsed, validated rule parameters (type-dependent subset; absent
/// pipeline/statement/node filters match every series).
#[derive(Debug, Clone, PartialEq)]
pub struct RuleParams {
    /// lag/heartbeat: seconds; window kinds: row count. `state_failed`
    /// ignores it (the condition is equality with FAILED).
    pub threshold: f64,
    /// window kinds only (seconds).
    pub window_secs: u64,
    pub pipeline: Option<String>,
    pub statement: Option<String>,
    pub node: Option<String>,
}

impl RuleParams {
    /// The SLO target of a lag rule is its threshold (task card: the lag
    /// threshold IS the lag_target).
    pub fn lag_target(&self) -> f64 {
        self.threshold
    }
}

/// Validate + parse the wire params object for one kind. `Err` carries the
/// reason text for the 400 envelope (API layer prefixes the dictionary
/// message).
pub fn parse_params(kind: RuleKind, params: &Value) -> Result<RuleParams, String> {
    if !params.is_object() {
        return Err("params must be a JSON object".into());
    }
    let get_str = |k: &str| -> Option<String> {
        params
            .get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    let base = RuleParams {
        threshold: 0.0,
        window_secs: 0,
        pipeline: get_str("pipeline"),
        statement: get_str("statement"),
        node: get_str("node"),
    };
    match kind {
        RuleKind::StateFailed => Ok(base),
        RuleKind::LagSeconds | RuleKind::HeartbeatAge => {
            let Some(threshold) = number(params, "threshold") else {
                return Err(format!(
                    "threshold (seconds, number > 0) is required for {}",
                    kind.as_str()
                ));
            };
            // NaN / inf are rejected too (a NaN threshold would never fire)
            if !threshold.is_finite() || threshold <= 0.0 {
                return Err("threshold must be > 0".into());
            }
            Ok(RuleParams { threshold, ..base })
        }
        RuleKind::DlqWindow | RuleKind::FlushErrorsWindow => {
            let Some(threshold) = number(params, "threshold") else {
                return Err(format!(
                    "threshold (row count, number >= 1) is required for {}",
                    kind.as_str()
                ));
            };
            if !threshold.is_finite() || threshold < 1.0 {
                return Err("threshold must be >= 1".into());
            }
            let Some(window) = params.get("windowSecs").and_then(Value::as_u64) else {
                return Err("windowSecs (seconds, 1..=3600) is required".into());
            };
            if !(1..=MAX_WINDOW_SECS).contains(&window) {
                return Err(format!("windowSecs must be within 1..={MAX_WINDOW_SECS}"));
            }
            Ok(RuleParams {
                threshold,
                window_secs: window,
                ..base
            })
        }
    }
}

fn number(params: &Value, key: &str) -> Option<f64> {
    match params.get(key) {
        Some(Value::Number(n)) => n.as_f64(),
        // tolerate a numeric string ("30") — the web form layer may hand
        // form fields through as strings
        Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// Does a sample match the rule's series filters? A filter set on a label
/// the series does not carry excludes the series (heartbeat series have no
/// pipeline label; a pipeline-filtered heartbeat rule matches nothing).
pub fn series_matches(sample: &Sample, params: &RuleParams) -> bool {
    let label = |k: &str| sample.labels.get(k).map(String::as_str);
    params
        .pipeline
        .as_deref()
        .is_none_or(|v| label("pipeline") == Some(v))
        && params
            .statement
            .as_deref()
            .is_none_or(|v| label("statement") == Some(v))
        && params
            .node
            .as_deref()
            .is_none_or(|v| label("node") == Some(v))
}

// ---------------- window history ----------------

/// In-memory counter history for the two increment rules, keyed by series
/// identity (rendered metric+labels). One `observe` per evaluation tick;
/// entries older than [`MAX_WINDOW_SECS`] (plus slack for clock skew) are
/// pruned on write.
#[derive(Default)]
pub struct CounterWindows {
    series: HashMap<String, VecDeque<(u64, f64)>>,
}

impl CounterWindows {
    /// Record this tick's counter samples. Series are matched by exact
    /// label set (node included — the console's re-labelling makes two
    /// nodes' pipeline-1 series distinct).
    pub fn observe(&mut self, samples: &[Sample], now_ms: u64) {
        let horizon = now_ms.saturating_sub((MAX_WINDOW_SECS + 60) * 1000);
        for s in samples {
            if s.metric != "tailrace_dlq_rows_total" && s.metric != "tailrace_flush_total" {
                continue;
            }
            let key = series_key(s);
            let q = self.series.entry(key).or_default();
            q.push_back((now_ms, s.value));
            while q.front().is_some_and(|(ts, _)| *ts < horizon) {
                q.pop_front();
            }
        }
        self.series.retain(|_, q| !q.is_empty());
    }

    /// Increment of one series over the window ending at `now_ms`:
    /// latest − newest sample at or before `now − window`. `None` when no
    /// sample at or before the cutoff exists (history shorter than the
    /// window — not evaluated, never fires); a reset (latest < base)
    /// credits the post-reset value, a true lower bound for the window.
    fn increment(&self, key: &str, window_secs: u64, now_ms: u64) -> Option<f64> {
        let q = self.series.get(key)?;
        let latest = q.back()?;
        let cutoff = now_ms.saturating_sub(window_secs * 1000);
        let base = q.iter().rev().find(|(ts, _)| *ts <= cutoff).copied()?;
        if latest.1 >= base.1 {
            Some(latest.1 - base.1)
        } else {
            Some(latest.1)
        }
    }
}

fn series_key(s: &Sample) -> String {
    // BTreeMap iteration is sorted — same labels, same key regardless of
    // exposition order.
    let labels: Vec<String> = s
        .labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect();
    format!("{}{{{}}}", s.metric, labels.join(","))
}

// ---------------- evaluation ----------------

/// One rule's verdict for one snapshot.
pub struct Verdict {
    pub firing: bool,
    /// Observed values for the episode row / notices.
    pub sample: Value,
}

/// Evaluate one rule against one snapshot. `windows` must already hold this
/// tick's counters ([`CounterWindows::observe`]).
pub fn evaluate_rule(
    kind: RuleKind,
    params: &RuleParams,
    samples: &[Sample],
    windows: &CounterWindows,
    now_ms: u64,
) -> Verdict {
    match kind {
        RuleKind::LagSeconds => {
            threshold_verdict(samples, params, "tailrace_pipeline_lag_seconds", |v| v)
        }
        RuleKind::HeartbeatAge => threshold_verdict(
            samples,
            params,
            "tailrace_node_heartbeat_age_seconds",
            |v| v,
        ),
        RuleKind::StateFailed => {
            let mut worst: Option<(&Sample, usize)> = None;
            let mut violating = 0usize;
            for s in samples.iter().filter(|s| {
                s.metric == "tailrace_pipeline_state" && s.value == 3.0 && series_matches(s, params)
            }) {
                violating += 1;
                if worst.is_none() {
                    worst = Some((s, violating));
                }
            }
            match worst {
                Some((s, _)) => Verdict {
                    firing: true,
                    sample: json!({
                        "metric": s.metric, "value": s.value,
                        "labels": s.labels, "violating": violating,
                        "expected": "state != 3 (FAILED)",
                    }),
                },
                None => Verdict {
                    firing: false,
                    sample: json!({
                        "metric": "tailrace_pipeline_state", "value": null,
                        "violating": 0, "expected": "state != 3 (FAILED)",
                    }),
                },
            }
        }
        RuleKind::DlqWindow | RuleKind::FlushErrorsWindow => {
            let (metric, result_filter) = match kind {
                RuleKind::DlqWindow => ("tailrace_dlq_rows_total", None),
                _ => ("tailrace_flush_total", Some("error")),
            };
            let mut worst: Option<(f64, String)> = None; // (increment, series key)
            let mut observed: Option<f64> = None; // largest evaluated increment, violating or not
            let mut violating = 0usize;
            for s in samples.iter().filter(|s| {
                s.metric == metric
                    && result_filter
                        .is_none_or(|r| s.labels.get("result").map(String::as_str) == Some(r))
                    && series_matches(s, params)
            }) {
                let key = series_key(s);
                let Some(inc) = windows.increment(&key, params.window_secs, now_ms) else {
                    continue; // no baseline inside the window — not evaluated
                };
                if observed.is_none_or(|o| inc > o) {
                    observed = Some(inc);
                }
                if inc > params.threshold {
                    violating += 1;
                    if worst.as_ref().is_none_or(|(w, _)| inc > *w) {
                        worst = Some((inc, key));
                    }
                }
            }
            match worst {
                Some((inc, key)) => Verdict {
                    firing: true,
                    sample: json!({
                        "metric": metric, "increment": inc, "threshold": params.threshold,
                        "windowSecs": params.window_secs, "series": key, "violating": violating,
                    }),
                },
                None => Verdict {
                    firing: false,
                    sample: json!({
                        "metric": metric, "increment": observed,
                        "threshold": params.threshold, "windowSecs": params.window_secs,
                        "violating": 0,
                    }),
                },
            }
        }
    }
}

/// Shared shape of the three threshold-style kinds: the worst (largest)
/// matching series decides; comparison is strict (`> threshold`).
fn threshold_verdict(
    samples: &[Sample],
    params: &RuleParams,
    metric: &str,
    convert: impl Fn(f64) -> f64,
) -> Verdict {
    let mut worst: Option<&Sample> = None;
    let mut violating = 0usize;
    for s in samples
        .iter()
        .filter(|s| s.metric == metric && series_matches(s, params))
    {
        if s.value > params.threshold {
            violating += 1;
        }
        if worst.is_none_or(|w| s.value > w.value) {
            worst = Some(s);
        }
    }
    match worst {
        Some(w) => Verdict {
            firing: w.value > params.threshold,
            sample: json!({
                "metric": metric, "value": convert(w.value),
                "threshold": params.threshold, "labels": w.labels,
                "violating": violating,
            }),
        },
        None => Verdict {
            firing: false,
            sample: json!({"metric": metric, "value": null,
                           "threshold": params.threshold, "violating": 0}),
        },
    }
}

// ---------------- pass driver (store + notices) ----------------

/// A transition notice — what the SSE hub broadcasts and webhooks receive.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertNotice {
    /// "firing" | "resolved"
    pub event: String,
    pub event_id: i64,
    pub rule_id: i64,
    pub rule_name: String,
    pub rule_type: String,
    pub params: Value,
    pub sample: Value,
    pub ts_ms: u64,
}

/// Evaluator-owned per-process state (counter history). One instance lives
/// in the spawned loop; tests build their own and drive passes directly.
#[derive(Default)]
pub struct Evaluator {
    windows: CounterWindows,
}

impl Evaluator {
    pub fn new() -> Self {
        Self::default()
    }

    /// One evaluation pass: record counters, evaluate every rule, persist
    /// the state transitions, broadcast + webhook each notice. Returns the
    /// notices emitted (contract-test surface).
    pub async fn run_pass(
        &mut self,
        state: &AppState,
        samples: &[Sample],
        now_ms: u64,
    ) -> anyhow::Result<Vec<AlertNotice>> {
        self.windows.observe(samples, now_ms);
        let rules = state.store().list_alert_rules()?;
        let mut notices = Vec::new();
        for rule in rules {
            let (kind, params) = match decode_rule(&rule) {
                Some(v) => v,
                None => {
                    // Rules are validated at the API boundary; a bad row
                    // (hand-edited store, older shape) is skipped loudly —
                    // its state machine is deliberately left untouched.
                    tracing::warn!(rule = rule.id, rule_type = %rule.rule_type,
                        "alert rule has invalid type/params, skipping");
                    continue;
                }
            };
            let firing_state = rule.state == "firing";
            let (condition, sample) = if !rule.enabled {
                // A disabled rule cannot keep firing: resolve with the
                // reason so the episode always closes (resolved 必达).
                (false, json!({"reason": "disabled"}))
            } else {
                let v = evaluate_rule(kind, &params, samples, &self.windows, now_ms);
                (v.firing, v.sample)
            };
            let notice = match (firing_state, condition) {
                (false, true) => {
                    let event_id = state
                        .store()
                        .begin_alert_episode(rule.id, &sample.to_string())?;
                    Some((rule, sample, "firing", event_id))
                }
                (true, true) => {
                    state
                        .store()
                        .touch_alert_episode(rule.id, &sample.to_string())?;
                    None
                }
                (true, false) => {
                    let event_id = self.close_episode(state, &rule, &sample)?;
                    Some((rule, sample, "resolved", event_id))
                }
                (false, false) => None,
            };
            if let Some((rule, sample, event, event_id)) = notice {
                let n = AlertNotice {
                    event: event.to_string(),
                    event_id,
                    rule_id: rule.id,
                    rule_name: rule.name.clone(),
                    rule_type: rule.rule_type.clone(),
                    params: serde_json::from_str(&rule.params_json).unwrap_or(Value::Null),
                    sample,
                    ts_ms: now_ms,
                };
                // SSE hub: send fails only when nobody subscribed (fine).
                let _ = state.alerts_tx().send(n.clone());
                if let Some(url) = rule.webhook_url.as_deref() {
                    deliver_webhook(state, url, &n).await;
                }
                notices.push(n);
            }
        }
        Ok(notices)
    }

    /// Close the rule's open episode; the notice carries the episode id (or
    /// −1 when none was open — the rule state is still forced inactive).
    fn close_episode(
        &self,
        state: &AppState,
        rule: &AlertRule,
        sample: &Value,
    ) -> anyhow::Result<i64> {
        let open = state.store().open_alert_episode(rule.id)?;
        let event_id = open.map_or(-1, |e| e.id);
        state
            .store()
            .resolve_alert_episode(rule.id, &sample.to_string())?;
        Ok(event_id)
    }
}

/// Type + validated params of a stored rule.
fn decode_rule(rule: &AlertRule) -> Option<(RuleKind, RuleParams)> {
    let kind = RuleKind::parse(&rule.rule_type)?;
    let params: Value = serde_json::from_str(&rule.params_json).ok()?;
    parse_params(kind, &params).ok().map(|p| (kind, p))
}

/// Webhook POST (v1: fire-and-log, no retry). Failures never fail the pass.
async fn deliver_webhook(state: &AppState, url: &str, notice: &AlertNotice) {
    let resp = state
        .node_client()
        .post(url)
        .json(notice)
        .header("x-tailrace-event", &notice.event)
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => tracing::warn!(
            rule = notice.rule_id,
            url,
            status = r.status().as_u16(),
            "alert webhook delivery failed (v1: logged, not retried)"
        ),
        Err(e) => tracing::warn!(
            rule = notice.rule_id, url, error = %e,
            "alert webhook delivery failed (v1: logged, not retried)"
        ),
    }
}

/// Spawn the periodic evaluator (the backfill orphan-sweep startup slot in
/// `main.rs` calls this). One pass per `interval`; a slow pass delays the
/// next instead of bursting (MissedTickBehavior::Delay).
pub fn spawn_evaluator(
    state: AppState,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut evaluator = Evaluator::new();
        tracing::info!(
            interval_secs = interval.as_secs(),
            "alert evaluator started"
        );
        loop {
            ticker.tick().await;
            let gathered = crate::metrics::gather(&state).await;
            let samples = crate::metrics::node_samples(&gathered);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            if let Err(e) = evaluator.run_pass(&state, &samples, now_ms).await {
                tracing::warn!(error = %e, "alert evaluation pass failed");
            }
        }
    })
}

/// `TAILRACE_ALERT_INTERVAL_SECS` → interval (default 15s, min 1s).
pub fn interval_from_env() -> std::time::Duration {
    let secs = std::env::var("TAILRACE_ALERT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(DEFAULT_INTERVAL_SECS);
    std::time::Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(metric: &str, labels: &[(&str, &str)], value: f64) -> Sample {
        Sample {
            metric: metric.into(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value,
        }
    }

    fn samples_from_text(text: &str, node: Option<&str>) -> Vec<Sample> {
        parse_samples(text, node)
    }

    /// Parsing: labelled + label-less lines, escapes, node injection,
    /// comments skipped, `NaN`/`+Inf` accepted, junk ignored.
    #[test]
    fn parse_samples_handles_exposition_shapes() {
        let text = "# TYPE tailrace_pipeline_lag_seconds gauge\n\
                    # HELP tailrace_pipeline_lag_seconds lag\n\
                    tailrace_pipeline_lag_seconds{pipeline=\"3\",statement=\"0\"} 12.5\n\
                    tailrace_up 1\n\
                    tailrace_flush_total{pipeline=\"3\",result=\"error\"} NaN\n\
                    tailrace_pipeline_lag_seconds{pipeline=\"4\"} +Inf\n\
                    not a sample line\n\
                    tailrace_bad{unclosed=\"x} 3\n";
        let out = samples_from_text(text, Some("edge-1"));
        assert_eq!(out.len(), 4, "{out:?}");
        assert_eq!(
            out[0],
            sample(
                "tailrace_pipeline_lag_seconds",
                &[("node", "edge-1"), ("pipeline", "3"), ("statement", "0")],
                12.5
            )
        );
        assert_eq!(out[1], sample("tailrace_up", &[("node", "edge-1")], 1.0));
        assert!(out[2].value.is_nan());
        assert!(out[3].value.is_infinite());
        // escaped label values unescape
        let esc = r#"tailrace_x{k="a\"b\\c\nd"} 1"#;
        let out = samples_from_text(esc, None);
        assert_eq!(out[0].labels.get("k").unwrap(), "a\"b\\c\nd");
    }

    /// Threshold boundary (review focus): strictly greater — a value AT the
    /// threshold does not fire; the worst series decides; filters scope.
    #[test]
    fn lag_threshold_is_strict_and_takes_the_worst_series() {
        let params = parse_params(
            RuleKind::LagSeconds,
            &json!({"threshold": 30, "pipeline": "7"}),
        )
        .unwrap();
        let samples = vec![
            sample(
                "tailrace_pipeline_lag_seconds",
                &[("node", "a"), ("pipeline", "7"), ("statement", "0")],
                30.0,
            ),
            sample(
                "tailrace_pipeline_lag_seconds",
                &[("node", "a"), ("pipeline", "7"), ("statement", "1")],
                31.5,
            ),
            // other pipelines never considered
            sample(
                "tailrace_pipeline_lag_seconds",
                &[("node", "a"), ("pipeline", "9"), ("statement", "0")],
                900.0,
            ),
        ];
        let v = evaluate_rule(
            RuleKind::LagSeconds,
            &params,
            &samples,
            &CounterWindows::default(),
            0,
        );
        assert!(v.firing);
        assert_eq!(v.sample["value"], 31.5);
        assert_eq!(v.sample["violating"], 1);
        // exactly at the threshold: not firing (strict >)
        let samples = vec![sample(
            "tailrace_pipeline_lag_seconds",
            &[("node", "a"), ("pipeline", "7"), ("statement", "0")],
            30.0,
        )];
        let v = evaluate_rule(
            RuleKind::LagSeconds,
            &params,
            &samples,
            &CounterWindows::default(),
            0,
        );
        assert!(!v.firing, "value == threshold must not fire");
        assert_eq!(v.sample["violating"], 0);
        // no matching series at all: not firing, value null
        let v = evaluate_rule(
            RuleKind::LagSeconds,
            &params,
            &[],
            &CounterWindows::default(),
            0,
        );
        assert!(!v.firing);
        assert!(v.sample["value"].is_null());
    }

    /// state_failed fires on state==3 only; other pipelines' states ignored.
    #[test]
    fn state_failed_fires_on_failed_state() {
        let params = parse_params(RuleKind::StateFailed, &json!({})).unwrap();
        let samples = vec![
            sample(
                "tailrace_pipeline_state",
                &[("node", "a"), ("pipeline", "1")],
                0.0,
            ),
            sample(
                "tailrace_pipeline_state",
                &[("node", "a"), ("pipeline", "2")],
                3.0,
            ),
            sample(
                "tailrace_pipeline_state",
                &[("node", "a"), ("pipeline", "3")],
                1.0,
            ),
        ];
        let v = evaluate_rule(
            RuleKind::StateFailed,
            &params,
            &samples,
            &CounterWindows::default(),
            0,
        );
        assert!(v.firing);
        assert_eq!(v.sample["value"], 3.0);
        assert_eq!(v.sample["violating"], 1);
        let only_ok = vec![
            sample(
                "tailrace_pipeline_state",
                &[("node", "a"), ("pipeline", "1")],
                0.0,
            ),
            sample(
                "tailrace_pipeline_state",
                &[("node", "a"), ("pipeline", "2")],
                2.0,
            ),
        ];
        let v = evaluate_rule(
            RuleKind::StateFailed,
            &params,
            &only_ok,
            &CounterWindows::default(),
            0,
        );
        assert!(!v.firing);
    }

    /// Window alignment (review focus): the base is the newest sample at or
    /// before `now − window`; history younger than the window does not
    /// evaluate (no restart false-fires); the base slides forward with the
    /// window; a counter reset credits the post-reset value; samples older
    /// than the retention horizon are pruned (and cannot serve as a base).
    /// Timestamps are realistic epoch-ms (a 0 origin would let u64
    /// saturation fabricate a base at the cutoff).
    #[test]
    fn dlq_window_alignment_and_resets() {
        const T0: u64 = 1_000_000_000_000;
        let mut w = CounterWindows::default();
        let params = parse_params(
            RuleKind::DlqWindow,
            &json!({"threshold": 10, "windowSecs": 60}),
        )
        .unwrap();
        let s = |v: f64| {
            sample(
                "tailrace_dlq_rows_total",
                &[("node", "a"), ("pipeline", "1"), ("statement", "0")],
                v,
            )
        };

        // t=0 → 100, t=30s → 105: at t=45s the cutoff (t=−15s) has no
        // sample at or before it → not evaluated → not firing.
        w.observe(&[s(100.0)], T0);
        w.observe(&[s(105.0)], T0 + 30_000);
        let v = evaluate_rule(RuleKind::DlqWindow, &params, &[s(105.0)], &w, T0 + 45_000);
        assert!(!v.firing, "history younger than the window never fires");
        assert!(v.sample["increment"].is_null());

        // t=60s → 112: base = t=0 (<= cutoff t=0), increment 12 > 10 fires.
        w.observe(&[s(112.0)], T0 + 60_000);
        let v = evaluate_rule(RuleKind::DlqWindow, &params, &[s(112.0)], &w, T0 + 60_000);
        assert!(v.firing, "base at t=0, increment 12");
        assert_eq!(v.sample["increment"], 12.0);

        // t=100s → 120: cutoff t=40s, base = newest <= 40s = t=30s (105),
        // increment 15 — the t=0 sample no longer decides.
        w.observe(&[s(120.0)], T0 + 100_000);
        let v = evaluate_rule(RuleKind::DlqWindow, &params, &[s(120.0)], &w, T0 + 100_000);
        assert!(v.firing);
        assert_eq!(
            v.sample["increment"], 15.0,
            "base slides to the t=30s sample"
        );

        // t=200s → 125: cutoff t=140s, base = t=100s (120), increment 5 →
        // resolved (the big early jump has left the window).
        w.observe(&[s(125.0)], T0 + 200_000);
        let v = evaluate_rule(RuleKind::DlqWindow, &params, &[s(125.0)], &w, T0 + 200_000);
        assert!(!v.firing, "increment 5 within the current window");
        assert_eq!(v.sample["increment"], 5.0);

        // counter reset: latest (30) < base (120) → increment credited as
        // the post-reset value (30 rows certainly landed inside the window).
        let mut w2 = CounterWindows::default();
        w2.observe(&[s(120.0)], T0);
        w2.observe(&[s(5.0)], T0 + 30_000);
        w2.observe(&[s(30.0)], T0 + 70_000);
        let v = evaluate_rule(RuleKind::DlqWindow, &params, &[s(30.0)], &w2, T0 + 70_000);
        assert!(v.firing);
        assert_eq!(
            v.sample["increment"], 30.0,
            "reset between base and latest credits the post-reset value"
        );

        // retention: a sample older than MAX_WINDOW_SECS+60s is pruned on
        // observe, so a max-length window right after a 2h gap has no base.
        let mut w3 = CounterWindows::default();
        let long = parse_params(
            RuleKind::DlqWindow,
            &json!({"threshold": 10, "windowSecs": MAX_WINDOW_SECS}),
        )
        .unwrap();
        w3.observe(&[s(0.0)], T0);
        w3.observe(&[s(50.0)], T0 + 7_200_000);
        let v = evaluate_rule(RuleKind::DlqWindow, &long, &[s(50.0)], &w3, T0 + 7_200_000);
        assert!(!v.firing, "pruned history cannot serve as a base");
        assert!(v.sample["increment"].is_null());
    }

    /// flush_errors_window only counts the result="error" series; the
    /// success series is invisible to it.
    #[test]
    fn flush_errors_window_filters_result_label() {
        let mut w = CounterWindows::default();
        let params = parse_params(
            RuleKind::FlushErrorsWindow,
            &json!({"threshold": 2, "windowSecs": 60}),
        )
        .unwrap();
        let err = |v: f64| {
            sample(
                "tailrace_flush_total",
                &[
                    ("node", "a"),
                    ("pipeline", "1"),
                    ("statement", "0"),
                    ("result", "error"),
                ],
                v,
            )
        };
        let ok = |v: f64| {
            sample(
                "tailrace_flush_total",
                &[
                    ("node", "a"),
                    ("pipeline", "1"),
                    ("statement", "0"),
                    ("result", "success"),
                ],
                v,
            )
        };
        w.observe(&[err(0.0), ok(0.0)], 1_000_000_000_000);
        w.observe(&[err(3.0), ok(1000.0)], 1_000_000_060_000);
        let v = evaluate_rule(
            RuleKind::FlushErrorsWindow,
            &params,
            &[err(3.0), ok(1000.0)],
            &w,
            1_000_000_060_000,
        );
        assert!(v.firing);
        assert_eq!(v.sample["increment"], 3.0);
    }

    /// Param validation: each kind's required fields and bounds.
    #[test]
    fn params_validation() {
        assert!(parse_params(RuleKind::LagSeconds, &json!({})).is_err());
        assert!(parse_params(RuleKind::LagSeconds, &json!({"threshold": 0})).is_err());
        assert!(parse_params(RuleKind::LagSeconds, &json!({"threshold": "30"})).is_ok());
        assert!(parse_params(RuleKind::HeartbeatAge, &json!({"threshold": 90})).is_ok());
        assert!(parse_params(RuleKind::StateFailed, &json!({})).is_ok());
        assert!(parse_params(RuleKind::DlqWindow, &json!({"threshold": 5})).is_err());
        assert!(parse_params(
            RuleKind::DlqWindow,
            &json!({"threshold": 5, "windowSecs": 0})
        )
        .is_err());
        assert!(parse_params(
            RuleKind::DlqWindow,
            &json!({"threshold": 5, "windowSecs": 3601})
        )
        .is_err());
        let p = parse_params(
            RuleKind::LagSeconds,
            &json!({"threshold": 30, "pipeline": "7", "statement": "1", "node": "edge-1"}),
        )
        .unwrap();
        assert_eq!(p.lag_target(), 30.0);
        assert_eq!(p.pipeline.as_deref(), Some("7"));
    }

    /// A filter set on a label the series does not carry excludes it (a
    /// pipeline-scoped heartbeat rule matches nothing, not everything).
    #[test]
    fn filter_on_missing_label_excludes() {
        let params = parse_params(
            RuleKind::HeartbeatAge,
            &json!({"threshold": 90, "pipeline": "1"}),
        )
        .unwrap();
        let samples = vec![sample(
            "tailrace_node_heartbeat_age_seconds",
            &[("node", "a"), ("node_id", "sn-1")],
            500.0,
        )];
        let v = evaluate_rule(
            RuleKind::HeartbeatAge,
            &params,
            &samples,
            &CounterWindows::default(),
            0,
        );
        assert!(!v.firing, "series without the filtered label is excluded");
        assert!(v.sample["value"].is_null());
    }
}
