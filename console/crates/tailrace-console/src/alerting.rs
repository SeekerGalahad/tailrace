//! T-E2 alerting: the console-side rules engine (R9 task card §一, shapes
//! revised by the R9 review fix round).
//!
//! The console already proxies and re-labels every registered node's
//! `/metrics` (A6), so the evaluator lives here — one periodic pass pulls
//! that aggregated snapshot ([`crate::metrics::gather`] →
//! [`crate::metrics::snapshot`]), evaluates the five v1 rule kinds against
//! it and drives the episode state machine in `alert_rule` / `alert_event`:
//!
//! - `lag_seconds` — `tailrace_pipeline_lag_seconds > threshold`
//! - `state_failed` — `tailrace_pipeline_state == 3` (ControlFlags FAILED)
//! - `heartbeat_age` — console-side heartbeat age > threshold (the age is
//!   derived from `node.last_heartbeat`, which the CONSOLE observed — a
//!   node that is down stops beating, so the rule keeps rising and firing;
//!   the node's self-reported series only covers a stuck heartbeat loop
//!   while /metrics still answers)
//! - `dlq_window` — increment of `tailrace_dlq_rows_total` over
//!   `windowSecs` > threshold
//! - `flush_errors_window` — increment of
//!   `tailrace_flush_total{result="error"}` over `windowSecs` > threshold.
//!   The engine currently emits only `result="success"` (A6: the
//!   Dedup/Retry/Error split lands with the A2 deterministic labels), so
//!   the series is absent and the rule stays dormant until that lands —
//!   the evaluation contract is pinned by tests against mock snapshots.
//!
//! Anti-flood / anti-false-alarm decisions (review focus of the task card;
//! the R9 review fix round tightened the false-alarm half):
//!
//! - **Event rows land on transitions only**: a series' inactive→firing
//!   tick inserts one episode row, further firing ticks touch it, its
//!   firing→inactive flip turns it `resolved`. A resolved notice is
//!   therefore always reachable for every firing — condition cleared,
//!   series gone while its node still answers, rule disabled mid-flight
//!   (`reason:"disabled"`), or the rule deleted (`reason:"deleted"`).
//! - **Episodes are PER SERIES**: a rule matching three pipelines keeps
//!   three independent episodes (`alert_event.series_key`); a second
//!   pipeline going bad while the first is already firing still notifies,
//!   and a resolved notice always carries the same series as its firing.
//!   `alert_rule.state` is the aggregate (`firing` while any episode is
//!   open).
//! - **Three-state verdicts**: Firing / Clear / **Unknown**. Unknown HOLDS
//!   the current state — it never resolves an open episode and never opens
//!   one. Unknown covers (a) a window rule with no usable base sample
//!   (console restart, or a sample gap: the base must lie within
//!   [`BASE_SLACK_SECS`] before the cutoff — an increment spanning a
//!   larger gap is not a window increment and must not fire), and (b) a
//!   series whose node was unreachable this pass ([`Snapshot`]):
//!   a node down is not "condition cleared", so its open episodes hold
//!   until the node answers again or is deleted.
//! - **Strict comparison** (`value > threshold`): a value exactly at the
//!   threshold does not fire (boundary pinned by tests). Window thresholds
//!   accept `>= 0` — with an integer counter, `threshold: 0` means "any
//!   row in the window fires" (`inc > 0`).
//! - **Counter reset** (latest < base) credits the post-reset value — a
//!   true lower bound of what happened inside the window.
//! - **Series absent while its node answers** resolves (a pipeline that
//!   stopped exposing lag is not lagging anymore).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use console_store::{AlertEvent, AlertRule};
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

/// How far before the window cutoff the base sample may lie (R9 review
/// fix). Evaluation normally samples every few seconds; one missed pass or
/// a short stall still yields a base close enough to the cutoff that the
/// increment is honest. A base older than that (a real gap — node
/// unreachable, evaluator stalled) makes the increment span the whole gap,
/// so the series is not evaluated this tick (Unknown) instead of firing on
/// a window that no longer means `windowSecs`.
pub const BASE_SLACK_SECS: u64 = 60;

/// Default evaluator period (task card: 15s 默认). Override with
/// `TAILRACE_ALERT_INTERVAL_SECS` (min 1).
pub const DEFAULT_INTERVAL_SECS: u64 = 15;

// ---------------- snapshot ----------------

/// Evaluator input for one pass (R9 review fix): the reachable nodes'
/// samples plus the nodes whose `/metrics` could not be fetched this round.
/// An unreachable node's series are absent for a reason that is NOT "the
/// condition cleared", so open episodes on them are held, not resolved;
/// `heartbeat_age` (console-derived) is what fires for a node that is down.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub samples: Vec<Sample>,
    pub unreachable_nodes: Vec<String>,
}

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

/// Filters that can never match are rejected, not accepted silently (R9
/// review fix): `state_failed` series carry only `pipeline`/`node` labels,
/// heartbeat series only `node`/`node_id` — a filter on a label the kind's
/// series never carry would match nothing forever while the user believes
/// they are protected.
fn validate_filters(kind: RuleKind, params: &RuleParams) -> Result<(), String> {
    match kind {
        RuleKind::StateFailed if params.statement.is_some() => Err(
            "state_failed series carry no statement label — a statement \
                 filter can never match"
                .into(),
        ),
        RuleKind::HeartbeatAge if params.pipeline.is_some() || params.statement.is_some() => {
            Err("heartbeat_age series carry no pipeline/statement labels — \
                 those filters can never match"
                .into())
        }
        _ => Ok(()),
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
    let parsed = match kind {
        RuleKind::StateFailed => base,
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
            RuleParams { threshold, ..base }
        }
        RuleKind::DlqWindow | RuleKind::FlushErrorsWindow => {
            let Some(threshold) = number(params, "threshold") else {
                return Err(format!(
                    "threshold (row count, number >= 0) is required for {}",
                    kind.as_str()
                ));
            };
            // >= 0 (R9 review fix): counters are integers, so threshold 0 +
            // strict > means "any single row in the window fires" — the
            // fire-once semantics users expect for flush errors.
            if !threshold.is_finite() || threshold < 0.0 {
                return Err("threshold must be >= 0".into());
            }
            let Some(window) = params.get("windowSecs").and_then(Value::as_u64) else {
                return Err("windowSecs (seconds, 1..=3600) is required".into());
            };
            if !(1..=MAX_WINDOW_SECS).contains(&window) {
                return Err(format!("windowSecs must be within 1..={MAX_WINDOW_SECS}"));
            }
            RuleParams {
                threshold,
                window_secs: window,
                ..base
            }
        }
    };
    validate_filters(kind, &parsed)?;
    Ok(parsed)
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
/// the series does not carry excludes the series.
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

// ---------------- series identity ----------------

fn escape_label_value(v: &str) -> String {
    // same escapes the exposition parser unreads (\, ", \n) — the key stays
    // parseable by `parse_label_set` and round-trips `series_key_label`
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

fn series_key(s: &Sample) -> String {
    // BTreeMap iteration is sorted — same labels, same key regardless of
    // exposition order.
    let labels: Vec<String> = s
        .labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect();
    format!("{}{{{}}}", s.metric, labels.join(","))
}

/// Read one label back out of a rendered series key (`node` of
/// `metric{node="a",pipeline="1"}`) — used to attribute an open episode to
/// its node when deciding whether the series is absent because the node is
/// unreachable.
fn series_key_label(key: &str, name: &str) -> Option<String> {
    let body = key.split_once('{')?.1.strip_suffix('}')?;
    parse_label_set(body)?.remove(name)
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
    /// latest − newest sample at or before `now − window`. `None` when the
    /// newest such sample is missing (history shorter than the window) OR
    /// older than [`BASE_SLACK_SECS`] before the cutoff (a sample gap — the
    /// difference would span the whole gap, not the window; not evaluated);
    /// a reset (latest < base) credits the post-reset value, a true lower
    /// bound for the window.
    fn increment(&self, key: &str, window_secs: u64, now_ms: u64) -> Option<f64> {
        let q = self.series.get(key)?;
        let latest = q.back()?;
        let cutoff = now_ms.saturating_sub(window_secs * 1000);
        let oldest_allowed = cutoff.saturating_sub(BASE_SLACK_SECS * 1000);
        let base = q
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= cutoff && *ts >= oldest_allowed)
            .copied()?;
        if latest.1 >= base.1 {
            Some(latest.1 - base.1)
        } else {
            Some(latest.1)
        }
    }
}

// ---------------- evaluation ----------------

/// One series' verdict: Firing / Clear / Unknown (Unknown = "not evaluated
/// this tick — hold whatever state the episode is in").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Firing,
    Clear,
    Unknown,
}

/// One matching series' evaluation result.
#[derive(Debug, Clone)]
pub struct SeriesVerdict {
    pub series_key: String,
    pub outcome: Outcome,
    /// Observed values for the episode row / notices.
    pub sample: Value,
}

/// Evaluate one rule against one snapshot, PER SERIES. `windows` must
/// already hold this tick's counters ([`CounterWindows::observe`]). Series
/// that do not match the rule's filters are not returned; the driver
/// (`run_pass_snapshot`) reconciles the returned keys against open
/// episodes (absent series resolve when their node answers, hold when it
/// does not).
pub fn evaluate_rule(
    kind: RuleKind,
    params: &RuleParams,
    samples: &[Sample],
    windows: &CounterWindows,
    now_ms: u64,
) -> Vec<SeriesVerdict> {
    match kind {
        RuleKind::LagSeconds => {
            threshold_verdicts(samples, params, "tailrace_pipeline_lag_seconds")
        }
        RuleKind::HeartbeatAge => {
            threshold_verdicts(samples, params, "tailrace_node_heartbeat_age_seconds")
        }
        RuleKind::StateFailed => samples
            .iter()
            .filter(|s| s.metric == "tailrace_pipeline_state" && series_matches(s, params))
            .map(|s| {
                let firing = s.value == 3.0;
                SeriesVerdict {
                    series_key: series_key(s),
                    outcome: if firing {
                        Outcome::Firing
                    } else {
                        Outcome::Clear
                    },
                    sample: json!({
                        "metric": s.metric, "value": s.value,
                        "labels": s.labels,
                        "expected": "state != 3 (FAILED)",
                    }),
                }
            })
            .collect(),
        RuleKind::DlqWindow | RuleKind::FlushErrorsWindow => {
            let (metric, result_filter) = match kind {
                RuleKind::DlqWindow => ("tailrace_dlq_rows_total", None),
                _ => ("tailrace_flush_total", Some("error")),
            };
            samples
                .iter()
                .filter(|s| {
                    s.metric == metric
                        && result_filter
                            .is_none_or(|r| s.labels.get("result").map(String::as_str) == Some(r))
                        && series_matches(s, params)
                })
                .map(|s| {
                    let key = series_key(s);
                    let (outcome, inc) = match windows.increment(&key, params.window_secs, now_ms) {
                        // no honest base inside the window — hold, never fire
                        None => (Outcome::Unknown, None),
                        Some(inc) => (
                            if inc > params.threshold {
                                Outcome::Firing
                            } else {
                                Outcome::Clear
                            },
                            Some(inc),
                        ),
                    };
                    let mut sample = json!({
                        "metric": metric, "threshold": params.threshold,
                        "windowSecs": params.window_secs, "series": key,
                    });
                    match inc {
                        Some(inc) => sample["increment"] = json!(inc),
                        None => {
                            sample["increment"] = Value::Null;
                            sample["reason"] = json!("no_baseline");
                        }
                    }
                    SeriesVerdict {
                        series_key: key,
                        outcome,
                        sample,
                    }
                })
                .collect()
        }
    }
}

/// The two threshold-style kinds, per series: strict `> threshold`.
fn threshold_verdicts(samples: &[Sample], params: &RuleParams, metric: &str) -> Vec<SeriesVerdict> {
    samples
        .iter()
        .filter(|s| s.metric == metric && series_matches(s, params))
        .map(|s| {
            let firing = s.value > params.threshold;
            SeriesVerdict {
                series_key: series_key(s),
                outcome: if firing {
                    Outcome::Firing
                } else {
                    Outcome::Clear
                },
                sample: json!({
                    "metric": metric, "value": s.value,
                    "threshold": params.threshold, "labels": s.labels,
                }),
            }
        })
        .collect()
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
    /// The series the transition belongs to (`metric{label="value",…}`) —
    /// a rule matching several pipelines keeps one episode per series.
    pub series_key: String,
    pub params: Value,
    pub sample: Value,
    pub ts_ms: u64,
}

impl AlertNotice {
    fn new(
        rule: &AlertRule,
        event: &str,
        event_id: i64,
        series_key: &str,
        sample: Value,
        now_ms: u64,
    ) -> Self {
        AlertNotice {
            event: event.to_string(),
            event_id,
            rule_id: rule.id,
            rule_name: rule.name.clone(),
            rule_type: rule.rule_type.clone(),
            series_key: series_key.to_string(),
            params: serde_json::from_str(&rule.params_json).unwrap_or(Value::Null),
            sample,
            ts_ms: now_ms,
        }
    }
}

/// Evaluator-owned per-process state (counter history + the in-flight
/// webhook delivery). One instance lives in the spawned loop; tests build
/// their own and drive passes directly.
#[derive(Default)]
pub struct Evaluator {
    windows: CounterWindows,
    /// The previous pass' webhook delivery task. Deliveries are spawned so
    /// a black-holed webhook can never stall evaluation, persistence or
    /// SSE; each new delivery awaits the previous one first, so one
    /// receiver still sees notices in transition order.
    delivery: Option<tokio::task::JoinHandle<()>>,
}

impl Evaluator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience wrapper over [`Self::run_pass_snapshot`] with every node
    /// considered reachable (tests driving mock samples directly).
    pub async fn run_pass(
        &mut self,
        state: &AppState,
        samples: &[Sample],
        now_ms: u64,
    ) -> anyhow::Result<Vec<AlertNotice>> {
        let snap = Snapshot {
            samples: samples.to_vec(),
            unreachable_nodes: Vec::new(),
        };
        self.run_pass_snapshot(state, &snap, now_ms).await
    }

    /// One evaluation pass: record counters, evaluate every rule per
    /// series, persist the state transitions, broadcast + webhook each
    /// notice. Returns the notices emitted (contract-test surface).
    pub async fn run_pass_snapshot(
        &mut self,
        state: &AppState,
        snap: &Snapshot,
        now_ms: u64,
    ) -> anyhow::Result<Vec<AlertNotice>> {
        self.windows.observe(&snap.samples, now_ms);
        let rules = state.store().list_alert_rules()?;
        let mut notices = Vec::new();
        let mut hooks: Vec<(String, AlertNotice)> = Vec::new();
        for rule in rules {
            let open = state.store().open_alert_episodes(rule.id)?;
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
            let mut rule_notices = Vec::new();
            if !rule.enabled {
                // A disabled rule cannot keep firing: resolve every open
                // episode with the reason so the notice always arrives
                // (resolved 必达).
                for ep in open {
                    let sample = json!({"reason": "disabled", "series": ep.series_key});
                    state
                        .store()
                        .resolve_alert_episode(ep.id, &sample.to_string())?;
                    rule_notices.push(AlertNotice::new(
                        &rule,
                        "resolved",
                        ep.id,
                        &ep.series_key,
                        sample,
                        now_ms,
                    ));
                }
            } else {
                let verdicts = evaluate_rule(kind, &params, &snap.samples, &self.windows, now_ms);
                let mut covered: HashSet<String> = HashSet::new();
                // pre-migration-05 rows (series_key "") — the first series
                // that fires under this rule adopts the oldest one instead
                // of opening a duplicate episode
                let mut legacy: Vec<&AlertEvent> =
                    open.iter().filter(|e| e.series_key.is_empty()).collect();
                let any_unknown = verdicts.iter().any(|v| v.outcome == Outcome::Unknown);
                for v in &verdicts {
                    covered.insert(v.series_key.clone());
                    let existing = open.iter().find(|e| e.series_key == v.series_key);
                    match (existing, v.outcome) {
                        (None, Outcome::Firing) => {
                            if let Some(l) = legacy.pop() {
                                // adopt silently — the episode was already
                                // firing under this rule, the receiver has it
                                state.store().adopt_alert_episode(l.id, &v.series_key)?;
                                state
                                    .store()
                                    .touch_alert_episode(l.id, &v.sample.to_string())?;
                            } else {
                                let event_id = state.store().begin_alert_episode(
                                    rule.id,
                                    &v.series_key,
                                    &v.sample.to_string(),
                                )?;
                                rule_notices.push(AlertNotice::new(
                                    &rule,
                                    "firing",
                                    event_id,
                                    &v.series_key,
                                    v.sample.clone(),
                                    now_ms,
                                ));
                            }
                        }
                        (Some(ep), Outcome::Firing) => {
                            state
                                .store()
                                .touch_alert_episode(ep.id, &v.sample.to_string())?;
                        }
                        (Some(ep), Outcome::Clear) => {
                            state
                                .store()
                                .resolve_alert_episode(ep.id, &v.sample.to_string())?;
                            rule_notices.push(AlertNotice::new(
                                &rule,
                                "resolved",
                                ep.id,
                                &v.series_key,
                                v.sample.clone(),
                                now_ms,
                            ));
                        }
                        // not evaluated this tick — the episode keeps its state
                        (Some(_), Outcome::Unknown) => {}
                        (None, Outcome::Clear | Outcome::Unknown) => {}
                    }
                }
                // open episodes whose series did not appear at all this pass
                let mut any_held = any_unknown || !snap.unreachable_nodes.is_empty();
                for ep in open.iter().filter(|e| !covered.contains(&e.series_key)) {
                    let node = series_key_label(&ep.series_key, "node");
                    let unreachable = node
                        .as_deref()
                        .is_some_and(|n| snap.unreachable_nodes.iter().any(|u| u == n));
                    if unreachable {
                        any_held = true;
                        continue; // node down ≠ condition cleared — hold
                    }
                    if ep.series_key.is_empty() && any_held {
                        // legacy row nobody fired under this pass: close it
                        // only when nothing else is being held
                        continue;
                    }
                    let sample = json!({
                        "reason": "series_absent", "series": ep.series_key,
                        "labels": {"node": node},
                    });
                    state
                        .store()
                        .resolve_alert_episode(ep.id, &sample.to_string())?;
                    rule_notices.push(AlertNotice::new(
                        &rule,
                        "resolved",
                        ep.id,
                        &ep.series_key,
                        sample,
                        now_ms,
                    ));
                }
            }
            if let Some(url) = rule.webhook_url.as_deref() {
                hooks.extend(rule_notices.iter().map(|n| (url.to_string(), n.clone())));
            }
            notices.extend(rule_notices);
        }
        emit(state, &mut self.delivery, &notices, hooks);
        Ok(notices)
    }
}

/// Broadcast the notices on the SSE hub and hand the webhook deliveries to
/// a background task that awaits the previous one (transition order for a
/// receiver, no evaluation-loop stall for a black-holed URL).
fn emit(
    state: &AppState,
    delivery: &mut Option<tokio::task::JoinHandle<()>>,
    notices: &[AlertNotice],
    hooks: Vec<(String, AlertNotice)>,
) {
    for n in notices {
        // SSE hub: send fails only when nobody subscribed (fine).
        let _ = state.alerts_tx().send(n.clone());
    }
    if hooks.is_empty() {
        return;
    }
    let prev = delivery.take();
    let client = state.node_client().clone();
    *delivery = Some(tokio::spawn(async move {
        if let Some(p) = prev {
            let _ = p.await;
        }
        for (url, n) in hooks {
            deliver_webhook(&client, &url, &n).await;
        }
    }));
}

/// Resolve every open episode of a rule with a reason (`deleted`), persist,
/// broadcast + webhook the notices. The DELETE endpoint calls this BEFORE
/// dropping the rule (resolved 必达: the receiver must learn the alert is
/// over even though the rule — and with it the episode rows — then goes).
pub fn resolve_rule_episodes(
    state: &AppState,
    rule: &AlertRule,
    reason: &str,
    now_ms: u64,
) -> anyhow::Result<Vec<AlertNotice>> {
    let mut notices = Vec::new();
    for ep in state.store().open_alert_episodes(rule.id)? {
        let sample = json!({"reason": reason, "series": ep.series_key});
        state
            .store()
            .resolve_alert_episode(ep.id, &sample.to_string())?;
        notices.push(AlertNotice::new(
            rule,
            "resolved",
            ep.id,
            &ep.series_key,
            sample,
            now_ms,
        ));
    }
    if let Some(url) = rule.webhook_url.as_deref() {
        let client = state.node_client().clone();
        let hooks: Vec<(String, AlertNotice)> = notices
            .iter()
            .map(|n| (url.to_string(), n.clone()))
            .collect();
        tokio::spawn(async move {
            for (url, n) in hooks {
                deliver_webhook(&client, &url, &n).await;
            }
        });
    }
    for n in &notices {
        let _ = state.alerts_tx().send(n.clone());
    }
    Ok(notices)
}

/// Type + validated params of a stored rule.
fn decode_rule(rule: &AlertRule) -> Option<(RuleKind, RuleParams)> {
    let kind = RuleKind::parse(&rule.rule_type)?;
    let params: Value = serde_json::from_str(&rule.params_json).ok()?;
    parse_params(kind, &params).ok().map(|p| (kind, p))
}

/// Webhook POST (v1: fire-and-log, no retry). Failures never fail anything.
async fn deliver_webhook(client: &reqwest::Client, url: &str, notice: &AlertNotice) {
    let resp = client
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
            let snap = crate::metrics::snapshot(&gathered, &state);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            if let Err(e) = evaluator.run_pass_snapshot(&state, &snap, now_ms).await {
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

    /// Series keys: sorted labels, values escaped, `series_key_label` reads
    /// a label (node) back out of a rendered key.
    #[test]
    fn series_key_escapes_and_round_trips_labels() {
        let s = sample(
            "tailrace_pipeline_lag_seconds",
            &[("node", "edge \"1\""), ("pipeline", "7")],
            1.0,
        );
        let key = series_key(&s);
        assert_eq!(
            key,
            "tailrace_pipeline_lag_seconds{node=\"edge \\\"1\\\"\",pipeline=\"7\"}"
        );
        assert_eq!(
            series_key_label(&key, "node").as_deref(),
            Some("edge \"1\"")
        );
        assert_eq!(series_key_label(&key, "pipeline").as_deref(), Some("7"));
        assert_eq!(series_key_label(&key, "statement"), None);
    }

    /// Threshold boundary (review focus): strictly greater — a value AT the
    /// threshold is Clear; verdicts are per series; filters scope.
    #[test]
    fn lag_threshold_is_strict_per_series() {
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
        let vs = evaluate_rule(
            RuleKind::LagSeconds,
            &params,
            &samples,
            &CounterWindows::default(),
            0,
        );
        assert_eq!(vs.len(), 2, "pipeline 9 filtered out: {vs:?}");
        assert_eq!(vs[0].outcome, Outcome::Clear, "value == threshold is Clear");
        assert_eq!(vs[0].sample["value"], 30.0);
        assert_eq!(vs[1].outcome, Outcome::Firing, "strict > threshold");
        assert_eq!(vs[1].sample["value"], 31.5);
        assert!(vs[1].series_key.contains("statement=\"1\""));
        // no matching series at all: no verdicts (the driver resolves or
        // holds open episodes — absence is not a firing)
        let vs = evaluate_rule(
            RuleKind::LagSeconds,
            &params,
            &[],
            &CounterWindows::default(),
            0,
        );
        assert!(vs.is_empty());
    }

    /// state_failed is per series: only the FAILED series fires.
    #[test]
    fn state_failed_fires_on_failed_state_per_series() {
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
        let vs = evaluate_rule(
            RuleKind::StateFailed,
            &params,
            &samples,
            &CounterWindows::default(),
            0,
        );
        assert_eq!(vs.len(), 3);
        let firing: Vec<&SeriesVerdict> =
            vs.iter().filter(|v| v.outcome == Outcome::Firing).collect();
        assert_eq!(firing.len(), 1);
        assert!(firing[0].series_key.contains("pipeline=\"2\""));
        assert_eq!(firing[0].sample["value"], 3.0);
    }

    /// Window alignment (review focus): the base is the newest sample at or
    /// before `now − window` AND not older than `now − window −
    /// BASE_SLACK_SECS`; history younger than the window and a stale base
    /// both yield Unknown (held, never fired); the base slides forward with
    /// the window; a counter reset credits the post-reset value; samples
    /// older than the retention horizon are pruned (and cannot serve as a
    /// base). Timestamps are realistic epoch-ms (a 0 origin would let u64
    /// saturation fabricate a base at the cutoff).
    #[test]
    fn dlq_window_alignment_gaps_and_resets() {
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
        let outcome = |w: &CounterWindows, now: u64| {
            evaluate_rule(RuleKind::DlqWindow, &params, &[s(0.0)], w, now)
                .into_iter()
                .next()
                .map(|v| (v.outcome, v.sample.clone()))
        };

        // t=0 → 100, t=30s → 105: at t=45s the cutoff (t=−15s) has no
        // sample at or before it → Unknown (never fires).
        w.observe(&[s(100.0)], T0);
        w.observe(&[s(105.0)], T0 + 30_000);
        let (o, sample) = outcome(&w, T0 + 45_000).unwrap();
        assert_eq!(o, Outcome::Unknown, "history younger than the window");
        assert!(sample["increment"].is_null());
        assert_eq!(sample["reason"], "no_baseline");

        // t=60s → 112: base = t=0 (<= cutoff t=0), increment 12 > 10 fires.
        w.observe(&[s(112.0)], T0 + 60_000);
        let (o, sample) = outcome(&w, T0 + 60_000).unwrap();
        assert_eq!(o, Outcome::Firing, "base at t=0, increment 12");
        assert_eq!(sample["increment"], 12.0);

        // t=100s → 120: cutoff t=40s, base = newest <= 40s = t=30s (105),
        // increment 15 — the t=0 sample no longer decides.
        w.observe(&[s(120.0)], T0 + 100_000);
        let (o, sample) = outcome(&w, T0 + 100_000).unwrap();
        assert_eq!(o, Outcome::Firing);
        assert_eq!(sample["increment"], 15.0, "base slides to the t=30s sample");

        // t=200s → 125: cutoff t=140s, base = t=100s (120), increment 5 →
        // Clear (the big early jump has left the window).
        w.observe(&[s(125.0)], T0 + 200_000);
        let (o, sample) = outcome(&w, T0 + 200_000).unwrap();
        assert_eq!(o, Outcome::Clear, "increment 5 within the current window");
        assert_eq!(sample["increment"], 5.0);

        // sample gap (R9 review fix): after a 10-minute hole the newest
        // base candidate is 540s before the cutoff — far outside the slack.
        // The increment would span the whole gap; the tick is Unknown until
        // a real base re-enters the window.
        let mut gap = CounterWindows::default();
        gap.observe(&[s(100.0)], T0);
        gap.observe(&[s(150.0)], T0 + 600_000);
        let (o, sample) = outcome(&gap, T0 + 600_000).unwrap();
        assert_eq!(
            o,
            Outcome::Unknown,
            "a base 540s before the cutoff is not a window base"
        );
        assert_eq!(sample["reason"], "no_baseline");
        // recovery: once a fresh sample sits at/before the cutoff within
        // the slack, evaluation resumes with an honest increment.
        gap.observe(&[s(152.0)], T0 + 660_000);
        let (o, sample) = outcome(&gap, T0 + 660_000).unwrap();
        assert_eq!(o, Outcome::Clear, "increment 2 over the 60s after recovery");
        assert_eq!(sample["increment"], 2.0);

        // counter reset: latest (30) < base (120) → increment credited as
        // the post-reset value (30 rows certainly landed inside the window).
        let mut w2 = CounterWindows::default();
        w2.observe(&[s(120.0)], T0);
        w2.observe(&[s(5.0)], T0 + 30_000);
        w2.observe(&[s(30.0)], T0 + 70_000);
        let (o, sample) = outcome(&w2, T0 + 70_000).unwrap();
        assert_eq!(o, Outcome::Firing);
        assert_eq!(
            sample["increment"], 30.0,
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
        let v = evaluate_rule(RuleKind::DlqWindow, &long, &[s(50.0)], &w3, T0 + 7_200_000)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            v.outcome,
            Outcome::Unknown,
            "pruned history cannot serve as a base"
        );
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
        let vs = evaluate_rule(
            RuleKind::FlushErrorsWindow,
            &params,
            &[err(3.0), ok(1000.0)],
            &w,
            1_000_000_060_000,
        );
        assert_eq!(vs.len(), 1, "only the error series: {vs:?}");
        assert_eq!(vs[0].outcome, Outcome::Firing);
        assert_eq!(vs[0].sample["increment"], 3.0);
    }

    /// Param validation: each kind's required fields and bounds, window
    /// thresholds down to 0 (any single row fires), and impossible filters
    /// rejected instead of silently accepted (R9 review fix).
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
        // threshold 0 is legal for the window kinds: counters are integers,
        // so `inc > 0` fires on any single row in the window
        assert!(parse_params(
            RuleKind::FlushErrorsWindow,
            &json!({"threshold": 0, "windowSecs": 600})
        )
        .is_ok());
        assert!(parse_params(
            RuleKind::DlqWindow,
            &json!({"threshold": -1, "windowSecs": 600})
        )
        .is_err());
        // impossible filters are rejected, not accepted
        let e = parse_params(
            RuleKind::HeartbeatAge,
            &json!({"threshold": 90, "pipeline": "1"}),
        )
        .unwrap_err();
        assert!(e.contains("can never match"), "{e}");
        assert!(parse_params(RuleKind::StateFailed, &json!({"statement": "0"}),).is_err());
        // node filter is fine everywhere (every proxied series carries it)
        assert!(parse_params(
            RuleKind::HeartbeatAge,
            &json!({"threshold": 90, "node": "edge-1"}),
        )
        .is_ok());
        let p = parse_params(
            RuleKind::LagSeconds,
            &json!({"threshold": 30, "pipeline": "7", "statement": "1", "node": "edge-1"}),
        )
        .unwrap();
        assert_eq!(p.lag_target(), 30.0);
        assert_eq!(p.pipeline.as_deref(), Some("7"));
    }

    /// A filter set on a label the series does not carry excludes it (a
    /// pipeline-scoped heartbeat rule matches nothing, not everything) —
    /// the direct-`RuleParams` form of what parse_params now rejects.
    #[test]
    fn filter_on_missing_label_excludes() {
        let params = RuleParams {
            threshold: 90.0,
            window_secs: 0,
            pipeline: Some("1".into()),
            statement: None,
            node: None,
        };
        let samples = vec![sample(
            "tailrace_node_heartbeat_age_seconds",
            &[("node", "a"), ("node_id", "sn-1")],
            500.0,
        )];
        let vs = evaluate_rule(
            RuleKind::HeartbeatAge,
            &params,
            &samples,
            &CounterWindows::default(),
            0,
        );
        assert!(
            vs.is_empty(),
            "series without the filtered label is excluded"
        );
    }
}
