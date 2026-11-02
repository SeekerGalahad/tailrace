//! A6/E1 (§7.4): console-side `/metrics` — Prometheus text format, zero
//! dependencies, hand-written like the engine's `tailrace_api::metrics`
//! (deliberately NOT shared: the console must not grow a dependency on the
//! engine node crates — `tailrace-api` pulls `tailrace-runtime` into the
//! management plane — and the console surface here is one gauge family plus
//! pass-through text, so the overlap is a few dozen lines).
//!
//! Body = console-local series + per-node blocks proxied from each
//! registered node's open `/metrics`:
//!
//! - `tailrace_console_db_write_seconds{op=…}` (gauge) — SQLite write-path
//!   latency from [`console_store::Store::db_write_stats`] (the `with_tx`
//!   chokepoint — the T8 batched transactional path: heartbeats, event
//!   batches, publishes; single-statement autocommit writes are not timed).
//!   The §7.4 histogram is simplified to a gauge for v1 (`tx_last` = most
//!   recent transaction, `tx_avg` = mean).
//! - Node blocks are re-labelled with `node="<name>"` (two nodes running
//!   pipeline 1 must not collide as identical series) and repeated
//!   `# TYPE`/`# HELP` lines are dropped — one family, one declaration,
//!   which is what the Prometheus text parser accepts.
//! - Unreachable nodes degrade to a `#` comment line (a scrape must not
//!   fail because one node is down).
//! - `tailrace_slo_lag_target_seconds` (gauge, T-E2 SLO v1) — the lag
//!   threshold of every enabled `lag_seconds` alert rule, emitted with the
//!   same labels as the lag series it watches ([`slo_target_lines`]) so
//!   target and current value sit side by side under one label set.
//!
//! [`gather`] is the shared snapshot step: the alert evaluator
//! ([`crate::alerting`]) runs on exactly what the scrape would serve.

use std::collections::HashSet;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use console_store::Store;

use crate::state::AppState;

/// Prometheus text-format content type (versioned form scrapers expect).
const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
/// Per-node proxy fetch timeout — the whole scrape must answer well inside
/// a typical 10s Prometheus scrape_timeout even with several nodes.
const NODE_SCRAPE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// One proxy gathering round: the console-local series text plus each
/// registered node's proxied `/metrics` body (T-E2: shared by the HTTP
/// scrape and the alert evaluator so both see the same snapshot shape).
pub struct Gathered {
    pub console_text: String,
    pub blocks: Vec<(String, Result<String, String>)>,
}

/// Fetch the console-local series + every node block (the scrape body's two
/// halves, before assembly).
pub async fn gather(state: &AppState) -> Gathered {
    let console_text = render_console_metrics(state.store());
    let nodes = state.store().list_nodes().unwrap_or_default();
    let client = reqwest::Client::builder()
        .timeout(NODE_SCRAPE_TIMEOUT)
        .build()
        .unwrap_or_else(|_| state.node_client().clone());
    let mut blocks: Vec<(String, Result<String, String>)> = Vec::new();
    for n in nodes {
        let result = match n.api_url.as_deref().map(|u| u.trim_end_matches('/')) {
            Some(base) if !base.is_empty() => {
                match client.get(format!("{base}/metrics")).send().await {
                    Ok(resp) if resp.status().is_success() => resp
                        .text()
                        .await
                        .map_err(|e| format!("body read failed: {e}")),
                    Ok(resp) => Err(format!("HTTP {}", resp.status().as_u16())),
                    Err(e) => Err(format!("unreachable: {e}")),
                }
            }
            _ => Err("no api_url yet (awaiting first heartbeat)".to_string()),
        };
        blocks.push((n.name, result));
    }
    Gathered {
        console_text,
        blocks,
    }
}

/// All reachable-node samples of one gathering round, with the `node` label
/// injected — the raw proxy input.
pub fn node_samples(gathered: &Gathered) -> Vec<crate::alerting::Sample> {
    gathered
        .blocks
        .iter()
        .filter_map(|(name, result)| {
            result
                .as_ref()
                .ok()
                .map(|text| crate::alerting::parse_samples(text, Some(name)))
        })
        .flatten()
        .collect()
}

/// The evaluator's input for one gathering round (R9 review fix).
///
/// - `unreachable_nodes` — every registered node whose `/metrics` could not
///   be fetched (down, partitioned, or one 3s scrape timeout). Their series
///   are absent for a reason that is not "the condition cleared", so the
///   evaluator holds open episodes on them instead of resolving.
/// - `heartbeat_age` switches to the CONSOLE-observed clock: the node's
///   self-reported `tailrace_node_heartbeat_age_seconds` is dropped and one
///   sample per registered node is synthesized from `node.last_heartbeat`
///   (the moment the console last accepted a heartbeat). A node that is
///   down cannot report its own silence; the console's own receive time
///   keeps rising, so a `heartbeat_age` rule fires exactly when the node
///   goes quiet — the node-side series only covered a stuck heartbeat loop
///   while /metrics still answered. The scrape output (`assemble`) still
///   proxies the node's self-report untouched; only the evaluator's view is
///   replaced. Nodes that never heartbeated have no age (no false alarm on
///   fresh registrations).
pub fn snapshot(gathered: &Gathered, state: &AppState) -> crate::alerting::Snapshot {
    let mut samples = node_samples(gathered);
    samples.retain(|s| s.metric != "tailrace_node_heartbeat_age_seconds");
    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    for n in state.store().list_nodes().unwrap_or_default() {
        let Some(last) = n
            .last_heartbeat
            .as_deref()
            .and_then(crate::api::nodes_admin::sqlite_ts_to_epoch)
        else {
            continue;
        };
        samples.push(crate::alerting::Sample {
            metric: "tailrace_node_heartbeat_age_seconds".to_string(),
            labels: [("node", n.name.as_str()), ("node_id", n.node_key.as_str())]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value: (now_epoch - last).max(0) as f64,
        });
    }
    let unreachable_nodes = gathered
        .blocks
        .iter()
        .filter_map(|(name, result)| result.as_ref().err().map(|_| name.clone()))
        .collect();
    crate::alerting::Snapshot {
        samples,
        unreachable_nodes,
    }
}

/// `GET /metrics` — open (no session; exporter convention, like /healthz).
pub async fn scrape(State(state): State<AppState>) -> Response {
    let gathered = gather(&state).await;
    // T-E2 SLO v1: enabled lag rules expose their threshold next to the lag
    // series they watch (same labels) — appended to the console-local
    // section, before the node blocks.
    let slo = slo_target_lines(
        &gathered,
        &state.store().list_alert_rules().unwrap_or_default(),
    );
    let mut console_text = gathered.console_text.clone();
    console_text.push_str(&slo);
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static(CONTENT_TYPE),
        )],
        assemble(console_text, &gathered.blocks),
    )
        .into_response()
}

/// Console-local series (one gauge family; seconds, per the metric name).
pub fn render_console_metrics(store: &Store) -> String {
    let s = store.db_write_stats();
    format!(
        "# TYPE tailrace_console_db_write_seconds gauge\n\
         tailrace_console_db_write_seconds{{op=\"tx_last\"}} {}\n\
         tailrace_console_db_write_seconds{{op=\"tx_avg\"}} {}\n",
        s.us_last as f64 / 1_000_000.0,
        s.avg_us() / 1_000_000.0,
    )
}

/// T-E2 SLO v1: every enabled `lag_seconds` rule exposes its threshold as
/// `tailrace_slo_lag_target_seconds` carrying the SAME label set as the
/// `tailrace_pipeline_lag_seconds` series it watches (node/pipeline/
/// statement, rendered in the proxy's label order) — one target line per
/// rule-matched lag series, so a panel overlays target vs current on labels
/// alone. Several rules matching one series collapse to the tightest
/// (lowest) target (the text format forbids duplicate series); lag series
/// no rule watches get no target line (absent ≠ 0). Empty string when
/// nothing applies — no stray TYPE line.
pub fn slo_target_lines(gathered: &Gathered, rules: &[console_store::AlertRule]) -> String {
    use crate::alerting::{parse_params, series_matches, RuleKind};
    let watched: Vec<crate::alerting::RuleParams> = rules
        .iter()
        .filter(|r| r.enabled && RuleKind::parse(&r.rule_type) == Some(RuleKind::LagSeconds))
        .filter_map(|r| {
            serde_json::from_str::<serde_json::Value>(&r.params_json)
                .ok()
                .and_then(|p| parse_params(RuleKind::LagSeconds, &p).ok())
        })
        .collect();
    if watched.is_empty() {
        return String::new();
    }
    // rendered label set → tightest target
    let mut targets: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    for s in node_samples(gathered)
        .iter()
        .filter(|s| s.metric == "tailrace_pipeline_lag_seconds")
    {
        for p in watched.iter().filter(|p| series_matches(s, p)) {
            let labels: Vec<String> = s
                .labels
                .iter()
                .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
                .collect();
            let key = format!("{{{}}}", labels.join(","));
            let t = p.lag_target();
            targets
                .entry(key)
                .and_modify(|cur| {
                    if t < *cur {
                        *cur = t;
                    }
                })
                .or_insert(t);
        }
    }
    if targets.is_empty() {
        return String::new();
    }
    let mut out = String::from("# TYPE tailrace_slo_lag_target_seconds gauge\n");
    for (labels, target) in targets {
        out.push_str(&format!(
            "tailrace_slo_lag_target_seconds{labels} {target}\n"
        ));
    }
    out
}

/// Concatenate the console series with the per-node blocks: inject
/// `node="<name>"` into every proxied sample line, emit each `# TYPE`/`# HELP`
/// declaration only once, and turn failed fetches into comment lines.
pub fn assemble(console_text: String, blocks: &[(String, Result<String, String>)]) -> String {
    let mut out = String::from(&console_text);
    let mut declared: HashSet<String> = HashSet::new();
    console_text
        .lines()
        .filter_map(declaration_key)
        .for_each(|k| {
            declared.insert(k);
        });
    for (name, result) in blocks {
        match result {
            Ok(text) => {
                out.push_str(&format!("# node {}\n", escape_comment(name)));
                for line in text.lines() {
                    if let Some(key) = declaration_key(line) {
                        if !declared.insert(key) {
                            continue; // family already declared by an earlier block
                        }
                    }
                    if line.starts_with('#') || line.trim().is_empty() {
                        out.push_str(line);
                    } else {
                        out.push_str(&retag_node(line, name));
                    }
                    out.push('\n');
                }
            }
            Err(e) => {
                out.push_str(&format!(
                    "# node {} metrics unavailable: {}\n",
                    escape_comment(name),
                    escape_comment(e)
                ));
            }
        }
    }
    out
}

/// `# TYPE name kind` / `# HELP name …` → the dedupe key (`TYPE name` /
/// `HELP name`); None for other lines.
fn declaration_key(line: &str) -> Option<String> {
    let rest = line.strip_prefix("# ")?;
    let kind = rest.split(' ').next()?;
    match kind {
        "TYPE" | "HELP" => rest.split(' ').nth(1).map(|name| format!("{kind} {name}")),
        _ => None,
    }
}

/// Inject `node="<name>"` into one sample line (`name{a="1"} v` →
/// `name{node="x",a="1"} v`; label-less `name v` → `name{node="x"} v`).
/// Assumes well-formed exposition lines (the engine renderer emits only
/// `{k="v"}` label bodies — no nested braces).
fn retag_node(line: &str, node: &str) -> String {
    let tag = format!("node=\"{}\"", escape_label_value(node));
    let (head, tail) = match line.find('{') {
        Some(open) => match line[open..].find('}') {
            Some(rel) => {
                let close = open + rel;
                let body = &line[open + 1..close];
                if body.is_empty() {
                    // empty label set `name{} v` — replace, don't append
                    (
                        format!("{}{{{}}}", &line[..open], tag),
                        line[close + 1..].to_string(),
                    )
                } else {
                    (
                        format!("{}{{{},{}}}", &line[..open], tag, body),
                        line[close + 1..].to_string(),
                    )
                }
            }
            None => return line.to_string(), // malformed — pass through untouched
        },
        None => match line.find(' ') {
            Some(sp) => (
                format!("{}{{{}}}", &line[..sp], tag),
                line[sp..].to_string(),
            ),
            None => return line.to_string(),
        },
    };
    format!("{head}{tail}")
}

/// Label-value escaping (same rules as the engine registry): `\`, `"`, `\n`.
fn escape_label_value(v: &str) -> String {
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

/// Comment bodies must stay single-line (`\n` would forge a new line).
fn escape_comment(v: &str) -> String {
    v.replace(['\n', '\r'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Console-local series: one TYPE line, two op-labelled gauges, values
    /// in seconds and parsable.
    #[test]
    fn console_series_shape() {
        let store = Store::open_in_memory().unwrap();
        store
            .create_user("metrics-u", "phc-hash", None, "admin")
            .unwrap();
        let out = render_console_metrics(&store);
        assert_eq!(
            out,
            format!(
                "# TYPE tailrace_console_db_write_seconds gauge\n\
                 tailrace_console_db_write_seconds{{op=\"tx_last\"}} {}\n\
                 tailrace_console_db_write_seconds{{op=\"tx_avg\"}} {}\n",
                store.db_write_stats().us_last as f64 / 1_000_000.0,
                store.db_write_stats().avg_us() / 1_000_000.0,
            )
        );
        for line in out.lines().filter(|l| !l.starts_with('#')) {
            let value = line.rsplit(' ').next().unwrap();
            assert!(value.parse::<f64>().is_ok(), "parsable seconds: {line}");
        }
    }

    /// Proxy assembly: `node` label injected (labelled AND label-less
    /// series), duplicate `# TYPE` declarations collapsed, unreachable nodes
    /// become comment lines.
    #[test]
    fn proxy_retag_and_type_dedupe() {
        let block_a =
            "# TYPE tailrace_pipeline_state gauge\ntailrace_pipeline_state{pipeline=\"1\"} 0\n";
        let block_b = "# TYPE tailrace_pipeline_state gauge\n# HELP tailrace_pipeline_state s\ntailrace_pipeline_state{pipeline=\"1\"} 1\ntailrace_up 1\n";
        let out = assemble(
            String::new(),
            &[
                ("node-a".to_string(), Ok(block_a.to_string())),
                ("node-b".to_string(), Ok(block_b.to_string())),
                ("node-c\"x".to_string(), Err("unreachable: boom".into())),
            ],
        );
        assert_eq!(
            out.matches("# TYPE tailrace_pipeline_state gauge").count(),
            1,
            "second node's duplicate TYPE dropped: {out}"
        );
        assert!(
            out.contains("# HELP tailrace_pipeline_state s\n"),
            "first HELP kept: {out}"
        );
        assert_eq!(
            out.matches("# HELP tailrace_pipeline_state").count(),
            1,
            "single HELP declaration: {out}"
        );
        assert!(
            out.contains("tailrace_pipeline_state{node=\"node-a\",pipeline=\"1\"} 0\n")
                && out.contains("tailrace_pipeline_state{node=\"node-b\",pipeline=\"1\"} 1\n"),
            "node label disambiguates identical pipelines: {out}"
        );
        assert!(
            out.contains("tailrace_up{node=\"node-b\"} 1\n"),
            "label-less series gets a label set: {out}"
        );
        assert!(
            out.contains("# node node-c\"x metrics unavailable: unreachable: boom\n"),
            "failed fetch degrades to a comment: {out}"
        );
        for line in out.lines().filter(|l| !l.starts_with('#') && !l.is_empty()) {
            let value = line.rsplit(' ').next().unwrap();
            assert!(value.parse::<f64>().is_ok(), "parsable value: {line}");
        }
    }

    /// T-E2 SLO v1: the lag target rides the same label set as the lag
    /// series it watches (node first — the proxy's re-labelled order), only
    /// rule-matched series get a target, overlapping rules collapse to the
    /// tightest target, disabled / non-lag rules contribute nothing, and an
    /// empty result carries no TYPE line. Assembled output keeps both
    /// families side by side under one label set.
    #[test]
    fn slo_lag_target_mirrors_lag_series_labels() {
        let rule =
            |id: i64, enabled: bool, rule_type: &str, params: &str| console_store::AlertRule {
                id,
                name: format!("r{id}"),
                rule_type: rule_type.into(),
                params_json: params.into(),
                enabled,
                webhook_url: None,
                state: "inactive".into(),
                created_at: String::new(),
                updated_at: String::new(),
            };
        let block = "# TYPE tailrace_pipeline_lag_seconds gauge\n\
                     tailrace_pipeline_lag_seconds{pipeline=\"3\",statement=\"0\"} 12\n\
                     tailrace_pipeline_lag_seconds{pipeline=\"4\",statement=\"0\"} 7\n";
        let gathered = Gathered {
            console_text: String::new(),
            blocks: vec![
                ("edge-1".to_string(), Ok(block.to_string())),
                ("edge-2".to_string(), Err("unreachable".to_string())),
            ],
        };
        // no rules → nothing (no stray TYPE line)
        assert_eq!(slo_target_lines(&gathered, &[]), "");
        let rules = vec![
            // pipeline 3 only, 30s
            rule(1, true, "lag_seconds", r#"{"threshold":30,"pipeline":"3"}"#),
            // every lag series, 60s — pipeline 3 collapses to the tighter 30
            rule(2, true, "lag_seconds", r#"{"threshold":60}"#),
            // disabled and non-lag rules contribute nothing
            rule(3, false, "lag_seconds", r#"{"threshold":1}"#),
            rule(4, true, "heartbeat_age", r#"{"threshold":1}"#),
        ];
        let slo = slo_target_lines(&gathered, &rules);
        assert_eq!(
            slo,
            "# TYPE tailrace_slo_lag_target_seconds gauge\n\
             tailrace_slo_lag_target_seconds{node=\"edge-1\",pipeline=\"3\",statement=\"0\"} 30\n\
             tailrace_slo_lag_target_seconds{node=\"edge-1\",pipeline=\"4\",statement=\"0\"} 60\n"
        );
        // assembled: the lag current value line carries the identical label
        // set as its target line
        let out = assemble(slo.clone(), &gathered.blocks);
        assert!(out.contains(
            "tailrace_pipeline_lag_seconds{node=\"edge-1\",pipeline=\"3\",statement=\"0\"} 12\n"
        ));
        assert!(out.contains(
            "tailrace_slo_lag_target_seconds{node=\"edge-1\",pipeline=\"3\",statement=\"0\"} 30\n"
        ));
        assert_eq!(
            out.matches("# TYPE tailrace_slo_lag_target_seconds gauge")
                .count(),
            1
        );
        // the evaluator input is the same snapshot (node label injected)
        let samples = node_samples(&gathered);
        assert_eq!(samples.len(), 2);
        assert_eq!(
            samples[0].labels.get("node").map(String::as_str),
            Some("edge-1")
        );
    }
}
