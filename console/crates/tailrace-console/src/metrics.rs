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

/// `GET /metrics` — open (no session; exporter convention, like /healthz).
pub async fn scrape(State(state): State<AppState>) -> Response {
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
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static(CONTENT_TYPE),
        )],
        assemble(console_text, &blocks),
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
}
