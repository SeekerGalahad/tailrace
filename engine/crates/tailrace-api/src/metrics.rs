//! A6/E1: hand-written Prometheus text-format registry (blueprint §7.4) —
//! zero external dependencies, ~150 lines, no `metrics`/exporter crates.
//!
//! Design: [`MetricsRegistry`] is a plain `Mutex<HashMap>` store (f64 gauges,
//! u64 counters) keyed by `metric + rendered labels`. The node /metrics
//! endpoint builds a fresh registry **at scrape time** from the live
//! [`tailrace_runtime::registry::PipelineRegistry`] counters (the flush loop
//! stays the single write site — no sampling thread, no staleness) and
//! renders it; nothing is global. The 9-metric surface:
//!
//! - `tailrace_pipeline_lag_seconds` (gauge, pipeline/statement) — v1 basis
//!   is `last_data_ms` (wall time of the last data-carrying flush), NOT
//!   payload event time (that needs per-schema extraction; future work).
//! - `tailrace_pipeline_rows_total` (counter, +result) — result="ok" maps to
//!   flushed rows, result="dlq" to decoded bad rows; per-row "failed" is not
//!   tracked (a failed statement surfaces via `tailrace_pipeline_state=3`).
//! - `tailrace_pipeline_bytes_total` — v1: not yet fed (sinks report only
//!   in-flight `buffered_bytes`, not cumulative bytes; wiring awaits a sink
//!   ack byte count) — declared here for the §7.4 surface audit.
//! - `tailrace_flush_total` (counter, +result) — result="success"; the
//!   FlushOutcome Dedup/Retry/Error split lands with A2 deterministic
//!   labels (v1 flushes are single-state).
//! - `tailrace_flush_duration_seconds` (gauge) — §7.4 histogram simplified
//!   to gauge+counter for v1: the gauge is the last flush duration, the
//!   counter side is `tailrace_flush_total`.
//! - `tailrace_dlq_rows_total` (counter) — decoded bad rows quarantined.
//! - `tailrace_pipeline_state` (gauge) — ControlFlags u8 verbatim:
//!   0=RUNNING 1=PAUSED 2=STOPPED 3=FAILED.
//! - `tailrace_node_heartbeat_age_seconds` (gauge, node_id) — rendered from
//!   [`crate::heartbeat`] last-beat note; age is computed at scrape time.
//! - `tailrace_console_db_write_seconds` — console-side (tailrace-console
//!   renders it from its own store stats; it does not appear here).

use std::collections::HashMap;
use std::sync::Mutex;

/// Prometheus text-format content type (the versioned form scrapers expect).
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Default)]
struct Inner {
    /// series key (metric + rendered labels) → gauge series
    gauges: HashMap<String, Series<f64>>,
    /// series key → counter series
    counters: HashMap<String, Series<u64>>,
}

struct Series<V> {
    metric: String,
    /// rendered label set (`{a="1",b="2"}`, empty when label-less)
    labels: String,
    value: V,
}

/// Thread-safe gauge/counter store rendering Prometheus text format.
/// A series' type is fixed by its first write: a name registered as a gauge
/// ignores later `inc_counter` calls (and vice versa) — a programming error
/// we surface by refusing rather than emitting two TYPE lines.
#[derive(Default)]
pub struct MetricsRegistry {
    inner: Mutex<Inner>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set (or overwrite) a gauge series.
    pub fn set_gauge(&self, metric: &str, labels: &[(&str, &str)], value: f64) {
        let key = series_key(metric, labels);
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.counters.contains_key(&key) {
            return; // type conflict — see struct doc
        }
        g.gauges.insert(
            key,
            Series {
                metric: metric.to_string(),
                labels: render_labels(labels),
                value,
            },
        );
    }

    /// Add `delta` to a counter series (first write = set).
    pub fn inc_counter(&self, metric: &str, labels: &[(&str, &str)], delta: u64) {
        let key = series_key(metric, labels);
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.gauges.contains_key(&key) {
            return; // type conflict — see struct doc
        }
        let e = g.counters.entry(key).or_insert_with(|| Series {
            metric: metric.to_string(),
            labels: render_labels(labels),
            value: 0,
        });
        e.value = e.value.saturating_add(delta);
    }

    /// Read a gauge series back (0.0 when never set).
    pub fn get_gauge(&self, metric: &str, labels: &[(&str, &str)]) -> f64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .gauges
            .get(&series_key(metric, labels))
            .map_or(0.0, |s| s.value)
    }

    /// Read a counter series back (0 when never set).
    pub fn get_counter(&self, metric: &str, labels: &[(&str, &str)]) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .counters
            .get(&series_key(metric, labels))
            .map_or(0, |s| s.value)
    }

    /// Render the Prometheus text exposition format: one `# TYPE` line per
    /// metric name, then one value line per series. Metric groups and series
    /// are emitted in sorted order (byte-deterministic output — scrapes and
    /// tests can diff).
    pub fn render(&self) -> String {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // metric name → (type, [(value line, series sort key)])
        let mut families: Families = HashMap::new();
        for s in g.gauges.values() {
            push_series(
                &s.metric,
                "gauge",
                &mut families,
                &s.labels,
                fmt_f64(s.value),
            );
        }
        for s in g.counters.values() {
            push_series(
                &s.metric,
                "counter",
                &mut families,
                &s.labels,
                s.value.to_string(),
            );
        }
        let mut names: Vec<String> = families.keys().cloned().collect();
        names.sort_unstable();
        let mut out = String::new();
        for name in names {
            let Some((kind, series)) = families.get_mut(&name) else {
                continue;
            };
            series.sort_unstable_by(|a, b| a.1.cmp(&b.1));
            out.push_str(&format!("# TYPE {name} {kind}\n"));
            for (line, _) in series.iter() {
                out.push_str(line);
                out.push('\n');
            }
        }
        out
    }
}

/// Render-time family bucket: metric name → (type, [(value line, sort key)]).
type Families = HashMap<String, (&'static str, Vec<(String, String)>)>;

/// One series → its family bucket (free fn: a closure over three distinct
/// borrows fights lifetime inference for no gain).
fn push_series(
    metric: &str,
    kind: &'static str,
    families: &mut Families,
    labels: &str,
    value: String,
) {
    let sort_key = format!("{metric}{labels}");
    let line = format!("{metric}{labels} {value}");
    families
        .entry(metric.to_string())
        .or_insert_with(|| (kind, Vec::new()))
        .1
        .push((line, sort_key));
}

/// Series identity: metric + rendered labels (labels sorted by name so
/// `set_gauge("m", &[("a","1"),("b","2")])` and `&[("b","2"),("a","1")]`
/// address the same series).
fn series_key(metric: &str, labels: &[(&str, &str)]) -> String {
    format!("{metric}{}", render_labels(labels))
}

/// Render a sorted, escaped label set: `{k="v",…}` (empty string when none).
fn render_labels(labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut sorted: Vec<&(&str, &str)> = labels.iter().collect();
    sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let body: Vec<String> = sorted
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect();
    format!("{{{}}}", body.join(","))
}

/// Prometheus label-value escaping: `\` → `\\`, `"` → `\"`, newline → `\n`.
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

/// Prometheus float exposition: `NaN`, `+Inf`/`-Inf` (Rust's bare `inf`
/// is invalid in the format), otherwise plain `{}`.
fn fmt_f64(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        format!("{v}")
    }
}

/// Node /metrics body: live pipeline counters + heartbeat age folded into a
/// one-shot registry and rendered.
pub fn node_metrics_text(registry: &tailrace_runtime::registry::PipelineRegistry) -> String {
    let m = MetricsRegistry::new();
    render_pipeline_metrics(
        &m,
        &registry.metrics_snapshot(),
        crate::heartbeat::heartbeat_age_seconds(),
        now_ms(),
    );
    m.render()
}

/// Fold one scrape's data into `m` (split out for unit tests — the mapping
/// from counter snapshots to the §7.4 series is the part worth pinning).
pub fn render_pipeline_metrics(
    m: &MetricsRegistry,
    pipes: &[tailrace_runtime::registry::PipelineMetricsSnapshot],
    heartbeat: Option<(String, f64)>,
    now: u64,
) {
    for pipe in pipes {
        let pid = pipe.pipeline_id.to_string();
        m.set_gauge(
            "tailrace_pipeline_state",
            &[("pipeline", &pid)],
            pipe.state as f64,
        );
        for (i, s) in pipe.statements.iter().enumerate() {
            let stmt = i.to_string();
            let ps: &[(&str, &str)] = &[("pipeline", pid.as_str()), ("statement", stmt.as_str())];
            let mut ok = ps.to_vec();
            ok.push(("result", "ok"));
            let mut dlq = ps.to_vec();
            dlq.push(("result", "dlq"));
            m.inc_counter("tailrace_pipeline_rows_total", &ok, s.flushed);
            m.inc_counter("tailrace_pipeline_rows_total", &dlq, s.bad);
            let mut succ = ps.to_vec();
            succ.push(("result", "success"));
            m.inc_counter("tailrace_flush_total", &succ, s.flushes);
            m.inc_counter("tailrace_dlq_rows_total", ps, s.bad);
            m.set_gauge(
                "tailrace_flush_duration_seconds",
                ps,
                s.last_flush_dur_ms as f64 / 1000.0,
            );
            if s.last_data_ms > 0 {
                m.set_gauge(
                    "tailrace_pipeline_lag_seconds",
                    ps,
                    now.saturating_sub(s.last_data_ms) as f64 / 1000.0,
                );
            }
        }
    }
    if let Some((node_id, age)) = heartbeat {
        m.set_gauge(
            "tailrace_node_heartbeat_age_seconds",
            &[("node_id", &node_id)],
            age,
        );
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A6 acceptance: text-format validity — `# TYPE` line per metric, value
    /// lines `name{labels} value`, label-less gauges render bare, label
    /// values escaped, and two renders are byte-identical (sorted output).
    #[test]
    fn text_format_validity() {
        let m = MetricsRegistry::new();
        m.inc_counter(
            "tailrace_pipeline_rows_total",
            &[("pipeline", "1"), ("statement", "0"), ("result", "ok")],
            42,
        );
        m.set_gauge("tailrace_pipeline_state", &[("pipeline", "1")], 0.0);
        m.set_gauge("tailrace_up", &[], 1.0);
        m.set_gauge("esc", &[("path", "a\"b\\c\nd")], 2.0);
        let out = m.render();
        assert_eq!(out, m.render(), "deterministic (sorted) output");
        assert!(
            out.contains("# TYPE tailrace_pipeline_rows_total counter\n"),
            "counter TYPE line: {out}"
        );
        assert!(
            out.contains("# TYPE tailrace_pipeline_state gauge\n"),
            "gauge TYPE line: {out}"
        );
        assert!(
            out.contains(
                "tailrace_pipeline_rows_total{pipeline=\"1\",result=\"ok\",statement=\"0\"} 42\n"
            ),
            "value line with sorted labels: {out}"
        );
        assert!(
            out.contains("tailrace_pipeline_state{pipeline=\"1\"} 0\n"),
            "gauge 0.0 renders as `0`: {out}"
        );
        assert!(
            out.contains("\ntailrace_up 1\n"),
            "label-less series renders without braces: {out}"
        );
        assert!(
            out.contains("esc{path=\"a\\\"b\\\\c\\nd\"} 2\n"),
            "label escaping (quote/backslash/newline): {out}"
        );
        // every non-# line carries a numeric value
        for line in out.lines().filter(|l| !l.starts_with('#')) {
            let value = line.rsplit(' ').next().unwrap();
            assert!(
                value.parse::<f64>().is_ok(),
                "parsable value token in {line:?}"
            );
        }
    }

    /// A6 acceptance: counter increments accumulate; get_counter reads back.
    #[test]
    fn counter_increments() {
        let m = MetricsRegistry::new();
        let lbl: &[(&str, &str)] = &[("pipeline", "7"), ("result", "success")];
        m.inc_counter("tailrace_flush_total", lbl, 3);
        m.inc_counter("tailrace_flush_total", lbl, 4);
        m.inc_counter(
            "tailrace_flush_total",
            &[("pipeline", "7"), ("result", "error")],
            1,
        );
        assert_eq!(m.get_counter("tailrace_flush_total", lbl), 7);
        assert_eq!(
            m.get_counter(
                "tailrace_flush_total",
                &[("pipeline", "7"), ("result", "error")]
            ),
            1,
            "distinct label sets are distinct series"
        );
        assert_eq!(
            m.get_counter("tailrace_flush_total", &[("pipeline", "999")]),
            0,
            "unset series reads 0"
        );
        assert!(
            m.render()
                .contains("tailrace_flush_total{pipeline=\"7\",result=\"success\"} 7\n"),
            "render shows the accumulated value"
        );
    }

    /// A6 acceptance: gauge sets overwrite; label order does not matter;
    /// specials (NaN/±inf) render Prometheus-valid tokens.
    #[test]
    fn gauge_sets() {
        let m = MetricsRegistry::new();
        let a: &[(&str, &str)] = &[("pipeline", "1"), ("statement", "0")];
        let b: &[(&str, &str)] = &[("statement", "0"), ("pipeline", "1")];
        m.set_gauge("tailrace_pipeline_lag_seconds", a, 1.5);
        m.set_gauge("tailrace_pipeline_lag_seconds", b, 2.5);
        assert_eq!(
            m.get_gauge("tailrace_pipeline_lag_seconds", a),
            2.5,
            "last set wins; label order irrelevant (same series)"
        );
        m.set_gauge("nan_g", &[], f64::NAN);
        m.set_gauge("pos_inf", &[], f64::INFINITY);
        m.set_gauge("neg_inf", &[], f64::NEG_INFINITY);
        let out = m.render();
        assert!(out.contains("nan_g NaN\n"), "{out}");
        assert!(out.contains("pos_inf +Inf\n"), "{out}");
        assert!(out.contains("neg_inf -Inf\n"), "{out}");
    }

    /// A gauge/counter name clash refuses the second type (no double TYPE).
    #[test]
    fn type_conflict_refused() {
        let m = MetricsRegistry::new();
        m.set_gauge("clash", &[("k", "v")], 1.0);
        m.inc_counter("clash", &[("k", "v")], 5);
        let out = m.render();
        assert_eq!(
            out, "# TYPE clash gauge\nclash{k=\"v\"} 1\n",
            "counter write on a gauge series is ignored"
        );
    }

    /// The §7.4 mapping: counter snapshots → the 8 node-side series families
    /// (state / rows ok+dlq / flush success / duration / dlq / lag), plus
    /// heartbeat age. lag omitted until data has actually landed.
    #[test]
    fn pipeline_series_mapping() {
        use tailrace_runtime::pipeline_log::StatementCountersSnapshot as S;
        use tailrace_runtime::registry::PipelineMetricsSnapshot as P;
        let mk = |flushed, bad, flushes, dur, data| S {
            consumed: 0,
            flushed,
            skipped: 0,
            bad,
            flushes,
            last_flush_ms: 0,
            flush_ms_total: 0,
            last_flush_dur_ms: dur,
            last_data_ms: data,
        };
        let pipes = vec![
            P {
                pipeline_id: 3,
                state: 1, // PAUSED
                statements: vec![mk(100, 4, 7, 250, 1_000)],
            },
            P {
                pipeline_id: 4,
                state: 0,
                statements: vec![mk(0, 0, 0, 0, 0)], // never flushed data
            },
        ];
        let m = MetricsRegistry::new();
        render_pipeline_metrics(&m, &pipes, Some(("node-a".into(), 12.5)), 61_000);
        let out = m.render();
        assert!(
            out.contains("tailrace_pipeline_state{pipeline=\"3\"} 1\n")
                && out.contains("tailrace_pipeline_state{pipeline=\"4\"} 0\n"),
            "state exported as ControlFlags u8: {out}"
        );
        assert!(
            out.contains(
                "tailrace_pipeline_rows_total{pipeline=\"3\",result=\"ok\",statement=\"0\"} 100\n"
            ) && out.contains(
                "tailrace_pipeline_rows_total{pipeline=\"3\",result=\"dlq\",statement=\"0\"} 4\n"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "tailrace_flush_total{pipeline=\"3\",result=\"success\",statement=\"0\"} 7\n"
            ) && out.contains("tailrace_dlq_rows_total{pipeline=\"3\",statement=\"0\"} 4\n"),
            "{out}"
        );
        assert!(
            out.contains("tailrace_flush_duration_seconds{pipeline=\"3\",statement=\"0\"} 0.25\n"),
            "250ms → 0.25s: {out}"
        );
        assert!(
            out.contains("tailrace_pipeline_lag_seconds{pipeline=\"3\",statement=\"0\"} 60\n")
                && out.contains("tailrace_node_heartbeat_age_seconds{node_id=\"node-a\"} 12.5\n"),
            "lag = now - last_data_ms (61_000-1_000 ms = 60s): {out}"
        );
        assert!(
            !out.contains("tailrace_pipeline_lag_seconds{pipeline=\"4\""),
            "no lag series before the first data-carrying flush: {out}"
        );
    }
}
