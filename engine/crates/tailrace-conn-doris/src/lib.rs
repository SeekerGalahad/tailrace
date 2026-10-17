//! tailrace-conn-doris: Doris sink (BE-direct stream load + label
//! idempotency + Status: Success judgment).
//!
//! reqwest blocking connects straight to the BE (an FE 307 redirect strips
//! the Authorization header — redirects are explicitly not followed).
//! Carrier is JSON lines (`format: json` + `read_json_by_line: true`):
//! apache/doris be-2.1.7's CSV splitter has no RFC4180 mode — `""` doubled
//! quotes are unparseable with every enclose/escape header combination
//! (T11 first-run matrix), while JSON escaping is total over arbitrary
//! UTF-8 (in-field newlines / quotes / emoji / backslashes).

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;

use tailrace_catalog::{credential_from_env, ReliabilityOptions, TableObject};
use tailrace_connector::registry::ConnectorRegistry;
use tailrace_connector::{Sink, SinkAck};

/// Append one JSON object per row (Doris stream load `format: json` +
/// `read_json_by_line`): column i of the batch is emitted under
/// `columns[i]` (the sink's declared column order — the same positional
/// contract the CSV carrier used). Types follow the engine's column
/// vocabulary (tailrace-ir `arrow_type`): int64/float64/bool as JSON
/// numbers/literals, everything else as a JSON string; nulls stay null.
/// serde_json escaping makes the carrier total over arbitrary UTF-8 —
/// no field value can break the framing.
fn append_json_lines(
    out: &mut BytesMut,
    batch: &RecordBatch,
    columns: &[String],
) -> anyhow::Result<()> {
    use datafusion::arrow::datatypes::DataType;
    use std::fmt::Write as _;
    anyhow::ensure!(
        batch.num_columns() == columns.len(),
        "doris sink: batch has {} columns but {} were declared ({})",
        batch.num_columns(),
        columns.len(),
        columns.join(",")
    );
    let mut line = String::with_capacity(64 + 16 * columns.len());
    for row in 0..batch.num_rows() {
        line.clear();
        line.push('{');
        for (ci, name) in columns.iter().enumerate() {
            if ci > 0 {
                line.push(',');
            }
            let _ = write!(line, "{}:", serde_json::to_string(name)?);
            let col = batch.column(ci);
            if col.is_null(row) {
                line.push_str("null");
                continue;
            }
            match col.data_type() {
                DataType::Int64 => {
                    let _ = write!(
                        line,
                        "{}",
                        col.as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("Int64 column")
                            .value(row)
                    );
                }
                DataType::Float64 => {
                    let v = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("Float64 column")
                        .value(row);
                    // JSON has no NaN/Infinity — degrade to null rather
                    // than emit an unparseable token
                    if v.is_finite() {
                        let _ = write!(line, "{v}");
                    } else {
                        line.push_str("null");
                    }
                }
                DataType::Boolean => {
                    let _ = write!(
                        line,
                        "{}",
                        col.as_any()
                            .downcast_ref::<BooleanArray>()
                            .expect("Boolean column")
                            .value(row)
                    );
                }
                DataType::Utf8 => {
                    let _ = write!(
                        line,
                        "{}",
                        serde_json::to_string(
                            col.as_any()
                                .downcast_ref::<StringArray>()
                                .expect("Utf8 column")
                                .value(row)
                        )?
                    );
                }
                // any other arrow type (dates/timestamps/decimal…) formats
                // through the display formatter and rides as a JSON string
                _ => {
                    let f = datafusion::arrow::util::display::ArrayFormatter::try_new(
                        col,
                        &datafusion::arrow::util::display::FormatOptions::default(),
                    )?;
                    let s = f.value(row).to_string();
                    let _ = write!(line, "{}", serde_json::to_string(&s)?);
                }
            }
        }
        line.push_str("}\n");
        out.extend_from_slice(line.as_bytes());
    }
    Ok(())
}

/// Register the doris sink factory (plus the "starrocks" alias when that
/// feature is on — the Stream Load protocol is shared).
pub fn register(reg: &mut ConnectorRegistry) {
    reg.register_sink("doris", sink_factory);
    #[cfg(feature = "starrocks")]
    reg.register_sink("starrocks", sink_factory);
}

fn sink_factory(
    obj: &TableObject,
    rel: Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Sink>> {
    let auth_env = obj.auth_env.as_deref().ok_or_else(|| {
        anyhow::anyhow!("doris sink must declare auth_env (credentials never enter files)")
    })?;
    let auth = credential_from_env(auth_env)?;
    let url = obj
        .stream_load_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("doris sink must declare stream_load_url (BE direct)"))?;
    let columns = obj.columns.iter().map(|(n, _)| n.clone()).collect();
    let prefix = obj
        .label_prefix
        .clone()
        .unwrap_or_else(|| format!("p{}", obj.datasource));
    // sequence column wiring + reliability knobs (retry backoff / request timeout)
    let mut sink =
        DorisSink::with_sequence(url, auth, columns, prefix, obj.sequence_column.clone());
    if let Some(r) = rel {
        sink = sink
            .with_retry_backoff(r.flush_backoff_ms())
            .with_request_timeout(r.doris_timeout_ms());
    }
    Ok(Box::new(sink))
}

/// Terminal outcome of one stream load attempt.
#[derive(Debug, PartialEq, Eq)]
enum FlushOutcome {
    /// Status=Success / Publish Timeout (the latter means data is committed,
    /// publishing is asynchronous — treated as success)
    Success,
    /// Label Already Exists — the same label succeeded before (a restart
    /// replay); idempotently converges to success
    Dedup,
}

/// Flush failure classification: retryable=true retries with bounded backoff
/// (transient faults self-heal); false kills the pipeline immediately
/// (retrying is pointless).
struct FlushError {
    retryable: bool,
    message: String,
}

impl FlushError {
    fn retry(msg: impl Into<String>) -> Self {
        Self {
            retryable: true,
            message: msg.into(),
        }
    }
    fn fatal(msg: impl Into<String>) -> Self {
        Self {
            retryable: false,
            message: msg.into(),
        }
    }
}

/// Doris sink (BE direct + label idempotency + Status: Success judgment).
/// flush retries with bounded backoff (default 3 attempts, 2s→8s→30s) —
/// transient faults (FE/BE restarts, network jitter) self-heal instead of
/// killing the pipeline; the buffer is kept across retries (a failure never
/// drops buffered rows).
pub struct DorisSink {
    /// The blocking client is built lazily on first flush: serve-mode plan
    /// assembly runs inside an axum tokio context, and building a blocking
    /// client there panics ("Cannot drop a runtime in a context where
    /// blocking is not allowed"). The statement thread is fully synchronous —
    /// legal. OnceCell is single-thread-written, Send-safe.
    client: std::cell::OnceCell<reqwest::blocking::Client>,
    url: String,
    auth: String,
    columns: Vec<String>,
    /// Sequence column name (optional — a Doris UNIQUE KEY table adds a
    /// sequence column to cancel out-of-order overwrites)
    sequence_column: Option<String>,
    label_prefix: String,
    label_seq: u64,
    pending_rows: u64,
    /// In-flight carrier buffer (T3: `BytesMut` grows like a Vec on the write
    /// side; at flush time it `freeze()`s into a refcounted `Bytes` the retry
    /// loop hands to reqwest via cheap clones — the old `body.to_vec()`
    /// memcpy'd the whole buffer on every attempt).
    buf: BytesMut,
    /// Retry backoff sequence (after the Nth failure sleep backoff_ms[N-1]
    /// then retry; exhaustion kills)
    retry_backoff_ms: Vec<u64>,
    /// Per-request timeout (default 120s; applies when the client is lazily built)
    request_timeout_ms: u64,
}

impl DorisSink {
    /// url looks like http://host:8040/api/{db}/{table}/_stream_load;
    /// auth = Basic base64(user:pass).
    pub fn new(
        url: impl Into<String>,
        auth: impl Into<String>,
        columns: Vec<String>,
        label_prefix: impl Into<String>,
    ) -> Self {
        Self::with_sequence(url, auth, columns, label_prefix, None)
    }

    /// Constructor with a sequence column (recommended for Doris UNIQUE KEY
    /// tables: a monotonic column like ogg_ts/etl_time).
    pub fn with_sequence(
        url: impl Into<String>,
        auth: impl Into<String>,
        columns: Vec<String>,
        label_prefix: impl Into<String>,
        sequence_column: Option<String>,
    ) -> Self {
        Self {
            sequence_column,
            client: std::cell::OnceCell::new(),
            url: url.into(),
            auth: auth.into(),
            columns,
            // label idempotency collision guard: unique per process run
            // (Doris labels are globally deduplicated — a rerun would be rejected)
            label_prefix: format!("{}-r{}", label_prefix.into(), std::process::id()),
            label_seq: 0,
            pending_rows: 0,
            buf: BytesMut::with_capacity(16 * 1024 * 1024),
            retry_backoff_ms: vec![2_000, 8_000, 30_000],
            request_timeout_ms: 120_000,
        }
    }

    /// Retry backoff sequence override (empty = the default [2s,8s,30s]).
    pub fn with_retry_backoff(mut self, backoff_ms: Vec<u64>) -> Self {
        if !backoff_ms.is_empty() {
            self.retry_backoff_ms = backoff_ms;
        }
        self
    }

    /// Per-request timeout override (default 120s; clamped 1s-600s).
    pub fn with_request_timeout(mut self, ms: u64) -> Self {
        self.request_timeout_ms = ms.clamp(1_000, 600_000);
        self
    }

    /// Lazily take the blocking client (built on the statement thread at
    /// first use — a synchronous context, legal).
    fn client(&self) -> anyhow::Result<&reqwest::blocking::Client> {
        if self.client.get().is_none() {
            // No redirect following: an FE 307 strips the Authorization
            // header, and silently following it produces a 401 illusion;
            // Policy::none() makes the 307 a fatal error that tells the
            // operator to switch to a BE-direct URL.
            let c = reqwest::blocking::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_millis(self.request_timeout_ms))
                .build()
                .map_err(|e| anyhow::anyhow!("reqwest blocking client build failed: {e}"))?;
            let _ = self.client.set(c);
        }
        Ok(self.client.get().expect("just initialized"))
    }

    /// One send attempt with terminal-outcome judgment. The body is a
    /// refcounted `Bytes` — the reqwest clone is free, and on failure the
    /// shared buffer stays intact for the retry (a failure never drops the
    /// buffered rows).
    fn send_once(&self, label: &str, body: &Bytes) -> Result<FlushOutcome, FlushError> {
        let brief = |t: &str| -> String { t.chars().take(300).collect() };
        let resp = self
            .client()
            .map_err(|e| FlushError::fatal(e.to_string()))?
            .put(&self.url)
            .header("Authorization", &self.auth)
            .header("label", label)
            // JSON-lines carrier (one object per line): Doris maps by key
            // name; serde_json escaping carries any UTF-8 payload verbatim.
            // The old CSV path sent enclose/escape headers and be-2.1.7
            // could not parse RFC4180 doubled quotes under any combination
            // of them (T11 first-run matrix) — quoted/comma'd fields died.
            .header("format", "json")
            .header("read_json_by_line", "true")
            // Doris sequence column declaration (the out-of-order dedup basis
            // for UNIQUE KEY tables)
            .header(
                "sequence_column",
                self.sequence_column.as_deref().unwrap_or(""),
            )
            .header("Expect", "100-continue")
            .body(body.clone())
            .send();
        let resp = match resp {
            Ok(r) => r,
            // network-layer errors (connection refused/timeout/reset) —
            // typically transient, retryable
            Err(e) => return Err(FlushError::retry(format!("network error: {e}"))),
        };
        let status = resp.status();
        let text = resp.text().unwrap_or_default();
        if status.is_redirection() {
            return Err(FlushError::fatal(format!(
                "HTTP {} redirect — stream_load_url points at an FE (which strips the Authorization header); use a BE:8040 direct URL: {}",
                status.as_u16(),
                brief(&text)
            )));
        }
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(FlushError::fatal(format!(
                "authentication failed (HTTP {}): {} — check TAILRACE_DORIS_AUTH",
                status.as_u16(),
                brief(&text)
            )));
        }
        if status.is_client_error() {
            return Err(FlushError::fatal(format!(
                "HTTP {}: {}",
                status.as_u16(),
                brief(&text)
            )));
        }
        // JSON parsing instead of string sniffing + char-boundary-safe
        // truncation (slicing at a non-char boundary used to panic)
        let v = match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(v) => v,
            // non-JSON response (a gateway/proxy error page) — likely
            // transient, retryable
            Err(_) => {
                return Err(FlushError::retry(format!(
                    "non-JSON response (HTTP {}): {}",
                    status.as_u16(),
                    brief(&text)
                )))
            }
        };
        let st = v.get("Status").and_then(|s| s.as_str()).unwrap_or("");
        if st == "Success" || st == "Publish Timeout" {
            return Ok(FlushOutcome::Success);
        }
        let reason = v
            .get("Reason")
            .or_else(|| v.get("Message"))
            .and_then(|r| r.as_str())
            .unwrap_or("");
        // ErrorURL passthrough: the bad-row details of DATA_QUALITY-type
        // failures (original row + column + reason) only exist in ErrorURL.
        // Truncation applies to the reason text only (Doris quality messages
        // are long templates); the URL must stay complete and curl-able — a
        // half URL is unusable for triage.
        let error_url = v.get("ErrorURL").and_then(|u| u.as_str()).unwrap_or("");
        let reason = if error_url.is_empty() {
            brief(reason).to_string()
        } else {
            format!("{} | bad rows: {}", brief(reason), error_url)
        };
        let lower = format!("{st} {reason}").to_lowercase();
        // The same label already committed successfully (a restart replay) →
        // converge to success (the data is in the warehouse); do not kill
        if lower.contains("label already exists") || lower.contains("has already been used") {
            return Ok(FlushOutcome::Dedup);
        }
        if lower.contains("timeout") || lower.contains("too many") || status.is_server_error() {
            return Err(FlushError::retry(format!("Status={st} Reason={reason}")));
        }
        // Other Fail outcomes (strict-mode filtering, schema mismatch, syntax
        // errors) — deterministic failures, retrying is pointless
        Err(FlushError::fatal(format!("Status={st} Reason={reason}")))
    }
}

impl Sink for DorisSink {
    fn buffered_bytes(&self) -> u64 {
        self.buf.len() as u64
    }

    fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        for b in &batches {
            append_json_lines(&mut self.buf, b, &self.columns)?;
        }
        self.pending_rows += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<SinkAck> {
        if self.pending_rows == 0 {
            return Ok(SinkAck {
                rows: 0,
                kind: "DORIS_STREAM_LOAD",
                idempotency_key: format!("{}.noop", self.label_prefix),
            });
        }
        let label = format!("{}_{}", self.label_prefix, self.label_seq);
        // (T3) freeze the accumulated carrier into a refcounted body: every
        // retry attempt clones the handle (refcount), not the bytes.
        let body = std::mem::take(&mut self.buf).freeze();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.send_once(&label, &body) {
                Ok(_) => {
                    self.label_seq += 1;
                    let rows = self.pending_rows;
                    self.pending_rows = 0;
                    // restore the buffer watermark (keep capacity to avoid
                    // regrowing per batch)
                    self.buf = BytesMut::with_capacity(16 * 1024 * 1024);
                    return Ok(SinkAck {
                        rows,
                        kind: "DORIS_STREAM_LOAD",
                        idempotency_key: label,
                    });
                }
                Err(e) => {
                    let exhausted = attempt as usize > self.retry_backoff_ms.len();
                    if e.retryable && !exhausted {
                        let backoff = self.retry_backoff_ms[(attempt - 1) as usize];
                        tracing::warn!(
                            attempt,
                            backoff_ms = backoff,
                            label = %label,
                            error = %e.message,
                            "stream load attempt failed, retrying"
                        );
                        std::thread::sleep(Duration::from_millis(backoff));
                        continue;
                    }
                    let verdict = if e.retryable {
                        "retries exhausted"
                    } else {
                        "not retryable"
                    };
                    // the attempt is terminal — hand the buffered bytes back
                    // (try_into_mut is free when reqwest released its clone;
                    // the fallback copy only happens if it somehow hasn't)
                    self.buf = body
                        .try_into_mut()
                        .unwrap_or_else(|b| BytesMut::from(&b[..]));
                    anyhow::bail!(
                        "stream load failed label={label} (attempt {attempt}, {verdict}): {}",
                        e.message
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::Ordering as Ord2;
    use std::sync::{Arc, Mutex};

    /// Minimal Doris mock (std TcpListener — zero new deps): responds to PUTs
    /// in scripted order, counting hits and keeping bodies.
    struct MockDoris {
        addr: std::net::SocketAddr,
        hits: Arc<std::sync::atomic::AtomicUsize>,
        bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len()).position(|w| w == needle)
    }

    impl MockDoris {
        /// script: respond in order with (HTTP status, body); requests beyond
        /// the script block on accept (tests assert on hits to intercept)
        fn start(script: Vec<(u16, String)>) -> Self {
            use std::io::{Read, Write};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let bodies = Arc::new(Mutex::new(Vec::new()));
            let (h2, b2) = (Arc::clone(&hits), Arc::clone(&bodies));
            std::thread::spawn(move || {
                for (code, body) in script {
                    let Ok((mut s, _)) = listener.accept() else {
                        return;
                    };
                    h2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mut data = Vec::new();
                    let mut buf = [0u8; 16384];
                    // read to the header boundary
                    let mut head_end = None;
                    while head_end.is_none() {
                        let n = s.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        data.extend_from_slice(&buf[..n]);
                        if let Some(p) = find_sub(&data, b"\r\n\r\n") {
                            head_end = Some(p + 4);
                        }
                    }
                    let Some(pos) = head_end else { continue };
                    let head = String::from_utf8_lossy(&data[..pos]).to_ascii_lowercase();
                    // honor Expect: 100-continue first (matches a real BE and
                    // avoids a 1s hyper wait)
                    if head.contains("expect: 100-continue") {
                        let _ = s.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
                    }
                    let cl = head
                        .lines()
                        .find_map(|l| {
                            l.strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    while data.len() < pos + cl {
                        let n = s.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        data.extend_from_slice(&buf[..n]);
                    }
                    b2.lock().unwrap().push(data[pos..].to_vec());
                    let reason = match code {
                        200 => "OK",
                        307 => "Temporary Redirect",
                        401 => "Unauthorized",
                        500 => "Internal Server Error",
                        _ => "Status",
                    };
                    let extra = if code == 307 {
                        "Location: http://127.0.0.1:1/fe\r\n"
                    } else {
                        ""
                    };
                    let resp = format!(
                        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\n{extra}\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes());
                    let _ = s.flush();
                }
            });
            Self { addr, hits, bodies }
        }

        fn url(&self) -> String {
            format!("http://{}/api/db/t/_stream_load", self.addr)
        }
    }

    #[test]
    fn doris_json_carrier_poison_roundtrip() -> anyhow::Result<()> {
        // The carrier must be total over arbitrary UTF-8: in-field
        // newlines / doubled quotes / commas / emoji / CJK / backslashes
        // ride JSON escaping verbatim (the old CSV carrier died on these
        // against apache/doris be-2.1.7 — T11 first-run finding).
        let poison = "line1\nline2 \"quoted, comma\" 🎉🚀 中文毒数据 <tag>&tailrace \\path";
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_no", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, true),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["P-1"])),
                Arc::new(StringArray::from(vec![Some(poison)])),
                Arc::new(Int64Array::from(vec![Some(7)])),
            ],
        )?;
        // null column + boolean column coverage
        let rb2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_no", DataType::Utf8, false),
                Field::new("note", DataType::Utf8, true),
                Field::new("qty", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["P-2"])),
                Arc::new(StringArray::from(vec![None::<String>])),
                Arc::new(Int64Array::from(vec![None])),
            ],
        )?;

        let m = MockDoris::start(vec![(200, r#"{"Status":"Success"}"#.into())]);
        let mut sink = DorisSink::new(
            m.url(),
            "Basic dGVzdDp0ZXN0",
            vec!["order_no".into(), "note".into(), "qty".into()],
            "mjson",
        );
        sink.write(vec![rb, rb2])?;
        assert_eq!(sink.flush()?.rows, 2);

        let bodies = m.bodies.lock().unwrap().clone();
        assert_eq!(bodies.len(), 1);
        let text = std::str::from_utf8(&bodies[0])?;
        // two JSON lines, each independently parseable (read_json_by_line)
        let mut lines = text.lines();
        let l1: serde_json::Value = serde_json::from_str(lines.next().expect("line 1"))?;
        let l2: serde_json::Value = serde_json::from_str(lines.next().expect("line 2"))?;
        assert!(lines.next().is_none(), "exactly two lines");
        assert_eq!(l1["order_no"], "P-1");
        assert_eq!(l1["note"], poison, "poison value must roundtrip verbatim");
        assert_eq!(l1["qty"], 7);
        assert_eq!(l2["note"], serde_json::Value::Null);
        assert_eq!(l2["qty"], serde_json::Value::Null);
        Ok(())
    }

    #[test]
    fn doris_status_matrix_mock() -> anyhow::Result<()> {
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        let new_sink =
            |url: String| DorisSink::new(url, "Basic dGVzdDp0ZXN0", vec!["name".into()], "mtest");

        // ① Success / Publish Timeout / Label Already Exists all converge to success
        for body in [
            r#"{"Status":"Success"}"#.to_string(),
            r#"{"Status":"Publish Timeout"}"#.to_string(),
            r#"{"Status":"Fail","Message":"Label Already Exists. label: mtest_x"}"#.to_string(),
        ] {
            let m = MockDoris::start(vec![(200, body)]);
            let mut sink = new_sink(m.url());
            sink.write(vec![mk("a")?])?;
            assert_eq!(sink.flush()?.rows, 1, "must converge to success");
            assert_eq!(m.hits.load(Ord2::Relaxed), 1);
        }

        // ② 5xx retryable: after backoff the same buf resends successfully
        // (the two request bodies are byte-identical = the buffer survived)
        let m = MockDoris::start(vec![
            (500, r#"{"Status":"Fail"}"#.into()),
            (200, r#"{"Status":"Success"}"#.into()),
        ]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0, 0, 0];
        sink.write(vec![mk("b")?])?;
        assert_eq!(sink.flush()?.rows, 1);
        assert_eq!(m.hits.load(Ord2::Relaxed), 2);
        let bodies = m.bodies.lock().unwrap().clone();
        assert_eq!(bodies.len(), 2);
        assert_eq!(
            bodies[0], bodies[1],
            "the resend must carry the same buffer"
        );

        // ③ retries exhausted: attempts = 1+len(backoff); buf/pending kept
        // (retry can continue)
        let m = MockDoris::start(vec![
            (500, "{}".into()),
            (500, "{}".into()),
            (500, "{}".into()),
        ]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("c")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(err.contains("retries exhausted"), "{err}");
        assert_eq!(m.hits.load(Ord2::Relaxed), 3);
        assert_eq!(sink.pending_rows, 1, "pending kept on failure");
        assert!(!sink.buf.is_empty(), "buffer kept on failure");

        // ④ 401 auth failure: fatal, no retry
        let m = MockDoris::start(vec![(401, "denied".into())]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("d")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(err.contains("authentication failed"), "{err}");
        assert_eq!(m.hits.load(Ord2::Relaxed), 1);

        // ⑤ 307 redirect (FE misconfiguration): fatal + BE:8040 hint
        let m = MockDoris::start(vec![(307, r#"{"msg":"fe redirect"}"#.into())]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("e")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(err.contains("BE:8040"), "{err}");
        assert_eq!(m.hits.load(Ord2::Relaxed), 1);

        // ⑥a DATA_QUALITY + ErrorURL: the URL passes through untruncated
        // (truncation only applies to the reason text)
        let long_reason = format!(
            "{}{}",
            "[CANCELLED]cancelled: [DATA_QUALITY_ERROR]Encountered unqualified data, stop processing. Please check if the source data matches the schema, and consider disabling strict mode or increasing max_filter_ratio.. cur path: ".repeat(2),
            "x".repeat(120)
        );
        let url =
            "http://be-host:8040/api/_load_error_log?file=__shard_13/error_log_1234567890_abcdef";
        let m = MockDoris::start(vec![(
            200,
            format!(
                "{{\"Status\":\"Fail\",\"Reason\":\"{}\",\"ErrorURL\":\"{}\"}}",
                long_reason, url
            ),
        )]);
        let mut sink = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "mtest");
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("z")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(err.contains("| bad rows: "), "URL marker present");
        assert!(err.ends_with(url), "URL complete, not truncated: {err}");

        // ⑥ strict-mode filtered Fail: deterministic failure, no retry
        let m = MockDoris::start(vec![(
            200,
            r#"{"Status":"Fail","Reason":"Insert has filtered data in strict mode"}"#.into(),
        )]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("f")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(err.contains("not retryable"), "{err}");
        assert_eq!(m.hits.load(Ord2::Relaxed), 1);

        // ⑦ timeout-type Fail: retryable, second attempt succeeds
        let m = MockDoris::start(vec![
            (200, r#"{"Status":"Fail","Reason":"queue timeout"}"#.into()),
            (200, r#"{"Status":"Success"}"#.into()),
        ]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0];
        sink.write(vec![mk("g")?])?;
        assert!(sink.flush().is_ok());
        assert_eq!(m.hits.load(Ord2::Relaxed), 2);
        Ok(())
    }

    #[test]
    fn doris_sink_live_stream_load() -> anyhow::Result<()> {
        // Runs only against the tailrace dev Doris stack (docker-compose.yml);
        // the table must be pre-created by the local dev environment. The
        // gate checks the *documented* precondition (FE accepts the tailrace
        // dev credentials and serves the tailrace_spike table as JSON) — an
        // unrelated listener on 8040/8030 (another project's stack) must not
        // turn this into a spurious failure (same rationale as the runtime
        // live gate). Note: some FE builds answer unauthorized API paths
        // with 200 + the HTML web app, so success requires a JSON body.
        if std::net::TcpStream::connect("localhost:8040").is_err() {
            return Ok(()); // CI-safe
        }
        let probe = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()?;
        let fe_ok = probe
            .get("http://localhost:8030/api/tailrace_spike/ogg_event/schema")
            .header("Authorization", "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=")
            .send()
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.text())
            .map(|body| body.trim_start().starts_with('{'))
            .unwrap_or(false);
        if !fe_ok {
            tracing::warn!(
                "[live-gate] the FE on 8030 does not serve tailrace_spike.ogg_event with the \
                 tailrace dev credentials (another project's stack?) — skipping the live test"
            );
            return Ok(()); // local-liveness only
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_no", DataType::Utf8, false),
            Field::new("batch_no", DataType::Utf8, true),
            Field::new("station", DataType::Utf8, true),
            Field::new("op_type", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("etl_time_ms", DataType::Int64, false),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ORD-SINK1"])),
                Arc::new(StringArray::from(vec!["B-1"])),
                Arc::new(StringArray::from(vec!["S1"])),
                Arc::new(StringArray::from(vec!["I"])),
                Arc::new(Int64Array::from(vec![999])),
                Arc::new(StringArray::from(vec!["sink-live-test"])),
                Arc::new(Int64Array::from(vec![1726000000000])),
            ],
        )?;
        let mut sink = DorisSink::new(
            "http://localhost:8040/api/tailrace_spike/ogg_event/_stream_load",
            "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=",
            COLUMNS.iter().map(|s| s.to_string()).collect(),
            format!("e2esink{}", std::process::id()),
        );
        sink.write(vec![rb])?;
        let ack = sink.flush()?;
        assert_eq!(ack.kind, "DORIS_STREAM_LOAD");
        assert_eq!(ack.rows, 1);
        Ok(())
    }

    const COLUMNS: [&str; 7] = [
        "order_no",
        "batch_no",
        "station",
        "op_type",
        "seq",
        "payload",
        "etl_time_ms",
    ];
}
