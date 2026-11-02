//! tailrace-conn-doris: Doris / StarRocks sink (BE-direct stream load +
//! label idempotency + Status: Success judgment).
//!
//! reqwest blocking connects straight to the BE (an FE 307 redirect strips
//! the Authorization header — redirects are explicitly not followed).
//! Carrier is JSON lines (`format: json` + `read_json_by_line: true`):
//! apache/doris be-2.1.7's CSV splitter has no RFC4180 mode — `""` doubled
//! quotes are unparseable with every enclose/escape header combination
//! (T11 first-run matrix), while JSON escaping is total over arbitrary
//! UTF-8 (in-field newlines / quotes / emoji / backslashes).
//!
//! T12 StarRocks flavor (feature "starrocks", kind=starrocks): same Stream
//! Load protocol, three flavor-gated deltas (full matrix in roadmap §九
//! T12): (1) FE 307 redirects ARE followed — re-attaching Authorization,
//! curl `--location-trusted` semantics (StarRocks' canonical entry is the
//! FE http port 8030, which redirects every load to a BE/CN 8040; docs
//! "Load data from a local file system" + Flink connector network notes);
//! (2) no `read_json_by_line` header — current StarRocks has no such
//! parameter (0 hits in source + BE http_load_params.h), and parses JSON
//! document streams (ndjson) natively when `strip_outer_array` is false
//! (the default; be/src/formats/json/json_parser.h
//! `JsonDocumentStreamParser`), so the JSON-lines carrier rides unchanged;
//! (3) no `sequence_column` header — StarRocks declares sequence columns
//! at table level, not at load time.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;

use tailrace_catalog::{credential_from_env, option_keys, ReliabilityOptions, TableObject};
use tailrace_connector::registry::ConnectorRegistry;
use tailrace_connector::{Sink, SinkAck, SinkCapabilities};

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

/// Protocol flavor of the stream-load sink (T12). The "starrocks" kind
/// rides the same protocol with three flavor-gated deltas — FE redirect
/// following, no `read_json_by_line` header, no `sequence_column` header
/// (module doc above + the roadmap §九 T12 difference matrix).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkFlavor {
    Doris,
    StarRocks,
}

/// Range fingerprint of a backfill replay window (T-A2, fixes F1): the
/// first 8 hex chars of sha256 over the canonical big-endian encoding of
/// `(from, to)` — 8 hex chars = the first 4 digest bytes. The byte-level
/// encoding (not a decimal string) is pinned on purpose: the fingerprint is
/// a cross-version contract, and `"{from},{to}"` formatting would drift the
/// moment anyone touches the separator or the integer rendering.
pub fn range_fingerprint(from: i64, to: i64) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update((from as u64).to_be_bytes());
    h.update((to as u64).to_be_bytes());
    let d = h.finalize();
    let mut out = String::with_capacity(8);
    for b in &d[..4] {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Label idempotency mode (T-A2). `ProcessScoped` is the default and the
/// historical behavior: labels are unique per process run (`-r{pid}`), so a
/// restart replay gets fresh labels and Doris dedups nothing — fine for
/// at-least-once resident pipelines (the table key model absorbs replays),
/// wrong for backfills, where the SAME range must map to the SAME labels
/// across processes or a replay double-commits (finding F1).
///
/// A stop→deploy cycle rebuilds a resident pipeline's sink INSIDE the same
/// process: a fresh instance's ordinal would restart at `_0` and reuse the
/// previous instance's exact label, Doris answers "label already exists",
/// the sink maps that to Dedup — the first post-restart batch is silently
/// dropped while the ledger advances (found live by the R6 evolve-restart
/// e2e). [`LABEL_SEQ_FLOORS`] therefore makes the ordinal continue where
/// the previous same-prefix instance stopped; the label FORMAT is
/// unchanged.
///
/// The idempotency window of `Deterministic` is Doris' label retention
/// (default 3 days) — replays older than that fall back to the table key
/// model, documented with the backfill API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelMode {
    /// `{prefix}-r{pid}_{seq}` — unique per process run (the F1 defect
    /// site, retained for resident pipelines; backfills must not use it).
    /// The trailing `_{seq}` ordinal is process-global per prefix (see
    /// [`LABEL_SEQ_FLOORS`]) so a same-process rebuild cannot collide with
    /// its predecessor's labels.
    ProcessScoped,
    /// `{prefix}-{statement_index}-{fingerprint}_{seq}`: the fingerprint is
    /// [`range_fingerprint`] (sha256(from,to)[..8]). Cross-process stable:
    /// a restart replay of batch k reuses batch k's exact label, so Doris
    /// answers "label already exists" → `FlushOutcome::Dedup` and the row
    /// count stays unchanged.
    ///
    /// The trailing `_{seq}` batch ordinal is a deliberate extension of the
    /// blueprint's `{prefix}-{statement}-{rangeFingerprint}` formula: one
    /// label per RANGE would make the range's second flush (any range
    /// larger than `flush_rows` has one) Dedup against its first and
    /// silently drop that data. The ordinal keeps in-range flushes distinct
    /// while staying cross-process deterministic (batch k of a replayed
    /// range still gets ordinal k — this mode is exempt from the floor: a
    /// replayed range MUST restart its ordinals at k to collide).
    Deterministic {
        statement_index: u32,
        from: i64,
        to: i64,
    },
}

/// Ordinal floors of ProcessScoped label prefixes, process-global: the next
/// sink built for the same `{prefix}-r{pid}` continues after the highest
/// ordinal a previous instance of this process successfully flushed. Keyed
/// by the pid-suffixed prefix, so nothing crosses process boundaries.
static LABEL_SEQ_FLOORS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, u64>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// The ordinal a freshly built ProcessScoped sink starts from.
fn label_seq_floor(prefix: &str) -> u64 {
    LABEL_SEQ_FLOORS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(prefix)
        .copied()
        .unwrap_or(0)
}

/// Raise a prefix's floor to at least `seq` (never lowers — two concurrent
/// instances of one prefix interleave, the floor tracks the frontier).
fn raise_label_seq_floor(prefix: &str, seq: u64) {
    let mut floors = LABEL_SEQ_FLOORS.lock().unwrap_or_else(|e| e.into_inner());
    let floor = floors.entry(prefix.to_string()).or_insert(0);
    if seq > *floor {
        *floor = seq;
    }
}

/// Register the doris sink factory (plus the "starrocks" alias when that
/// feature is on — same Stream Load protocol, StarRocks deltas gated on
/// the table object's kind, see [`SinkFlavor`]).
pub fn register(reg: &mut ConnectorRegistry) {
    reg.register_sink("doris", sink_factory);
    #[cfg(feature = "starrocks")]
    reg.register_sink("starrocks", sink_factory);
}

/// Doris/StarRocks-side typed view of [`TableObject`] options (R8 G1): the
/// pre-G1 struct fields the sink factory read, restored as named accessors
/// over the options map. Also the accessor set the console's backfill
/// executor reads (the console links this crate).
pub trait DorisTableExt {
    /// Stream load endpoint (BE direct, full
    /// `http://be:8040/api/{db}/{table}/_stream_load` shape — the sink
    /// PUTs it verbatim; host-allowlisted at plan push, engine H1).
    fn stream_load_url(&self) -> Option<&str>;
    /// Environment variable name holding the Basic credential (value like
    /// "Basic xxx"; read just-in-time through `credential_from_env`,
    /// TAILRACE_-prefix-only).
    fn auth_env(&self) -> Option<&str>;
    /// Label prefix (idempotency anchor; absent = the catalog's
    /// `p{pipeline}` / `{datasource}` default).
    fn label_prefix(&self) -> Option<&str>;
    /// Doris UNIQUE KEY sequence column name (out-of-order dedup basis;
    /// None = not declared).
    fn sequence_column(&self) -> Option<&str>;
    /// Partial-column update declaration (A7): the subset of `columns`
    /// each stream load writes. Some(non-empty subset) puts the sink into
    /// `partial_columns=true` mode — Doris merges the written keys into
    /// existing UNIQUE KEY merge-on-write rows, unlisted columns keep
    /// their previous values (CDC upsert shape: ogg/debezium sources that
    /// only carry the changed columns). None = full-row writes, the
    /// historical default. Validated at sink assembly (subset ⊆ columns,
    /// sequence column inside the subset when declared).
    fn partial_columns(&self) -> Option<Vec<String>>;
}

impl DorisTableExt for TableObject {
    fn stream_load_url(&self) -> Option<&str> {
        self.option_str(option_keys::STREAM_LOAD_URL)
    }
    fn auth_env(&self) -> Option<&str> {
        self.option_str(option_keys::AUTH_ENV)
    }
    fn label_prefix(&self) -> Option<&str> {
        self.option_str(option_keys::LABEL_PREFIX)
    }
    fn sequence_column(&self) -> Option<&str> {
        self.option_str(option_keys::SEQUENCE_COLUMN)
    }
    fn partial_columns(&self) -> Option<Vec<String>> {
        self.option_str_list(option_keys::PARTIAL_COLUMNS)
    }
}

fn sink_factory(
    obj: &TableObject,
    rel: Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Sink>> {
    let auth_env = obj.auth_env().ok_or_else(|| {
        anyhow::anyhow!("doris sink must declare auth_env (credentials never enter files)")
    })?;
    let auth = credential_from_env(auth_env)?;
    let url = obj
        .stream_load_url()
        .ok_or_else(|| anyhow::anyhow!("doris sink must declare stream_load_url (BE direct)"))?;
    let columns = obj.columns.iter().map(|(n, _)| n.clone()).collect();
    let prefix = obj
        .label_prefix()
        .map(str::to_string)
        .unwrap_or_else(|| format!("p{}", obj.datasource));
    // the kind itself is the datasource-level knob selecting the flavor
    // (catalog normalize_kind keeps "starrocks" canonical)
    let flavor = if obj.kind == "starrocks" {
        SinkFlavor::StarRocks
    } else {
        SinkFlavor::Doris
    };
    // sequence column wiring + reliability knobs (retry backoff / request timeout)
    let mut sink = DorisSink::with_sequence(
        url,
        auth,
        columns,
        prefix,
        obj.sequence_column().map(str::to_string),
    )
    .with_flavor(flavor);
    // A7 plan-level partial-column declaration: a non-empty subset switches
    // the sink into partial_columns mode (validated inside the builder —
    // subset ⊆ table columns, sequence column included when declared).
    // Doris flavor only: StarRocks partial update is a different load
    // contract (primary-key tables + a `partial_update` header, T12
    // matrix) and is not wired — reject the plan rather than silently
    // write subset rows under full-replace semantics.
    if let Some(subset) = obj.partial_columns() {
        anyhow::ensure!(
            flavor != SinkFlavor::StarRocks,
            "partial_columns is Doris-only (kind={}); StarRocks partial update \
             rides a different load contract and is not wired",
            obj.kind
        );
        sink = sink.with_partial_columns(subset.clone())?;
    }
    if let Some(r) = rel {
        sink = sink
            .with_retry_backoff(r.flush_backoff_ms())
            .with_request_timeout(r.doris_timeout_ms());
    }
    Ok(Box::new(sink))
}

/// Redirect budget for the StarRocks flavor (FE → BE is one hop; chained
/// FE proxies are rare but tolerated up to this bound, then fatal).
const MAX_REDIRECT_HOPS: u32 = 3;

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

/// BE memory-pressure refusal, matched on the lowercased `Status Reason`
/// text. The patterns are the spellings Doris actually emits (live-observed
/// on 3.1.4 during dogfooding round3; the older `Memory limit exceeded`
/// status is kept for 2.x): the `MEM_LIMIT_EXCEEDED` status code, the
/// process/sys low-water-mark refusal, and the allocator's `Cannot alloc`.
/// Deliberately narrow — a wider net (e.g. bare `memory`) would also catch
/// deterministic schema/quality messages that happen to mention memory.
fn is_memory_pressure(lower: &str) -> bool {
    lower.contains("mem_limit_exceeded")
        || lower.contains("memory limit exceeded")
        || lower.contains("memory exceed limit")
        || lower.contains("low water mark")
        || lower.contains("cannot alloc")
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
    /// Protocol flavor (T12): StarRocks follows FE 307 redirects and omits
    /// the Doris-only load headers (read_json_by_line / sequence_column).
    flavor: SinkFlavor,
    url: String,
    auth: String,
    columns: Vec<String>,
    /// Sequence column name (optional — a Doris UNIQUE KEY table adds a
    /// sequence column to cancel out-of-order overwrites)
    sequence_column: Option<String>,
    /// Partial-column update mode (A7): when on, every stream load carries
    /// `partial_columns: true` and `columns` (above) holds the declared
    /// subset — the JSON carrier writes exactly those keys, and Doris
    /// merges them into existing UNIQUE KEY (merge-on-write) rows keeping
    /// the unlisted columns' old values. Off (the default) = full-row
    /// writes, byte-identical to the historical request shape.
    partial_update: bool,
    /// Raw label prefix as configured (no pid suffix) — the Deterministic
    /// mode builds labels on it (a pid in the prefix would break
    /// cross-process determinism).
    raw_prefix: String,
    /// Pid-suffixed label prefix (`{raw}-r{pid}`) — the ProcessScoped
    /// label base (historical behavior).
    label_prefix: String,
    /// Label idempotency mode (T-A2); default ProcessScoped = historical.
    label_mode: LabelMode,
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
        let raw_prefix = label_prefix.into();
        // label idempotency collision guard: unique per process run (Doris
        // labels are globally deduplicated — a rerun would be rejected); the
        // ordinal continues after any earlier same-process instance of this
        // prefix (a rebuilt sink must not reuse its predecessor's labels)
        let label_prefix = format!("{}-r{}", raw_prefix, std::process::id());
        Self {
            sequence_column,
            client: std::cell::OnceCell::new(),
            flavor: SinkFlavor::Doris,
            url: url.into(),
            auth: auth.into(),
            columns,
            partial_update: false,
            raw_prefix,
            label_seq: label_seq_floor(&label_prefix),
            label_prefix,
            label_mode: LabelMode::ProcessScoped,
            pending_rows: 0,
            buf: BytesMut::with_capacity(16 * 1024 * 1024),
            retry_backoff_ms: vec![2_000, 8_000, 30_000],
            request_timeout_ms: 120_000,
        }
    }

    /// Label mode override (T-A2): backfills pass
    /// `LabelMode::Deterministic { statement_index, from, to }` so the same
    /// replay range maps to the same labels across processes; the default
    /// (ProcessScoped) is byte-identical to the historical label format.
    pub fn with_label_mode(mut self, mode: LabelMode) -> Self {
        // Deterministic owns its ordinal outright (batch k of a replayed
        // range must be ordinal k, in every process) — never floored;
        // ProcessScoped (re)reads the process floor for its prefix.
        self.label_seq = match mode {
            LabelMode::ProcessScoped => label_seq_floor(&self.label_prefix),
            LabelMode::Deterministic { .. } => 0,
        };
        self.label_mode = mode;
        self
    }

    /// The next stream-load label under the active mode. Pure on purpose:
    /// `label_seq` advances only after a successful flush — a failed flush
    /// must retry the SAME label (the retry loop in `flush` reuses it).
    fn next_label(&self) -> String {
        match &self.label_mode {
            LabelMode::ProcessScoped => format!("{}_{}", self.label_prefix, self.label_seq),
            LabelMode::Deterministic {
                statement_index,
                from,
                to,
            } => format!(
                "{}-{}-{}_{}",
                self.raw_prefix,
                statement_index,
                range_fingerprint(*from, *to),
                self.label_seq
            ),
        }
    }

    /// Retry backoff sequence override (empty = the default [2s,8s,30s]).
    pub fn with_retry_backoff(mut self, backoff_ms: Vec<u64>) -> Self {
        if !backoff_ms.is_empty() {
            self.retry_backoff_ms = backoff_ms;
        }
        self
    }

    /// Protocol flavor override (T12): the factory derives it from the table
    /// object's kind; tests and embedders can pin it explicitly.
    pub fn with_flavor(mut self, flavor: SinkFlavor) -> Self {
        self.flavor = flavor;
        self
    }

    /// Partial-column update mode (A7): `subset` names the columns every
    /// stream load writes. Enabling (1) adds the `partial_columns: true`
    /// load header together with a `columns` header listing the subset
    /// (both are required — see the flush-side note; without `columns`
    /// Doris nulls the unlisted columns), and (2) narrows the carrier to
    /// the subset — the positional contract is unchanged (batch column i
    /// is emitted under `subset[i]`), so the plan's SELECT must project
    /// exactly the subset, in declaration order, same as the full-column
    /// contract. Doris merges the written keys into existing UNIQUE KEY
    /// merge-on-write rows; unlisted columns keep their previous values.
    ///
    /// Validated here (fail-fast at assembly, not mid-pipeline):
    /// - non-empty, duplicate-free (duplicates would emit colliding JSON
    ///   keys);
    /// - every name among the sink's declared columns;
    /// - with a sequence column declared, the subset must CONTAIN it. This
    ///   is an engine-side safety margin, deliberately stricter than the
    ///   server: doris 2.1.7 live-probed (W4) ACCEPTS a partial load whose
    ///   `columns` omit the sequence column and keeps the row's old
    ///   sequence value — but then the loaded row carries no ordering
    ///   evidence of its own, so an out-of-order CDC change would merge
    ///   unguarded. Requiring the value in every partial load keeps the
    ///   dedup ordering explicit.
    ///
    /// The key columns of the table must also be inside the subset for
    /// Doris to match rows — that is a table-schema property the engine
    /// cannot see; a violating plan fails with Doris' own load error
    /// (locked in the W4 e2e, not here).
    ///
    /// Doris flavor only: the factory rejects starrocks + partial_columns
    /// (StarRocks partial update is a different contract — primary-key
    /// tables + a `partial_update` header — not wired; T12 matrix).
    pub fn with_partial_columns(mut self, subset: Vec<String>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !subset.is_empty(),
            "doris partial_update: the column subset is empty — declare the \
             columns to write, or drop partial_columns for full-row writes"
        );
        let mut seen = std::collections::HashSet::new();
        for c in &subset {
            anyhow::ensure!(
                seen.insert(c.as_str()),
                "doris partial_update: duplicate column {c} in the declared subset"
            );
            anyhow::ensure!(
                self.columns.iter().any(|n| n == c),
                "doris partial_update: column {c} is not among the sink's declared \
                 columns ({})",
                self.columns.join(",")
            );
        }
        if let Some(seq) = &self.sequence_column {
            anyhow::ensure!(
                subset.iter().any(|c| c == seq),
                "doris partial_update: sequence column {seq} must be inside the \
                 partial subset — every partial load must carry its own sequence \
                 value so the merge ordering stays explicit (engine-side rule; \
                 add {seq} to the INSERT column list)"
            );
        }
        self.columns = subset;
        self.partial_update = true;
        Ok(self)
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

    /// Ack label: honest about which engine committed the rows (a purely
    /// diagnostic field — nothing downstream pattern-matches on it).
    fn ack_kind(&self) -> &'static str {
        match self.flavor {
            SinkFlavor::Doris => "DORIS_STREAM_LOAD",
            SinkFlavor::StarRocks => "STARROCKS_STREAM_LOAD",
        }
    }

    /// One send attempt with terminal-outcome judgment. The body is a
    /// refcounted `Bytes` — the reqwest clone is free, and on failure the
    /// shared buffer stays intact for the retry (a failure never drops the
    /// buffered rows). StarRocks flavor: FE redirects are followed with the
    /// Authorization header re-attached (curl `--location-trusted`
    /// semantics — a plain reqwest redirect policy silently drops the
    /// credential cross-host).
    fn send_once(&self, label: &str, body: &Bytes) -> Result<FlushOutcome, FlushError> {
        let brief = |t: &str| -> String { t.chars().take(300).collect() };
        let mut url = self.url.clone();
        let mut hops = 0u32;
        loop {
            let mut req = self
                .client()
                .map_err(|e| FlushError::fatal(e.to_string()))?
                .put(&url)
                .header("Authorization", &self.auth)
                .header("label", label)
                // JSON-lines carrier (one object per line): both engines
                // map by key name; serde_json escaping carries any UTF-8
                // payload verbatim. The old CSV path sent enclose/escape
                // headers and be-2.1.7 could not parse RFC4180 doubled
                // quotes under any combination of them (T11 first-run
                // matrix) — quoted/comma'd fields died.
                .header("format", "json");
            if self.flavor == SinkFlavor::Doris {
                req = req
                    // Doris: per-line JSON objects are opt-in via this
                    // header. StarRocks has no such parameter — the
                    // document-stream (ndjson) parser is already the
                    // default when strip_outer_array=false (module doc,
                    // T12 difference matrix).
                    .header("read_json_by_line", "true")
                    // Doris sequence column declaration (the out-of-order
                    // dedup basis for UNIQUE KEY tables). StarRocks
                    // declares sequence columns at table level instead —
                    // no load-time header exists there.
                    .header(
                        "sequence_column",
                        self.sequence_column.as_deref().unwrap_or(""),
                    );
                // A7 partial-column update: `partial_columns: true` PLUS an
                // explicit `columns` header naming the subset. The header is
                // not optional — live-probed on doris 2.1.7 (W4 e2e, raw
                // stream loads): with format=json and NO `columns` header
                // Doris resolves the written set to EVERY table column, the
                // keys absent from the JSON become NULL, and the "partial"
                // load silently nulls the unlisted columns of a UNIQUE KEY
                // MoW row (Status Success, no warning). With `columns`
                // listing the subset the unlisted columns keep their old
                // values; extra JSON keys outside the list are ignored. The
                // W2 mock pinned the header-less shape and passed — the
                // request shape is only as right as the live run says.
                // `self.columns` is the subset once `with_partial_columns`
                // ran. Doris-only: the factory rejects starrocks +
                // partial_columns up front.
                if self.partial_update {
                    req = req
                        .header("partial_columns", "true")
                        .header("columns", self.columns.join(","));
                }
            }
            let resp = req
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
            if status.is_redirection() {
                let location = resp
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let text = resp.text().unwrap_or_default();
                // StarRocks: following the FE's redirect IS the protocol —
                // the FE http port (8030) forwards every load request to a
                // BE/CN webserver (8040) via redirect (docs "Load data from
                // a local file system"; Flink connector network notes).
                // The Authorization header must be re-attached by hand:
                // clients strip it cross-host on redirect.
                if self.flavor == SinkFlavor::StarRocks {
                    if let Some(loc) = &location {
                        if (loc.starts_with("http://") || loc.starts_with("https://"))
                            && hops < MAX_REDIRECT_HOPS
                        {
                            tracing::debug!(from = %url, to = %loc, hops, "starrocks fe redirect followed");
                            url = loc.clone();
                            hops += 1;
                            continue;
                        }
                    }
                    return Err(FlushError::fatal(format!(
                        "HTTP {} redirect chain not followable (hops={hops}): Location={location:?} {} — check the StarRocks FE http_port/be_http_port wiring",
                        status.as_u16(),
                        brief(&text)
                    )));
                }
                return Err(FlushError::fatal(format!(
                    "HTTP {} redirect — stream_load_url points at an FE (which strips the Authorization header); use a BE:8040 direct URL: {}",
                    status.as_u16(),
                    brief(&text)
                )));
            }
            let text = resp.text().unwrap_or_default();
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
            // BE memory pressure (R10, dogfooding round3 #2): the BE refuses
            // the load because IT is short of memory, not because the rows
            // are bad — a transient resource fault on the same footing as a
            // timeout. It used to fall through to the fatal branch below:
            // fatal → statement error → restart loop → three restarts without
            // cursor progress → the poison-data verdict killed a healthy
            // pipeline during a 20-40 minute memory window. Classified before
            // the generic patterns so the verdict is explicit and pinned.
            if is_memory_pressure(&lower) {
                return Err(FlushError::retry(format!(
                    "BE memory pressure (transient): Status={st} Reason={reason}"
                )));
            }
            if lower.contains("timeout") || lower.contains("too many") || status.is_server_error() {
                return Err(FlushError::retry(format!("Status={st} Reason={reason}")));
            }
            // Other Fail outcomes (strict-mode filtering, schema mismatch, syntax
            // errors) — deterministic failures, retrying is pointless
            return Err(FlushError::fatal(format!("Status={st} Reason={reason}")));
        }
    }
}

impl Sink for DorisSink {
    /// G4 declaration. idempotent_label=true: every flush carries a stream
    /// load label Doris deduplicates globally, so a replayed flush after a
    /// restart converges ("label already exists" is success, not a
    /// duplicate write). partial_update mirrors the A7 mode: true only when
    /// `with_partial_columns` narrowed the carrier to a declared subset
    /// (default construction stays false — full-row writes). Everything
    /// else is honestly false today: the prepare/commit/abort hooks stay
    /// no-op (stream-load 2PC is T36③), there is no whole-table replace
    /// mode, and no sink-side partition routing (Doris distributes rows
    /// server-side).
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent_label: true,
            transactional: false,
            overwrite: false,
            partial_update: self.partial_update,
            partition_aware: false,
        }
    }

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
                kind: self.ack_kind(),
                idempotency_key: format!("{}.noop", self.label_prefix),
            });
        }
        let label = self.next_label();
        // (T3) freeze the accumulated carrier into a refcounted body: every
        // retry attempt clones the handle (refcount), not the bytes.
        let body = std::mem::take(&mut self.buf).freeze();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.send_once(&label, &body) {
                Ok(_) => {
                    self.label_seq += 1;
                    if self.label_mode == LabelMode::ProcessScoped {
                        // a same-process rebuild of this prefix must start
                        // AFTER this flush's ordinal, or its first label
                        // would collide (Dedup, data silently dropped)
                        raise_label_seq_floor(&self.label_prefix, self.label_seq);
                    }
                    let rows = self.pending_rows;
                    self.pending_rows = 0;
                    // restore the buffer watermark (keep capacity to avoid
                    // regrowing per batch)
                    self.buf = BytesMut::with_capacity(16 * 1024 * 1024);
                    return Ok(SinkAck {
                        rows,
                        kind: self.ack_kind(),
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

    /// Minimal Doris/StarRocks mock (std TcpListener — zero new deps):
    /// responds to PUTs in scripted order, counting hits and keeping
    /// bodies (plus the raw request head, for header-contract asserts).
    struct MockDoris {
        addr: std::net::SocketAddr,
        hits: Arc<std::sync::atomic::AtomicUsize>,
        bodies: Arc<Mutex<Vec<Vec<u8>>>>,
        heads: Arc<Mutex<Vec<String>>>,
    }

    fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len()).position(|w| w == needle)
    }

    impl MockDoris {
        /// script: respond in order with (HTTP status, body); requests beyond
        /// the script block on accept (tests assert on hits to intercept)
        fn start(script: Vec<(u16, String)>) -> Self {
            Self::start_with_redirect(script, None)
        }

        /// Same, but 307 responses carry the given Location instead of the
        /// historical `http://127.0.0.1:1/fe` (the StarRocks FE-redirect
        /// tests point it at a second mock BE).
        fn start_with_redirect(script: Vec<(u16, String)>, location: Option<String>) -> Self {
            use std::io::{Read, Write};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let bodies = Arc::new(Mutex::new(Vec::new()));
            let heads = Arc::new(Mutex::new(Vec::new()));
            let (h2, b2, hd2) = (Arc::clone(&hits), Arc::clone(&bodies), Arc::clone(&heads));
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
                    hd2.lock().unwrap().push(head.clone());
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
                    let loc = location
                        .clone()
                        .unwrap_or_else(|| "http://127.0.0.1:1/fe".to_string());
                    let extra = if code == 307 {
                        format!("Location: {loc}\r\n")
                    } else {
                        String::new()
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
            Self {
                addr,
                hits,
                bodies,
                heads,
            }
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

        // ⑧ BE memory pressure (R10, dogfooding round3 #2): the live 3.1.4
        // refusal text is retryable — the same buffer is resent and the
        // window passing turns into success, no statement error, no
        // restart, no poison count
        let mem_limit = r#"{"Status":"Fail","Message":"[MEM_LIMIT_EXCEEDED]PreCatch error code:11, [E11] Allocator sys memory check failed: Cannot alloc:1048576, consuming tracker:<Load#Id=...>, process memory used 1.23 GB exceed limit 6.40 GB or sys available memory 350.00 MB less than low water mark 400.00 MB"}"#;
        let m = MockDoris::start(vec![
            (200, mem_limit.into()),
            (200, r#"{"Status":"Success"}"#.into()),
        ]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("h")?])?;
        assert_eq!(sink.flush()?.rows, 1, "memory window passed → success");
        assert_eq!(m.hits.load(Ord2::Relaxed), 2, "exactly one retry");
        let bodies = m.bodies.lock().unwrap().clone();
        assert_eq!(bodies[0], bodies[1], "the resend carries the same buffer");

        // ⑧b a window outliving the backoff budget: the error is the
        // retryable-exhausted shape (NOT "not retryable"), buffer kept so
        // the statement-level restart replays it
        let m = MockDoris::start(vec![
            (200, mem_limit.into()),
            (200, mem_limit.into()),
            (200, mem_limit.into()),
        ]);
        let mut sink = new_sink(m.url());
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("i")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(err.contains("retries exhausted"), "{err}");
        assert!(!err.contains("not retryable"), "{err}");
        assert!(err.contains("BE memory pressure"), "{err}");
        assert_eq!(m.hits.load(Ord2::Relaxed), 3);
        assert_eq!(sink.pending_rows, 1, "pending kept");
        assert!(!sink.buf.is_empty(), "buffer kept");
        Ok(())
    }

    /// R10 (dogfooding round3 #2): the memory-pressure predicate pins the
    /// Doris spellings it accepts and — as important — the deterministic
    /// failures it must NOT absorb (those stay fatal = the poison-data path
    /// of ⑥ above is unchanged).
    #[test]
    fn memory_pressure_classification_pins_doris_spellings() {
        for s in [
            // live 3.1.4 dogfooding round3 text (status code + low water mark)
            "fail [mem_limit_exceeded]precatch error code:11, [e11] allocator sys memory check failed: cannot alloc:1048576, sys available memory 350.00 mb less than low water mark 400.00 mb",
            // status code alone
            "fail [mem_limit_exceeded] query memory exceeded",
            // 2.x status spelling
            "memory limit exceeded",
            "fail memory exceed limit: load",
            // allocator refusal without the status code
            "fail cannot alloc: 65536",
        ] {
            assert!(is_memory_pressure(s), "must be transient: {s}");
        }
        for s in [
            // ⑥'s strict-mode filter — deterministic
            "fail insert has filtered data in strict mode",
            "fail [data_quality_error]encountered unqualified data, stop processing",
            "fail column count mismatch",
            "fail [internal_error]label already exists",
            // mentions memory without being a pressure refusal
            "fail column memory_note is not nullable",
            "fail [analysis_error]unknown column 'mem_usage'",
            "",
        ] {
            assert!(!is_memory_pressure(s), "must stay deterministic: {s}");
        }
    }

    /// T-A2 regression anchor: the default (ProcessScoped) mode must keep
    /// the historical label format byte-identical — `{prefix}-r{pid}_{seq}`
    /// (the `with_sequence` anchor) with the seq advancing per SUCCESSFUL
    /// flush; a failed-then-retried flush reuses the same label.
    #[test]
    fn process_scoped_labels_pin_the_historical_format() -> anyhow::Result<()> {
        let pid = std::process::id();
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        // ① constructor: stored prefix is exactly `{prefix}-r{pid}` and the
        // first label is `{prefix}-r{pid}_0`; the default mode is
        // ProcessScoped (new/with_sequence semantics unchanged)
        let sink = DorisSink::new("http://127.0.0.1:1/", "auth", vec!["name".into()], "mreg");
        assert_eq!(sink.label_prefix, format!("mreg-r{pid}"));
        assert_eq!(sink.next_label(), format!("mreg-r{pid}_0"));
        assert_eq!(sink.label_mode, LabelMode::ProcessScoped);

        // ② the seq advances per successful flush: _0 then _1
        let m = MockDoris::start(vec![
            (200, r#"{"Status":"Success"}"#.into()),
            (200, r#"{"Status":"Success"}"#.into()),
        ]);
        let mut a = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "mseq");
        a.retry_backoff_ms = vec![0, 0];
        a.write(vec![mk("x")?])?;
        assert_eq!(a.flush()?.idempotency_key, format!("mseq-r{pid}_0"));
        a.write(vec![mk("y")?])?;
        assert_eq!(a.flush()?.idempotency_key, format!("mseq-r{pid}_1"));

        // ③ a retried flush reuses the SAME label (500 → 200): both
        // requests carry the identical label line
        let m = MockDoris::start(vec![
            (500, r#"{"Status":"Fail"}"#.into()),
            (200, r#"{"Status":"Success"}"#.into()),
        ]);
        let mut b = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "mretry");
        b.retry_backoff_ms = vec![0];
        b.write(vec![mk("z")?])?;
        assert_eq!(b.flush()?.idempotency_key, format!("mretry-r{pid}_0"));
        let heads = m.heads.lock().unwrap().clone();
        let labels: Vec<&str> = heads
            .iter()
            .map(|h| {
                h.lines()
                    .find(|l| l.starts_with("label:"))
                    .expect("label header present")
            })
            .collect();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], labels[1], "a retry must reuse the same label");
        Ok(())
    }

    /// Same-process rebuild (stop→deploy rebuilds the sink; the process —
    /// and its pid — stays): the fresh instance's first label must continue
    /// after the previous instance's last flushed ordinal, not restart at
    /// `_0` (which Doris would answer "label already exists" → Dedup → the
    /// post-restart batch silently dropped while the ledger advances —
    /// found live by the R6 evolve-restart e2e). Deterministic mode is
    /// exempt: a replayed range MUST restart its ordinals to collide.
    #[test]
    fn process_scoped_ordinal_continues_across_same_process_rebuild() -> anyhow::Result<()> {
        let pid = std::process::id();
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        // instance #1 flushes twice: ordinals _0 and _1
        let m = MockDoris::start(vec![
            (200, r#"{"Status":"Success"}"#.into()),
            (200, r#"{"Status":"Success"}"#.into()),
            (200, r#"{"Status":"Success"}"#.into()),
        ]);
        let mut first =
            DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "mfloor");
        first.retry_backoff_ms = vec![0];
        first.write(vec![mk("a")?])?;
        assert_eq!(first.flush()?.idempotency_key, format!("mfloor-r{pid}_0"));
        first.write(vec![mk("b")?])?;
        assert_eq!(first.flush()?.idempotency_key, format!("mfloor-r{pid}_1"));

        // the rebuild: same prefix, same process → continues at _2
        let mut second =
            DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "mfloor");
        second.retry_backoff_ms = vec![0];
        assert_eq!(second.next_label(), format!("mfloor-r{pid}_2"));
        second.write(vec![mk("c")?])?;
        assert_eq!(second.flush()?.idempotency_key, format!("mfloor-r{pid}_2"));

        // a sink whose mode is switched to Deterministic is NOT floored: a
        // replayed range restarts its ordinals (that is the dedup contract)
        let mut det = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "mfloor")
            .with_label_mode(LabelMode::Deterministic {
                statement_index: 0,
                from: 0,
                to: 9,
            });
        det.retry_backoff_ms = vec![0];
        assert!(det.next_label().ends_with("_0"), "{}", det.next_label());
        Ok(())
    }

    /// T-A2 deterministic labels: the same statement + range generates
    /// byte-identical labels across independently built sinks (a restart
    /// replay reuses them — cross-process stability is proxied here by the
    /// pid never entering the label); different ranges or statements
    /// diverge; the fingerprint itself is pinned to precomputed vectors
    /// (algorithm-drift guard).
    #[test]
    fn deterministic_labels_are_range_stable() {
        let det = |statement_index: u32, from: i64, to: i64| LabelMode::Deterministic {
            statement_index,
            from,
            to,
        };
        // fingerprint vectors: sha256 over the big-endian bytes of (from,to),
        // first 8 hex chars
        assert_eq!(range_fingerprint(100, 200), "13f92842");
        assert_eq!(range_fingerprint(100, 300), "67bc82bc");
        assert_eq!(range_fingerprint(0, 4294967296), "3bbf4dc5");
        assert_eq!(range_fingerprint(-5, 7).len(), 8);

        let base = || DorisSink::new("http://127.0.0.1:1/", "auth", vec!["name".into()], "tr-p7");
        let a = base().with_label_mode(det(0, 100, 200));
        let b = base().with_label_mode(det(0, 100, 200));
        // same statement + range → byte-identical labels, pinned in full
        assert_eq!(a.next_label(), b.next_label());
        assert_eq!(a.next_label(), "tr-p7-0-13f92842_0");
        // a different range (or statement) diverges
        let c = base().with_label_mode(det(0, 100, 300));
        assert_ne!(a.next_label(), c.next_label());
        let d = base().with_label_mode(det(1, 100, 200));
        assert_ne!(a.next_label(), d.next_label());
        // the pid must not leak into deterministic labels
        assert!(!a
            .next_label()
            .contains(&format!("-r{}", std::process::id())));
        // next_label is pure: the seq advances only on a successful flush
        assert_eq!(a.next_label(), a.next_label());
    }

    /// T-A2 through the mock BE: a Deterministic replay of the same range
    /// hits "label already exists" on the committed label and converges via
    /// the Dedup path (send_once → FlushOutcome::Dedup) — no double commit.
    /// In-range batch ordinals stay distinct: flush #1's label is never
    /// reused by flush #2 (one-label-per-range would Dedup real data away).
    #[test]
    fn deterministic_replay_converges_and_batches_stay_distinct() -> anyhow::Result<()> {
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        let det = |from: i64, to: i64| LabelMode::Deterministic {
            statement_index: 0,
            from,
            to,
        };
        // run 1 commits two in-range batches; the replay of batch 0 answers
        // "label already exists" → must converge to success
        let m = MockDoris::start(vec![
            (200, r#"{"Status":"Success"}"#.into()),
            (200, r#"{"Status":"Success"}"#.into()),
            (
                200,
                r#"{"Status":"Fail","Message":"Label Already Exists. label: bf7"}"#.into(),
            ),
        ]);
        let mut run1 = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "bf7")
            .with_label_mode(det(100, 200));
        run1.retry_backoff_ms = vec![0, 0];
        run1.write(vec![mk("r1a")?])?;
        let ack0 = run1.flush()?;
        run1.write(vec![mk("r1b")?])?;
        assert_eq!(run1.flush()?.rows, 1);
        // restart replay (a fresh sink instance, same statement + range):
        // batch 0's label is already committed → Dedup, ack key identical
        let mut run2 = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "bf7")
            .with_label_mode(det(100, 200));
        run2.retry_backoff_ms = vec![0, 0];
        run2.write(vec![mk("r1a")?])?;
        let replay = run2.flush()?;
        assert_eq!(replay.idempotency_key, ack0.idempotency_key);
        // the BE saw: batch0, batch1, batch0(replay) — the replay reuses
        // batch 0's exact label while batch 1 stays distinct
        let heads = m.heads.lock().unwrap().clone();
        let labels: Vec<&str> = heads
            .iter()
            .map(|h| {
                h.lines()
                    .find(|l| l.starts_with("label:"))
                    .expect("label header present")
            })
            .collect();
        assert_eq!(labels.len(), 3);
        assert_eq!(labels[0], labels[2], "the replay reuses batch 0's label");
        assert_ne!(
            labels[0], labels[1],
            "in-range batches keep distinct labels"
        );
        Ok(())
    }

    /// A7 partial-column update, both states through the mock BE (the
    /// W4 e2e adds the live MoW "unwritten column keeps its old value"
    /// assertion; here the contract is the request shape):
    /// - off (default): NO partial_columns header, carrier carries every
    ///   declared column — byte-shape identical to the pre-A7 request;
    /// - on: `partial_columns: true` header rides, carrier carries exactly
    ///   the declared subset, capabilities().partial_update flips.
    #[test]
    fn partial_update_two_state_contract_mock() -> anyhow::Result<()> {
        let mk = |cols: &[&str]| -> anyhow::Result<RecordBatch> {
            let arrays: Vec<Arc<dyn Array>> = cols
                .iter()
                .map(|c| Arc::new(StringArray::from(vec![format!("v-{c}")])) as Arc<dyn Array>)
                .collect();
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(
                    cols.iter()
                        .map(|c| Field::new(*c, DataType::Utf8, true))
                        .collect::<Vec<_>>(),
                )),
                arrays,
            )?)
        };
        let full = vec!["k".to_string(), "a".to_string(), "b".to_string()];

        // —— off (default): the regression anchor ——
        let m = MockDoris::start(vec![(200, r#"{"Status":"Success"}"#.into())]);
        let mut sink = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", full.clone(), "puoff");
        assert!(!sink.capabilities().partial_update);
        sink.write(vec![mk(&["k", "a", "b"])?])?;
        assert_eq!(sink.flush()?.rows, 1);
        let head = m.heads.lock().unwrap()[0].clone();
        assert!(
            !head.contains("partial_columns"),
            "default must not send partial_columns: {head}"
        );
        // no `columns` header either: the full-row request shape is the
        // historical one, byte-for-byte (`\n`-anchored so the
        // `sequence_column` header line cannot satisfy the match)
        assert!(
            !head.contains("\ncolumns:"),
            "default must not send a columns header: {head}"
        );
        let body = m.bodies.lock().unwrap()[0].clone();
        let body = std::str::from_utf8(&body)?;
        let v: serde_json::Value = serde_json::from_str(body.lines().next().expect("1 line"))?;
        assert_eq!(v["k"], "v-k");
        assert_eq!(v["a"], "v-a");
        assert_eq!(v["b"], "v-b", "full-row carrier: every column rides");

        // —— on: both headers + subset carrier + capability flip ——
        let m = MockDoris::start(vec![(200, r#"{"Status":"Success"}"#.into())]);
        let mut sink = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", full, "puon")
            .with_partial_columns(vec!["k".into(), "a".into()])?;
        assert!(sink.capabilities().partial_update);
        // positional contract unchanged: the batch projects the subset, in
        // declaration order — batch column i is emitted under subset[i]
        sink.write(vec![mk(&["k", "a"])?])?;
        assert_eq!(sink.flush()?.rows, 1);
        let head = m.heads.lock().unwrap()[0].clone();
        assert!(
            head.contains("partial_columns: true"),
            "partial mode must send the header: {head}"
        );
        // the explicit column list is what makes Doris treat the load as a
        // subset write (live-probed on 2.1.7: without it the unlisted
        // columns are nulled) — the header names the subset, in order
        assert!(
            head.contains("\ncolumns: k,a\r\n"),
            "partial mode must send the columns header naming the subset: {head}"
        );
        // the other Doris load headers stay (format/read_json_by_line ride
        // unchanged — partial is a two-header delta on the same carrier)
        assert!(head.contains("read_json_by_line: true"));
        assert!(head.contains("format: json"));
        let body = m.bodies.lock().unwrap()[0].clone();
        let body = std::str::from_utf8(&body)?;
        let v: serde_json::Value = serde_json::from_str(body.lines().next().expect("1 line"))?;
        assert_eq!(v["k"], "v-k");
        assert_eq!(v["a"], "v-a");
        assert!(
            v.get("b").is_none(),
            "the subset carrier must not emit column b"
        );
        Ok(())
    }

    /// A7 × sequence column: the subset must carry the declared sequence
    /// column — an engine-side safety margin (doris 2.1.7 live-probed in W4
    /// accepts a partial load without it and keeps the old sequence value,
    /// which leaves the loaded change without ordering evidence of its own;
    /// the engine refuses the shape at assembly so the gap surfaces at
    /// deploy). When the subset DOES carry it, the request locks the three
    /// headers riding together: sequence_column, partial_columns and the
    /// `columns` list that includes the sequence column.
    #[test]
    fn partial_update_with_sequence_column_combination() -> anyhow::Result<()> {
        let mk = |cols: &[(&str, DataType)]| -> anyhow::Result<RecordBatch> {
            // col 0 rides a string value, col 1 the int64 sequence value 42
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(
                    cols.iter()
                        .map(|(n, t)| Field::new(*n, t.clone(), true))
                        .collect::<Vec<_>>(),
                )),
                vec![
                    Arc::new(StringArray::from(vec![format!("v-{}", cols[0].0)])),
                    Arc::new(Int64Array::from(vec![Some(42)])),
                ],
            )?)
        };
        // ① sequence column OUTSIDE the subset → assembly-time rejection
        //    (engine safety margin, see the test doc)
        let err = DorisSink::with_sequence(
            "http://127.0.0.1:1/",
            "auth",
            vec!["k".into(), "seq".into(), "note".into()],
            "puseq",
            Some("seq".into()),
        )
        .with_partial_columns(vec!["k".into(), "note".into()])
        .map(|_| ())
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("sequence column seq must be inside the partial subset"),
            "{err}"
        );

        // ② sequence column INSIDE the subset → the three headers ride
        //    together, the sequence value is in the carrier
        let m = MockDoris::start(vec![(200, r#"{"Status":"Success"}"#.into())]);
        let mut sink = DorisSink::with_sequence(
            m.url(),
            "Basic dGVzdDp0ZXN0",
            vec!["k".into(), "seq".into(), "note".into()],
            "puseq2",
            Some("seq".into()),
        )
        .with_partial_columns(vec!["k".into(), "seq".into()])?;
        sink.write(vec![mk(&[
            ("k", DataType::Utf8),
            ("seq", DataType::Int64),
        ])?])?;
        assert_eq!(sink.flush()?.rows, 1);
        let head = m.heads.lock().unwrap()[0].clone();
        assert!(head.contains("sequence_column: seq"), "{head}");
        assert!(head.contains("partial_columns: true"), "{head}");
        assert!(head.contains("\ncolumns: k,seq\r\n"), "{head}");
        let body = m.bodies.lock().unwrap()[0].clone();
        let body = std::str::from_utf8(&body)?;
        let v: serde_json::Value = serde_json::from_str(body.lines().next().expect("1 line"))?;
        assert_eq!(v["seq"], 42, "the sequence value rides the subset carrier");
        assert!(v.get("note").is_none());
        Ok(())
    }

    /// A7 builder validation: the subset must be non-empty, duplicate-free,
    /// and within the sink's declared columns (assembly fail-fast).
    #[test]
    fn partial_update_builder_validates_the_subset() {
        let full = || vec!["k".to_string(), "a".to_string(), "b".to_string()];
        let base = || DorisSink::new("http://127.0.0.1:1/", "auth", full(), "puval");
        assert!(base()
            .with_partial_columns(vec![])
            .map(|_| ())
            .unwrap_err()
            .to_string()
            .contains("subset is empty"));
        assert!(base()
            .with_partial_columns(vec!["k".into(), "k".into()])
            .map(|_| ())
            .unwrap_err()
            .to_string()
            .contains("duplicate column k"));
        assert!(base()
            .with_partial_columns(vec!["k".into(), "nope".into()])
            .map(|_| ())
            .unwrap_err()
            .to_string()
            .contains("nope is not among"));
        // the full set as the subset is shape-legal (a no-op partial over
        // every column) — Doris accepts it, semantics identical to a full
        // write plus the MoW merge path
        assert!(base().with_partial_columns(full()).is_ok());
    }

    /// A7 factory wiring: a TableObject declaring partial_columns builds a
    /// sink in partial mode (header verified through the mock BE); a
    /// starrocks-kind object declaring it is rejected up front (its
    /// partial-update is a different load contract — T12 matrix — reject
    /// rather than silently write subset rows under full-replace
    /// semantics). Credentials via env: factory reads auth_env.
    #[test]
    fn sink_factory_wires_partial_columns_from_the_table_object() -> anyhow::Result<()> {
        std::env::set_var("TAILRACE_TEST_AUTH", "Basic dGVzdDp0ZXN0");
        let obj = |kind: &str, partial: Option<Vec<String>>| {
            let o = TableObject::new("db", "t", kind)
                .with_columns(vec![
                    ("k".into(), "utf8".into()),
                    ("a".into(), "utf8".into()),
                    ("b".into(), "utf8".into()),
                ])
                .with_option(
                    option_keys::STREAM_LOAD_URL,
                    "http://127.0.0.1:1/api/db/t/_stream_load",
                )
                .with_option(option_keys::AUTH_ENV, "TAILRACE_TEST_AUTH");
            match partial {
                Some(p) => o.with_option(option_keys::PARTIAL_COLUMNS, serde_json::json!(p)),
                None => o,
            }
        };
        // declared subset → partial mode with the narrowed carrier
        let m = MockDoris::start(vec![(200, r#"{"Status":"Success"}"#.into())]);
        let partial_obj = obj("doris", Some(vec!["k".into(), "a".into()]))
            .with_option(option_keys::STREAM_LOAD_URL, m.url());
        let mut sink = sink_factory(&partial_obj, None)?;
        assert!(sink.capabilities().partial_update);
        let rb = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, true),
                Field::new("a", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["K1"])),
                Arc::new(StringArray::from(vec!["A1"])),
            ],
        )?;
        sink.write(vec![rb])?;
        assert_eq!(sink.flush()?.rows, 1);
        let head = m.heads.lock().unwrap()[0].clone();
        assert!(
            head.contains("partial_columns: true"),
            "the factory-declared subset rides the load header: {head}"
        );
        assert!(
            head.contains("\ncolumns: k,a\r\n"),
            "the factory-declared subset rides the columns header: {head}"
        );

        // no declaration → the historical full-row sink
        let sink = sink_factory(&obj("doris", None), None)?;
        assert!(!sink.capabilities().partial_update);

        // starrocks + partial_columns → rejected at assembly
        let err = sink_factory(&obj("starrocks", Some(vec!["k".into()])), None)
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(err.contains("Doris-only"), "{err}");
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

    /// T12 StarRocks flavor contract (mock FE/BE): FE 307 redirect is
    /// followed with the Authorization header re-attached (curl
    /// --location-trusted semantics), the Doris-only load headers are
    /// absent, and the JSON-lines carrier rides unchanged.
    #[cfg(feature = "starrocks")]
    #[test]
    fn starrocks_flavor_contract_mock() -> anyhow::Result<()> {
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };

        // ① FE 307 → BE: exactly one request each; the BE request carries
        // Authorization + label + the same carrier bytes; ack kind is
        // StarRocks-honest
        let be = MockDoris::start(vec![(200, r#"{"Status":"Success"}"#.into())]);
        let fe = MockDoris::start_with_redirect(vec![(307, "{}".into())], Some(be.url()));
        let mut sink = DorisSink::new(fe.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "srfe")
            .with_flavor(SinkFlavor::StarRocks);
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("a")?])?;
        let ack = sink.flush()?;
        assert_eq!(ack.rows, 1);
        assert_eq!(ack.kind, "STARROCKS_STREAM_LOAD");
        assert_eq!(fe.hits.load(Ord2::Relaxed), 1, "one FE request");
        assert_eq!(be.hits.load(Ord2::Relaxed), 1, "one BE request");
        let fe_head = fe.heads.lock().unwrap()[0].clone();
        let be_head = be.heads.lock().unwrap()[0].clone();
        assert!(
            fe_head.contains("authorization: basic"),
            "initial leg authenticated"
        );
        assert!(
            be_head.contains("authorization: basic dgvzddp0zxn0"),
            "credential re-attached on redirect: {be_head}"
        );
        assert!(be_head.contains("label: "), "label rides the redirect");
        // header contract (BE request): shared headers present …
        assert!(be_head.contains("format: json"));
        assert!(be_head.contains("expect: 100-continue"));
        // … Doris-only headers absent (T12 matrix: StarRocks has no
        // read_json_by_line parameter and no load-time sequence column)
        assert!(
            !be_head.contains("read_json_by_line"),
            "starrocks must not send read_json_by_line: {be_head}"
        );
        assert!(
            !be_head.contains("sequence_column"),
            "starrocks must not send sequence_column: {be_head}"
        );
        // carrier identical on both legs (the buffer survives the hop)
        assert_eq!(fe.bodies.lock().unwrap()[0], be.bodies.lock().unwrap()[0]);

        // ② redirect budget: hops are bounded — a chain longer than
        // MAX_REDIRECT_HOPS is fatal (4 chained mocks: the initial request
        // plus 3 followed hops, the 4th redirect is not followed)
        let m4 = MockDoris::start(vec![(307, "{}".into())]);
        let m3 = MockDoris::start_with_redirect(vec![(307, "{}".into())], Some(m4.url()));
        let m2 = MockDoris::start_with_redirect(vec![(307, "{}".into())], Some(m3.url()));
        let m1 = MockDoris::start_with_redirect(vec![(307, "{}".into())], Some(m2.url()));
        let mut sink = DorisSink::new(m1.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "srlp")
            .with_flavor(SinkFlavor::StarRocks);
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("b")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(
            err.contains("redirect chain not followable"),
            "hop budget error: {err}"
        );
        let total = m1.hits.load(Ord2::Relaxed)
            + m2.hits.load(Ord2::Relaxed)
            + m3.hits.load(Ord2::Relaxed)
            + m4.hits.load(Ord2::Relaxed);
        assert_eq!(total, 4, "1 initial + MAX_REDIRECT_HOPS follows");

        // ③ relative Location → fatal without following (StarRocks FEs emit
        // absolute URLs; anything else is a wiring bug, not to be guessed at)
        let m = MockDoris::start_with_redirect(
            vec![(307, "{}".into())],
            Some("/relative/api/db/t/_stream_load".into()),
        );
        let mut sink = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "srna")
            .with_flavor(SinkFlavor::StarRocks);
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("c")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(err.contains("redirect chain not followable"), "{err}");
        assert_eq!(m.hits.load(Ord2::Relaxed), 1);

        // ④ label idempotency, StarRocks wording: the reference page lists
        // "Label Already Exists" among the Status values (Doris instead
        // answers Status:Fail + Message) — both must converge to success
        let m = MockDoris::start(vec![
            (200, r#"{"Status":"Success"}"#.into()),
            (200, r#"{"Status":"Label Already Exists"}"#.into()),
        ]);
        let prefix = "srreplay";
        let mut a = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], prefix)
            .with_flavor(SinkFlavor::StarRocks);
        let mut b = DorisSink::new(m.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], prefix)
            .with_flavor(SinkFlavor::StarRocks);
        a.write(vec![mk("d")?])?;
        assert_eq!(a.flush()?.rows, 1);
        // restart replay: same label (same prefix, same seq) → Dedup converge
        b.write(vec![mk("d")?])?;
        assert_eq!(b.flush()?.rows, 1, "label replay converges to success");
        assert_eq!(m.hits.load(Ord2::Relaxed), 2);
        let h0 = m.heads.lock().unwrap()[0].clone();
        let h1 = m.heads.lock().unwrap()[1].clone();
        let l0 = h0.lines().find(|l| l.starts_with("label:"));
        let l1 = h1.lines().find(|l| l.starts_with("label:"));
        assert_eq!(l0, l1, "the replay must reuse the exact label");

        // ⑤ Doris flavor on a 307 stays fatal (regression guard for the
        // existing stance — test ⑤ of doris_status_matrix_mock covers the
        // Doris-flavor wording; here we pin the flavor split itself)
        let be = MockDoris::start(vec![(200, r#"{"Status":"Success"}"#.into())]);
        let fe = MockDoris::start_with_redirect(vec![(307, "{}".into())], Some(be.url()));
        let mut sink = DorisSink::new(fe.url(), "Basic dGVzdDp0ZXN0", vec!["name".into()], "drfs");
        sink.retry_backoff_ms = vec![0, 0];
        sink.write(vec![mk("e")?])?;
        let err = sink.flush().unwrap_err().to_string();
        assert!(
            err.contains("BE:8040"),
            "doris keeps the fatal stance: {err}"
        );
        assert_eq!(be.hits.load(Ord2::Relaxed), 0, "doris never follows");
        Ok(())
    }

    /// T12 live gate: runs only against the tailrace dev StarRocks stack
    /// (docker-compose.yml `--profile starrocks`, feprocks entry on
    /// 127.0.0.1:48080 — the profile pre-creates user tailrace + db
    /// tailrace_spike.sr_e2e). CI-safe: skipped when the port is closed.
    #[cfg(feature = "starrocks")]
    #[test]
    fn starrocks_sink_live_stream_load() -> anyhow::Result<()> {
        // compose default (TAILRACE_SR_PROXY_PORT); hardcoded like the Doris
        // live gate hardcodes 8040/8030
        let entry = "localhost:48080";
        if std::net::TcpStream::connect(entry).is_err() {
            return Ok(()); // CI-safe: profile not up
        }
        // liveness probe: the feproxy forwards to the FE http port — an
        // unrelated listener on 48080 must not turn this into a spurious pass
        let probe = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()?;
        let fe_ok = probe
            .get(format!("http://{entry}/api/health"))
            .send()
            .and_then(|r| r.error_for_status())
            .is_ok();
        if !fe_ok {
            tracing::warn!(
                "[live-gate] the StarRocks entry on {entry} does not answer /api/health — \
                 skipping the live test"
            );
            return Ok(());
        }
        std::env::set_var(
            "TAILRACE_TEST_SR_AUTH",
            "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=",
        );
        let url = format!("http://{entry}/api/tailrace_spike/sr_e2e/_stream_load");
        let mk = |order_no: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("order_no", DataType::Utf8, false),
                    Field::new("qty", DataType::Int64, true),
                    Field::new("etl_ts", DataType::Int64, true),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![order_no])),
                    Arc::new(Int64Array::from(vec![Some(7)])),
                    Arc::new(Int64Array::from(vec![Some(1726000000000)])),
                ],
            )?)
        };
        let prefix = format!("srlive{}", std::process::id());
        // first flush commits label {prefix}_0
        let mut a = DorisSink::new(
            &url,
            "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=",
            vec!["order_no".into(), "qty".into(), "etl_ts".into()],
            &prefix,
        )
        .with_flavor(SinkFlavor::StarRocks);
        a.write(vec![mk("SR-LIVE-1")?])?;
        let ack = a.flush()?;
        assert_eq!(ack.kind, "STARROCKS_STREAM_LOAD");
        assert_eq!(ack.rows, 1);
        // restart replay with the same label: Label Already Exists must
        // converge to success (the row must not double-commit)
        let mut b = DorisSink::new(
            &url,
            "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=",
            vec!["order_no".into(), "qty".into(), "etl_ts".into()],
            &prefix,
        )
        .with_flavor(SinkFlavor::StarRocks);
        b.write(vec![mk("SR-LIVE-1")?])?;
        let ack2 = b.flush()?;
        assert_eq!(ack2.rows, 1, "label replay converges (dedup)");
        assert_eq!(ack2.idempotency_key, ack.idempotency_key);
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
