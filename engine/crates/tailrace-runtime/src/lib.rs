//! tailrace-runtime: pipeline executor — the engineered micro-batch main loop.
//!
//! Each statement is an independent data flow:
//! `source.poll(8k|100ms) → decode (constant-predicate early filter) →
//! datafusion transform → sink.write (batching) → on boundary sink.flush +
//! state.save (at-least-once anchor: the cursor commits only after a
//! successful write)`.
//! Statements never block each other (each advances one micro-batch round per
//! turn; each runs on its own thread).

pub mod dim;
pub mod dlq;
pub mod pipeline_log;
pub mod registry;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::LogicalPlan;

use dlq::{BadRowPolicy, BadRowRecord};
use pipeline_log::PipelineLogHub;
use tailrace_catalog::{CatalogProvider, TableObject};
use tailrace_connector::ConnectorRegistry;
use tailrace_decode::{
    ColBatch, CsvDecoder, DebeziumJsonDecoder, Decoded, Decoder, JsonDecoder, OggDecoder,
    ParquetDecoder, Pushdown,
};
use tailrace_ir::{Cursor, SourceChunk};
use tailrace_state::{FileOffsetStore, OffsetStore};

/// Default connector registry: every connector feature compiled into this
/// crate (mirrors the binary's build_registry wiring).
pub fn default_registry() -> ConnectorRegistry {
    let mut reg = ConnectorRegistry::new();
    #[cfg(feature = "connector-kafka")]
    tailrace_conn_kafka::register(&mut reg);
    #[cfg(feature = "connector-file")]
    tailrace_conn_file::register(&mut reg);
    #[cfg(feature = "connector-doris")]
    tailrace_conn_doris::register(&mut reg);
    reg
}

/// Run options.
///
/// The three sink batching thresholds follow the Doris Flink connector
/// `sink.buffer-flush.*` parameter semantics — row / byte / time, **any one
/// satisfied → flush** ([`RunOptions::flush_due`]). The plan surface carries
/// the same three dimensions (`PlanOptions::tuning`: `flushRows` /
/// `flushBytes` / `flushIntervalMs`, registry-clamped).
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Max run duration
    pub duration: Duration,
    /// sink batching row boundary — Doris Flink connector
    /// `sink.buffer-flush.max-rows` (default 10_000)
    pub flush_rows: u64,
    /// sink batching time boundary — Doris Flink connector
    /// `sink.buffer-flush.interval` (default 10s; also drains the trailing
    /// batch on idle polls after a stream stops)
    pub flush_interval: Duration,
    /// sink batching byte boundary — Doris Flink connector
    /// `sink.buffer-flush.max-bytes` (default 8 MiB; 0 = the byte boundary is
    /// disabled — sinks without a real [`tailrace_connector::Sink::buffered_bytes`]
    /// report 0 and never trigger it)
    pub flush_bytes: u64,
    /// micro-batch pull row cap
    pub poll_rows: usize,
    /// micro-batch pull time slice
    pub poll_slice: Duration,
    /// T-D1 broadcast join: max rows per dimension table (fail-fast at
    /// assembly; default [`dim::DEFAULT_DIM_MAX_ROWS`] = 500_000, plan tuning
    /// `dimMaxRows`)
    pub dim_max_rows: usize,
    /// T-D1 broadcast join: max decoded-Arrow bytes per dimension table
    /// (fail-fast at assembly; default [`dim::DEFAULT_DIM_MAX_BYTES`] =
    /// 64 MiB, plan tuning `dimMaxBytes`)
    pub dim_max_bytes: u64,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(30),
            flush_rows: 10_000,
            flush_interval: Duration::from_secs(10),
            flush_bytes: 8 * 1024 * 1024, // 8MB batching byte boundary
            poll_rows: 8_000,
            poll_slice: Duration::from_millis(100),
            dim_max_rows: dim::DEFAULT_DIM_MAX_ROWS,
            dim_max_bytes: dim::DEFAULT_DIM_MAX_BYTES,
        }
    }
}

impl RunOptions {
    /// Three-threshold flush boundary (Doris Flink connector
    /// `sink.buffer-flush.max-rows / max-bytes / interval` semantics): flush
    /// when ANY of the row, byte or time boundary is reached. The flush
    /// checkpoint is one micro-batch (poll), so a boundary can overshoot by at
    /// most one poll's rows — the connector checks per record (granularity
    /// documented in docs/BENCHMARKS.md §4.6).
    pub fn flush_due(&self, pending_rows: u64, buffered_bytes: u64, since_flush: Duration) -> bool {
        pending_rows >= self.flush_rows
            || since_flush >= self.flush_interval
            || (self.flush_bytes > 0 && buffered_bytes >= self.flush_bytes)
    }
}

/// One statement's execution assembly (resolved at compile/deploy time; zero
/// catalog queries on the runtime hot path).
/// Hot-path event emitter: (level, kind, message, statement_idx).
type EmitFn = Box<dyn Fn(&str, &str, &str, Option<u32>) + Send>;

struct StatementRuntime {
    source: Box<dyn tailrace_connector::Source>,
    decoder: Box<dyn Decoder>,
    pushdown: Pushdown,
    sink: Box<dyn tailrace_connector::Sink>,
    /// Materialized statement plan (T7: parse+plan once per statement at
    /// assembly — `tailrace_ir::build_exec_plan`; per micro-batch only the
    /// `src` data source is swapped)
    exec_plan: Arc<LogicalPlan>,
    /// sink-side column list (transform output column-order check)
    sink_columns: Vec<String>,
    /// declared source schema (T5: per-worker ColBatch construction on the
    /// parallel decode path)
    schema: datafusion::arrow::datatypes::SchemaRef,
    batch: ColBatch,
    pending_rows: u64,
    last_flush: Instant,
    statement_index: u32,
    /// Bad-row handling policy (plan options badRowPolicy; default dlq).
    bad_row_policy: BadRowPolicy,
    /// Quarantine writer (present iff policy = dlq).
    dlq: Option<dlq::BadRowDlq>,
    /// Observability trio: hot-path event emitter + atomic counters (injected
    /// by run_serve).
    emit: Option<EmitFn>,
    counters: Option<std::sync::Arc<pipeline_log::StatementCounters>>,
}

// —— observability trio: flush-point unified counters (shared by all three
// flush sites in serve/standalone) ——
impl StatementRuntime {
    /// Ledger cumulative snapshot (committed at the same point as the cursor):
    /// with counters present (the serve production path) returns the totals
    /// including the assembly seed; without injected counters
    /// (standalone/tests) returns None = keep the ledger's old value.
    fn ledger_totals(&self) -> Option<tailrace_state::StatementTotals> {
        self.counters
            .as_ref()
            .map(|c| tailrace_state::StatementTotals {
                consumed: c.consumed.load(std::sync::atomic::Ordering::Relaxed),
                flushed: c.flushed.load(std::sync::atomic::Ordering::Relaxed),
                flushes: c.flushes.load(std::sync::atomic::Ordering::Relaxed),
            })
    }

    /// Flush-point unified counter update (A6 /metrics wiring): rows + flush
    /// count + the duration metrics — `flush_ms_total` (cumulative) and
    /// `last_flush_dur_ms` feed `tailrace_flush_duration_seconds`; a
    /// data-carrying flush (ack_rows > 0) stamps `last_data_ms`, the basis of
    /// `tailrace_pipeline_lag_seconds` (an empty trailing flush carries no
    /// data-freshness signal).
    fn obs_flush(&self, ack_rows: u64, dur_ms: u64) {
        if let Some(c) = self.counters.as_ref() {
            c.flushed
                .fetch_add(ack_rows, std::sync::atomic::Ordering::Relaxed);
            c.flushes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            c.flush_ms_total
                .fetch_add(dur_ms, std::sync::atomic::Ordering::Relaxed);
            c.last_flush_dur_ms
                .store(dur_ms, std::sync::atomic::Ordering::Relaxed);
            c.last_flush_ms
                .store(now_ms(), std::sync::atomic::Ordering::Relaxed);
            if ack_rows > 0 {
                c.last_data_ms
                    .store(now_ms(), std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Flush observability span (T2 OTLP instrumentation, four groups): 批次大小
/// `rows` / 批次延迟 `flush_ms` (+ span duration) / sink 在途 `in_flight_rows`
/// / 成功率 (spans that reach the `rows` record vs. ones dropped on a failed
/// flush). Zero-cost without a subscriber; exported through
/// tracing-opentelemetry under the binary's `otel` feature.
fn sink_flush_span(pipeline_id: u64, st: &StatementRuntime) -> tracing::Span {
    tracing::info_span!(
        "sink_flush",
        pipeline_id,
        statement_idx = st.statement_index,
        in_flight_rows = st.pending_rows,
        rows = tracing::field::Empty,
        flush_ms = tracing::field::Empty
    )
}

/// Bad-row post-processing shared by both statement loops (upgrade plan §二
/// engine H2): policy handling (fail → bail before the cursor can cross the
/// batch; dlq → quarantine raw bytes + poll-time cursor + timestamp to the
/// side file — a quarantine write failure is fatal for the same reason),
/// counters, and the throttled DECODE_BAD event carrying the DLQ path (not a
/// raw sample).
#[allow(clippy::too_many_arguments)]
fn handle_bad_rows(
    st: &StatementRuntime,
    pipeline_id: u64,
    bad_now: u64,
    bad_raws: Vec<Vec<u8>>,
    fail_sample: Option<String>,
    bad_since_event: &mut u64,
    bad_event_emitted: &mut bool,
) -> anyhow::Result<()> {
    if bad_now == 0 {
        return Ok(());
    }
    if let Some(c) = st.counters.as_ref() {
        c.bad.fetch_add(bad_now, Ordering::Relaxed);
    }
    if let Some(sample) = fail_sample {
        anyhow::bail!(
            "decode bad row under badRowPolicy=fail (statement halted before the cursor crossed the batch) — first bad row sample: {sample}"
        );
    }
    if !bad_raws.is_empty() {
        let cursor = st.source.cursor();
        let records: Vec<BadRowRecord> = bad_raws
            .iter()
            .map(|raw| dlq::record(pipeline_id, st.statement_index, &cursor, raw))
            .collect();
        let writer = st
            .dlq
            .as_ref()
            .expect("dlq writer present under the Dlq policy");
        if let Err(e) = writer.write(&records) {
            anyhow::bail!(
                "DLQ write failed ({}: {e:#}) — refusing to cross the cursor over unpersisted bad rows",
                writer.path().display()
            );
        }
    }
    *bad_since_event += bad_now;
    if *bad_since_event >= 1000 || !*bad_event_emitted {
        if let Some(emit) = st.emit.as_ref() {
            let note = match (&st.dlq, st.bad_row_policy) {
                (Some(d), _) => format!(" — quarantined to DLQ: {}", d.path().display()),
                (None, BadRowPolicy::Skip) => {
                    " — badRowPolicy=skip (rows dropped, counted only)".to_string()
                }
                (None, _) => String::new(),
            };
            emit(
                "WARN",
                "DECODE_BAD",
                &format!(
                    "decoded bad rows total {} (this batch {}){}",
                    *bad_since_event, bad_now, note
                ),
                Some(st.statement_index),
            );
        }
        *bad_since_event = 0;
        *bad_event_emitted = true;
    }
    Ok(())
}

/// Pipeline executor (each statement advances on its own thread — statements
/// never block each other).
pub struct PipelineRunner {
    pipeline_id: u64,
    statements: Vec<StatementRuntime>,
    state: Arc<Mutex<FileOffsetStore>>,
    options: RunOptions,
}

// ==================== T5 plan-a: intra-poll parallel decode ====================

/// Fan-out floor: below this many chunks per poll, a serial decode into the
/// reused statement batch beats spawning scoped threads (trickle traffic
/// keeps the zero-overhead path unchanged; a full 8000-chunk poll amortizes
/// thread spawn cost ~100× over the decode work at measured decode rates).
const PARALLEL_DECODE_MIN_CHUNKS: usize = 512;

/// Worker count for one poll's decode: 1 (serial) below the fan-out floor,
/// otherwise one worker per `PARALLEL_DECODE_MIN_CHUNKS` chunks capped at the
/// machine's parallelism.
fn decode_workers(n: usize) -> usize {
    if n < PARALLEL_DECODE_MIN_CHUNKS * 2 {
        return 1;
    }
    let avail = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
        .max(2);
    (n / PARALLEL_DECODE_MIN_CHUNKS).clamp(2, avail)
}

/// One worker group's decode outcome.
#[derive(Default)]
struct GroupDecode {
    appended: u64,
    skipped: u64,
    bad_rows: u64,
    bad_raws: Vec<Vec<u8>>,
    fail_sample: Option<String>,
}

/// One poll's decode outcome (T5 plan-a): the poll's chunks are mutually
/// independent — fan them out to worker threads, each decoding into its own
/// `ColBatch`, then merge in chunk order (`concat_batches`) so the row order
/// is byte-identical to the serial path. Runs entirely between `source.poll`
/// and the flush-point cursor commit — the cursor semantics (whole-map commit
/// at flush) are untouched.
struct PollDecode {
    /// merged appended rows (`None` = nothing appended this poll — all
    /// skipped)
    merged: Option<datafusion::arrow::record_batch::RecordBatch>,
    skipped: u64,
    /// every bad row this poll, **all policies** (the fail-policy bail fires
    /// on this count; `bad_raws` only carries payloads under Dlq)
    bad_now: u64,
    bad_raws: Vec<Vec<u8>>,
    /// first bad-row sample under the Fail policy, in chunk order
    fail_sample: Option<String>,
}

/// Decode one poll's chunks (T5 plan-a). Serial for small polls (the reused
/// statement batch keeps its builder capacity — today's path unchanged);
/// parallel for large ones. `stats` accumulates appended/skipped/bad exactly
/// as the old inline loop did; the caller folds `skipped` into the counters
/// and hands the bad-row collections to [`handle_bad_rows`].
fn decode_poll(
    decoder: &dyn Decoder,
    pushdown: &Pushdown,
    schema: &datafusion::arrow::datatypes::SchemaRef,
    reuse: &mut ColBatch,
    chunks: Vec<SourceChunk>,
    policy: BadRowPolicy,
    stats: &mut StatementStats,
) -> anyhow::Result<PollDecode> {
    let workers = decode_workers(chunks.len());
    let mut pd = PollDecode {
        merged: None,
        skipped: 0,
        bad_now: 0,
        bad_raws: Vec::new(),
        fail_sample: None,
    };
    if workers <= 1 {
        // serial: today's path verbatim — decode into the reused batch
        // (builders keep capacity across polls)
        for chunk in chunks {
            match decoder.decode(chunk.raw, reuse, pushdown) {
                Decoded::Append => stats.appended += 1,
                Decoded::Skip => {
                    stats.skipped += 1;
                    pd.skipped += 1;
                }
                Decoded::Bad(raw) => {
                    stats.bad_rows += 1;
                    pd.bad_now += 1;
                    match policy {
                        BadRowPolicy::Fail => {
                            if pd.fail_sample.is_none() {
                                pd.fail_sample =
                                    Some(String::from_utf8_lossy(&raw).chars().take(120).collect());
                            }
                        }
                        BadRowPolicy::Dlq => pd.bad_raws.push(raw),
                        BadRowPolicy::Skip => {}
                    }
                }
            }
        }
        if reuse.is_empty() {
            return Ok(pd);
        }
        pd.merged = Some(reuse.finish()?);
        return Ok(pd);
    }
    // parallel: contiguous chunk-order groups (split_off moves elements —
    // no raw-buffer copies), one scoped thread per group, each decoding into
    // its own ColBatch sized for its share
    let per = chunks.len().div_ceil(workers);
    let mut groups: Vec<Vec<SourceChunk>> = Vec::with_capacity(workers);
    let mut rest = chunks;
    while rest.len() > per {
        groups.push(rest.split_off(rest.len() - per));
    }
    groups.push(rest);
    groups.reverse(); // split_off takes tails — restore chunk order

    let groups_out = std::thread::scope(
        |scope| -> anyhow::Result<
            Vec<(
                GroupDecode,
                Option<datafusion::arrow::record_batch::RecordBatch>,
            )>,
        > {
            let mut handles = Vec::with_capacity(groups.len());
            for chunks in groups {
                handles.push(scope.spawn(move || {
                    let mut r = GroupDecode::default();
                    let mut batch = ColBatch::new(schema.clone(), chunks.len());
                    for chunk in chunks {
                        match decoder.decode(chunk.raw, &mut batch, pushdown) {
                            Decoded::Append => r.appended += 1,
                            Decoded::Skip => r.skipped += 1,
                            Decoded::Bad(raw) => {
                                r.bad_rows += 1;
                                match policy {
                                    BadRowPolicy::Fail => {
                                        if r.fail_sample.is_none() {
                                            r.fail_sample = Some(
                                                String::from_utf8_lossy(&raw)
                                                    .chars()
                                                    .take(120)
                                                    .collect(),
                                            );
                                        }
                                    }
                                    BadRowPolicy::Dlq => r.bad_raws.push(raw),
                                    BadRowPolicy::Skip => {}
                                }
                            }
                        }
                    }
                    let rb = if batch.is_empty() {
                        None
                    } else {
                        Some(batch.finish()?)
                    };
                    anyhow::Ok((r, rb))
                }));
            }
            let mut out = Vec::with_capacity(handles.len());
            let mut panicked = false;
            for h in handles {
                match h.join() {
                    Ok(Ok(v)) => out.push(v),
                    Ok(Err(e)) => return Err(e), // ColBatch::finish (arrow) error
                    Err(_) => panicked = true,   // scope joins the rest before unwinding
                }
            }
            if panicked {
                anyhow::bail!("decode worker thread panicked — cursor not crossed (batch replays)");
            }
            Ok(out)
        },
    )?;
    // merge in group order = chunk order (row order identical to serial)
    let mut non_empty: Vec<datafusion::arrow::record_batch::RecordBatch> =
        Vec::with_capacity(groups_out.len());
    for (r, rb) in groups_out {
        stats.appended += r.appended;
        stats.skipped += r.skipped;
        stats.bad_rows += r.bad_rows;
        pd.skipped += r.skipped;
        pd.bad_now += r.bad_rows;
        if !r.bad_raws.is_empty() {
            pd.bad_raws.extend(r.bad_raws);
        }
        pd.fail_sample = pd.fail_sample.take().or(r.fail_sample);
        if let Some(rb) = rb {
            non_empty.push(rb);
        }
    }
    pd.merged = match non_empty.len() {
        0 => None,
        1 => non_empty.into_iter().next(),
        _ => Some(datafusion::arrow::compute::concat_batches(
            schema, &non_empty,
        )?),
    };
    Ok(pd)
}

/// Stats of one statement's execution round.
#[derive(Debug, Default, Clone)]
pub struct StatementStats {
    pub consumed: u64,
    pub appended: u64,
    pub skipped: u64,
    /// decoded bad-row count (Decoded::Bad used to be fully silent — bad data
    /// vanished invisibly)
    pub bad_rows: u64,
    pub flushed_rows: u64,
    pub flushes: u64,
}

impl PipelineRunner {
    /// Assemble from the catalog (reference resolution → source/decoder/sink
    /// via the connector registry + cursor restore).
    pub fn assemble(
        catalog: &dyn CatalogProvider,
        state_dir: &std::path::Path,
        options: RunOptions,
        reliability: Option<tailrace_catalog::ReliabilityOptions>,
        bad_row_policy: BadRowPolicy,
        connectors: &ConnectorRegistry,
    ) -> anyhow::Result<Self> {
        let pipeline_id = catalog.pipeline_id();
        let state = Arc::new(Mutex::new(FileOffsetStore::new(state_dir)?));
        let mut statements = Vec::new();
        for (idx, spec) in catalog.statements()?.into_iter().enumerate() {
            let idx = idx as u32;
            let src_obj = catalog.resolve(&spec.source)?;
            let sink_obj = catalog.resolve(&spec.sink)?;
            let source = connectors.build_source(&src_obj, &spec, reliability.as_ref())?;
            let decoder = build_decoder(&src_obj)?;
            let mut pushdown = Pushdown::default();
            if !spec.pushdown.is_empty() {
                pushdown
                    .eq_whitelist
                    .insert("op_type".into(), spec.pushdown.clone());
            }
            let sink = connectors.build_sink(&sink_obj, reliability.as_ref())?;
            let schema = Arc::new(Schema::new(
                src_obj
                    .columns
                    .iter()
                    .map(|(n, t)| Field::new(n.clone(), arrow_type(t), true))
                    .collect::<Vec<_>>(),
            ));
            // T-D1 broadcast join (blueprint D narrow path): dim tables load
            // FULL-TABLE over JDBC here — pipeline compile time, fail-fast
            // caps (rows / decoded bytes), unique-key gate via the JDBC
            // source's keyset precondition. The loaded snapshots are static
            // for the plan's lifetime (broadcast, NOT streaming: no
            // watermark, no state backend, no refresh).
            let dim_limits = dim::DimLimits {
                max_rows: options.dim_max_rows,
                max_bytes: options.dim_max_bytes,
            };
            let mut dims: Vec<tailrace_transform::DimTable> = Vec::new();
            for d in &spec.dims {
                let obj = catalog.resolve(&d.reference)?;
                if obj.kind != "dim" {
                    anyhow::bail!(
                        "dim reference {} must be a kind=dim table object (got {}) — \
                         only ${{ds:dim.datasource.table}} references load as dimension tables",
                        d.reference,
                        obj.kind
                    );
                }
                let batch =
                    dim::load_dim(&obj, &d.name, &spec, connectors, &dim_limits).map_err(|e| {
                        anyhow::anyhow!("statement #{idx} dim table {}: {e:#}", d.reference)
                    })?;
                dims.push(tailrace_transform::DimTable {
                    name: d.name.clone(),
                    batch,
                });
            }
            // T7 plan materialization: parse+plan ONCE per statement (fail
            // fast on bad SQL at assembly), per micro-batch the decoded batch
            // is injected as `src` — no re-parse/re-plan on the hot path.
            // Dim-join statements plan via build_dim_plan (the dim MemTables
            // ride inside the plan; inject_src still swaps only `src`).
            let exec_plan = if dims.is_empty() {
                Arc::new(tailrace_ir::build_exec_plan(
                    &spec.select_sql,
                    &src_obj.columns,
                )?)
            } else {
                Arc::new(tailrace_transform::build_dim_plan(
                    &spec.select_sql,
                    schema.clone(),
                    &dims,
                )?)
            };
            let batch = ColBatch::new(schema.clone(), options.poll_rows);
            let _ = state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .load(pipeline_id, idx)?; // open = restore (cursor passed to source.open)
            let sink_cols = sink_obj.columns.iter().map(|(n, _)| n.clone()).collect();
            let dlq = (bad_row_policy == BadRowPolicy::Dlq)
                .then(|| dlq::BadRowDlq::new(state_dir, pipeline_id, idx));
            statements.push(StatementRuntime {
                source,
                decoder,
                pushdown,
                sink,
                exec_plan,
                sink_columns: sink_cols,
                schema,
                batch,
                pending_rows: 0,
                last_flush: Instant::now(),
                statement_index: idx,
                bad_row_policy,
                dlq,
                emit: None,
                counters: None,
            });
        }
        Ok(Self {
            pipeline_id,
            statements,
            state,
            options,
        })
    }

    /// serve-mode execution: same shape as run but with an unlimited duration
    /// — the ControlFlags stop flag drives exit. pause semantics: stop pulling
    /// but keep the already-pulled batch (no flush, no commit — resume
    /// continues from it).
    pub fn run_serve(
        &mut self,
        flags: crate::registry::ControlFlags,
    ) -> anyhow::Result<Vec<StatementStats>> {
        self.run_serve_observed(flags, None, None)
    }

    /// Observed variant: hub (event log) + per-pipeline counters injected
    /// into the statement threads.
    pub fn run_serve_observed(
        &mut self,
        flags: crate::registry::ControlFlags,
        log_hub: Option<Arc<PipelineLogHub>>,
        pipe_counters: Option<Arc<pipeline_log::PipeCounters>>,
    ) -> anyhow::Result<Vec<StatementStats>> {
        use std::thread;
        let mut handles = Vec::new();
        for mut st in self.statements.drain(..) {
            let state = Arc::clone(&self.state);
            let pipeline_id = self.pipeline_id;
            let opts = self.options.clone();
            let fl = flags.clone();
            let idx = st.statement_index;
            if let (Some(hub), Some(pc)) = (&log_hub, &pipe_counters) {
                let hub = Arc::clone(hub);
                st.emit = Some(Box::new(
                    move |level: &str, kind: &str, msg: &str, sidx: Option<u32>| {
                        hub.push(pipeline_id, level, kind, msg, sidx);
                    },
                ));
                st.counters = Some(std::sync::Arc::clone(&pc.statements[idx as usize]));
            }
            handles.push(thread::spawn(move || {
                run_statement_serve(st, pipeline_id, state, opts, fl)
            }));
        }
        // join all, aggregate without short-circuiting — the first error fails
        // everyone (remaining threads stop quickly); stats and errors are all
        // collected
        let mut stats_out = Vec::new();
        let mut first_err: Option<anyhow::Error> = None;
        let mut any_panic = false;
        for h in handles {
            match h.join() {
                Ok(Ok(stats)) => stats_out.push(stats),
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => any_panic = true,
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        if any_panic {
            anyhow::bail!("statement thread panicked");
        }
        Ok(stats_out)
    }

    /// Start (cursor restore) → per-statement thread main loop → final flush
    /// (join aggregates the stats).
    pub fn run(&mut self) -> anyhow::Result<Vec<StatementStats>> {
        use std::thread;
        tracing::info!(
            pipeline_id = self.pipeline_id,
            statements = self.statements.len(),
            duration_secs = self.options.duration.as_secs(),
            "pipeline run started (standalone)"
        );
        let deadline = Instant::now() + self.options.duration;
        let mut handles = Vec::new();
        for st in self.statements.drain(..) {
            let state = Arc::clone(&self.state);
            let pipeline_id = self.pipeline_id;
            let opts = self.options.clone();
            handles.push(thread::spawn(move || {
                run_statement(st, pipeline_id, state, opts, deadline)
            }));
        }
        let mut stats_out = Vec::new();
        let mut first_err: Option<anyhow::Error> = None;
        let mut any_panic = false;
        for h in handles {
            match h.join() {
                Ok(Ok(stats)) => stats_out.push(stats),
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => any_panic = true,
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        if any_panic {
            anyhow::bail!("statement thread panicked");
        }
        Ok(stats_out)
    }

    /// Bounded execution mode (T-A2 backfill): like [`PipelineRunner::run`]
    /// but each statement exits when its source cursor reaches `to` (or the
    /// source goes idle past the EOF guard), then final flush + cursor
    /// commit + return. A NEW mode alongside run / run_serve — the serve
    /// loop and the registry's restart loop are untouched.
    ///
    /// `from` seeds the source on a cold start (no ledger entry); a
    /// ledger entry takes precedence, so a mid-job restart resumes from the
    /// last bounded flush point. Assemble the runner against
    /// [`backfill_state_dir`], NEVER the main pipeline ledger dir — a
    /// bounded run commits every cursor it is given, and pointing it at the
    /// main ledger would advance main-pipeline cursors.
    ///
    /// `to` alignment granularity is one poll (same overshoot semantics as
    /// [`RunOptions::flush_due`]): the poll that crosses `to` is fully
    /// processed.
    pub fn run_bounded(&mut self, from: i64, to: i64) -> anyhow::Result<Vec<StatementStats>> {
        use std::thread;
        tracing::info!(
            pipeline_id = self.pipeline_id,
            statements = self.statements.len(),
            from,
            to,
            "pipeline run started (bounded backfill)"
        );
        let mut handles = Vec::new();
        for st in self.statements.drain(..) {
            let state = Arc::clone(&self.state);
            let pipeline_id = self.pipeline_id;
            let opts = self.options.clone();
            handles.push(thread::spawn(move || {
                run_statement_bounded(st, pipeline_id, state, opts, from, to)
            }));
        }
        // same join-aggregation contract as run(): the first error fails the
        // call, stats and panics are all collected
        let mut stats_out = Vec::new();
        let mut first_err: Option<anyhow::Error> = None;
        let mut any_panic = false;
        for h in handles {
            match h.join() {
                Ok(Ok(stats)) => stats_out.push(stats),
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => any_panic = true,
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        if any_panic {
            anyhow::bail!("statement thread panicked");
        }
        Ok(stats_out)
    }
}

/// serve-mode statement thread body: the loop is driven by ControlFlags
/// (stop → final flush and exit, pause → idle-wait).
fn run_statement_serve(
    mut st: StatementRuntime,
    pipeline_id: u64,
    state: Arc<Mutex<FileOffsetStore>>,
    options: RunOptions,
    flags: crate::registry::ControlFlags,
) -> anyhow::Result<StatementStats> {
    let mut stats = StatementStats::default();
    let mut backoff_secs: u32 = 0; // transient-error backoff counter
    let mut bad_since_event: u64 = 0; // Bad event throttling accumulator
    let mut bad_event_emitted = false; // first-bad visibility (engine H2)
    let mut was_paused = false; // PAUSED edge — enter the suspended zone / leave to recover
    let mut pause_ok = false; // only a successful pause allows keepalive polls (otherwise poll = real consumption, dropped)
    let resume = state
        .lock()
        .unwrap_or_else(|e| e.into_inner()) // poisoned ≠ corrupt — recover, don't cascade a sibling thread's panic (T39 ④; rationale at run_statement)
        .load(pipeline_id, st.statement_index)?
        .map(|(c, _, _)| c)
        .unwrap_or(Cursor::None);
    st.source.open(&resume)?;
    // T7 session assembly: plan execution batch size fixed to poll_rows (NOT
    // the first micro-batch's rows) + per-statement DataFusion memory pool.
    let tx_opts = tailrace_transform::TransformOptions {
        max_rows_per_batch: options.poll_rows,
        ..Default::default()
    };
    loop {
        if flags.is_stopped() {
            break;
        }
        if !flags.is_running() {
            // PAUSED = suspended partitions, not stopped polls — a long pause
            // (>max.poll.interval) triggers group eviction → full replay on
            // resume. Paused polls return empty, consuming and committing
            // nothing.
            if !was_paused {
                pause_ok = st.source.pause();
                was_paused = true;
            }
            if pause_ok {
                st.source.keepalive(std::time::Duration::from_millis(200));
            } else {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            continue;
        }
        if was_paused {
            if pause_ok {
                st.source.resume();
            }
            was_paused = false;
        }
        let chunks = match st.source.poll(options.poll_rows, options.poll_slice) {
            Ok(c) => c,
            Err(e) => {
                // Flink exponential-delay pattern: back off and retry on
                // transient errors instead of killing the pipeline
                backoff_secs += 1;
                let delay = std::cmp::min(1u64 << backoff_secs.min(6), 300); // 1s→2s→...→capped at 300s
                tracing::warn!(
                    pipeline_id,
                    statement_idx = st.statement_index,
                    kind = "POLL_BACKOFF",
                    attempt = backoff_secs,
                    delay_secs = delay,
                    error = %e,
                    "poll failed, backing off"
                );
                if backoff_secs == 1 {
                    // observability: emit one event only when entering backoff
                    // (recovery is implied by the reset); no per-attempt spam
                    if let Some(emit) = st.emit.as_ref() {
                        emit(
                            "WARN",
                            "SOURCE_BACKOFF",
                            &format!("poll failed, entering backoff (from {delay}s): {e}"),
                            Some(st.statement_index),
                        );
                    }
                }
                std::thread::sleep(std::time::Duration::from_secs(delay));
                if backoff_secs >= 10 {
                    return Err(e); // ~5 minutes of consecutive failures = give up — at-least-once: the uncommitted cursor replays on restart
                }
                continue;
            }
        };
        backoff_secs = 0; // any poll Ok (including an empty batch) resets the
                          // backoff — resetting only on non-empty used to make
                          // long-idle healthy pipelines accumulate toward
                          // giving up
        if chunks.is_empty() {
            // idle-poll sleep prevents spinning (100% CPU with a FileSource
            // at EOF)
            std::thread::sleep(std::time::Duration::from_millis(50));
            // check flush_interval on idle polls too — the trailing batch
            // after a stream stops must not linger in memory indefinitely
            if st.pending_rows > 0 && st.last_flush.elapsed() >= options.flush_interval {
                let span = sink_flush_span(pipeline_id, &st);
                let _flush_span = span.enter();
                let flush_t0 = Instant::now();
                let ack = st.sink.flush()?;
                let dur_ms = flush_t0.elapsed().as_millis() as u64;
                span.record("rows", ack.rows);
                span.record("flush_ms", dur_ms);
                stats.flushed_rows += ack.rows;
                stats.flushes += 1;
                st.obs_flush(ack.rows, dur_ms);
                st.pending_rows = 0;
                st.last_flush = Instant::now();
                state.lock().unwrap_or_else(|e| e.into_inner()).save(
                    pipeline_id,
                    st.statement_index,
                    &st.source.cursor(),
                    Some(&ack.idempotency_key),
                    st.ledger_totals(),
                )?;
            }
            continue;
        }
        stats.consumed += chunks.len() as u64;
        if let Some(c) = st.counters.as_ref() {
            c.consumed.fetch_add(chunks.len() as u64, Ordering::Relaxed);
        }
        // T5 plan-a: intra-poll parallel decode (chunks are mutually
        // independent) — serial below the fan-out floor, parallel above it,
        // merged in chunk order. Bad-row policy handling stays exactly here:
        // after the full poll decodes, before the cursor can cross.
        let pd = decode_poll(
            st.decoder.as_ref(),
            &st.pushdown,
            &st.schema,
            &mut st.batch,
            chunks,
            st.bad_row_policy,
            &mut stats,
        )?;
        if pd.skipped > 0 {
            if let Some(c) = st.counters.as_ref() {
                c.skipped.fetch_add(pd.skipped, Ordering::Relaxed);
            }
        }
        handle_bad_rows(
            &st,
            pipeline_id,
            pd.bad_now,
            pd.bad_raws,
            pd.fail_sample,
            &mut bad_since_event,
            &mut bad_event_emitted,
        )?;
        let Some(rb) = pd.merged else {
            continue;
        };
        let out = tailrace_transform::transform_plan(&st.exec_plan, rb, &tx_opts)?; // materialized-plan variant (T7)
                                                                                    // Validate the output column 【order】 before the first row is written —
                                                                                    // a HashSet equality used to let a same-columns-different-order
                                                                                    // projection through (silently shuffled columns); only the ordered
                                                                                    // comparison locks the order
        if let Some(first) = out.first() {
            let out_cols: Vec<String> = first
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            if out_cols != st.sink_columns && !st.sink_columns.is_empty() {
                anyhow::bail!(
                    "transform output column order != sink: output {:?} vs sink {:?} — check the SELECT projection order",
                    out_cols, st.sink_columns
                );
            }
        }
        let rows: u64 = out.iter().map(|b| b.num_rows() as u64).sum();
        if rows > 0 {
            st.sink.write(out)?;
        }
        st.pending_rows += rows;
        // three-threshold boundary (Doris Flink connector
        // `sink.buffer-flush.max-rows / max-bytes / interval`: any satisfied →
        // flush) — byte boundary wired through the sink's real
        // buffered_bytes() (file/doris/kafka all measure; other sinks
        // reporting 0 degrade honestly and never trigger the byte boundary)
        if options.flush_due(
            st.pending_rows,
            st.sink.buffered_bytes(),
            st.last_flush.elapsed(),
        ) {
            let span = sink_flush_span(pipeline_id, &st);
            let _flush_span = span.enter();
            let flush_t0 = Instant::now();
            let ack = st.sink.flush()?;
            let dur_ms = flush_t0.elapsed().as_millis() as u64;
            span.record("rows", ack.rows);
            span.record("flush_ms", dur_ms);
            stats.flushed_rows += ack.rows;
            stats.flushes += 1;
            st.obs_flush(ack.rows, dur_ms);
            if let Some(emit) = st.emit.as_ref() {
                emit(
                    "INFO",
                    "FLUSH_OK",
                    &format!(
                        "flush rows={} totals flushed={} flushes={} skipped={} took {}ms",
                        ack.rows,
                        stats.flushed_rows,
                        stats.flushes,
                        stats.skipped,
                        flush_t0.elapsed().as_millis()
                    ),
                    Some(st.statement_index),
                );
            }
            st.pending_rows = 0;
            st.last_flush = Instant::now();
            state.lock().unwrap_or_else(|e| e.into_inner()).save(
                pipeline_id,
                st.statement_index,
                &st.source.cursor(),
                Some(&ack.idempotency_key),
                st.ledger_totals(),
            )?;
        }
    }
    // stop finalization: flush the remainder + commit the cursor
    // (at-least-once anchor — same semantics as standalone)
    if st.pending_rows > 0 {
        let span = sink_flush_span(pipeline_id, &st);
        let _flush_span = span.enter();
        let flush_t0 = Instant::now();
        let ack = st.sink.flush()?;
        let dur_ms = flush_t0.elapsed().as_millis() as u64;
        span.record("rows", ack.rows);
        span.record("flush_ms", dur_ms);
        stats.flushed_rows += ack.rows;
        stats.flushes += 1;
        st.obs_flush(ack.rows, dur_ms);
        if let Some(emit) = st.emit.as_ref() {
            emit(
                "INFO",
                "FLUSH_OK",
                &format!(
                    "final flush rows={} totals flushed={} flushes={} skipped={}",
                    ack.rows, stats.flushed_rows, stats.flushes, stats.skipped
                ),
                Some(st.statement_index),
            );
        }
    }
    state.lock().unwrap_or_else(|e| e.into_inner()).save(
        pipeline_id,
        st.statement_index,
        &st.source.cursor(),
        None,
        st.ledger_totals(),
    )?;
    Ok(stats)
}

/// Standalone statement thread body: micro-batch loop until the deadline
/// (including final flush + cursor commit).
///
/// Offset-store locking (here and in `run_statement_serve`, roadmap §九 T39
/// ④): every `state.lock()` recovers a poisoned `Mutex` with
/// `unwrap_or_else(|e| e.into_inner())` — registry.rs parity. Poisoning only
/// means some sibling statement thread panicked while holding the lock; the
/// store itself (whole-row save/load) stays consistent, so the guard is
/// recovered and this statement keeps committing instead of cascading the
/// sibling's panic through every flush/commit site.
fn run_statement(
    mut st: StatementRuntime,
    pipeline_id: u64,
    state: Arc<Mutex<FileOffsetStore>>,
    options: RunOptions,
    deadline: Instant,
) -> anyhow::Result<StatementStats> {
    let mut stats = StatementStats::default();
    let mut bad_since_event: u64 = 0; // Bad event throttling accumulator
    let mut bad_event_emitted = false; // first-bad visibility (engine H2)
    let resume = state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .load(pipeline_id, st.statement_index)?
        .map(|(c, _, _)| c)
        .unwrap_or(Cursor::None);
    st.source.open(&resume)?;
    // T7 session assembly: same knobs as the serve loop
    let tx_opts = tailrace_transform::TransformOptions {
        max_rows_per_batch: options.poll_rows,
        ..Default::default()
    };
    while Instant::now() < deadline {
        let chunks = st.source.poll(options.poll_rows, options.poll_slice)?;
        if chunks.is_empty() {
            // idle-poll sleep prevents spinning (100% CPU with a FileSource
            // at EOF)
            std::thread::sleep(std::time::Duration::from_millis(50));
            // check flush_interval on idle polls too — the trailing batch
            // after a stream stops must not linger in memory indefinitely
            if st.pending_rows > 0 && st.last_flush.elapsed() >= options.flush_interval {
                let span = sink_flush_span(pipeline_id, &st);
                let _flush_span = span.enter();
                let flush_t0 = Instant::now();
                let ack = st.sink.flush()?;
                let dur_ms = flush_t0.elapsed().as_millis() as u64;
                span.record("rows", ack.rows);
                span.record("flush_ms", dur_ms);
                stats.flushed_rows += ack.rows;
                stats.flushes += 1;
                st.obs_flush(ack.rows, dur_ms);
                st.pending_rows = 0;
                st.last_flush = Instant::now();
                state.lock().unwrap_or_else(|e| e.into_inner()).save(
                    pipeline_id,
                    st.statement_index,
                    &st.source.cursor(),
                    Some(&ack.idempotency_key),
                    st.ledger_totals(),
                )?;
            }
            continue;
        }
        stats.consumed += chunks.len() as u64;
        if let Some(c) = st.counters.as_ref() {
            c.consumed.fetch_add(chunks.len() as u64, Ordering::Relaxed);
        }
        // T5 plan-a: intra-poll parallel decode (same helper as the serve
        // loop — serial below the fan-out floor, merged in chunk order)
        let pd = decode_poll(
            st.decoder.as_ref(),
            &st.pushdown,
            &st.schema,
            &mut st.batch,
            chunks,
            st.bad_row_policy,
            &mut stats,
        )?;
        if pd.skipped > 0 {
            if let Some(c) = st.counters.as_ref() {
                c.skipped.fetch_add(pd.skipped, Ordering::Relaxed);
            }
        }
        handle_bad_rows(
            &st,
            pipeline_id,
            pd.bad_now,
            pd.bad_raws,
            pd.fail_sample,
            &mut bad_since_event,
            &mut bad_event_emitted,
        )?;
        let Some(rb) = pd.merged else {
            continue;
        };
        let out = tailrace_transform::transform_plan(&st.exec_plan, rb, &tx_opts)?; // materialized-plan variant (T7)
                                                                                    // Validate the output column 【order】 before the first row is written
                                                                                    // (see run_statement_serve)
        if let Some(first) = out.first() {
            let out_cols: Vec<String> = first
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            if out_cols != st.sink_columns && !st.sink_columns.is_empty() {
                anyhow::bail!(
                    "transform output column order != sink: output {:?} vs sink {:?} — check the SELECT projection order",
                    out_cols, st.sink_columns
                );
            }
        }
        let rows: u64 = out.iter().map(|b| b.num_rows() as u64).sum();
        if rows > 0 {
            st.sink.write(out)?;
        }
        st.pending_rows += rows;
        // batching boundary reached: flush (idempotency boundary) → cursor
        // commit (at-least-once anchor). Same three-threshold predicate as
        // the serve loop (`sink.buffer-flush.max-rows / max-bytes / interval`)
        if options.flush_due(
            st.pending_rows,
            st.sink.buffered_bytes(),
            st.last_flush.elapsed(),
        ) {
            let span = sink_flush_span(pipeline_id, &st);
            let _flush_span = span.enter();
            let flush_t0 = Instant::now();
            let ack = st.sink.flush()?;
            let dur_ms = flush_t0.elapsed().as_millis() as u64;
            span.record("rows", ack.rows);
            span.record("flush_ms", dur_ms);
            stats.flushed_rows += ack.rows;
            stats.flushes += 1;
            st.obs_flush(ack.rows, dur_ms);
            if let Some(emit) = st.emit.as_ref() {
                emit(
                    "INFO",
                    "FLUSH_OK",
                    &format!(
                        "flush rows={} totals flushed={} flushes={} skipped={} took {}ms",
                        ack.rows,
                        stats.flushed_rows,
                        stats.flushes,
                        stats.skipped,
                        flush_t0.elapsed().as_millis()
                    ),
                    Some(st.statement_index),
                );
            }
            st.pending_rows = 0;
            st.last_flush = Instant::now();
            state.lock().unwrap_or_else(|e| e.into_inner()).save(
                pipeline_id,
                st.statement_index,
                &st.source.cursor(),
                Some(&ack.idempotency_key),
                st.ledger_totals(),
            )?;
        }
    }
    // finalization: flush the remainder + commit the cursor
    if st.pending_rows > 0 {
        let span = sink_flush_span(pipeline_id, &st);
        let _flush_span = span.enter();
        let flush_t0 = Instant::now();
        let ack = st.sink.flush()?;
        let dur_ms = flush_t0.elapsed().as_millis() as u64;
        span.record("rows", ack.rows);
        span.record("flush_ms", dur_ms);
        stats.flushed_rows += ack.rows;
        stats.flushes += 1;
        st.obs_flush(ack.rows, dur_ms);
        if let Some(emit) = st.emit.as_ref() {
            emit(
                "INFO",
                "FLUSH_OK",
                &format!(
                    "final flush rows={} totals flushed={} flushes={} skipped={}",
                    ack.rows, stats.flushed_rows, stats.flushes, stats.skipped
                ),
                Some(st.statement_index),
            );
        }
    }
    state.lock().unwrap_or_else(|e| e.into_inner()).save(
        pipeline_id,
        st.statement_index,
        &st.source.cursor(),
        None,
        st.ledger_totals(),
    )?;
    Ok(stats)
}

/// Bounded-mode EOF guard: a source that stops producing (file at EOF, no
/// more keyset rows) for this many consecutive idle polls is judged
/// exhausted — the bounded run flushes, commits and exits instead of
/// spinning forever on an unreachable `to` (e.g. a `to` past the end of
/// the data). ~3s at the 50ms idle sleep.
const BOUNDED_IDLE_POLLS: u32 = 60;

/// Bounded-mode terminal check: has the source cursor reached `to`? The
/// judgment is per cursor shape — File: byte position; JdbcKey: the keyset
/// value parsed as i64 (a non-numeric keyset never reaches; the idle guard
/// ends those runs); Kafka: EVERY assigned partition's next offset (a
/// single-dimensional `to` over a multi-partition topic needs the console
/// to resolve it per partition first).
fn cursor_reached(c: &Cursor, to: i64) -> bool {
    match c {
        Cursor::File { byte_pos, .. } => *byte_pos as i64 >= to,
        Cursor::JdbcKey { value, .. } => value.parse::<i64>().map(|v| v >= to).unwrap_or(false),
        Cursor::Kafka { offsets, .. } => !offsets.is_empty() && offsets.values().all(|o| *o >= to),
        Cursor::None => false,
    }
}

/// Seed `from` into the source's own cursor shape (cold start of a bounded
/// run — no ledger entry yet). The probe cursor comes from opening the
/// source with `Cursor::None` first (it then carries the real shape:
/// path / keyset / topic). Kafka shapes cannot be seeded from a single
/// dimension (the partition map is unknown here) — the empty map is the
/// no-cursor equivalent, so the statement starts from its declared start
/// position (timestamp→offset resolution belongs to the console side).
fn seed_from_cursor(probe: &Cursor, from: i64) -> Cursor {
    match probe {
        Cursor::File { path, .. } => Cursor::File {
            path: path.clone(),
            byte_pos: from.max(0) as u64,
        },
        Cursor::JdbcKey { key, .. } => Cursor::JdbcKey {
            key: key.clone(),
            value: from.to_string(),
        },
        Cursor::Kafka { topic, group, .. } => {
            tracing::warn!(
                from,
                topic = %topic,
                "bounded run over a kafka-shaped source: a scalar `from` cannot address the \
                 partition map — starting from the statement's declared start position"
            );
            Cursor::Kafka {
                topic: topic.clone(),
                group: group.clone(),
                offsets: Default::default(),
            }
        }
        Cursor::None => Cursor::None,
    }
}

/// Bounded statement thread body (T-A2 backfill): micro-batch loop until the
/// source cursor reaches `to` (or the idle/EOF guard fires), then final
/// flush + cursor commit — the bounded twin of [`run_statement`]. The serve
/// loop (`run_statement_serve`) is NOT modified; this is a parallel body
/// with the same flush-point at-least-once anchor (cursor commits only
/// after a successful write).
fn run_statement_bounded(
    mut st: StatementRuntime,
    pipeline_id: u64,
    state: Arc<Mutex<FileOffsetStore>>,
    options: RunOptions,
    from: i64,
    to: i64,
) -> anyhow::Result<StatementStats> {
    let mut stats = StatementStats::default();
    let mut bad_since_event: u64 = 0; // Bad event throttling accumulator
    let mut bad_event_emitted = false; // first-bad visibility (engine H2)
                                       // Cursor restore order: the (bf-namespaced) ledger first — a mid-job
                                       // restart resumes from the last bounded flush point; on a cold start
                                       // probe-open the source so cursor() carries its real shape, then seed
                                       // `from` into that shape. Neither open commits anything.
    let saved = state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .load(pipeline_id, st.statement_index)?
        .map(|(c, _, _)| c);
    st.source.open(&Cursor::None)?;
    let resume = match saved {
        Some(c) => c,
        None => seed_from_cursor(&st.source.cursor(), from),
    };
    st.source.open(&resume)?;
    // T7 session assembly: same knobs as the serve/standalone loops
    let tx_opts = tailrace_transform::TransformOptions {
        max_rows_per_batch: options.poll_rows,
        ..Default::default()
    };
    let mut idle_polls: u32 = 0;
    loop {
        if cursor_reached(&st.source.cursor(), to) {
            break;
        }
        let chunks = st.source.poll(options.poll_rows, options.poll_slice)?;
        if chunks.is_empty() {
            // idle-poll sleep prevents spinning; the flush_interval still
            // drains a trailing batch (same as the serve/standalone loops)
            idle_polls += 1;
            std::thread::sleep(std::time::Duration::from_millis(50));
            if st.pending_rows > 0 && st.last_flush.elapsed() >= options.flush_interval {
                let span = sink_flush_span(pipeline_id, &st);
                let _flush_span = span.enter();
                let flush_t0 = Instant::now();
                let ack = st.sink.flush()?;
                let dur_ms = flush_t0.elapsed().as_millis() as u64;
                span.record("rows", ack.rows);
                span.record("flush_ms", dur_ms);
                stats.flushed_rows += ack.rows;
                stats.flushes += 1;
                st.obs_flush(ack.rows, dur_ms);
                st.pending_rows = 0;
                st.last_flush = Instant::now();
                state.lock().unwrap_or_else(|e| e.into_inner()).save(
                    pipeline_id,
                    st.statement_index,
                    &st.source.cursor(),
                    Some(&ack.idempotency_key),
                    st.ledger_totals(),
                )?;
            }
            if idle_polls >= BOUNDED_IDLE_POLLS {
                tracing::info!(
                    pipeline_id,
                    statement_idx = st.statement_index,
                    to,
                    "bounded run: source idle past the EOF guard — judged exhausted, exiting"
                );
                break;
            }
            continue;
        }
        idle_polls = 0;
        stats.consumed += chunks.len() as u64;
        if let Some(c) = st.counters.as_ref() {
            c.consumed.fetch_add(chunks.len() as u64, Ordering::Relaxed);
        }
        let pd = decode_poll(
            st.decoder.as_ref(),
            &st.pushdown,
            &st.schema,
            &mut st.batch,
            chunks,
            st.bad_row_policy,
            &mut stats,
        )?;
        if pd.skipped > 0 {
            if let Some(c) = st.counters.as_ref() {
                c.skipped.fetch_add(pd.skipped, Ordering::Relaxed);
            }
        }
        handle_bad_rows(
            &st,
            pipeline_id,
            pd.bad_now,
            pd.bad_raws,
            pd.fail_sample,
            &mut bad_since_event,
            &mut bad_event_emitted,
        )?;
        let Some(rb) = pd.merged else {
            continue;
        };
        let out = tailrace_transform::transform_plan(&st.exec_plan, rb, &tx_opts)?; // materialized-plan variant (T7)
                                                                                    // Validate the output column 【order】 before the first row is written
                                                                                    // (see run_statement_serve)
        if let Some(first) = out.first() {
            let out_cols: Vec<String> = first
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            if out_cols != st.sink_columns && !st.sink_columns.is_empty() {
                anyhow::bail!(
                    "transform output column order != sink: output {:?} vs sink {:?} — check the SELECT projection order",
                    out_cols, st.sink_columns
                );
            }
        }
        let rows: u64 = out.iter().map(|b| b.num_rows() as u64).sum();
        if rows > 0 {
            st.sink.write(out)?;
        }
        st.pending_rows += rows;
        // same three-threshold flush predicate as the serve/standalone loops
        // (the cursor commits only behind a successful flush — the
        // at-least-once anchor)
        if options.flush_due(
            st.pending_rows,
            st.sink.buffered_bytes(),
            st.last_flush.elapsed(),
        ) {
            let span = sink_flush_span(pipeline_id, &st);
            let _flush_span = span.enter();
            let flush_t0 = Instant::now();
            let ack = st.sink.flush()?;
            let dur_ms = flush_t0.elapsed().as_millis() as u64;
            span.record("rows", ack.rows);
            span.record("flush_ms", dur_ms);
            stats.flushed_rows += ack.rows;
            stats.flushes += 1;
            st.obs_flush(ack.rows, dur_ms);
            st.pending_rows = 0;
            st.last_flush = Instant::now();
            state.lock().unwrap_or_else(|e| e.into_inner()).save(
                pipeline_id,
                st.statement_index,
                &st.source.cursor(),
                Some(&ack.idempotency_key),
                st.ledger_totals(),
            )?;
        }
    }
    // finalization: flush the remainder + commit the cursor (same as the
    // serve/standalone finalization)
    if st.pending_rows > 0 {
        let span = sink_flush_span(pipeline_id, &st);
        let _flush_span = span.enter();
        let flush_t0 = Instant::now();
        let ack = st.sink.flush()?;
        let dur_ms = flush_t0.elapsed().as_millis() as u64;
        span.record("rows", ack.rows);
        span.record("flush_ms", dur_ms);
        stats.flushed_rows += ack.rows;
        stats.flushes += 1;
        st.obs_flush(ack.rows, dur_ms);
    }
    state.lock().unwrap_or_else(|e| e.into_inner()).save(
        pipeline_id,
        st.statement_index,
        &st.source.cursor(),
        None,
        st.ledger_totals(),
    )?;
    Ok(stats)
}

fn arrow_type(t: &str) -> DataType {
    match t {
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "bool" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn build_decoder(obj: &TableObject) -> anyhow::Result<Box<dyn Decoder>> {
    match obj.format.as_deref() {
        Some("ogg") | None => Ok(Box::new(OggDecoder)),
        Some("json") | Some("jsonl") => Ok(Box::new(JsonDecoder)),
        Some("csv") => Ok(Box::new(CsvDecoder)),
        Some("parquet") => Ok(Box::new(ParquetDecoder)),
        Some("debezium-json") => Ok(Box::new(DebeziumJsonDecoder)),
        Some(other) => anyhow::bail!("decode format unsupported: {other}"),
    }
}

/// Convenience entry: catalog file → assemble (default registry) → run →
/// stats.
pub fn run_standalone(
    spec_path: &std::path::Path,
    state_dir: &std::path::Path,
    options: RunOptions,
) -> anyhow::Result<(u64, Vec<StatementStats>)> {
    let catalog = tailrace_catalog::FileCatalog::load(spec_path)?;
    let id = catalog.pipeline_id();
    let reg = default_registry();
    let mut runner = PipelineRunner::assemble(
        &catalog,
        state_dir,
        options,
        None,
        BadRowPolicy::default(),
        &reg,
    )?;
    let stats = runner.run()?;
    Ok((id, stats))
}

/// Backfill ledger namespace (T-A2): a backfill job commits its cursors
/// under `bf-{job}` beside the main ledger dir, isolating them from the
/// main pipeline's `offsets-{id}.json` — a backfill (success or failure
/// alike) never reads or advances main-pipeline cursors, and the main
/// pipeline keeps running undisturbed. Assemble the bounded runner with
/// THIS dir, never the main ledger dir. A job id already carrying the
/// `bf-` marker is not double-prefixed.
pub fn backfill_state_dir(state_dir: &std::path::Path, job_id: &str) -> std::path::PathBuf {
    let job = job_id.strip_prefix("bf-").unwrap_or(job_id);
    state_dir.join(format!("bf-{job}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T6: the three-threshold flush boundary in isolation — each dimension
    /// fires alone with the other two held far below their thresholds, and
    /// nothing fires when all three are below (Doris Flink connector
    /// `sink.buffer-flush.max-rows / max-bytes / interval` semantics: any
    /// satisfied → flush).
    #[test]
    fn t6_flush_boundary_threshold_isolation() {
        let opts = RunOptions {
            flush_rows: 10_000,
            flush_interval: Duration::from_secs(10),
            flush_bytes: 8 * 1024 * 1024,
            ..Default::default()
        };
        // rows alone (no bytes buffered, no time elapsed)
        assert!(!opts.flush_due(9_999, 0, Duration::ZERO));
        assert!(opts.flush_due(10_000, 0, Duration::ZERO));
        // bytes alone
        assert!(!opts.flush_due(0, 8 * 1024 * 1024 - 1, Duration::ZERO));
        assert!(opts.flush_due(0, 8 * 1024 * 1024, Duration::ZERO));
        // time alone
        assert!(!opts.flush_due(0, 0, Duration::from_millis(9_999)));
        assert!(opts.flush_due(0, 0, Duration::from_secs(10)));
        // flush_bytes = 0 disables the byte boundary entirely (the honest
        // degradation for sinks that cannot measure)
        let no_bytes = RunOptions {
            flush_bytes: 0,
            ..opts.clone()
        };
        assert!(!no_bytes.flush_due(0, u64::MAX, Duration::from_secs(9)));
        // all three far below their boundaries → no flush
        assert!(!opts.flush_due(1, 1, Duration::from_millis(1)));
    }

    /// T5 equivalence acceptance: the parallel decode path (fan-out above
    /// PARALLEL_DECODE_MIN_CHUNKS×2, one ColBatch per worker, concat_batches
    /// merge) must produce a **byte-identical** RecordBatch to the serial
    /// decode of the same chunks (arrow JSON-lines encoding is
    /// order-sensitive, so byte equality pins row order too), with identical
    /// stats (appended/skipped/bad) and identical bad-row payloads (plain
    /// malformed rows come back byte-identical per the T3 note).
    #[test]
    fn t5_parallel_decode_equivalence() -> anyhow::Result<()> {
        use datafusion::arrow::json::LineDelimitedWriter;
        let cols = [
            ("op_type".to_string(), "utf8".to_string()),
            ("order_no".to_string(), "utf8".to_string()),
            ("seq".to_string(), "int64".to_string()),
        ];
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|(n, t)| Field::new(n.clone(), arrow_type(t), true))
                .collect::<Vec<_>>(),
        ));
        let n = 4096; // → decode_workers ≥ 2 (floor is 512×2)
        assert!(
            decode_workers(n) >= 2,
            "this poll size must take the parallel path"
        );
        // pushdown skip only exists on the OGG path (JsonDecoder ignores the
        // whitelist) — use OGG-shaped rows so Skip/Append/Bad all occur
        let mut pushdown = Pushdown::default();
        pushdown
            .eq_whitelist
            .insert("op_type".into(), vec!["I".into()]);
        let mut chunks = Vec::with_capacity(n);
        for i in 0..n {
            let raw: Vec<u8> = match i % 8 {
                0 => format!(r#"{{"op_type":"D","table":"T","ts":{i},"after":{{}}}}"#).into_bytes(),
                3 => b"{broken".to_vec(), // plain malformed — byte-identical Bad return
                _ => format!(
                    r#"{{"op_type":"I","table":"T","ts":{i},"after":{{"order_no":"O-{i:06}","seq":{i}}}}}"#
                )
                .into_bytes(),
            };
            chunks.push(SourceChunk::kafka(raw, (i % 3) as i32, i as i64));
        }
        // —— parallel path ——
        let mut reuse = ColBatch::new(schema.clone(), 128);
        let mut stats_par = StatementStats::default();
        let pd = decode_poll(
            &OggDecoder,
            &pushdown,
            &schema,
            &mut reuse,
            chunks.clone(),
            BadRowPolicy::Dlq,
            &mut stats_par,
        )?;
        // —— serial reference (the pre-T5 inline loop, verbatim) ——
        let mut ref_batch = ColBatch::new(schema.clone(), n);
        let mut ref_stats = StatementStats::default();
        let mut ref_bad_raws: Vec<Vec<u8>> = Vec::new();
        for c in chunks {
            match OggDecoder.decode(c.raw, &mut ref_batch, &pushdown) {
                Decoded::Append => ref_stats.appended += 1,
                Decoded::Skip => ref_stats.skipped += 1,
                Decoded::Bad(raw) => {
                    ref_stats.bad_rows += 1;
                    ref_bad_raws.push(raw);
                }
            }
        }
        assert_eq!(
            (stats_par.appended, stats_par.skipped, stats_par.bad_rows),
            (ref_stats.appended, ref_stats.skipped, ref_stats.bad_rows),
            "per-outcome stats identical"
        );
        assert_eq!(pd.bad_raws.len(), ref_bad_raws.len());
        for (a, b) in pd.bad_raws.iter().zip(ref_bad_raws.iter()) {
            assert_eq!(a, b, "bad-row payloads identical and in chunk order");
        }
        let rb_ref = ref_batch.finish()?;
        let rb_par = pd.merged.expect("appended rows present");
        let enc = |rb: &datafusion::arrow::record_batch::RecordBatch| -> anyhow::Result<Vec<u8>> {
            let mut buf = Vec::new();
            {
                let mut w = LineDelimitedWriter::new(&mut buf);
                w.write(rb)?;
                w.finish()?;
            }
            Ok(buf)
        };
        assert_eq!(
            enc(&rb_par)?,
            enc(&rb_ref)?,
            "parallel decode is byte-identical to serial (row order preserved)"
        );
        // 汇的行数也逐一相等（顺带把 Skip/Bad 的份额算清楚）
        let expected_appended = (0..n).filter(|i| i % 8 != 0 && i % 8 != 3).count();
        assert_eq!(stats_par.appended as usize, expected_appended);
        // Fail 策略在并行路径的形状：bad_now 统计**所有**坏行（含不进
        // bad_raws 的），首样本在 chunk 序——串行路径由 contract 套件的
        // bad_row_policy_branches 钉住，这里补并行路径
        let mut reuse2 = ColBatch::new(schema.clone(), 128);
        let mut stats_fail = StatementStats::default();
        let chunks2: Vec<SourceChunk> = (0..n)
            .map(|i| {
                let raw: Vec<u8> = if i % 8 == 3 {
                    b"{broken".to_vec()
                } else {
                    format!(
                        r#"{{"op_type":"I","table":"T","ts":{i},"after":{{"order_no":"O-{i:06}","seq":{i}}}}}"#
                    )
                    .into_bytes()
                };
                SourceChunk::kafka(raw, (i % 3) as i32, i as i64)
            })
            .collect();
        let pd_fail = decode_poll(
            &OggDecoder,
            &pushdown,
            &schema,
            &mut reuse2,
            chunks2,
            BadRowPolicy::Fail,
            &mut stats_fail,
        )?;
        assert_eq!(
            pd_fail.bad_now, ref_stats.bad_rows,
            "bad_now counts all bad rows under Fail"
        );
        assert!(
            pd_fail.bad_raws.is_empty(),
            "Fail policy quarantines nothing"
        );
        assert!(
            pd_fail
                .fail_sample
                .as_deref()
                .is_some_and(|s| s.contains("{broken")),
            "first bad-row sample in chunk order: {:?}",
            pd_fail.fail_sample
        );
        Ok(())
    }

    /// T7 wiring acceptance: the runtime materializes each statement's plan
    /// via `tailrace_ir::build_exec_plan` (declared schema) at assembly and
    /// executes micro-batches via `tailrace_transform::transform_plan` with
    /// batch size fixed to `poll_rows` — a first 1-row micro-batch must NOT
    /// pin the session batch size (later micro-batches still execute as
    /// single batches), and the projection/filter semantics are unchanged.
    /// Each statement runs on its own thread (one statement = one thread-local
    /// session, matching the statement-thread runtime).
    #[test]
    fn t7_exec_plan_wiring() -> anyhow::Result<()> {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::record_batch::RecordBatch;
        let cols = vec![
            ("op_type".to_string(), "utf8".to_string()),
            ("order_no".to_string(), "utf8".to_string()),
            ("seq".to_string(), "int64".to_string()),
        ];
        // the declared-schema plan the runtime assembles (nullability matches
        // the ColBatch schema built in assemble)
        let plan = std::sync::Arc::new(tailrace_ir::build_exec_plan(
            "SELECT order_no, seq * 10 AS seq10 FROM src WHERE op_type <> 'D'",
            &cols,
        )?);
        let cols_for_batches = cols.clone();
        let mk_batch = move |rows: usize| -> anyhow::Result<RecordBatch> {
            let schema = Arc::new(Schema::new(
                cols_for_batches
                    .iter()
                    .map(|(n, t)| Field::new(n.clone(), arrow_type(t), true))
                    .collect::<Vec<_>>(),
            ));
            let ops: Vec<String> = (0..rows)
                .map(|i| if i % 2 == 0 { "I" } else { "D" }.to_string())
                .collect();
            let orders: Vec<String> = (0..rows).map(|i| format!("O-{i:06}")).collect();
            Ok(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(ops)),
                    Arc::new(StringArray::from(orders)),
                    Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                ],
            )?)
        };
        let opts = tailrace_transform::TransformOptions {
            max_rows_per_batch: RunOptions::default().poll_rows, // = the runtime wiring
            ..Default::default()
        };
        std::thread::spawn(move || -> anyhow::Result<()> {
            // first micro-batch: a single row (the old batch_size pinning trigger)
            let first = tailrace_transform::transform_plan(&plan, mk_batch(1)?, &opts)?;
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].num_rows(), 1, "I row passes the filter");
            // later 8001-row micro-batch (4001 I rows survive): still ONE
            // executed batch — the old pinning degraded aggregate-shaped
            // statements to single-row batches (see transform crate tests)
            let out = tailrace_transform::transform_plan(&plan, mk_batch(8001)?, &opts)?;
            assert_eq!(out.len(), 1, "later micro-batch executes as one batch");
            let rows: usize = out.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 4001, "D rows filtered, I rows flow");
            // projection + column order (the sink column-order contract)
            let names: Vec<String> = out[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(names, vec!["order_no".to_string(), "seq10".to_string()]);
            // seq10 = seq * 10 spot check (first I row: seq=0 → 0; second: seq=2 → 20)
            let seq10 = out[0]
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("seq10 int64");
            assert_eq!(seq10.value(0), 0);
            assert_eq!(seq10.value(1), 20);
            Ok(())
        })
        .join()
        .expect("wiring test thread")?;
        Ok(())
    }

    /// Metadata-based topic presence probe (the live-gate precondition —
    /// the seeded tailrace.discover.test topic must actually exist on the
    /// broker; a bare TCP probe misfires on unrelated listeners).
    fn kafka_topic_exists(bootstrap: &str, topic: &str) -> bool {
        use rdkafka::consumer::{BaseConsumer, Consumer};
        let mut cfg = rdkafka::config::ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap);
        let client: BaseConsumer = match cfg.create() {
            Ok(c) => c,
            Err(_) => return false,
        };
        client
            .fetch_metadata(None, std::time::Duration::from_secs(3))
            .map(|m| m.topics().iter().any(|t| t.name() == topic))
            .unwrap_or(false)
    }

    /// Acceptance: a standalone dual-statement pipeline — write Doris + send
    /// a Kafka notification, asserted independently.
    /// Preconditions: the tailrace compose stack (docker-compose.yml:
    /// tailrace-kafka on 9092 + tailrace-doris-be on 8040) reachable,
    /// tailrace.discover.test seeded with ogg samples + env
    /// TAILRACE_TEST_DORIS_AUTH=Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=.
    /// The gate checks the *documented* precondition (the seeded topic), not
    /// bare TCP reachability — an unrelated listener on 9092 (another
    /// project's stack) must not turn this into a spurious failure.
    #[test]
    fn standalone_dual_statement_live() -> anyhow::Result<()> {
        if std::net::TcpStream::connect("localhost:9092").is_err()
            || std::net::TcpStream::connect("localhost:8040").is_err()
        {
            return Ok(()); // CI-safe
        }
        if !kafka_topic_exists("localhost:9092", "tailrace.discover.test") {
            tracing::warn!(
                "[live-gate] kafka on 9092 has no seeded tailrace.discover.test topic \
                 (another project's stack?) — skipping the live acceptance"
            );
            return Ok(()); // local-liveness only
        }
        std::env::set_var(
            "TAILRACE_TEST_DORIS_AUTH",
            "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=",
        );
        // notification sink topic (an exclusive name; reruns are idempotent —
        // labels / group cursors are isolated)
        let dir = std::env::temp_dir().join(format!(
            "tailrace-e2e-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis()
        ));
        std::fs::create_dir_all(&dir)?;
        let spec = dir.join("pipeline.toml");
        std::fs::write(
            &spec,
            r#"
[pipeline]
id = 3201
name = "e2e-acceptance"

[[datasource]]
name = "k"
type = "kafka"
bootstrap = "localhost:9092"

[[datasource]]
name = "d"
type = "doris"
stream_load_url = "http://localhost:8040/api/tailrace_spike/ogg_event/_stream_load"
auth_env = "TAILRACE_TEST_DORIS_AUTH"

[[table]]
datasource = "k"
name = "tailrace.discover.test"
format = "ogg"
columns = [["op_type","utf8"],["ogg_table","utf8"],["ogg_ts","int64"],["order_no","utf8"],["batch_no","utf8"],["station","utf8"],["seq","int64"],["ratio","utf8"],["payload","utf8"],["extra_col","utf8"]]

[[table]]
datasource = "d"
name = "ogg_event"
kind = "doris_table"
label_prefix = "e2eacc"
columns = [["order_no","utf8"],["batch_no","utf8"],["station","utf8"],["op_type","utf8"],["seq","int64"],["payload","utf8"],["etl_time_ms","int64"]]

[[table]]
datasource = "k"
name = "tailrace.e2e.notify"
format = "json"
columns = [["batch_key","utf8"],["op_type","utf8"],["ogg_ts","int64"]]

[[statement]]
source = "k.tailrace.discover.test"
group = "tr.acc.s0"
sink = "d.ogg_event"
pushdown = ["I","U"]
select_sql = "SELECT order_no, batch_no, station, op_type, seq, payload, ogg_ts AS etl_time_ms FROM src WHERE op_type <> 'D'"

[[statement]]
source = "k.tailrace.discover.test"
group = "tr.acc.s1"
sink = "k.tailrace.e2e.notify"
pushdown = ["I","U"]
select_sql = "SELECT order_no AS batch_key, op_type, ogg_ts FROM src WHERE op_type <> 'D'"
"#,
        )?;
        // create the notification topic (idempotent)
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let admin: rdkafka::admin::AdminClient<_> = rdkafka::config::ClientConfig::new()
                    .set("bootstrap.servers", "localhost:9092")
                    .create()?;
                use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
                let _ = admin
                    .create_topics(
                        &[NewTopic::new(
                            "tailrace.e2e.notify",
                            1,
                            TopicReplication::Fixed(1),
                        )],
                        &AdminOptions::new(),
                    )
                    .await;
                Ok::<(), anyhow::Error>(())
            })?;

        let opts = RunOptions {
            duration: Duration::from_secs(8),
            flush_rows: 1,
            flush_interval: Duration::from_secs(2),
            flush_bytes: 0, // byte boundary wired but not asserted here
            poll_rows: 100,
            poll_slice: Duration::from_millis(300),
            ..Default::default()
        };
        let (id, stats) = run_standalone(&spec, &dir, opts)?;
        assert_eq!(id, 3201);
        assert_eq!(stats.len(), 2, "two statements");
        // statement 0: writes Doris — flushed rows > 0 (seeded ogg samples in
        // discover.test)
        assert!(
            stats[0].flushed_rows > 0,
            "the Doris statement must have written: {:?}",
            stats[0]
        );
        assert!(stats[0].flushes >= 1);
        // statement 1: sends the Kafka notification — flushed rows > 0
        assert!(
            stats[1].flushed_rows > 0,
            "the notification statement must have written: {:?}",
            stats[1]
        );
        // the cursor ledger landed (the resume anchor)
        assert!(dir.join("offsets-3201.json").exists());
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
