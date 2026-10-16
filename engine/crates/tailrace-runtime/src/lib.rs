//! tailrace-runtime: pipeline executor — the engineered micro-batch main loop.
//!
//! Each statement is an independent data flow:
//! `source.poll(8k|100ms) → decode (constant-predicate early filter) →
//! datafusion transform → sink.write (batching) → on boundary sink.flush +
//! state.save (at-least-once anchor: the cursor commits only after a
//! successful write)`.
//! Statements never block each other (each advances one micro-batch round per
//! turn; each runs on its own thread).

pub mod dlq;
pub mod pipeline_log;
pub mod registry;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::datatypes::{DataType, Field, Schema};

use dlq::{BadRowPolicy, BadRowRecord};
use pipeline_log::PipelineLogHub;
use tailrace_catalog::{CatalogProvider, StatementSpec, TableObject};
use tailrace_connector::ConnectorRegistry;
use tailrace_decode::{
    ColBatch, CsvDecoder, Decoded, Decoder, JsonDecoder, OggDecoder, ParquetDecoder, Pushdown,
};
use tailrace_ir::Cursor;
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
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Max run duration
    pub duration: Duration,
    /// sink batching row boundary
    pub flush_rows: u64,
    /// sink batching time boundary
    pub flush_interval: Duration,
    /// sink batching byte boundary (fewer stream loads at low traffic — flush
    /// once N bytes accumulate)
    pub flush_bytes: u64,
    /// micro-batch pull row cap
    pub poll_rows: usize,
    /// micro-batch pull time slice
    pub poll_slice: Duration,
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
        }
    }
}

/// One statement's execution assembly (resolved at compile/deploy time; zero
/// catalog queries on the runtime hot path).
/// Hot-path event emitter: (level, kind, message, statement_idx).
type EmitFn = Box<dyn Fn(&str, &str, &str, Option<u32>) + Send>;

struct StatementRuntime {
    spec: StatementSpec,
    source: Box<dyn tailrace_connector::Source>,
    decoder: Box<dyn Decoder>,
    pushdown: Pushdown,
    sink: Box<dyn tailrace_connector::Sink>,
    /// sink-side column list (transform output column-order check)
    sink_columns: Vec<String>,
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

    fn obs_flush(&self, ack_rows: u64) {
        if let Some(c) = self.counters.as_ref() {
            c.flushed
                .fetch_add(ack_rows, std::sync::atomic::Ordering::Relaxed);
            c.flushes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            c.last_flush_ms
                .store(now_ms(), std::sync::atomic::Ordering::Relaxed);
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
            let batch = ColBatch::new(schema, options.poll_rows);
            let _ = state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .load(pipeline_id, idx)?; // open = restore (cursor passed to source.open)
            let sink_cols = sink_obj.columns.iter().map(|(n, _)| n.clone()).collect();
            let dlq = (bad_row_policy == BadRowPolicy::Dlq)
                .then(|| dlq::BadRowDlq::new(state_dir, pipeline_id, idx));
            statements.push(StatementRuntime {
                spec,
                source,
                decoder,
                pushdown,
                sink,
                sink_columns: sink_cols,
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
        .unwrap()
        .load(pipeline_id, st.statement_index)?
        .map(|(c, _, _)| c)
        .unwrap_or(Cursor::None);
    st.source.open(&resume)?;
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
                span.record("rows", ack.rows);
                span.record("flush_ms", flush_t0.elapsed().as_millis() as u64);
                stats.flushed_rows += ack.rows;
                stats.flushes += 1;
                st.obs_flush(ack.rows);
                st.pending_rows = 0;
                st.last_flush = Instant::now();
                state.lock().unwrap().save(
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
        // Bad-row handling (engine H2): policy dispatch inside the chunk
        // loop, shared post-processing after it
        let mut bad_now: u64 = 0;
        let mut bad_raws: Vec<Vec<u8>> = Vec::new();
        let mut fail_sample: Option<String> = None;
        for chunk in chunks {
            match st.decoder.decode(&chunk.raw, &mut st.batch, &st.pushdown) {
                Decoded::Append => stats.appended += 1,
                Decoded::Skip => {
                    stats.skipped += 1;
                    if let Some(c) = st.counters.as_ref() {
                        c.skipped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Decoded::Bad => {
                    stats.bad_rows += 1;
                    bad_now += 1;
                    match st.bad_row_policy {
                        BadRowPolicy::Fail => {
                            if fail_sample.is_none() {
                                fail_sample = Some(
                                    String::from_utf8_lossy(&chunk.raw)
                                        .chars()
                                        .take(120)
                                        .collect(),
                                );
                            }
                        }
                        BadRowPolicy::Dlq => bad_raws.push(chunk.raw.clone()),
                        BadRowPolicy::Skip => {}
                    }
                }
            }
        }
        handle_bad_rows(
            &st,
            pipeline_id,
            bad_now,
            bad_raws,
            fail_sample,
            &mut bad_since_event,
            &mut bad_event_emitted,
        )?;
        if st.batch.is_empty() {
            continue;
        }
        let rb = st.batch.finish()?;
        let out = tailrace_transform::transform_cached(&st.spec.select_sql, rb)?; // cached variant
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
        // flush_bytes byte-boundary wiring (previously declared but never
        // checked — a dead config). Sinks whose buffered_bytes()=0 (Kafka's
        // internal queue is unmeasurable) degrade honestly: this condition
        // never triggers for them.
        if st.pending_rows >= options.flush_rows
            || st.last_flush.elapsed() >= options.flush_interval
            || (options.flush_bytes > 0 && st.sink.buffered_bytes() >= options.flush_bytes)
        {
            let span = sink_flush_span(pipeline_id, &st);
            let _flush_span = span.enter();
            let flush_t0 = Instant::now();
            let ack = st.sink.flush()?;
            span.record("rows", ack.rows);
            span.record("flush_ms", flush_t0.elapsed().as_millis() as u64);
            stats.flushed_rows += ack.rows;
            stats.flushes += 1;
            st.obs_flush(ack.rows);
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
            state.lock().unwrap().save(
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
        span.record("rows", ack.rows);
        span.record("flush_ms", flush_t0.elapsed().as_millis() as u64);
        stats.flushed_rows += ack.rows;
        stats.flushes += 1;
        st.obs_flush(ack.rows);
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
    state.lock().unwrap().save(
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
        .unwrap()
        .load(pipeline_id, st.statement_index)?
        .map(|(c, _, _)| c)
        .unwrap_or(Cursor::None);
    st.source.open(&resume)?;
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
                span.record("rows", ack.rows);
                span.record("flush_ms", flush_t0.elapsed().as_millis() as u64);
                stats.flushed_rows += ack.rows;
                stats.flushes += 1;
                st.obs_flush(ack.rows);
                st.pending_rows = 0;
                st.last_flush = Instant::now();
                state.lock().unwrap().save(
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
        // Bad-row handling (engine H2): policy dispatch inside the chunk
        // loop, shared post-processing after it
        let mut bad_now: u64 = 0;
        let mut bad_raws: Vec<Vec<u8>> = Vec::new();
        let mut fail_sample: Option<String> = None;
        for chunk in chunks {
            match st.decoder.decode(&chunk.raw, &mut st.batch, &st.pushdown) {
                Decoded::Append => stats.appended += 1,
                Decoded::Skip => {
                    stats.skipped += 1;
                    if let Some(c) = st.counters.as_ref() {
                        c.skipped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Decoded::Bad => {
                    stats.bad_rows += 1;
                    bad_now += 1;
                    match st.bad_row_policy {
                        BadRowPolicy::Fail => {
                            if fail_sample.is_none() {
                                fail_sample = Some(
                                    String::from_utf8_lossy(&chunk.raw)
                                        .chars()
                                        .take(120)
                                        .collect(),
                                );
                            }
                        }
                        BadRowPolicy::Dlq => bad_raws.push(chunk.raw.clone()),
                        BadRowPolicy::Skip => {}
                    }
                }
            }
        }
        handle_bad_rows(
            &st,
            pipeline_id,
            bad_now,
            bad_raws,
            fail_sample,
            &mut bad_since_event,
            &mut bad_event_emitted,
        )?;
        if st.batch.is_empty() {
            continue;
        }
        let rb = st.batch.finish()?;
        let out = tailrace_transform::transform_cached(&st.spec.select_sql, rb)?; // cached variant
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
        // commit (at-least-once anchor). flush_bytes byte boundary (same
        // condition as the serve loop; unmeasurable sinks degrade and never
        // trigger)
        if st.pending_rows >= options.flush_rows
            || st.last_flush.elapsed() >= options.flush_interval
            || (options.flush_bytes > 0 && st.sink.buffered_bytes() >= options.flush_bytes)
        {
            let span = sink_flush_span(pipeline_id, &st);
            let _flush_span = span.enter();
            let flush_t0 = Instant::now();
            let ack = st.sink.flush()?;
            span.record("rows", ack.rows);
            span.record("flush_ms", flush_t0.elapsed().as_millis() as u64);
            stats.flushed_rows += ack.rows;
            stats.flushes += 1;
            st.obs_flush(ack.rows);
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
            state.lock().unwrap().save(
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
        span.record("rows", ack.rows);
        span.record("flush_ms", flush_t0.elapsed().as_millis() as u64);
        stats.flushed_rows += ack.rows;
        stats.flushes += 1;
        st.obs_flush(ack.rows);
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
    state.lock().unwrap().save(
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

#[cfg(test)]
mod tests {
    use super::*;

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
