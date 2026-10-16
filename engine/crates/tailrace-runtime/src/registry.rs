//! PipelineRegistry — the serve-mode execution loop.
//!
//! Responsibilities: plan push → assemble PipelineRunner → resident execution
//! thread; pause/resume/stop atomic flags drive the real control; a
//! pipeline-level stop removes the registration after the final flush + cursor
//! commit. at-least-once semantics inside the thread are identical to
//! standalone (cursor commits only after successful writes); the only
//! difference is the unlimited duration (the stop flag replaces the deadline).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};

/// Registry event callback: (pipeline_id, kind, ts_ms, message).
pub type EventSink = Arc<dyn Fn(&str, &str, u64, &str) + Send + Sync>;
use std::sync::{Arc, Mutex};

use crate::pipeline_log::{LogEntry, PipeCounters, PipelineLogHub, StatementCountersSnapshot};
use crate::{PipelineRunner, RunOptions};
use tailrace_catalog::{PlanCatalog, StatementSpec};
use tailrace_connector::ConnectorRegistry;
use tailrace_ir::Cursor;
use tailrace_state::OffsetStore as _;

/// Poison-data criterion — consecutive failures without progress (an
/// unchanged cursor fingerprint across restarts = a deterministic fault point,
/// e.g. a poison row or a config error; infrastructure outages usually make
/// progress again after some restart and never hit this limit).
/// Deliberately not configurable: exposing it would open a poison-data
/// infinite-loop backdoor.
const NO_PROGRESS_LIMIT: u32 = 3;

/// Ledger cumulative seeds: at assembly time read each statement's last
/// committed consumed/flushed/flushes and inject them as PipeCounters
/// initial values — stats/heartbeat totals stay continuous across restarts.
fn load_counter_seeds(
    state_dir: &std::path::Path,
    pipeline_id: u64,
    n_statements: usize,
) -> Vec<tailrace_state::StatementTotals> {
    let Ok(store) = tailrace_state::FileOffsetStore::new(state_dir) else {
        return vec![Default::default(); n_statements];
    };
    (0..n_statements)
        .map(|i| {
            store
                .load(pipeline_id, i as u32)
                .ok()
                .flatten()
                .and_then(|(_, _, t)| t)
                .unwrap_or_default()
        })
        .collect()
}

/// Persistent cursor progress fingerprint of a pipeline (each statement's
/// cursor folded into one u64) — the poison-data criterion basis.
fn cursor_progress(state_dir: &std::path::Path, pipeline_id: u64, n_statements: usize) -> u64 {
    let Ok(store) = tailrace_state::FileOffsetStore::new(state_dir) else {
        return 0;
    };
    let mut acc: u64 = 0;
    for i in 0..n_statements {
        if let Ok(Some((cur, _, _))) = store.load(pipeline_id, i as u32) {
            let h = match &cur {
                Cursor::Kafka { offsets, .. } => offsets.iter().fold(7u64, |a, (p, o)| {
                    a.wrapping_mul(31)
                        .wrapping_add(*p as u64)
                        .wrapping_add(*o as u64)
                }),
                Cursor::File { byte_pos, .. } => 11 + *byte_pos,
                Cursor::JdbcKey { .. } => 13,
                Cursor::None => 5,
            };
            acc = acc.wrapping_mul(1_000_003).wrapping_add(h);
        }
    }
    acc
}

/// Pipeline control flags (three states; u8 encoding: 0=RUNNING 1=PAUSED 2=STOP).
#[derive(Clone)]
pub struct ControlFlags(Arc<ControlInner>);

struct ControlInner {
    state: AtomicU8,
}

pub const STATE_RUNNING: u8 = 0;
pub const STATE_PAUSED: u8 = 1;
pub const STATE_STOPPED: u8 = 2;
/// Statement-thread failure terminal state — snapshots/heartbeats show FAILED
/// truthfully (fixes the "zombie RUNNING").
pub const STATE_FAILED: u8 = 3;

impl ControlFlags {
    fn new() -> Self {
        Self(Arc::new(ControlInner {
            state: AtomicU8::new(STATE_RUNNING),
        }))
    }

    pub fn pause(&self) {
        self.0.state.store(STATE_PAUSED, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.0.state.store(STATE_RUNNING, Ordering::SeqCst);
    }

    pub fn stop(&self) {
        self.0.state.store(STATE_STOPPED, Ordering::SeqCst);
    }

    pub fn state(&self) -> u8 {
        self.0.state.load(Ordering::SeqCst)
    }

    pub(crate) fn is_running(&self) -> bool {
        self.state() == STATE_RUNNING
    }

    /// Failure terminal state (set once; thereafter behaves like stopped for
    /// loop-exit purposes).
    pub fn fail(&self) {
        self.0.state.store(STATE_FAILED, Ordering::SeqCst);
    }
    pub(crate) fn is_stopped(&self) -> bool {
        self.0.state.load(Ordering::SeqCst) >= STATE_STOPPED
    }
}

/// Pipeline registry entry.
struct PipelineEntry {
    flags: ControlFlags,
    handle: Option<std::thread::JoinHandle<()>>,
    statement_count: usize,
    /// Last execution outcome (Ok=running normally / Err=failure reason).
    last_error: Arc<Mutex<Option<String>>>,
    /// dispatch id of this push (the Flink ExecutionAttemptID equivalent —
    /// a repeated push of the same id is idempotently rejected)
    dispatch_id: u64,
    /// Observability trio: atomic counters (read by the stats endpoint and
    /// piggybacked on heartbeats).
    counters: Arc<PipeCounters>,
}

/// Pipeline registry (serve mode): plan push starts threads, actions control
/// them, snapshots query them.
pub struct PipelineRegistry {
    inner: Mutex<HashMap<u64, PipelineEntry>>,
    state_dir: std::path::PathBuf,
    /// Connector registry used at assembly (passed in by the binary's
    /// build_registry, or the crate default).
    connectors: ConnectorRegistry,
    /// stream_load_url host allowlist (engine H1): `None` = the loopback
    /// default; `Some` = exactly these hosts. A plan body cannot point a
    /// Doris stream load (and its Basic credential) anywhere outside it.
    stream_load_hosts: Option<std::collections::HashSet<String>>,
    /// Event callback (in serve mode injected by the node main into the
    /// heartbeat EventRing).
    event_sink: Mutex<Option<EventSink>>,
    /// Observability trio: per-pipeline structured event log hub (read by the
    /// logs endpoint).
    log_hub: Arc<PipelineLogHub>,
}

impl PipelineRegistry {
    pub fn new(state_dir: &std::path::Path) -> Self {
        Self::with_connectors(state_dir, crate::default_registry())
    }

    /// Construct with an explicit connector registry (the binary's
    /// build_registry product).
    pub fn with_connectors(state_dir: &std::path::Path, connectors: ConnectorRegistry) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            state_dir: state_dir.to_path_buf(),
            connectors,
            stream_load_hosts: None,
            event_sink: Mutex::new(None),
            // WARN/ERROR events mirror to state_dir/events.log (a node-side
            // trail for when the console is unreachable)
            log_hub: Arc::new(PipelineLogHub::with_mirror(
                &state_dir.to_path_buf().join("events.log"),
            )),
        }
    }

    /// With an explicit stream_load_url host allowlist (engine H1; `None`
    /// keeps the loopback-only default — see
    /// `tailrace_catalog::stream_load_url_allowed`).
    pub fn with_stream_load_hosts(
        state_dir: &std::path::Path,
        connectors: ConnectorRegistry,
        stream_load_hosts: Option<std::collections::HashSet<String>>,
    ) -> Self {
        let mut reg = Self::with_connectors(state_dir, connectors);
        reg.stream_load_hosts = stream_load_hosts;
        reg
    }

    /// Log hub access (the API logs route).
    pub fn log_hub(&self) -> Arc<PipelineLogHub> {
        Arc::clone(&self.log_hub)
    }

    /// Inject the event callback (level, kind, pipeline_id, message).
    pub fn set_event_sink(&self, f: EventSink) {
        *self.event_sink.lock().unwrap_or_else(|e| e.into_inner()) = Some(f);
    }

    fn emit(&self, level: &str, kind: &str, pipeline_id: u64, msg: &str) {
        self.log_hub.push(pipeline_id, level, kind, msg, None);
        if let Some(f) = self
            .event_sink
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            f(level, kind, pipeline_id, msg);
        }
    }

    /// Plan push: stop the old thread → assemble a new runner → start the
    /// resident execution thread. Returns Ok(statement count); missing table
    /// objects or assembly failures return Err (the whole plan is rejected —
    /// no half-assembled pipeline starts).
    /// `initial_paused`: a PAUSED pipeline declares its initial state with
    /// the plan — assembly pauses the partitions immediately, eliminating the
    /// brief consumption window of "start RUNNING, then pause" (the
    /// upgrade/migration/reconciliation recovery path).
    #[allow(clippy::too_many_arguments)]
    pub fn deploy(
        &self,
        pipeline_id: u64,
        dispatch_id: u64,
        statements: Vec<StatementSpec>,
        tables: Vec<tailrace_catalog::PlanTableObject>,
        checkpoint_ms: Option<u64>,
        plan_options: Option<tailrace_catalog::PlanOptions>,
        on_failure: Option<std::sync::Arc<dyn Fn(u64) + Send + Sync>>,
        initial_paused: bool,
    ) -> anyhow::Result<usize> {
        // Same dispatch_id already running → idempotent rejection (the Flink
        // TaskSlotTable.addTask pattern)
        {
            let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = guard.get(&pipeline_id) {
                if entry.dispatch_id == dispatch_id && !entry.flags.is_stopped() {
                    anyhow::bail!(
                        "duplicate push dispatch_id={} (pipeline already running, statements={}) — idempotent rejection",
                        dispatch_id, entry.statement_count
                    );
                }
            }
        }
        // stop the old thread (if any)
        self.stop_pipeline(pipeline_id);

        let catalog = PlanCatalog::new(pipeline_id, statements.clone(), tables.clone());
        let missing = catalog.missing_tables();
        if !missing.is_empty() {
            anyhow::bail!("plan table object metadata missing: {:?} (the console did not send it with the plan)", missing);
        }

        // engine H1: stream_load_url domain allowlist — the plan body cannot
        // point a Doris stream load (and its Basic credential) at an
        // arbitrary host; the node trusts only its configured hosts (the
        // domains of the console-registered table objects, set at deploy
        // time via --stream-load-hosts / TAILRACE_STREAM_LOAD_HOSTS)
        for t in tables.iter() {
            if let Some(url) = t
                .stream_load_url
                .as_deref()
                .filter(|u| !u.trim().is_empty())
            {
                if !tailrace_catalog::stream_load_url_allowed(url, self.stream_load_hosts.as_ref())
                {
                    let host = tailrace_catalog::url_host(url).unwrap_or_default();
                    let allowed: Vec<String> = match &self.stream_load_hosts {
                        Some(set) => set.iter().cloned().collect(),
                        None => tailrace_catalog::default_stream_load_hosts(),
                    };
                    anyhow::bail!(
                        "plan rejected: stream_load_url host {host:?} not in the node allowlist (allowed: {allowed:?}) — raise --stream-load-hosts on the node to add it"
                    );
                }
            }
        }

        // engine H2: strict bad-row policy parse (typo'd values reject the
        // plan instead of silently degrading)
        let bad_row_policy = crate::dlq::BadRowPolicy::parse_strict(
            plan_options
                .as_ref()
                .and_then(|o| o.bad_row_policy.as_deref()),
        )?;

        let mut options = RunOptions {
            duration: std::time::Duration::from_secs(u64::MAX / 2), // unlimited run
            ..Default::default()
        };
        if let Some(ms) = checkpoint_ms.filter(|v| *v >= 1000) {
            options.flush_interval = std::time::Duration::from_millis(ms);
        }
        // tuning: RunOptions knobs exposed for the first time (empty section
        // = defaults)
        if let Some(t) = plan_options.as_ref().and_then(|o| o.tuning.as_ref()) {
            if let Some(v) = t.flush_rows {
                options.flush_rows = v.clamp(100, 1_000_000);
            }
            if let Some(v) = t.flush_interval_ms {
                options.flush_interval = std::time::Duration::from_millis(v.clamp(1_000, 600_000));
            }
            if let Some(v) = t.flush_bytes {
                options.flush_bytes = v.clamp(1024, 1 << 30);
            }
            if let Some(v) = t.poll_rows {
                options.poll_rows = v.clamp(1, 100_000);
            }
            if let Some(v) = t.poll_slice_ms {
                options.poll_slice = std::time::Duration::from_millis(v.clamp(10, 1_000));
            }
        }
        let reliability = plan_options.as_ref().and_then(|o| o.reliability.clone());
        // recovery: self-healing policy (the poison-data criterion is not
        // affected)
        let recovery = plan_options
            .as_ref()
            .and_then(|o| o.recovery.clone())
            .unwrap_or_default();

        let runner = PipelineRunner::assemble(
            &catalog,
            &self.state_dir,
            options.clone(),
            reliability.clone(),
            bad_row_policy,
            &self.connectors,
        )?;
        drop(runner); // plan preflight only (assembly failure rejects the whole plan); the thread re-assembles on every restart
        let n = statements.len();
        // The re-push generation mechanism is gone: deploy follows the
        // stop-old-start-new path (stop_pipeline → new thread, new flags);
        // the old thread exits by observing is_stopped — no generation
        // comparison needed.
        let flags = ControlFlags::new();
        if initial_paused {
            flags.pause();
        }
        let last_err = Arc::new(Mutex::new(None));
        let thread_flags = flags.clone();
        let thread_err = Arc::clone(&last_err);
        let event_cb: EventSink = {
            let sink = self.event_sink.lock().unwrap_or_else(|e| e.into_inner());
            match sink.as_ref() {
                Some(f) => Arc::clone(f),
                None => Arc::new(|_: &str, _: &str, _: u64, _: &str| {}),
            }
        };
        // Observability trio: counters + log hub injected into the execution
        // thread; totals are restart-continuous (seeded from the ledger at
        // assembly — after a process restart or plan re-push the totals
        // continue from the last commit point; read failures tolerate zero —
        // observability never blocks assembly)
        let seeds = load_counter_seeds(&self.state_dir, pipeline_id, n);
        let counters = Arc::new(PipeCounters::new_seeded(n, &seeds));
        let n_st = n;
        let thread_counters = Arc::clone(&counters);
        let thread_hub = Arc::clone(&self.log_hub);
        let thread_connectors = self.connectors.clone();
        let on_failure_cb = on_failure;
        // Restart-loop context (re-assembly needs it — catalog materials and
        // options cloned into the thread)
        let rl_statements = statements.clone();
        let rl_tables = tables;
        let rl_state_dir = self.state_dir.clone();
        let rl_options = options;
        let rl_reliability = reliability;
        let rl_recovery = recovery;
        let rl_bad_row_policy = bad_row_policy;
        let handle = std::thread::Builder::new()
            .name(format!("pipeline-{pipeline_id}"))
            .spawn(move || {
                let mut attempt: u32 = 0;
                let mut no_progress: u32 = 0;
                tracing::info!(
                    pipeline_id,
                    statements = n_st,
                    dispatch_id,
                    bad_row_policy = %rl_bad_row_policy.as_str(),
                    "pipeline started (serve)"
                );
                thread_hub.push(pipeline_id, "INFO", "PIPELINE_STARTED",
                    &format!("plan started: statements={} dispatch_id={} badRowPolicy={}", n_st, dispatch_id, rl_bad_row_policy.as_str()), None);
                loop {
                    // re-assemble before every attempt (cursors restore from
                    // the ledger — the at-least-once replay window)
                    let catalog = PlanCatalog::new(pipeline_id, rl_statements.clone(), rl_tables.clone());
                    let mut runner = match PipelineRunner::assemble(
                        &catalog,
                        &rl_state_dir,
                        rl_options.clone(),
                        rl_reliability.clone(),
                        rl_bad_row_policy,
                        &thread_connectors,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            let msg = format!("re-assembly failed (deterministic error, not restarting): {e:#}");
                            *thread_err.lock().unwrap_or_else(|e| e.into_inner()) = Some(msg.clone());
                            thread_flags.fail();
                            thread_hub.push(pipeline_id, "ERROR", "ASSEMBLE_FAILED", &msg, None);
                            event_cb("ERROR", "ASSEMBLE_FAILED", pipeline_id, &msg);
                            if let Some(cb) = &on_failure_cb { cb(pipeline_id); }
                            return;
                        }
                    };
                    let progress_before = cursor_progress(&rl_state_dir, pipeline_id, n_st);
                    if let Err(e) = runner.run_serve_observed(
                        thread_flags.clone(),
                        Some(Arc::clone(&thread_hub)),
                        Some(Arc::clone(&thread_counters)),
                    ) {
                        let msg = format!("{e:#}");
                        *thread_err.lock().unwrap_or_else(|e| e.into_inner()) = Some(msg.clone());
                        // an error during a console-initiated stop is the
                        // normal exit path — no restart
                        if thread_flags.state() == STATE_STOPPED {
                            return;
                        }
                        // poison-data criterion: the cursor did not advance
                        // (not a single successful flush this round)
                        let progressed =
                            cursor_progress(&rl_state_dir, pipeline_id, n_st) != progress_before;
                        attempt += 1;
                        no_progress = if progressed { 0 } else { no_progress + 1 };
                        // recovery.auto_restart=false: failure is terminal
                        // (awaiting a human/console)
                        let max_restarts = rl_recovery.max_restarts();
                        if !rl_recovery.enabled()
                            || no_progress >= NO_PROGRESS_LIMIT
                            || attempt > max_restarts
                        {
                            let verdict = if !rl_recovery.enabled() {
                                "auto-restart disabled (recovery.autoRestart=false)".to_string()
                            } else if no_progress >= NO_PROGRESS_LIMIT {
                                format!("{no_progress} consecutive failures without cursor progress — judged poison data / deterministic fault")
                            } else {
                                format!("auto-restart budget of {max_restarts} exhausted")
                            };
                            thread_flags.fail();   // FAILED terminal state (snapshots/heartbeats show it truthfully)
                            let full = format!("{msg}（{verdict}，auto-restart stopped）");
                            thread_hub.push(pipeline_id, "ERROR", "STATEMENT_FAILED", &full, None);
                            event_cb("ERROR", "STATEMENT_FAILED", pipeline_id, &full);
                            // actively write NodeState (the Flink
                            // updateTaskExecutionState pattern) → the next
                            // heartbeat carries FAILED truthfully → the
                            // console writes it back
                            if let Some(cb) = &on_failure_cb { cb(pipeline_id); }
                            return;
                        }
                        let backoffs = rl_recovery.backoff_secs();
                        let backoff = backoffs
                            [((attempt - 1) as usize).min(backoffs.len() - 1)];
                        thread_hub.push(pipeline_id, "WARN", "PIPELINE_RESTART",
                            &format!("auto-restart #{attempt} (backing off {backoff}s): {msg}"), None);
                        event_cb("WARN", "PIPELINE_RESTART", pipeline_id, &msg);
                        // sleep in slices: a console stop exits immediately
                        // (stop_pipeline joins this thread)
                        let mut left = backoff;
                        while left > 0 && !thread_flags.is_stopped() {
                            std::thread::sleep(std::time::Duration::from_secs(1));
                            left -= 1;
                        }
                        continue;
                    }
                    tracing::info!(
                        pipeline_id,
                        "pipeline stopped (final flush + cursor commit done in the statement threads)"
                    );
                    return; // normal stop (final flush + cursor commit already done in the statement threads)
                }
            })?;

        self.inner.lock().unwrap_or_else(|e| e.into_inner()).insert(
            pipeline_id,
            PipelineEntry {
                flags,
                handle: Some(handle),
                statement_count: n,
                last_error: last_err,
                dispatch_id,
                counters,
            },
        );
        Ok(n)
    }

    /// pause (keep the cursor, stop pulling).
    pub fn pause(&self, id: u64) -> bool {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match guard.get(&id) {
            Some(e) => {
                e.flags.pause();
                drop(guard);
                self.emit(
                    "INFO",
                    "PIPELINE_PAUSED",
                    id,
                    "console pause — stop pulling, keep the cursor",
                );
                true
            }
            None => false,
        }
    }

    /// resume.
    pub fn resume(&self, id: u64) -> bool {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match guard.get(&id) {
            Some(e) => {
                e.flags.resume();
                drop(guard);
                self.emit("INFO", "PIPELINE_RESUMED", id, "console resume — continue");
                true
            }
            None => false,
        }
    }

    /// stop (final flush + cursor commit, then removal).
    pub fn stop_pipeline(&self, id: u64) {
        let entry = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        if let Some(mut e) = entry {
            e.flags.stop();
            if let Some(h) = e.handle.take() {
                let _ = h.join();
            }
            self.emit(
                "INFO",
                "PIPELINE_STOPPED",
                id,
                "console stop — final flush + cursor commit",
            );
        }
    }

    /// Snapshot (GET /v1/pipelines): id + statement count + status + last
    /// error.
    pub fn snapshot(&self) -> Vec<(u64, usize, String, Option<String>)> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .iter()
            .map(|(id, e)| {
                let st = match e.flags.state() {
                    STATE_RUNNING => "RUNNING",
                    STATE_PAUSED => "PAUSED",
                    STATE_FAILED => "FAILED",
                    _ => "STOPPED",
                };
                let err = e
                    .last_error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                (*id, e.statement_count, st.to_string(), err)
            })
            .collect()
    }

    /// Running pipeline count (dynamic capacity self-report).
    pub fn running_count(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Statement total (capacity self-report).
    pub fn statement_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|e| e.statement_count)
            .sum()
    }

    /// Observability trio: stats endpoint data (status + totals +
    /// per-statement snapshot). Returns None when the pipeline is not
    /// registered (stopped/undeployed — logs may still be queryable).
    pub fn stats(&self, id: u64) -> Option<PipeStatsSnapshot> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let e = guard.get(&id)?;
        let st = match e.flags.state() {
            STATE_RUNNING => "RUNNING",
            STATE_PAUSED => "PAUSED",
            STATE_FAILED => "FAILED",
            _ => "STOPPED",
        };
        let statements: Vec<StatementCountersSnapshot> =
            e.counters.statements.iter().map(|s| s.snapshot()).collect();
        Some(PipeStatsSnapshot {
            pipeline_id: id,
            status: st.to_string(),
            totals: e.counters.totals(),
            statements,
        })
    }

    /// Observability trio: logs endpoint data (pass-through to the hub — a
    /// stopped pipeline's logs survive until LRU eviction).
    pub fn logs(&self, id: u64, after_seq: u64, limit: usize) -> (Vec<LogEntry>, u64) {
        self.log_hub.tail(id, after_seq, limit)
    }

    /// Observability trio: offsets endpoint data — pass through the on-disk
    /// ledger (state file). The ledger is the source of truth, so stopped or
    /// undeployed pipelines still answer.
    pub fn offsets(&self, id: u64) -> anyhow::Result<serde_json::Value> {
        let store = tailrace_state::FileOffsetStore::new(&self.state_dir)?;
        let entries = store.load_all(id)?;
        let deployed = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&id);
        let statements: Vec<serde_json::Value> = entries
            .iter()
            .map(|(k, e)| serde_json::json!({ "statement": k, "ledger": e }))
            .collect();
        Ok(serde_json::json!({
            "ok": true,
            "pipelineId": id,
            "deployed": deployed,
            "statements": statements,
        }))
    }

    /// Observability trio: heartbeat piggyback — totals summary of registered
    /// pipelines (pipeline_id → (in, out, flushes)).
    pub fn heartbeat_totals(&self) -> HashMap<u64, (u64, u64, u64)> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .iter()
            .map(|(id, e)| {
                let t = e.counters.totals();
                (*id, (t.consumed, t.flushed, t.flushes))
            })
            .collect()
    }

    /// Explicit graceful shutdown — stops all pipelines (final flush + cursor
    /// commit, then removal). The Drop path is unreachable (Arc clones live in
    /// the axum router until process end); main's exit path must call this
    /// explicitly.
    pub fn shutdown_all(&self) {
        let entries: Vec<(u64, PipelineEntry)> = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        for (_, mut e) in entries {
            e.flags.stop();
            if let Some(h) = e.handle.take() {
                let _ = h.join();
            }
        }
    }
}

/// stats endpoint response shape.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PipeStatsSnapshot {
    #[serde(rename = "pipelineId")]
    pub pipeline_id: u64,
    pub status: String,
    pub totals: StatementCountersSnapshot,
    pub statements: Vec<StatementCountersSnapshot>,
}

impl Drop for PipelineRegistry {
    fn drop(&mut self) {
        self.shutdown_all();
    }
}
