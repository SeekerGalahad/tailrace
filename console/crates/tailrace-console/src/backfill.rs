//! T-A2 backfill executor (blueprint §7.3 / R6 W4): the console-side half of
//! a bounded replay, on top of the W2 engine primitives
//! (`PipelineRunner::run_bounded` + `backfill_state_dir`).
//!
//! Architecture note — why the console executes this itself instead of
//! pushing a plan to an engine node: the engine has no bounded-run /v1
//! endpoint (W2 deliberately only added the runtime API), while the console
//! already owns everything a bounded replay needs — the published spec, the
//! plan-table resolution (`build_plan_table`, shared with deploy) and the
//! sealed sink credentials. A bounded one-shot replay in the console process
//! is a deliberate exception to "console drives nodes via /v1"; resident
//! pipelines keep flowing through the node path untouched. Because the
//! replay runs in THIS process, file sources must not resolve against the
//! console's cwd — `build_plan_table` rides the datasource's registered
//! directory as the plan `bootstrap` and the engine file factory joins it
//! with the table name, so the replay reads the same file a resident node
//! does from any cwd (R6 review round).
//!
//! Cursor isolation (the A2 acceptance anchor): the runner is assembled
//! against `backfill_state_dir(<data>/backfill, jobId)` — a `bf-{jobId}`
//! ledger namespace — so a backfill, in success AND failure alike, never
//! reads or advances a main-pipeline ledger (those live in the engine
//! nodes' own state dirs, physically out of this process's reach; the
//! console's own `pipeline` row is never written by the executor either).
//!
//! Deterministic labels (the F1 fix): the doris/starrocks sink factory here
//! wraps `DorisSink::with_label_mode(Deterministic { statement_index, from,
//! to })`. The label prefix is JOB-INDEPENDENT on purpose —
//! `tr-p{pipeline}-s{statement}-bf` — so replaying the same range under a
//! NEW job id reproduces the same labels, Doris answers "label already
//! exists" and the replay converges to zero new rows. Baking the job id
//! into the prefix would re-introduce F1 (every job = fresh labels = double
//! commit on a DUPLICATE KEY table).

use std::collections::HashMap;

use anyhow::{Context as _, Result};
use base64::Engine as _;
use tailrace_conn_doris::{DorisSink, DorisTableExt as _, LabelMode, SinkFlavor};
use tailrace_connector::{ConnectorRegistry, Sink, SinkFactory};
use tailrace_runtime::dlq::BadRowPolicy;
use tailrace_runtime::{backfill_state_dir, PipelineRunner, RunOptions};

use crate::compile::{compile_sql, StatementOut};
use crate::state::AppState;

/// Deterministic label family of one (pipeline, statement) pair. The full
/// label rendering (W2 DorisSink) is `{prefix}-{statement_index}-{fp}_{seq}`
/// where `fp = sha256(from,to)[..8]` — stable across jobs and processes.
pub fn label_prefix(pipeline_id: i64, statement_index: u32) -> String {
    format!("tr-p{pipeline_id}-s{statement_index}-bf")
}

/// The isolated consumer group of one job's statement (kafka-shaped sources
/// read the group; file sources ignore it). Distinct from the main
/// pipeline's `tr.p{id}.s{n}` so a replay can never rebalance the resident
/// consumer.
fn bf_group(job_id: &str, statement_index: u32) -> String {
    format!("bf-{job_id}.s{statement_index}")
}

// ---------------- the deterministic-label sink factory ----------------

/// Assembly-time context read by the fn-pointer sink factory below (the
/// engine's `SinkFactory` type is a plain fn — no capture environment, so
/// the parameters ride a process static for the duration of ONE
/// `PipelineRunner::assemble`). TWO locks on purpose: `ASSEMBLY_SERIALIZER`
/// is held across the whole assemble (concurrent job assemblies serialize —
/// assembly is milliseconds, no data movement), while `ASSEMBLY_CTX` is only
/// taken MOMENTARILY by each side (set/clear by the executor, read by the
/// factory). A single lock would self-deadlock: the factory runs INSIDE
/// `PipelineRunner::assemble`, i.e. under whatever the executor still holds.
struct AssemblyCtx {
    statement_index: u32,
    from: i64,
    to: i64,
    label_prefix: String,
    /// datasource reference segment (the id as written in the SQL reference,
    /// e.g. "4" of "4.shop.orders") → `Basic base64(user:pass)` for the
    /// sink's cluster (decrypted in-memory at claim time; values never touch
    /// disk or logs).
    auth: HashMap<String, String>,
}

/// (Arc so the factory can clone it out and release the lock immediately.)
static ASSEMBLY_CTX: std::sync::Mutex<Option<std::sync::Arc<AssemblyCtx>>> =
    std::sync::Mutex::new(None);
static ASSEMBLY_SERIALIZER: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Doris/StarRocks sink factory for backfills. Same construction contract as
/// the engine's `sink_factory` (stream_load_url, columns, sequence column,
/// reliability knobs); two backfill-only differences: the credential
/// comes from the console's sealed store instead of the process env, and
/// the label mode is `Deterministic` for the job's (statement, from, to).
fn backfill_doris_sink(
    obj: &tailrace_catalog::TableObject,
    rel: Option<&tailrace_catalog::ReliabilityOptions>,
) -> Result<Box<dyn Sink>> {
    let ctx = ASSEMBLY_CTX
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .context("backfill sink factory called outside an assembly window")?;
    let auth = ctx
        .auth
        .get(&obj.datasource)
        .with_context(|| {
            format!(
                "no console-stored credential for datasource {}",
                obj.datasource
            )
        })?
        .clone();
    let url = obj
        .stream_load_url()
        .context("doris sink must declare stream_load_url (BE direct)")?;
    let columns = obj.columns.iter().map(|(n, _)| n.clone()).collect();
    let flavor = if obj.kind == "starrocks" {
        SinkFlavor::StarRocks
    } else {
        SinkFlavor::Doris
    };
    let mut sink = DorisSink::with_sequence(
        url,
        auth.clone(),
        columns,
        ctx.label_prefix.clone(),
        obj.sequence_column().map(str::to_string),
    )
    .with_flavor(flavor)
    .with_label_mode(LabelMode::Deterministic {
        statement_index: ctx.statement_index,
        from: ctx.from,
        to: ctx.to,
    });
    // A7 partial-column declaration, mirrored from the engine's sink_factory:
    // the replayed statement projects the declared subset, so the sink must
    // write exactly that subset under partial_columns=true — a full-row
    // sink here would mismatch the transform's column count. Doris-only, as
    // there (StarRocks partial update is a different load contract).
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

/// Connector registry for bounded replays: the kafka/file SOURCE factories
/// straight from the engine crates, the doris/starrocks SINK factories
/// replaced by [`backfill_doris_sink`] (deterministic labels + sealed
/// credentials — the one place a backfill diverges from a resident
/// pipeline's connector wiring).
fn backfill_registry() -> ConnectorRegistry {
    let mut reg = ConnectorRegistry::new();
    tailrace_conn_kafka::register(&mut reg);
    tailrace_conn_file::register(&mut reg);
    let sink: SinkFactory = backfill_doris_sink;
    reg.register_sink("doris", sink);
    reg.register_sink("starrocks", sink);
    reg
}

// ---------------- the executor ----------------

/// One compiled statement + its resolved plan tables, ready for
/// [`PipelineRunner::assemble`]. Built by re-compiling the CURRENT published
/// spec at claim time (a stale request against a re-published pipeline fails
/// the job with a clear error instead of replaying the wrong SQL).
struct BoundedPlan {
    statement: StatementOut,
    tables: Vec<tailrace_catalog::PlanTableObject>,
}

fn bounded_plan(state: &AppState, pipeline_id: i64, statement_index: u32) -> Result<BoundedPlan> {
    let full = state
        .store()
        .pipeline_full(pipeline_id)?
        .with_context(|| format!("pipeline #{pipeline_id} disappeared"))?;
    let spec = full
        .base
        .spec_json
        .clone()
        .context("pipeline has no published spec")?;
    let (statements, diagnostics) = compile_sql(&spec, pipeline_id as u64);
    if !diagnostics.is_empty() {
        anyhow::bail!("spec no longer compiles: {}", diagnostics[0].message);
    }
    let idx = statement_index as usize;
    let statement = statements
        .into_iter()
        .nth(idx)
        .with_context(|| format!("statement index {idx} out of range"))?;
    if !statement.dims.is_empty() {
        anyhow::bail!(
            "statement {idx} carries dim joins — the backfill executor does not assemble dim tables"
        );
    }
    let mut tables = Vec::with_capacity(2);
    for reference in [&statement.source, &statement.sink] {
        // A7: the sink entry carries the statement's partial-column
        // declaration exactly as a deploy push would project it
        let partial = (reference == &statement.sink)
            .then_some(statement.partial_columns.as_deref())
            .flatten();
        let entry = crate::api::pipelines::build_plan_table(state.store(), reference, partial)?
            .with_context(|| format!("table object {reference} no longer registered"))?;
        tables.push(serde_json::from_value(entry)?);
    }
    Ok(BoundedPlan { statement, tables })
}

/// Decrypt the sink datasource's credential into the Basic-auth header
/// value the stream-load protocol wants (`Basic base64(user:pass)` — the
/// same value an engine node receives through its `TAILRACE_DS_*` env).
/// Missing credential → the assembly will fail with the factory's own
/// message; the empty map keeps that error path honest.
fn sealed_auth(state: &AppState, datasource_id: i64) -> Result<String> {
    let Some(c) = state.store().credential(datasource_id)? else {
        anyhow::bail!("datasource #{datasource_id} has no stored credential");
    };
    let plain = state
        .cipher()
        .open(&c.nonce, &c.ciphertext)
        .context("credential unseal failed")?;
    let v: serde_json::Value = serde_json::from_str(&plain)?;
    let user = v.get("username").and_then(|x| x.as_str()).unwrap_or("");
    let pass = v.get("password").and_then(|x| x.as_str()).unwrap_or("");
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    Ok(format!("Basic {encoded}"))
}

/// The sink table object's datasource id (credential owner).
fn sink_datasource_id(state: &AppState, reference: &str) -> Result<i64> {
    let t = state
        .store()
        .table_object_for_reference(reference)?
        .with_context(|| format!("sink {reference} not registered"))?;
    Ok(t.datasource_id)
}

/// Execute one accepted job (runs on a blocking thread): claim → re-resolve
/// the plan → assemble under the bf- ledger namespace → `run_bounded` →
/// terminal state. Every failure lands in the job row as `failed` with the
/// error chain text — a backfill failure NEVER propagates to the main
/// pipeline (no pipeline-row writes anywhere on this path).
///
/// The claim is taken HERE, before `execute`, so a claim that does not go
/// through (the row is not `pending`: another executor holds it, or it is
/// already terminal) returns without touching the row — falling through to
/// `finish_backfill_job` would let its `WHERE state='running'` CAS mark a
/// job that someone else is still running as failed. The POST handler is
/// the only spawn site today; the guard is for the next one (a retry, an
/// admin re-run) that is not.
pub fn run_job(state: AppState, job_id: String, statement_index: u32, from: i64, to: i64) {
    match state.store().claim_backfill_job(&job_id) {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                job = %job_id,
                "backfill job not claimable (already running or terminal) — executor exits without touching it"
            );
            return;
        }
        Err(e) => {
            tracing::error!(job = %job_id, error = %e, "backfill claim failed");
            return;
        }
    }
    let outcome = execute(&state, &job_id, statement_index, from, to);
    let (done, rows, err) = match outcome {
        Ok(rows) => (true, rows, None),
        Err(e) => (false, 0, Some(format!("{e:#}"))),
    };
    if let Err(e) = state
        .store()
        .finish_backfill_job(&job_id, done, rows, err.as_deref())
    {
        tracing::error!(job = %job_id, error = %e, "backfill terminal-state write failed");
    }
}

/// The claimed job's data movement: plan re-resolution → assembly →
/// `run_bounded`. Returns the flushed row total.
fn execute(
    state: &AppState,
    job_id: &str,
    statement_index: u32,
    from: i64,
    to: i64,
) -> Result<i64> {
    let store = state.store();
    let pipeline_id = store
        .backfill_job(job_id)?
        .context("job row vanished")?
        .pipeline_id;
    let plan = bounded_plan(state, pipeline_id, statement_index)?;

    // capability re-check at execution time (the 422 request gate may have
    // raced a re-registration of the sink table object)
    let sink_kind = plan
        .tables
        .iter()
        .find(|t| t.reference == plan.statement.sink)
        .map(|t| t.kind.clone())
        .unwrap_or_default();
    if !matches!(sink_kind.as_str(), "doris" | "starrocks") {
        anyhow::bail!("sink kind is {sink_kind} — only doris/starrocks sinks are backfillable");
    }
    let ds_id = sink_datasource_id(state, &plan.statement.sink)?;
    let mut auth = HashMap::with_capacity(1);
    auth.insert(ds_id.to_string(), sealed_auth(state, ds_id)?);

    // bf- ledger namespace: the bounded runner commits every cursor it is
    // given — pointing it at the main ledger dir would advance main-pipeline
    // cursors (W2's documented hazard). This is THE cursor-isolation anchor.
    let bf_dir = backfill_state_dir(state.backfill_dir(), job_id);
    std::fs::create_dir_all(&bf_dir)
        .with_context(|| format!("create backfill state dir {}", bf_dir.display()))?;

    let spec = tailrace_catalog::StatementSpec {
        source: plan.statement.source.clone(),
        group: bf_group(job_id, statement_index),
        sink: plan.statement.sink.clone(),
        pushdown: plan.statement.pushdown.clone(),
        select_sql: plan.statement.select_sql.clone(),
        start_offset: plan.statement.start_offset.clone(),
        dims: Vec::new(),
        partial_columns: None,
    };
    let catalog = tailrace_catalog::PlanCatalog::new(pipeline_id as u64, vec![spec], plan.tables);

    // assemble under the assembly window (the sink factory reads its
    // parameters from the static); run_bounded itself is context-free.
    // The serializer is poison-tolerant and MUST be released even if
    // assemble panics (guard drop) — a stale ctx is overwritten by the next
    // window before any factory call can read it.
    let mut runner = {
        let _serialize = ASSEMBLY_SERIALIZER
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *ASSEMBLY_CTX.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(std::sync::Arc::new(AssemblyCtx {
                statement_index,
                from,
                to,
                label_prefix: label_prefix(pipeline_id, statement_index),
                auth,
            }));
        let assembled = PipelineRunner::assemble(
            &catalog,
            &bf_dir,
            RunOptions::default(),
            None,
            BadRowPolicy::default(),
            &backfill_registry(),
        );
        *ASSEMBLY_CTX.lock().unwrap_or_else(|e| e.into_inner()) = None;
        assembled?
    };

    tracing::info!(
        job = job_id,
        pipeline_id,
        statement_index,
        from,
        to,
        "console backfill started (bf- ledger namespace, deterministic labels)"
    );
    let stats = runner.run_bounded(from, to)?;
    let rows: i64 = stats.iter().map(|s| s.flushed_rows as i64).sum();
    tracing::info!(job = job_id, pipeline_id, rows, "console backfill finished");
    Ok(rows)
}
