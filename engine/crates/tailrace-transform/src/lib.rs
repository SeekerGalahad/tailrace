//! tailrace-transform: DataFusion execution integration — each statement's
//! `select_sql` is materialized into a LogicalPlan **once** (T7), per
//! micro-batch the decoded batch is injected as the `src` data source and the
//! plan executed.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionConfig};

/// Per-statement plan execution options (T7): the knobs the DataFusion
/// session is assembled with — fixed at statement assembly, NOT derived from
/// the first micro-batch.
#[derive(Debug, Clone)]
pub struct TransformOptions {
    /// Max rows per RecordBatch flowing through the plan (`SessionConfig`
    /// batch_size). Fixed to the runtime `poll_rows` — the old behavior
    /// pinned it to the first micro-batch's row count, which degraded every
    /// later micro-batch of aggregate-shaped statements to single-row output
    /// batches.
    pub max_rows_per_batch: usize,
    /// DataFusion memory pool cap in bytes for this statement's session
    /// (GreedyMemoryPool via `RuntimeEnvBuilder`; guardrail for spillable
    /// operators). Note DataFusion documents the pool as advisory for some
    /// operators ("not yet respected in all cases", datafusion-execution
    /// runtime_env docs).
    pub memory_pool_bytes: usize,
}

impl Default for TransformOptions {
    fn default() -> Self {
        Self {
            // equals the runtime default poll_rows (RunOptions::default)
            max_rows_per_batch: 8_000,
            // per-statement cap; 2-4GB edge boxes host several statements
            memory_pool_bytes: 256 * 1024 * 1024,
        }
    }
}

// SessionContext is cached per statement thread (Ballista-style session
// reuse): building a runtime + SessionContext + SQL parse/plan per micro-batch
// used to be the dominant fixed cost on the hot path. DataFusion's catalog and
// plan cache hang off the context, so reuse keeps the caches warm. T7 adds
// per-statement materialized plans (parse+plan once; per micro-batch only the
// `src` data source is swapped).
struct StatementSession {
    ctx: SessionContext,
    /// sql → materialized (unoptimized) LogicalPlan, schema-anchored; the
    /// optimizer still runs per execution (physical planning needs the live
    /// data), SQL parse/analyze does not.
    plans: HashMap<String, LogicalPlan>,
    /// options the session was assembled with (per statement thread these are
    /// fixed; a changed request rebuilds the session)
    opts: TransformOptions,
}

thread_local! {
    static SESSION: RefCell<Option<StatementSession>> = const { RefCell::new(None) };
}

fn new_session(opts: &TransformOptions) -> anyhow::Result<StatementSession> {
    let config = SessionConfig::new()
        .with_target_partitions(1) // single partition per micro-batch
        .with_batch_size(opts.max_rows_per_batch.max(1));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(opts.memory_pool_bytes, 1.0)
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    Ok(StatementSession {
        ctx,
        plans: HashMap::new(),
        opts: TransformOptions {
            max_rows_per_batch: opts.max_rows_per_batch.max(1),
            memory_pool_bytes: opts.memory_pool_bytes,
        },
    })
}

/// Return the thread's session, rebuilding it when the requested options
/// differ from the ones it was assembled with.
fn ensure_session<'a>(
    slot: &'a mut Option<StatementSession>,
    opts: &TransformOptions,
) -> anyhow::Result<&'a mut StatementSession> {
    let matches = slot.as_ref().is_some_and(|s| {
        s.opts.max_rows_per_batch == opts.max_rows_per_batch.max(1)
            && s.opts.memory_pool_bytes == opts.memory_pool_bytes
    });
    if !matches {
        *slot = Some(new_session(opts)?);
    }
    Ok(slot.as_mut().expect("session just ensured"))
}

/// Swap the `src` TableScan data source to the live micro-batch. The plan
/// (built once per statement) anchors schema qualifiers and projection —
/// only the source `Arc` is replaced, so the CustomScan-qualifier boundary
/// that gates proto decoding never comes into play. A batch whose schema
/// differs from the anchored one is refused (deployment drift — misexecuting
/// a mismatched projection would silently corrupt the sink).
fn inject_src(plan: &LogicalPlan, batch: RecordBatch) -> anyhow::Result<LogicalPlan> {
    let schema = batch.schema();
    let source = provider_as_source(Arc::new(MemTable::try_new(
        schema.clone(),
        vec![vec![batch]],
    )?));
    let swapped = plan.clone().transform_down(|node| match node {
        LogicalPlan::TableScan(mut scan) if scan.table_name.table() == "src" => {
            if scan.source.schema() != schema {
                return Err(datafusion::error::DataFusionError::Plan(
                    "micro-batch schema differs from the materialized plan's anchored src schema"
                        .to_string(),
                ));
            }
            scan.source = source.clone();
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(swapped.data)
}

/// The schema the plan's `src` scan is anchored to, if any.
fn src_schema(plan: &LogicalPlan) -> Option<SchemaRef> {
    use datafusion::common::tree_node::TreeNodeRecursion;
    let mut found = None;
    let _ = plan.apply(|node| {
        if found.is_none() {
            if let LogicalPlan::TableScan(scan) = node {
                if scan.table_name.table() == "src" {
                    found = Some(scan.source.schema());
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Execute a materialized statement plan over one micro-batch (runtime hot
/// path, T7): per micro-batch the batch is injected as `src` and the plan
/// executed — no SQL parse/plan.
pub fn transform_plan(
    plan: &LogicalPlan,
    batch: RecordBatch,
    opts: &TransformOptions,
) -> anyhow::Result<Vec<RecordBatch>> {
    SESSION.with(|cell| {
        let mut slot = cell.borrow_mut();
        let session = ensure_session(&mut slot, opts)?;
        let exec = inject_src(plan, batch)?;
        let df = DataFrame::new(session.ctx.state(), exec);
        let rt = shared_rt();
        rt.block_on(df.collect())
            .map_err(|e| anyhow::anyhow!("collect: {e}"))
    })
}

/// `EXPLAIN ANALYZE` variant of [`transform_plan`] (test/diagnostic use):
/// same execution path; returns the metrics table, in which each operator's
/// `output_batches` makes the plan's executed batch count observable.
pub fn transform_plan_explain(
    plan: &LogicalPlan,
    batch: RecordBatch,
    opts: &TransformOptions,
) -> anyhow::Result<String> {
    SESSION.with(|cell| {
        let mut slot = cell.borrow_mut();
        let session = ensure_session(&mut slot, opts)?;
        let exec = inject_src(plan, batch)?;
        let df = DataFrame::new(session.ctx.state(), exec).explain(false, true)?;
        let rt = shared_rt();
        let batches = rt
            .block_on(df.collect())
            .map_err(|e| anyhow::anyhow!("collect: {e}"))?;
        Ok(datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string())
    })
}

/// Cached transform (sql-string entry: plan materialized on first use per
/// (sql, schema), then injected per micro-batch — same execution path as
/// [`transform_plan`]).
pub fn transform_cached(
    select_sql: &str,
    batch: RecordBatch,
    opts: &TransformOptions,
) -> anyhow::Result<Vec<RecordBatch>> {
    SESSION.with(|cell| {
        let mut slot = cell.borrow_mut();
        let session = ensure_session(&mut slot, opts)?;
        let plan = materialize(session, select_sql, &batch)?;
        let exec = inject_src(&plan, batch)?;
        let df = DataFrame::new(session.ctx.state(), exec);
        let rt = shared_rt();
        rt.block_on(df.collect())
            .map_err(|e| anyhow::anyhow!("collect: {e}"))
    })
}

fn materialize(
    session: &mut StatementSession,
    select_sql: &str,
    batch: &RecordBatch,
) -> anyhow::Result<LogicalPlan> {
    // reuse the cached plan when the schema it anchors matches; rebuild on
    // drift (a different source feeding the same thread + sql)
    if let Some(p) = session.plans.get(select_sql) {
        if src_schema(p).as_ref() == Some(&batch.schema()) {
            return Ok(p.clone());
        }
    }
    let plan = {
        let ctx = &session.ctx;
        let _ = ctx.deregister_table("src");
        ctx.register_batch("src", batch.clone())
            .map_err(|e| anyhow::anyhow!("register: {e}"))?;
        let rt = shared_rt();
        let df = rt
            .block_on(ctx.sql(select_sql))
            .map_err(|e| anyhow::anyhow!("sql: {e}"))?;
        let plan = df.logical_plan().clone();
        let _ = ctx.deregister_table("src");
        plan
    };
    session.plans.insert(select_sql.to_string(), plan.clone());
    Ok(plan)
}

/// Memory pool wiring probe (test use): the limit the thread's session pool
/// was assembled with, as DataFusion reports it.
#[cfg(test)]
fn session_memory_limit(
    opts: &TransformOptions,
) -> anyhow::Result<datafusion::execution::memory_pool::MemoryLimit> {
    SESSION.with(|cell| {
        let mut slot = cell.borrow_mut();
        let session = ensure_session(&mut slot, opts)?;
        Ok(session.ctx.runtime_env().memory_pool.memory_limit())
    })
}

/// Process-wide tokio runtime singleton (one shared runtime, Ballista-style).
static TOKIO_RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
fn shared_rt() -> &'static tokio::runtime::Runtime {
    TOKIO_RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("shared tokio rt")
    })
}

/// Plain transform: `select_sql` references the input batch as `src`.
pub fn transform(select_sql: &str, input: RecordBatch) -> anyhow::Result<Vec<RecordBatch>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let ctx = SessionContext::new();
        ctx.register_batch("src", input)?;
        let df = ctx.sql(select_sql).await?;
        let out = df.collect().await?;
        Ok(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn two_col_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("op_type", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["I"; rows])),
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn select_filters_and_projects() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("op_type", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["I", "D", "U"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )?;
        let out = transform("SELECT seq * 10 AS seq10 FROM src WHERE op_type <> 'D'", rb)?;
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        Ok(())
    }

    /// T7: batch_size must be fixed from options (poll_rows), NOT pinned by
    /// the first micro-batch — an aggregate statement whose first batch is a
    /// single row must still execute later 8000-row micro-batches as one
    /// batch. Each scenario runs on its own thread (one statement = one
    /// session, matching the runtime's per-statement threads).
    #[test]
    fn batch_size_fixed_from_options_not_first_batch() -> anyhow::Result<()> {
        let agg_sql = "SELECT seq, COUNT(*) AS n FROM src GROUP BY seq";
        let run = |batch_size: usize| {
            std::thread::spawn(move || {
                let opts = TransformOptions {
                    max_rows_per_batch: batch_size,
                    ..Default::default()
                };
                // first micro-batch: a single row (the old pinning trigger)
                let first = transform_cached(agg_sql, two_col_batch(1), &opts)?;
                assert_eq!(first.len(), 1);
                assert_eq!(first[0].num_rows(), 1);
                // later micro-batch: 8000 rows
                let out = transform_cached(agg_sql, two_col_batch(8000), &opts)?;
                let batches: usize = out.len();
                let rows: usize = out.iter().map(|b| b.num_rows()).sum();
                anyhow::Ok((batches, rows))
            })
            .join()
            .expect("scenario thread")
        };
        // fixed batch_size=8000: the later micro-batch executes as ONE batch
        let (batches, rows) = run(8000)?;
        assert_eq!(rows, 8000, "all rows flow through");
        assert_eq!(batches, 1, "8000-row micro-batch must stay one batch");
        // the old pinning reproduced for contrast (batch_size=1): the same
        // micro-batch executed as 8000 single-row batches
        let (batches_old, rows_old) = run(1)?;
        assert_eq!(rows_old, 8000);
        assert_eq!(batches_old, 8000, "old behavior: single-row batches");
        Ok(())
    }

    /// T7 acceptance: first batch 1 row + later large batch — the later
    /// micro-batch's executed batch count drops, evidenced on the DataFusion
    /// EXPLAIN ANALYZE run of the same plan. T33 (DF 55): `output_batches`
    /// is now a standard BaselineMetrics metric (RecordOutput auto-records
    /// it on every operator) and shows up on aggregate/projection plans —
    /// the DF 48 limitation (only join/coalesce/unnest emitted it) is
    /// lifted. The executed batch count for the drop comparison is still
    /// taken at the plan output (collect); the explain table pins the other
    /// execution facts (operators ran, all 8000 rows flowed, output_batches
    /// present) for both scenarios. Count metrics render human-readable on
    /// 55 (datafusion-common display/human_readable.rs: `8000` → `8.00 K`);
    /// both renderings are accepted below.
    #[test]
    fn explain_metrics_batch_count_drops() -> anyhow::Result<()> {
        let agg_sql = "SELECT seq, COUNT(*) AS n FROM src GROUP BY seq";
        let run = |batch_size: usize| {
            std::thread::spawn(move || {
                let opts = TransformOptions {
                    max_rows_per_batch: batch_size,
                    ..Default::default()
                };
                // first micro-batch 1 row (builds the materialized plan)
                transform_cached(agg_sql, two_col_batch(1), &opts)?;
                // later 8000-row micro-batch: explain run + executed output
                let table = transform_cached_explain(agg_sql, two_col_batch(8000), &opts)?;
                assert!(
                    table.contains("AggregateExec"),
                    "explain metrics must show the aggregate ran:\n{table}"
                );
                // all 8000 rows flowed through the aggregate (DF 48 rendered
                // the count raw, DF 55 renders it human-readable)
                let all_rows =
                    table.contains("output_rows=8000") || table.contains("output_rows=8.00 K");
                assert!(
                    all_rows,
                    "explain metrics must show all 8000 rows flowed:\n{table}"
                );
                // T33 (DF 55): output_batches is now emitted by
                // aggregate/projection operators too (BaselineMetrics) — the
                // metric-face unlock this test documents.
                assert!(
                    table.contains("output_batches="),
                    "DF 55 must expose output_batches on aggregate plans:\n{table}"
                );
                let out = transform_cached(agg_sql, two_col_batch(8000), &opts)?;
                anyhow::Ok(out.len())
            })
            .join()
            .expect("scenario thread")
        };
        let fixed = run(8000)?;
        let pinned = run(1)?;
        assert_eq!(
            fixed, 1,
            "fixed batch_size=poll_rows: the micro-batch executes as one batch"
        );
        assert_eq!(
            pinned, 8000,
            "old first-batch pinning: the same micro-batch executed as 8000 single-row batches"
        );
        assert!(fixed < pinned, "plan execution batch count must drop");
        Ok(())
    }

    /// EXPLAIN ANALYZE variant of the sql-string entry (assertion helper
    /// surface — mirrors `transform_plan_explain` for cached plans).
    fn transform_cached_explain(
        select_sql: &str,
        batch: RecordBatch,
        opts: &TransformOptions,
    ) -> anyhow::Result<String> {
        SESSION.with(|cell| {
            let mut slot = cell.borrow_mut();
            let session = ensure_session(&mut slot, opts)?;
            let plan = materialize(session, select_sql, &batch)?;
            let exec = inject_src(&plan, batch)?;
            let df = DataFrame::new(session.ctx.state(), exec).explain(false, true)?;
            let rt = shared_rt();
            let batches = rt
                .block_on(df.collect())
                .map_err(|e| anyhow::anyhow!("collect: {e}"))?;
            Ok(datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string())
        })
    }

    /// T7: the memory pool is assembled into the session runtime with the
    /// configured cap (wiring assertion — enforcement semantics for a given
    /// operator stay DataFusion's).
    #[test]
    fn memory_pool_wired_with_configured_cap() -> anyhow::Result<()> {
        use datafusion::execution::memory_pool::MemoryLimit;
        let cap = 16 * 1024 * 1024;
        let opts = TransformOptions {
            max_rows_per_batch: 8,
            memory_pool_bytes: cap,
        };
        std::thread::spawn(move || {
            // run one batch so the session exists, then probe the pool
            transform_cached("SELECT * FROM src", two_col_batch(4), &opts)?;
            match session_memory_limit(&opts)? {
                MemoryLimit::Finite(n) => assert_eq!(n, cap),
                MemoryLimit::Infinite | MemoryLimit::Unknown => {
                    panic!("expected a finite pool cap")
                }
            }
            anyhow::Ok(())
        })
        .join()
        .expect("pool probe thread")?;
        Ok(())
    }
}
