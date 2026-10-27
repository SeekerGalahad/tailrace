//! tailrace-transform: DataFusion execution integration — each statement's
//! `select_sql` is materialized into a LogicalPlan **once** (T7), per
//! micro-batch the decoded batch is injected as the `src` data source and the
//! plan executed.
//!
//! T-D1 (blueprint D narrow path): statements with `JOIN
//! ${ds:dim.datasource.table}` get their plan materialized by
//! [`build_dim_plan`] — the dimension tables are registered as **static
//! MemTables** (loaded full-table over JDBC at assembly) and ride inside the
//! plan. This is a **broadcast join, NOT a streaming join**: no watermark, no
//! state backend, no dim refresh while the pipeline runs; the loader enforces
//! explicit row/memory caps and fails the assembly on breach (fail-fast, not
//! OOM). Per micro-batch only the `src` scan swaps ([`inject_src`] is
//! untouched); the dim scans keep their static sources for the plan's
//! lifetime.

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

/// One loaded dimension table (T-D1 broadcast join side): the full-table
/// snapshot taken at assembly, registered under `name` (`dim_<alias>`, the
/// table name the rewritten select_sql joins against).
#[derive(Debug, Clone)]
pub struct DimTable {
    /// Registered table name (`dim_<alias>`)
    pub name: String,
    /// Full-table data (limits already enforced by the loader)
    pub batch: RecordBatch,
}

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

/// T-D1 (blueprint D narrow path): materialize a dim-join statement plan.
///
/// Registers each dim table as a **static MemTable** (`dim_<alias>`, the
/// full-table snapshot loaded at assembly) plus the `src` placeholder with
/// the declared source schema, then parses+plans `select_sql`. The dim
/// MemTable sources ride inside the returned plan — execution needs no dim
/// re-registration ([`transform_plan`]/[`inject_src`] swap only the `src`
/// scan per micro-batch, exactly as for dim-free statements).
///
/// **Broadcast join, NOT a streaming join**: the dim snapshot is fixed for
/// the plan's lifetime (no watermark, no state backend, no refresh); the
/// loader's row/memory caps are the only memory guardrail.
///
/// Plans on a dedicated thread: statement assembly may run inside an async
/// runtime (the node serve deployment handler) — nesting a tokio runtime
/// there panics (same reason as `tailrace_ir::build_exec_plan`).
pub fn build_dim_plan(
    select_sql: &str,
    src_schema: SchemaRef,
    dims: &[DimTable],
) -> anyhow::Result<LogicalPlan> {
    std::thread::scope(|scope| {
        scope
            .spawn(move || -> anyhow::Result<LogicalPlan> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    // src placeholder: the schema anchors the plan, per
                    // micro-batch inject_src swaps in the live batch
                    ctx.register_batch("src", RecordBatch::new_empty(src_schema))?;
                    for d in dims {
                        let table =
                            MemTable::try_new(d.batch.schema(), vec![vec![d.batch.clone()]])?;
                        // register_table errors on a duplicate name — a
                        // twice-registered dim_<alias> fails the plan build
                        ctx.register_table(d.name.as_str(), Arc::new(table))
                            .map_err(|e| {
                                anyhow::anyhow!("dim table {} registration failed: {e}", d.name)
                            })?;
                    }
                    let df = ctx.sql(select_sql).await?;
                    Ok(df.logical_plan().clone())
                })
            })
            .join()
            .map_err(|_| anyhow::anyhow!("dim plan build thread panicked"))?
    })
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

    // ==================== T-D1 broadcast dim join ====================

    use datafusion::arrow::array::Array;

    fn dim_batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids: Int64Array = rows.iter().map(|(id, _)| Some(*id)).collect();
        let names: StringArray = rows.iter().map(|(_, n)| n.map(|s| s.to_string())).collect();
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).expect("dim batch")
    }

    fn src_batch_uid(rows: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("uid", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]));
        let uids: Int64Array = rows.to_vec().into();
        let vals: StringArray = rows.iter().map(|r| Some(format!("v{r}"))).collect();
        RecordBatch::try_new(schema, vec![Arc::new(uids), Arc::new(vals)]).expect("src batch")
    }

    /// T-D1 core acceptance: a LEFT JOIN against the static dim MemTable
    /// produces matched rows AND NULL sides for unmatched keys — the dim data
    /// rides inside the plan, so `inject_src` (unchanged) only swaps `src`.
    #[test]
    fn dim_left_join_produces_matches_and_null_sides() -> anyhow::Result<()> {
        let dim = DimTable {
            name: "dim_d".into(),
            batch: dim_batch(&[(1, Some("alice")), (2, Some("bob"))]),
        };
        let src_schema = src_batch_uid(&[]).schema();
        let sql = "SELECT s.uid, s.val, d.name FROM src s LEFT JOIN dim_d d ON s.uid = d.id";
        let plan = build_dim_plan(sql, src_schema, &[dim])?;
        // micro-batch: uid 1 matches alice, uid 9 has NO dim side (NULL),
        // uid 2 matches bob
        let out = transform_plan(
            &plan,
            src_batch_uid(&[1, 9, 2]),
            &TransformOptions::default(),
        )?;
        let mut rows: Vec<(i64, Option<String>)> = out
            .iter()
            .flat_map(|b| {
                let uids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("uid col");
                let names = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("name col");
                (0..b.num_rows())
                    .map(|i| {
                        (
                            uids.value(i),
                            if names.is_null(i) {
                                None
                            } else {
                                Some(names.value(i).to_string())
                            },
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        // hash-join output order is not the input order — compare as a set
        rows.sort_by_key(|(uid, _)| *uid);
        assert_eq!(
            rows,
            vec![
                (1, Some("alice".into())),
                (2, Some("bob".into())),
                (9, None)
            ],
            "LEFT JOIN: matched dim values + NULL side for the unmatched key"
        );
        Ok(())
    }

    /// T-D1: the dim snapshot is STATIC across micro-batches — the same dim
    /// rows join every batch (no re-load, no drift), and multiple dim-free
    /// statements on the same thread keep working (session reuse unaffected).
    /// Runs on its own thread (one statement = one thread-local session).
    #[test]
    fn dim_static_across_micro_batches() -> anyhow::Result<()> {
        let dim = DimTable {
            name: "dim_d".into(),
            batch: dim_batch(&[(1, Some("alice"))]),
        };
        let src_schema = src_batch_uid(&[]).schema();
        let plan = Arc::new(build_dim_plan(
            "SELECT s.uid, d.name FROM src s INNER JOIN dim_d d ON s.uid = d.id",
            src_schema,
            &[dim],
        )?);
        let out = std::thread::spawn(move || {
            let opts = TransformOptions::default();
            let first = transform_plan(&plan, src_batch_uid(&[1, 2]), &opts)?;
            let second = transform_plan(&plan, src_batch_uid(&[1]), &opts)?;
            anyhow::Ok((first, second))
        })
        .join()
        .expect("dim statement thread")?;
        // INNER JOIN: unmatched uid 2 drops; the dim row still matches batch 2
        let names = |out: &[RecordBatch]| {
            out.iter()
                .flat_map(|b| {
                    b.column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("name col")
                        .iter()
                        .map(|v| v.map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&out.0), vec![Some("alice".to_string())]);
        assert_eq!(
            names(&out.1),
            vec![Some("alice".to_string())],
            "the static dim MemTable keeps serving later micro-batches"
        );
        Ok(())
    }

    /// T-D1: an empty dim table (0 rows) is legal — every LEFT JOIN row gets
    /// the NULL side (the "dim not yet populated" edge).
    #[test]
    fn dim_empty_table_left_join_all_null_sides() -> anyhow::Result<()> {
        let dim = DimTable {
            name: "dim_d".into(),
            batch: dim_batch(&[]),
        };
        let src_schema = src_batch_uid(&[]).schema();
        let plan = build_dim_plan(
            "SELECT s.uid, d.name FROM src s LEFT JOIN dim_d d ON s.uid = d.id",
            src_schema,
            &[dim],
        )?;
        let out = transform_plan(&plan, src_batch_uid(&[1, 2]), &TransformOptions::default())?;
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "all src rows survive");
        let nulls: usize = out
            .iter()
            .map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("name col")
                    .null_count()
            })
            .sum();
        assert_eq!(nulls, 2, "empty dim → every row has the NULL side");
        Ok(())
    }

    /// T-D1: planning a select_sql that references an unregistered dim name
    /// fails at plan build (assembly time), not mid-run.
    #[test]
    fn dim_plan_rejects_unknown_table() {
        let src_schema = src_batch_uid(&[]).schema();
        let err = build_dim_plan(
            "SELECT s.uid FROM src s LEFT JOIN dim_gone g ON s.uid = g.id",
            src_schema,
            &[DimTable {
                name: "dim_d".into(),
                batch: dim_batch(&[(1, Some("alice"))]),
            }],
        )
        .expect_err("unknown dim table must fail the plan build");
        assert!(
            err.to_string().contains("dim_gone"),
            "error names the missing dim table: {err:#}"
        );
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
