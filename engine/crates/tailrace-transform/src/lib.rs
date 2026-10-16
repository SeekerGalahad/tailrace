//! tailrace-transform: DataFusion execution integration — register the decoded
//! batch as the temp table `src`, run select_sql, return the transformed
//! batches.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;

// SessionContext is cached per statement thread (Ballista-style session reuse):
// building a runtime + SessionContext + SQL parse/plan per micro-batch used to
// be the dominant fixed cost on the hot path. DataFusion's catalog and plan
// cache hang off the context, so reuse keeps the caches warm.
use std::cell::RefCell;

thread_local! {
    static CTX: RefCell<Option<datafusion::prelude::SessionContext>> = const { RefCell::new(None) };
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

/// Cached transform (drop-in replacement for `transform` — same interface,
/// zero caller changes).
pub fn transform_cached(select_sql: &str, batch: RecordBatch) -> anyhow::Result<Vec<RecordBatch>> {
    CTX.with(|cell| {
        let mut ctx_ref = cell.borrow_mut();
        let ctx = ctx_ref.get_or_insert_with(|| {
            let config = datafusion::prelude::SessionConfig::new()
                .with_target_partitions(1) // single partition per micro-batch
                .with_batch_size(batch.num_rows().max(1));
            datafusion::prelude::SessionContext::new_with_config(config)
        });
        let _ = ctx.deregister_table("src");
        ctx.register_batch("src", batch)
            .map_err(|e| anyhow::anyhow!("register: {e}"))?;
        let rt = shared_rt();
        let df = rt
            .block_on(ctx.sql(select_sql))
            .map_err(|e| anyhow::anyhow!("sql: {e}"))?;
        let rt = shared_rt();
        rt.block_on(df.collect())
            .map_err(|e| anyhow::anyhow!("collect: {e}"))
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
    use std::sync::Arc;

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
}
