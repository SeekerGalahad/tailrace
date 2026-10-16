//! `src` placeholder table source + IR extension codec.
//!
//! datafusion-proto drops the schema when decoding EmptyRelation/Values —
//! CustomScan + LogicalExtensionCodec is the way to carry the placeholder
//! source schema through the IR.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::logical_plan::Extension;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};

/// `src` placeholder table (compile-time schema carrier; scans empty — the
/// runtime injects the real batch at execution time).
#[derive(Debug)]
pub struct SrcPlaceholderTable {
    pub schema: SchemaRef,
}

impl SrcPlaceholderTable {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait::async_trait]
impl TableProvider for SrcPlaceholderTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mem = datafusion::datasource::MemTable::try_new(
            self.schema.clone(),
            vec![vec![
                datafusion::arrow::record_batch::RecordBatch::new_empty(self.schema.clone()),
            ]],
        )?;
        mem.scan(_state, _projection, _filters, _limit).await
    }
}

/// IR codec: the `src` placeholder round-trips via CustomScan (schema carried
/// as Arrow IPC bytes).
#[derive(Debug)]
pub struct IrCodec;

impl Default for IrCodec {
    fn default() -> Self {
        Self
    }
}

impl LogicalExtensionCodec for IrCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &SessionContext,
    ) -> Result<Extension> {
        DefaultLogicalExtensionCodec {}.try_decode(buf, inputs, ctx)
    }
    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        DefaultLogicalExtensionCodec {}.try_encode(node, buf)
    }
    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::sql::TableReference,
        schema: SchemaRef,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
        if table_ref.table() == "src" {
            return Ok(Arc::new(SrcPlaceholderTable::new(schema)));
        }
        DefaultLogicalExtensionCodec {}.try_decode_table_provider(buf, table_ref, schema, ctx)
    }
    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if table_ref.table() == "src" {
            // schema rides on the CustomScan node itself (protobuf::Schema);
            // the byte payload stays empty.
            let _ = (&node, buf);
            return Ok(());
        }
        DefaultLogicalExtensionCodec {}.try_encode_table_provider(table_ref, node, buf)
    }
}

/// Register the placeholder source on the compile-time context.
pub fn register_src(ctx: &SessionContext, schema: SchemaRef) -> Result<()> {
    ctx.register_table("src", Arc::new(SrcPlaceholderTable::new(schema)))?;
    let _ = provider_as_source;
    Ok(())
}
