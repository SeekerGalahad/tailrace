//! tailrace-ir: plan IR — datafusion-proto LogicalPlan serialization + version header.
//!
//! Iron rule: **the IR never carries credentials** (references are catalog
//! anchors only); `debug_sql_echo` is diagnostic echo and never executed.

pub mod column_type;
pub use column_type::{arrow_type_or_utf8, ColumnType, UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW};
pub mod placeholder;
pub use placeholder::{IrCodec, SrcPlaceholderTable};

use base64::Engine;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;

pub const IR_VERSION: &str = "1.0";

/// One statement in the IR (source/sink are catalog references).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StatementIr {
    pub source_ref: String,
    pub sink_ref: String,
    pub group: String,
    pub pushdown: Vec<String>,
    /// datafusion-proto LogicalPlan bytes (base64)
    pub proto_plan_b64: String,
    /// Diagnostic echo (not executed)
    pub debug_sql_echo: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PipelineIr {
    pub ir_version: String,
    pub pipeline_id: u64,
    pub statements: Vec<StatementIr>,
}

/// select_sql → execution LogicalPlan (T7 plan materialization): parsed and
/// planned **once per statement** against the declared `src` schema. The
/// TableScan keeps the placeholder source — the execution layer
/// (`tailrace_transform::transform_plan`) swaps in the real batch per
/// micro-batch; schema qualifiers and projection stay anchored by this plan
/// (only the source `Arc` is replaced, sidestepping the CustomScan qualifier
/// boundary that gates proto decoding below).
pub fn build_exec_plan(
    select_sql: &str,
    src_columns: &[(String, String)],
) -> anyhow::Result<LogicalPlan> {
    // Plan on a dedicated thread: statement assembly may run inside an async
    // runtime (the node serve deployment handler) — nesting a tokio runtime
    // there panics ("Cannot start a runtime from within a runtime", caught by
    // the v1 contract suite). One-time cost per statement deployment.
    std::thread::scope(|scope| {
        scope
            .spawn(|| -> anyhow::Result<LogicalPlan> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    let schema = datafusion::arrow::datatypes::Schema::new(
                        src_columns
                            .iter()
                            .map(|(n, t)| {
                                // G2: vocabulary goes through ColumnType::parse
                                // (16 words, blueprint §7.1) — unknown words
                                // warn + fold to Utf8 for one minor version
                                Ok(datafusion::arrow::datatypes::Field::new(
                                    n.clone(),
                                    arrow_type_or_utf8(n, t)?,
                                    true,
                                ))
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?,
                    );
                    placeholder::register_src(&ctx, std::sync::Arc::new(schema))?;
                    let df = ctx.sql(select_sql).await?;
                    Ok(df.logical_plan().clone())
                })
            })
            .join()
            .map_err(|_| anyhow::anyhow!("plan build thread panicked"))?
    })
}

/// select_sql → proto bytes (planned against an empty `src` table with the
/// declared schema — the plan anchors the schema, real batches are injected
/// at execution time).
pub fn plan_to_b64(select_sql: &str, src_columns: &[(String, String)]) -> anyhow::Result<String> {
    let plan = build_plan(select_sql, src_columns)?;
    let bytes =
        datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec(&plan, &IrCodec)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// proto bytes → LogicalPlan.
///
/// DataFusion 48 proto decoding rebuilds CustomScan without schema qualifiers
/// ("No field named src.x"): serialization works, decoding does not. Gated
/// until that upstream boundary is lifted or a custom codec deepens; the JSON
/// carrier stays canonical meanwhile (layer A/B golden fixtures guard drift).
pub fn b64_to_plan(b64: &str) -> anyhow::Result<LogicalPlan> {
    let _ = b64;
    anyhow::bail!("proto decoding gated on the DataFusion CustomScan qualifier boundary — the JSON carrier is canonical");
}
#[allow(dead_code)]
#[allow(clippy::derivable_impls)]
fn b64_to_plan_unchecked(b64: &str) -> anyhow::Result<LogicalPlan> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let plan: LogicalPlan = rt.block_on(async {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        // T33 (DF 55): logical_plan_from_bytes_with_extension_codec takes the
        // task context (bytes/mod.rs:164 on 55.2.0) — SessionContext::task_ctx
        // still hands it out.
        let p = datafusion_proto::bytes::logical_plan_from_bytes_with_extension_codec(
            &bytes,
            &ctx.task_ctx(),
            &IrCodec,
        )?;
        Ok::<_, anyhow::Error>(p)
    })?;
    Ok(plan)
}

fn build_plan(select_sql: &str, src_columns: &[(String, String)]) -> anyhow::Result<LogicalPlan> {
    use datafusion::common::tree_node::TreeNode;
    let plan = build_exec_plan(select_sql, src_columns)?;
    // TableScan("src") → EmptyRelation: the data source stays out of the IR
    // (real batches are injected at execution time) — natively proto-serializable.
    let plan = plan.transform_down(|p| match p {
        LogicalPlan::TableScan(scan) if scan.table_name.table() == "src" => {
            Ok(datafusion::common::tree_node::Transformed::yes(
                LogicalPlan::EmptyRelation(datafusion::logical_expr::logical_plan::EmptyRelation {
                    produce_one_row: false,
                    schema: scan.projected_schema.clone(),
                }),
            ))
        }
        other => Ok(datafusion::common::tree_node::Transformed::no(other)),
    })?;
    Ok(plan.data)
}

// ==== Cursor/SourceChunk contract (the runtime-side read cursors) ====
mod cursor_sourcechunk_impl {
    use std::collections::BTreeMap;
    include!("cursor_sourcechunk_impl.rs");
}
pub use cursor_sourcechunk_impl::{Cursor, SourceChunk};

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<(String, String)> {
        vec![
            ("op_type".to_string(), "utf8".to_string()),
            ("order_no".to_string(), "utf8".to_string()),
            ("seq".to_string(), "int64".to_string()),
        ]
    }

    /// Layer A (SQL→IR): serialization succeeds + the JSON carrier snapshot is
    /// stable (structure-level drift guard).
    #[test]
    fn layer_a_ir_json_snapshot() -> anyhow::Result<()> {
        let b64 = plan_to_b64(
            "SELECT order_no, seq * 10 AS seq10 FROM src WHERE op_type <> 'D'",
            &cols(),
        )?;
        assert!(!b64.is_empty(), "proto serialization must succeed");
        let ir = PipelineIr {
            ir_version: IR_VERSION.to_string(),
            pipeline_id: 42,
            statements: vec![StatementIr {
                source_ref: "12.default.orders_ogg".into(),
                sink_ref: "3.shop.orders".into(),
                group: "tr.p42.s0".into(),
                pushdown: vec!["I".into(), "U".into()],
                proto_plan_b64: b64,
                debug_sql_echo: "SELECT order_no, seq * 10 AS seq10 FROM src WHERE op_type <> 'D'"
                    .into(),
            }],
        };
        let json = serde_json::to_string_pretty(&ir)?;
        let fixture = "tests/golden_layer_a.json";
        if std::env::var("UPDATE_GOLDEN").is_ok() {
            std::fs::write(fixture, &json)?;
        }
        let expected = std::fs::read_to_string(fixture)?;
        // proto_plan_b64 is byte-stable across processes on the same DataFusion
        // version; the snapshot is compared byte for byte.
        assert_eq!(
            json.trim(),
            expected.trim(),
            "layer A golden snapshot drifted (UPDATE_GOLDEN=1 to regenerate, review required)"
        );
        assert!(
            !json.to_lowercase().contains("password"),
            "IR never carries credentials"
        );
        Ok(())
    }

    /// Layer B (IR→execution): semantic execution snapshot of debug_sql_echo
    /// (behavior-level drift guard; switches to the proto plan once decoding
    /// is unblocked).
    #[test]
    fn layer_b_execute_snapshot() -> anyhow::Result<()> {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use std::sync::Arc;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let summary = rt.block_on(async {
            let ctx = SessionContext::new();
            let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                datafusion::arrow::datatypes::Field::new(
                    "op_type",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    true,
                ),
                datafusion::arrow::datatypes::Field::new(
                    "order_no",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    true,
                ),
                datafusion::arrow::datatypes::Field::new(
                    "seq",
                    datafusion::arrow::datatypes::DataType::Int64,
                    true,
                ),
            ]));
            let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["I", "D", "U"])),
                    Arc::new(StringArray::from(vec!["ORD-1", "ORD-2", "ORD-3"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
            )?;
            ctx.register_batch("src", rb)?;
            let df = ctx
                .sql("SELECT order_no, seq * 10 AS seq10 FROM src WHERE op_type <> 'D'")
                .await?;
            let batches = df.collect().await?;
            let mut out = String::new();
            for b in &batches {
                out.push_str(
                    &datafusion::arrow::util::pretty::pretty_format_batches(std::slice::from_ref(
                        b,
                    ))?
                    .to_string(),
                );
            }
            Ok::<_, anyhow::Error>(out)
        })?;
        let fixture = "tests/golden_layer_b.txt";
        if std::env::var("UPDATE_GOLDEN").is_ok() {
            std::fs::write(fixture, &summary)?;
        }
        let expected = std::fs::read_to_string(fixture)?;
        assert_eq!(
            summary.trim(),
            expected.trim(),
            "layer B execution snapshot drifted"
        );
        assert!(
            summary.contains("ORD-1") && summary.contains("30"),
            "{summary}"
        );
        assert!(!summary.contains("ORD-2"), "D rows must be filtered");
        Ok(())
    }

    /// G2 (R8, blueprint §7.7): golden coverage for the parameterized type
    /// words. The original layer A/B fixtures stay untouched (the 4-legacy-
    /// word regression anchor); these wide twins carry `decimal(18,2)` and
    /// `timestamp(us)` columns through the same SQL→IR→execution path.
    fn wide_cols() -> Vec<(String, String)> {
        vec![
            ("op_type".to_string(), "utf8".to_string()),
            ("order_no".to_string(), "utf8".to_string()),
            ("amount".to_string(), "decimal(18,2)".to_string()),
            ("created_at".to_string(), "timestamp(us)".to_string()),
        ]
    }

    const WIDE_SQL: &str = "SELECT order_no, amount, created_at FROM src WHERE op_type <> 'D'";

    /// Layer A wide (SQL→IR): the decimal/timestamp vocabulary plans, the
    /// declared schema survives into the LogicalPlan byte-for-byte, and the
    /// JSON carrier snapshot is stable.
    #[test]
    fn layer_a_wide_ir_json_snapshot() -> anyhow::Result<()> {
        let b64 = plan_to_b64(WIDE_SQL, &wide_cols())?;
        assert!(!b64.is_empty(), "proto serialization must succeed");
        // readable schema anchor: the parameterized words reached the plan
        // as Decimal128(18,2) / Timestamp(Microsecond, None), not Utf8 folds
        let plan = build_plan(WIDE_SQL, &wide_cols())?;
        let schema = plan.schema();
        let amount = schema.field_with_name(None, "amount")?;
        assert_eq!(
            amount.data_type(),
            &datafusion::arrow::datatypes::DataType::Decimal128(18, 2)
        );
        let created_at = schema.field_with_name(None, "created_at")?;
        assert_eq!(
            created_at.data_type(),
            &datafusion::arrow::datatypes::DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                None
            )
        );
        let ir = PipelineIr {
            ir_version: IR_VERSION.to_string(),
            pipeline_id: 42,
            statements: vec![StatementIr {
                source_ref: "12.default.orders_ogg".into(),
                sink_ref: "3.shop.orders".into(),
                group: "tr.p42.s0".into(),
                pushdown: vec!["I".into(), "U".into()],
                proto_plan_b64: b64,
                debug_sql_echo: WIDE_SQL.into(),
            }],
        };
        let json = serde_json::to_string_pretty(&ir)?;
        let fixture = "tests/golden_layer_a_wide.json";
        if std::env::var("UPDATE_GOLDEN").is_ok() {
            std::fs::write(fixture, &json)?;
        }
        let expected = std::fs::read_to_string(fixture)?;
        assert_eq!(
            json.trim(),
            expected.trim(),
            "layer A wide golden snapshot drifted (UPDATE_GOLDEN=1 to regenerate, review required)"
        );
        Ok(())
    }

    /// Layer B wide (IR→execution): decimal/timestamp values survive a real
    /// DataFusion execution and pretty-print identically (the local
    /// counterpart of the W5 file→doris precision e2e).
    #[test]
    fn layer_b_wide_execute_snapshot() -> anyhow::Result<()> {
        use datafusion::arrow::array::{Decimal128Array, StringArray, TimestampMicrosecondArray};
        use std::sync::Arc;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let summary = rt.block_on(async {
            let ctx = SessionContext::new();
            let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                datafusion::arrow::datatypes::Field::new(
                    "op_type",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    true,
                ),
                datafusion::arrow::datatypes::Field::new(
                    "order_no",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    true,
                ),
                datafusion::arrow::datatypes::Field::new(
                    "amount",
                    datafusion::arrow::datatypes::DataType::Decimal128(18, 2),
                    true,
                ),
                datafusion::arrow::datatypes::Field::new(
                    "created_at",
                    datafusion::arrow::datatypes::DataType::Timestamp(
                        datafusion::arrow::datatypes::TimeUnit::Microsecond,
                        None,
                    ),
                    true,
                ),
            ]));
            let rb = datafusion::arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["I", "D", "U"])),
                    Arc::new(StringArray::from(vec!["ORD-1", "ORD-2", "ORD-3"])),
                    Arc::new(
                        Decimal128Array::from(vec![Some(1999_i128), Some(-5_i128), Some(0_i128)])
                            .with_data_type(datafusion::arrow::datatypes::DataType::Decimal128(
                                18, 2,
                            )),
                    ),
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        Some(1_i64),
                        Some(2_i64),
                        Some(3_i64),
                    ])),
                ],
            )?;
            ctx.register_batch("src", rb)?;
            let df = ctx.sql(WIDE_SQL).await?;
            let batches = df.collect().await?;
            let mut out = String::new();
            for b in &batches {
                out.push_str(
                    &datafusion::arrow::util::pretty::pretty_format_batches(std::slice::from_ref(
                        b,
                    ))?
                    .to_string(),
                );
            }
            Ok::<_, anyhow::Error>(out)
        })?;
        let fixture = "tests/golden_layer_b_wide.txt";
        if std::env::var("UPDATE_GOLDEN").is_ok() {
            std::fs::write(fixture, &summary)?;
        }
        let expected = std::fs::read_to_string(fixture)?;
        assert_eq!(
            summary.trim(),
            expected.trim(),
            "layer B wide execution snapshot drifted"
        );
        // decimal precision shape: 1999 with scale 2 renders as 19.99 — the
        // 0.01-class value survives planning + execution unfolded
        assert!(summary.contains("19.99"), "{summary}");
        assert!(!summary.contains("ORD-2"), "D rows must be filtered");
        Ok(())
    }
}
