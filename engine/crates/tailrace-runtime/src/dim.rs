//! T-D1 broadcast dimension tables (blueprint D narrow path, 演化蓝图 §D1).
//!
//! **Broadcast join, NOT a streaming join** (project red line): a dim table
//! is loaded FULL-TABLE over JDBC at pipeline assembly (compile time) and
//! registered as a static `dim_<alias>` MemTable inside the statement plan —
//! no watermark, no state backend, no refresh while the pipeline runs. The
//! dim snapshot is fixed for the plan's lifetime; a restart re-loads it.
//!
//! Guardrails (fail-fast at assembly, never OOM):
//! - **row cap** (default 500_000, `RunOptions::dim_max_rows` / plan tuning
//!   `dimMaxRows`): the JDBC pagination (`SELECT * FROM t ORDER BY key ASC
//!   LIMIT page` then keyset windows — the conn-jdbc full-table path) is
//!   drained page by page and aborted as soon as the running total crosses
//!   the cap;
//! - **memory cap** (default 64 MiB, `dimMaxBytes`): measured on the decoded
//!   Arrow data (`RecordBatch::get_array_memory_size`), same page-by-page
//!   abort;
//! - **unique key**: the dim table must have a PRIMARY KEY / single-column
//!   UNIQUE index — enforced by reusing the JDBC source's keyset uniqueness
//!   gate (`MYSQL_UNIQUE_SQL`/`PG_UNIQUE_SQL` in tailrace-conn-jdbc) on the
//!   declared `sequence_column`; a table with no unique key refuses to
//!   compile (cartesian-amplification guard, blueprint V5's hard constraint).

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use tailrace_catalog::{option_keys, StatementSpec, TableObject};
use tailrace_connector::{ConnectorRegistry, Cursor};
use tailrace_decode::{ColBatch, Decoded, Decoder, JsonDecoder, Pushdown};

use crate::arrow_type;

/// Default dim row cap (blueprint D1: 500_000 rows).
pub const DEFAULT_DIM_MAX_ROWS: usize = 500_000;
/// Default dim memory cap (blueprint D1: 64 MiB, measured on the decoded
/// Arrow data).
pub const DEFAULT_DIM_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// JDBC page size for the full-table drain (each page is one
/// `SELECT … ORDER BY key ASC LIMIT page` round trip; the caps are checked
/// between pages, so the overshoot is bounded by one page).
const DIM_PAGE_ROWS: usize = 10_000;

/// Per-dim-table load limits (enforced at assembly — compile time).
#[derive(Debug, Clone, Copy)]
pub struct DimLimits {
    pub max_rows: usize,
    pub max_bytes: u64,
}

impl Default for DimLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_DIM_MAX_ROWS,
            max_bytes: DEFAULT_DIM_MAX_BYTES,
        }
    }
}

/// Load one dim table FULL-TABLE over JDBC: first-round full-table query +
/// keyset page loop (the conn-jdbc `SELECT * FROM t ORDER BY key ASC LIMIT n`
/// path), each row decoded through the standard JsonDecoder against the
/// declared catalog schema. Returns the static snapshot batch (possibly 0
/// rows — an empty dim table is legal, every LEFT JOIN row then takes the
/// NULL side).
///
/// The JDBC access is bridged through the connector registry (the dim object
/// is served by the `jdbc` source factory): a binary built without the JDBC
/// connector fails here, keeping the connector feature gating in the
/// registry where it lives.
pub fn load_dim(
    obj: &TableObject,
    registered_name: &str,
    spec: &StatementSpec,
    connectors: &ConnectorRegistry,
    limits: &DimLimits,
) -> anyhow::Result<RecordBatch> {
    if obj.kind != "dim" {
        anyhow::bail!(
            "dim table {registered_name}: kind must be dim (got {}) — only ${{ds:dim.datasource.table}} references load as dimension tables",
            obj.kind
        );
    }
    if obj.option_str(option_keys::SEQUENCE_COLUMN).is_none() {
        anyhow::bail!(
            "dim table {registered_name} ({}.{}) requires `sequence_column` — the PRIMARY KEY / \
             single-column UNIQUE column used as the full-table load pagination key (broadcast join guard)",
            obj.datasource, obj.name
        );
    }
    if obj.columns.is_empty() {
        anyhow::bail!(
            "dim table {registered_name} declares no columns — declare the projection the join needs"
        );
    }
    let schema = Arc::new(Schema::new(
        obj.columns
            .iter()
            .map(|(n, t)| Ok(Field::new(n.clone(), arrow_type(n, t)?, true)))
            .collect::<anyhow::Result<Vec<_>>>()?,
    ));
    // bridge to the JDBC source factory through the registry (kind rewrite —
    // dim is the catalog kind, jdbc is the connector kind that serves it)
    let mut jdbc_obj = obj.clone();
    jdbc_obj.kind = "jdbc".into();
    let mut src = connectors.build_source(&jdbc_obj, spec, None).map_err(|e| {
        anyhow::anyhow!(
            "dim table {registered_name} needs the JDBC connector (kind=dim loads full-table over jdbc): {e}"
        )
    })?;
    // open + first poll run the connect probe: the keyset uniqueness gate
    // (PRIMARY KEY / single-column UNIQUE on `key`) refuses the load here —
    // the no-unique-key dim table never compiles
    src.open(&Cursor::None)?;
    let decoder = JsonDecoder; // the jdbc source emits one JSON object per row
                               // page size: bounded by the row cap so a small cap aborts after ONE page
    let page = limits.max_rows.saturating_add(1).clamp(1, DIM_PAGE_ROWS);
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut rows_total: usize = 0;
    let mut bytes_total: u64 = 0;
    loop {
        let chunks = src.poll(page, Duration::from_secs(5))?;
        if chunks.is_empty() {
            break;
        }
        let mut col_batch = ColBatch::new(schema.clone(), chunks.len());
        for chunk in chunks {
            match decoder.decode(chunk.raw, &mut col_batch, &Pushdown::default()) {
                Decoded::Append => {}
                Decoded::Skip => {}
                Decoded::Bad(raw) => anyhow::bail!(
                    "dim table {registered_name}: a row does not match the declared schema (first bad row: {}) — fix the catalog columns",
                    String::from_utf8_lossy(&raw).chars().take(120).collect::<String>()
                ),
            }
        }
        if col_batch.is_empty() {
            continue;
        }
        let rb = col_batch.finish()?;
        rows_total += rb.num_rows();
        bytes_total += rb.get_array_memory_size() as u64;
        if rows_total > limits.max_rows {
            anyhow::bail!(
                "dim table {registered_name} exceeds the row cap: {rows_total} rows > maxDimRows {} — \
                 broadcast join refuses the load (raise tuning.dimMaxRows or shrink the table)",
                limits.max_rows
            );
        }
        if bytes_total > limits.max_bytes {
            anyhow::bail!(
                "dim table {registered_name} exceeds the memory cap: {bytes_total} bytes > maxDimBytes {} — \
                 broadcast join refuses the load (raise tuning.dimMaxBytes or project fewer columns)",
                limits.max_bytes
            );
        }
        batches.push(rb);
    }
    let all = match batches.len() {
        0 => RecordBatch::new_empty(schema),
        1 => batches.into_iter().next().expect("one batch"),
        _ => datafusion::arrow::compute::concat_batches(&schema, &batches)?,
    };
    tracing::info!(
        table = %obj.name,
        registered = %registered_name,
        rows = rows_total,
        bytes = bytes_total,
        "dim table loaded full-table (broadcast join static snapshot)"
    );
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tailrace_connector::{Source, SourceChunk};

    /// Scripted source standing in for the JDBC connector: hands out JSON
    /// rows `page` at a time until exhausted (the same drain shape the real
    /// JdbcSource's full-table first round + keyset windows produce).
    struct RowsSource {
        rows: Vec<Vec<u8>>,
        given: usize,
    }

    impl Source for RowsSource {
        fn open(&mut self, _: &Cursor) -> anyhow::Result<()> {
            Ok(())
        }
        fn poll(&mut self, max_rows: usize, _: Duration) -> anyhow::Result<Vec<SourceChunk>> {
            if self.given >= self.rows.len() {
                return Ok(vec![]);
            }
            let take = max_rows.min(self.rows.len() - self.given);
            let out: Vec<SourceChunk> = self.rows[self.given..self.given + take]
                .iter()
                .map(|r| SourceChunk::plain(r.clone()))
                .collect();
            self.given += take;
            Ok(out)
        }
        fn cursor(&self) -> Cursor {
            Cursor::None
        }
    }

    fn dim_obj(overrides: impl FnOnce(&mut TableObject)) -> TableObject {
        let mut o = TableObject::new("db", "dim_user", "dim")
            .with_format("json")
            .with_columns(vec![
                ("id".into(), "int64".into()),
                ("name".into(), "utf8".into()),
            ])
            .with_option(option_keys::BOOTSTRAP, "mysql://root@localhost/test")
            .with_option(option_keys::SEQUENCE_COLUMN, "id");
        overrides(&mut o);
        o
    }

    fn spec() -> StatementSpec {
        StatementSpec {
            source: "k.orders".into(),
            group: "tr.p9901.s0".into(),
            sink: "d.orders_wide".into(),
            pushdown: vec![],
            select_sql: "SELECT s.uid, d.name FROM src s LEFT JOIN dim_d d ON s.uid = d.id".into(),
            start_offset: None,
            dims: vec![tailrace_catalog::DimJoin {
                reference: "db.dim_user".into(),
                name: "dim_d".into(),
            }],
            partial_columns: None,
        }
    }

    /// Registry with a mock factory registered under the `jdbc` kind — the
    /// same registry bridge the real JDBC connector plugs into.
    /// `SourceFactory` is a plain fn pointer, so the scripted rows ride via a
    /// thread_local side channel (each test runs on its own thread; the
    /// factory is invoked on the same thread inside `load_dim`).
    fn mock_registry(rows: Vec<Vec<u8>>) -> ConnectorRegistry {
        std::thread_local! {
            static PAYLOAD: std::cell::RefCell<Option<Vec<Vec<u8>>>> =
                const { std::cell::RefCell::new(None) };
        }
        fn payload_factory(
            _o: &TableObject,
            _s: &StatementSpec,
            _r: Option<&tailrace_catalog::ReliabilityOptions>,
        ) -> anyhow::Result<Box<dyn Source>> {
            let rows = PAYLOAD.with(|p| p.borrow_mut().take()).unwrap_or_default();
            Ok(Box::new(RowsSource { rows, given: 0 }))
        }
        PAYLOAD.with(|p| *p.borrow_mut() = Some(rows));
        let mut reg = ConnectorRegistry::new();
        reg.register_source("jdbc", payload_factory);
        reg
    }

    fn user_rows(n: usize) -> Vec<Vec<u8>> {
        (1..=n)
            .map(|i| format!(r#"{{"id":{i},"name":"u{i}"}}"#).into_bytes())
            .collect()
    }

    fn cols_map(rb: &RecordBatch) -> BTreeMap<i64, Option<String>> {
        use datafusion::arrow::array::{Array, Int64Array, StringArray};
        let ids = rb
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let names = rb
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name");
        (0..rb.num_rows())
            .map(|i| {
                (
                    ids.value(i),
                    if names.is_null(i) {
                        None
                    } else {
                        Some(names.value(i).to_string())
                    },
                )
            })
            .collect()
    }

    /// Happy path: the full-table drain produces the complete snapshot with
    /// declared-schema decoding (multi-page included — 10 rows at page ≤4).
    #[test]
    fn loads_full_table_across_pages() -> anyhow::Result<()> {
        let reg = mock_registry(user_rows(10));
        let limits = DimLimits {
            max_rows: 100,
            max_bytes: 1 << 20,
        };
        let rb = load_dim(&dim_obj(|_| ()), "dim_d", &spec(), &reg, &limits)?;
        assert_eq!(rb.num_rows(), 10, "all rows loaded");
        let m = cols_map(&rb);
        assert_eq!(m.get(&1), Some(&Some("u1".to_string())));
        assert_eq!(m.get(&10), Some(&Some("u10".to_string())));
        Ok(())
    }

    /// Row cap: exceeding the cap aborts the load with a clear error at the
    /// FIRST page crossing it (fail-fast, not OOM) — and the error names the
    /// cap and the table.
    #[test]
    fn row_cap_exceeded_fails_fast() {
        let reg = mock_registry(user_rows(10));
        let limits = DimLimits {
            max_rows: 3,
            max_bytes: 1 << 20,
        };
        // page = min(max_rows+1, 10_000) = 4 → the first page (4 rows)
        // already crosses the cap of 3
        let err = load_dim(&dim_obj(|_| ()), "dim_d", &spec(), &reg, &limits)
            .expect_err("row cap must refuse the load");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("exceeds the row cap") && msg.contains("maxDimRows"),
            "{msg}"
        );
        assert!(
            msg.contains("dim_d"),
            "error names the registered dim table: {msg}"
        );
    }

    /// Memory cap: a tiny byte cap on a load that fits the row cap refuses
    /// with the memory message (measured on the decoded Arrow data).
    #[test]
    fn memory_cap_exceeded_fails_fast() {
        let reg = mock_registry(user_rows(10));
        let limits = DimLimits {
            max_rows: 100,
            max_bytes: 16, // far below even one decoded row's arrow footprint
        };
        let err = load_dim(&dim_obj(|_| ()), "dim_d", &spec(), &reg, &limits)
            .expect_err("memory cap must refuse the load");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("exceeds the memory cap") && msg.contains("maxDimBytes"),
            "{msg}"
        );
    }

    /// Guards: wrong kind, missing sequence_column, missing columns — each
    /// refuses with a message naming what is missing.
    #[test]
    fn refuses_bad_dim_objects() {
        let reg = mock_registry(user_rows(2));
        let limits = DimLimits::default();
        let err = load_dim(
            &dim_obj(|o| o.kind = "kafka".into()),
            "dim_d",
            &spec(),
            &reg,
            &limits,
        )
        .expect_err("non-dim kind must refuse");
        assert!(format!("{err:#}").contains("kind must be dim"), "{err:#}");
        let err = load_dim(
            &dim_obj(|o| {
                o.options.remove(option_keys::SEQUENCE_COLUMN);
            }),
            "dim_d",
            &spec(),
            &reg,
            &limits,
        )
        .expect_err("missing sequence_column must refuse");
        assert!(format!("{err:#}").contains("sequence_column"), "{err:#}");
        let err = load_dim(
            &dim_obj(|o| o.columns.clear()),
            "dim_d",
            &spec(),
            &reg,
            &limits,
        )
        .expect_err("missing columns must refuse");
        assert!(
            format!("{err:#}").contains("declares no columns"),
            "{err:#}"
        );
    }

    /// A dim row that does not match the declared schema (bad JSON) refuses
    /// the load — catalog drift fails at assembly, not mid-run.
    #[test]
    fn bad_row_refuses_the_load() {
        let mut rows = user_rows(2);
        rows.push(b"{broken".to_vec());
        let reg = mock_registry(rows);
        let err = load_dim(
            &dim_obj(|_| ()),
            "dim_d",
            &spec(),
            &reg,
            &DimLimits::default(),
        )
        .expect_err("bad row must refuse");
        assert!(
            format!("{err:#}").contains("does not match the declared schema"),
            "{err:#}"
        );
    }

    /// Empty dim table: a legal 0-row snapshot (every LEFT JOIN row takes
    /// the NULL side downstream).
    #[test]
    fn empty_dim_table_loads_zero_rows() -> anyhow::Result<()> {
        let reg = mock_registry(vec![]);
        let rb = load_dim(
            &dim_obj(|_| ()),
            "dim_d",
            &spec(),
            &reg,
            &DimLimits::default(),
        )?;
        assert_eq!(rb.num_rows(), 0);
        assert_eq!(rb.schema().fields().len(), 2, "schema stays declared");
        Ok(())
    }
}
