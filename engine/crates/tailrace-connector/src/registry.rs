use std::collections::HashMap;

use tailrace_catalog::{ReliabilityOptions, StatementSpec, TableObject};

use crate::{Sink, Source};

/// Source factory: table object + statement spec (+ optional reliability
/// options) → a ready-to-open source.
pub type SourceFactory = fn(
    &TableObject,
    &StatementSpec,
    Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Source>>;

/// Sink factory: table object (+ optional reliability options) → a
/// ready-to-write sink.
pub type SinkFactory =
    fn(&TableObject, Option<&ReliabilityOptions>) -> anyhow::Result<Box<dyn Sink>>;

/// Kind-keyed connector registry. Keys are canonical kinds ("kafka"/"file"/
/// "doris"/"starrocks"/…); legacy aliases ("topic", "doris_table") are
/// normalized by the catalog before lookup.
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use datafusion::arrow::record_batch::RecordBatch;
/// # use tailrace_catalog::{ReliabilityOptions, StatementSpec, TableObject};
/// # use tailrace_connector::{ConnectorRegistry, Cursor, Sink, SinkAck, Source, SourceChunk};
/// # struct NoopSource;
/// # impl Source for NoopSource {
/// #     fn open(&mut self, _: &Cursor) -> anyhow::Result<()> {
/// #         Ok(())
/// #     }
/// #     fn poll(&mut self, _: usize, _: Duration) -> anyhow::Result<Vec<SourceChunk>> {
/// #         Ok(vec![])
/// #     }
/// #     fn cursor(&self) -> Cursor {
/// #         Cursor::None
/// #     }
/// # }
/// # struct NoopSink;
/// # impl Sink for NoopSink {
/// #     fn write(&mut self, _: Vec<RecordBatch>) -> anyhow::Result<()> {
/// #         Ok(())
/// #     }
/// #     fn flush(&mut self) -> anyhow::Result<SinkAck> {
/// #         Ok(SinkAck { rows: 0, kind: "noop", idempotency_key: String::new() })
/// #     }
/// # }
/// # fn make_source(
/// #     _o: &TableObject,
/// #     _s: &StatementSpec,
/// #     _r: Option<&ReliabilityOptions>,
/// # ) -> anyhow::Result<Box<dyn Source>> {
/// #     Ok(Box::new(NoopSource))
/// # }
/// # fn make_sink(
/// #     _o: &TableObject,
/// #     _r: Option<&ReliabilityOptions>,
/// # ) -> anyhow::Result<Box<dyn Sink>> {
/// #     Ok(Box::new(NoopSink))
/// # }
/// let mut reg = ConnectorRegistry::new();
/// reg.register_source("kafka", make_source);
/// reg.register_sink("kafka", make_sink);
///
/// let mut obj = TableObject::new("ds", "t", "kafka");
/// let spec = StatementSpec {
///     source: "ds.t".into(),
///     group: "tr.p1.s0".into(),
///     sink: "ds.t".into(),
///     pushdown: vec![],
///     select_sql: "SELECT * FROM src".into(),
///     start_offset: None,
///     dims: Vec::new(),
///     partial_columns: None,
/// };
/// assert!(reg.build_source(&obj, &spec, None).is_ok());
/// assert!(reg.build_sink(&obj, None).is_ok());
///
/// // An unregistered kind fails loudly: this binary was built without that
/// // connector feature.
/// obj.kind = "jdbc".into();
/// assert!(reg.build_sink(&obj, None).is_err());
/// ```
#[derive(Default, Clone)]
pub struct ConnectorRegistry {
    sources: HashMap<String, SourceFactory>,
    sinks: HashMap<String, SinkFactory>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a source factory; a duplicate kind panics (surfaced at
    /// startup, not at first use).
    pub fn register_source(&mut self, kind: &str, f: SourceFactory) {
        if self.sources.insert(kind.to_string(), f).is_some() {
            panic!("source connector already registered: {kind}");
        }
    }

    /// Register a sink factory; a duplicate kind panics (same rationale).
    pub fn register_sink(&mut self, kind: &str, f: SinkFactory) {
        if self.sinks.insert(kind.to_string(), f).is_some() {
            panic!("sink connector already registered: {kind}");
        }
    }

    pub fn build_source(
        &self,
        obj: &TableObject,
        spec: &StatementSpec,
        rel: Option<&ReliabilityOptions>,
    ) -> anyhow::Result<Box<dyn Source>> {
        let f = self.sources.get(&obj.kind).ok_or_else(|| {
            anyhow::anyhow!(
                "源类型未支持：{}（本二进制未编译该连接器 feature）",
                obj.kind
            )
        })?;
        f(obj, spec, rel)
    }

    pub fn build_sink(
        &self,
        obj: &TableObject,
        rel: Option<&ReliabilityOptions>,
    ) -> anyhow::Result<Box<dyn Sink>> {
        let f = self.sinks.get(&obj.kind).ok_or_else(|| {
            anyhow::anyhow!(
                "sink 类型未支持：{}（本二进制未编译该连接器 feature）",
                obj.kind
            )
        })?;
        f(obj, rel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> StatementSpec {
        StatementSpec {
            source: "k.t".into(),
            group: "tr.p1.s0".into(),
            sink: "k.t".into(),
            pushdown: vec![],
            select_sql: "SELECT * FROM src".into(),
            start_offset: None,
            dims: Vec::new(),
            partial_columns: None,
        }
    }

    #[test]
    fn unknown_kind_reports_unsupported() {
        let reg = ConnectorRegistry::new();
        let mut obj = tailrace_catalog::TableObject::new("k", "t", "mqtt");
        let err = match reg.build_source(&obj, &spec(), None) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("unsupported kind must fail"),
        };
        assert!(err.contains("mqtt"), "{err}");
        obj.kind = "jdbc".into();
        assert!(reg.build_sink(&obj, None).is_err());
    }

    #[test]
    #[should_panic(expected = "source connector already registered: kafka")]
    fn duplicate_registration_panics_at_startup() {
        fn never(
            _obj: &TableObject,
            _s: &StatementSpec,
            _r: Option<&ReliabilityOptions>,
        ) -> anyhow::Result<Box<dyn Source>> {
            unreachable!()
        }
        let mut reg = ConnectorRegistry::new();
        reg.register_source("kafka", never);
        reg.register_source("kafka", never);
    }
}
