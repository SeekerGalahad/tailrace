use datafusion::arrow::record_batch::RecordBatch;

/// Write acknowledgement (idempotency anchor: label/transaction id is owned
/// by the implementation).
#[derive(Debug, Clone)]
pub struct SinkAck {
    pub rows: u64,
    pub kind: &'static str,
    pub idempotency_key: String,
}

/// Batched idempotent write: write=buffer/enqueue; flush=the batching
/// boundary (the real storage/delivery commit point).
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion::arrow::array::StringArray;
/// # use datafusion::arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion::arrow::record_batch::RecordBatch;
/// # use tailrace_connector::{Sink, SinkAck};
/// struct DevNull {
///     pending_rows: u64,
/// }
///
/// impl Sink for DevNull {
///     fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
///         self.pending_rows += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
///         Ok(())
///     }
///     fn flush(&mut self) -> anyhow::Result<SinkAck> {
///         Ok(SinkAck {
///             rows: std::mem::take(&mut self.pending_rows),
///             kind: "devnull",
///             idempotency_key: "run-1".into(),
///         })
///     }
/// }
///
/// let batch = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, true)])),
///     vec![Arc::new(StringArray::from(vec!["a"]))],
/// )?;
/// let mut sink = DevNull { pending_rows: 0 };
/// sink.write(vec![batch])?; // buffered only…
/// assert_eq!(sink.flush()?.rows, 1); // …flush is the delivery commit point
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Sink: Send {
    fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()>;
    fn flush(&mut self) -> anyhow::Result<SinkAck>;
    /// Approximate unflushed bytes buffered — the byte-boundary input of the
    /// three-threshold flush (Doris Flink connector
    /// `sink.buffer-flush.max-bytes`). Real implementations: FileSink (arrow
    /// memory of pending batches), DorisSink (carrier bytes pending stream
    /// load), KafkaSink (payload bytes awaiting delivery confirmation).
    /// Default 0 = unmeasurable: the byte boundary degrades honestly, the
    /// row/time boundaries still work.
    fn buffered_bytes(&self) -> u64 {
        0
    }
}
