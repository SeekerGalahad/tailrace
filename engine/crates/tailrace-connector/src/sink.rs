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
pub trait Sink: Send {
    fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()>;
    fn flush(&mut self) -> anyhow::Result<SinkAck>;
    /// Approximate unflushed bytes buffered (default 0=unmeasurable: the
    /// byte batching boundary degrades honestly, row/time boundaries still
    /// work).
    fn buffered_bytes(&self) -> u64 {
        0
    }
}
