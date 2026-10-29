use datafusion::arrow::record_batch::RecordBatch;

/// Write acknowledgement (idempotency anchor: label/transaction id is owned
/// by the implementation).
#[derive(Debug, Clone)]
pub struct SinkAck {
    pub rows: u64,
    pub kind: &'static str,
    pub idempotency_key: String,
}

/// Static capability declaration of a sink implementation (G4): what the
/// sink can really do beyond plain buffered-append + flush. Callers branch
/// on these flags instead of sniffing the concrete type; a flag flip must
/// land together with the behavior it names (A7 partial-column update and
/// T36③ 2PC flip doris fields when the behavior ships).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SinkCapabilities {
    /// Flush acks carry a target-side dedup label: replaying a flush after
    /// a restart converges (Doris stream load labels are globally
    /// deduplicated — "label already exists" is success). False = the
    /// ack's idempotency_key is informational only and a replayed flush
    /// re-appends (at-least-once).
    pub idempotent_label: bool,
    /// The transaction hooks below are real on this sink: staged data only
    /// becomes visible at commit() and is discarded by abort(). False (all
    /// sinks today) = flush remains the single commit point and the hooks
    /// stay inert defaults.
    pub transactional: bool,
    /// Whole-target replace (truncate/overwrite) is available as a write
    /// mode.
    pub overwrite: bool,
    /// Writing a column subset preserves the unlisted columns' existing
    /// row values (Doris UNIQUE KEY partial_columns; flipped per-instance
    /// by DorisSink's A7 partial mode, off on default construction). The
    /// runtime branches on this flag: a plan declaring `partial_columns` on
    /// a sink whose flag is false is refused at assembly instead of
    /// silently writing subset rows under full-row semantics.
    pub partial_update: bool,
    /// The sink itself routes writes per data partition (target-side
    /// auto-partitioning does not count).
    pub partition_aware: bool,
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

    /// Long-transaction hooks (G4). The defaults are deliberately no-op:
    /// flush stays the one and only commit point, so every existing sink
    /// keeps its exact behavior with zero changes. A sink that can really
    /// stage data (Doris 2PC stream load, lake-format commits — T36③/B)
    /// overrides these AND declares `capabilities().transactional`; the
    /// engine must only drive the hooks when that capability is declared.
    ///
    /// prepare: stage the current buffer boundary for a later commit.
    /// Idempotent under retry — restaging after a crash must converge on
    /// the same label/id, not duplicate.
    fn prepare(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// commit: atomically make everything staged since the last commit
    /// visible and durable.
    fn commit(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// abort: discard everything staged since the last commit (the
    /// recovery path of a failed transaction).
    fn abort(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Static capability declaration (G4). Default = all false, the
    /// honest floor of a plain append sink; production sinks override
    /// with their real capabilities (contract-locked in tests/contract).
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::default()
    }
}
