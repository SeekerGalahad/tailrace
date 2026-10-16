use std::time::Duration;

use tailrace_ir::{Cursor, SourceChunk};

/// Pull semantics: micro-batch friendly, cursor-resumable (0..=max_rows per
/// poll, never blocking past `deadline`).
pub trait Source: Send {
    /// Open/become ready (subscribe, or position the file at the cursor).
    fn open(&mut self, resume: &Cursor) -> anyhow::Result<()>;
    /// Micro-batch pull: returns 0..=max_rows chunks; returns as soon as the
    /// deadline hits (an empty batch = no upstream data right now).
    fn poll(&mut self, max_rows: usize, deadline: Duration) -> anyhow::Result<Vec<SourceChunk>>;
    /// Current cursor (the runtime commits it to the OffsetStore only after a
    /// successful write — the at-least-once anchor).
    fn cursor(&self) -> Cursor;
    /// Pause pulling (PAUSED state). Returns true=paused; false=unsupported/
    /// failed (the caller must not poll).
    fn pause(&mut self) -> bool {
        false
    }
    /// Resume pulling (paired with a successful pause).
    fn resume(&mut self) {}
    /// Liveness heartbeat while PAUSED (the Kafka implementation keeps the
    /// librdkafka poll thread alive to avoid max.poll eviction).
    fn keepalive(&mut self, deadline: Duration) {
        std::thread::sleep(deadline);
    }
}
