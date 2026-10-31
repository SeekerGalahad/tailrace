use std::time::Duration;

use tailrace_ir::{Cursor, SourceChunk};

/// Pull semantics: micro-batch friendly, cursor-resumable (0..=max_rows per
/// poll, never blocking past `deadline`).
///
/// # Example
///
/// A minimal tailing source over an in-memory buffer:
///
/// ```
/// # use std::time::Duration;
/// # use tailrace_connector::{Cursor, Source, SourceChunk};
/// struct MemLines {
///     lines: Vec<Vec<u8>>,
///     next: usize,
/// }
///
/// impl Source for MemLines {
///     fn open(&mut self, resume: &Cursor) -> anyhow::Result<()> {
///         // Cursor::None = fresh start; a resumed cursor repositions.
///         if let Cursor::File { byte_pos, .. } = resume {
///             self.next = *byte_pos as usize;
///         }
///         Ok(())
///     }
///     fn poll(
///         &mut self,
///         max_rows: usize,
///         _deadline: Duration,
///     ) -> anyhow::Result<Vec<SourceChunk>> {
///         let take = max_rows.min(self.lines.len() - self.next);
///         let chunks = self.lines[self.next..][..take]
///             .iter()
///             .map(|l| SourceChunk::plain(l.clone()))
///             .collect();
///         self.next += take;
///         Ok(chunks)
///     }
///     fn cursor(&self) -> Cursor {
///         Cursor::File {
///             path: "mem".into(),
///             byte_pos: self.next as u64,
///         }
///     }
/// }
///
/// let mut src = MemLines {
///     lines: vec![b"a".to_vec(), b"b".to_vec()],
///     next: 0,
/// };
/// src.open(&Cursor::None)?;
/// assert_eq!(src.poll(10, Duration::from_millis(1))?.len(), 2);
/// // The runtime commits this cursor to the OffsetStore only after the
/// // matching write succeeded — the at-least-once anchor.
/// assert_eq!(
///     src.cursor(),
///     Cursor::File {
///         path: "mem".into(),
///         byte_pos: 2
///     }
/// );
/// # Ok::<(), anyhow::Error>(())
/// ```
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
    /// Source-side idle verdict (T-V1 idle-aware alerting), read by the
    /// runtime after every successful `poll`: `Some(true)` = no backlog
    /// behind the cursor (a file read to EOF; a Kafka consumer whose next
    /// offsets have matched the partition end offsets for several
    /// consecutive checks), `Some(false)` = backlog known to exist, `None` =
    /// the connector cannot tell (the default — the runtime then exports no
    /// idle series and lag rules evaluate exactly as before).
    ///
    /// The verdict must be a **position** comparison, never a clock: a slow
    /// consumer with rows still ahead of it is lagging, not idle, and the
    /// console's lag rules must keep evaluating it.
    fn idle(&self) -> Option<bool> {
        None
    }
}
