/// Read cursor (engine-side shape of the cursor semantics).
/// KAFKA: partition → next offset to read; FILE: byte position; JDBC: incremental key value.
///
/// Cursors serialize through the JSON ledger (tailrace-state), so they must
/// round-trip losslessly:
///
/// ```
/// # use std::collections::BTreeMap;
/// # use tailrace_ir::Cursor;
/// // partition → next offset to read (consumed offset + 1)
/// let cur = Cursor::Kafka {
///     topic: "events".into(),
///     group: "tr.p1.s0".into(),
///     offsets: BTreeMap::from([(0, 42), (1, 7)]),
/// };
/// let json = serde_json::to_string(&cur)?;
/// let back: Cursor = serde_json::from_str(&json)?;
/// assert_eq!(back, cur);
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Cursor {
    Kafka {
        topic: String,
        group: String,
        /// partition → next offset to read (consumed offset + 1)
        offsets: BTreeMap<i32, i64>,
    },
    File {
        path: String,
        /// bytes already read
        byte_pos: u64,
    },
    JdbcKey {
        key: String,
        value: String,
    },
    None,
}

/// Source chunk: raw data pulled by one poll + the cursor delta (Decoder input).
///
/// ```
/// # use tailrace_ir::SourceChunk;
/// // Kafka chunks carry their cursor advance …
/// let k = SourceChunk::kafka(b"{\"k\":1}".to_vec(), 0, 100);
/// assert_eq!((k.partition, k.next_offset), (Some(0), Some(100)));
/// // … non-partitioned carriers (file tailing, jdbc polling) are plain bytes.
/// let p = SourceChunk::plain(b"line\n".to_vec());
/// assert_eq!((p.partition, p.next_offset), (None, None));
/// assert_eq!(p.raw, b"line\n".to_vec());
/// ```
#[derive(Debug, Clone)]
pub struct SourceChunk {
    pub raw: Vec<u8>,
    /// Cursor advance for this chunk (e.g. Kafka (partition, next_offset))
    pub partition: Option<i32>,
    pub next_offset: Option<i64>,
}

impl SourceChunk {
    pub fn kafka(raw: Vec<u8>, partition: i32, next_offset: i64) -> Self {
        Self {
            raw,
            partition: Some(partition),
            next_offset: Some(next_offset),
        }
    }

    pub fn plain(raw: Vec<u8>) -> Self {
        Self {
            raw,
            partition: None,
            next_offset: None,
        }
    }
}
