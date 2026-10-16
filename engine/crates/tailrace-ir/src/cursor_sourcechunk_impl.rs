/// Read cursor (engine-side shape of the cursor semantics).
/// KAFKA: partition → next offset to read; FILE: byte position; JDBC: incremental key value.
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
