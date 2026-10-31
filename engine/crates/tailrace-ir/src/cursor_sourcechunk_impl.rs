/// Read cursor (engine-side shape of the cursor semantics).
/// KAFKA: partition → next offset to read; FILE: byte position; JDBC: incremental key value;
/// CUSTOM: connector-defined `kind` + opaque JSON `payload` (G3, blueprint §5.2).
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
///
/// `Custom` opens the enum to connector-defined position semantics (a CDC
/// LSN, an Iceberg snapshot id / ETag, …) without touching the four built-in
/// shapes: the enum stays externally tagged, so the wire form of those four
/// is byte-identical to pre-G3 and old ledgers load unchanged (read old,
/// write new — see tailrace-state). The engine never interprets `payload`;
/// it only fingerprints it for the poison-data criterion.
///
/// ```
/// # use tailrace_ir::Cursor;
/// let cur = Cursor::Custom {
///     kind: "pg-lsn".into(),
///     payload: serde_json::json!({ "lsn": "0/16B3748", "ts": 1730000000 }),
/// };
/// let json = serde_json::to_string(&cur)?;
/// assert!(json.starts_with(r#"{"Custom":{"kind":"pg-lsn""#));
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
    /// Connector-defined cursor (G3). `kind` names the semantics (e.g.
    /// `pg-lsn`, `iceberg-snapshot`) so a connector can refuse a cursor it
    /// did not write; `payload` is whatever that connector needs to resume —
    /// opaque to the engine. Keep payloads JSON-canonical (finite numbers,
    /// stable key set) so the ledger round-trip stays lossless.
    Custom {
        kind: String,
        payload: serde_json::Value,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// G3 wire-format anchor (blueprint §7.6 "serde tagged enum, old
    /// variants unchanged"): the four pre-G3 shapes keep their externally-
    /// tagged encoding byte for byte — that is what lets pre-G3 ledgers load
    /// without migration — and `Custom` joins them under its own tag with
    /// the payload passed through verbatim. Unknown tags still fail loudly:
    /// openness goes through `Custom`, never through silent acceptance.
    #[test]
    fn wire_format_pins_legacy_tags_and_custom() -> anyhow::Result<()> {
        let legacy = [
            (
                Cursor::Kafka {
                    topic: "t".into(),
                    group: "g".into(),
                    offsets: BTreeMap::from([(0, 42), (1, 7)]),
                },
                r#"{"Kafka":{"topic":"t","group":"g","offsets":{"0":42,"1":7}}}"#,
            ),
            (
                Cursor::File {
                    path: "/var/log/a.log".into(),
                    byte_pos: 1024,
                },
                r#"{"File":{"path":"/var/log/a.log","byte_pos":1024}}"#,
            ),
            (
                Cursor::JdbcKey {
                    key: "id".into(),
                    value: "100".into(),
                },
                r#"{"JdbcKey":{"key":"id","value":"100"}}"#,
            ),
            (Cursor::None, r#""None""#),
        ];
        for (cur, wire) in legacy {
            assert_eq!(serde_json::to_string(&cur)?, wire, "{cur:?} encoding drifted");
            assert_eq!(serde_json::from_str::<Cursor>(wire)?, cur, "{wire} no longer loads");
        }

        // Custom: own tag, kind + payload; serde_json's Map is key-sorted, so
        // the object payload has a canonical form (the runtime fingerprint
        // relies on that stability).
        let custom = Cursor::Custom {
            kind: "iceberg-snapshot".into(),
            payload: serde_json::json!({ "snapshot_id": 123456789012_i64, "etag": "\"abc\"" }),
        };
        let wire = serde_json::to_string(&custom)?;
        assert_eq!(
            wire,
            r#"{"Custom":{"kind":"iceberg-snapshot","payload":{"etag":"\"abc\"","snapshot_id":123456789012}}}"#
        );
        assert_eq!(serde_json::from_str::<Cursor>(&wire)?, custom);

        // payload shapes the engine does not interpret still round-trip
        // losslessly: scalar string (an LSN), null, nested array/object
        for payload in [
            serde_json::json!("0/16B3748"),
            serde_json::json!(null),
            serde_json::json!([1, { "a": [true, 2.5] }]),
        ] {
            let cur = Cursor::Custom {
                kind: "k".into(),
                payload,
            };
            let back: Cursor = serde_json::from_str(&serde_json::to_string(&cur)?)?;
            assert_eq!(back, cur);
        }

        // an unknown variant name is rejected, not folded into anything
        assert!(
            serde_json::from_str::<Cursor>(r#"{"Lsn":{"value":"0/1"}}"#).is_err(),
            "unknown cursor tag must fail to deserialize"
        );
        Ok(())
    }
}
