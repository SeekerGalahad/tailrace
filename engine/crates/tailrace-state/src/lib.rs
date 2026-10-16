//! tailrace-state: OffsetStore trait (cursor ledger: commit only after a
//! successful write — the at-least-once anchor) + the local JSON file
//! implementation.
//!
//! A control-plane ledger client (REST) lands with the managed integration;
//! a local RocksDB mirror waits until pipeline volume demands it (YAGNI — a
//! single JSON file per pipeline is enough for v1).
//!
//! Durability: save writes tmp (pid-suffixed to avoid cross-process races) →
//! sync_data → rename → parent-dir fsync, so the rename itself survives power
//! loss. Corrupt-file self-healing: load quarantines corrupt JSON (renamed
//! `.corrupt-{ts}`, scene preserved) → falls back to the previous `.bak`
//! (kept on every successful save) → only when .bak is also bad does it fail
//! closed (better dead than dirty).

use std::path::PathBuf;

use tailrace_ir::Cursor;

/// Statement cumulative counters (committed to the ledger at the same point
/// as the cursor — after a process restart or plan re-push, assembly re-seeds
/// from them, so consumed/flushed totals stay continuous across restarts;
/// same principle as "cursors belong to the data, not the program").
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StatementTotals {
    pub consumed: u64,
    pub flushed: u64,
    pub flushes: u64,
}

/// Ledger entry: cursor + the last flush's idempotency key (replay-window
/// reconciliation) + statement cumulative counters (snapshot at commit time —
/// uncommitted in-memory batches are replayed together with the cursor).
/// Legacy format (bare Cursor object, e.g. {"Kafka":{...}}) is accepted —
/// a hand-written Deserialize instead of untagged: serde untagged buffers
/// through Content, and BTreeMap<i32,i64> integer keys cannot convert from
/// string keys (observed as "data did not match any variant"), so any Cursor
/// holding an i32-keyed partition map must bypass that path.
#[derive(Debug, Clone)]
pub enum LedgerEntry {
    WithAck {
        cursor: Cursor,
        last_ack: Option<String>,
        totals: Option<StatementTotals>,
    },
    Bare(Cursor),
}

impl serde::Serialize for LedgerEntry {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            LedgerEntry::WithAck {
                cursor,
                last_ack,
                totals,
            } => {
                let mut m = s.serialize_map(Some(3))?;
                m.serialize_entry("cursor", cursor)?;
                m.serialize_entry("lastAck", last_ack)?;
                m.serialize_entry("totals", totals)?;
                m.end()
            }
            LedgerEntry::Bare(c) => c.serialize(s),
        }
    }
}

impl<'de> serde::Deserialize<'de> for LedgerEntry {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        if let Some(obj) = v.as_object() {
            if let Some(cv) = obj.get("cursor") {
                let cursor: Cursor =
                    serde_json::from_value(cv.clone()).map_err(serde::de::Error::custom)?;
                let last_ack = obj
                    .get("lastAck")
                    .and_then(|x| x.as_str())
                    .map(String::from);
                // Entries written before totals existed load as None (seed
                // falls back to zero — same behavior as the old version).
                let totals = obj
                    .get("totals")
                    .and_then(|x| serde_json::from_value::<StatementTotals>(x.clone()).ok());
                return Ok(LedgerEntry::WithAck {
                    cursor,
                    last_ack,
                    totals,
                });
            }
        }
        let cursor: Cursor = serde_json::from_value(v).map_err(serde::de::Error::custom)?;
        Ok(LedgerEntry::Bare(cursor))
    }
}

/// Cursor ledger: save=commit after successful write; load=restore at startup.
/// last_ack: None=keep the existing value (cursor commits without a new flush
/// do not clear the account);
/// totals: None=keep the existing value (calls outside commit points carry no
/// counters).
/// What [`OffsetStore::load`] returns: cursor, optional poison-cursor note,
/// optional statement totals.
pub type LoadedCursor = Option<(Cursor, Option<String>, Option<StatementTotals>)>;

pub trait OffsetStore: Send {
    fn save(
        &mut self,
        pipeline_id: u64,
        statement: u32,
        cursor: &Cursor,
        last_ack: Option<&str>,
        totals: Option<StatementTotals>,
    ) -> anyhow::Result<()>;
    fn load(&self, pipeline_id: u64, statement: u32) -> anyhow::Result<LoadedCursor>;
}

/// Local JSON file ledger: one file per pipeline (offsets-{pipeline}.json),
/// atomic writes (tmp+rename).
pub struct FileOffsetStore {
    dir: PathBuf,
}

impl FileOffsetStore {
    pub fn new(dir: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn file_of(&self, pipeline_id: u64) -> PathBuf {
        self.dir.join(format!("offsets-{pipeline_id}.json"))
    }

    fn bak_of(&self, pipeline_id: u64) -> PathBuf {
        self.dir.join(format!("offsets-{pipeline_id}.json.bak"))
    }

    /// All ledger entries of a pipeline (statement index string → entry).
    /// Also the pass-through source for the engine offsets endpoint.
    pub fn load_all(
        &self,
        pipeline_id: u64,
    ) -> anyhow::Result<std::collections::BTreeMap<String, LedgerEntry>> {
        let path = self.file_of(pipeline_id);
        if !path.exists() {
            return Ok(Default::default());
        }
        let data = std::fs::read(&path)?;
        // Compatibility read: try the legacy format (BTreeMap<String, Cursor>)
        // first, then the new format (LedgerEntry).
        match serde_json::from_slice::<std::collections::BTreeMap<String, LedgerEntry>>(&data)
            .or_else(|_| {
                serde_json::from_slice::<std::collections::BTreeMap<String, Cursor>>(&data).map(
                    |m| {
                        m.into_iter()
                            .map(|(k, v)| (k, LedgerEntry::Bare(v)))
                            .collect()
                    },
                )
            }) {
            Ok(map) => Ok(map),
            Err(e) => {
                // Corrupt: quarantine (scene preserved), then try .bak; only
                // fail closed when both are unreadable.
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let corrupt = self
                    .dir
                    .join(format!("offsets-{pipeline_id}.json.corrupt-{ts}"));
                eprintln!(
                    "[STATE] offsets corrupt (pipeline={}): {e} — quarantined as {}, falling back to .bak",
                    pipeline_id,
                    corrupt.display()
                );
                let _ = std::fs::rename(&path, &corrupt);
                let bak = self.bak_of(pipeline_id);
                if bak.exists() {
                    let bd = std::fs::read(&bak);
                    if let Ok(bd) = bd {
                        let parsed: Option<std::collections::BTreeMap<String, LedgerEntry>> =
                            serde_json::from_slice(&bd).ok().or_else(|| {
                                serde_json::from_slice::<std::collections::BTreeMap<String, Cursor>>(&bd)
                                    .ok()
                                    .map(|m| {
                                        m.into_iter()
                                            .map(|(k, v)| (k, LedgerEntry::Bare(v)))
                                            .collect()
                                    })
                            });
                        if let Some(map) = parsed {
                            eprintln!(
                                "[STATE] pipeline={} .bak fallback ok (restored {} statement cursors, replay window = previous version)",
                                pipeline_id,
                                map.len()
                            );
                            return Ok(map);
                        }
                    }
                    eprintln!(
                        "[STATE] pipeline={} .bak unreadable too — staying fail-closed",
                        pipeline_id
                    );
                }
                anyhow::bail!(
                    "offsets corrupt with no usable backup (pipeline={}): {e}",
                    pipeline_id
                )
            }
        }
    }
}

impl OffsetStore for FileOffsetStore {
    fn save(
        &mut self,
        pipeline_id: u64,
        statement: u32,
        cursor: &Cursor,
        last_ack: Option<&str>,
        totals: Option<StatementTotals>,
    ) -> anyhow::Result<()> {
        let path = self.file_of(pipeline_id);
        let mut map = self.load_all(pipeline_id)?;
        // None=keep existing last_ack/totals (cursor commits without a flush
        // keep the account and the counters)
        let (prev_ack, prev_totals) = match map.get(&statement.to_string()) {
            Some(LedgerEntry::WithAck {
                last_ack, totals, ..
            }) => (last_ack.clone(), *totals),
            _ => (None, None),
        };
        let last_ack = last_ack.map(String::from).or(prev_ack);
        let totals = totals.or(prev_totals);
        map.insert(
            statement.to_string(),
            LedgerEntry::WithAck {
                cursor: cursor.clone(),
                last_ack,
                totals,
            },
        );
        // tmp carries the pid — concurrent writers for the same pipeline no
        // longer clobber each other's tmp (rename is atomic, last writer wins)
        let tmp = self
            .dir
            .join(format!("offsets-{pipeline_id}.{}.tmp", std::process::id()));
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&serde_json::to_vec_pretty(&map)?)?;
            // content hits disk before rename — no "rename visible but content
            // is all zeros" after power loss
            f.sync_data()?;
        }
        // keep the current version as .bak before replacing (fallback source
        // for corrupt-file self-healing)
        if path.exists() {
            let _ = std::fs::copy(&path, self.bak_of(pipeline_id));
        }
        std::fs::rename(&tmp, &path)?;
        // directory fsync — the rename directory entry itself is durable
        if let Ok(dirf) = std::fs::File::open(&self.dir) {
            let _ = dirf.sync_all();
        }
        Ok(())
    }

    fn load(
        &self,
        pipeline_id: u64,
        statement: u32,
    ) -> anyhow::Result<Option<(Cursor, Option<String>, Option<StatementTotals>)>> {
        Ok(self
            .load_all(pipeline_id)?
            .remove(&statement.to_string())
            .map(|e| match e {
                LedgerEntry::WithAck {
                    cursor,
                    last_ack,
                    totals,
                } => (cursor, last_ack, totals),
                LedgerEntry::Bare(c) => (c, None, None),
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn roundtrip_and_statement_isolation() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-state-{}", std::process::id()));
        let mut store = FileOffsetStore::new(&dir)?;
        let cur = Cursor::Kafka {
            topic: "t".into(),
            group: "g".into(),
            offsets: BTreeMap::from([(0, 42)]),
        };
        let totals = StatementTotals {
            consumed: 100,
            flushed: 98,
            flushes: 7,
        };
        store.save(7, 0, &cur, Some("p7-r1_0"), Some(totals))?;
        store.save(7, 1, &Cursor::None, None, None)?;
        assert_eq!(
            store.load(7, 0)?,
            Some((cur, Some("p7-r1_0".into()), Some(totals)))
        );
        assert_eq!(store.load(7, 1)?, Some((Cursor::None, None, None)));
        assert_eq!(store.load(7, 2)?, None, "uncommitted statement = no cursor");
        assert_eq!(store.load(8, 0)?, None, "pipeline isolation");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// Totals ride the ledger at cursor commit points: None keeps the old
    /// counters (no reset outside commit points); Some overwrites.
    #[test]
    fn totals_preserved_on_none_and_overwritten_on_some() -> anyhow::Result<()> {
        let dir =
            std::env::temp_dir().join(format!("tailrace-state-totals-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        let mut store = FileOffsetStore::new(&dir)?;
        let cur = Cursor::Kafka {
            topic: "t".into(),
            group: "g".into(),
            offsets: BTreeMap::from([(0, 1)]),
        };
        store.save(
            7,
            0,
            &cur,
            Some("a"),
            Some(StatementTotals {
                consumed: 10,
                flushed: 9,
                flushes: 2,
            }),
        )?;
        // cursor commit without counters (standalone/test path) → totals kept
        store.save(7, 0, &cur, None, None)?;
        assert_eq!(
            store.load(7, 0)?.unwrap().2,
            Some(StatementTotals {
                consumed: 10,
                flushed: 9,
                flushes: 2
            })
        );
        // next commit point carries new totals → overwrite
        store.save(
            7,
            0,
            &cur,
            Some("b"),
            Some(StatementTotals {
                consumed: 25,
                flushed: 24,
                flushes: 5,
            }),
        )?;
        assert_eq!(
            store.load(7, 0)?.unwrap().2,
            Some(StatementTotals {
                consumed: 25,
                flushed: 24,
                flushes: 5
            })
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// Legacy ledger entries (cursor+lastAck without a totals key) load with
    /// totals=None — zero-migration upgrade.
    #[test]
    fn legacy_entry_without_totals_loads_none() -> anyhow::Result<()> {
        let dir =
            std::env::temp_dir().join(format!("tailrace-state-legacy-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join("offsets-5.json"),
            br#"{"0": {"cursor": {"Kafka": {"topic": "t", "group": "g", "offsets": {"0": 7}}}, "lastAck": "old"}}"#,
        )?;
        let store = FileOffsetStore::new(&dir)?;
        let (cur, ack, totals) = store.load(5, 0)?.expect("legacy entry readable");
        assert!(matches!(cur, Cursor::Kafka { .. }));
        assert_eq!(ack.as_deref(), Some("old"));
        assert_eq!(
            totals, None,
            "no totals key = seed falls back to zero (old behavior)"
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// Corrupt-file self-healing — .bak fallback + scene quarantine.
    #[test]
    fn corrupt_file_quarantined_and_bak_fallback() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-state-heal-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        let mut store = FileOffsetStore::new(&dir)?;
        let v1 = Cursor::Kafka {
            topic: "t".into(),
            group: "g".into(),
            offsets: BTreeMap::from([(0, 10)]),
        };
        store.save(9, 0, &v1, None, None)?; // v1 → .bak candidate
        let v2 = Cursor::Kafka {
            topic: "t".into(),
            group: "g".into(),
            offsets: BTreeMap::from([(0, 20)]),
        };
        store.save(9, 0, &v2, Some("lbl-2"), None)?; // v2 lands, v1 goes to .bak
                                                     // corrupt the main file (truncated JSON)
        std::fs::write(dir.join("offsets-9.json"), b"{\"0\": {\"Kafka\": {\"off")?;
        // load must fall back to .bak (v1: offset 10, legacy Bare, no last_ack), not error
        assert_eq!(store.load(9, 0)?, Some((v1, None, None)));
        // scene quarantined (.corrupt-* exists, main file moved away)
        let corrupts: Vec<_> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
            .collect();
        assert_eq!(corrupts.len(), 1, "exactly one quarantined scene");
        // .bak corrupt too → fail-closed (Err, not empty/dirty values)
        std::fs::write(dir.join("offsets-9.json.bak"), b"garbage{{")?;
        let store2 = FileOffsetStore::new(&dir)?;
        std::fs::write(dir.join("offsets-9.json"), b"still bad")?;
        assert!(
            store2.load(9, 0).is_err(),
            "main + .bak both bad = fail-closed"
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
