//! Bad-row dead letter (upgrade plan §二 engine H2).
//!
//! A decoded-bad row used to be permanently dropped (a counter + a 120-char
//! log sample); the cursor crossed it regardless. The DLQ quarantines the
//! raw bytes next to the cursor ledger (`state_dir/bad_rows/`, one JSONL
//! side file per statement) with the poll-time cursor and a timestamp, so
//! the row is recoverable and the at-least-once commit can legitimately
//! advance past it.
//!
//! Policy (plan options `badRowPolicy`, default `dlq`):
//! - `dlq`: quarantine to the side file (this module),
//! - `fail`: statement error → restart cycle / FAILED (strict mode),
//! - `skip`: count only (legacy behavior, the row is lost).
//!
//! DLQ write failure is fatal for the statement: if the raw bytes cannot be
//! persisted, crossing the cursor would destroy them — bail instead and let
//! the at-least-once replay retry.
//!
//! **Row key dedup (T-V2, v1.3)**: the side file is a *set* of bad rows, not
//! a log of isolation attempts. A record's key is `(statement, row position,
//! rawB64)` where the row position is the chunk's **own** cursor advance —
//! Kafka `(partition, next offset)`, file `next byte position` — not the
//! poll-time cursor (R11 review): the poll-end cursor moves with whatever
//! boundary a replay happens to cut (a tail appended during the backoff,
//! a multi-poll batch re-read as one poll, interleaved partitions), so a
//! key on it let replayed rows land again, while two identical bad rows in
//! one poll shared it and collapsed to one record. The row position is
//! stable across replays and distinct per row, so one bad row lands exactly
//! once however the replay is cut, and byte-identical twins at two
//! positions land twice. Carriers without a per-row position (parquet's
//! whole-file chunk, jdbc) fall back to the poll-time cursor — the pre-review
//! key, with its limits. The dedup index lives in memory and is seeded from
//! the side file on first write: a statement restart (new [`BadRowDlq`]) or
//! a process restart therefore still dedups. Lines written before the
//! position landed in the record (pre-1.3) carry none and seed as
//! cursor-keyed; a positioned replay of such a row lands once more — a
//! one-time duplicate across the upgrade, never a loss. **The attempt
//! counters are untouched** — `StatementCounters::bad` /
//! `tailrace_dlq_rows_total` keep counting every isolation attempt (the
//! replay storm stays visible on the audit face); only the table is unique.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tailrace_ir::{Cursor, SourceChunk};

/// One quarantined bad row (JSONL record shape).
#[derive(Debug, serde::Serialize)]
pub struct BadRowRecord {
    /// unix ms
    #[serde(rename = "tsMs")]
    pub ts_ms: u64,
    pub pipeline: u64,
    pub statement: u32,
    /// source cursor at poll time (the replay anchor for manual recovery)
    pub cursor: Cursor,
    /// row position, Kafka partition (absent on file / positionless carriers)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
    /// row position: Kafka next offset / file next byte position — the key's
    /// position component; absent when the carrier has none (then the key
    /// falls back to `cursor`)
    #[serde(rename = "nextOffset", skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<i64>,
    /// raw bytes, base64 (bad rows are frequently invalid UTF-8)
    #[serde(rename = "rawB64")]
    pub raw_b64: String,
}

/// Bad-row policy (plan options `badRowPolicy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BadRowPolicy {
    /// Quarantine to the bad_rows/ side file (default).
    #[default]
    Dlq,
    /// Statement error on the first bad row (restart cycle / FAILED).
    Fail,
    /// Count only — legacy behavior, the row is lost.
    Skip,
}

impl BadRowPolicy {
    /// Lenient parse for runtime-internal use; deploy-time validation
    /// (rejecting unknown values) happens in the registry.
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_lowercase()).as_deref() {
            Some("fail") => BadRowPolicy::Fail,
            Some("skip") => BadRowPolicy::Skip,
            _ => BadRowPolicy::Dlq,
        }
    }

    /// Strict parse (deploy-time plan validation): unknown values are an
    /// error instead of silently degrading.
    pub fn parse_strict(value: Option<&str>) -> anyhow::Result<Self> {
        match value {
            None => Ok(BadRowPolicy::Dlq),
            Some(v) if v.eq_ignore_ascii_case("dlq") => Ok(BadRowPolicy::Dlq),
            Some(v) if v.eq_ignore_ascii_case("fail") => Ok(BadRowPolicy::Fail),
            Some(v) if v.eq_ignore_ascii_case("skip") => Ok(BadRowPolicy::Skip),
            Some(v) => anyhow::bail!(
                "invalid options.badRowPolicy {v:?} (expected dlq|fail|skip) — plan rejected"
            ),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BadRowPolicy::Dlq => "dlq",
            BadRowPolicy::Fail => "fail",
            BadRowPolicy::Skip => "skip",
        }
    }
}

/// Append-only JSONL side file for one statement's bad rows, unique on the
/// row key (module doc, T-V2).
#[derive(Debug)]
pub struct BadRowDlq {
    path: PathBuf,
    /// Row keys already in the side file. `None` until the first write
    /// seeds it from disk ([`seed_index`]) — a statement that never sees a
    /// bad row never reads the file. Mutex (not `&mut`): the statement loop
    /// holds its runtime by shared reference at the write site.
    seen: Mutex<Option<HashSet<u64>>>,
}

impl BadRowDlq {
    /// `state_dir/bad_rows/p{pipeline}-s{statement}.jsonl`.
    pub fn new(state_dir: &Path, pipeline_id: u64, statement: u32) -> Self {
        Self {
            path: state_dir
                .join("bad_rows")
                .join(format!("p{pipeline_id}-s{statement}.jsonl")),
            seen: Mutex::new(None),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one batch of bad rows (open-append-flush-sync per batch; bad
    /// rows are rare compared to the data path), deduplicated on the row key:
    /// a record already in the side file — a replayed bad row — is skipped.
    /// Returns the number of records appended; the caller's attempt counter
    /// is independent of it (metrics keep counting every attempt).
    pub fn write(&self, records: &[BadRowRecord]) -> anyhow::Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut guard = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let seen = guard.get_or_insert_with(|| seed_index(&self.path));
        // keys land in the index only after the bytes are on disk — a failed
        // append must not leave a key that silently swallows the retry
        let mut fresh: HashSet<u64> = HashSet::new();
        let mut buf = String::new();
        for r in records {
            let key = record_key(
                r.statement,
                &r.cursor,
                r.partition,
                r.next_offset,
                &r.raw_b64,
            );
            if seen.contains(&key) || !fresh.insert(key) {
                continue; // replayed (or, positionless, repeated within the batch): already quarantined
            }
            buf.push_str(&serde_json::to_string(r)?);
            buf.push('\n');
        }
        if fresh.len() < records.len() {
            tracing::debug!(
                path = %self.path.display(),
                skipped = records.len() - fresh.len(),
                "DLQ dedup: replayed bad rows already quarantined"
            );
        }
        if fresh.is_empty() {
            return Ok(0);
        }
        use std::io::Write;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(buf.as_bytes())?;
        f.sync_data()?;
        let appended = fresh.len();
        seen.extend(fresh);
        Ok(appended)
    }
}

/// Row key: `(statement, row position | poll cursor, rawB64)` folded to one
/// u64 (SipHash via `DefaultHasher::new()`, fixed keys — stable within a
/// binary, which is all the index needs: it is rebuilt from the file by
/// whichever binary runs). A chunk with its own position hashes
/// `(partition, next_offset)` — stable across replays whatever poll boundary
/// they cut, distinct for identical bytes at two positions; a positionless
/// one hashes the poll cursor's JSON form (so a fresh record and a seeded
/// line hash identically). The two shapes are tagged apart.
fn record_key(
    statement: u32,
    cursor: &Cursor,
    partition: Option<i32>,
    next_offset: Option<i64>,
    raw_b64: &str,
) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    statement.hash(&mut h);
    match next_offset {
        Some(offset) => {
            1u8.hash(&mut h);
            partition.hash(&mut h);
            offset.hash(&mut h);
        }
        None => {
            0u8.hash(&mut h);
            serde_json::to_string(cursor)
                .unwrap_or_default()
                .hash(&mut h);
        }
    }
    raw_b64.hash(&mut h);
    h.finish()
}

/// The key-bearing subset of a side-file line (seeding the index). The
/// position fields default to absent so pre-1.3 lines (no position written)
/// seed as cursor-keyed.
#[derive(serde::Deserialize)]
struct SeedRow {
    statement: u32,
    cursor: Cursor,
    #[serde(default)]
    partition: Option<i32>,
    #[serde(rename = "nextOffset", default)]
    next_offset: Option<i64>,
    #[serde(rename = "rawB64")]
    raw_b64: String,
}

/// Rebuild the dedup index from the side file's existing lines — a
/// statement or process restart rebuilds [`BadRowDlq`], and the index must
/// survive that or the replay after the restart would land every bad row
/// again. Unparseable lines are skipped (a corrupt line must not block
/// quarantine); a missing file is an empty index.
fn seed_index(path: &Path) -> HashSet<u64> {
    let mut seen = HashSet::new();
    let Ok(bytes) = std::fs::read(path) else {
        return seen;
    };
    for line in String::from_utf8_lossy(&bytes).lines() {
        if let Ok(row) = serde_json::from_str::<SeedRow>(line) {
            seen.insert(record_key(
                row.statement,
                &row.cursor,
                row.partition,
                row.next_offset,
                &row.raw_b64,
            ));
        }
    }
    seen
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build one record from the bad chunk (its raw bytes base64-encoded —
/// avoids invalid-UTF-8 JSON escapes — and its own position, the row key).
pub fn record(pipeline: u64, statement: u32, cursor: &Cursor, chunk: &SourceChunk) -> BadRowRecord {
    BadRowRecord {
        ts_ms: now_ms(),
        pipeline,
        statement,
        cursor: cursor.clone(),
        partition: chunk.partition,
        next_offset: chunk.next_offset,
        raw_b64: {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(&chunk.raw)
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "tailrace-dlq-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A positionless chunk (the parquet / jdbc shape).
    fn plain(raw: &[u8]) -> SourceChunk {
        SourceChunk::plain(raw.to_vec())
    }

    /// Non-blank line count of a side file (0 when absent).
    fn lines(p: &Path) -> usize {
        std::fs::read_to_string(p)
            .map(|t| t.lines().filter(|l| !l.trim().is_empty()).count())
            .unwrap_or(0)
    }

    #[test]
    fn policy_parse_lenient_and_strict() {
        assert_eq!(BadRowPolicy::parse(None), BadRowPolicy::Dlq);
        assert_eq!(BadRowPolicy::parse(Some("fail")), BadRowPolicy::Fail);
        assert_eq!(BadRowPolicy::parse(Some("SKIP")), BadRowPolicy::Skip);
        assert_eq!(BadRowPolicy::parse(Some("???")), BadRowPolicy::Dlq);
        assert_eq!(BadRowPolicy::parse_strict(None).unwrap(), BadRowPolicy::Dlq);
        assert_eq!(
            BadRowPolicy::parse_strict(Some("Dlq")).unwrap(),
            BadRowPolicy::Dlq
        );
        let err = BadRowPolicy::parse_strict(Some("drop"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("badRowPolicy") && err.contains("drop"),
            "{err}"
        );
    }

    #[test]
    fn side_file_records_and_recoverability() -> anyhow::Result<()> {
        let dir = tmp_dir("side");
        let dlq = BadRowDlq::new(&dir, 42, 1);
        assert!(
            dlq.path().ends_with("bad_rows/p42-s1.jsonl"),
            "{:?}",
            dlq.path()
        );
        let cur = Cursor::File {
            path: "/tmp/in.jsonl".into(),
            byte_pos: 77,
        };
        // invalid UTF-8 raw bytes must round-trip exactly
        let raw_bad: &[u8] = b"{\"broken\": \xff\xfe";
        let rec = record(42, 1, &cur, &SourceChunk::file(raw_bad.to_vec(), 77));
        assert_eq!(rec.pipeline, 42);
        assert_eq!(rec.statement, 1);
        dlq.write(&[rec])?;
        // a second batch appends — a positionless chunk this time
        dlq.write(&[record(42, 1, &cur, &plain(b"also bad"))])?;
        let text = std::fs::read_to_string(dlq.path())?;
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let v: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(v["pipeline"], 42);
        assert_eq!(v["statement"], 1);
        assert_eq!(v["cursor"]["File"]["byte_pos"], 77);
        assert!(v["tsMs"].as_u64().unwrap() > 0);
        // the row position rides the record: file = next byte position, no
        // partition key at all
        assert_eq!(v["nextOffset"], 77);
        assert!(
            v.get("partition").is_none(),
            "a file row writes no partition key: {v}"
        );
        // base64 round-trip of the invalid-UTF-8 payload
        use base64::Engine as _;
        let raw_back = base64::engine::general_purpose::STANDARD
            .decode(v["rawB64"].as_str().unwrap())
            .unwrap();
        assert_eq!(raw_back, raw_bad);
        // a positionless chunk writes neither position key — the pre-1.3
        // line shape, byte for byte
        let v2: serde_json::Value = serde_json::from_str(lines[1])?;
        assert!(
            v2.get("partition").is_none() && v2.get("nextOffset").is_none(),
            "{v2}"
        );
        // a kafka chunk writes both
        dlq.write(&[record(
            42,
            1,
            &cur,
            &SourceChunk::kafka(b"k-bad".to_vec(), 3, 1001),
        )])?;
        let text = std::fs::read_to_string(dlq.path())?;
        let v3: serde_json::Value = serde_json::from_str(text.lines().nth(2).unwrap())?;
        assert_eq!(
            (v3["partition"].as_i64(), v3["nextOffset"].as_i64()),
            (Some(3), Some(1001))
        );
        // empty write = no-op (no file created)
        let fresh = BadRowDlq::new(&dir, 43, 0);
        assert_eq!(fresh.write(&[])?, 0);
        assert!(!fresh.path().exists());
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// The positionless fallback (parquet / jdbc chunks carry no per-row
    /// position): the key is `(statement, poll-time cursor, rawB64)` — the
    /// pre-review key with its limits, kept so those carriers still dedup
    /// a replay that cuts the same poll boundary. A replayed batch (same
    /// statement, same cursor, same bytes) appends nothing — within one
    /// writer, and across a writer rebuilt on the same path (statement /
    /// process restart seeds the index from the file). The same bytes at
    /// another cursor and other bytes at the same cursor are distinct rows;
    /// a repeat inside one batch collapses (this carrier cannot tell two
    /// identical rows apart — the documented limit); a corrupt line in the
    /// file neither breaks seeding nor hides the rows around it.
    #[test]
    fn positionless_rows_fall_back_to_the_cursor_key() -> anyhow::Result<()> {
        let dir = tmp_dir("dedup");
        let cur = Cursor::File {
            path: "/tmp/in.jsonl".into(),
            byte_pos: 120,
        };
        let batch = |c: &Cursor| -> Vec<BadRowRecord> {
            vec![
                record(7, 0, c, &plain(b"{bad-1")),
                record(7, 0, c, &plain(b"\xff\xfe bad-2")),
                record(7, 0, c, &plain(b"bad-3}")),
            ]
        };
        let dlq = BadRowDlq::new(&dir, 7, 0);
        assert_eq!(
            dlq.write(&batch(&cur))?,
            3,
            "first isolation lands all three"
        );
        // replay storm within the same statement incarnation
        for _ in 0..3 {
            assert_eq!(
                dlq.write(&batch(&cur))?,
                0,
                "replayed batch appends nothing"
            );
        }
        assert_eq!(lines(dlq.path()), 3);
        // statement restart: a fresh writer on the same path seeds from disk
        let again = BadRowDlq::new(&dir, 7, 0);
        assert_eq!(again.write(&batch(&cur))?, 0, "index rebuilt from the file");
        assert_eq!(lines(again.path()), 3);
        // distinct rows: same bytes further down the stream, new bytes at
        // the same cursor
        let later = Cursor::File {
            path: "/tmp/in.jsonl".into(),
            byte_pos: 480,
        };
        assert_eq!(again.write(&[record(7, 0, &later, &plain(b"{bad-1"))])?, 1);
        assert_eq!(again.write(&[record(7, 0, &cur, &plain(b"bad-4"))])?, 1);
        assert_eq!(lines(again.path()), 5);
        // a repeat inside one batch collapses to one row (the positionless
        // limit — a positioned carrier keeps both, see the next test)
        assert_eq!(
            again.write(&[
                record(7, 0, &cur, &plain(b"twice")),
                record(7, 0, &cur, &plain(b"twice"))
            ])?,
            1
        );
        assert_eq!(lines(again.path()), 6);
        // corrupt line on disk: seeding skips it, dedup still holds for the
        // parseable rows, and new rows still append
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(again.path())?;
            writeln!(f, "{{not json")?;
        }
        let third = BadRowDlq::new(&dir, 7, 0);
        assert_eq!(third.write(&batch(&cur))?, 0);
        assert_eq!(third.write(&[record(7, 0, &cur, &plain(b"bad-5"))])?, 1);
        assert_eq!(lines(third.path()), 8, "6 rows + corrupt line + bad-5");
        // the per-statement file is the key's scope: statement 1 at the same
        // cursor/bytes is its own row in its own file
        let other = BadRowDlq::new(&dir, 7, 1);
        assert_eq!(other.write(&[record(7, 1, &cur, &plain(b"{bad-1"))])?, 1);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// R11 review of T-V2: a row with its own position is keyed on it, not
    /// on the poll-time cursor. A replay that cuts a different poll
    /// boundary (the tail grew during the backoff, a multi-poll batch
    /// re-read as one poll) carries a different cursor but the same row
    /// positions → nothing lands twice; byte-identical rows at two
    /// positions in ONE batch both land (the collapse the cursor key
    /// caused was a quarantine loss); the same holds for kafka
    /// `(partition, offset)` with two partitions sharing an offset; a fresh
    /// writer seeds positioned lines from disk; and a pre-1.3 line (no
    /// position written) stays cursor-keyed — a positioned replay of it
    /// lands once more, the documented one-time upgrade duplicate.
    #[test]
    fn row_position_keys_survive_replay_boundaries_and_keep_twins() -> anyhow::Result<()> {
        let dir = tmp_dir("rowpos");
        let dlq = BadRowDlq::new(&dir, 9, 0);
        let eof_first = Cursor::File {
            path: "/tmp/in.jsonl".into(),
            byte_pos: 60,
        };
        // first attempt: a 5-line file, lines 2 and 4 bad, read to EOF=60
        let first = vec![
            record(9, 0, &eof_first, &SourceChunk::file(b"{bad-a".to_vec(), 24)),
            record(9, 0, &eof_first, &SourceChunk::file(b"{bad-b".to_vec(), 48)),
        ];
        assert_eq!(dlq.write(&first)?, 2);
        // the tail grows during the backoff; the replay reads everything in
        // one poll and ends at EOF=90 — a different cursor, the same two
        // positions, plus one new bad line at 84
        let eof_second = Cursor::File {
            path: "/tmp/in.jsonl".into(),
            byte_pos: 90,
        };
        let replay = vec![
            record(
                9,
                0,
                &eof_second,
                &SourceChunk::file(b"{bad-a".to_vec(), 24),
            ),
            record(
                9,
                0,
                &eof_second,
                &SourceChunk::file(b"{bad-b".to_vec(), 48),
            ),
            record(
                9,
                0,
                &eof_second,
                &SourceChunk::file(b"{bad-c".to_vec(), 84),
            ),
        ];
        assert_eq!(
            dlq.write(&replay)?,
            1,
            "only the new line lands — the replayed rows are keyed on their position, not the cursor"
        );
        assert_eq!(lines(dlq.path()), 3);
        // byte-identical twins at two positions in one batch: both land
        assert_eq!(
            dlq.write(&[
                record(
                    9,
                    0,
                    &eof_second,
                    &SourceChunk::file(b"twice".to_vec(), 100)
                ),
                record(
                    9,
                    0,
                    &eof_second,
                    &SourceChunk::file(b"twice".to_vec(), 106)
                ),
            ])?,
            2,
            "two rows, two positions, two records"
        );
        // and the same twin replayed at its position is still one row
        assert_eq!(
            dlq.write(&[record(
                9,
                0,
                &eof_second,
                &SourceChunk::file(b"twice".to_vec(), 100)
            )])?,
            0
        );
        assert_eq!(lines(dlq.path()), 5);
        // kafka: partition is part of the position — offset 7 on p0 and p1
        // are two rows; the same (p0, 7) under another poll cursor is one
        let kcur = |o: i64| Cursor::Kafka {
            topic: "t".into(),
            group: "g".into(),
            offsets: std::collections::BTreeMap::from([(0, o), (1, o)]),
        };
        let kdlq = BadRowDlq::new(&dir, 9, 1);
        assert_eq!(
            kdlq.write(&[
                record(9, 1, &kcur(8), &SourceChunk::kafka(b"k-bad".to_vec(), 0, 7)),
                record(9, 1, &kcur(8), &SourceChunk::kafka(b"k-bad".to_vec(), 1, 7)),
            ])?,
            2
        );
        assert_eq!(
            kdlq.write(&[record(
                9,
                1,
                &kcur(40),
                &SourceChunk::kafka(b"k-bad".to_vec(), 0, 7)
            )])?,
            0,
            "replayed under a different poll cursor: same partition/offset, one row"
        );
        // restart: a fresh writer seeds the positioned lines and still
        // dedups the positioned replay
        let again = BadRowDlq::new(&dir, 9, 0);
        assert_eq!(
            again.write(&replay)?,
            0,
            "positioned index rebuilt from the file"
        );
        assert_eq!(lines(again.path()), 5);
        // a pre-1.3 line (no position written) seeds cursor-keyed: the same
        // bytes at the same cursor, positionless, dedup against it; the
        // positioned form of that row does not — one more line, once
        let legacy = BadRowDlq::new(&dir, 9, 2);
        {
            use std::io::Write;
            std::fs::create_dir_all(legacy.path().parent().unwrap())?;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(legacy.path())?;
            writeln!(
                f,
                r#"{{"tsMs":1,"pipeline":9,"statement":2,"cursor":{{"File":{{"path":"/tmp/in.jsonl","byte_pos":60}}}},"rawB64":"e2JhZC1h"}}"#
            )?; // "{bad-a"
        }
        assert_eq!(
            legacy.write(&[record(9, 2, &eof_first, &plain(b"{bad-a"))])?,
            0
        );
        assert_eq!(
            legacy.write(&[record(
                9,
                2,
                &eof_first,
                &SourceChunk::file(b"{bad-a".to_vec(), 24)
            )])?,
            1,
            "the upgrade boundary: one more record, then positioned from here on"
        );
        assert_eq!(
            legacy.write(&[record(
                9,
                2,
                &eof_second,
                &SourceChunk::file(b"{bad-a".to_vec(), 24)
            )])?,
            0
        );
        assert_eq!(lines(legacy.path()), 2);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
