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
//! a log of isolation attempts. A record's key is `(statement, cursor,
//! rawB64)` — the same bad bytes at the same poll-time cursor can only recur
//! through an at-least-once replay (flush failure → restart from the last
//! committed cursor re-polls the batch), so within a replay window one bad
//! row lands exactly once. The dedup index lives in memory and is seeded
//! from the side file on first write: a statement restart (new
//! [`BadRowDlq`]) or a process restart therefore still dedups. **The attempt
//! counters are untouched** — `StatementCounters::bad` /
//! `tailrace_dlq_rows_total` keep counting every isolation attempt (the
//! replay storm stays visible on the audit face); only the table is unique.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tailrace_ir::Cursor;

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
            let key = record_key(r.statement, &r.cursor, &r.raw_b64);
            if seen.contains(&key) || !fresh.insert(key) {
                continue; // replayed (or repeated within the batch): already quarantined
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

/// Row key: `(statement, cursor, rawB64)` folded to one u64 (SipHash via
/// `DefaultHasher::new()`, fixed keys — stable within a binary, which is all
/// the index needs: it is rebuilt from the file by whichever binary runs).
/// The cursor enters through its JSON form so a fresh record and a seeded
/// line (both `Cursor` values) hash identically.
fn record_key(statement: u32, cursor: &Cursor, raw_b64: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    statement.hash(&mut h);
    serde_json::to_string(cursor)
        .unwrap_or_default()
        .hash(&mut h);
    raw_b64.hash(&mut h);
    h.finish()
}

/// The key-bearing subset of a side-file line (seeding the index).
#[derive(serde::Deserialize)]
struct SeedRow {
    statement: u32,
    cursor: Cursor,
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
            seen.insert(record_key(row.statement, &row.cursor, &row.raw_b64));
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

/// Build one record (base64 encoding avoids invalid-UTF-8 JSON escapes).
pub fn record(pipeline: u64, statement: u32, cursor: &Cursor, raw: &[u8]) -> BadRowRecord {
    BadRowRecord {
        ts_ms: now_ms(),
        pipeline,
        statement,
        cursor: cursor.clone(),
        raw_b64: {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(raw)
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
        let rec = record(42, 1, &cur, raw_bad);
        assert_eq!(rec.pipeline, 42);
        assert_eq!(rec.statement, 1);
        dlq.write(&[rec])?;
        // a second batch appends
        dlq.write(&[record(42, 1, &cur, b"also bad")])?;
        let text = std::fs::read_to_string(dlq.path())?;
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let v: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(v["pipeline"], 42);
        assert_eq!(v["statement"], 1);
        assert_eq!(v["cursor"]["File"]["byte_pos"], 77);
        assert!(v["tsMs"].as_u64().unwrap() > 0);
        // base64 round-trip of the invalid-UTF-8 payload
        use base64::Engine as _;
        let raw_back = base64::engine::general_purpose::STANDARD
            .decode(v["rawB64"].as_str().unwrap())
            .unwrap();
        assert_eq!(raw_back, raw_bad);
        // empty write = no-op (no file created)
        let fresh = BadRowDlq::new(&dir, 43, 0);
        assert_eq!(fresh.write(&[])?, 0);
        assert!(!fresh.path().exists());
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// T-V2 row-key dedup: a replayed batch (same statement, same poll-time
    /// cursor, same bytes) appends nothing — within one writer, and across a
    /// writer rebuilt on the same path (statement/process restart seeds the
    /// index from the file). The same bytes at another cursor and other
    /// bytes at the same cursor are distinct rows; a repeat inside one batch
    /// lands once; a corrupt line in the file neither breaks seeding nor
    /// hides the rows around it.
    #[test]
    fn replayed_bad_rows_land_once_per_row_key() -> anyhow::Result<()> {
        let dir = tmp_dir("dedup");
        let lines = |p: &Path| -> usize {
            std::fs::read_to_string(p)
                .map(|t| t.lines().filter(|l| !l.trim().is_empty()).count())
                .unwrap_or(0)
        };
        let cur = Cursor::File {
            path: "/tmp/in.jsonl".into(),
            byte_pos: 120,
        };
        let batch = |c: &Cursor| -> Vec<BadRowRecord> {
            vec![
                record(7, 0, c, b"{bad-1"),
                record(7, 0, c, b"\xff\xfe bad-2"),
                record(7, 0, c, b"bad-3}"),
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
        assert_eq!(again.write(&[record(7, 0, &later, b"{bad-1")])?, 1);
        assert_eq!(again.write(&[record(7, 0, &cur, b"bad-4")])?, 1);
        assert_eq!(lines(again.path()), 5);
        // a repeat inside one batch collapses to one row
        assert_eq!(
            again.write(&[record(7, 0, &cur, b"twice"), record(7, 0, &cur, b"twice")])?,
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
        assert_eq!(third.write(&[record(7, 0, &cur, b"bad-5")])?, 1);
        assert_eq!(lines(third.path()), 8, "6 rows + corrupt line + bad-5");
        // the per-statement file is the key's scope: statement 1 at the same
        // cursor/bytes is its own row in its own file
        let other = BadRowDlq::new(&dir, 7, 1);
        assert_eq!(other.write(&[record(7, 1, &cur, b"{bad-1")])?, 1);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
