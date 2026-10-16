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

use std::path::{Path, PathBuf};

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

/// Append-only JSONL side file for one statement's bad rows.
#[derive(Debug)]
pub struct BadRowDlq {
    path: PathBuf,
}

impl BadRowDlq {
    /// `state_dir/bad_rows/p{pipeline}-s{statement}.jsonl`.
    pub fn new(state_dir: &Path, pipeline_id: u64, statement: u32) -> Self {
        Self {
            path: state_dir
                .join("bad_rows")
                .join(format!("p{pipeline_id}-s{statement}.jsonl")),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one batch of bad rows (open-append-flush-sync per batch; bad
    /// rows are rare compared to the data path).
    pub fn write(&self, records: &[BadRowRecord]) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        use std::io::Write;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut buf = String::new();
        for r in records {
            buf.push_str(&serde_json::to_string(r)?);
            buf.push('\n');
        }
        f.write_all(buf.as_bytes())?;
        f.sync_data()?;
        Ok(())
    }
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
        fresh.write(&[])?;
        assert!(!fresh.path().exists());
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
