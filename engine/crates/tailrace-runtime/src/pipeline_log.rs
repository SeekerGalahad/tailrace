//! Pipeline observability: per-pipeline structured event log (in-memory ring)
//! + atomic counters.
//!
//! Log: `PipelineLogHub` is a global hub, per pipeline a `VecDeque` capped at
//! 500, global LRU cap 5000 pipelines (pipeline churn never blows up memory);
//! a stopped pipeline's logs survive until LRU eviction (the failure tail
//! cause stays inspectable). seq is a global Atomic monotonic; `after_seq`
//! cursor-increment pulls are idempotent. A process restart loses history
//! (triage relies on new events + the console event table).
//! Counters: `PipeCounters` (totals + per-statement AtomicU64), incremented
//! at three hot-path sites: poll return (consumed) / decode skip (skipped) /
//! flush ack (flushed/flushes/last_flush_ms).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// One log entry (serde camelCase externally).
#[derive(Debug, Clone, serde::Serialize)]
pub struct LogEntry {
    pub seq: u64,
    #[serde(rename = "tsMs")]
    pub ts_ms: u64,
    pub level: String,
    pub kind: String,
    pub message: String,
    #[serde(rename = "statementIdx")]
    pub statement_idx: Option<u32>,
}

const PER_PIPELINE_CAP: usize = 500;
const GLOBAL_PIPELINE_CAP: usize = 5_000; // pipeline-count LRU cap (not entry count)

struct PipelineBuffer {
    entries: VecDeque<LogEntry>,
    last_seq: u64,
    /// unix-ms of the last write (LRU eviction basis).
    last_touch_ms: u64,
}

/// Global log hub (clone shares the interior state).
pub struct PipelineLogHub {
    inner: Mutex<HubInner>,
    global_seq: AtomicU64,
    /// WARN/ERROR events mirror to a file (state_dir/events.log). Motivation:
    /// the in-memory ring dies with the process and page-side logs ride the
    /// console SSE bridge — when the console is down or not yet upgraded, the
    /// node keeps zero forensics otherwise. Low volume (WARN/ERROR only),
    /// appended without rotation.
    mirror: Mutex<Option<std::fs::File>>,
}

impl Default for PipelineLogHub {
    fn default() -> Self {
        Self::new()
    }
}

struct HubInner {
    buffers: HashMap<u64, PipelineBuffer>,
    /// access order (head = oldest) — for LRU eviction.
    order: VecDeque<u64>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl PipelineLogHub {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HubInner {
                buffers: HashMap::new(),
                order: VecDeque::new(),
            }),
            global_seq: AtomicU64::new(0),
            mirror: Mutex::new(None),
        }
    }

    /// Constructor with file mirroring: WARN/ERROR events append to disk (an
    /// open failure degrades to pure memory — a log-path problem must not
    /// refuse startup; one eprintln notice).
    pub fn with_mirror(path: &std::path::Path) -> Self {
        let hub = Self::new();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(f) => {
                *hub.mirror.lock().unwrap() = Some(f);
            }
            Err(e) => eprintln!(
                "[LOG-HUB] event mirror file open failed (degrading to memory only): {}: {e}",
                path.display()
            ),
        }
        hub
    }

    /// Append one entry (pipeline log + LRU touch).
    pub fn push(
        &self,
        pipeline_id: u64,
        level: &str,
        kind: &str,
        message: &str,
        statement_idx: Option<u32>,
    ) {
        let seq = self.global_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let entry = LogEntry {
            seq,
            ts_ms: now_ms(),
            level: level.to_string(),
            kind: kind.to_string(),
            message: message.to_string(),
            statement_idx,
        };
        // WARN/ERROR mirror to disk before the in-memory enqueue (the file
        // keeps the trail even if the memory path fails)
        if level == "WARN" || level == "ERROR" {
            if let Some(f) = self.mirror.lock().unwrap().as_mut() {
                use std::io::Write;
                let _ = writeln!(
                    f,
                    "{}\t{}\t{}\tpipeline#{}\t{}",
                    now_ms(),
                    level,
                    kind,
                    pipeline_id,
                    message
                );
            }
        }
        let mut g = self.inner.lock().unwrap();
        if !g.buffers.contains_key(&pipeline_id) {
            g.order.push_back(pipeline_id);
        }
        let buf = g
            .buffers
            .entry(pipeline_id)
            .or_insert_with(|| PipelineBuffer {
                entries: VecDeque::new(),
                last_seq: 0,
                last_touch_ms: 0,
            });
        buf.entries.push_back(entry);
        buf.last_seq = seq;
        buf.last_touch_ms = now_ms();
        if buf.entries.len() > PER_PIPELINE_CAP {
            buf.entries.pop_front();
        }
        // pipeline-count LRU: evict the longest-unwritten pipelines past the
        // cap (all their logs go; entries are not evicted individually)
        if g.buffers.len() > GLOBAL_PIPELINE_CAP {
            while g.buffers.len() > GLOBAL_PIPELINE_CAP {
                if let Some(oldest) = g.order.pop_front() {
                    // touched ones moved to the tail; only evict the truly
                    // oldest and inactive ones
                    if let Some(b) = g.buffers.get(&oldest) {
                        if b.last_touch_ms + 60_000 > now_ms() {
                            g.order.push_back(oldest);
                            continue;
                        }
                    }
                    g.buffers.remove(&oldest);
                } else {
                    break;
                }
            }
        }
    }

    /// Cursor-increment pull: entries after after_seq (last_seq included for
    /// client resumption).
    pub fn tail(&self, pipeline_id: u64, after_seq: u64, limit: usize) -> (Vec<LogEntry>, u64) {
        let g = self.inner.lock().unwrap();
        match g.buffers.get(&pipeline_id) {
            Some(buf) => {
                let entries: Vec<LogEntry> = buf
                    .entries
                    .iter()
                    .filter(|e| e.seq > after_seq)
                    .take(limit)
                    .cloned()
                    .collect();
                (entries, buf.last_seq)
            }
            None => (Vec::new(), 0),
        }
    }

    /// Initial pull: the trailing limit entries (SSE-bridge first screen).
    pub fn last(&self, pipeline_id: u64, limit: usize) -> (Vec<LogEntry>, u64) {
        let g = self.inner.lock().unwrap();
        match g.buffers.get(&pipeline_id) {
            Some(buf) => {
                let skip = buf.entries.len().saturating_sub(limit);
                let entries: Vec<LogEntry> = buf.entries.iter().skip(skip).cloned().collect();
                (entries, buf.last_seq)
            }
            None => (Vec::new(), 0),
        }
    }
}

/// Per-statement counters (AtomicU64, Relaxed increments on the hot path —
/// observability tolerates momentarily stale reads).
#[derive(Debug, Default)]
pub struct StatementCounters {
    pub consumed: AtomicU64,
    pub flushed: AtomicU64,
    pub skipped: AtomicU64,
    /// decoded bad-row count
    pub bad: AtomicU64,
    pub flushes: AtomicU64,
    pub last_flush_ms: AtomicU64,
}

impl StatementCounters {
    pub fn snapshot(&self) -> StatementCountersSnapshot {
        StatementCountersSnapshot {
            consumed: self.consumed.load(Ordering::Relaxed),
            flushed: self.flushed.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            bad: self.bad.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            last_flush_ms: self.last_flush_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatementCountersSnapshot {
    pub consumed: u64,
    pub flushed: u64,
    pub skipped: u64,
    /// decoded bad-row count
    pub bad: u64,
    pub flushes: u64,
    #[serde(rename = "lastFlushMs")]
    pub last_flush_ms: u64,
}

/// Pipeline-level counters (Arc-shared: the registry holds them, statement
/// threads write, stats routes read).
#[derive(Debug)]
pub struct PipeCounters {
    pub statements: Vec<std::sync::Arc<StatementCounters>>,
    pub created_ms: u64,
}

impl PipeCounters {
    pub fn new(statement_count: usize) -> Self {
        Self {
            statements: (0..statement_count)
                .map(|_| std::sync::Arc::new(StatementCounters::default()))
                .collect(),
            created_ms: now_ms(),
        }
    }

    /// Seeded constructor: ledger totals injected at assembly time (totals
    /// stay continuous across process restarts / plan re-pushes); seeds
    /// shorter than the statement count leave the rest at zero (correct
    /// semantics for newly added statements).
    pub fn new_seeded(statement_count: usize, seeds: &[tailrace_state::StatementTotals]) -> Self {
        Self {
            statements: (0..statement_count)
                .map(|i| {
                    let s = seeds.get(i).copied().unwrap_or_default();
                    let c = StatementCounters::default();
                    c.consumed.store(s.consumed, Ordering::Relaxed);
                    c.flushed.store(s.flushed, Ordering::Relaxed);
                    c.flushes.store(s.flushes, Ordering::Relaxed);
                    std::sync::Arc::new(c)
                })
                .collect(),
            created_ms: now_ms(),
        }
    }

    /// Aggregate (totals = per-statement sums).
    pub fn totals(&self) -> StatementCountersSnapshot {
        let mut t = StatementCountersSnapshot {
            consumed: 0,
            flushed: 0,
            skipped: 0,
            bad: 0,
            flushes: 0,
            last_flush_ms: 0,
        };
        for s in &self.statements {
            let s = s.snapshot();
            t.consumed += s.consumed;
            t.flushed += s.flushed;
            t.skipped += s.skipped;
            t.bad += s.bad;
            t.flushes += s.flushes;
            t.last_flush_ms = t.last_flush_ms.max(s.last_flush_ms);
        }
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Restart-continuous totals: seeded construction injects ledger totals;
    /// the aggregate includes the baseline (statistics stay continuous across
    /// restarts).
    #[test]
    fn seeded_counters_totals_include_baseline() {
        let seeds = vec![
            tailrace_state::StatementTotals {
                consumed: 100,
                flushed: 98,
                flushes: 7,
            },
            tailrace_state::StatementTotals {
                consumed: 50,
                flushed: 50,
                flushes: 5,
            },
        ];
        let pc = PipeCounters::new_seeded(3, &seeds); // third statement unseeded = zero
        pc.statements[0].consumed.fetch_add(10, Ordering::Relaxed);
        pc.statements[2].flushed.fetch_add(3, Ordering::Relaxed);
        let t = pc.totals();
        assert_eq!(t.consumed, 160, "100 seed + 10 this round");
        assert_eq!(t.flushed, 151, "98+50 seeds + 3");
        assert_eq!(t.flushes, 12);
        // unseeded (a brand-new pipeline) equals new()
        assert_eq!(PipeCounters::new_seeded(2, &[]).totals().consumed, 0);
    }

    #[test]
    fn seq_monotonic_and_tail_cursor() {
        let hub = PipelineLogHub::new();
        hub.push(1, "INFO", "FLUSH_OK", "a", Some(0));
        hub.push(1, "INFO", "FLUSH_OK", "b", Some(1));
        let (entries, last) = hub.last(1, 10);
        assert_eq!(entries.len(), 2);
        assert!(entries[0].seq < entries[1].seq);
        assert_eq!(last, entries[1].seq);
        // cursor increment: only the new entries
        hub.push(1, "WARN", "SOURCE_BACKOFF", "c", None);
        let (inc, last2) = hub.tail(1, last, 10);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].kind, "SOURCE_BACKOFF");
        assert_eq!(last2, inc[0].seq);
        // unknown pipeline
        assert!(hub.tail(999, 0, 10).0.is_empty());
    }

    #[test]
    fn per_pipeline_cap_evicts_oldest() {
        let hub = PipelineLogHub::new();
        for i in 0..600 {
            hub.push(7, "INFO", "FLUSH_OK", &format!("m{i}"), None);
        }
        let (entries, _) = hub.last(7, 10_000);
        assert_eq!(entries.len(), 500);
        assert!(
            entries[0].message.starts_with("m100"),
            "the oldest 100 entries were evicted"
        );
    }

    /// Mirror: WARN/ERROR hit the file, INFO does not; the format carries
    /// time/level/kind/pipeline id.
    #[test]
    fn mirror_writes_warn_error_only() {
        let dir = std::env::temp_dir().join(format!("tailrace-log-mirror-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("events.log");
        let hub = PipelineLogHub::with_mirror(&path);
        hub.push(1, "INFO", "PIPELINE_STARTED", "started", None);
        hub.push(1, "WARN", "PIPELINE_RESTART", "auto-restart #1", None);
        hub.push(
            1,
            "ERROR",
            "STATEMENT_FAILED",
            "stream load failed",
            Some(0),
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 2, "INFO not mirrored: {text}");
        assert!(text.contains("\tWARN\tPIPELINE_RESTART\tpipeline#1\t"));
        assert!(text.contains("\tERROR\tSTATEMENT_FAILED\tpipeline#1\t"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn counters_concurrent_increment() {
        let c = Arc::new(PipeCounters::new(2));
        let hs: Vec<_> = (0..4)
            .map(|k| {
                let c = Arc::clone(&c);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        c.statements[k % 2].consumed.fetch_add(1, Ordering::Relaxed);
                        c.statements[k % 2].flushed.fetch_add(2, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let t = c.totals();
        assert_eq!(t.consumed, 4000);
        assert_eq!(t.flushed, 8000);
    }
}
