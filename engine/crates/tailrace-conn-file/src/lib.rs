//! tailrace-conn-file: file source/sink.
//!
//! Source: text mode tails line by line (FILE_POINTER = byte position) with
//! half-line buffering and truncation/rotation detection; parquet mode is
//! batch-shaped (whole file per read, cursor = file size; a replaced —
//! shorter — file is treated as new data from the beginning).
//! Sink: jsonl appends; parquet rolls a new file per flush boundary.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use datafusion::arrow::record_batch::RecordBatch;

use tailrace_catalog::{ReliabilityOptions, TableObject};
use tailrace_connector::registry::ConnectorRegistry;
use tailrace_connector::{Cursor, Sink, SinkAck, Source, SourceChunk};

/// Register the file source/sink factories.
pub fn register(reg: &mut ConnectorRegistry) {
    reg.register_source("file", source_factory);
    reg.register_sink("file", sink_factory);
}

fn source_factory(
    obj: &TableObject,
    _spec: &tailrace_catalog::StatementSpec,
    _rel: Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Source>> {
    if obj.format.as_deref() == Some("parquet") {
        Ok(Box::new(FileSource::parquet(&obj.name)))
    } else {
        Ok(Box::new(FileSource::new(&obj.name)))
    }
}

fn sink_factory(
    obj: &TableObject,
    _rel: Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Sink>> {
    Ok(Box::new(FileSink::new(&obj.name, obj.format.as_deref())))
}

// ==================== source ====================

/// File source (line-by-line reads, FILE_POINTER = byte position; tailing
/// resident mode ships with the runtime).
///
/// Robustness:
/// - text-mode truncation/rotation detection: file length < cursor → rewind
///   to zero and re-read (same semantics as the parquet branch);
/// - half-line buffering: a line without a trailing newline means the writer
///   is mid-write — buffered, not delivered, cursor not advanced (a crash
///   re-reads the whole line; no half lines are ever produced);
/// - parquet TOCTOU: one fs::read, then judge by data.len() (removes the
///   second-metadata race slice);
/// - missing-file tolerance: NotFound counts a warning (giving up only after
///   ~100 polls ≈ seconds×100), other IO errors propagate.
pub struct FileSource {
    path: PathBuf,
    reader: Option<std::io::BufReader<std::fs::File>>,
    byte_pos: u64,
    parquet: bool,
    /// half-line buffer (a line at EOF without a newline — delivered once the
    /// next round completes it)
    partial: Vec<u8>,
    /// consecutive missing-file polls (throw past the threshold — the file
    /// was moved away, not merely not yet created)
    miss_streak: u32,
    /// truncation/rotation event count (observability)
    truncated_events: u32,
}

/// Consecutive missing-file tolerance (one poll per second-ish — "not created
/// yet" stays tolerated for a ~two-minute window).
const FILE_MISSING_TOLERANCE: u32 = 100;

impl FileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            reader: None,
            byte_pos: 0,
            parquet: false,
            partial: Vec::new(),
            miss_streak: 0,
            truncated_events: 0,
        }
    }

    /// parquet batch shape (the format is decided by the catalog table
    /// object).
    pub fn parquet(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            reader: None,
            byte_pos: 0,
            parquet: true,
            partial: Vec::new(),
            miss_streak: 0,
            truncated_events: 0,
        }
    }

    fn note_missing(&mut self, what: &str) -> anyhow::Result<()> {
        self.miss_streak += 1;
        if self.miss_streak >= FILE_MISSING_TOLERANCE {
            anyhow::bail!(
                "source file missing {} consecutive polls ({}: {}) — judged moved away, not uncreated",
                self.miss_streak,
                what,
                self.path.display()
            );
        }
        if self.miss_streak == 1 || self.miss_streak.is_multiple_of(20) {
            eprintln!(
                "[SOURCE] source file temporarily missing (poll #{}, tolerance {}): {}",
                self.miss_streak,
                FILE_MISSING_TOLERANCE,
                self.path.display()
            );
        }
        Ok(())
    }
}

impl Source for FileSource {
    fn open(&mut self, resume: &Cursor) -> anyhow::Result<()> {
        if let Cursor::File { byte_pos, .. } = resume {
            self.byte_pos = *byte_pos;
        }
        if self.parquet {
            return Ok(());
        }
        use std::io::{Seek, SeekFrom};
        match std::fs::File::open(&self.path) {
            Ok(mut f) => {
                self.miss_streak = 0;
                f.seek(SeekFrom::Start(self.byte_pos))?;
                self.reader = Some(std::io::BufReader::new(f));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // missing is not fatal — in tail scenarios the file may be
                // created later (retry + count during poll)
                eprintln!(
                    "[SOURCE] source file not created yet (will retry during poll): {}",
                    self.path.display()
                );
                self.reader = None;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to open source file {}: {e}",
                    self.path.display()
                ))
            }
        }
        Ok(())
    }

    fn poll(&mut self, max_rows: usize, deadline: Duration) -> anyhow::Result<Vec<SourceChunk>> {
        use std::io::{BufRead, Seek, SeekFrom};
        if self.parquet {
            // TOCTOU removal: read once, then judge by data.len() (a second
            // metadata call previously raced with a truncating slice → an
            // out-of-bounds panic killed the thread)
            let data = match std::fs::read(&self.path) {
                Ok(d) => d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.note_missing("parquet")?;
                    return Ok(vec![]);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to read source file {}: {e}",
                        self.path.display()
                    ))
                }
            };
            self.miss_streak = 0;
            let len = data.len() as u64;
            if len == 0 || self.byte_pos == len {
                return Ok(vec![]); // no new data (or already fully read)
            }
            if self.byte_pos > len {
                self.truncated_events += 1;
                eprintln!(
                    "[SOURCE] parquet file truncated/rotated (len={len} < cursor={}) — rewinding to zero (event #{})",
                    self.byte_pos, self.truncated_events
                );
                self.byte_pos = 0; // the file was replaced (shorter) — treat as new
            }
            let take = &data[self.byte_pos as usize..];
            self.byte_pos = data.len() as u64;
            let _ = (max_rows, deadline); // batch-shaped: one file per batch, micro-batch args unused
            return Ok(vec![SourceChunk::plain(take.to_vec())]);
        }
        // reader rebuild (the tail scenario where the file did not exist at
        // open time — picks up automatically once it appears)
        if self.reader.is_none() {
            match std::fs::File::open(&self.path) {
                Ok(mut f) => {
                    self.miss_streak = 0;
                    f.seek(SeekFrom::Start(self.byte_pos))?;
                    self.reader = Some(std::io::BufReader::new(f));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.note_missing("text")?;
                    return Ok(vec![]);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to open source file {}: {e}",
                        self.path.display()
                    ))
                }
            }
        }
        // text-mode truncation/rotation detection (length < cursor = truncated/rotated)
        if let Ok(m) = std::fs::metadata(&self.path) {
            if m.len() < self.byte_pos {
                self.truncated_events += 1;
                eprintln!(
                    "[SOURCE] text source truncated/rotated (len={} < cursor={}) — rewinding to zero (event #{})",
                    m.len(), self.byte_pos, self.truncated_events
                );
                self.byte_pos = 0;
                self.partial.clear();
                let mut f = std::fs::File::open(&self.path)?;
                f.seek(SeekFrom::Start(0))?;
                self.reader = Some(std::io::BufReader::new(f));
            }
        }
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("source not opened"))?;
        let mut out = Vec::new();
        let started = Instant::now();
        let mut line = String::new();
        while out.len() < max_rows && started.elapsed() < deadline {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                break; // EOF (read everything this round; a half line stays buffered)
            }
            if line.ends_with('\n') {
                // full line: joined with any earlier half line, cursor advances
                // (a crash re-reads from the line start)
                let mut full = std::mem::take(&mut self.partial);
                full.extend_from_slice(line.as_bytes());
                self.byte_pos += full.len() as u64;
                out.push(SourceChunk::plain(
                    String::from_utf8_lossy(&full)
                        .trim_end()
                        .as_bytes()
                        .to_vec(),
                ));
            } else {
                // half line: buffered, not delivered, cursor not advanced —
                // delivered only once the writer completes the newline; a
                // crash also re-reads from the line start (no half-line data)
                self.partial.extend_from_slice(line.as_bytes());
            }
        }
        Ok(out)
    }

    fn cursor(&self) -> Cursor {
        Cursor::File {
            path: self.path.display().to_string(),
            byte_pos: self.byte_pos,
        }
    }
}

// ==================== sink ====================

/// File sink: jsonl=append (target = file path); parquet=cannot append, rolls
/// a new file per flush boundary (target = directory:
/// part-{run}-{seq}.parquet; a file path's parent directory is used instead).
pub struct FileSink {
    target: PathBuf,
    parquet: bool,
    pending: Vec<RecordBatch>,
    pending_rows: u64,
    run: String,
    seq: u64,
}

impl FileSink {
    pub fn new(target: impl Into<PathBuf>, format: Option<&str>) -> Self {
        let target = target.into();
        let parquet = matches!(format, Some("parquet"));
        Self {
            target,
            parquet,
            pending: Vec::new(),
            pending_rows: 0,
            run: format!("run{}", std::process::id()),
            seq: 0,
        }
    }
}

impl Sink for FileSink {
    /// Arrow memory estimate of the pending batches (both jsonl and parquet
    /// shapes buffer in memory up to the flush boundary).
    fn buffered_bytes(&self) -> u64 {
        use datafusion::arrow::array::Array;
        self.pending
            .iter()
            .map(|b| {
                b.columns()
                    .iter()
                    .map(|c| c.get_array_memory_size())
                    .sum::<usize>() as u64
            })
            .sum()
    }

    fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        self.pending_rows += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        self.pending.extend(batches);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<SinkAck> {
        if self.pending_rows == 0 {
            return Ok(SinkAck {
                rows: 0,
                kind: "FILE_WRITE",
                idempotency_key: format!("{}.noop", self.run),
            });
        }
        let rows = self.pending_rows;
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        if self.parquet {
            use datafusion::parquet::arrow::ArrowWriter;
            let dir = if self.target.is_dir() {
                self.target.clone()
            } else {
                self.target
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| PathBuf::from("."))
            };
            std::fs::create_dir_all(&dir)?;
            let file = dir.join(format!("part-{}-{}.parquet", self.run, self.seq));
            self.seq += 1;
            let schema = batches
                .first()
                .map(|b| b.schema())
                .ok_or_else(|| anyhow::anyhow!("parquet flush without batches"))?;
            // write tmp, fsync, then rename to the final name — a crash no
            // longer leaves a corrupt part file behind (writing the final
            // name directly used to permanently burn this run's sequence
            // number on a half-written file)
            let tmp = file.with_extension("parquet.tmp");
            {
                let f = std::fs::File::create(&tmp)?;
                let mut w = ArrowWriter::try_new(f.try_clone()?, schema, None)?;
                for b in &batches {
                    w.write(b)?;
                }
                w.close()?;
                f.sync_all()?; // File::sync_all is an inherent method (&self)
            }
            std::fs::rename(&tmp, &file)?;
            Ok(SinkAck {
                rows,
                kind: "FILE_WRITE",
                idempotency_key: file.display().to_string(),
            })
        } else {
            use std::io::Write;
            let mut out = Vec::new();
            {
                let mut w = datafusion::arrow::json::LineDelimitedWriter::new(&mut out);
                for b in &batches {
                    w.write(b)?;
                }
                w.finish()?;
            }
            if let Some(parent) = self.target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // the whole batch lands in a tmp file (fsync) before appending to
            // the final file — a crash leaves an ignorable tmp behind and the
            // final file never shows a half JSON line
            let tmp = self.target.with_extension(format!(
                "append.{}.{}.tmp",
                std::process::id(),
                self.seq
            ));
            {
                let mut t = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)?;
                t.write_all(&out)?;
                t.sync_all()?;
            }
            // before the first open, recover a historical half line (a
            // newline-less tail left by a past crash — truncate and warn)
            if self.seq == 0 {
                recover_partial_tail(&self.target)?;
            }
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.target)?;
            let buf = std::fs::read(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
            let _ = std::fs::remove_file(&tmp);
            // seq advances (a constant append.0 used to give two flushes the
            // same idempotency key, breaking reconciliation)
            let key = format!("{}/append.{}", self.target.display(), self.seq);
            self.seq += 1;
            Ok(SinkAck {
                rows,
                kind: "FILE_WRITE",
                idempotency_key: key,
            })
        }
    }
}

/// jsonl half-line recovery: a non-newline-terminated tail in the final file
/// is a half line left by a historical crash — truncate to the last newline
/// (lose the half line, keep the whole lines; the jsonl parser would fail the
/// whole batch on a half line anyway).
fn recover_partial_tail(path: &std::path::Path) -> anyhow::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    else {
        return Ok(()); // no historical file
    };
    let len = f.seek(SeekFrom::End(0))?;
    if len == 0 {
        return Ok(());
    }
    // read the last 64KB, find the final newline
    let window = len.min(64 * 1024);
    f.seek(SeekFrom::Start(len - window))?;
    let mut tail = vec![0u8; window as usize];
    let mut got = 0;
    while got < tail.len() {
        let n = f.read(&mut tail[got..])?;
        if n == 0 {
            break;
        }
        got += n;
    }
    tail.truncate(got);
    if tail.last() == Some(&b'\n') {
        return Ok(()); // tail is complete
    }
    let keep = match tail[..].iter().rposition(|b| *b == b'\n') {
        Some(i) => (len - window) + i as u64 + 1,
        None => 0, // no newline within 64KB = the whole span is a half line (abnormal) — clear to 0
    };
    eprintln!(
        "[SINK] jsonl half-line recovery: {} tail {} bytes without newline — truncating to {}",
        path.display(),
        len - keep,
        keep
    );
    f.set_len(keep)?;
    f.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn file_source_reads_and_cursors() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-src-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("a.jsonl");
        std::fs::write(&f, "line1\nline2\nline3\n")?;
        let mut src = FileSource::new(&f);
        src.open(&Cursor::None)?;
        let chunks = src.poll(2, Duration::from_millis(200))?;
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].raw, b"line1");
        let cur = src.cursor();
        let Cursor::File { byte_pos, .. } = &cur else {
            panic!("cursor kind")
        };
        assert_eq!(*byte_pos, 12); // "line1\n" + "line2\n"
                                   // resume from the checkpoint
        let mut src2 = FileSource::new(&f);
        src2.open(&cur)?;
        let rest = src2.poll(10, Duration::from_millis(200))?;
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].raw, b"line3");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn parquet_source_batch_once_and_cursor() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-src-pq-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("batch.parquet");
        std::fs::write(&f, b"fake-parquet-bytes-not-parsed-here")?;
        let mut src = FileSource::parquet(&f);
        src.open(&Cursor::None)?;
        let chunks = src.poll(8, Duration::from_millis(200))?;
        assert_eq!(chunks.len(), 1, "batch-shaped: the whole file is one batch");
        assert_eq!(chunks[0].raw.len(), std::fs::metadata(&f)?.len() as usize);
        let cur = src.cursor();
        let Cursor::File { byte_pos, .. } = &cur else {
            panic!("cursor kind")
        };
        assert_eq!(
            *byte_pos,
            std::fs::metadata(&f)?.len(),
            "cursor = file size"
        );
        // cursor resume: no further output
        let mut resumed = FileSource::parquet(&f);
        resumed.open(&cur)?;
        assert!(resumed.poll(8, Duration::from_millis(100))?.is_empty());
        // file replaced (smaller) → read from the beginning
        std::fs::write(&f, b"new")?;
        let mut replaced = FileSource::parquet(&f);
        replaced.open(&cur)?;
        let again = replaced.poll(8, Duration::from_millis(100))?;
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].raw, b"new");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn text_half_line_and_truncation() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-src-half-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("t.jsonl");
        // ① half line: no newline → not delivered, cursor not advanced
        std::fs::write(&f, b"abc")?;
        let mut src = FileSource::new(&f);
        src.open(&Cursor::None)?;
        assert!(
            src.poll(10, Duration::from_millis(100))?.is_empty(),
            "half line not delivered"
        );
        let Cursor::File { byte_pos, .. } = src.cursor() else {
            panic!()
        };
        assert_eq!(
            byte_pos, 0,
            "half-line cursor does not advance (crash re-reads from the line start)"
        );
        // ② newline completed → full line delivered
        std::fs::write(&f, b"abc\n")?;
        let out = src.poll(10, Duration::from_millis(100))?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].raw, b"abc");
        let Cursor::File { byte_pos, .. } = src.cursor() else {
            panic!()
        };
        assert_eq!(byte_pos, 4);
        // ③ truncation/rotation: file shorter than cursor → rewind and re-read
        std::fs::write(&f, b"x\n")?;
        let out = src.poll(10, Duration::from_millis(100))?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].raw, b"x", "re-read from zero after truncation");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn missing_file_tolerated_then_opened() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-src-miss-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("late.jsonl");
        let mut src = FileSource::new(&f);
        src.open(&Cursor::None)?; // missing is not immediately fatal
        assert!(src.poll(10, Duration::from_millis(50))?.is_empty());
        assert_eq!(src.miss_streak, 1);
        // the file appears late → poll picks it up automatically
        std::fs::write(&f, b"m\n")?;
        let out = src.poll(10, Duration::from_millis(200))?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].raw, b"m");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// jsonl half-line recovery: a historical newline-less tail row gets
    /// truncated before the first flush.
    #[test]
    fn file_sink_jsonl_halfline_recovery() -> anyhow::Result<()> {
        use datafusion::arrow::array::Int64Array;
        let dir = std::env::temp_dir().join(format!("tailrace-sink-half-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir)?;
        let jl = dir.join("half.jsonl");
        let mut hist = b"{\"a\":1}".to_vec();
        hist.push(b'\n');
        hist.extend_from_slice(b"{\"a\":2"); // a half row (simulating an old crash mid-write)
        std::fs::write(&jl, &hist)?;
        let mk = |i: i64| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
                vec![Arc::new(Int64Array::from(vec![i]))],
            )?)
        };
        let mut sink = FileSink::new(&jl, Some("jsonl"));
        sink.write(vec![mk(3)?])?;
        let ack = sink.flush()?;
        assert_eq!(ack.rows, 1);
        let text = std::fs::read_to_string(&jl)?;
        assert!(
            text.as_bytes().last() == Some(&b'\n'),
            "the file ends with a newline after flush"
        );
        assert!(
            !text.contains("a\":2"),
            "the historical half line was truncated"
        );
        assert!(text.contains("a\":1"), "complete lines kept");
        assert!(text.contains("a\":3"), "the new batch was appended");
        // idempotency key seq advances (it used to be a constant append.0)
        sink.write(vec![mk(4)?])?;
        let ack2 = sink.flush()?;
        assert_ne!(
            ack.idempotency_key, ack2.idempotency_key,
            "append seq advances"
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn file_sink_jsonl_append_and_parquet_rollup() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("ratio", DataType::Float64, true),
        ]));
        let mk = |i: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![i])),
                    Arc::new(StringArray::from(vec![format!("n{i}")])),
                    Arc::new(Float64Array::from(vec![i as f64 * 0.5])),
                ],
            )
        };
        let dir = std::env::temp_dir().join(format!("tailrace-sink-file-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        // ① jsonl: append semantics (two flushes, two lines)
        let jl = dir.join("out.jsonl");
        let mut sink = FileSink::new(&jl, Some("jsonl"));
        sink.write(vec![mk(1)?])?;
        assert_eq!(sink.flush()?.rows, 1);
        sink.write(vec![mk(2)?])?;
        assert_eq!(sink.flush()?.rows, 1);
        let text = std::fs::read_to_string(&jl)?;
        assert_eq!(text.lines().count(), 2, "jsonl appends without overwriting");
        assert!(text.contains("\"oid\":1"));
        // ② parquet: rolling files (two flushes, two parts)
        let pdir = dir.join("parquet");
        std::fs::create_dir_all(&pdir)?;
        let mut psink = FileSink::new(&pdir, Some("parquet"));
        psink.write(vec![mk(3)?])?;
        let ack1 = psink.flush()?;
        psink.write(vec![mk(4)?])?;
        let ack2 = psink.flush()?;
        assert_eq!(ack1.rows + ack2.rows, 2);
        assert_ne!(
            ack1.idempotency_key, ack2.idempotency_key,
            "rolling file names differ"
        );
        let parts: Vec<_> = std::fs::read_dir(&pdir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|x| x == "parquet")
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(parts.len(), 2, "two flushes = two part files");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
