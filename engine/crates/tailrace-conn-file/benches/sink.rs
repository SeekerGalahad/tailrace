//! T1 micro-benchmarks (roadmap §四 T1): sink serialization — the jsonl line
//! encoding `FileSink::flush` performs (`arrow::json::LineDelimitedWriter`,
//! lib.rs:376-384) before the tmp-file append.
//!
//! Caliber (mirrors docs/BENCHMARKS.md §方法学):
//! - one iteration = encode ROWS rows (runtime default `flush_rows` = 10000,
//!   3 columns: int64/utf8/float64) into a fresh `Vec<u8>`;
//! - two shapes: `jsonl_encode_*` = serialization only (stable, disk-free);
//!   `jsonl_flush_*` = the full `FileSink::flush` (tmp write + fsync +
//!   read-back + append + fsync — DISK-INCLUSIVE, quoted separately as a
//!   disk-sensitive number);
//! - `Throughput::Elements(ROWS)` → criterion reports rows/s.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use tailrace_conn_file::FileSink;
use tailrace_connector::Sink;

/// Rows per flush — equal to the runtime default `flush_rows` (10000).
const ROWS: usize = 10_000;

fn batch() -> datafusion::arrow::record_batch::RecordBatch {
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ratio", DataType::Float64, true),
    ]));
    let oid: Vec<i64> = (0..ROWS as i64).collect();
    let name: Vec<String> = (0..ROWS)
        .map(|i| format!("n{i:07}-abcdefghijklmnopqrstuvwxyz"))
        .collect();
    let ratio: Vec<f64> = (0..ROWS).map(|i| (i % 1000) as f64 + 0.5).collect();
    datafusion::arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(oid)),
            Arc::new(StringArray::from(name)),
            Arc::new(Float64Array::from(ratio)),
        ],
    )
    .expect("bench batch")
}

fn bench_sink(c: &mut Criterion) {
    let mut group = c.benchmark_group("sink");
    group.throughput(Throughput::Elements(ROWS as u64));

    // serialization only — the exact writer/type FileSink::flush uses
    group.bench_function("jsonl_encode_10k_rows", |b| {
        let batch = batch();
        b.iter(|| {
            let mut out = Vec::new();
            {
                let mut w = datafusion::arrow::json::LineDelimitedWriter::new(&mut out);
                w.write(std::hint::black_box(&batch)).expect("write");
                w.finish().expect("finish");
            }
            std::hint::black_box(&out);
        });
    });

    // full flush (tmp write + fsync ×2 + read-back + append) — disk-sensitive;
    // setup (target removal + sink construction + in-memory buffering) is
    // excluded from timing; 8 rotating targets cap /tmp usage (~18MB)
    group.bench_function("jsonl_flush_10k_rows", |b| {
        let batch = batch();
        let dir = std::env::temp_dir().join(format!("tailrace-bench-sink-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("bench dir");
        let seq = std::sync::atomic::AtomicU64::new(0);
        b.iter_batched(
            || {
                let target = dir.join(format!(
                    "out-{}.jsonl",
                    seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 8
                ));
                let _ = std::fs::remove_file(&target);
                let mut sink = FileSink::new(&target, Some("jsonl"));
                sink.write(vec![batch.clone()]).expect("buffer");
                sink
            },
            |mut sink| {
                let ack = sink.flush().expect("flush");
                std::hint::black_box(ack);
            },
            BatchSize::PerIteration,
        );
        let _ = std::fs::remove_dir_all(&dir);
    });

    group.finish();
}

criterion_group!(benches, bench_sink);
criterion_main!(benches);
