//! T1 micro-benchmarks (roadmap §四 T1): decoder throughput — bytes → Arrow
//! columns, the first hot-path stage of every pipeline.
//!
//! Caliber (mirrors docs/BENCHMARKS.md §方法学):
//! - one iteration = decode ROWS raw rows into a fresh `ColBatch` + `finish()`
//!   (the same per-micro-batch work the runtime performs; poll_rows default
//!   is 8000, so ROWS = 8000);
//! - `Throughput::Elements(ROWS)` → criterion reports rows/s;
//! - fixtures are deterministic (index-derived), generated once outside the
//!   timed loop; JSON root / OGG envelope / CSV carry the same logical row
//!   (5 columns: utf8 ×3, int64, float64/bool), row width ≈ 90-130 bytes.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use tailrace_decode::{ColBatch, CsvDecoder, Decoder, JsonDecoder, OggDecoder, Pushdown};

/// Rows per iteration — equal to the runtime default `poll_rows` (8000).
const ROWS: usize = 8000;

/// Raw payload rows for one payload shape.
type RawRows = Vec<Vec<u8>>;

fn json_schema() -> datafusion::arrow::datatypes::SchemaRef {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("order_no", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("ok", DataType::Boolean, true),
        Field::new("note", DataType::Utf8, true),
    ]))
}

fn ogg_schema() -> datafusion::arrow::datatypes::SchemaRef {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("op_type", DataType::Utf8, false),
        Field::new("ogg_table", DataType::Utf8, true),
        Field::new("ogg_ts", DataType::Int64, true),
        Field::new("order_no", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
    ]))
}

/// Deterministic fixtures for the three payload shapes (same logical row).
fn fixtures() -> (RawRows, RawRows, RawRows) {
    let mut json = Vec::with_capacity(ROWS);
    let mut ogg = Vec::with_capacity(ROWS);
    let mut csv = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        let op = ["I", "U", "D"][i % 3];
        json.push(
            format!(
                r#"{{"order_no":"O-{i:07}","qty":{i},"price":{}.5,"ok":{},"note":"payload-{i:07}-abcdefghijklmnopqrstuvwxyz"}}"#,
                i % 1000,
                i % 2 == 0
            )
            .into_bytes(),
        );
        ogg.push(
            format!(
                r#"{{"table":"SHOPDB.ORDERS","op_type":"{op}","ts":{},"after":{{"order_no":"O-{i:07}","qty":{i}}}}}"#,
                1_726_000_000_000 + i as i64
            )
            .into_bytes(),
        );
        csv.push(
            format!(
                "O-{i:07},{i},{}.5,{},payload-{i:07}-abcdefghijklmnopqrstuvwxyz",
                i % 1000,
                i % 2 == 0
            )
            .into_bytes(),
        );
    }
    (json, ogg, csv)
}

fn bench_decode(c: &mut Criterion) {
    let (json_rows, ogg_rows, csv_rows) = fixtures();

    let mut group = c.benchmark_group("decode");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("json_root_5col", |b| {
        let dec = JsonDecoder;
        b.iter(|| {
            let mut batch = ColBatch::new(json_schema(), ROWS);
            for raw in &json_rows {
                dec.decode(std::hint::black_box(raw), &mut batch, &Pushdown::default());
            }
            let rb = batch.finish().expect("finish");
            std::hint::black_box(rb);
        });
    });

    group.bench_function("ogg_envelope_5col", |b| {
        let dec = OggDecoder;
        b.iter(|| {
            let mut batch = ColBatch::new(ogg_schema(), ROWS);
            for raw in &ogg_rows {
                dec.decode(std::hint::black_box(raw), &mut batch, &Pushdown::default());
            }
            let rb = batch.finish().expect("finish");
            std::hint::black_box(rb);
        });
    });

    group.bench_function("csv_5col", |b| {
        let dec = CsvDecoder;
        b.iter(|| {
            let mut batch = ColBatch::new(json_schema(), ROWS);
            for raw in &csv_rows {
                dec.decode(std::hint::black_box(raw), &mut batch, &Pushdown::default());
            }
            let rb = batch.finish().expect("finish");
            std::hint::black_box(rb);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
