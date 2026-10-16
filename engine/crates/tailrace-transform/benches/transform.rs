//! T1 micro-benchmarks (roadmap §四 T1): hot-path SQL operators — the per-batch
//! fixed cost of running a micro-batch through DataFusion via
//! `transform_cached` (the cached-session path the runtime uses; the doc
//! comment in lib.rs notes session rebuilds used to dominate this cost).
//!
//! Caliber (mirrors docs/BENCHMARKS.md §方法学):
//! - one iteration = ONE 8000-row RecordBatch (runtime default poll_rows)
//!   through `transform_cached` — the per-micro-batch cost, SQL parse/plan
//!   included (v0.1.0 re-plans every micro-batch; T7 changes that and this
//!   bench is its before/after);
//! - `Throughput::Elements(ROWS)` → criterion reports rows/s;
//! - three operator shapes: identity passthrough, filter+project (CPU-light
//!   expression), group-by aggregate (hash build per batch).

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use tailrace_transform::transform_cached;

/// Rows per batch — equal to the runtime default `poll_rows` (8000).
const ROWS: usize = 8000;

/// Deterministic 4-column input batch (utf8 ×2, int64, float64).
fn input_batch() -> datafusion::arrow::record_batch::RecordBatch {
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("op_type", DataType::Utf8, true),
        Field::new("order_no", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
    ]));
    let op: Vec<&str> = (0..ROWS).map(|i| ["I", "U", "D"][i % 3]).collect();
    let order: Vec<String> = (0..ROWS).map(|i| format!("O-{i:07}")).collect();
    let qty: Vec<i64> = (0..ROWS as i64).collect();
    let price: Vec<f64> = (0..ROWS).map(|i| (i % 1000) as f64 + 0.5).collect();
    datafusion::arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(op)),
            Arc::new(StringArray::from(order)),
            Arc::new(Int64Array::from(qty)),
            Arc::new(Float64Array::from(price)),
        ],
    )
    .expect("input batch")
}

fn bench_transform(c: &mut Criterion) {
    let batch = input_batch();

    let mut group = c.benchmark_group("transform");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("identity_select_star", |b| {
        b.iter(|| {
            let out = transform_cached("SELECT * FROM src", batch.clone()).expect("transform");
            std::hint::black_box(out);
        });
    });

    group.bench_function("filter_project", |b| {
        b.iter(|| {
            let out = transform_cached(
                "SELECT order_no, qty * 10 AS qty10, price FROM src WHERE qty > 4000",
                batch.clone(),
            )
            .expect("transform");
            std::hint::black_box(out);
        });
    });

    group.bench_function("groupby_aggregate", |b| {
        b.iter(|| {
            let out = transform_cached(
                "SELECT op_type, COUNT(*) AS n, SUM(qty) AS total_qty FROM src GROUP BY op_type",
                batch.clone(),
            )
            .expect("transform");
            std::hint::black_box(out);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_transform);
criterion_main!(benches);
