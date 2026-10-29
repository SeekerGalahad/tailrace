//! Sink capability contract (R7 G4, blueprint §7.7 G4 row): the three
//! production sinks declare what they can really do, and the declarations
//! are locked here. Flipping any flag must land together with the behavior
//! it names — A7 (shipped) flips doris `partial_update` per instance, only
//! when `with_partial_columns` narrowed the carrier to a declared subset;
//! T36③ flips `transactional` when 2PC staging ships. The flags are
//! load-bearing: the runtime refuses a `partial_columns` declaration on any
//! sink whose `partial_update` is false (tailrace-runtime
//! partial_capability_gate tests). Zero external dependencies: sinks are
//! constructed (no client built, no broker contact, no path touched) and
//! only the static declaration is read.

use datafusion::arrow::record_batch::RecordBatch;
use tailrace_conn_doris::DorisSink;
use tailrace_conn_file::FileSink;
use tailrace_conn_kafka::KafkaSink;
use tailrace_connector::{Sink, SinkAck, SinkCapabilities};

/// The all-false floor: what a plain append sink without server-side
/// idempotency honestly is.
fn floor() -> SinkCapabilities {
    SinkCapabilities {
        idempotent_label: false,
        transactional: false,
        overwrite: false,
        partial_update: false,
        partition_aware: false,
    }
}

/// doris, default construction: label idempotency only — every flush
/// carries a stream load label Doris deduplicates globally ("label already
/// exists" converges to success). 2PC staging, overwrite and sink-side
/// partition routing are not implemented (T36③ will flip `transactional`
/// here when it lands); partial-column writes exist (A7) but are off until
/// a subset is declared — see the next test.
#[test]
fn doris_sink_declares_label_idempotency_only() {
    let sink = DorisSink::new(
        "http://127.0.0.1:1/api/ct/db/_stream_load",
        "Basic ct:never-used",
        vec!["k".into()],
        "ct-doris-caps",
    );
    assert_eq!(
        sink.capabilities(),
        SinkCapabilities {
            idempotent_label: true,
            ..floor()
        }
    );
}

/// doris, A7 partial mode: `with_partial_columns` (a validated subset of
/// the sink's columns) flips `partial_update` on THAT instance and nothing
/// else — the declaration follows the behavior, per instance, not per type.
/// The builder is pure (subset validation only), so the on-state is locked
/// here without a BE.
#[test]
fn doris_sink_flips_partial_update_per_instance() {
    let sink = DorisSink::new(
        "http://127.0.0.1:1/api/ct/db/_stream_load",
        "Basic ct:never-used",
        vec!["k".into(), "a".into(), "b".into()],
        "ct-doris-caps-partial",
    )
    .with_partial_columns(vec!["k".into(), "a".into()])
    .expect("a subset of the declared columns is a valid declaration");
    assert_eq!(
        sink.capabilities(),
        SinkCapabilities {
            idempotent_label: true,
            partial_update: true,
            ..floor()
        }
    );
}

/// file: the honest floor. jsonl appends / parquet rolls new part files;
/// the ack key is run-pid based (informational — a replayed flush
/// re-appends), no staging, no replace mode, no column-subset semantics,
/// one flat target path.
#[test]
fn file_sink_declares_the_honest_floor() {
    // construction touches no path (dirs are created at flush time)
    let jsonl = FileSink::new("/tmp/tailrace-ct-caps/never-flushed", Some("jsonl"));
    let parquet = FileSink::new("/tmp/tailrace-ct-caps/never-flushed", Some("parquet"));
    assert_eq!(jsonl.capabilities(), floor());
    assert_eq!(parquet.capabilities(), floor());
}

/// kafka: the honest floor. Producer-session idempotence
/// (enable.idempotence) is NOT idempotent_label — Kafka has no
/// broker-side dedup label, so a replayed flush after a restart
/// re-appends (at-least-once). No transactional producer, no overwrite,
/// no column-subset semantics, payloads carry no partition key.
#[test]
fn kafka_sink_declares_the_honest_floor() {
    // producer construction is lazy — the dummy bootstrap is never
    // contacted (same pattern as conn-kafka's own T6/guard unit tests)
    let sink = KafkaSink::new("localhost:1", "ct-kafka-caps")
        .expect("producer construction needs no broker");
    assert_eq!(sink.capabilities(), floor());
}

/// The trait defaults ARE the contract floor: a sink that overrides
/// nothing declares all-false, and the transaction hooks are callable
/// no-ops (flush stays the single commit point — zero behavior drift for
/// every pre-G4 sink).
#[test]
fn trait_defaults_are_floor_and_txn_hooks_are_noop() {
    struct Plain;
    impl Sink for Plain {
        fn write(&mut self, _batches: Vec<RecordBatch>) -> anyhow::Result<()> {
            Ok(())
        }
        fn flush(&mut self) -> anyhow::Result<SinkAck> {
            Ok(SinkAck {
                rows: 0,
                kind: "plain",
                idempotency_key: String::new(),
            })
        }
    }
    let mut sink = Plain;
    assert_eq!(sink.capabilities(), floor());
    // inert by default: callable, succeed, change nothing
    sink.prepare().expect("default prepare is no-op");
    sink.commit().expect("default commit is no-op");
    sink.abort().expect("default abort is no-op");
    assert_eq!(sink.capabilities(), floor());
}
