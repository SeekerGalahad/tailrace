//! tailrace-conn-kafka: Kafka source/sink.
//!
//! **Sync API decision**: rdkafka's async wrappers (StreamConsumer/
//! FutureProducer) have a delivery/future resolution black hole on
//! current-thread runtimes (measured: recv ~0.2ms, DeliveryFuture ~6.4ms per
//! record), so the data plane is synchronous throughout — thread orchestration
//! belongs to the runtime. The trait stays directly drivable and testable
//! without a runtime.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::csv;
use datafusion::arrow::record_batch::RecordBatch;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use tailrace_catalog::{ReliabilityOptions, StatementSpec, TableObject};
use tailrace_connector::registry::ConnectorRegistry;
use tailrace_connector::{Cursor, Sink, SinkAck, Source, SourceChunk};

/// Register the kafka source/sink factories (called by the binary's
/// build_registry / the runtime's default_registry).
pub fn register(reg: &mut ConnectorRegistry) {
    reg.register_source("kafka", source_factory);
    reg.register_sink("kafka", sink_factory);
}

fn source_factory(
    obj: &TableObject,
    spec: &StatementSpec,
    rel: Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Source>> {
    let mut src = KafkaSource::new(
        obj.bootstrap.as_deref().unwrap_or_default(),
        &obj.name,
        &spec.group,
    );
    if let Some(r) = rel {
        src = src.with_poll_interval(r.poll_interval_ms());
    }
    // Statement-level consume start (earliest/latest; default earliest —
    // effective only when no committed offset exists)
    if let Some(so) = spec.start_offset.as_deref() {
        src = src.with_start_offset(so);
    }
    Ok(Box::new(src))
}

fn sink_factory(
    obj: &TableObject,
    rel: Option<&ReliabilityOptions>,
) -> anyhow::Result<Box<dyn Sink>> {
    let sink = match rel {
        Some(r) => KafkaSink::with_send_timeout(
            obj.bootstrap.as_deref().unwrap_or_default(),
            &obj.name,
            Some(r.kafka_timeout_ms()),
        )?,
        None => KafkaSink::new(obj.bootstrap.as_deref().unwrap_or_default(), &obj.name)?,
    };
    Ok(Box::new(sink))
}

// ==================== source ====================

/// Kafka source (rdkafka BaseConsumer, synchronous poll; group = the
/// statement cursor group convention tr.p{pipeline}.s{stmt}).
pub struct KafkaSource {
    bootstrap: String,
    topic: String,
    group: String,
    consumer: Option<BaseConsumer>,
    offsets: BTreeMap<i32, i64>,
    /// Last partition-discovery check (Flink partition discovery pattern —
    /// diff every 30s)
    last_partition_check: Option<std::time::Instant>,
    /// open() entered assign mode (non-empty resume cursor) — newly
    /// discovered partitions require an assignment refresh
    assigned: bool,
    /// Current pause state (an assignment refresh clears librdkafka's pause —
    /// must re-pause, otherwise newly discovered partitions get consumed by
    /// the keepalive poll and dropped while PAUSED)
    paused: bool,
    /// max.poll.interval.ms (default 600000 — must exceed the longest backoff
    /// 300s + the flush retry window)
    poll_interval_ms: u64,
    /// Consume start (earliest/latest, default earliest): effective only when
    /// no committed offset exists — used as auto.offset.reset when subscribing
    /// without a cursor, and as the start position for newly discovered
    /// partitions.
    start_offset: String,
}

impl KafkaSource {
    pub fn new(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
        group: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            topic: topic.into(),
            group: group.into(),
            consumer: None,
            offsets: BTreeMap::new(),
            last_partition_check: None,
            assigned: false,
            paused: false,
            poll_interval_ms: 600_000,
            start_offset: "earliest".into(),
        }
    }

    /// Eviction-guard interval override (injected at construction time).
    pub fn with_poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms.clamp(60_000, 3_600_000);
        self
    }

    /// Consume start override (earliest/latest; invalid values fall back to
    /// earliest) — effective only without a saved cursor.
    pub fn with_start_offset(mut self, offset: impl Into<String>) -> Self {
        let v = offset.into();
        self.start_offset = if v.eq_ignore_ascii_case("latest") {
            "latest".into()
        } else {
            "earliest".into()
        };
        self
    }
}

impl Source for KafkaSource {
    fn open(&mut self, resume: &Cursor) -> anyhow::Result<()> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap)
            .set("group.id", &self.group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &self.start_offset)
            .set("fetch.max.bytes", "8388608")
            // Eviction guard: the longest backoff 300s + the worst-case Doris
            // flush retry window (~520s = 4×120s + 40s backoff) must both stay
            // under this interval, otherwise group eviction → rejoin → full
            // replay storm.
            .set("max.poll.interval.ms", self.poll_interval_ms.to_string())
            .set("session.timeout.ms", "30000")
            .create()?;
        if let Cursor::Kafka {
            topic,
            group,
            offsets,
        } = resume
        {
            // Cursor repositioning check (Debezium offset-namespace pattern):
            // a topic/group mismatch is treated as no-cursor (prevents
            // misaligned consumption across topics after plan changes shift
            // statement indices)
            if *topic != self.topic || *group != self.group {
                tracing::warn!(
                    topic = %self.topic,
                    group = %self.group,
                    cursor_topic = %topic,
                    cursor_group = %group,
                    "cursor reposition failed — treating as no-cursor"
                );
            } else if !offsets.is_empty() {
                // The resume path **assigns, never subscribes** — mixing
                // subscribe+assign lets the background group rebalance
                // overwrite the manual assignment once it completes (no
                // Kafka-side commits → auto.offset.reset=earliest replays the
                // whole topic even with offsets {600,300} saved). Assign-only
                // mode has no group session, so no rebalance exists and the
                // offsets are fully self-managed.
                self.offsets = offsets.clone();
                let mut tpl = TopicPartitionList::new();
                for (p, next) in &self.offsets {
                    // Partitions at offset 0 assign too (seek 0 = earliest) —
                    // filtering on *next>0 used to permanently exclude
                    // "partitions that had no messages last round" from
                    // consumption after recovery.
                    tpl.add_partition_offset(&self.topic, *p, Offset::Offset(*next))?;
                }
                consumer.assign(&tpl)?;
                self.assigned = true;
                self.consumer = Some(consumer);
                return Ok(());
            }
        }
        consumer.subscribe(&[self.topic.as_str()])?;
        self.consumer = Some(consumer);
        Ok(())
    }

    fn poll(&mut self, max_rows: usize, deadline: Duration) -> anyhow::Result<Vec<SourceChunk>> {
        // read the start-offset criterion first (cannot borrow all of self
        // while the consumer is mutably borrowed)
        let latest = self.start_offset == "latest";
        let consumer = self
            .consumer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("source not opened"))?;

        // Flink partition discovery pattern: diff the partition set every 30s
        // — new partitions consume from the start position
        let should_check = self
            .last_partition_check
            .map(|t| t.elapsed() >= Duration::from_secs(30))
            .unwrap_or(true);
        if should_check {
            self.last_partition_check = Some(std::time::Instant::now());
            if let Ok(meta) = consumer.fetch_metadata(None, Duration::from_secs(5)) {
                if let Some(topic_meta) = meta.topics().iter().find(|t| t.name() == self.topic) {
                    let known: std::collections::BTreeSet<i32> =
                        self.offsets.keys().copied().collect();
                    let mut discovered = false;
                    for part in topic_meta.partitions() {
                        if !known.contains(&part.id()) {
                            // New partitions start at the statement's start
                            // position (earliest=0; latest=high watermark,
                            // fetch_watermarks failure fails safe to 0)
                            let next = if latest {
                                consumer
                                    .fetch_watermarks(&self.topic, part.id(), Duration::from_secs(5))
                                    .map(|(_, hw)| hw)
                                    .unwrap_or_else(|e| {
                                        tracing::warn!(
                                            topic = %self.topic,
                                            partition = part.id(),
                                            error = %e,
                                            "partition discovery: high watermark fetch failed → falling back to the beginning"
                                        );
                                        0
                                    })
                            } else {
                                0
                            };
                            tracing::info!(
                                topic = %self.topic,
                                partition = part.id(),
                                start = %self.start_offset,
                                "partition discovery: new partition"
                            );
                            self.offsets.entry(part.id()).or_insert(next);
                            discovered = true;
                        }
                    }
                    // In assign mode, incremental partitions must explicitly
                    // refresh the assignment — rdkafka BaseConsumer does not
                    // extend it automatically; only updating the offsets map
                    // would mean the new partition's messages are never
                    // fetched. (In subscribe mode the group rebalance covers
                    // it — untouched.)
                    if discovered && self.assigned {
                        let mut tpl = TopicPartitionList::new();
                        for (p, next) in &self.offsets {
                            tpl.add_partition_offset(&self.topic, *p, Offset::Offset(*next))?;
                        }
                        consumer.assign(&tpl)?;
                        // assign resets the pause state — re-pause if paused
                        // (otherwise the new partition gets truly consumed by
                        // the keepalive poll and dropped = data loss)
                        if self.paused {
                            let _ = consumer.pause(&tpl);
                        }
                    }
                }
            }
        }
        let mut out = Vec::new();
        // In-batch cursor staging: on a mid-batch Err the cursor map does not
        // advance, and the consumer's internal position is seek back to the
        // batch start (poll advances on return — not seeking back silently
        // loses the half batch). The first message's offset is the batch's
        // per-partition start (holds in both assign and subscribe modes).
        let mut first_seen: std::collections::BTreeMap<i32, i64> =
            std::collections::BTreeMap::new();
        let mut pending_offsets: Vec<(i32, i64)> = Vec::new();
        let started = Instant::now();
        while out.len() < max_rows && started.elapsed() < deadline {
            match consumer.poll(Duration::from_millis(50)) {
                Some(Ok(msg)) => {
                    let partition = msg.partition();
                    let next = msg.offset() + 1;
                    let raw = msg.payload().unwrap_or(&[]).to_vec();
                    first_seen.entry(partition).or_insert(msg.offset());
                    pending_offsets.push((partition, next));
                    out.push(SourceChunk::kafka(raw, partition, next));
                }
                Some(Err(e)) => {
                    for (p, start) in &first_seen {
                        if let Err(se) = consumer.seek(
                            &self.topic,
                            *p,
                            Offset::Offset(*start),
                            Duration::from_secs(5),
                        ) {
                            tracing::error!(
                                topic = %self.topic,
                                partition = *p,
                                batch_start_offset = *start,
                                error = %se,
                                "half-batch rollback seek failed"
                            );
                        }
                    }
                    return Err(anyhow::anyhow!(
                        "kafka poll (half batch rolled back, awaiting re-pull): {e}"
                    ));
                }
                None => {}
            }
        }
        for (p, next) in pending_offsets {
            self.offsets.insert(p, next);
        }
        Ok(out)
    }

    fn cursor(&self) -> Cursor {
        Cursor::Kafka {
            topic: self.topic.clone(),
            group: self.group.clone(),
            offsets: self.offsets.clone(),
        }
    }

    fn pause(&mut self) -> bool {
        let Some(c) = self.consumer.as_ref() else {
            return false;
        };
        match c.assignment() {
            Ok(tpl) => match c.pause(&tpl) {
                Ok(()) => {
                    self.paused = true;
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        topic = %self.topic,
                        error = %e,
                        "pause failed (degrading to sleep heartbeat, eviction risk remains)"
                    );
                    false
                }
            },
            Err(e) => {
                tracing::warn!(
                    topic = %self.topic,
                    error = %e,
                    "pause could not fetch assignment"
                );
                false
            }
        }
    }

    fn resume(&mut self) {
        self.paused = false;
        let Some(c) = self.consumer.as_ref() else {
            return;
        };
        if let Ok(tpl) = c.assignment() {
            if let Err(e) = c.resume(&tpl) {
                tracing::warn!(topic = %self.topic, error = %e, "resume failed");
            }
        }
    }

    fn keepalive(&mut self, deadline: Duration) {
        // Paused partitions poll empty — this keeps the librdkafka poll
        // thread alive (evading group eviction) without consuming or committing
        let _ = self.poll(1, deadline);
    }
}

// ==================== sink ====================

/// Delivery-report collection context: librdkafka silently drops undeliverable
/// messages while flush still "succeeds" — a false-success data-loss channel.
/// The explicit callback turns failures into pipeline errors → the cursor is
/// not committed → the batch replays.
#[derive(Clone, Default)]
struct SinkProducerContext {
    /// first delivery failure (drained once; later failures do not overwrite)
    failed: Arc<Mutex<Option<String>>>,
    /// in-flight acks: send +1 / delivery report -1 (must be 0 after flush
    /// drains)
    pending_acks: Arc<AtomicI64>,
}

impl rdkafka::client::ClientContext for SinkProducerContext {}

impl rdkafka::producer::ProducerContext for SinkProducerContext {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &rdkafka::producer::DeliveryResult, _opaque: ()) {
        self.pending_acks.fetch_sub(1, Ordering::Relaxed);
        if let Err((e, _)) = result {
            let mut g = self.failed.lock().unwrap();
            if g.is_none() {
                *g = Some(format!("{e}"));
            }
        }
    }
}

/// Kafka sink (BaseProducer batching + flush = at-least-once; acks=all +
/// idempotence: success means leader + all replicas persisted, and in-process
/// retries deduplicate).
pub struct KafkaSink {
    producer: BaseProducer<SinkProducerContext>,
    topic: String,
    pending: u64,
    label_seq: u64,
    run: String,
    ctx: SinkProducerContext,
    /// flush drain wait cap (default 180s = message.timeout.ms; tests inject
    /// short values)
    flush_timeout: Duration,
}

impl KafkaSink {
    pub fn new(bootstrap: &str, topic: impl Into<String>) -> anyhow::Result<Self> {
        Self::with_send_timeout(bootstrap, topic, None)
    }

    /// Delivery timeout configurable (message.timeout.ms and the flush drain
    /// wait share the source; None = 180s default, clamped 1s-600s).
    /// librdkafka cannot change this at runtime — construction-time only.
    pub fn with_send_timeout(
        bootstrap: &str,
        topic: impl Into<String>,
        send_timeout_ms: Option<u64>,
    ) -> anyhow::Result<Self> {
        let ms = send_timeout_ms.unwrap_or(180_000).clamp(1_000, 600_000);
        let ctx = SinkProducerContext::default();
        let producer: BaseProducer<SinkProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("queue.buffering.max.ms", "5")
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", ms.to_string())
            .create_with_context(ctx.clone())?;
        Ok(Self {
            producer,
            topic: topic.into(),
            pending: 0,
            label_seq: 0,
            run: format!("run{}", std::process::id()),
            ctx,
            flush_timeout: Duration::from_millis(ms),
        })
    }
}

impl Sink for KafkaSink {
    fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        {
            let mut w = csv::WriterBuilder::new().with_header(false).build(&mut buf);
            for b in &batches {
                w.write(b)?;
            }
        }
        let text = String::from_utf8(buf)?;
        for line in text.lines().filter(|l| !l.is_empty()) {
            // RFC4180: a quoted field containing newlines gets split by
            // lines() into two broken messages. A complete record has an even
            // quote count (pairs + doubled quotes inside); odd = the record
            // was cut — fail explicitly so it replays.
            if line.matches('"').count() % 2 == 1 {
                anyhow::bail!(
                    "kafka sink: CSV carrier has an in-field newline (record cut) — clean newlines in the source data or switch to a JSON carrier"
                );
            }
            if let Err((e, _)) = self
                .producer
                .send(BaseRecord::<(), str>::to(&self.topic).payload(line))
            {
                anyhow::bail!("kafka enqueue: {e}");
            }
            self.ctx.pending_acks.fetch_add(1, Ordering::Relaxed);
            self.pending += 1;
        }
        self.producer.poll(Duration::ZERO);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<SinkAck> {
        self.producer.flush(self.flush_timeout)?;
        // flush returning != delivered: check the delivery failure slot and
        // the in-flight ack count; anything short of zero = explicit failure
        if let Some(err) = self.ctx.failed.lock().unwrap().take() {
            anyhow::bail!("kafka delivery failed (cursor not committed, will replay): {err}");
        }
        let outstanding = self.ctx.pending_acks.load(Ordering::Relaxed);
        if outstanding > 0 {
            anyhow::bail!(
                "kafka flush timeout: {outstanding} records without delivery confirmation (cursor not committed, will replay)"
            );
        }
        let key = format!("kafka.{}.{}", self.run, self.label_seq);
        self.label_seq += 1;
        let rows = self.pending;
        self.pending = 0;
        Ok(SinkAck {
            rows,
            kind: "KAFKA_PRODUCE",
            idempotency_key: key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    /// Live-gate probe: the documented precondition of the live tests is the
    /// seeded dev topic (runtime live gate rationale), not bare reachability.
    fn kafka_topic_seeded(bootstrap: &str, topic: &str) -> bool {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap);
        let client: BaseConsumer = match cfg.create() {
            Ok(c) => c,
            Err(_) => return false,
        };
        client
            .fetch_metadata(None, Duration::from_secs(3))
            .map(|m| m.topics().iter().any(|t| t.name() == topic))
            .unwrap_or(false)
    }

    #[test]
    fn kafka_source_live() -> anyhow::Result<()> {
        // Runs only against the tailrace dev Kafka stack (docker-compose.yml);
        // the topic must be pre-seeded by the local dev environment. The gate
        // checks the *documented* precondition (the seeded topic), not bare
        // TCP reachability — an unrelated listener on 9092 (another project's
        // stack) must not turn this into a spurious failure (same rationale
        // as the runtime live gate).
        if std::net::TcpStream::connect("localhost:9092").is_err() {
            return Ok(()); // CI-safe
        }
        if !kafka_topic_seeded("localhost:9092", "tailrace.discover.test") {
            tracing::warn!(
                "[live-gate] kafka on 9092 has no seeded tailrace.discover.test topic \
                 (another project's stack?) — skipping the live test"
            );
            return Ok(()); // local-liveness only
        }
        let mut src = KafkaSource::new(
            "localhost:9092",
            "tailrace.discover.test",
            format!("tailrace-src-test-{}", std::process::id()),
        );
        src.open(&Cursor::None)?;
        let chunks = src.poll(3, Duration::from_secs(8))?; // a fresh group join/rebalance takes seconds
        assert!(!chunks.is_empty(), "seeded samples must be pullable");
        let Cursor::Kafka { offsets, .. } = src.cursor() else {
            panic!()
        };
        assert!(!offsets.is_empty());
        Ok(())
    }

    #[test]
    fn kafka_sink_csv_guard_and_fail_closed() -> anyhow::Result<()> {
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        // constructible even with an unreachable broker (the producer connects
        // lazily; send only queues locally)
        let mut sink = KafkaSink::new("localhost:1", "t-guard")?;
        // ① a normal single-line CSV: queued and counted
        sink.write(vec![mk("plain")?])?;
        assert_eq!(sink.pending, 1);
        // ② an in-field newline (RFC4180 quoted, spanning lines) → lines()
        // cuts the record → explicit failure, no broken message produced
        let err = sink.write(vec![mk("a\nb")?]).unwrap_err().to_string();
        assert!(err.contains("in-field newline"), "{err}");
        assert_eq!(sink.pending, 1, "the failed line is not queued");
        // ③ unreachable broker: flush must explicitly fail (fail-closed —
        // cursor not committed, at-least-once)
        sink.flush_timeout = Duration::from_millis(300);
        assert!(sink.flush().is_err());
        Ok(())
    }
}
