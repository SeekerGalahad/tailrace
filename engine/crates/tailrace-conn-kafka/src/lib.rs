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

use datafusion::arrow::json::LineDelimitedWriter;
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
/// not committed → the batch replays. The same callback also accounts
/// in-flight payload bytes (T6 `buffered_bytes`): enqueue +len / delivery
/// report −len.
#[derive(Clone, Default)]
struct SinkProducerContext {
    /// first delivery failure (drained once; later failures do not overwrite)
    failed: Arc<Mutex<Option<String>>>,
    /// in-flight acks: send +1 / delivery report -1 (must be 0 after flush
    /// drains)
    pending_acks: Arc<AtomicI64>,
    /// in-flight payload bytes: send +len / delivery report −len — the byte
    /// count handed to librdkafka but not yet delivery-confirmed (the
    /// `sink.buffer-flush.max-bytes` boundary input)
    in_flight_bytes: Arc<AtomicI64>,
}

impl rdkafka::client::ClientContext for SinkProducerContext {}

impl rdkafka::producer::ProducerContext for SinkProducerContext {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &rdkafka::producer::DeliveryResult, _opaque: ()) {
        self.pending_acks.fetch_sub(1, Ordering::Relaxed);
        let delivered = match result {
            Ok(msg) => msg.payload_len(),
            Err((_, msg)) => msg.payload_len(),
        } as i64;
        self.in_flight_bytes.fetch_sub(delivered, Ordering::Relaxed);
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
/// retries deduplicate). Carrier = JSON-lines: one message per row, one
/// `\n`-terminated JSON object per message (see [`encode_json_lines`]).
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

/// JSON-lines carrier: Arrow rows → one JSON object per `\n`-terminated line
/// (the same `arrow::json::LineDelimitedWriter` the file sink ships). JSON
/// string escaping encodes in-field newlines, quotes and emoji inside the
/// line, so one row is always exactly one line — no line split can ever cut
/// a record (the CSV carrier's poison-record failure mode), and numeric
/// columns stay numeric (CSV stringified every column).
fn encode_json_lines(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut w = LineDelimitedWriter::new(&mut buf);
        for b in batches {
            w.write(b)?;
        }
        w.finish()?;
    }
    Ok(buf)
}

impl Sink for KafkaSink {
    /// Real in-flight measurement (T6): payload bytes enqueued to librdkafka
    /// minus bytes whose delivery report already arrived — the byte boundary
    /// (`sink.buffer-flush.max-bytes`) no longer degrades to "never triggers"
    /// for kafka sinks. Drains toward 0 as the broker acks (acks=all);
    /// after a failed flush the undelivered share stays counted (at-least-once
    /// — the batch replays).
    fn buffered_bytes(&self) -> u64 {
        self.ctx.in_flight_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        let buf = encode_json_lines(&batches)?;
        // one \n-terminated JSON object per row — splitting on line
        // boundaries is record-safe by construction
        let text = String::from_utf8(buf)?;
        for line in text.lines().filter(|l| !l.is_empty()) {
            if let Err((e, _)) = self
                .producer
                .send(BaseRecord::<(), str>::to(&self.topic).payload(line))
            {
                anyhow::bail!("kafka enqueue: {e}");
            }
            self.ctx.pending_acks.fetch_add(1, Ordering::Relaxed);
            self.ctx
                .in_flight_bytes
                .fetch_add(line.len() as i64, Ordering::Relaxed);
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

    /// Shared liveness flow (both modes): seed 3 one-column records via the
    /// crate's own sink, consume them back through the source, assert chunks
    /// arrive and the cursor map advanced.
    fn live_roundtrip(bootstrap: &str, topic: &str) -> anyhow::Result<()> {
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        let mut sink = KafkaSink::new(bootstrap, topic)?;
        sink.write(vec![mk("live-a")?, mk("live-b")?, mk("live-c")?])?;
        let ack = sink.flush()?;
        assert_eq!(ack.rows, 3, "sink must deliver all 3 seeded records");
        assert_eq!(
            sink.buffered_bytes(),
            0,
            "delivered+flushed records drain the in-flight byte count"
        );
        let mut src = KafkaSource::new(
            bootstrap,
            topic,
            format!("tailrace-src-test-{}", std::process::id()),
        );
        src.open(&Cursor::None)?;
        // a fresh group join/rebalance takes seconds — and the FIRST join on
        // a cold ephemeral broker (offsets-topic creation + SyncGroup) took
        // 23s+ on a 4-vCPU CI runner (2026-10-17, chaos run 39023147490);
        // poll returns as soon as data arrives, so a 90s ceiling is free on
        // fast hosts and only rescues the slow cold-join case
        let chunks = src.poll(3, Duration::from_secs(90))?;
        assert!(!chunks.is_empty(), "seeded samples must be pullable");
        let Cursor::Kafka { offsets, .. } = src.cursor() else {
            panic!()
        };
        assert!(!offsets.is_empty());
        Ok(())
    }

    #[ignore = "live: TAILRACE_TESTCONTAINERS=1 (ephemeral Kafka) or --ignored against the docker-compose dev stack (seeded tailrace.discover.test topic)"]
    #[test]
    fn kafka_source_live() -> anyhow::Result<()> {
        // Mode ① (opt-in, CI chaos job): ephemeral Kafka via testcontainers
        // (TAILRACE_TESTCONTAINERS=1) — the apache module (default image:
        // apache/kafka-native, which ships no /opt/kafka/bin scripts, so
        // topic creation is best-effort; brokers auto-create on produce).
        // Round-trip sink→source is the actual assertion.
        if std::env::var("TAILRACE_TESTCONTAINERS").ok().as_deref() == Some("1") {
            use testcontainers::core::ExecCommand;
            use testcontainers::runners::SyncRunner;
            use testcontainers_modules::kafka::apache;
            match apache::Kafka::default().start() {
                Ok(kafka) => {
                    let bootstrap = format!(
                        "127.0.0.1:{}",
                        kafka.get_host_port_ipv4(apache::KAFKA_PORT)?
                    );
                    let topic = format!("tailrace.live.{}", std::process::id());
                    if let Ok(create) = kafka.exec(ExecCommand::new([
                        "/opt/kafka/bin/kafka-topics.sh",
                        "--bootstrap-server",
                        "localhost:9092",
                        "--create",
                        "--if-not-exists",
                        "--topic",
                        topic.as_str(),
                        "--partitions",
                        "1",
                        "--replication-factor",
                        "1",
                    ])) {
                        // JVM image path; the native image has no scripts —
                        // log-and-continue (auto-create covers it)
                        if create.exit_code()?.unwrap_or(1) != 0 {
                            tracing::warn!(
                                "[testcontainers] explicit topic create failed — \
                                 relying on broker auto-create"
                            );
                        }
                    }
                    return live_roundtrip(&bootstrap, &topic);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "[testcontainers] docker unavailable — falling back to the live gate"
                    );
                }
            }
        }
        // Mode ② (default): documented live gate — runs only against the
        // tailrace dev Kafka stack (docker-compose.yml); the topic must be
        // pre-seeded by the local dev environment. The gate checks the
        // *documented* precondition (the seeded topic), not bare TCP
        // reachability — an unrelated listener on 9092 (another project's
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
        live_roundtrip("localhost:9092", "tailrace.discover.test")
    }

    /// T6: `buffered_bytes()` is a real in-flight measurement — payload bytes
    /// enqueued minus delivery-confirmed. Offline (unreachable broker: send
    /// only queues locally): the count equals the exact JSON-lines byte total
    /// written and survives a failed flush (nothing delivered → nothing
    /// drained). The drain-to-zero half of the property is pinned by
    /// `live_roundtrip` (testcontainers / live-gated modes).
    #[test]
    fn kafka_sink_buffered_bytes_tracks_in_flight_payload() -> anyhow::Result<()> {
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        let mut sink = KafkaSink::new("localhost:1", "t-t6bytes")?;
        assert_eq!(sink.buffered_bytes(), 0, "empty sink buffers nothing");
        let rows = vec![
            mk("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?,
            mk("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")?,
            mk("cccccccccccccccccccccccccccccc")?,
        ];
        // expected = the exact per-line byte sum the sink enqueues
        let encoded = encode_json_lines(&rows)?;
        let expect: u64 = std::str::from_utf8(&encoded)?
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| l.len() as u64)
            .sum();
        sink.write(rows)?;
        assert_eq!(
            sink.buffered_bytes(),
            expect,
            "in-flight bytes == the exact enqueued payload byte total"
        );
        // a second write accumulates on top
        sink.write(vec![mk("dddddddddddddddddddddddddddddd")?])?;
        assert!(sink.buffered_bytes() > expect, "count keeps accumulating");
        // unreachable broker: flush fails and the undelivered share stays
        // counted (at-least-once — the batch replays)
        sink.flush_timeout = Duration::from_millis(300);
        assert!(sink.flush().is_err());
        assert!(
            sink.buffered_bytes() > 0,
            "undelivered bytes stay counted after a failed flush"
        );
        Ok(())
    }

    /// T15 dynamic partition discovery (pre-work test, written before touching
    /// the poll path): a **running** KafkaSource folds newly added topic
    /// partitions into its poll face **without reopen** — both open modes:
    /// ① subscribe (no cursor: the source's own 30s metadata diff complements
    /// the group's assignment with a cursor-map entry);
    /// ② assign (resumed from a cursor: BaseConsumer does NOT extend a manual
    /// assignment — the source must refresh it itself).
    /// Discovery-latency caliber: the metadata diff runs every 30s
    /// (`fetch_metadata` capped at 5s), so a new partition's first record
    /// enters the poll face at most ~35s after the partition increase; this
    /// test bounds it at 75s and reports the measured latency. `kc` runs
    /// kafka-topics.sh inside the ephemeral container (create/alter — the
    /// synchronous std-API path; no async AdminClient ⇒ no executor
    /// dev-dependency). Returns the grow→consumed latency for the caliber log.
    fn discovery_roundtrip(
        bootstrap: &str,
        topic: &str,
        assign_mode: bool,
        kc: &dyn Fn(&[&str]) -> anyhow::Result<()>,
    ) -> anyhow::Result<Duration> {
        fn wait_partition_count(bootstrap: &str, topic: &str, want: usize) -> anyhow::Result<()> {
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", bootstrap);
            let c: BaseConsumer = cfg.create()?;
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let meta = c.fetch_metadata(Some(topic), Duration::from_secs(5))?;
                let n = meta
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic)
                    .map(|t| t.partitions().len())
                    .unwrap_or(0);
                if n >= want {
                    return Ok(());
                }
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "topic {topic} never reached {want} partitions (has {n})"
                );
                std::thread::sleep(Duration::from_millis(500));
            }
        }

        /// Produce one payload to an explicit partition (fresh producer = fresh
        /// metadata; retries the metadata-race window where partition N is not
        /// yet in the producer's cache).
        fn produce_to(
            bootstrap: &str,
            topic: &str,
            part: i32,
            payload: &str,
        ) -> anyhow::Result<()> {
            let producer: BaseProducer = ClientConfig::new()
                .set("bootstrap.servers", bootstrap)
                .create()?;
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let rec = BaseRecord::<(), str>::to(topic)
                    .partition(part)
                    .payload(payload);
                match producer.send(rec) {
                    Ok(()) => {
                        producer.flush(Duration::from_secs(10))?;
                        return Ok(());
                    }
                    Err((e, _)) => {
                        anyhow::ensure!(
                            Instant::now() < deadline,
                            "produce to {topic}:{part} kept failing: {e}"
                        );
                        std::thread::sleep(Duration::from_millis(500));
                    }
                }
            }
        }

        // ① topic with 2 partitions (in-container kafka-topics.sh — the JVM
        // image ships /opt/kafka/bin; the native image does not)
        kc(&[
            "--create",
            "--topic",
            topic,
            "--partitions",
            "2",
            "--replication-factor",
            "1",
        ])?;
        wait_partition_count(bootstrap, topic, 2)?;
        produce_to(bootstrap, topic, 0, "seed-p0")?;
        produce_to(bootstrap, topic, 1, "seed-p1")?;

        // ② open the source in the requested mode
        let group = format!("tailrace-disc-{}-{assign_mode}", std::process::id());
        let mut src = KafkaSource::new(bootstrap, topic, &group);
        let resume = if assign_mode {
            Cursor::Kafka {
                topic: topic.into(),
                group: group.clone(),
                offsets: [(0, 0), (1, 0)].into_iter().collect(),
            }
        } else {
            Cursor::None
        };
        src.open(&resume)?;

        // ③ baseline: both seed partitions deliver
        let mut seen: std::collections::BTreeSet<i32> = Default::default();
        let t0 = Instant::now();
        while seen.len() < 2 && t0.elapsed() < Duration::from_secs(90) {
            for c in src.poll(100, Duration::from_secs(2))? {
                if let Some(p) = c.partition {
                    seen.insert(p);
                }
            }
        }
        assert_eq!(
            seen.iter().copied().collect::<Vec<_>>(),
            vec![0, 1],
            "baseline: both seed partitions consumed (assign_mode={assign_mode})"
        );

        // ④ grow the topic 2 → 4 partitions and seed the new ones
        kc(&["--alter", "--topic", topic, "--partitions", "4"])?;
        wait_partition_count(bootstrap, topic, 4)?;
        let t_grow = Instant::now();
        produce_to(bootstrap, topic, 2, "new-p2")?;
        produce_to(bootstrap, topic, 3, "new-p3")?;

        // ⑤ keep polling the SAME source — the new partitions must join the
        // poll face (30s diff interval + fetch_metadata; 75s ceiling)
        while !(seen.contains(&2) && seen.contains(&3))
            && t_grow.elapsed() < Duration::from_secs(75)
        {
            for c in src.poll(100, Duration::from_secs(2))? {
                if let Some(p) = c.partition {
                    seen.insert(p);
                }
            }
        }
        let latency = t_grow.elapsed();
        assert!(
            seen.contains(&2) && seen.contains(&3),
            "new partitions joined the poll face without reopen (assign_mode={assign_mode}, seen={seen:?}, waited {latency:?})"
        );
        let Cursor::Kafka { offsets, .. } = src.cursor() else {
            panic!("kafka cursor");
        };
        assert_eq!(
            offsets.keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2, 3],
            "cursor map carries all four partitions (assign_mode={assign_mode})"
        );
        assert!(
            offsets.values().all(|o| *o >= 1),
            "each partition advanced past its record (assign_mode={assign_mode}): {offsets:?}"
        );
        Ok(latency)
    }

    /// Testcontainers-only (TAILRACE_TESTCONTAINERS=1, the CI chaos job):
    /// ephemeral Kafka on the **JVM image** — `apache/kafka:3.9.0` (the exact
    /// image docker-compose.yml runs, ships the /opt/kafka/bin scripts the
    /// native image lacks and which this test execs for topic create/alter).
    /// Booted as a raw image with the compose env recipe + a pre-allocated
    /// fixed host port (advertised.listeners must name a routable address —
    /// the testcontainers-modules apache module's 0.0.0.0 machinery trips the
    /// JVM wrapper's validation). The test **creates and alters topics**, so
    /// it must not run against a shared dev broker — default mode is
    /// `#[ignore]`d (the non-mutating liveness stays in kafka_source_live).
    #[ignore = "live: TAILRACE_TESTCONTAINERS=1 only (creates/alters topics — never a shared broker); CI chaos job runs it via --include-ignored kafka_source"]
    #[test]
    fn kafka_source_discovers_new_partitions_without_reopen() -> anyhow::Result<()> {
        if std::env::var("TAILRACE_TESTCONTAINERS").ok().as_deref() != Some("1") {
            eprintln!(
                "[T15 discovery] TAILRACE_TESTCONTAINERS!=1 — skipping (this test creates and \
                 alters topics; it must not run against a shared dev broker)"
            );
            return Ok(()); // CI-safe / default local
        }
        use testcontainers::core::{CmdWaitFor, ExecCommand, WaitFor};
        use testcontainers::runners::SyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        // pre-allocate a free host port (advertised.listeners needs a
        // routable address BEFORE start; dynamic mapping is unknowable then)
        let host_port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            let p = l.local_addr()?.port();
            drop(l);
            p
        };
        let kafka = GenericImage::new("apache/kafka", "3.9.0")
            .with_wait_for(WaitFor::message_on_stdout(
                // the BROKER's transition — the ControllerServer logs the same
                // phrase earlier, before the broker accepts clients
                "[BrokerServer id=1] Transition from STARTING to STARTED",
            ))
            .with_env_var("KAFKA_NODE_ID", "1")
            .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
            // dual listeners, verbatim the docker-compose.yml recipe: PLAINTEXT
            // serves in-container clients (exec'd kafka-topics.sh) and
            // advertises 9092; EXTERNAL serves host clients and advertises the
            // pre-allocated host port (a single listener cannot do both)
            .with_env_var(
                "KAFKA_LISTENERS",
                "PLAINTEXT://:9092,EXTERNAL://:9094,CONTROLLER://:9093",
            )
            .with_env_var(
                "KAFKA_ADVERTISED_LISTENERS",
                format!("PLAINTEXT://127.0.0.1:9092,EXTERNAL://127.0.0.1:{host_port}"),
            )
            .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
            .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env_var(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
            )
            .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .with_env_var("CLUSTER_ID", "tailR4ceT15Discovery1")
            .with_mapped_port(host_port, testcontainers::core::ContainerPort::Tcp(9094))
            .start()?;
        let bootstrap = format!("127.0.0.1:{host_port}");
        // kafka-topics.sh inside the container (synchronous std API — no async
        // AdminClient ⇒ no executor dev-dependency, Cargo.toml/lock untouched)
        let kc = |args: &[&str]| -> anyhow::Result<()> {
            let mut cmd = vec![
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:9092",
            ];
            cmd.extend_from_slice(args);
            // block until the script exits — the default CmdWaitFor::Nothing
            // returns before the JVM finishes (exit code still unset)
            let mut out =
                kafka.exec(ExecCommand::new(cmd).with_cmd_ready_condition(CmdWaitFor::exit()))?;
            let code = out.exit_code()?.unwrap_or(1);
            let stdout_bytes = out.stdout_to_vec().unwrap_or_default();
            let stderr_bytes = out.stderr_to_vec().unwrap_or_default();
            anyhow::ensure!(
                code == 0,
                "kafka-topics {args:?} exited {code}: stdout={} stderr={}",
                String::from_utf8_lossy(&stdout_bytes),
                String::from_utf8_lossy(&stderr_bytes)
            );
            Ok(())
        };
        // ephemeral broker: the pid keeps the two topics distinct
        let tag = format!("disc{}", std::process::id());
        // both open modes on disjoint topics
        let l_sub = discovery_roundtrip(&bootstrap, &format!("tailrace.{tag}.sub"), false, &kc)?;
        let l_asg = discovery_roundtrip(&bootstrap, &format!("tailrace.{tag}.asg"), true, &kc)?;
        eprintln!(
            "[T15 discovery] subscribe-mode latency {l_sub:?}, assign-mode latency {l_asg:?} (30s diff interval caliber)"
        );
        Ok(())
    }

    /// Poison rows (in-field newlines / quotes / emoji) ride the JSON-lines
    /// carrier without bailing — the CSV carrier used to fail the write
    /// explicitly, which escalated to a registry restart loop and a FAILED
    /// terminal state (a poison record permanently killed the pipeline).
    /// Plus the unchanged fail-closed flush property.
    #[test]
    fn kafka_sink_jsonl_poison_rows_no_bail_and_flush_fails_closed() -> anyhow::Result<()> {
        let mk = |v: &str| -> anyhow::Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec![v.to_string()]))],
            )?)
        };
        // constructible even with an unreachable broker (the producer connects
        // lazily; send only queues locally)
        let mut sink = KafkaSink::new("localhost:1", "t-guard")?;
        // ① a plain row: queued and counted
        sink.write(vec![mk("plain")?])?;
        assert_eq!(sink.pending, 1);
        // ② poison rows — in-field newline, doubled quotes, emoji: one JSON
        // object per row, no record cut, no bail, all queued
        sink.write(vec![
            mk("a\nb")?,
            mk("he said \"hi\" 🎉")?,
            mk("comma, inside\nand a newline 🚀")?,
        ])?;
        assert_eq!(
            sink.pending, 4,
            "poison rows queue (the CSV carrier bailed here)"
        );
        // ③ unreachable broker: flush must explicitly fail (fail-closed —
        // cursor not committed, at-least-once)
        sink.flush_timeout = Duration::from_millis(300);
        assert!(sink.flush().is_err());
        Ok(())
    }

    /// T4 carrier acceptance: rows with in-field newlines, quotes and emoji,
    /// serialized by the kafka sink's JSON-lines carrier, read back
    /// **losslessly** by `arrow::json::Reader` — field values equal, row
    /// count equal, numeric columns stay numeric (the CSV carrier degraded
    /// every column to a string).
    #[test]
    fn kafka_sink_jsonl_carrier_roundtrips_poison_fields() -> anyhow::Result<()> {
        use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
        use datafusion::arrow::json::ReaderBuilder;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
        ]));
        let poisons = [
            "line1\nline2",                    // in-field newline
            "he said \"hi\", twice 🎉",        // doubled quotes + comma + emoji
            "🚀🀄 multiline\nemoji \"field\"", // emoji + newline + quotes
            "tab\tcr\rcontrols",               // control characters
        ];
        let rows: Vec<RecordBatch> = poisons
            .iter()
            .map(|p| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec![Some(*p)])),
                        Arc::new(Int64Array::from(vec![Some(7)])),
                        Arc::new(Float64Array::from(vec![Some(0.5)])),
                    ],
                )
            })
            .collect::<Result<_, _>>()?;
        // the exact bytes the sink ships per write()
        let bytes = encode_json_lines(&rows)?;
        // carrier invariant: exactly one \n-terminated line per row (what makes
        // the per-line produce/consume split record-safe)
        assert_eq!(
            std::str::from_utf8(&bytes)?.lines().count(),
            poisons.len(),
            "one JSON line per row"
        );
        // lossless read-back via arrow::json::Reader
        let mut reader = ReaderBuilder::new(schema.clone()).build(bytes.as_slice())?;
        let mut names: Vec<Option<String>> = Vec::new();
        let mut qtys: Vec<i64> = Vec::new();
        let mut ratios: Vec<f64> = Vec::new();
        for rb in &mut reader {
            let rb = rb?;
            let n = rb
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let q = rb
                .column_by_name("qty")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let r = rb
                .column_by_name("ratio")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..rb.num_rows() {
                names.push(if n.is_null(i) {
                    None
                } else {
                    Some(n.value(i).to_string())
                });
                qtys.push(q.value(i));
                ratios.push(r.value(i));
            }
        }
        let expect: Vec<Option<String>> = poisons.iter().map(|p| Some(p.to_string())).collect();
        assert_eq!(
            names, expect,
            "poison fields (newline/quote/emoji) roundtrip losslessly"
        );
        assert!(qtys.iter().all(|q| *q == 7), "int64 columns stay int64");
        assert!(
            ratios.iter().all(|r| *r == 0.5),
            "float64 columns stay float64"
        );
        Ok(())
    }
}
