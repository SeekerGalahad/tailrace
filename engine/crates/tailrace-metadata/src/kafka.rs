//! Kafka metadata via rdkafka (librdkafka is blocking — callers bridge
//! through `spawn_blocking`, see lib.rs).

use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

/// librdkafka's internal topics (`__consumer_offsets`, `__transaction_state`
/// …) are bookkeeping, not user assets.
const INTERNAL_PREFIX: &str = "__";

const METADATA_TIMEOUT: Duration = Duration::from_secs(5);
/// Total sampling ceiling; an empty topic waits it out before reporting
/// "no samples".
const SAMPLE_DEADLINE: Duration = Duration::from_secs(5);
const POLL_SLICE: Duration = Duration::from_millis(200);
/// Consecutive quiet polls (after the first message) that mean "drained".
const QUIET_POLLS_DONE: u32 = 3;

pub(crate) struct TopicInfo {
    pub(crate) name: String,
    pub(crate) partitions: i32,
}

/// List every non-internal topic with its partition count.
pub(crate) fn list_topics(bootstrap: &str) -> anyhow::Result<Vec<TopicInfo>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .map_err(|e| anyhow::anyhow!("kafka 客户端构建失败（{bootstrap}）: {e}"))?;
    let meta = consumer
        .fetch_metadata(None, METADATA_TIMEOUT)
        .map_err(|e| anyhow::anyhow!("kafka 元数据拉取失败（{bootstrap}）: {e}"))?;
    Ok(meta
        .topics()
        .iter()
        .filter(|t| !t.name().starts_with(INTERNAL_PREFIX))
        .map(|t| TopicInfo {
            name: t.name().to_string(),
            partitions: t.partitions().len() as i32,
        })
        .collect())
}

/// Read up to `want` **tail** messages from every partition of `topic`
/// (newest `want` per partition — latest schema wins over ancient formats).
/// Assign-only: no group join, no offset commit, nothing mutated.
pub(crate) fn sample_tail(
    bootstrap: &str,
    topic: &str,
    want: usize,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        // group.id is required by librdkafka's consumer defaults; with
        // assign-only (no subscribe) it never joins a group.
        .set("group.id", format!("tailrace-infer-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        // OffsetTail below the log start resolves here → beginning
        .set("auto.offset.reset", "earliest")
        .set("fetch.message.max.bytes", "1048576")
        .create()
        .map_err(|e| anyhow::anyhow!("kafka 客户端构建失败（{bootstrap}）: {e}"))?;
    let meta = consumer
        .fetch_metadata(Some(topic), METADATA_TIMEOUT)
        .map_err(|e| anyhow::anyhow!("kafka 元数据拉取失败（{bootstrap}）: {e}"))?;
    let parts = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map(|t| t.partitions().len())
        .filter(|n| *n > 0)
        .ok_or_else(|| anyhow::anyhow!("主题不存在或无分区: {topic}"))?;
    let mut tpl = TopicPartitionList::new();
    for p in 0..parts {
        tpl.add_partition_offset(topic, p as i32, Offset::OffsetTail(want as i64))
            .map_err(|e| anyhow::anyhow!("采样位点分配失败（{topic}#{p}）: {e}"))?;
    }
    consumer
        .assign(&tpl)
        .map_err(|e| anyhow::anyhow!("kafka assign 失败（{topic}）: {e}"))?;
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(want);
    let deadline = Instant::now() + SAMPLE_DEADLINE;
    let mut quiet = 0u32;
    while out.len() < want && Instant::now() < deadline {
        match consumer.poll(POLL_SLICE) {
            Some(Ok(msg)) => {
                quiet = 0;
                if let Some(p) = msg.payload() {
                    out.push(p.to_vec());
                }
            }
            Some(Err(e)) => return Err(anyhow::anyhow!("kafka 采样拉取失败（{topic}）: {e}")),
            None => {
                quiet += 1;
                if !out.is_empty() && quiet >= QUIET_POLLS_DONE {
                    break;
                }
            }
        }
    }
    Ok(out)
}
