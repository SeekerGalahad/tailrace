//! Plan-level pipeline configuration: pushed by the console, defaults when
//! absent (COALESCE semantics).
//!
//! Layering: correctness guardrails (acks/fsync/half-batch rollback and the
//! like) are locked code constants, not exposed here; this module only carries
//! operational-rhythm knobs: reliability (delivery retries) / recovery
//! (self-healing policy) / tuning (batching). The locked defaults are also
//! this module's Default — single source of truth.

use serde::{Deserialize, Serialize};

/// Shape of the plan body's top-level `"options"` key (all sections absent =
/// engine defaults, forward/backward compatible both ways).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanOptions {
    #[serde(default)]
    pub reliability: Option<ReliabilityOptions>,
    #[serde(default)]
    pub recovery: Option<RecoveryOptions>,
    #[serde(default)]
    pub tuning: Option<TuningOptions>,
    /// Consumer group prefix: when set, statement groups are named
    /// `{prefix}.s{statement index}`; otherwise the engine convention
    /// `tr.p{pipeline}.s{n}` applies. Changing the prefix changes the group —
    /// existing offsets are dropped and consumption restarts.
    #[serde(default)]
    pub group_prefix: Option<String>,
    /// Consume start (effective only when no committed offset exists — Kafka
    /// auto.offset.reset semantics): earliest=from the beginning /
    /// latest=from the newest; default earliest (matches existing behavior).
    #[serde(default)]
    pub start_offset: Option<String>,
    /// Bad decoded-row handling (upgrade plan §二 engine H2):
    /// "dlq"=quarantine raw bytes to the bad_rows/ side file (default),
    /// "fail"=statement error (restart cycle / FAILED),
    /// "skip"=count only (legacy behavior, rows are lost).
    /// Invalid values reject the whole plan at deploy time.
    #[serde(default)]
    pub bad_row_policy: Option<String>,
}

impl PlanOptions {
    /// Non-blank prefix (blank after trim = unset).
    pub fn group_prefix(&self) -> Option<&str> {
        self.group_prefix
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
    /// Normalized consume start: invalid values fall back to earliest
    /// (fail-safe to existing behavior).
    pub fn start_offset(&self) -> &'static str {
        match self.start_offset.as_deref().map(str::trim) {
            Some("latest") => "latest",
            _ => "earliest",
        }
    }
}

/// Delivery reliability (doris flush retries / kafka delivery timeout /
/// consumer eviction guard).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReliabilityOptions {
    /// Doris flush retry count (1-10; default 3)
    pub doris_flush_retries: Option<u32>,
    /// Doris retry backoff base in ms (sequence = base, base*4, base*16 capped
    /// at 30000; default 2000)
    pub doris_flush_backoff_base_ms: Option<u64>,
    /// Per stream load HTTP timeout in ms (default 120000)
    pub doris_request_timeout_ms: Option<u64>,
    /// KafkaSink message.timeout.ms (default 180000)
    pub kafka_send_timeout_ms: Option<u64>,
    /// Consumer max.poll.interval.ms (default 600000 — must exceed the longest
    /// backoff 300s + the flush retry window)
    pub max_poll_interval_ms: Option<u64>,
}

impl ReliabilityOptions {
    pub fn flush_retries(&self) -> u32 {
        self.doris_flush_retries.unwrap_or(3).clamp(1, 10)
    }
    /// Backoff sequence: base, base*4, base*16… capped at 30s, length = retry
    /// count.
    pub fn flush_backoff_ms(&self) -> Vec<u64> {
        let base = self
            .doris_flush_backoff_base_ms
            .unwrap_or(2_000)
            .clamp(500, 60_000);
        let n = self.flush_retries() as usize;
        (0..n)
            .map(|i| (base.saturating_mul(1u64 << (2 * i))).min(30_000))
            .collect()
    }
    pub fn doris_timeout_ms(&self) -> u64 {
        self.doris_request_timeout_ms.unwrap_or(120_000)
    }
    pub fn kafka_timeout_ms(&self) -> u64 {
        self.kafka_send_timeout_ms.unwrap_or(180_000)
    }
    pub fn poll_interval_ms(&self) -> u64 {
        self.max_poll_interval_ms.unwrap_or(600_000)
    }
}

/// Automatic recovery (FAILED auto-restart policy; the poison-data guard is
/// unaffected — deterministic faults always stay down).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryOptions {
    /// Auto-restart switch (default true; false = failure is a terminal FAILED
    /// state awaiting a human/console)
    pub auto_restart: Option<bool>,
    /// Max restarts (1-10; default 5)
    pub auto_restart_max: Option<u32>,
    /// Restart backoff sequence (seconds; default [30,60,300], beyond the end
    /// the last value repeats)
    pub restart_backoff_secs: Option<Vec<u64>>,
}

impl RecoveryOptions {
    pub fn enabled(&self) -> bool {
        self.auto_restart.unwrap_or(true)
    }
    pub fn max_restarts(&self) -> u32 {
        self.auto_restart_max.unwrap_or(5).clamp(1, 10)
    }
    pub fn backoff_secs(&self) -> Vec<u64> {
        match &self.restart_backoff_secs {
            Some(v) if !v.is_empty() => v.iter().copied().take(10).collect(),
            _ => vec![30, 60, 300],
        }
    }
}

/// Plan-level overrides of RunOptions (previously fixed defaults, never
/// exposed).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TuningOptions {
    pub flush_rows: Option<u64>,
    pub flush_interval_ms: Option<u64>,
    pub flush_bytes: Option<u64>,
    pub poll_rows: Option<usize>,
    pub poll_slice_ms: Option<u64>,
}
