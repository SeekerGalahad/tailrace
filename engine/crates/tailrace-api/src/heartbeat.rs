//! Engine node heartbeat and identity.
//!
//! Enabled with `serve --control-plane URL`: identity persists at
//! `state_dir/identity.json` (node_key stays stable across restarts; **the
//! console echo is authoritative** — a differing nodeKey in the heartbeat
//! response means adoption: switch to it and persist). The heartbeat thread
//! (`--heartbeat-secs`, default 30s) registers first (idempotent) then
//! heartbeats; events buffer in a ring (capacity 64, cleared on a 200);
//! graceful shutdown (SIGTERM/SIGINT → NODE_STOP event + one final heartbeat
//! → process exit). Without `--control-plane` nothing of this activates.

use crate::StateHandle;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub const EVENT_RING_CAPACITY: usize = 64;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

// ==================== A6 (/metrics): last-beat note ====================

/// Last console-accepted contact: (node_key, unix-ms). Written by the
/// heartbeat thread on every 200 (heartbeat or register), read by the
/// /metrics renderer. Age is computed **at scrape time** — a beat-time gauge
/// would always read ~0 by construction.
static LAST_BEAT: Mutex<Option<(String, u64)>> = Mutex::new(None);

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Record a successful console contact (called by the heartbeat loop).
pub fn note_beat(node_key: &str) {
    *LAST_BEAT.lock().unwrap_or_else(|e| e.into_inner()) = Some((node_key.to_string(), unix_ms()));
}

/// `(node_key, seconds since the last accepted beat)` for
/// `tailrace_node_heartbeat_age_seconds` — None when no beat ever succeeded
/// (no --control-plane, or the console has never been reachable).
pub fn heartbeat_age_seconds() -> Option<(String, f64)> {
    let g = LAST_BEAT.lock().unwrap_or_else(|e| e.into_inner());
    g.as_ref()
        .map(|(k, ms)| (k.clone(), unix_ms().saturating_sub(*ms) as f64 / 1000.0))
}

// ==================== identity ====================

/// Node identity (persisted at state_dir/identity.json).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeIdentity {
    pub node_key: String,
    pub name: String,
}

/// Identity loading priority: the startup flag `--node-key` (rendered by the
/// deploy chain CONFIGURE step, assigned authoritatively by the console) >
/// identity.json (stable across restarts) > a self-generated sn-&lt;16hex&gt;
/// (the self-registration fallback). Loaded = persisted.
pub fn load_or_create_identity(
    state_dir: &std::path::Path,
    flag_key: Option<&str>,
    name: &str,
) -> anyhow::Result<NodeIdentity> {
    std::fs::create_dir_all(state_dir)?;
    let file = state_dir.join("identity.json");
    if let Ok(text) = std::fs::read_to_string(&file) {
        if let Ok(id) = serde_json::from_str::<NodeIdentity>(&text) {
            if is_valid_key(&id.node_key) {
                if let Some(k) = flag_key {
                    // the flag (the in-line key reused on redeploy) wins — a
                    // stale local cache gets corrected (the row is the anchor
                    // of identity)
                    if k != id.node_key {
                        let corrected = NodeIdentity {
                            node_key: k.to_string(),
                            name: id.name,
                        };
                        std::fs::write(&file, serde_json::to_string_pretty(&corrected)?)?;
                        return Ok(corrected);
                    }
                }
                return Ok(id);
            }
        }
    }
    let id = NodeIdentity {
        node_key: flag_key
            .map(str::to_string)
            .unwrap_or_else(|| format!("sn-{}", random_hex16())),
        name: name.to_string(),
    };
    std::fs::write(&file, serde_json::to_string_pretty(&id)?)?;
    Ok(id)
}

fn is_valid_key(k: &str) -> bool {
    k.starts_with("sn-") && k.len() == 19 && k[3..].chars().all(|c| c.is_ascii_hexdigit())
}

/// Overwrite-style persistence (called after console-echo adoption).
pub fn persist_identity(state_dir: &std::path::Path, id: &NodeIdentity) -> anyhow::Result<()> {
    std::fs::create_dir_all(state_dir)?;
    std::fs::write(
        state_dir.join("identity.json"),
        serde_json::to_string_pretty(id)?,
    )?;
    Ok(())
}

/// 16 hex chars: concatenated fixed-value hashes of two RandomState instances
/// (std-only pseudo-random — enough for the self-registration fallback; the
/// deploy chain's main path gets its key assigned by the console).
fn random_hex16() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut out = String::with_capacity(16);
    for salt in [0u64, 1] {
        let mut h = RandomState::new().build_hasher();
        h.write(&salt.to_le_bytes());
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out.truncate(16);
    out
}

// ==================== per-node token (console register echo — upgrade plan §二 console H1 engine side) ====================

/// The m2m credential the node presents on register/heartbeat, and (via
/// [`crate::auth`]) also the /v1 credential. Sources: the register-response
/// echo (persisted here, authoritative once adopted) > the
/// `--bootstrap-token` flag (deploy-chain shared token, first-admission
/// only). The console stores only the sha256 of this token.
pub fn load_persisted_node_token(state_dir: &std::path::Path) -> Option<String> {
    crate::auth::read_token_file(&state_dir.join(crate::auth::NODE_TOKEN_FILE))
}

/// Persist the adopted per-node token (0600).
pub fn persist_node_token(state_dir: &std::path::Path, token: &str) -> anyhow::Result<()> {
    crate::auth::write_token_file(&state_dir.join(crate::auth::NODE_TOKEN_FILE), token)
}

/// Drop the adopted token (register 401 while presenting it — the console
/// revoked/rotated it away; fall back to the deploy-chain bootstrap token on
/// the next cycle).
pub fn drop_persisted_node_token(state_dir: &std::path::Path) {
    let _ = std::fs::remove_file(state_dir.join(crate::auth::NODE_TOKEN_FILE));
}

/// Extract the per-node token from a register response. The canonical field
/// is the top-level `bootstrapToken` (the console echoes the plaintext
/// exactly once); tolerant fallbacks keep older/alternative console shapes
/// consumable (§4.3 consumer-side field tolerance).
pub fn extract_register_token(body: &serde_json::Value) -> Option<String> {
    const PATHS: [&str; 4] = [
        "/bootstrapToken",
        "/nodeToken",
        "/node/token",
        "/node/bootstrapToken",
    ];
    for p in PATHS {
        if let Some(t) = body.pointer(p).and_then(|v| v.as_str()) {
            let t = t.trim();
            if !t.is_empty() && t.len() <= 256 {
                return Some(t.to_string());
            }
        }
    }
    None
}

// ==================== event ring (capacity 64, cleared on confirmation) ====================

/// Engine event (reported in heartbeat batches; cleared after confirmation
/// (HTTP 200)). `rename_all=camelCase` keeps the payload shape stable for
/// console-side parsing.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineEvent {
    pub ts: i64,
    pub level: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_id: Option<u64>,
    pub message: String,
}

#[derive(Default)]
pub struct EventRing {
    inner: Mutex<VecDeque<EngineEvent>>,
    /// count of events dropped because the ring was full (reported on the
    /// heartbeat — makes event loss observable)
    dropped: std::sync::atomic::AtomicU64,
}

impl EventRing {
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn push(&self, ev: EngineEvent) {
        let mut q = self.inner.lock().unwrap();
        if q.len() >= EVENT_RING_CAPACITY {
            q.pop_front();
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        q.push_back(ev);
    }

    pub fn emit(&self, level: &str, kind: &str, message: impl Into<String>) {
        self.push(EngineEvent {
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            level: level.to_string(),
            kind: kind.to_string(),
            pipeline_id: None,
            message: message.into(),
        });
    }

    /// Current batch (not cleared — confirmation semantics belong to
    /// confirm).
    pub fn take_batch(&self) -> Vec<EngineEvent> {
        self.inner.lock().unwrap().iter().cloned().collect()
    }

    /// Confirm the first n entries (cleared per the reported count after a
    /// 200 heartbeat).
    pub fn confirm(&self, n: usize) {
        let mut q = self.inner.lock().unwrap();
        let n = n.min(q.len());
        q.drain(..n);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ==================== heartbeat payload (pure functions — unit-test anchors) ====================

/// Capacity limit constants (for scheduling). Running counts derive live from
/// NodeState.
pub const CAPACITY_LIMITS: (usize, usize) = (8, 24); // (pipelines, statements)
pub const CAPACITY_MEMORY_MB: usize = 2048;

/// Capacity self-report (dynamic): running pipeline/statement counts come
/// from the live NodeState snapshot.
fn capacity_json(running_pipelines: usize, running_statements: usize) -> String {
    format!(
        r#"{{"pipelines":{},"statements":{},"memory_mb":{},"running_pipelines":{},"running_statements":{}}}"#,
        CAPACITY_LIMITS.0,
        CAPACITY_LIMITS.1,
        CAPACITY_MEMORY_MB,
        running_pipelines,
        running_statements
    )
}

#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub control_plane: String,
    pub advertise: String,
    pub name: String,
    pub state_dir: PathBuf,
    pub heartbeat_secs: u64,
    pub engine_version: String,
    pub flag_node_key: Option<String>,
    /// Bootstrap token written into node.conf at install time (apiUrl
    /// correction / re-registration on address change). Once a per-node
    /// token is adopted from the register echo, that token takes over for
    /// m2m and /v1 — the bootstrap token remains the first-admission
    /// credential and an auxiliary /v1 credential.
    pub bootstrap_token: Option<String>,
}

/// The m2m credential sent in the `bootstrapToken` payload field: the
/// adopted per-node token when present, else the deploy-chain bootstrap
/// token.
fn effective_token(node_token: &Option<String>, cfg: &HeartbeatConfig) -> Option<String> {
    node_token.clone().or_else(|| cfg.bootstrap_token.clone())
}

/// Heartbeat payload. pipelineStates carry totals {in,out,flushes} (monotonic
/// cumulatives — the console derives rates into trends); pipelines not
/// registered carry no totals key.
/// resources: node resource sampling deltas (None = non-Linux target / first
/// beat with no rate — key omitted).
#[allow(clippy::too_many_arguments)]
pub fn build_heartbeat_payload(
    id: &NodeIdentity,
    cfg: &HeartbeatConfig,
    snap: &crate::NodeState,
    events: &[EngineEvent],
    totals: &std::collections::HashMap<u64, (u64, u64, u64)>,
    resources: Option<serde_json::Value>,
    token: Option<&str>,
) -> serde_json::Value {
    let states: Vec<serde_json::Value> = snap
        .pipelines
        .iter()
        .map(|p| {
            let mut v = serde_json::json!({
                "id": p.id, "state": p.status, "statements": p.statements.len()
            });
            if let Some(&(cin, cout, cf)) = totals.get(&p.id) {
                v["totals"] = serde_json::json!({ "in": cin, "out": cout, "flushes": cf });
            }
            v
        })
        .collect();
    let mut payload = serde_json::json!({
        "nodeKey": id.node_key,
        "name": id.name,
        "apiUrl": format!("http://{}", cfg.advertise),
        "engineVersion": cfg.engine_version,
        "capacityJson": capacity_json(snap.pipelines.len(), snap.pipelines.iter().map(|p| p.statements.len()).sum()),
        "pipelineStates": states,
        "events": events,
    });
    if let Some(tok) = token {
        payload["bootstrapToken"] = serde_json::Value::String(tok.to_string());
    }
    if let Some(res) = resources {
        payload["resources"] = res;
    }
    payload
}

// ==================== node resource sampling (/proc direct reads — musl-static, zero external deps) ====================

/// One raw sample (Linux /proc): RSS/physical totals + process and system
/// CPU jiffies + NIC cumulative bytes. `peak_rss_bytes` is the kernel
/// high-water mark (VmHWM) — the true process-lifetime peak, unlike a
/// beat-interval sample max it cannot miss spikes between two beats.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResourceSample {
    pub rss_bytes: u64,
    pub peak_rss_bytes: u64,
    pub mem_total_bytes: u64,
    /// utime+stime from /proc/self/stat (jiffies)
    pub proc_jiffies: u64,
    /// sum of all columns of /proc/stat's first line (whole-machine jiffies)
    pub total_jiffies: u64,
    /// cumulative rx/tx bytes of non-lo NICs from /proc/net/dev
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

/// Linux reads /proc; non-Linux (dev-machine unit tests) is always None — the
/// payload omits the resources key, the console tolerates it.
#[cfg(target_os = "linux")]
pub fn read_proc_sample() -> Option<ResourceSample> {
    Some(ResourceSample {
        rss_bytes: proc_status_kb("VmRSS")? * 1024,
        peak_rss_bytes: proc_status_kb("VmHWM")? * 1024,
        mem_total_bytes: proc_status_kb("MemTotal")? * 1024,
        proc_jiffies: proc_self_stat_jiffies()?,
        total_jiffies: proc_stat_total_jiffies()?,
        rx_bytes: net_dev_sum(false)?,
        tx_bytes: net_dev_sum(true)?,
    })
}

#[cfg(not(target_os = "linux"))]
pub fn read_proc_sample() -> Option<ResourceSample> {
    None
}

#[cfg(target_os = "linux")]
fn proc_status_kb(key: &str) -> Option<u64> {
    let txt = std::fs::read_to_string(if key == "MemTotal" {
        "/proc/meminfo"
    } else {
        "/proc/self/status"
    })
    .ok()?;
    for line in txt.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            return rest
                .trim_start_matches(':')
                .split_whitespace()
                .next()?
                .parse()
                .ok();
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn proc_self_stat_jiffies() -> Option<u64> {
    // /proc/self/stat fields 14/15 = utime/stime; comm may contain spaces —
    // split after the last ')'
    let txt = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after = txt.rsplit_once(')')?.1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?; // the 12th column after ')' = stat field 14
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

#[cfg(target_os = "linux")]
fn proc_stat_total_jiffies() -> Option<u64> {
    let txt = std::fs::read_to_string("/proc/stat").ok()?;
    let first = txt.lines().next()?;
    let total: u64 = first
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse::<u64>().ok())
        .sum();
    Some(total)
}

#[cfg(target_os = "linux")]
fn net_dev_sum(tx: bool) -> Option<u64> {
    let txt = std::fs::read_to_string("/proc/net/dev").ok()?;
    let mut sum = 0u64;
    for line in txt.lines().skip(2) {
        let (iface, counters) = line.split_once(':')?;
        let name = iface.trim();
        if name == "lo" {
            continue;
        }
        let fields: Vec<&str> = counters.split_whitespace().collect();
        // column order: rx_bytes rx_packets ... tx_bytes (9th column, index 8)
        let v: u64 = if tx { fields.get(8)? } else { fields.first()? }
            .parse()
            .ok()?;
        sum += v;
    }
    Some(sum)
}

/// Delta between two adjacent beats (a pure function, unit-testable on any
/// platform): rates = B/s; CPU% = the process jiffies delta's share of the
/// whole-machine delta (machine-relative, clamped 0..100); the first beat
/// (no prev) omits the rate keys and carries only the RSS/peak-RSS/total
/// instantaneous values.
pub fn resource_delta_json(
    prev: Option<(ResourceSample, f64)>,
    cur: ResourceSample,
    elapsed_secs: f64,
) -> serde_json::Value {
    let mut v = serde_json::json!({
        "rssBytes": cur.rss_bytes,
        "peakRssBytes": cur.peak_rss_bytes,
        "memTotalBytes": cur.mem_total_bytes,
    });
    if let Some((p, _)) = prev {
        if elapsed_secs > 0.0 {
            let d_proc = cur.proc_jiffies.saturating_sub(p.proc_jiffies);
            let d_total = cur.total_jiffies.saturating_sub(p.total_jiffies);
            // machine-relative share needs no CLK_TCK conversion (numerator
            // and denominator are both jiffies)
            let cpu_pct = if d_total > 0 {
                (d_proc as f64 / d_total as f64) * 100.0
            } else {
                0.0
            };
            let rx = cur.rx_bytes.saturating_sub(p.rx_bytes) as f64 / elapsed_secs;
            let tx = cur.tx_bytes.saturating_sub(p.tx_bytes) as f64 / elapsed_secs;
            v["cpuPercent"] = serde_json::json!(cpu_pct.clamp(0.0, 100.0));
            v["rxBps"] = serde_json::json!(rx.round() as u64);
            v["txBps"] = serde_json::json!(tx.round() as u64);
        }
    }
    v
}

/// Per-beat delta sampler (one per node process).
#[derive(Default)]
pub struct ResourceSampler {
    prev: Option<(ResourceSample, std::time::Instant)>,
}

impl ResourceSampler {
    pub fn new() -> Self {
        Self { prev: None }
    }

    /// Sample one beat and compute the delta; unavailable (non-Linux / read
    /// failure) returns None.
    pub fn tick(&mut self) -> Option<serde_json::Value> {
        let cur = read_proc_sample()?;
        let now = std::time::Instant::now();
        let prev = self.prev.replace((cur, now));
        let elapsed = prev
            .as_ref()
            .map(|(_, at)| at.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        Some(resource_delta_json(
            prev.map(|(s, _)| (s, 0.0)),
            cur,
            elapsed,
        ))
    }
}

/// Registration payload (the self-registration channel; idempotent by
/// node_key — an existing key updates address/capacity). `token` is the
/// adopted per-node token (a known node must re-present it) or the global
/// bootstrap token (first admission).
pub fn build_register_payload(
    id: &NodeIdentity,
    cfg: &HeartbeatConfig,
    token: Option<&str>,
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "nodeKey": id.node_key,
        "apiUrl": format!("http://{}", cfg.advertise),
        "capacityJson": capacity_json(0, 0),
    });
    if let Some(tok) = token {
        payload["bootstrapToken"] = serde_json::Value::String(tok.to_string());
    }
    payload
}

/// Identity echo: a response node.nodeKey differing from the local one →
/// adoption (the console is authoritative).
pub fn apply_echo(id: &NodeIdentity, body: &serde_json::Value) -> Option<NodeIdentity> {
    let echoed = body.pointer("/node/nodeKey").and_then(|v| v.as_str())?;
    if echoed != id.node_key && is_valid_key(echoed) {
        Some(NodeIdentity {
            node_key: echoed.to_string(),
            name: id.name.clone(),
        })
    } else {
        None
    }
}

// ==================== heartbeat thread ====================

pub struct HeartbeatWorker {
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    finished: Arc<(Mutex<bool>, Condvar)>,
    pub events: Arc<EventRing>,
}

impl HeartbeatWorker {
    /// Start the heartbeat thread (returns the control handle; the thread
    /// runs the register → heartbeat loop and the graceful-shutdown final
    /// beat). With a registry present, pipelineStates defer to it (the same
    /// source of truth as GET /v1/pipelines). `api_token` (when given) is
    /// rotated live whenever the console issues a fresh per-node token.
    pub fn start(
        cfg: HeartbeatConfig,
        state: StateHandle,
        registry: Option<Arc<tailrace_runtime::registry::PipelineRegistry>>,
        api_token: Option<crate::auth::NodeApiToken>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let wake: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let finished: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let events = Arc::new(EventRing::default());
        let worker = Self {
            stop: stop.clone(),
            wake: wake.clone(),
            finished: finished.clone(),
            events: events.clone(),
        };
        std::thread::Builder::new()
            .name("engine-heartbeat".into())
            .spawn(move || {
                run_loop(
                    cfg, state, registry, api_token, stop, wake, finished, events,
                );
            })
            .expect("spawn engine-heartbeat");
        worker
    }

    /// Request shutdown (called by the signal thread): set stop + wake → the
    /// thread sends the final heartbeat (NODE_STOP) then exits.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let mut w = self.wake.0.lock().unwrap();
        *w = true;
        self.wake.1.notify_all();
    }

    /// Wait for the thread to finish (with a timeout guard — the final beat's
    /// HTTP is ≤10s, 30s is plenty).
    pub fn join_timeout(&self, max: Duration) {
        let (lock, cv) = &*self.finished;
        let done = lock.lock().unwrap();
        if *done {
            return;
        }
        let _ = cv.wait_timeout(done, max);
    }

    /// Whether shutdown completed (the main thread polls this while serving —
    /// join_timeout must not be used as the resident wait: exiting right
    /// after its timeout would make the engine "self-terminate N seconds
    /// after startup").
    pub fn is_finished(&self) -> bool {
        *self.finished.0.lock().unwrap()
    }
}

fn intern_status(s: &str) -> &'static str {
    match s {
        "PAUSED" => "PAUSED",
        "STOPPED" => "STOPPED",
        "FAILED" => "FAILED",
        _ => "RUNNING",
    }
}

fn snapshot_for_hb(
    state: &StateHandle,
    registry: Option<&Arc<tailrace_runtime::registry::PipelineRegistry>>,
) -> crate::NodeState {
    let Some(reg) = registry else {
        return state.snapshot();
    };
    let mut ns = crate::NodeState {
        version: env!("CARGO_PKG_VERSION").to_string(),
        pipelines: Vec::new(),
    };
    for (id, n, st, _) in reg.snapshot() {
        ns.pipelines.push(crate::PipelineSnapshot {
            id,
            statements: vec!["stmt".into(); n],
            status: intern_status(&st),
        });
    }
    ns
}

#[allow(clippy::too_many_arguments)]
fn run_loop(
    cfg: HeartbeatConfig,
    state: StateHandle,
    registry: Option<Arc<tailrace_runtime::registry::PipelineRegistry>>,
    api_token: Option<crate::auth::NodeApiToken>,
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    finished: Arc<(Mutex<bool>, Condvar)>,
    events: Arc<EventRing>,
) {
    let client = match reqwest::blocking::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "heartbeat client build failed");
            mark_finished(&finished);
            return;
        }
    };
    let base = cfg.control_plane.trim_end_matches('/');
    let mut identity =
        match load_or_create_identity(&cfg.state_dir, cfg.flag_node_key.as_deref(), &cfg.name) {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "heartbeat identity load failed");
                mark_finished(&finished);
                return;
            }
        };
    // console-H1 engine side: the adopted per-node token wins over the
    // deploy-chain bootstrap token once it exists
    let mut node_token = load_persisted_node_token(&cfg.state_dir);
    if node_token.is_some() {
        tracing::info!(
            path = %cfg.state_dir.join(crate::auth::NODE_TOKEN_FILE).display(),
            "per-node token loaded"
        );
    }
    tracing::info!(
        node_key = %identity.node_key,
        name = %identity.name,
        control_plane = base,
        "identity ready"
    );
    events.emit(
        "INFO",
        "NODE_START",
        format!("serve started advertise={}", cfg.advertise),
    );

    let mut registered = false;
    let mut res_sampler = ResourceSampler::new();
    loop {
        // heartbeat (404/unregistered → register first, then heartbeat
        // immediately — registration is idempotent by node_key)
        let snap = snapshot_for_hb(&state, registry.as_ref());
        let totals = registry
            .as_ref()
            .map(|r| r.heartbeat_totals())
            .unwrap_or_default();
        let batch = events.take_batch();
        let payload = build_heartbeat_payload(
            &identity,
            &cfg,
            &snap,
            &batch,
            &totals,
            res_sampler.tick(),
            effective_token(&node_token, &cfg).as_deref(),
        );
        match post_json(&client, &format!("{base}/api/nodes/heartbeat"), &payload) {
            Ok((200, body)) => {
                registered = true;
                events.confirm(batch.len());
                note_beat(&identity.node_key); // A6: heartbeat-age basis
                if let Some(adopted) = apply_echo(&identity, &body) {
                    tracing::info!(
                        from = %identity.node_key,
                        to = %adopted.node_key,
                        "identity echo adoption"
                    );
                    let _ = persist_identity(&cfg.state_dir, &adopted);
                    identity = adopted;
                }
            }
            Ok((status, _)) if status == 404 || status == 401 || !registered => {
                let reg = build_register_payload(
                    &identity,
                    &cfg,
                    effective_token(&node_token, &cfg).as_deref(),
                );
                match post_json(&client, &format!("{base}/api/nodes/register"), &reg) {
                    Ok((200, body)) => {
                        registered = true;
                        note_beat(&identity.node_key); // A6: register counts as console contact
                                                       // per-node token echo: adopt (persist + rotate the
                                                       // live /v1 token) whenever it differs — the console
                                                       // mints a fresh one on every successful register
                        if let Some(t) = extract_register_token(&body) {
                            if Some(&t) != node_token.as_ref() {
                                match persist_node_token(&cfg.state_dir, &t) {
                                    Ok(()) => {
                                        tracing::info!(
                                            "adopted console-issued per-node token (persisted, m2m + /v1 rotated)"
                                        );
                                        events.emit(
                                            "INFO",
                                            "NODE_TOKEN_ADOPTED",
                                            "console-issued per-node token adopted (register echo)",
                                        );
                                        if let Some(h) = api_token.as_ref() {
                                            h.rotate(&t);
                                        }
                                        node_token = Some(t);
                                    }
                                    Err(e) => tracing::warn!(
                                        error = %format!("{e:#}"),
                                        "per-node token persist failed (will retry adoption on next register)"
                                    ),
                                }
                            }
                        }
                        tracing::info!(node_key = %identity.node_key, "self-registration ok");
                    }
                    Ok((s, b)) => {
                        // presenting an adopted token that the console no
                        // longer accepts (revoked/rotated away): drop it and
                        // fall back to the bootstrap token next cycle
                        if s == 401 && node_token.take().is_some() {
                            drop_persisted_node_token(&cfg.state_dir);
                            tracing::warn!(
                                "register 401 with the adopted per-node token — dropped it, falling back to the bootstrap token"
                            );
                        }
                        tracing::warn!(status = s, body = %truncate(&b), "registration failed");
                    }
                    Err(e) => tracing::warn!(error = %e, "registration unreachable"),
                }
            }
            Ok((status, body)) => {
                tracing::warn!(status, body = %truncate(&body), "heartbeat abnormal");
            }
            Err(e) => {
                // network down / console restarting: retry silently (events
                // stay pending confirmation and catch up on recovery)
                tracing::warn!(
                    retry_secs = cfg.heartbeat_secs,
                    error = %e,
                    "console unreachable, retrying"
                );
            }
        }

        // interruptible sleep: a shutdown signal wakes immediately
        {
            let mut w = wake.0.lock().unwrap();
            let deadline =
                std::time::Instant::now() + Duration::from_secs(cfg.heartbeat_secs.max(1));
            while !*w && !stop.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                let (guard, _) = wake
                    .1
                    .wait_timeout(w, deadline - std::time::Instant::now())
                    .unwrap();
                w = guard;
            }
            *w = false;
        }
        if stop.load(Ordering::SeqCst) {
            // graceful-shutdown final beat: NODE_STOP event + one heartbeat
            // (the result does not block exit)
            events.emit(
                "INFO",
                "NODE_STOP",
                "serve graceful shutdown (SIGTERM/SIGINT)",
            );
            let snap = snapshot_for_hb(&state, registry.as_ref());
            let totals = registry
                .as_ref()
                .map(|r| r.heartbeat_totals())
                .unwrap_or_default();
            let batch = events.take_batch();
            // the resource sampler's scope ended with the loop — no resources
            // on the final beat (meaningless after shutdown)
            let payload = build_heartbeat_payload(
                &identity,
                &cfg,
                &snap,
                &batch,
                &totals,
                None,
                effective_token(&node_token, &cfg).as_deref(),
            );
            if let Ok((200, body)) =
                post_json(&client, &format!("{base}/api/nodes/heartbeat"), &payload)
            {
                events.confirm(batch.len());
                note_beat(&identity.node_key); // A6: final-beat contact
                if let Some(adopted) = apply_echo(&identity, &body) {
                    let _ = persist_identity(&cfg.state_dir, &adopted);
                }
            }
            tracing::info!("heartbeat shut down");
            break;
        }
    }
    mark_finished(&finished);
}

fn post_json(
    client: &reqwest::blocking::Client,
    url: &str,
    payload: &serde_json::Value,
) -> Result<(u16, serde_json::Value), reqwest::Error> {
    let resp = client.post(url).json(payload).send()?;
    let status = resp.status().as_u16();
    let body = resp
        .json::<serde_json::Value>()
        .unwrap_or(serde_json::Value::Null);
    Ok((status, body))
}

fn truncate(v: &serde_json::Value) -> String {
    let s = v.to_string();
    if s.len() > 200 {
        format!("{}…", &s[..200])
    } else {
        s
    }
}

fn mark_finished(f: &Arc<(Mutex<bool>, Condvar)>) {
    let mut done = f.0.lock().unwrap();
    *done = true;
    f.1.notify_all();
}

// ==================== unit tests (pure-function surface; the full HTTP chain = deploy drills) ====================

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("tailrace-hb-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn identity_generate_then_stable_across_restart() {
        let dir = tmp_dir("gen");
        let id = load_or_create_identity(&dir, None, "n1").unwrap();
        assert_eq!(id.node_key.len(), 19, "sn-<16hex>");
        assert!(id.node_key.starts_with("sn-"));
        // stable across restarts (identity.json wins)
        let again = load_or_create_identity(&dir, None, "n1").unwrap();
        assert_eq!(id, again);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_flag_key_overrides_stale_cache() {
        let dir = tmp_dir("flag");
        let _ = load_or_create_identity(&dir, None, "n1").unwrap();
        // redeploy scenario: the flag key (in-line authority) ≠ the local
        // cache → corrected and persisted
        let adopted = load_or_create_identity(&dir, Some("sn-aaaaaaaaaaaaaaaa"), "n1").unwrap();
        assert_eq!(adopted.node_key, "sn-aaaaaaaaaaaaaaaa");
        let reread = load_or_create_identity(&dir, None, "n1").unwrap();
        assert_eq!(reread.node_key, "sn-aaaaaaaaaaaaaaaa");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn event_ring_capacity_and_confirm() {
        let ring = EventRing::default();
        for i in 0..70 {
            ring.emit("INFO", "T", format!("e{i}"));
        }
        assert_eq!(
            ring.len(),
            EVENT_RING_CAPACITY,
            "at 64 the oldest is dropped"
        );
        let batch = ring.take_batch();
        assert_eq!(
            batch.first().unwrap().message,
            "e6",
            "the oldest 6 were squeezed out"
        );
        ring.confirm(10);
        assert_eq!(
            ring.len(),
            EVENT_RING_CAPACITY - 10,
            "confirmation clears exactly the first n"
        );
        ring.confirm(usize::MAX);
        assert!(ring.is_empty());
    }

    #[test]
    fn heartbeat_payload_shape() {
        let id = NodeIdentity {
            node_key: "sn-0123456789abcdef".into(),
            name: "n1".into(),
        };
        let cfg = HeartbeatConfig {
            control_plane: "http://127.0.0.1:8080".into(),
            advertise: "10.1.1.5:18100".into(),
            name: "n1".into(),
            state_dir: PathBuf::from("/tmp/s"),
            heartbeat_secs: 30,
            engine_version: "0.3.0".into(),
            flag_node_key: None,
            bootstrap_token: Some("tok-secret".into()),
        };
        let mut snap = crate::NodeState::default();
        snap.pipelines.push(crate::PipelineSnapshot {
            id: 42,
            statements: vec!["s1".into(), "s2".into()],
            status: "RUNNING",
        });
        let ev = EngineEvent {
            ts: 1,
            level: "ERROR".into(),
            kind: "SINK_WRITE_FAILED".into(),
            pipeline_id: Some(42),
            message: "x".into(),
        };
        let p = build_heartbeat_payload(
            &id,
            &cfg,
            &snap,
            std::slice::from_ref(&ev),
            &Default::default(),
            None,
            Some("tok-secret"),
        );
        assert_eq!(p["nodeKey"], "sn-0123456789abcdef");
        assert_eq!(p["name"], "n1");
        assert_eq!(p["apiUrl"], "http://10.1.1.5:18100");
        assert_eq!(p["engineVersion"], "0.3.0");
        assert_eq!(p["pipelineStates"][0]["id"], 42);
        assert_eq!(p["pipelineStates"][0]["statements"], 2);
        assert_eq!(p["events"][0]["kind"], "SINK_WRITE_FAILED");
        assert_eq!(
            p["events"][0]["pipelineId"], 42,
            "event pipelineId is camelCase"
        );
        assert!(p["events"][0].get("pipeline_id").is_none());
        assert_eq!(p["bootstrapToken"], "tok-secret");
        // resources=None (non-Linux / not sampled) — key omitted, the console
        // tolerates the default
        assert!(p.get("resources").is_none());
        // with a resource sample: the key mounts as-is
        let p2 = build_heartbeat_payload(
            &id,
            &cfg,
            &snap,
            &[],
            &Default::default(),
            Some(serde_json::json!({"rssBytes": 1, "cpuPercent": 3.5})),
            None,
        );
        assert_eq!(p2["resources"]["cpuPercent"], 3.5);
        assert!(
            p2.get("bootstrapToken").is_none(),
            "no token material = key omitted"
        );
    }

    #[test]
    fn resource_delta_first_tick_has_no_rates() {
        let cur = ResourceSample {
            rss_bytes: 200 * 1024 * 1024,
            peak_rss_bytes: 260 * 1024 * 1024,
            mem_total_bytes: 8 * 1024 * 1024 * 1024,
            proc_jiffies: 5,
            total_jiffies: 100,
            rx_bytes: 1,
            tx_bytes: 1,
        };
        let v = resource_delta_json(None, cur, 0.0);
        assert_eq!(v["rssBytes"], 200 * 1024 * 1024);
        // T20: kernel high-water mark rides along from the very first beat
        // (it is an instantaneous read, not a delta)
        assert_eq!(v["peakRssBytes"], 260 * 1024 * 1024);
        assert_eq!(v["memTotalBytes"], 8_u64 * 1024 * 1024 * 1024);
        assert!(
            v.get("cpuPercent").is_none(),
            "first beat has no delta baseline — rate keys omitted"
        );
        assert!(v.get("rxBps").is_none());
    }

    #[test]
    fn resource_delta_computes_rates_and_cpu_share() {
        let prev = ResourceSample {
            rss_bytes: 100,
            peak_rss_bytes: 100,
            mem_total_bytes: 1000,
            proc_jiffies: 10,
            total_jiffies: 1000,
            rx_bytes: 5000,
            tx_bytes: 7000,
        };
        let cur = ResourceSample {
            rss_bytes: 150,
            peak_rss_bytes: 180,
            mem_total_bytes: 1000,
            proc_jiffies: 60,
            total_jiffies: 2000,
            rx_bytes: 8000,
            tx_bytes: 14000,
        };
        let v = resource_delta_json(Some((prev, 0.0)), cur, 30.0);
        // CPU: process jiffies delta 50 / machine delta 1000 = 5%
        assert_eq!(v["cpuPercent"], 5.0);
        // network: rx delta 3000B / 30s = 100 B/s; tx 7000/30 = 233.33 → 233
        assert_eq!(v["rxBps"], 100);
        assert_eq!(v["txBps"], 233);
        assert_eq!(v["rssBytes"], 150);
        // peak is the current sample's HWM (monotonic by kernel contract, ≥ rss)
        assert_eq!(v["peakRssBytes"], 180);
    }

    #[test]
    fn register_payload_shape() {
        let id = NodeIdentity {
            node_key: "sn-0123456789abcdef".into(),
            name: "n1".into(),
        };
        let cfg = HeartbeatConfig {
            control_plane: "http://127.0.0.1:8080".into(),
            advertise: "10.1.1.5:18100".into(),
            name: "n1".into(),
            state_dir: PathBuf::from("/tmp/s"),
            heartbeat_secs: 30,
            engine_version: "0.3.0".into(),
            flag_node_key: None,
            bootstrap_token: Some("tok-secret".into()),
        };
        // the registration payload carries no name (the current contract's
        // three fields; extensions land later)
        let r = build_register_payload(&id, &cfg, Some("tok-secret"));
        assert_eq!(r["nodeKey"], "sn-0123456789abcdef");
        assert!(r.get("name").is_none());
        assert_eq!(r["bootstrapToken"], "tok-secret");
        // the adopted per-node token wins over the bootstrap flag
        let with_adopted = build_register_payload(&id, &cfg, Some("per-node-tok"));
        assert_eq!(with_adopted["bootstrapToken"], "per-node-tok");
        assert!(build_register_payload(&id, &cfg, None)
            .get("bootstrapToken")
            .is_none());
    }

    /// console-H1 engine side: register-echo token extraction (canonical
    // top-level bootstrapToken + tolerant fallbacks) and persistence.
    #[test]
    fn register_token_extract_and_persist() {
        let canonical = serde_json::json!({
            "node": {"id": 1, "nodeKey": "sn-0123456789abcdef", "name": "n"},
            "bootstrapToken": "tr_b_fresh"
        });
        assert_eq!(
            extract_register_token(&canonical).as_deref(),
            Some("tr_b_fresh")
        );
        let legacy = serde_json::json!({"node": {"nodeKey": "sn-0123456789abcdef", "token": "t1"}});
        assert_eq!(extract_register_token(&legacy).as_deref(), Some("t1"));
        // absent / empty / oversized echoes are not adopted
        assert_eq!(
            extract_register_token(&serde_json::json!({"node": {}})),
            None
        );
        assert_eq!(
            extract_register_token(&serde_json::json!({"bootstrapToken": "  "})),
            None
        );
        assert_eq!(
            extract_register_token(&serde_json::json!({"bootstrapToken": "x".repeat(300)})),
            None
        );
        // persistence round-trip (0600 on unix)
        let dir = tmp_dir("node-token");
        assert!(load_persisted_node_token(&dir).is_none());
        persist_node_token(&dir, "tr_b_adopted").unwrap();
        assert_eq!(
            load_persisted_node_token(&dir).as_deref(),
            Some("tr_b_adopted")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join(crate::auth::NODE_TOKEN_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        drop_persisted_node_token(&dir);
        assert!(load_persisted_node_token(&dir).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn echo_adoption_only_on_valid_differing_key() {
        let id = NodeIdentity {
            node_key: "sn-0123456789abcdef".into(),
            name: "n".into(),
        };
        let echo_other =
            serde_json::json!({"ok": true, "node": {"nodeKey": "sn-ffffffffffffffff"}});
        assert_eq!(
            apply_echo(&id, &echo_other).unwrap().node_key,
            "sn-ffffffffffffffff"
        );
        let echo_same = serde_json::json!({"ok": true, "node": {"nodeKey": "sn-0123456789abcdef"}});
        assert!(
            apply_echo(&id, &echo_same).is_none(),
            "identical = no churn"
        );
        let echo_invalid = serde_json::json!({"ok": true, "node": {"nodeKey": "bogus"}});
        assert!(
            apply_echo(&id, &echo_invalid).is_none(),
            "malformed shapes are not adopted"
        );
    }
}
