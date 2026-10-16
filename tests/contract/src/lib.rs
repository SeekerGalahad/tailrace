//! Shared fixtures for the in-process contract tests (upgrade plan §二
//! engine H3①②). FileSource/FileSink paths are cwd-relative by design (the
//! engine's deployment topology runs the node with the data dir as cwd), so
//! each cwd-sensitive test section holds a process-global guard and chdirs
//! into its own work dir.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

/// The /v1 node token handed to the in-process server.
pub const TEST_TOKEN: &str = "tr_b_contract_test_token";

static CWD_LOCK: Mutex<()> = Mutex::new(());

/// Serialize cwd-sensitive test sections (see module docs).
pub struct CwdGuard {
    prev: PathBuf,
    _lock: MutexGuard<'static, ()>,
}

impl CwdGuard {
    pub fn enter(dir: &Path) -> Self {
        let lock = CWD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::current_dir().expect("current_dir");
        std::env::set_current_dir(dir).unwrap_or_else(|e| {
            panic!("chdir into {}: {e}", dir.display());
        });
        Self { prev, _lock: lock }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.prev);
    }
}

/// Unique temp work dir per (tag, pid, nanos).
pub fn tmp_workdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tailrace-contract-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).expect("create workdir");
    d
}

/// Grab a free loopback port (bind :0, read it back, release).
pub fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// Spawn the in-process engine node: API server (with the test node token)
/// over a temp state dir. Returns (base_url, state_dir). The server thread
/// is a test-process leak by design (axum serves until process exit).
pub fn spawn_node(tag: &str) -> (String, PathBuf) {
    let port = free_port();
    let state_dir = tmp_workdir(tag);
    let registry = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
        &state_dir,
    ));
    let state = tailrace_api::StateHandle::new();
    let token = tailrace_api::auth::NodeApiToken::new(TEST_TOKEN);
    tailrace_api::start_server(&format!("127.0.0.1:{port}"), state, registry, token)
        .expect("start in-process api server");
    (format!("127.0.0.1:{port}"), state_dir)
}

/// Blocking client with a bounded timeout (the API is loopback-fast).
pub fn http() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("test http client")
}

/// Poll `probe` every 100ms until it returns Some, or `timeout` elapses.
pub fn poll_until<T>(timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return Some(v);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A file table object for the plan body.
pub fn file_table(reference: &str, columns: &[(&str, &str)]) -> serde_json::Value {
    serde_json::json!({
        "reference": reference,
        "kind": "file",
        "format": "jsonl",
        "columns": columns
            .iter()
            .map(|(n, t)| vec![n.to_string(), t.to_string()])
            .collect::<Vec<_>>(),
    })
}

/// Append jsonl lines to `file` (cwd-relative — hold the CwdGuard).
pub fn append_jsonl(file: &str, rows: &[serde_json::Value]) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file)
        .expect("open append");
    for r in rows {
        writeln!(f, "{r}").expect("write line");
    }
}

/// Read a cwd-relative file's line count.
pub fn line_count(file: &str) -> usize {
    std::fs::read_to_string(file)
        .map(|t| t.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0)
}

/// File→file plan body (SQL-first canon path).
pub fn file_plan(pipeline_id: u64, dispatch_id: u64) -> serde_json::Value {
    serde_json::json!({
        "pipelineId": pipeline_id,
        "sqlContent": "INSERT INTO ${ds:1.local.orders_out} SELECT order_no, qty FROM ${ds:1.local.orders_in}",
        "tables": [
            file_table("1.local.orders_in", &[("order_no", "utf8"), ("qty", "int64")]),
            file_table("1.local.orders_out", &[("order_no", "utf8"), ("qty", "int64")]),
        ],
        "checkpointMs": 1000,
        "dispatchId": dispatch_id,
    })
}
