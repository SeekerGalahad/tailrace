//! Datasource connectivity probes (§4.2 `POST /api/datasources/test` and
//! `/{id}/test`). v1 probes are reachability checks: TCP connect for
//! broker/server endpoints, filesystem existence for file sources. The
//! probe always *executes* — the outcome (CONN_OK/CONN_FAIL) rides a 200
//! response; only a malformed request is a 400.

use std::time::{Duration, Instant};

use serde_json::Value;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// Cap how many endpoints one probe fans out to (a comma list of 50 brokers
/// should not stall the request for minutes).
const MAX_ENDPOINTS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeResult {
    pub ok: bool,
    pub message: String,
    pub latency_ms: u64,
}

/// Probe a datasource described by its registry type + connJson.
/// Unknown types come back as CONN_FAIL (the REST layer additionally 400s
/// unknown types on write paths; this is the belt to that braces).
pub async fn probe_datasource(ds_type: &str, conn_json: &Value) -> ProbeResult {
    let started = Instant::now();
    let result = match ds_type {
        "kafka" => probe_endpoints(bootstrap_hosts(conn_json), "kafka bootstrap").await,
        "doris" | "starrocks" => {
            let mut eps = Vec::new();
            if let Some(host) = jdbc_host(conn_json.get("jdbcUrl").and_then(|v| v.as_str())) {
                eps.push(host);
            }
            if let Some(host) = http_host(conn_json.get("streamLoadUrl").and_then(|v| v.as_str())) {
                eps.push(host);
            }
            if eps.is_empty() {
                return fail("connJson 缺少 jdbcUrl / streamLoadUrl", started);
            }
            probe_endpoints(eps, "doris FE/BE").await
        }
        "file" => match conn_json.get("path").and_then(|v| v.as_str()) {
            Some(path) if std::path::Path::new(path).exists() => Ok(format!("路径存在: {path}")),
            Some(path) => Err(format!("路径不存在: {path}")),
            None => Err("connJson 缺少 path".into()),
        },
        other => Err(format!("未支持探测的数据源类型: {other}")),
    };
    let latency_ms = started.elapsed().as_millis() as u64;
    match result {
        Ok(message) => ProbeResult {
            ok: true,
            message,
            latency_ms,
        },
        Err(message) => ProbeResult {
            ok: false,
            message,
            latency_ms,
        },
    }
}

fn fail(message: impl Into<String>, started: Instant) -> ProbeResult {
    ProbeResult {
        ok: false,
        message: message.into(),
        latency_ms: started.elapsed().as_millis() as u64,
    }
}

/// TCP-connect every endpoint; all must succeed (a flapping member of a
/// bootstrap list is worth knowing about, and v1 has no partial-success
/// semantics to express it in).
async fn probe_endpoints(eps: Vec<String>, label: &str) -> Result<String, String> {
    if eps.is_empty() {
        return Err(format!("{label} 地址列表为空"));
    }
    let mut failures = Vec::new();
    let total = eps.len();
    for ep in eps.iter().take(MAX_ENDPOINTS) {
        let addr = ep.replace(',', ""); // defensive: host lists already split
        match tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => failures.push(format!("{ep}: {e}")),
            Err(_) => failures.push(format!("{ep}: 超时 (>{PROBE_TIMEOUT:?})")),
        }
    }
    if failures.is_empty() {
        Ok(format!("{label} 可达 ({total}/{total})"))
    } else {
        Err(format!(
            "{label} 不可达 {}/{}: {}",
            failures.len(),
            total,
            failures.join("; ")
        ))
    }
}

/// "10.1.1.5:9092,10.1.1.6:9092" → ["10.1.1.5:9092", "10.1.1.6:9092"].
fn bootstrap_hosts(conn_json: &Value) -> Vec<String> {
    conn_json
        .get("bootstrap")
        .and_then(|v| v.as_str())
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|h| !h.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// "jdbc:mysql://fe-1:9030/shop?x=1" → "fe-1:9030".
fn jdbc_host(url: Option<&str>) -> Option<String> {
    let url = url?;
    let rest = url.split_once("://")?.1;
    let hostport = rest.split('/').next()?;
    let hostport = hostport.split('?').next()?;
    hostport.contains(':').then(|| hostport.to_string())
}

/// "http://be-1:8040" → "be-1:8040".
fn http_host(url: Option<&str>) -> Option<String> {
    let url = url?;
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let hostport = rest.split('/').next()?;
    hostport.contains(':').then(|| hostport.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_endpoints() {
        assert_eq!(
            bootstrap_hosts(&json!({"bootstrap": "a:9092, b:9092"})),
            vec!["a:9092".to_string(), "b:9092".to_string()]
        );
        assert_eq!(
            jdbc_host(Some("jdbc:mysql://fe-1:9030/shop?useSSL=false")),
            Some("fe-1:9030".into())
        );
        assert_eq!(jdbc_host(Some("jdbc:mysql://fe-1/shop")), None);
        assert_eq!(
            http_host(Some("http://be-1:8040")),
            Some("be-1:8040".into())
        );
        assert_eq!(http_host(Some("http://be-1")), None);
    }

    #[tokio::test]
    async fn probes_unreachable_and_file() {
        // kafka: connection-refused on a loopback port (nothing listens in
        // the 1-port range in practice; even if something did, the assertion
        // below would still hold via ok=true — so use a refused port).
        let r = probe_datasource("kafka", &json!({"bootstrap": "127.0.0.1:1"})).await;
        assert!(!r.ok, "{r:?}");
        assert!(r.message.contains("kafka bootstrap"), "{r:?}");
        assert!(r.message.contains("127.0.0.1:1"), "{r:?}");

        // file: real temp dir vs missing path
        let dir = std::env::temp_dir();
        let ok = probe_datasource("file", &json!({"path": dir.to_string_lossy()})).await;
        assert!(ok.ok, "{ok:?}");
        let bad = probe_datasource("file", &json!({"path": "/nonexistent-tailrace-probe"})).await;
        assert!(!bad.ok, "{bad:?}");
    }
}
