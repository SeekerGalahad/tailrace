//! Shared reqwest client factory for console → node loopback calls.
//!
//! Node advertise URLs are plain HTTP on the LAN/loopback (§3.2), so the
//! workspace reqwest (http2/json, no TLS stack) is sufficient. Timeouts keep
//! one dead node from pinning console workers.

use std::time::Duration;

/// Build the shared console→node HTTP client.
///
/// - connect timeout 5s — dead hosts fail fast;
/// - total timeout 30s — covers plan compile / stats pulls, leaves headroom
///   under the 30s heartbeat cadence;
/// - SSE log streaming must NOT use this client (no total timeout there);
///   it gets its own factory when the proxy surface lands.
pub fn node_http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
}

/// Client for long-lived node interactions (the SSE log bridge's 1.5s
/// polling loop): connect timeout only, no total timeout (§4.5 — the SSE
/// session outlives 30s by design, bounded by its own 300s session timer).
pub fn node_stream_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
}

#[cfg(test)]
mod tests {
    #[test]
    fn factory_builds_a_client() {
        let client = super::node_http_client().expect("client builds");
        let _ = client.get("http://127.0.0.1:1/healthz");
    }
}
