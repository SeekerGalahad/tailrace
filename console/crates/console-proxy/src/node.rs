//! Typed console → engine-node /v1 calls (§4.6 division matrix: the engine
//! implements plan push / actions / stats / logs / offsets; the console only
//! aggregates and proxies — one origin for the UI).

use serde_json::Value;

/// A proxy call failure, mapped by handlers to §4.0's 502 semantics.
#[derive(Debug)]
pub enum ProxyError {
    /// TCP/HTTP transport failure — node down or unreachable.
    Unreachable(String),
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyError::Unreachable(e) => write!(f, "引擎节点不可达: {e}"),
        }
    }
}

/// One engine node's /v1 surface, addressed by its advertise URL. `token`
/// rides as `Authorization: Bearer …` — the engine's /v1 requires the shared
/// node token (upgrade plan §二 engine H1: the console proxy carries it).
#[derive(Debug, Clone)]
pub struct NodeEndpoint {
    pub base_url: String,
    pub token: Option<String>,
}

impl NodeEndpoint {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: None,
        }
    }

    /// With the bearer token the engine's /v1 middleware expects.
    pub fn with_token(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: Some(token.into()),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Liveness probe (§4.3 manual registration gate): true iff the node
    /// answers /healthz with 200.
    pub async fn healthz_ok(&self, client: &reqwest::Client) -> Result<bool, ProxyError> {
        let resp = client
            .get(self.url("/healthz"))
            .send()
            .await
            .map_err(|e| ProxyError::Unreachable(e.to_string()))?;
        Ok(resp.status().is_success())
    }

    /// Plan push (§4.5 deploy): PUT /v1/pipelines/{id}/plan. Returns the
    /// engine's JSON verdict ({"ok":bool,...}); `ok:false` is a *valid*
    /// response the caller turns into DEPLOY_FAILED + 502.
    pub async fn put_plan(
        &self,
        client: &reqwest::Client,
        pipeline_id: i64,
        body: &Value,
    ) -> Result<Value, ProxyError> {
        self.send_json(
            client,
            reqwest::Method::PUT,
            &format!("/v1/pipelines/{pipeline_id}/plan"),
            Some(body),
        )
        .await
    }

    /// Pipeline action (§4.5 pause/resume/stop): POST /v1/pipelines/{id}/{action}.
    pub async fn pipeline_action(
        &self,
        client: &reqwest::Client,
        pipeline_id: i64,
        action: &str,
    ) -> Result<Value, ProxyError> {
        self.send_json(
            client,
            reqwest::Method::POST,
            &format!("/v1/pipelines/{pipeline_id}/{action}"),
            None,
        )
        .await
    }

    /// GET a /v1 JSON resource verbatim (stats / offsets / pipelines list /
    /// logs pages — the §4.5 observation proxies pass the engine body
    /// through unchanged).
    pub async fn get_json(
        &self,
        client: &reqwest::Client,
        path_and_query: &str,
    ) -> Result<Value, ProxyError> {
        self.send_json(client, reqwest::Method::GET, path_and_query, None)
            .await
    }

    async fn send_json(
        &self,
        client: &reqwest::Client,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, ProxyError> {
        let mut req = client.request(method, self.url(path));
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ProxyError::Unreachable(e.to_string()))?;
        let status = resp.status();
        // The engine's own convention: even failures carry JSON bodies
        // ({"ok":false,...}); a non-JSON body means something else answered
        // on that port — surface as unreachable-style 502.
        let value: Value = resp
            .json()
            .await
            .map_err(|e| ProxyError::Unreachable(format!("引擎响应非法 (HTTP {status}): {e}")))?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_trims_trailing_slash() {
        let ep = NodeEndpoint::new("http://10.1.1.5:18100/");
        assert_eq!(
            ep.url("/v1/pipelines"),
            "http://10.1.1.5:18100/v1/pipelines"
        );
        assert!(ep.token.is_none());
    }

    #[test]
    fn with_token_keeps_url_and_token() {
        let ep = NodeEndpoint::with_token("http://10.1.1.5:18100/", "tr_b_x");
        assert_eq!(
            ep.url("/v1/pipelines"),
            "http://10.1.1.5:18100/v1/pipelines"
        );
        assert_eq!(ep.token.as_deref(), Some("tr_b_x"));
    }
}
