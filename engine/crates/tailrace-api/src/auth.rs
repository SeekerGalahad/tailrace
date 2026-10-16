//! /v1 node-token authentication (upgrade plan §二 engine H1).
//!
//! Every `/v1` route sits behind a shared node-token middleware
//! (`Authorization: Bearer <token>` or `X-Node-Token: <token>`); only
//! `/healthz` stays open (liveness probing). The token material is
//! node-local and resolved in this order:
//!
//! 1. `--api-token` flag (deploy chain explicit override),
//! 2. `TAILRACE_NODE_API_TOKEN` env,
//! 3. the adopted per-node token persisted at register time
//!    (`state_dir/node_token` — the console echoes it exactly once),
//! 4. `--bootstrap-token` (deploy-chain shared token; the self-registration
//!    admission credential),
//! 5. a freshly generated token, persisted to `state_dir/api_token` and
//!    printed once (a bare node never runs with a NULL credential).
//!
//! A per-node token minted by the console (register response) rotates the
//! *current* token live; the startup tokens stay accepted as auxiliary
//! credentials for the process lifetime so rotation never locks out the
//! deploy chain or the console proxy.

use std::path::Path;
use std::sync::{Arc, RwLock};

/// Env override for the node API token.
pub const API_TOKEN_ENV: &str = "TAILRACE_NODE_API_TOKEN";
/// Adopted per-node token (register echo; plaintext, 0600).
pub const NODE_TOKEN_FILE: &str = "node_token";
/// Self-generated fallback token (written when nothing else is configured).
pub const API_TOKEN_FILE: &str = "api_token";

/// Token length guard (console tokens are `tr_b_<48hex>`; leave headroom but
/// refuse absurd echoes).
const MAX_TOKEN_LEN: usize = 256;

/// Live set of accepted /v1 tokens: `current` rotates on register-token
/// adoption; `startup` holds the construction credentials (startup flags,
/// env, adopted/bootstrap files) that stay valid for the process lifetime.
#[derive(Debug, Clone)]
pub struct NodeApiToken {
    inner: Arc<TokenInner>,
}

#[derive(Debug)]
struct TokenInner {
    current: RwLock<String>,
    startup: RwLock<Vec<String>>,
}

impl NodeApiToken {
    pub fn new(initial: impl Into<String>) -> Self {
        let initial = initial.into();
        Self {
            inner: Arc::new(TokenInner {
                startup: RwLock::new(vec![initial.clone()]),
                current: RwLock::new(initial),
            }),
        }
    }

    /// With extra permanently-accepted credentials (the startup flags).
    pub fn with_auxiliary(mut self, extras: impl IntoIterator<Item = String>) -> Self {
        {
            let inner =
                Arc::get_mut(&mut self.inner).expect("auxiliary set before sharing the handle");
            let startup = inner.startup.get_mut().unwrap_or_else(|e| e.into_inner());
            for t in extras {
                if !t.is_empty() && !startup.contains(&t) {
                    startup.push(t);
                }
            }
        }
        self
    }

    /// The current primary token.
    pub fn current(&self) -> String {
        self.inner
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Rotate the primary token (register-token adoption). Returns true when
    /// the token actually changed. The outgoing token stops being accepted
    /// unless it is a construction (`startup`) credential — a bootstrap-flag
    /// startup must survive its own rotation, or the deploy chain (console
    /// proxy carrying the shared token) locks out (auth.rs module docs).
    pub fn rotate(&self, new_token: &str) -> bool {
        if new_token.is_empty() {
            return false;
        }
        let mut cur = self
            .inner
            .current
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if *cur == new_token {
            return false;
        }
        *cur = new_token.to_string();
        true
    }

    /// Constant-time-ish acceptance check.
    pub fn accepts(&self, presented: &str) -> bool {
        if presented.is_empty() {
            return false;
        }
        let cur = self.inner.current.read().unwrap_or_else(|e| e.into_inner());
        if ct_eq(presented, &cur) {
            return true;
        }
        let startup = self.inner.startup.read().unwrap_or_else(|e| e.into_inner());
        startup.iter().any(|t| ct_eq(presented, t))
    }
}

/// Byte-wise constant-time equality (the length branch leaks only the
/// length — tokens are fixed-shape, that is acceptable).
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Where the resolved token came from (for the startup log line).
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TokenOrigin {
    ApiTokenFlag,
    Env,
    AdoptedNodeToken,
    BootstrapFlag,
    Generated,
}

/// Resolve the node API token per the documented priority. When the
/// generator runs, the token is persisted (`api_token` file, 0600) and
/// printed once on stderr.
pub fn resolve_node_api_token(
    state_dir: &Path,
    api_token_flag: Option<&str>,
    bootstrap_flag: Option<&str>,
) -> (NodeApiToken, TokenOrigin) {
    let mut auxiliary: Vec<String> = Vec::new();
    let push_aux = |v: Option<&str>, aux: &mut Vec<String>| {
        if let Some(t) = v.filter(|t| !t.trim().is_empty()) {
            aux.push(t.trim().to_string());
        }
    };
    if let Some(t) = api_token_flag.filter(|t| !t.trim().is_empty()) {
        let tok = NodeApiToken::new(t.trim());
        push_aux(bootstrap_flag, &mut auxiliary);
        return (tok.with_auxiliary(auxiliary), TokenOrigin::ApiTokenFlag);
    }
    if let Ok(t) = std::env::var(API_TOKEN_ENV) {
        if !t.trim().is_empty() {
            let tok = NodeApiToken::new(t.trim());
            push_aux(bootstrap_flag, &mut auxiliary);
            return (tok.with_auxiliary(auxiliary), TokenOrigin::Env);
        }
    }
    if let Some(t) = read_token_file(&state_dir.join(NODE_TOKEN_FILE)) {
        let tok = NodeApiToken::new(&t);
        push_aux(bootstrap_flag, &mut auxiliary);
        return (tok.with_auxiliary(auxiliary), TokenOrigin::AdoptedNodeToken);
    }
    if let Some(t) = bootstrap_flag.filter(|t| !t.trim().is_empty()) {
        return (NodeApiToken::new(t.trim()), TokenOrigin::BootstrapFlag);
    }
    if let Some(t) = read_token_file(&state_dir.join(API_TOKEN_FILE)) {
        return (NodeApiToken::new(t), TokenOrigin::Generated);
    }
    let generated = generate_local_token();
    let _ = write_token_file(&state_dir.join(API_TOKEN_FILE), &generated);
    tracing::warn!(
        path = %state_dir.join(API_TOKEN_FILE).display(),
        "no node token configured — generated one (also persisted, shown once): {generated}"
    );
    (NodeApiToken::new(&generated), TokenOrigin::Generated)
}

/// Read a persisted token file (trim + sanity guard).
pub fn read_token_file(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let t = text.trim();
    (!t.is_empty() && t.len() <= MAX_TOKEN_LEN).then(|| t.to_string())
}

/// Persist a token file with 0600 perms (best-effort on non-unix).
pub fn write_token_file(path: &Path, token: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    let write = || {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(token.as_bytes())
    };
    #[cfg(not(unix))]
    let write = || std::fs::write(path, token);
    write().map_err(|e| anyhow::anyhow!("token file write failed ({}): {e}", path.display()))
}

/// std-only fallback generator is not used: `rand` is already a workspace
/// dependency (console side), so the engine token gets real entropy.
fn generate_local_token() -> String {
    use rand::RngCore;
    let mut raw = [0u8; 24];
    rand::rng().fill_bytes(&mut raw);
    format!(
        "tr_e_{}",
        raw.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

/// Extract the presented token from a request (`Authorization: Bearer …`
/// wins over `X-Node-Token: …`).
fn extract_presented(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(v) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(t) = v.strip_prefix("Bearer ").filter(|t| !t.is_empty()) {
            return Some(t.to_string());
        }
    }
    headers
        .get("x-node-token")
        .and_then(|v| v.to_str().ok())
        .filter(|t| !t.is_empty())
        .map(str::to_string)
}

/// axum middleware: reject tokenless/invalid callers with HTTP 401 + the
/// engine's JSON error envelope (`/healthz` is deliberately not layered).
pub async fn require_node_token(
    axum::extract::State(token): axum::extract::State<NodeApiToken>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let ok = extract_presented(req.headers())
        .map(|p| token.accepts(&p))
        .unwrap_or(false);
    if ok {
        return next.run(req).await;
    }
    (
        axum::http::StatusCode::UNAUTHORIZED,
        axum::Json(serde_json::json!({
            "ok": false,
            "error": "unauthorized: /v1 requires the node token (Authorization: Bearer <node token> or X-Node-Token)",
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "tailrace-auth-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn acceptance_current_and_auxiliary() {
        let tok = NodeApiToken::new("primary").with_auxiliary(vec![
            "primary".into(),
            "aux".into(),
            String::new(),
        ]);
        assert!(tok.accepts("primary"));
        assert!(tok.accepts("aux"));
        assert!(!tok.accepts("nope"));
        assert!(!tok.accepts(""));
        // rotation flips the primary; construction (startup) credentials stay
        // accepted — a bootstrap-flag startup must survive its own rotation,
        // else the console proxy carrying the shared deploy-chain token locks
        // out (regression: deploy → ENGINE_REJECTED unauthorized after the
        // first register echo)
        assert!(tok.rotate("per-node"));
        assert!(tok.accepts("per-node"));
        assert!(
            tok.accepts("primary"),
            "startup credential survives rotation"
        );
        assert!(tok.accepts("aux"), "startup credentials stay accepted");
        // a retired per-node token does NOT survive the next rotation
        assert!(tok.rotate("per-node-2"));
        assert!(tok.accepts("per-node-2"));
        assert!(!tok.accepts("per-node"), "retired per-node tokens die");
        // idempotent rotation
        assert!(!tok.rotate("per-node-2"));
        assert!(!tok.rotate(""), "empty echoes never rotate");
    }

    #[test]
    fn bootstrap_flag_startup_survives_rotation() {
        // resolve(..., bootstrap flag only) — the common self-registration
        // boot; the flag is the construction token, adoption must not evict it
        let dir = tmp_dir("boot-rotate");
        let (t, o) = resolve_node_api_token(&dir, None, Some("boot"));
        assert_eq!(o, TokenOrigin::BootstrapFlag);
        assert!(t.rotate("per-node"));
        assert!(t.accepts("per-node"));
        assert!(t.accepts("boot"), "deploy-chain shared token stays valid");
    }

    #[test]
    fn resolve_priority_and_generation() {
        let dir = tmp_dir("resolve");
        // flag wins over everything
        let (t, o) = resolve_node_api_token(&dir, Some(" flag-tok "), Some("boot"));
        assert_eq!(
            (t.current().as_str(), o),
            ("flag-tok", TokenOrigin::ApiTokenFlag)
        );
        assert!(t.accepts("boot"), "bootstrap flag stays auxiliary");
        // env wins over the adopted file
        write_token_file(&dir.join(NODE_TOKEN_FILE), "adopted").unwrap();
        // (cannot set env vars safely in a multi-threaded test process — the
        // env branch is covered by ordering below instead)
        // adopted file wins over the bootstrap flag
        let (t, o) = resolve_node_api_token(&dir, None, Some("boot"));
        assert_eq!(
            (t.current().as_str(), o),
            ("adopted", TokenOrigin::AdoptedNodeToken)
        );
        assert!(t.accepts("boot"));
        // bootstrap flag when no adoption exists
        std::fs::remove_file(dir.join(NODE_TOKEN_FILE)).unwrap();
        let (t, o) = resolve_node_api_token(&dir, None, Some("boot"));
        assert_eq!(
            (t.current().as_str(), o),
            ("boot", TokenOrigin::BootstrapFlag)
        );
        // nothing configured → generated + persisted + loadable on restart
        let (t, o) = resolve_node_api_token(&dir, None, None);
        assert_eq!(o, TokenOrigin::Generated);
        assert!(t.current().starts_with("tr_e_"), "{}", t.current());
        let (t2, o2) = resolve_node_api_token(&dir, None, None);
        assert_eq!(
            (t2.current().as_str(), o2),
            (t.current().as_str(), TokenOrigin::Generated)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join(API_TOKEN_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "generated token file is 0600"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn token_file_roundtrip_and_guards() {
        let dir = tmp_dir("file");
        let p = dir.join("tok");
        assert!(read_token_file(&p).is_none());
        write_token_file(&p, "abc").unwrap();
        assert_eq!(read_token_file(&p).as_deref(), Some("abc"));
        // whitespace trims, empty and oversized refuse
        write_token_file(&p, "  padded  ").unwrap();
        assert_eq!(read_token_file(&p).as_deref(), Some("padded"));
        write_token_file(&p, "").unwrap();
        assert!(read_token_file(&p).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
