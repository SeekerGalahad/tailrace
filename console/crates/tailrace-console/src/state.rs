//! Application state shared by all handlers.

use std::path::PathBuf;
use std::sync::Arc;

use console_auth::{BootstrapAuthority, M2mState, SessionService, SessionState};
use console_store::{CredentialCipher, Store};

#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    store: Store,
    sessions: SessionService,
    bootstrap: BootstrapAuthority,
    /// §4.4 artifact storage root (`<data-dir>/artifacts`).
    artifact_dir: PathBuf,
    /// T-A2 backfill ledger root (`<data-dir>/backfill`, a sibling of the
    /// artifact dir — derived once here so the executor and tests share the
    /// same layout). One `bf-{jobId}` subdirectory per bounded replay job
    /// (engine W2 `backfill_state_dir`); main-pipeline ledgers never live
    /// here.
    backfill_dir: PathBuf,
    /// Console → node client (30s total timeout; §4.6 proxies).
    node_client: reqwest::Client,
    /// Long-lived client for the SSE log bridge (no total timeout).
    stream_client: reqwest::Client,
    /// §5.3 credential cipher (AES-256-GCM; the key is loaded by main and
    /// never persisted anywhere the cipher could leak it).
    cipher: CredentialCipher,
}

impl AppState {
    pub fn new(store: Store, bootstrap: BootstrapAuthority) -> Self {
        Self::new_with_artifact_dir(store, bootstrap, PathBuf::from("./data/artifacts"))
    }

    /// Constructor with a lazily-created artifact dir and an ephemeral
    /// in-memory credential key (tests: sealed credentials do not outlive
    /// the process). Production main uses [`Self::new_full`] with the §5.3
    /// master key.
    pub fn new_with_artifact_dir(
        store: Store,
        bootstrap: BootstrapAuthority,
        artifact_dir: PathBuf,
    ) -> Self {
        Self::new_full(
            store,
            bootstrap,
            artifact_dir,
            CredentialCipher::ephemeral(),
        )
    }

    /// Full constructor: `artifact_dir` is created lazily by the upload
    /// handler (tests pass per-test temp dirs); `cipher` seals datasource
    /// credentials at rest (§5.3).
    pub fn new_full(
        store: Store,
        bootstrap: BootstrapAuthority,
        artifact_dir: PathBuf,
        cipher: CredentialCipher,
    ) -> Self {
        Self(Arc::new(Inner {
            sessions: SessionService::new(store.clone()),
            store,
            bootstrap,
            node_client: console_proxy::node_http_client().expect("node http client"),
            stream_client: console_proxy::node_stream_client().expect("node stream client"),
            artifact_dir: artifact_dir.clone(),
            backfill_dir: artifact_dir
                .parent()
                .map(|data| data.join("backfill"))
                .unwrap_or_else(|| PathBuf::from("./data/backfill")),
            cipher,
        }))
    }

    pub fn store(&self) -> &Store {
        &self.0.store
    }

    pub fn session_service(&self) -> &SessionService {
        &self.0.sessions
    }

    pub fn artifact_dir(&self) -> &std::path::Path {
        &self.0.artifact_dir
    }

    /// T-A2: backfill ledger root (`<data-dir>/backfill`). The executor
    /// creates per-job `bf-{jobId}` subdirectories lazily.
    pub fn backfill_dir(&self) -> &std::path::Path {
        &self.0.backfill_dir
    }

    pub fn node_client(&self) -> &reqwest::Client {
        &self.0.node_client
    }

    pub fn stream_client(&self) -> &reqwest::Client {
        &self.0.stream_client
    }

    pub fn cipher(&self) -> &CredentialCipher {
        &self.0.cipher
    }
}

impl SessionState for AppState {
    fn sessions(&self) -> &SessionService {
        &self.0.sessions
    }
}

impl M2mState for AppState {
    fn store(&self) -> &Store {
        &self.0.store
    }

    fn bootstrap(&self) -> &BootstrapAuthority {
        &self.0.bootstrap
    }
}
