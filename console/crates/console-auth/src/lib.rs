//! console-auth — authentication plane of the tailrace console (design doc
//! §4.0/§4.1, A3): argon2id login with brute-force lockout, opaque session
//! tokens (cookie `tr_session` first, `Authorization: Bearer` also accepted),
//! the m2m bootstrap/node-token channel, and the shared error envelope.

pub mod bootstrap;
pub mod error;
pub mod lockout;
pub mod middleware;
pub mod session;
pub mod token;

pub use bootstrap::{BootstrapAuthority, VerifyError, BOOTSTRAP_TOKEN_ENV};
pub use error::ApiError;
pub use lockout::{LockCheck, LoginLockout};
pub use middleware::{
    extract_session_token, require_node_heartbeat, require_node_register, require_session,
    AuthContext, M2mState, NodePrincipal, SessionState,
};
pub use session::{
    clear_session_cookie, session_cookie, AuthUser, LoginError, LoginSuccess, SessionService,
    SESSION_COOKIE, SESSION_TTL_SECS,
};
pub use token::{generate_token, token_hash, BOOTSTRAP_TOKEN_PREFIX, SESSION_TOKEN_PREFIX};

// argon2 hashing primitives are provided by console-store (seed needs them
// without a cycle); re-export for one-stop imports.
pub use console_store::{hash_password, verify_password};
