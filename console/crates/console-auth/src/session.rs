//! Session service (§4.1): argon2id verification + opaque token issuance +
//! lockout. Cookie assembly for the `tr_session` cookie also lives here.

use std::time::Instant;

use console_store::{verify_password, Store};

use crate::lockout::{LockCheck, LoginLockout};
use crate::token::{generate_token, token_hash, SESSION_TOKEN_PREFIX};

/// Session lifetime (§4.1: `expiresInSec: 86400`).
pub const SESSION_TTL_SECS: i64 = 86400;

/// Cookie name (§4.0: `tr_session=<token>`; HttpOnly; SameSite=Lax; Path=/).
pub const SESSION_COOKIE: &str = "tr_session";

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub id: i64,
    pub username: String,
    pub display_name: Option<String>,
    pub role: String,
}

#[derive(Debug)]
pub struct LoginSuccess {
    pub token: String,
    pub expires_in_secs: i64,
    pub user: AuthUser,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginError {
    /// Username+password mismatch (or unknown/disabled user).
    BadCredentials,
    /// Brute-force lockout active (§4.1).
    Locked { retry_in_secs: u64 },
}

#[derive(Clone)]
pub struct SessionService {
    store: Store,
    lockout: std::sync::Arc<LoginLockout>,
}

impl SessionService {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            lockout: std::sync::Arc::new(LoginLockout::new()),
        }
    }

    /// Verify credentials, applying the lockout policy, and mint a session.
    pub fn login(&self, username: &str, password: &str) -> Result<LoginSuccess, LoginError> {
        match self.lockout.check(username, Instant::now()) {
            LockCheck::Locked { retry_in_secs } => {
                return Err(LoginError::Locked { retry_in_secs });
            }
            LockCheck::Allowed => {}
        }
        let user = self.store.user_by_username(username).ok().flatten();
        let ok = match &user {
            Some(u) if !u.disabled => verify_password(password, &u.password_hash),
            _ => false,
        };
        if !ok {
            // Unknown users also burn the counter (no username oracle).
            self.lockout.record_failure(username, Instant::now());
            return Err(LoginError::BadCredentials);
        }
        self.lockout.record_success(username);
        let user = user.expect("checked above");
        let token = generate_token(SESSION_TOKEN_PREFIX);
        self.store
            .insert_session(&token_hash(&token), user.id, SESSION_TTL_SECS)
            .map_err(|_| LoginError::BadCredentials)?;
        Ok(LoginSuccess {
            token,
            expires_in_secs: SESSION_TTL_SECS,
            user: AuthUser {
                id: user.id,
                username: user.username,
                display_name: user.display_name,
                role: user.role,
            },
        })
    }

    /// Resolve a presented token (cookie or bearer) to its user. `None` when
    /// unknown, expired, or the user is disabled.
    pub fn authenticate(&self, token: &str) -> Option<(AuthUser, String)> {
        let hash = token_hash(token);
        let user = self.store.session_user(&hash).ok().flatten()?;
        Some((
            AuthUser {
                id: user.id,
                username: user.username,
                display_name: user.display_name,
                role: user.role,
            },
            hash,
        ))
    }

    /// Revoke the session bound to a token hash (logout).
    pub fn logout(&self, token_hash: &str) -> bool {
        self.store.delete_session(token_hash).unwrap_or(false)
    }
}

/// Set-Cookie value for a fresh login session.
pub fn session_cookie(token: &str) -> String {
    format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={SESSION_TTL_SECS}")
}

/// Set-Cookie value that clears the session cookie (logout).
pub fn clear_session_cookie() -> String {
    format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0")
}

#[cfg(test)]
mod tests {
    use super::*;
    use console_store::hash_password;

    fn setup() -> SessionService {
        let store = Store::open_in_memory().unwrap();
        store
            .create_user(
                "admin",
                &hash_password("pw-123").unwrap(),
                Some("管理员"),
                "admin",
            )
            .unwrap();
        SessionService::new(store)
    }

    #[test]
    fn login_issues_token_and_authenticates() {
        let svc = setup();
        let err = svc.login("admin", "wrong").unwrap_err();
        assert_eq!(err, LoginError::BadCredentials);
        let ok = svc.login("admin", "pw-123").unwrap();
        assert_eq!(ok.user.username, "admin");
        assert_eq!(ok.expires_in_secs, 86400);
        assert!(ok.token.starts_with("tr_s_"));
        let (user, hash) = svc.authenticate(&ok.token).unwrap();
        assert_eq!(user.username, "admin");
        // logout revokes
        assert!(svc.logout(&hash));
        assert!(svc.authenticate(&ok.token).is_none());
    }

    #[test]
    fn cookie_strings() {
        assert!(session_cookie("tr_s_abc")
            .starts_with("tr_session=tr_s_abc; HttpOnly; SameSite=Lax; Path=/"));
        assert!(session_cookie("t").contains("Max-Age=86400"));
        assert_eq!(
            clear_session_cookie(),
            "tr_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0"
        );
    }

    #[test]
    fn five_failures_then_locked_even_with_right_password() {
        let svc = setup();
        for _ in 0..5 {
            let _ = svc.login("admin", "nope");
        }
        assert!(matches!(
            svc.login("admin", "pw-123").unwrap_err(),
            LoginError::Locked {
                retry_in_secs: 59..=60
            }
        ));
    }
}
