//! argon2id password hashing primitives (§4.1: PHC string in `user.password_hash`).
//!
//! Lives in console-store because the first-boot admin seed (§5) must hash a
//! generated password without depending on console-auth (which depends on this
//! crate). console-auth re-exports these for the login flow.

use anyhow::{Context, Result};
use argon2::password_hash::phc::PasswordHash;
use argon2::password_hash::{PasswordHasher, PasswordVerifier};
use argon2::{Algorithm, Argon2};

/// Hash a password into an argon2id PHC string (`$argon2id$v=19$m=…`),
/// salt auto-generated (argon2 0.6 `hash_password`).
pub fn hash_password(password: &str) -> Result<String> {
    Argon2::default()
        .hash_password(password.as_bytes())
        .map(|h| h.to_string())
        .context("argon2 hash")
}

/// Verify a password against a PHC string; only argon2id hashes are honored.
pub fn verify_password(password: &str, phc: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(phc) else {
        return false;
    };
    if parsed.algorithm.as_str() != "argon2id" {
        return false;
    }
    let _ = Algorithm::Argon2id; // referenced for clarity; default() is argon2id
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let phc = hash_password("s3cret-PW").unwrap();
        assert!(phc.starts_with("$argon2id$"), "got: {phc}");
        assert!(verify_password("s3cret-PW", &phc));
        assert!(!verify_password("wrong", &phc));
        assert!(!verify_password("s3cret-PW", "not-a-phc-string"));
    }
}
