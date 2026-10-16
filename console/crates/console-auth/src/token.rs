//! Opaque session tokens (§4.1): `tr_s_<48hex>` random plaintext handed to the
//! client once; only sha256(token) is stored server-side.

use rand::RngCore;
use sha2::{Digest, Sha256};

/// Prefix for console session tokens (design doc §4.1 example `tr_s_9f8e…`).
pub const SESSION_TOKEN_PREFIX: &str = "tr_s_";

/// Prefix for the console-global bootstrap token (self-registration pairing).
pub const BOOTSTRAP_TOKEN_PREFIX: &str = "tr_b_";

/// Generate a fresh opaque token with the given prefix (24 random bytes hex).
pub fn generate_token(prefix: &str) -> String {
    let mut raw = [0u8; 24];
    rand::rng().fill_bytes(&mut raw);
    format!("{prefix}{}", hex(&raw))
}

/// sha256 hex digest (what the `session` table stores).
pub fn token_hash(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_shape_and_hash() {
        let t = generate_token(SESSION_TOKEN_PREFIX);
        assert!(t.starts_with("tr_s_"));
        assert_eq!(t.len(), "tr_s_".len() + 48);
        assert!(t.chars().skip(5).all(|c| c.is_ascii_hexdigit()));
        // deterministic hash, different per token
        assert_eq!(token_hash(&t), token_hash(&t));
        assert_ne!(
            token_hash(&t),
            token_hash(&generate_token(SESSION_TOKEN_PREFIX))
        );
    }
}
