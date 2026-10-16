//! Credential encryption (§5.3 static half): AES-256-GCM over the plaintext
//! `{"username":…,"password":…}` JSON; the two DDL columns carry the 12-byte
//! nonce and the ciphertext, each base64-encoded.
//!
//! Master key resolution, fail-closed (no machine-fingerprint derivation
//! fallback — that path historically bricked every stored password after a
//! host/user change):
//! 1. `TAILRACE_MASTER_KEY` env (base64 of exactly 32 bytes);
//! 2. else `<data-dir>/master.key` (0600, base64, generated on first boot);
//!
//! any resolution failure is an error — the caller (console main) refuses to
//! start rather than degrade to plaintext.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use rand::RngCore;

/// 12 bytes (GCM standard nonce size; matches the DDL comment "12B base64").
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
pub const MASTER_KEY_ENV: &str = "TAILRACE_MASTER_KEY";
pub const MASTER_KEY_FILE: &str = "master.key";

#[derive(Clone)]
pub struct CredentialCipher {
    cipher: Aes256Gcm,
}

fn encode(bytes: &[u8]) -> String {
    B64.encode(bytes)
}

impl CredentialCipher {
    pub fn from_key(key: [u8; KEY_LEN]) -> Self {
        Self {
            cipher: Aes256Gcm::new((&key).into()),
        }
    }

    /// Random in-memory key (tests and in-process-only usage).
    pub fn ephemeral() -> Self {
        Self::from_key(generate_key())
    }

    /// §5.3 resolution: env value (if present) wins; else the key file,
    /// created on first boot. Errors are fatal by contract (fail-closed).
    pub fn load(env_value: Option<&str>, data_dir: &std::path::Path) -> Result<Self> {
        if let Some(v) = env_value {
            let key = decode_key(v).context(format!("{MASTER_KEY_ENV}: invalid master key"))?;
            return Ok(Self::from_key(key));
        }
        let path = data_dir.join(MASTER_KEY_FILE);
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let key = decode_key(text.trim())
                    .with_context(|| format!("{}: invalid master key file", path.display()))?;
                Ok(Self::from_key(key))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let key = generate_key();
                write_key_file(&path, &key)?;
                Ok(Self::from_key(key))
            }
            Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
        }
    }

    /// Seal → (nonce_b64, ciphertext_b64), the two `datasource_credential`
    /// columns. A fresh random nonce per call (never reused with one key).
    pub fn seal(&self, plaintext: &str) -> Result<(String, String)> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ct = self
            .cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|e| anyhow::anyhow!("aes-gcm encrypt: {e}"))?;
        Ok((encode(&nonce_bytes), encode(&ct)))
    }

    /// Open the stored halves (reverse of `seal`).
    pub fn open(&self, nonce_b64: &str, ciphertext_b64: &str) -> Result<String> {
        let nonce_bytes = B64
            .decode(nonce_b64)
            .map_err(|e| anyhow::anyhow!("nonce base64: {e}"))?;
        if nonce_bytes.len() != NONCE_LEN {
            bail!("nonce must be {NONCE_LEN} bytes, got {}", nonce_bytes.len());
        }
        let ct = B64
            .decode(ciphertext_b64)
            .map_err(|e| anyhow::anyhow!("ciphertext base64: {e}"))?;
        let pt = self
            .cipher
            .decrypt(Nonce::from_slice(&nonce_bytes), ct.as_ref())
            .map_err(|e| anyhow::anyhow!("aes-gcm decrypt (wrong key or tampered): {e}"))?;
        String::from_utf8(pt).context("decrypted credential is not utf-8")
    }
}

fn generate_key() -> [u8; KEY_LEN] {
    let mut key = [0u8; KEY_LEN];
    rand::rng().fill_bytes(&mut key);
    key
}

fn decode_key(text: &str) -> Result<[u8; KEY_LEN]> {
    let bytes = B64
        .decode(text)
        .map_err(|e| anyhow::anyhow!("base64: {e}"))?;
    if bytes.len() != KEY_LEN {
        bail!("expected {KEY_LEN} bytes after base64, got {}", bytes.len());
    }
    let mut key = [0u8; KEY_LEN];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn write_key_file(path: &std::path::Path, key: &[u8; KEY_LEN]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    std::fs::write(path, format!("{}\n", encode(key)))
        .with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod 600 {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_roundtrip_and_no_plaintext_leak() {
        let c = CredentialCipher::ephemeral();
        let (n, ct) = c
            .seal(r#"{"username":"u1","password":"s3cret-pw"}"#)
            .unwrap();
        assert_eq!(
            c.open(&n, &ct).unwrap(),
            r#"{"username":"u1","password":"s3cret-pw"}"#
        );
        // stored halves never carry the plaintext
        assert!(!ct.contains("s3cret-pw"));
        assert!(!n.contains("s3cret-pw"));
        // nonce is 12 bytes after base64 decode
        assert_eq!(B64.decode(&n).unwrap().len(), 12);
        // a fresh seal produces a different nonce (never reused)
        let (n2, _) = c.seal("x").unwrap();
        assert_ne!(n, n2);
    }

    #[test]
    fn wrong_key_fails_to_open() {
        let (n, ct) = CredentialCipher::ephemeral().seal("secret").unwrap();
        let other = CredentialCipher::ephemeral();
        assert!(other.open(&n, &ct).is_err());
    }

    #[test]
    fn load_env_wins_over_file() {
        let key = generate_key();
        let dir = std::env::temp_dir().join(format!("tailrace-sb-env-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let c = CredentialCipher::load(Some(&encode(&key)), &dir).unwrap();
        // env key must not touch the key file
        assert!(!dir.join(MASTER_KEY_FILE).exists());
        let (n, ct) = c.seal("v").unwrap();
        assert_eq!(CredentialCipher::from_key(key).open(&n, &ct).unwrap(), "v");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_generates_key_file_on_first_boot_then_reuses() {
        let dir = std::env::temp_dir().join(format!("tailrace-sb-file-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let first = CredentialCipher::load(None, &dir).unwrap();
        let path = dir.join(MASTER_KEY_FILE);
        assert!(path.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "key file must be 0600");
        }
        // reboot path: same file → same key (sealed data stays openable)
        let (n, ct) = first.seal("persist").unwrap();
        let second = CredentialCipher::load(None, &dir).unwrap();
        assert_eq!(second.open(&n, &ct).unwrap(), "persist");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_fails_closed_on_bad_env_and_corrupt_file() {
        assert!(
            CredentialCipher::load(Some("not-base64!!"), std::path::Path::new("/tmp")).is_err()
        );
        let short = B64.encode([1u8; 8]);
        assert!(CredentialCipher::load(Some(&short), std::path::Path::new("/tmp")).is_err());
        let dir = std::env::temp_dir().join(format!("tailrace-sb-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(MASTER_KEY_FILE), "garbage").unwrap();
        assert!(
            CredentialCipher::load(None, &dir).is_err(),
            "corrupt key file must fail closed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
