//! Session repository (§5.2 `session` table; plaintext tokens never stored,
//! only sha256 hashes).

use anyhow::Result;

use super::models::User;
use super::Store;

impl Store {
    /// Insert a session; `expires_at` is now + `ttl_secs` (SQLite UTC clock).
    pub fn insert_session(&self, token_hash: &str, user_id: i64, ttl_secs: i64) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO session (token_hash, user_id, expires_at) \
                 VALUES (?1, ?2, datetime('now', ?3 || ' seconds'))",
                rusqlite::params![token_hash, user_id, ttl_secs.to_string()],
            )?;
            Ok(())
        })
    }

    /// Resolve a token hash to its user; expired sessions and disabled users
    /// resolve to `None`.
    pub fn session_user(&self, token_hash: &str) -> Result<Option<User>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT u.id, u.username, u.password_hash, u.display_name, u.role, u.disabled, u.created_at \
                 FROM session s JOIN user u ON u.id = s.user_id \
                 WHERE s.token_hash = ?1 AND s.expires_at > datetime('now') AND u.disabled = 0",
            )?;
            let mut rows = stmt.query([token_hash])?;
            match rows.next()? {
                Some(row) => Ok(Some(super::users::user_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    /// Delete one session; returns whether a row was removed.
    pub fn delete_session(&self, token_hash: &str) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(conn.execute("DELETE FROM session WHERE token_hash = ?1", [token_hash])? > 0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_lifecycle_and_expiry() {
        let store = Store::open_in_memory().unwrap();
        let u = store.create_user("admin", "h", None, "admin").unwrap();
        store.insert_session("hash1", u.id, 3600).unwrap();
        assert!(store.session_user("hash1").unwrap().is_some());
        assert!(store.session_user("bogus").unwrap().is_none());
        // TTL already elapsed -> expired
        store.insert_session("hash2", u.id, -10).unwrap();
        assert!(store.session_user("hash2").unwrap().is_none());
        // revoked
        assert!(store.delete_session("hash1").unwrap());
        assert!(store.session_user("hash1").unwrap().is_none());
        assert!(!store.delete_session("hash1").unwrap());
    }
}
