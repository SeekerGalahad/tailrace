//! User repository (§5.2 `user` table).

use anyhow::Result;

use super::models::User;
use super::Store;

const USER_COLS: &str = "id, username, password_hash, display_name, role, disabled, created_at";

pub(super) fn user_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<User> {
    Ok(User {
        id: row.get(0)?,
        username: row.get(1)?,
        password_hash: row.get(2)?,
        display_name: row.get(3)?,
        role: row.get(4)?,
        disabled: row.get::<_, i64>(5)? != 0,
        created_at: row.get(6)?,
    })
}

impl Store {
    pub fn create_user(
        &self,
        username: &str,
        password_hash: &str,
        display_name: Option<&str>,
        role: &str,
    ) -> Result<User> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO user (username, password_hash, display_name, role) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![username, password_hash, display_name, role],
            )?;
            let id = conn.last_insert_rowid();
            Ok(conn.query_row(
                &format!("SELECT {USER_COLS} FROM user WHERE id = ?1"),
                [id],
                user_from_row,
            )?)
        })
    }

    pub fn user_by_username(&self, username: &str) -> Result<Option<User>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare(&format!("SELECT {USER_COLS} FROM user WHERE username = ?1"))?;
            let mut rows = stmt.query([username])?;
            match rows.next()? {
                Some(row) => Ok(Some(user_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    pub fn user_count(&self) -> Result<i64> {
        self.with_conn(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM user", [], |r| r.get(0))?))
    }

    /// First boot seed (§4.1/§5): when the `user` table is empty, create
    /// `admin` with a random argon2id-hashed password and return the plaintext
    /// **once** for the caller to print. No default weak password. Subsequent
    /// calls (or a non-empty table) return `None`.
    pub fn seed_admin_if_empty(&self) -> Result<Option<(String, String)>> {
        if self.user_count()? > 0 {
            return Ok(None);
        }
        use rand::RngCore;
        let mut raw = [0u8; 12];
        rand::rng().fill_bytes(&mut raw);
        let password: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        let hash = crate::password::hash_password(&password)?;
        self.create_user("admin", &hash, Some("管理员"), "admin")?;
        Ok(Some(("admin".to_string(), password)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_lookup() {
        let store = Store::open_in_memory().unwrap();
        assert_eq!(store.user_count().unwrap(), 0);
        let u = store
            .create_user("admin", "phc$hash", Some("管理员"), "admin")
            .unwrap();
        assert_eq!(u.username, "admin");
        assert_eq!(u.role, "admin");
        assert!(!u.disabled);
        assert_eq!(store.user_count().unwrap(), 1);
        assert!(store.user_by_username("nobody").unwrap().is_none());
        let again = store.user_by_username("admin").unwrap().unwrap();
        assert_eq!(again.id, u.id);
        assert_eq!(again.display_name.as_deref(), Some("管理员"));
        // duplicate username is rejected
        assert!(store.create_user("admin", "x", None, "admin").is_err());
    }

    #[test]
    fn seed_admin_once_then_never_again() {
        let store = Store::open_in_memory().unwrap();
        let (username, password) = store
            .seed_admin_if_empty()
            .unwrap()
            .expect("first boot seeds");
        assert_eq!(username, "admin");
        assert_eq!(password.len(), 24); // 12 bytes hex
        let u = store.user_by_username("admin").unwrap().unwrap();
        assert!(
            u.password_hash.starts_with("$argon2id$"),
            "PHC string expected: {}",
            u.password_hash
        );
        assert_eq!(u.display_name.as_deref(), Some("管理员"));
        // verify the printed password actually authenticates
        assert!(crate::password::verify_password(
            &password,
            &u.password_hash
        ));
        // non-empty table -> no second seed; plaintext never returned again
        assert!(store.seed_admin_if_empty().unwrap().is_none());
        // a store that already has users never seeds
        let other = Store::open_in_memory().unwrap();
        other.create_user("op", "h", None, "admin").unwrap();
        assert!(other.seed_admin_if_empty().unwrap().is_none());
    }
}
