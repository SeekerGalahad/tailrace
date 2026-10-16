//! Audit repository (§5.2 `audit_log`). Actors: session username, "node:sn-…"
//! or "bootstrap" (§5.2 comment).

use anyhow::Result;

use super::models::AuditEntry;
use super::Store;

impl Store {
    pub fn insert_audit(&self, entry: &AuditEntry) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO audit_log (actor, action, entity_type, entity_id, detail_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    entry.actor,
                    entry.action,
                    entry.entity_type,
                    entry.entity_id,
                    entry.detail_json
                ],
            )?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_insert() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_audit(&AuditEntry {
                actor: "bootstrap".into(),
                action: "NODE_REGISTER".into(),
                entity_type: "node".into(),
                entity_id: Some("sn-x".into()),
                detail_json: Some(r#"{"registerMode":"self"}"#.into()),
            })
            .unwrap();
        store
            .with_conn(|c| {
                let n: i64 = c.query_row("SELECT COUNT(*) FROM audit_log", [], |r| r.get(0))?;
                assert_eq!(n, 1);
                Ok(())
            })
            .unwrap();
    }
}
