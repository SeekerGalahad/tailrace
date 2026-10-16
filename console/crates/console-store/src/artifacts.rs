//! Artifact repository (§5.2 `artifact`; §4.4 upload/list/delete + refCount).

use anyhow::Result;

use super::models::Artifact;
use super::Store;

/// Upload outcome: (name, version) duplicates are 409 (§4.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactInsert {
    Created(i64),
    Duplicate,
}

fn artifact_from_row(row: &rusqlite::Row<'_>, ref_count: i64) -> rusqlite::Result<Artifact> {
    Ok(Artifact {
        id: row.get(0)?,
        name: row.get(1)?,
        version: row.get(2)?,
        filename: row.get(3)?,
        sha256: row.get(4)?,
        size_bytes: row.get(5)?,
        uploaded_by: row.get(6)?,
        created_at: row.get(7)?,
        ref_count,
    })
}

impl Store {
    pub fn insert_artifact(
        &self,
        name: &str,
        version: &str,
        filename: &str,
        sha256: &str,
        size_bytes: i64,
        uploaded_by: &str,
    ) -> Result<ArtifactInsert> {
        self.with_conn(|conn| {
            let dup: i64 = conn.query_row(
                "SELECT COUNT(*) FROM artifact WHERE name = ?1 AND version = ?2",
                rusqlite::params![name, version],
                |r| r.get(0),
            )?;
            if dup > 0 {
                return Ok(ArtifactInsert::Duplicate);
            }
            conn.execute(
                "INSERT INTO artifact (name, version, filename, sha256, size_bytes, uploaded_by) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![name, version, filename, sha256, size_bytes, uploaded_by],
            )?;
            Ok(ArtifactInsert::Created(conn.last_insert_rowid()))
        })
    }

    pub fn artifact(&self, id: i64) -> Result<Option<Artifact>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT a.id, a.name, a.version, a.filename, a.sha256, a.size_bytes, a.uploaded_by, a.created_at, \
                 (SELECT COUNT(*) FROM node n WHERE n.artifact_id = a.id) \
                 FROM artifact a WHERE a.id = ?1",
            )?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(artifact_from_row(row, row.get(8)?)?)),
                None => Ok(None),
            }
        })
    }

    /// List with the node refCount joined in (§4.4 GET /api/artifacts).
    pub fn list_artifacts(&self) -> Result<Vec<Artifact>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT a.id, a.name, a.version, a.filename, a.sha256, a.size_bytes, a.uploaded_by, a.created_at, \
                 (SELECT COUNT(*) FROM node n WHERE n.artifact_id = a.id) \
                 FROM artifact a ORDER BY a.id DESC",
            )?;
            let rows = stmt.query_map([], |r| artifact_from_row(r, r.get(8)?))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Nodes still pointing at this artifact (the delete guard, §4.4).
    pub fn nodes_referencing_artifact(&self, id: i64) -> Result<Vec<i64>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare("SELECT id FROM node WHERE artifact_id = ?1 ORDER BY id")?;
            let rows = stmt.query_map([id], |r| r.get::<_, i64>(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub fn delete_artifact(&self, id: i64) -> Result<bool> {
        self.with_conn(|conn| Ok(conn.execute("DELETE FROM artifact WHERE id = ?1", [id])? > 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_roundtrip_and_refcount() {
        let store = Store::open_in_memory().unwrap();
        let id = match store
            .insert_artifact("tailrace-node", "1.0.0", "f.tar.gz", "9a8b", 100, "admin")
            .unwrap()
        {
            ArtifactInsert::Created(id) => id,
            ArtifactInsert::Duplicate => panic!("first insert must succeed"),
        };
        assert_eq!(
            store
                .insert_artifact("tailrace-node", "1.0.0", "f.tar.gz", "9a8b", 100, "admin")
                .unwrap(),
            ArtifactInsert::Duplicate
        );
        let a = store.artifact(id).unwrap().unwrap();
        assert_eq!(
            (a.name.as_str(), a.version.as_str(), a.ref_count),
            ("tailrace-node", "1.0.0", 0)
        );
        assert_eq!(store.list_artifacts().unwrap().len(), 1);
        assert!(store.nodes_referencing_artifact(id).unwrap().is_empty());
        assert!(store.delete_artifact(id).unwrap());
        assert!(store.artifact(id).unwrap().is_none());
    }
}
