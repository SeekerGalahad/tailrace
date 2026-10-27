//! Datasource / credential / table-object repositories (§5.2).
//!
//! P0 covers storage primitives only; the REST surface (discover / infer-schema,
//! §4.2) lands in a later stage. Credentials are stored as AES-256-GCM halves
//! (nonce + ciphertext); the crypto itself is console-side later (§5.3).

use anyhow::Result;
use serde_json::Value;

use super::models::{Datasource, DatasourceCredential, TableObject};
use super::Store;

/// Outcome of a CAS-guarded update (§4.2 PUT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasourceUpdate {
    /// Applied; carries the new generation.
    Applied(i64),
    /// `generation` did not match (409 GENERATION_CONFLICT).
    GenerationConflict,
    /// Another datasource already owns `name` (409).
    NameConflict,
    NotFound,
}

const DS_COLS: &str = "id, name, type, conn_json, status, generation, created_at, updated_at";

fn ds_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Datasource> {
    Ok(Datasource {
        id: row.get(0)?,
        name: row.get(1)?,
        ds_type: row.get(2)?,
        conn_json: row.get(3)?,
        status: row.get(4)?,
        generation: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

impl Store {
    pub fn insert_datasource(&self, name: &str, ds_type: &str, conn_json: &str) -> Result<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO datasource (name, type, conn_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![name, ds_type, conn_json],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    /// Update with generation CAS (§4.2). `credential` = Some only when the
    /// caller supplied a fresh password (blank password keeps the old one);
    /// generation is bumped on success. Atomic (T8): the row UPDATE and the
    /// credential UPSERT commit together, so a failed credential write
    /// cannot leave a bumped generation with the old secret.
    pub fn update_datasource(
        &self,
        id: i64,
        name: &str,
        ds_type: &str,
        conn_json: &str,
        expected_generation: i64,
        credential: Option<(&str, &str)>,
    ) -> Result<DatasourceUpdate> {
        self.with_tx(|conn| {
            let dup: i64 = conn.query_row(
                "SELECT COUNT(*) FROM datasource WHERE name = ?1 AND id <> ?2",
                rusqlite::params![name, id],
                |r| r.get(0),
            )?;
            if dup > 0 {
                return Ok(DatasourceUpdate::NameConflict);
            }
            let updated = conn.execute(
                "UPDATE datasource SET name = ?1, type = ?2, conn_json = ?3, \
                 generation = generation + 1, updated_at = datetime('now') \
                 WHERE id = ?4 AND generation = ?5",
                rusqlite::params![name, ds_type, conn_json, id, expected_generation],
            )?;
            if updated == 0 {
                let exists: i64 =
                    conn.query_row("SELECT COUNT(*) FROM datasource WHERE id = ?1", [id], |r| {
                        r.get(0)
                    })?;
                return Ok(if exists == 0 {
                    DatasourceUpdate::NotFound
                } else {
                    DatasourceUpdate::GenerationConflict
                });
            }
            if let Some((nonce, ciphertext)) = credential {
                conn.execute(
                    "INSERT INTO datasource_credential (datasource_id, nonce, ciphertext) \
                     VALUES (?1, ?2, ?3) \
                     ON CONFLICT(datasource_id) DO UPDATE SET nonce = excluded.nonce, \
                     ciphertext = excluded.ciphertext, updated_at = datetime('now')",
                    rusqlite::params![id, nonce, ciphertext],
                )?;
            }
            let gen: i64 = conn.query_row(
                "SELECT generation FROM datasource WHERE id = ?1",
                [id],
                |r| r.get(0),
            )?;
            Ok(DatasourceUpdate::Applied(gen))
        })
    }

    /// Probe write-back (§4.2 `POST /{id}/test`): CREATED/CONN_OK/CONN_FAIL.
    pub fn set_datasource_status(&self, id: i64, status: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE datasource SET status = ?1, updated_at = datetime('now') WHERE id = ?2",
                rusqlite::params![status, id],
            )?;
            Ok(())
        })
    }

    /// Datasource ids whose `ds_mapping_json` maps some db onto this
    /// datasource (the §4.2 delete guard, refs.pipelines).
    pub fn pipelines_referencing_datasource(&self, id: i64) -> Result<Vec<i64>> {
        let all = self.list_pipelines()?;
        let mut out = Vec::new();
        for p in all {
            if let Some(raw) = &p.ds_mapping_json {
                if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(raw) {
                    if map.values().any(|v| v.as_i64() == Some(id)) {
                        out.push(p.id);
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn datasource(&self, id: i64) -> Result<Option<Datasource>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare(&format!("SELECT {DS_COLS} FROM datasource WHERE id = ?1"))?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(ds_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    pub fn list_datasources(&self, ds_type: Option<&str>) -> Result<Vec<Datasource>> {
        self.with_conn(|conn| match ds_type {
            Some(t) => {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {DS_COLS} FROM datasource WHERE type = ?1 ORDER BY id"
                ))?;
                let rows = stmt.query_map([t], ds_from_row)?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            }
            None => {
                let mut stmt =
                    conn.prepare(&format!("SELECT {DS_COLS} FROM datasource ORDER BY id"))?;
                let rows = stmt.query_map([], ds_from_row)?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            }
        })
    }

    pub fn table_count(&self, datasource_id: i64) -> Result<i64> {
        self.with_conn(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM table_object WHERE datasource_id = ?1",
                [datasource_id],
                |r| r.get(0),
            )?)
        })
    }

    pub fn delete_datasource(&self, id: i64) -> Result<bool> {
        self.with_conn(|conn| Ok(conn.execute("DELETE FROM datasource WHERE id = ?1", [id])? > 0))
    }

    pub fn upsert_credential(
        &self,
        datasource_id: i64,
        nonce: &str,
        ciphertext: &str,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO datasource_credential (datasource_id, nonce, ciphertext) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(datasource_id) DO UPDATE SET nonce = excluded.nonce, \
                 ciphertext = excluded.ciphertext, updated_at = datetime('now')",
                rusqlite::params![datasource_id, nonce, ciphertext],
            )?;
            Ok(())
        })
    }

    pub fn credential(&self, datasource_id: i64) -> Result<Option<DatasourceCredential>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT nonce, ciphertext FROM datasource_credential WHERE datasource_id = ?1",
            )?;
            let mut rows = stmt.query([datasource_id])?;
            match rows.next()? {
                Some(row) => Ok(Some(DatasourceCredential {
                    nonce: row.get(0)?,
                    ciphertext: row.get(1)?,
                })),
                None => Ok(None),
            }
        })
    }

    /// Register a table object (§4.2). `cursor_semantics` derives from the
    /// kind: kafka → KAFKA_OFFSET, file → FILE_POINTER, jdbc → JDBC_KEY
    /// (IR `Cursor::JdbcKey`, the mysql/postgresql batch sources — T14 ←
    /// R3-W1 review low#4), else NONE (doris/starrocks sinks and dim
    /// broadcast tables carry no cursor). `sequence_column` (T-D1): the
    /// declared key column (dim full-table load pagination key / doris
    /// UNIQUE KEY dedup basis / jdbc incremental cursor), None = not
    /// declared. Errors with a constraint violation when (datasource, db,
    /// table) already exists.
    #[allow(clippy::too_many_arguments)] // the §4.2 registration form is one flat tuple
    pub fn insert_table_object(
        &self,
        datasource_id: i64,
        db_name: &str,
        table_name: &str,
        kind: &str,
        format: Option<&str>,
        columns_json: Option<&str>,
        sequence_column: Option<&str>,
    ) -> Result<i64> {
        let cursor = match kind {
            "kafka" => "KAFKA_OFFSET",
            "file" => "FILE_POINTER",
            "jdbc" => "JDBC_KEY",
            _ => "NONE",
        };
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO table_object (datasource_id, db_name, table_name, kind, format, columns_json, cursor_semantics, source, sequence_column) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'manual', ?8)",
                rusqlite::params![datasource_id, db_name, table_name, kind, format, columns_json, cursor, sequence_column],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    /// Look up by business key (the publish/deploy gate resolution path).
    pub fn table_object_by_ref(
        &self,
        datasource_id: i64,
        db_name: &str,
        table_name: &str,
    ) -> Result<Option<TableObject>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, datasource_id, db_name, table_name, kind, format, cursor_semantics, source, sequence_column \
                 FROM table_object WHERE datasource_id = ?1 AND db_name = ?2 AND table_name = ?3",
            )?;
            let mut rows = stmt.query(rusqlite::params![datasource_id, db_name, table_name])?;
            match rows.next()? {
                Some(row) => Ok(Some(table_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    pub fn table_object(&self, id: i64) -> Result<Option<TableObject>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, datasource_id, db_name, table_name, kind, format, cursor_semantics, source, sequence_column \
                 FROM table_object WHERE id = ?1",
            )?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(table_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    /// Resolve a plan/statement reference ("dsId.db.table") → table object
    /// (the publish gate and plan push-down resolution path, §4.5).
    pub fn table_object_for_reference(&self, reference: &str) -> Result<Option<TableObject>> {
        let parts: Vec<&str> = reference.split('.').collect();
        if parts.len() != 3 {
            return Ok(None);
        }
        let (Ok(ds_id), db, table) = (parts[0].parse::<i64>(), parts[1], parts[2]) else {
            return Ok(None);
        };
        self.table_object_by_ref(ds_id, db, table)
    }

    /// Resolve a 2-part dim-join reference ("dsId.table") → table object
    /// (T-D1): the parser emits `${ds:dim.<datasourceId>.<table>}` for
    /// dimension joins, so the reference carries no db segment — the JDBC
    /// URL pins the database. The lookup matches ANY kind (the caller
    /// diagnoses a non-dim kind with a precise message); zero rows → None
    /// (unregistered), more than one (same table name under two dbs of one
    /// datasource) → error (ambiguous).
    pub fn table_object_for_dim_reference(&self, reference: &str) -> Result<Option<TableObject>> {
        let Some((ds_part, table)) = reference.split_once('.') else {
            return Ok(None);
        };
        let Ok(ds_id) = ds_part.parse::<i64>() else {
            return Ok(None);
        };
        if table.is_empty() || table.contains('.') {
            return Ok(None);
        }
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, datasource_id, db_name, table_name, kind, format, cursor_semantics, source, sequence_column \
                 FROM table_object WHERE datasource_id = ?1 AND table_name = ?2 ORDER BY id",
            )?;
            let mut rows = stmt.query(rusqlite::params![ds_id, table])?;
            let mut hits: Vec<TableObject> = Vec::new();
            while let Some(row) = rows.next()? {
                hits.push(table_from_row(row)?);
            }
            match hits.len() {
                0 => Ok(None),
                1 => Ok(hits.into_iter().next()),
                n => anyhow::bail!(
                    "ambiguous dim reference {reference}: {n} table objects named {table:?} \
                     under datasource {ds_id} — dim references carry no db segment, give the \
                     tables distinct names"
                ),
            }
        })
    }

    /// Full columns (registration form round-trip): + columns_json.
    pub fn table_object_columns_json(&self, id: i64) -> Result<Option<Option<String>>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT columns_json FROM table_object WHERE id = ?1")?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(row.get(0)?)),
                None => Ok(None),
            }
        })
    }

    pub fn list_table_objects(
        &self,
        datasource_id: Option<i64>,
        kind: Option<&str>,
    ) -> Result<Vec<TableObject>> {
        self.with_conn(|conn| {
            let mut sql = String::from(
                "SELECT id, datasource_id, db_name, table_name, kind, format, cursor_semantics, source, sequence_column \
                 FROM table_object WHERE 1=1",
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            if let Some(id) = datasource_id {
                sql.push_str(&format!(" AND datasource_id = ?{}", params.len() + 1));
                params.push(Box::new(id));
            }
            if let Some(k) = kind {
                sql.push_str(&format!(" AND kind = ?{}", params.len() + 1));
                params.push(Box::new(k.to_string()));
            }
            sql.push_str(" ORDER BY id");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(
                params.iter().map(|p| p.as_ref()).collect::<Vec<_>>().as_slice(),
                table_from_row,
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Pipelines whose published `spec_json` references this table object
    /// (the §4.2 tables delete guard: "被已发布管道引用 409"). Reference form
    /// is `${ds:<dsId>.<db>.<table>}` — except dim tables, which the SQL
    /// references as `${ds:dim.<dsId>.<table>}` (no db segment; T-D1);
    /// matched with instr (no LIKE wildcard hazards from `_` in names).
    pub fn pipelines_referencing_table(&self, table: &TableObject) -> Result<Vec<i64>> {
        let needle = if table.kind == "dim" {
            format!("${{ds:dim.{}.{}}}", table.datasource_id, table.table_name)
        } else {
            format!(
                "${{ds:{}.{}.{}}}",
                table.datasource_id, table.db_name, table.table_name
            )
        };
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id FROM pipeline WHERE spec_json IS NOT NULL AND instr(spec_json, ?1) > 0 ORDER BY id",
            )?;
            let rows = stmt.query_map([needle], |r| r.get::<_, i64>(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub fn delete_table_object(&self, id: i64) -> Result<bool> {
        self.with_conn(|conn| Ok(conn.execute("DELETE FROM table_object WHERE id = ?1", [id])? > 0))
    }
}

fn table_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TableObject> {
    Ok(TableObject {
        id: row.get(0)?,
        datasource_id: row.get(1)?,
        db_name: row.get(2)?,
        table_name: row.get(3)?,
        kind: row.get(4)?,
        format: row.get(5)?,
        cursor_semantics: row.get(6)?,
        source: row.get(7)?,
        sequence_column: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datasource_and_credential_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let id = store
            .insert_datasource("edge-kafka", "kafka", r#"{"bootstrap":"10.1.1.5:9092"}"#)
            .unwrap();
        let ds = store.datasource(id).unwrap().unwrap();
        assert_eq!(ds.name, "edge-kafka");
        assert_eq!(ds.status, "CREATED");
        assert_eq!(ds.generation, 1);
        assert_eq!(store.list_datasources(None).unwrap().len(), 1);
        assert_eq!(store.list_datasources(Some("doris")).unwrap().len(), 0);

        assert!(store.credential(id).unwrap().is_none());
        store.upsert_credential(id, "nonce12", "ct-xyz").unwrap();
        store.upsert_credential(id, "nonce12b", "ct-xyz2").unwrap(); // idempotent upsert
        let cred = store.credential(id).unwrap().unwrap();
        assert_eq!(cred.nonce, "nonce12b");

        let t = store
            .insert_table_object(id, "shop", "orders", "kafka", Some("ogg"), Some("[]"), None)
            .unwrap();
        let to = store.table_object(t).unwrap().unwrap();
        assert_eq!(to.table_name, "orders");
        assert_eq!(to.cursor_semantics, "KAFKA_OFFSET");
        assert_eq!(to.source, "manual");
        // jdbc derivation (T14 ← R3-W1 low#4): IR Cursor::JdbcKey — the
        // mysql/postgresql batch sources register kind "jdbc" table objects
        let jdbc_t = store
            .insert_table_object(
                id,
                "shop",
                "mysql_src",
                "jdbc",
                Some("json"),
                None,
                Some("id"),
            )
            .unwrap();
        assert_eq!(
            store
                .table_object(jdbc_t)
                .unwrap()
                .unwrap()
                .cursor_semantics,
            "JDBC_KEY"
        );
        assert_eq!(
            store.table_object(jdbc_t).unwrap().unwrap().sequence_column,
            Some("id".into()),
            "sequence_column round-trips (T-D1)"
        );
        // sink kinds stay cursor-less
        let doris_t = store
            .insert_table_object(id, "ods", "orders", "doris", None, None, None)
            .unwrap();
        assert_eq!(
            store
                .table_object(doris_t)
                .unwrap()
                .unwrap()
                .cursor_semantics,
            "NONE"
        );
        assert_eq!(
            store
                .table_object_by_ref(id, "shop", "orders")
                .unwrap()
                .unwrap()
                .id,
            t
        );
        // duplicate (datasource, db, table) rejected
        assert!(store
            .insert_table_object(id, "shop", "orders", "kafka", None, None, None)
            .is_err());
        // kafka + jdbc + doris registrations above
        assert_eq!(store.table_count(id).unwrap(), 3);
        assert_eq!(
            store
                .list_table_objects(Some(id), Some("kafka"))
                .unwrap()
                .len(),
            1
        );

        // status write-back (§4.2 testById)
        store.set_datasource_status(id, "CONN_OK").unwrap();
        assert_eq!(store.datasource(id).unwrap().unwrap().status, "CONN_OK");

        // cascade: datasource delete removes credential + table objects
        assert!(store.delete_datasource(id).unwrap());
        assert!(store.credential(id).unwrap().is_none());
        assert!(store.table_object(t).unwrap().is_none());
    }

    #[test]
    fn datasource_update_cas() {
        let store = Store::open_in_memory().unwrap();
        let id = store
            .insert_datasource("a", "kafka", r#"{"bootstrap":"h:9092"}"#)
            .unwrap();
        // wrong generation -> conflict
        assert_eq!(
            store
                .update_datasource(id, "a", "kafka", r#"{"bootstrap":"h2:9092"}"#, 9, None)
                .unwrap(),
            DatasourceUpdate::GenerationConflict
        );
        // name owned by another row
        let other = store.insert_datasource("b", "kafka", "{}").unwrap();
        assert_eq!(
            store
                .update_datasource(id, "b", "kafka", "{}", 1, None)
                .unwrap(),
            DatasourceUpdate::NameConflict
        );
        let _ = other;
        // happy path bumps generation
        assert_eq!(
            store
                .update_datasource(
                    id,
                    "a2",
                    "kafka",
                    r#"{"bootstrap":"h2:9092"}"#,
                    1,
                    Some(("n", "c"))
                )
                .unwrap(),
            DatasourceUpdate::Applied(2)
        );
        let ds = store.datasource(id).unwrap().unwrap();
        assert_eq!((ds.name.as_str(), ds.generation), ("a2", 2));
        assert_eq!(store.credential(id).unwrap().unwrap().ciphertext, "c");
        assert_eq!(
            store
                .update_datasource(999, "x", "kafka", "{}", 1, None)
                .unwrap(),
            DatasourceUpdate::NotFound
        );
    }

    /// T-D1 dim reference resolution ("dsId.table", the form the parser
    /// emits for `${ds:dim.<datasourceId>.<table>}`): resolves across dbs
    /// (the JDBC URL pins the database), None for unregistered / malformed,
    /// error when the same table name sits under two dbs of one datasource.
    /// Also locks the dim delete-guard needle (`${ds:dim.<dsId>.<table>}`).
    #[test]
    fn dim_reference_resolution_and_guard_needle() {
        let store = Store::open_in_memory().unwrap();
        let ds = store
            .insert_datasource(
                "shop-mysql",
                "mysql",
                r#"{"jdbcUrl":"jdbc:mysql://127.0.0.1:3306/shop"}"#,
            )
            .unwrap();
        let dim = store
            .insert_table_object(
                ds,
                "shop",
                "dim_user",
                "dim",
                None,
                Some(r#"[["id","int64"]]"#),
                Some("id"),
            )
            .unwrap();
        // resolves; carries kind + sequence_column
        let hit = store
            .table_object_for_dim_reference(&format!("{ds}.dim_user"))
            .unwrap()
            .unwrap();
        assert_eq!((hit.id, hit.kind.as_str()), (dim, "dim"));
        assert_eq!(hit.sequence_column.as_deref(), Some("id"));
        // unregistered / malformed / non-numeric ds → None
        assert!(store
            .table_object_for_dim_reference(&format!("{ds}.no_such"))
            .unwrap()
            .is_none());
        assert!(store
            .table_object_for_dim_reference("no_such.table")
            .unwrap()
            .is_none());
        assert!(store
            .table_object_for_dim_reference("only-one-part")
            .unwrap()
            .is_none());
        // a same-named table under a second db of the same datasource → ambiguous
        store
            .insert_table_object(ds, "other", "dim_user", "dim", None, None, Some("id"))
            .unwrap();
        let err = store
            .table_object_for_dim_reference(&format!("{ds}.dim_user"))
            .unwrap_err();
        assert!(err.to_string().contains("ambiguous"), "{err}");
        // delete guard: the dim needle matches the ${ds:dim.<dsId>.<table>} form
        store
            .create_pipeline("dim-join", "SELECT 1", Some("{}"), 10000, None, None)
            .unwrap();
        let spec = format!("INSERT INTO ${{ds:9.o.w}} SELECT s.id FROM ${{ds:8.s.i}} s LEFT JOIN ${{ds:dim.{ds}.dim_user}} d ON s.id = d.id");
        store
            .set_pipeline_published(1, &spec, "raw", "admin")
            .unwrap();
        let table = store.table_object(dim).unwrap().unwrap();
        assert_eq!(
            store.pipelines_referencing_table(&table).unwrap(),
            vec![1],
            "dim tables guard on the ${{ds:dim.<dsId>.<table>}} needle"
        );
    }
}
