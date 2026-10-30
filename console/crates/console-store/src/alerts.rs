//! Alert repository (T-E2, `alert_rule` + `alert_event` tables): rule CRUD
//! plus the episode state machine the console-side evaluator drives. The
//! three transition writers below are the ONLY writers of `alert_rule.state`
//! and of `alert_event` rows; each runs in one `with_tx` so a rule's state
//! flip and its episode row can never disagree. An episode row is inserted
//! on inactive → firing and flipped to `resolved` on firing → inactive;
//! ticks that keep a rule firing touch the open row (last_seen_at / sample)
//! and never add a row — the anti-flood contract the API surface relies on.

use anyhow::Result;

use super::models::{AlertEvent, AlertRule};
use super::Store;

const RULE_COLS: &str =
    "id, name, rule_type, params_json, enabled, webhook_url, state, created_at, updated_at";
const EVENT_COLS: &str = "id, rule_id, state, first_seen_at, last_seen_at, resolved_at, \
                          sample_json, resolved_sample_json";

fn rule_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AlertRule> {
    Ok(AlertRule {
        id: row.get(0)?,
        name: row.get(1)?,
        rule_type: row.get(2)?,
        params_json: row.get(3)?,
        enabled: row.get::<_, i64>(4)? != 0,
        webhook_url: row.get(5)?,
        state: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AlertEvent> {
    Ok(AlertEvent {
        id: row.get(0)?,
        rule_id: row.get(1)?,
        state: row.get(2)?,
        first_seen_at: row.get(3)?,
        last_seen_at: row.get(4)?,
        resolved_at: row.get(5)?,
        sample_json: row.get(6)?,
        resolved_sample_json: row.get(7)?,
    })
}

/// Create/update outcome: rule names are UNIQUE (409 on collision); update
/// of a missing id is `NotFound` (404).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertRuleWrite {
    Written(i64),
    NameConflict,
    NotFound,
}

impl Store {
    /// All rules, oldest first (the `GET /api/alerts` rule list).
    pub fn list_alert_rules(&self) -> Result<Vec<AlertRule>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare(&format!("SELECT {RULE_COLS} FROM alert_rule ORDER BY id"))?;
            let rows = stmt.query_map([], rule_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub fn alert_rule(&self, id: i64) -> Result<Option<AlertRule>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare(&format!("SELECT {RULE_COLS} FROM alert_rule WHERE id = ?1"))?;
            let mut rows = stmt.query([id])?;
            match rows.next()? {
                Some(row) => Ok(Some(rule_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    /// Insert a rule in state `inactive`. `params_json` is validated by the
    /// API layer (type-dependent shape); the store keeps it verbatim.
    pub fn create_alert_rule(
        &self,
        name: &str,
        rule_type: &str,
        params_json: &str,
        enabled: bool,
        webhook_url: Option<&str>,
    ) -> Result<AlertRuleWrite> {
        self.with_conn(|conn| {
            let dup: i64 = conn.query_row(
                "SELECT COUNT(*) FROM alert_rule WHERE name = ?1",
                [name],
                |r| r.get(0),
            )?;
            if dup > 0 {
                return Ok(AlertRuleWrite::NameConflict);
            }
            conn.execute(
                "INSERT INTO alert_rule (name, rule_type, params_json, enabled, webhook_url) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![name, rule_type, params_json, enabled as i64, webhook_url],
            )?;
            Ok(AlertRuleWrite::Written(conn.last_insert_rowid()))
        })
    }

    /// Replace every user-editable field of a rule. `state` is untouched —
    /// an open episode keeps running under the new parameters and resolves
    /// (or not) on the next evaluation, so an edit never silently drops a
    /// firing alert.
    pub fn update_alert_rule(
        &self,
        id: i64,
        name: &str,
        rule_type: &str,
        params_json: &str,
        enabled: bool,
        webhook_url: Option<&str>,
    ) -> Result<AlertRuleWrite> {
        self.with_conn(|conn| {
            let exists: i64 =
                conn.query_row("SELECT COUNT(*) FROM alert_rule WHERE id = ?1", [id], |r| {
                    r.get(0)
                })?;
            if exists == 0 {
                return Ok(AlertRuleWrite::NotFound);
            }
            let dup: i64 = conn.query_row(
                "SELECT COUNT(*) FROM alert_rule WHERE name = ?1 AND id <> ?2",
                rusqlite::params![name, id],
                |r| r.get(0),
            )?;
            if dup > 0 {
                return Ok(AlertRuleWrite::NameConflict);
            }
            conn.execute(
                "UPDATE alert_rule SET name = ?2, rule_type = ?3, params_json = ?4, enabled = ?5, \
                 webhook_url = ?6, updated_at = datetime('now') WHERE id = ?1",
                rusqlite::params![
                    id,
                    name,
                    rule_type,
                    params_json,
                    enabled as i64,
                    webhook_url
                ],
            )?;
            Ok(AlertRuleWrite::Written(id))
        })
    }

    /// Delete a rule; its episodes go with it (ON DELETE CASCADE).
    pub fn delete_alert_rule(&self, id: i64) -> Result<bool> {
        self.with_conn(|conn| Ok(conn.execute("DELETE FROM alert_rule WHERE id = ?1", [id])? > 0))
    }

    /// The rule's open (state=firing) episode, if any — at most one exists
    /// by construction of the transition writers.
    pub fn open_alert_episode(&self, rule_id: i64) -> Result<Option<AlertEvent>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {EVENT_COLS} FROM alert_event WHERE rule_id = ?1 AND state = 'firing' \
                 ORDER BY id DESC LIMIT 1"
            ))?;
            let mut rows = stmt.query([rule_id])?;
            match rows.next()? {
                Some(row) => Ok(Some(event_from_row(row)?)),
                None => Ok(None),
            }
        })
    }

    /// inactive → firing: insert the episode row and flip the rule state,
    /// atomically. Returns the new event id.
    pub fn begin_alert_episode(&self, rule_id: i64, sample_json: &str) -> Result<i64> {
        self.with_tx(|conn| {
            conn.execute(
                "INSERT INTO alert_event (rule_id, state, sample_json) VALUES (?1, 'firing', ?2)",
                rusqlite::params![rule_id, sample_json],
            )?;
            let id = conn.last_insert_rowid();
            conn.execute(
                "UPDATE alert_rule SET state = 'firing' WHERE id = ?1",
                [rule_id],
            )?;
            Ok(id)
        })
    }

    /// firing → firing: bump the open episode's last_seen_at and latest
    /// sample. No new row. Ok(false) = no open episode (nothing touched).
    pub fn touch_alert_episode(&self, rule_id: i64, sample_json: &str) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "UPDATE alert_event SET last_seen_at = datetime('now'), sample_json = ?2 \
                 WHERE rule_id = ?1 AND state = 'firing'",
                rusqlite::params![rule_id, sample_json],
            )? > 0)
        })
    }

    /// firing → inactive: close the open episode (state=resolved, resolved_at,
    /// resolving sample) and flip the rule state, atomically. Ok(false) = the
    /// rule had no open episode (state is still forced to inactive so a
    /// stray `firing` flag can never outlive its episode).
    pub fn resolve_alert_episode(&self, rule_id: i64, sample_json: &str) -> Result<bool> {
        self.with_tx(|conn| {
            let closed = conn.execute(
                "UPDATE alert_event SET state = 'resolved', resolved_at = datetime('now'), \
                 last_seen_at = datetime('now'), resolved_sample_json = ?2 \
                 WHERE rule_id = ?1 AND state = 'firing'",
                rusqlite::params![rule_id, sample_json],
            )?;
            conn.execute(
                "UPDATE alert_rule SET state = 'inactive' WHERE id = ?1",
                [rule_id],
            )?;
            Ok(closed > 0)
        })
    }

    /// Most recent episodes across all rules, newest first (the
    /// `GET /api/alerts` events block).
    pub fn list_alert_events(&self, limit: usize) -> Result<Vec<AlertEvent>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {EVENT_COLS} FROM alert_event ORDER BY first_seen_at DESC, id DESC LIMIT ?1"
            ))?;
            let rows = stmt.query_map([limit as i64], event_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Episodes of one rule, newest first.
    pub fn list_alert_events_for_rule(
        &self,
        rule_id: i64,
        limit: usize,
    ) -> Result<Vec<AlertEvent>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {EVENT_COLS} FROM alert_event WHERE rule_id = ?1 \
                 ORDER BY first_seen_at DESC, id DESC LIMIT ?2"
            ))?;
            let rows = stmt.query_map(rusqlite::params![rule_id, limit as i64], event_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(store: &Store, name: &str) -> i64 {
        match store
            .create_alert_rule(name, "lag_seconds", r#"{"threshold":30}"#, true, None)
            .unwrap()
        {
            AlertRuleWrite::Written(id) => id,
            other => panic!("{other:?}"),
        }
    }

    /// CRUD: unique names (create and update), update keeps state, delete
    /// cascades the episodes.
    #[test]
    fn rule_crud_and_name_uniqueness() {
        let store = Store::open_in_memory().unwrap();
        let a = rule(&store, "lag-a");
        let b = rule(&store, "lag-b");
        assert_eq!(
            store
                .create_alert_rule("lag-a", "state_failed", "{}", true, None)
                .unwrap(),
            AlertRuleWrite::NameConflict
        );
        assert_eq!(
            store
                .update_alert_rule(b, "lag-a", "lag_seconds", "{}", true, None)
                .unwrap(),
            AlertRuleWrite::NameConflict,
            "rename onto a taken name is a conflict"
        );
        assert_eq!(
            store
                .update_alert_rule(999, "x", "lag_seconds", "{}", true, None)
                .unwrap(),
            AlertRuleWrite::NotFound
        );
        let r = store.alert_rule(a).unwrap().unwrap();
        assert_eq!(
            (
                r.name.as_str(),
                r.rule_type.as_str(),
                r.enabled,
                r.state.as_str()
            ),
            ("lag-a", "lag_seconds", true, "inactive")
        );
        assert_eq!(r.webhook_url, None);

        // firing, then edit: state survives the edit (an edit must not drop
        // a firing alert on the floor)
        store.begin_alert_episode(a, r#"{"lag":99}"#).unwrap();
        assert_eq!(
            store
                .update_alert_rule(
                    a,
                    "lag-a2",
                    "lag_seconds",
                    r#"{"threshold":60}"#,
                    false,
                    Some("http://127.0.0.1:1/hook")
                )
                .unwrap(),
            AlertRuleWrite::Written(a)
        );
        let r = store.alert_rule(a).unwrap().unwrap();
        assert_eq!(r.name, "lag-a2");
        assert!(!r.enabled);
        assert_eq!(r.webhook_url.as_deref(), Some("http://127.0.0.1:1/hook"));
        assert_eq!(r.state, "firing", "update keeps the evaluator state");
        assert_eq!(store.list_alert_rules().unwrap().len(), 2);

        // delete cascades the episode rows
        assert_eq!(store.list_alert_events_for_rule(a, 10).unwrap().len(), 1);
        assert!(store.delete_alert_rule(a).unwrap());
        assert!(!store.delete_alert_rule(a).unwrap());
        assert!(store.list_alert_events_for_rule(a, 10).unwrap().is_empty());
        assert_eq!(store.list_alert_rules().unwrap().len(), 1);
    }

    /// The episode state machine: begin inserts one row + flips the rule,
    /// touch never adds a row, resolve closes the row + flips back, a second
    /// firing opens a second row.
    #[test]
    fn episode_rows_land_only_on_transitions() {
        let store = Store::open_in_memory().unwrap();
        let id = rule(&store, "lag");
        assert!(store.open_alert_episode(id).unwrap().is_none());
        // touching / resolving with nothing open is a no-op (Ok(false))
        assert!(!store.touch_alert_episode(id, "{}").unwrap());
        assert!(!store.resolve_alert_episode(id, "{}").unwrap());
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "inactive");

        let ev = store.begin_alert_episode(id, r#"{"lag":45}"#).unwrap();
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "firing");
        let open = store.open_alert_episode(id).unwrap().unwrap();
        assert_eq!((open.id, open.state.as_str()), (ev, "firing"));
        assert_eq!(open.sample_json, r#"{"lag":45}"#);
        assert_eq!(open.resolved_at, None);

        // five more firing ticks: still exactly one row, latest sample kept
        for lag in [46, 47, 48, 49, 50] {
            assert!(store
                .touch_alert_episode(id, &format!(r#"{{"lag":{lag}}}"#))
                .unwrap());
        }
        let events = store.list_alert_events_for_rule(id, 10).unwrap();
        assert_eq!(events.len(), 1, "touch must not add rows");
        assert_eq!(events[0].sample_json, r#"{"lag":50}"#);

        // resolve: same row flips, resolving sample recorded, rule inactive
        assert!(store.resolve_alert_episode(id, r#"{"lag":3}"#).unwrap());
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "inactive");
        assert!(store.open_alert_episode(id).unwrap().is_none());
        let events = store.list_alert_events_for_rule(id, 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, "resolved");
        assert!(events[0].resolved_at.is_some());
        assert_eq!(
            events[0].resolved_sample_json.as_deref(),
            Some(r#"{"lag":3}"#)
        );
        assert_eq!(
            events[0].sample_json, r#"{"lag":50}"#,
            "firing-side sample kept"
        );

        // a second firing is a second row; the resolved one is immutable
        let ev2 = store.begin_alert_episode(id, r#"{"lag":70}"#).unwrap();
        assert_ne!(ev2, ev);
        let events = store.list_alert_events_for_rule(id, 10).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events.iter().filter(|e| e.state == "firing").count(), 1);
        assert_eq!(events.iter().filter(|e| e.state == "resolved").count(), 1);
        // cross-rule listing, newest first, honours the limit
        let other = rule(&store, "other");
        store.begin_alert_episode(other, "{}").unwrap();
        let all = store.list_alert_events(2).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].rule_id, other, "newest first");
    }
}
