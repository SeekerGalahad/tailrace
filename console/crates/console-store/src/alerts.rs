//! Alert repository (T-E2, `alert_rule` + `alert_event` tables): rule CRUD
//! plus the episode state machine the console-side evaluator drives. The
//! transition writers below are the ONLY writers of `alert_rule.state` and
//! of `alert_event` rows; each runs in one `with_tx` so a rule's state and
//! its episode rows can never disagree.
//!
//! Episodes are PER SERIES (R9 review fix): one open (`firing`) row per
//! `(rule_id, series_key)` — a rule matching three pipelines keeps three
//! independent episodes, each with its own firing/resolved notice.
//! `alert_rule.state` is the rule-level aggregate: `firing` while any
//! episode of the rule is open, `inactive` once the last one resolves.
//! A row is inserted on a series' inactive → firing, flipped to `resolved`
//! on the way back; ticks that keep a series firing touch its open row
//! (last_seen_at / sample) and never add a row — the anti-flood contract the
//! API surface relies on.

use anyhow::Result;

use super::models::{AlertEvent, AlertRule};
use super::Store;

const RULE_COLS: &str =
    "id, name, rule_type, params_json, enabled, webhook_url, state, created_at, updated_at";
const EVENT_COLS: &str = "id, rule_id, series_key, state, first_seen_at, last_seen_at, \
                          resolved_at, sample_json, resolved_sample_json";

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
        series_key: row.get(2)?,
        state: row.get(3)?,
        first_seen_at: row.get(4)?,
        last_seen_at: row.get(5)?,
        resolved_at: row.get(6)?,
        sample_json: row.get(7)?,
        resolved_sample_json: row.get(8)?,
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

/// `alert_rule.state` ← aggregate of the rule's open episodes (same
/// connection/transaction as the caller's episode write).
fn sync_rule_state(conn: &rusqlite::Connection, rule_id: i64) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE alert_rule SET state = CASE WHEN EXISTS(\
             SELECT 1 FROM alert_event WHERE rule_id = ?1 AND state = 'firing') \
         THEN 'firing' ELSE 'inactive' END WHERE id = ?1",
        [rule_id],
    )?;
    Ok(())
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
    /// open episodes keep running under the new parameters and resolve
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

    /// Delete a rule; its episode rows go with it (ON DELETE CASCADE). The
    /// API layer closes any open episode (resolved, reason `deleted`) and
    /// delivers the notices BEFORE calling this, so a receiver never keeps
    /// a firing alert for a rule that no longer exists.
    pub fn delete_alert_rule(&self, id: i64) -> Result<bool> {
        self.with_conn(|conn| Ok(conn.execute("DELETE FROM alert_rule WHERE id = ?1", [id])? > 0))
    }

    /// The rule's open (state=firing) episodes, oldest first — one per
    /// series by construction of the transition writers.
    pub fn open_alert_episodes(&self, rule_id: i64) -> Result<Vec<AlertEvent>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {EVENT_COLS} FROM alert_event WHERE rule_id = ?1 AND state = 'firing' \
                 ORDER BY id"
            ))?;
            let rows = stmt.query_map([rule_id], event_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// The rule's oldest open episode, if any (the `openEvent` the API
    /// shows first; `open_alert_episodes` has them all).
    pub fn open_alert_episode(&self, rule_id: i64) -> Result<Option<AlertEvent>> {
        Ok(self.open_alert_episodes(rule_id)?.into_iter().next())
    }

    /// A series' inactive → firing: insert its episode row and mark the
    /// rule firing, atomically. Returns the new event id.
    pub fn begin_alert_episode(
        &self,
        rule_id: i64,
        series_key: &str,
        sample_json: &str,
    ) -> Result<i64> {
        self.with_tx(|conn| {
            conn.execute(
                "INSERT INTO alert_event (rule_id, series_key, state, sample_json) \
                 VALUES (?1, ?2, 'firing', ?3)",
                rusqlite::params![rule_id, series_key, sample_json],
            )?;
            let id = conn.last_insert_rowid();
            conn.execute(
                "UPDATE alert_rule SET state = 'firing' WHERE id = ?1",
                [rule_id],
            )?;
            Ok(id)
        })
    }

    /// firing → firing: bump an open episode's last_seen_at and latest
    /// sample. No new row. Ok(false) = no such open episode (nothing touched).
    pub fn touch_alert_episode(&self, event_id: i64, sample_json: &str) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "UPDATE alert_event SET last_seen_at = datetime('now'), sample_json = ?2 \
                 WHERE id = ?1 AND state = 'firing'",
                rusqlite::params![event_id, sample_json],
            )? > 0)
        })
    }

    /// Give a pre-migration-05 episode (`series_key = ''`) the identity of
    /// the series the evaluator now sees firing for its rule. Ok(false) =
    /// the row is not an open legacy episode.
    pub fn adopt_alert_episode(&self, event_id: i64, series_key: &str) -> Result<bool> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "UPDATE alert_event SET series_key = ?2 \
                 WHERE id = ?1 AND state = 'firing' AND series_key = ''",
                rusqlite::params![event_id, series_key],
            )? > 0)
        })
    }

    /// A series' firing → inactive: close its episode (state=resolved,
    /// resolved_at, resolving sample) and re-derive the rule state from the
    /// episodes still open, atomically. Ok(false) = the row was not an open
    /// episode (the rule state is still re-derived, so a stray `firing` flag
    /// can never outlive its episodes).
    pub fn resolve_alert_episode(&self, event_id: i64, sample_json: &str) -> Result<bool> {
        self.with_tx(|conn| {
            let rule_id: Option<i64> = conn
                .query_row(
                    "SELECT rule_id FROM alert_event WHERE id = ?1",
                    [event_id],
                    |r| r.get(0),
                )
                .ok();
            let closed = conn.execute(
                "UPDATE alert_event SET state = 'resolved', resolved_at = datetime('now'), \
                 last_seen_at = datetime('now'), resolved_sample_json = ?2 \
                 WHERE id = ?1 AND state = 'firing'",
                rusqlite::params![event_id, sample_json],
            )?;
            if let Some(rule_id) = rule_id {
                sync_rule_state(conn, rule_id)?;
            }
            Ok(closed > 0)
        })
    }

    /// Re-derive `alert_rule.state` from the rule's open episodes (the
    /// evaluator calls it once per rule per pass as a belt-and-braces
    /// against a flag left behind by an older writer).
    pub fn sync_alert_rule_state(&self, rule_id: i64) -> Result<()> {
        self.with_conn(|conn| Ok(sync_rule_state(conn, rule_id)?))
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

    const S1: &str = "tailrace_pipeline_lag_seconds{node=\"a\",pipeline=\"1\",statement=\"0\"}";
    const S2: &str = "tailrace_pipeline_lag_seconds{node=\"a\",pipeline=\"2\",statement=\"0\"}";

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
        store.begin_alert_episode(a, S1, r#"{"lag":99}"#).unwrap();
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
        // touching / resolving a row that does not exist is a no-op (Ok(false))
        assert!(!store.touch_alert_episode(999, "{}").unwrap());
        assert!(!store.resolve_alert_episode(999, "{}").unwrap());
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "inactive");

        let ev = store.begin_alert_episode(id, S1, r#"{"lag":45}"#).unwrap();
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "firing");
        let open = store.open_alert_episode(id).unwrap().unwrap();
        assert_eq!((open.id, open.state.as_str()), (ev, "firing"));
        assert_eq!(open.series_key, S1);
        assert_eq!(open.sample_json, r#"{"lag":45}"#);
        assert_eq!(open.resolved_at, None);

        // five more firing ticks: still exactly one row, latest sample kept
        for lag in [46, 47, 48, 49, 50] {
            assert!(store
                .touch_alert_episode(ev, &format!(r#"{{"lag":{lag}}}"#))
                .unwrap());
        }
        let events = store.list_alert_events_for_rule(id, 10).unwrap();
        assert_eq!(events.len(), 1, "touch must not add rows");
        assert_eq!(events[0].sample_json, r#"{"lag":50}"#);

        // resolve: same row flips, resolving sample recorded, rule inactive
        assert!(store.resolve_alert_episode(ev, r#"{"lag":3}"#).unwrap());
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "inactive");
        assert!(store.open_alert_episode(id).unwrap().is_none());
        // resolving twice is a no-op
        assert!(!store.resolve_alert_episode(ev, r#"{"lag":3}"#).unwrap());
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
        let ev2 = store.begin_alert_episode(id, S1, r#"{"lag":70}"#).unwrap();
        assert_ne!(ev2, ev);
        let events = store.list_alert_events_for_rule(id, 10).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events.iter().filter(|e| e.state == "firing").count(), 1);
        assert_eq!(events.iter().filter(|e| e.state == "resolved").count(), 1);
        // cross-rule listing, newest first, honours the limit
        let other = rule(&store, "other");
        store.begin_alert_episode(other, S1, "{}").unwrap();
        let all = store.list_alert_events(2).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].rule_id, other, "newest first");
    }

    /// Per-series episodes (R9 review fix): two series of one rule keep two
    /// open rows; resolving one leaves the rule firing, resolving the last
    /// flips it inactive; a legacy '' row is adopted by a series.
    #[test]
    fn episodes_are_per_series_and_rule_state_aggregates() {
        let store = Store::open_in_memory().unwrap();
        let id = rule(&store, "lag");
        let e1 = store.begin_alert_episode(id, S1, "{}").unwrap();
        let e2 = store.begin_alert_episode(id, S2, "{}").unwrap();
        let open = store.open_alert_episodes(id).unwrap();
        assert_eq!(
            open.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![e1, e2],
            "oldest first"
        );
        assert_eq!(open[0].series_key, S1);
        assert_eq!(open[1].series_key, S2);
        assert_eq!(store.open_alert_episode(id).unwrap().unwrap().id, e1);

        assert!(store.resolve_alert_episode(e1, "{}").unwrap());
        assert_eq!(
            store.alert_rule(id).unwrap().unwrap().state,
            "firing",
            "one series still open keeps the rule firing"
        );
        assert_eq!(store.open_alert_episodes(id).unwrap().len(), 1);
        assert!(store.resolve_alert_episode(e2, "{}").unwrap());
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "inactive");
        assert!(store.open_alert_episodes(id).unwrap().is_empty());

        // legacy row (pre-05 shape) is adopted once, by one series only
        let legacy = store.begin_alert_episode(id, "", "{}").unwrap();
        assert!(store.adopt_alert_episode(legacy, S2).unwrap());
        assert!(!store.adopt_alert_episode(legacy, S1).unwrap());
        assert_eq!(
            store.open_alert_episode(id).unwrap().unwrap().series_key,
            S2
        );
        // a stray flag is re-derived from the rows
        store
            .with_conn(|c| {
                c.execute(
                    "UPDATE alert_event SET state = 'resolved' WHERE id = ?1",
                    [legacy],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "firing");
        store.sync_alert_rule_state(id).unwrap();
        assert_eq!(store.alert_rule(id).unwrap().unwrap().state, "inactive");
    }
}
