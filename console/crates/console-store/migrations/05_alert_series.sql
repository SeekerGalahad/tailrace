-- R9 review fix: alert episodes are kept PER SERIES, not per rule. A rule
-- without filters matches every pipeline; before this column one episode
-- stood for all of them, so a second pipeline going bad while the first was
-- already firing produced no notice, and the resolving sample could belong
-- to a different series than the firing one. `series_key` is the evaluator's
-- rendered series identity (`metric{label="value",…}`, labels sorted, the
-- same key the in-memory counter history uses); at most one open (firing)
-- row exists per (rule_id, series_key). alert_rule.state stays the
-- rule-level aggregate: 'firing' while ANY episode of the rule is open.
--
-- Rows written before this migration carry '' (unknown series) and are
-- adopted by the first series the evaluator sees firing, or closed when the
-- rule evaluates clear — never silently dropped.
ALTER TABLE alert_event ADD COLUMN series_key TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_alert_event_open ON alert_event(rule_id, state, series_key);
