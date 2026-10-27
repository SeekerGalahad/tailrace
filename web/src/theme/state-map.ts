/**
 * Status/type → color single-source table (design doc §6.6.5).
 *
 * StateTag is the ONLY consumer of this module. Pages must never pick their
 * own tag colors. Unknown values fall back to `neutral`.
 */

import type { Kind } from './tokens'

export type StateDomain =
  | 'pipeline' // pipeline status
  | 'node' // node status
  | 'deploy' // deploy 8-step / deploy-log step status
  | 'datasource' // datasource conn status
  | 'eventLevel' // node event severity
  | 'startOffset' // pipeline start point
  | 'backfillJob' // T-A2 backfill job state machine

/** Naive-ui tag semantic slot; `neutral` renders a bordered gray tag. */
export type TagTone = 'primary' | 'info' | 'success' | 'warning' | 'error' | 'neutral'

const pipeline: Record<string, TagTone> = {
  RUNNING: 'success',
  PAUSED: 'warning',
  FAILED: 'error',
  DEPLOY_FAILED: 'error',
  PUBLISHED: 'info',
  PENDING_CHANGES: 'primary',
  DRAFT: 'neutral',
}

const node: Record<string, TagTone> = {
  ONLINE: 'success',
  OFFLINE: 'neutral',
  REGISTERED: 'info',
}

// Deploy 8-step / log step states (§4.4): REGISTERED/OK → success; FAIL →
// error; in-progress → primary (set `active` on the running step, see
// StateTag); PENDING / not started → neutral.
const deploy: Record<string, TagTone> = {
  PENDING: 'neutral',
  OK: 'success',
  REGISTERED: 'success',
  FAIL: 'error',
  ACTIVE: 'primary',
}

const datasource: Record<string, TagTone> = {
  CONN_OK: 'success',
  CONN_FAIL: 'error',
  CREATED: 'neutral',
}

const eventLevel: Record<string, TagTone> = {
  ERROR: 'error',
  WARN: 'warning',
  INFO: 'neutral',
}

const startOffset: Record<string, TagTone> = {
  earliest: 'info', // from the beginning
  latest: 'warning', // skips backlog
}

// Backfill job states (T-A2, console-side executor): pending → running →
// done | failed; terminal rows are immutable. `running` takes the deploy
// in-progress tone.
const backfillJob: Record<string, TagTone> = {
  pending: 'warning',
  running: 'primary',
  done: 'success',
  failed: 'error',
}

const tables: Record<StateDomain, Record<string, TagTone>> = {
  pipeline,
  node,
  deploy,
  datasource,
  eventLevel,
  startOffset,
  backfillJob,
}

/** Resolve a state value to its tone; unknown → neutral. */
export function stateTone(domain: StateDomain, value: string | null | undefined): TagTone {
  if (value == null) return 'neutral'
  return tables[domain][value] ?? 'neutral'
}

/** Is `value` a known state of `domain`? (lets pages hide tags on null fields) */
export function isKnownState(domain: StateDomain, value: string | null | undefined): boolean {
  if (value == null) return false
  return value in tables[domain]
}

/** Kind colors come from the §6.6.1 type palette, reused via StateTag domain="kind". */
export function isKind(value: string | null | undefined): value is Kind {
  return value != null && ['kafka', 'file', 'doris', 'starrocks', 'jdbc', 'mqtt', 's3'].includes(value)
}
