/**
 * Datasource + table-object API (design doc §4.2). Types mirror the spec.
 *
 * Credentials never come back from the server — only `hasPassword` (§4.2).
 * The PUT carries `generation` for optimistic locking (409 GENERATION_CONFLICT).
 */

import { http, type HttpOptions } from './http'
import type { Kind } from '../theme/tokens'

export type DatasourceStatus = 'CONN_OK' | 'CONN_FAIL' | 'CREATED'

/** GET /api/datasource-types — dynamic-form single source of truth. */
export interface DatasourceTypeField {
  name: string
  label: string
  inputType: 'text' | 'password' | 'number' | 'select' | 'textarea'
  required: boolean
  placeholder?: string
}

export interface DatasourceType {
  type: Kind | string
  label: string
  family: string
  credentialLabel: string | null
  fields: DatasourceTypeField[]
}

export interface DatasourceTypeList {
  types: DatasourceType[]
}

export interface Datasource {
  id: number
  name: string
  type: Kind | string
  status: DatasourceStatus
  /** Optimistic-lock generation, bumped on every write. */
  generation: number
  hasPassword: boolean
  connJson: Record<string, unknown>
  updatedAt: string
}

// Type alias (not interface): gets an implicit index signature so it can be
// spread into the http query record.
export type DatasourceListParams = {
  type?: string
  layer?: string
}

export interface DatasourceList {
  items: Datasource[]
  total: number
}

/** Detail = list item fields + table-object count (field name decided here;
 * the doc only says "表对象计数" — align with console when it lands). */
export interface DatasourceDetail extends Datasource {
  tableCount?: number
}

export interface DatasourceUpsertInput {
  name: string
  type: Kind | string
  connJson: Record<string, unknown>
  username?: string | null
  /** Empty/omitted on update = keep the stored credential (§4.2). */
  password?: string | null
  /** CAS check; update-only (409 GENERATION_CONFLICT on mismatch). */
  generation?: number
}

export interface CreateDatasourceResponse {
  id: number
  generation: number
}

export interface ConnTestResult {
  status: 'CONN_OK' | 'CONN_FAIL'
  message: string
  latencyMs: number
}

export interface DiscoveredAsset {
  dbName: string
  tableName: string
  kind: Kind | string
  format: string | null
  partitions?: number
}

export interface DiscoverResponse {
  assets: DiscoveredAsset[]
}

export interface RegisterTableInput {
  dbName: string
  tableName: string
  format: string | null
  /** [name, type] pairs. */
  columnsJson: [string, string][]
}

export interface RegisterTableResponse {
  id: number
}

export interface InferSchemaInput {
  dbName: string
  tableName: string
  format?: string | null
}

export interface InferSchemaResponse {
  columns: [string, string][]
}

export interface TableObject {
  id: number
  datasourceId: number
  dbName: string
  tableName: string
  kind: Kind | string
  format: string | null
  cursorSemantics: string
  source: string
  /** `[name, type]` pairs as registered; `null` when registered without columns (T39 ⑤). */
  columnsJson: [string, string][] | null
}

export interface TableObjectList {
  items: TableObject[]
}

// ---------------- T-A1 create-table closed loop (blueprint §7.2) ----------------
// draft (pure, read-only) → human confirms the key model → execute (audited)
// → evolve (ADD COLUMN only). Doris/StarRocks datasources only.

export type DdlKeyModel = 'unique' | 'duplicate'

/** Round-two hints fed back into the generator — all optional; an empty
 * body yields the heuristic draft with its 「猜测」 flags. */
export interface DdlDraftInput {
  /** Confirmed key column — clears `keyColumnGuessed` in the response. */
  keyColumn?: string
  /** Override of the recommendation. */
  keyModel?: DdlKeyModel
  /** `replication_num`: 1 on a single-BE cluster (`SHOW BACKENDS`), omitted =
   * cluster default. The console does not probe BE count this wave. */
  replicationNum?: number
  /** Partition decision: omitted = the heuristic's candidate (when executable)
   * stays a guess; `true` = confirmed (guess flag clears); `false` = dropped —
   * no `PARTITION BY` clause and no `dynamic_partition.*` properties. */
  partition?: boolean
}

export interface DdlPartition {
  column: string
  /** Name-based guess the human has not confirmed yet (confirm/drop via
   * `DdlDraftInput.partition`). */
  guessed: boolean
}

export interface DdlDraftResponse {
  /** Full `CREATE TABLE` text — previewed verbatim, executed verbatim. */
  ddl: string
  keyModel: DdlKeyModel
  keyModelReason: string
  /** Column the key clause and hash distribution ride on. */
  keyColumn: string
  /** true = id / `*_id` / first-column heuristic — the 「猜测」 badge. */
  keyColumnGuessed: boolean
  partition: DdlPartition | null
  notes: string[]
  /** Registered `[name, type]` pairs the draft was built from. */
  columns: [string, string][]
  sequenceColumn: string | null
}

export interface DdlExecuteInput {
  /** The draft text echoed back verbatim. */
  ddl: string
  /** Mandatory — absent/unknown → 422 KEY_MODEL_REQUIRED, nothing reaches Doris. */
  keyModel: DdlKeyModel
}

export interface DdlExecuteResponse {
  ok: boolean
  keyModel: DdlKeyModel
  elapsedMs: number
  rowsAffected: number
}

/** 200: `changed:true` with the ADD COLUMN statements that ran, or
 * `changed:false` when the live table already carries every registered column. */
export interface DdlEvolveResponse {
  changed: boolean
  statements: string[]
  added: [string, string][]
  elapsedMs: number
  /** Doris schema-change / partial-update exclusivity notice (T-A1 ⑤ / T-A7). */
  partialUpdateNotice: string
}

/** 409 SCHEMA_CHANGE_UNSUPPORTED `error.refs` — suggestions only, nothing executed. */
export interface DdlEvolveConflictRefs {
  suggestedDdl: string[]
  added: [string, string][]
  retyped: Array<{ column: string; registered: string; current: string }>
  removed: [string, string][]
  partialUpdateNotice: string
}

export const datasourcesApi = {
  /** GET /api/datasource-types */
  listTypes(options?: HttpOptions): Promise<DatasourceTypeList> {
    return http.get('/api/datasource-types', options)
  },

  /** GET /api/datasources?type=&layer= */
  list(params?: DatasourceListParams, options?: HttpOptions): Promise<DatasourceList> {
    return http.get('/api/datasources', { query: params, ...options })
  },

  /** GET /api/datasources/{id} */
  get(id: number | string, options?: HttpOptions): Promise<DatasourceDetail> {
    return http.get(`/api/datasources/${id}`, options)
  },

  /** POST /api/datasources — plaintext credentials appear exactly once, here. */
  create(input: DatasourceUpsertInput, options?: HttpOptions): Promise<CreateDatasourceResponse> {
    return http.post('/api/datasources', input, options)
  },

  /** PUT /api/datasources/{id} — body = create + generation (CAS). */
  update(id: number | string, input: DatasourceUpsertInput, options?: HttpOptions): Promise<void> {
    return http.put(`/api/datasources/${id}`, input, options)
  },

  /** DELETE /api/datasources/{id} — 409 IN_USE with `refs` when referenced. */
  remove(id: number | string, options?: HttpOptions): Promise<void> {
    return http.delete(`/api/datasources/${id}`, options)
  },

  /** POST /api/datasources/test — probe without persisting (registry form). */
  test(
    input: Pick<DatasourceUpsertInput, 'type' | 'connJson' | 'username' | 'password'>,
    options?: HttpOptions,
  ): Promise<ConnTestResult> {
    // HTTP stays 200 even when the probe itself reports CONN_FAIL (§4.2).
    return http.post('/api/datasources/test', input, { silent: true, ...options })
  },

  /** POST /api/datasources/{id}/test — probe a stored source, writes back status. */
  testById(id: number | string, options?: HttpOptions): Promise<ConnTestResult> {
    return http.post(`/api/datasources/${id}/test`, undefined, { silent: true, ...options })
  },

  /** POST /api/datasources/{id}/discover {"sampleRows":20} — 502 on probe failure. */
  discover(id: number | string, sampleRows = 20, options?: HttpOptions): Promise<DiscoverResponse> {
    return http.post(`/api/datasources/${id}/discover`, { sampleRows }, options)
  },

  /** POST /api/datasources/{id}/infer-schema — schema inference, nothing persisted. */
  inferSchema(id: number | string, input: InferSchemaInput, options?: HttpOptions): Promise<InferSchemaResponse> {
    return http.post(`/api/datasources/${id}/infer-schema`, input, options)
  },

  /** POST /api/datasources/{id}/tables — register a table object. */
  registerTable(id: number | string, input: RegisterTableInput, options?: HttpOptions): Promise<RegisterTableResponse> {
    return http.post(`/api/datasources/${id}/tables`, input, options)
  },

  /** GET /api/datasources/tables?datasourceId=&kind= */
  listTables(
    params?: { datasourceId?: number | string; kind?: string },
    options?: HttpOptions,
  ): Promise<TableObjectList> {
    return http.get('/api/datasources/tables', { query: params, ...options })
  },

  /** DELETE /api/datasources/tables/{id} — 409 when referenced by published pipelines. */
  removeTable(id: number | string, options?: HttpOptions): Promise<void> {
    return http.delete(`/api/datasources/tables/${id}`, options)
  },

  /** POST /api/datasources/{id}/tables/{db}/{table}/ddl/draft — pure generator,
   * nothing touches Doris, nothing audited. Omit `input` for the first round. */
  ddlDraft(
    id: number | string,
    db: string,
    table: string,
    input?: DdlDraftInput,
    options?: HttpOptions,
  ): Promise<DdlDraftResponse> {
    return http.post(`${ddlPath(id, db, table)}/draft`, input, options)
  },

  /** POST …/ddl/execute — runs the echoed draft verbatim via mysql_exec.
   * 422 KEY_MODEL_REQUIRED / DDL_REQUIRED / KEY_MODEL_MISMATCH before any
   * connection; 502 DDL_FAILED carries the Doris error verbatim. */
  ddlExecute(
    id: number | string,
    db: string,
    table: string,
    input: DdlExecuteInput,
    options?: HttpOptions,
  ): Promise<DdlExecuteResponse> {
    return http.post(`${ddlPath(id, db, table)}/execute`, input, options)
  },

  /** POST …/ddl/evolve (no body) — DESC vs registered columns; add-only runs
   * as chained ADD COLUMN, retype/drop → 409 SCHEMA_CHANGE_UNSUPPORTED with
   * `error.refs` (DdlEvolveConflictRefs), nothing executed. */
  ddlEvolve(id: number | string, db: string, table: string, options?: HttpOptions): Promise<DdlEvolveResponse> {
    return http.post(`${ddlPath(id, db, table)}/evolve`, undefined, options)
  },
}

/** `/api/datasources/{id}/tables/{db}/{table}/ddl` with the path segments encoded. */
function ddlPath(id: number | string, db: string, table: string): string {
  return `/api/datasources/${id}/tables/${encodeURIComponent(db)}/${encodeURIComponent(table)}/ddl`
}
