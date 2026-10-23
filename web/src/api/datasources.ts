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
}
