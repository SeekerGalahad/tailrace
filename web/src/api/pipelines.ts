/**
 * Pipeline API (design doc §4.5). Response shapes mirror the spec; proxied
 * engine payloads (stats/offsets) keep the engine's own envelope.
 */

import { http, type HttpOptions } from './http'

export type PipelineStatus =
  | 'DRAFT'
  | 'PUBLISHED'
  | 'RUNNING'
  | 'PAUSED'
  | 'FAILED'
  | 'DEPLOY_FAILED'
  | 'PENDING_CHANGES'

export type StartOffset = 'earliest' | 'latest'

export interface PipelineListItem {
  id: number
  name: string
  status: PipelineStatus
  version: number
  groupPrefix: string | null
  consumedOffsetStart: StartOffset
  assignedNode: { id: number; name: string } | null
  updatedAt: string
}

export interface PipelineList {
  pipelines: PipelineListItem[]
  total: number
  page: number
  pageSize: number
}

// Type alias (not interface): implicit index signature for the http query record.
export type PipelineListParams = {
  page?: number
  pageSize?: number
}

/** Advanced-config five sections (§4.5 plan body options). */
export interface ReliabilityOptions {
  dorisFlushRetries?: number | null
  dorisFlushBackoffBaseMs?: number | null
  dorisRequestTimeoutMs?: number | null
  kafkaSendTimeoutMs?: number | null
  maxPollIntervalMs?: number | null
}

export interface RecoveryOptions {
  autoRestart?: boolean | null
  autoRestartMax?: number | null
  restartBackoffSecs?: number[] | null
}

export interface TuningOptions {
  flushRows?: number | null
  flushIntervalMs?: number | null
  flushBytes?: number | null
  pollRows?: number | null
  pollSliceMs?: number | null
}

export interface PipelineConfig {
  reliability?: ReliabilityOptions | null
  recovery?: RecoveryOptions | null
  tuning?: TuningOptions | null
  groupPrefix?: string | null
  startOffset?: StartOffset | null
}

export interface PipelineUpsertInput {
  name: string
  sqlContent: string
  /** Logical db name → datasource id. */
  dsMapping: Record<string, number>
  checkpointMs: number
  preferredNodeId: number | null
  config: PipelineConfig
}

export interface CreatePipelineResponse {
  id: number
}

export interface PipelineDetail extends PipelineListItem {
  sqlContent: string
  specJson?: string
  deployError?: string | null
  dsMapping?: Record<string, number>
  checkpointMs?: number
  preferredNodeId?: number | null
  config?: PipelineConfig
}

export interface Diagnostic {
  line: number
  message: string
}

export interface CompiledStatement {
  index: number
  /** `${datasourceId}.${dbName}.${tableName}` */
  source: string
  /** Default consumer group: tr.p{pipelineId}.s{index} (§4.5). */
  group: string
  sink: string
  pushdown: string[]
  selectSql: string
  startOffset: StartOffset
}

export interface CompileInput {
  sql: string
  dsMapping: Record<string, number>
}

export interface CompileResponse {
  ok: boolean
  statements: CompiledStatement[]
  diagnostics: Diagnostic[]
}

export interface PublishResponse {
  id: number
  version: number
  specJson: string
}

export interface PipelineActionResponse {
  pipelineId: number
  status: PipelineStatus
}

/** Engine stats passthrough: `{"ok":true,"stats":{…}}` / `{"ok":false,"error":"…"}`
 * on HTTP 200 (front-end degrades on ok=false); 502 when the node is unreachable. */
export interface StatsEnvelope {
  ok: boolean
  stats?: unknown
  error?: string
}

/** Trend points (console-side 5-minute buckets from heartbeats, §4.5). */
export interface PipelineTrendPoint {
  ts: string
  consumed: number
  flushed: number
  rateInRps: number
  rateOutRps: number
}

export interface PipelineTrend {
  points: PipelineTrendPoint[]
}

/** Engine offsets passthrough — per-statement kafka partition offsets
 * (partition→offset), topic/group; file sources carry path+bytePos; plus
 * totals and lastAck. Shape owned by the engine /v1 contract; fields below
 * are the documented surface, refine at P1 integration. */
export interface PipelineOffsets {
  statements?: Array<{
    group?: string
    topic?: string
    partitions?: Record<string, number>
    file?: { path: string; bytePos: number }
  }>
  totals?: { in: number; out: number; flushes: number }
  lastAck?: string | null
}

export interface PipelineTemplate {
  name: string
  label: string
  sql: string
}

export interface TemplateList {
  templates: PipelineTemplate[]
}

export interface RecoverInput {
  /** null = current node (plan re-push resumes from engine state). */
  nodeId: number | null
}

export const pipelinesApi = {
  /** GET /api/pipelines?page=&pageSize= — 30s polling (§6.5). */
  list(params?: PipelineListParams, options?: HttpOptions): Promise<PipelineList> {
    return http.get('/api/pipelines', { query: params, ...options })
  },

  /** GET /api/pipelines/{id} */
  get(id: number | string, options?: HttpOptions): Promise<PipelineDetail> {
    return http.get(`/api/pipelines/${id}`, options)
  },

  /** POST /api/pipelines — new draft → {id}. */
  create(input: PipelineUpsertInput, options?: HttpOptions): Promise<CreatePipelineResponse> {
    return http.post('/api/pipelines', input, options)
  },

  /** PUT /api/pipelines/{id} — saving a non-DRAFT pipeline moves it to
   * PENDING_CHANGES (changes need publish+deploy to take effect). */
  update(id: number | string, input: PipelineUpsertInput, options?: HttpOptions): Promise<void> {
    return http.put(`/api/pipelines/${id}`, input, options)
  },

  /** DELETE /api/pipelines/{id} — a RUNNING pipeline is auto-stopped first. */
  remove(id: number | string, options?: HttpOptions): Promise<void> {
    return http.delete(`/api/pipelines/${id}`, options)
  },

  /** POST /api/pipelines/compile — embedded parser diagnostics (400 on errors
   * carries the same envelope with diagnostics in `error`). */
  compile(input: CompileInput, options?: HttpOptions): Promise<CompileResponse> {
    return http.post('/api/pipelines/compile', input, { silent: true, ...options })
  },

  /** POST /api/pipelines/{id}/publish — gate failure → 400 + diagnostics. */
  publish(id: number | string, options?: HttpOptions): Promise<PublishResponse> {
    return http.post(`/api/pipelines/${id}/publish`, undefined, options)
  },

  /** POST /api/pipelines/{id}/deploy {"nodeId":null} — null auto-assigns
   * (preferred → least-loaded → lowest id); engine ok:false → 502 DEPLOY_FAILED. */
  deploy(id: number | string, nodeId: number | null = null, options?: HttpOptions): Promise<void> {
    return http.post(`/api/pipelines/${id}/deploy`, { nodeId }, options)
  },

  /** POST /api/pipelines/{id}/pause|resume|stop — proxied engine actions; 502
   * when unreachable. stop = graceful shutdown, cursor committed. */
  pause(id: number | string, options?: HttpOptions): Promise<PipelineActionResponse> {
    return http.post(`/api/pipelines/${id}/pause`, undefined, options)
  },
  resume(id: number | string, options?: HttpOptions): Promise<PipelineActionResponse> {
    return http.post(`/api/pipelines/${id}/resume`, undefined, options)
  },
  stop(id: number | string, options?: HttpOptions): Promise<PipelineActionResponse> {
    return http.post(`/api/pipelines/${id}/stop`, undefined, options)
  },

  /** GET /api/pipelines/{id}/stats — engine snapshot; totals are monotonic,
   * rates are computed by the front-end from consecutive samples (5s polling). */
  stats(id: number | string, options?: HttpOptions): Promise<StatsEnvelope> {
    return http.get(`/api/pipelines/${id}/stats`, { silent: true, ...options })
  },

  /** GET /api/pipelines/{id}/offsets — proxied engine offset ledger. */
  offsets(id: number | string, options?: HttpOptions): Promise<PipelineOffsets> {
    return http.get(`/api/pipelines/${id}/offsets`, options)
  },

  /** GET /api/pipelines/{id}/metrics/trend?hours=24 */
  trend(id: number | string, params?: { hours?: number }, options?: HttpOptions): Promise<PipelineTrend> {
    return http.get(`/api/pipelines/${id}/metrics/trend`, { query: params, ...options })
  },

  /** POST /api/pipelines/{id}/recover — re-push plan; engine resumes from
   * state/offsets-{id}.json (no loss, no duplication). */
  recover(id: number | string, input: RecoverInput, options?: HttpOptions): Promise<void> {
    return http.post(`/api/pipelines/${id}/recover`, input, options)
  },

  /** GET /api/pipelines/templates — SQL templates embedded in the console binary. */
  templates(options?: HttpOptions): Promise<TemplateList> {
    return http.get('/api/pipelines/templates', options)
  },

  /** SSE log endpoint URL for LogTerminal (native EventSource; cookie auth only,
   * see sse.ts). `lines` clamps to 50..500 server-side (§4.5). */
  logsUrl(id: number | string, lines = 200): string {
    return `/api/pipelines/${id}/logs?lines=${lines}`
  },
}
