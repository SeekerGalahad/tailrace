/**
 * Engine-node API (design doc §4.3, management endpoints only — m2m register/
 * heartbeat/ticket belong to the engine binary, not the console web client).
 * Deploy endpoints live here too (POST /api/nodes/deploy, deploy-log, action — §4.4).
 */

import { http, type HttpOptions } from './http'

export type NodeStatus = 'ONLINE' | 'OFFLINE'
export type DeployStatus = 'REGISTERED' | string // 8-step machine values, see §4.4
export type EventLevel = 'ERROR' | 'WARN' | 'INFO'

export interface NodeCapacity {
  pipelines: number
  statements: number
  memoryMb: number
  runningPipelines: number
}

export interface NodeListItem {
  id: number
  nodeKey: string
  name: string
  apiUrl: string
  status: NodeStatus
  engineVersion: string
  deployStatus: DeployStatus
  capacity: NodeCapacity
  artifactId: number | null
  errors24h: number
  lastHeartbeat: string | null
}

export interface NodesSummary {
  online: number
  total: number
  runningPipelines: number
  events24h: number
  errors24h: number
}

export interface NodeList {
  nodes: NodeListItem[]
  summary: NodesSummary
}

/** Detail adds host/ssh/enginePort/installDir/registerMode (exact field set
 * per §4.3 "等"; optional until the console contract freezes). */
export interface NodeDetail extends NodeListItem {
  host?: string
  sshUser?: string
  sshPort?: number
  enginePort?: number
  installDir?: string
  registerMode?: string
}

export interface NodeEvent {
  ts: string
  nodeKey: string
  level: EventLevel
  kind: string
  pipelineId?: number
  message: string
}

export interface NodeEventList {
  items: NodeEvent[]
}

export interface NodeMetricPoint {
  ts: string
  rssBytes: number
  cpuPercent: number
  rxBps: number
  txBps: number
}

export interface NodeMetrics {
  points: NodeMetricPoint[]
}

export interface ManualRegisterInput {
  name: string
  host: string
  enginePort: number
}

/** One-time echo of the generated credentials (§4.3): the per-node token is
 * never shown again after this response — the UI must display it before the
 * user closes the dialog. */
export interface ManualRegistered {
  id: number
  nodeKey: string
  bootstrapToken: string
}

export type NodeAction = 'start' | 'stop' | 'restart' | 'upgrade' | 'rollback' | 'uninstall'

export interface DeployHostInput {
  host: string
  name: string
  sshUser: string
  sshPort: number
  enginePort: number
  installDir: string
}

export interface DeployInput {
  /** Batch of new hosts (one node per line). */
  hosts?: DeployHostInput[]
  /** Redeploy an existing node. */
  nodeId?: number
  artifactId: number
  clean?: boolean
  wipeState?: boolean
}

export interface DeployResponse {
  deploys: { nodeId: number; status: string }[]
}

export interface DeployLogItem {
  step: string
  status: string
  output: string
  operator: string
  ts: string
}

export interface DeployLogList {
  items: DeployLogItem[]
}

export interface ActionResponse {
  accepted: boolean
}

export const nodesApi = {
  /** GET /api/nodes — node table + summary (30s polling, §6.4). */
  list(options?: HttpOptions): Promise<NodeList> {
    return http.get('/api/nodes', options)
  },

  /** GET /api/nodes/{id} */
  get(id: number | string, options?: HttpOptions): Promise<NodeDetail> {
    return http.get(`/api/nodes/${id}`, options)
  },

  /** POST /api/nodes/manual — console health-checks `GET http://host:port/healthz`
   * before registering; 502 when the probe fails. Response echoes node_key +
   * bootstrap_token exactly once (§4.3). */
  registerManual(input: ManualRegisterInput, options?: HttpOptions): Promise<ManualRegistered> {
    return http.post('/api/nodes/manual', input, options)
  },

  /** POST /api/nodes/{id}/revoke — revokes bootstrap token + unused tickets; 204. */
  revoke(id: number | string, options?: HttpOptions): Promise<void> {
    return http.post(`/api/nodes/${id}/revoke`, undefined, options)
  },

  /** DELETE /api/nodes/{id} — 409 when a pipeline is assigned; best-effort
   * remote cleanup is reported in the response. */
  remove(id: number | string, options?: HttpOptions): Promise<{ remoteCleaned: boolean; warning: string | null }> {
    return http.delete(`/api/nodes/${id}`, options)
  },

  /** GET /api/nodes/{id}/events?hours=168 */
  events(id: number | string, params?: { hours?: number }, options?: HttpOptions): Promise<NodeEventList> {
    return http.get(`/api/nodes/${id}/events`, { query: params, ...options })
  },

  /** GET /api/nodes/events?hours=24&level=ERROR — cluster-wide event feed. */
  allEvents(params?: { hours?: number; level?: EventLevel }, options?: HttpOptions): Promise<NodeEventList> {
    return http.get('/api/nodes/events', { query: params, ...options })
  },

  /** GET /api/nodes/{id}/metrics?hours=6 — 1-minute buckets, window clamped 1..24h. */
  metrics(id: number | string, params?: { hours?: number }, options?: HttpOptions): Promise<NodeMetrics> {
    return http.get(`/api/nodes/${id}/metrics`, { query: params, ...options })
  },

  /** GET /api/nodes/{id}/pipelines-runtime — proxied engine `GET /v1/pipelines`
   * (statement-level snapshot passthrough; 502 when the node is unreachable).
   * Shape is owned by the engine /v1 contract — treat as opaque here. */
  pipelinesRuntime(id: number | string, options?: HttpOptions): Promise<unknown> {
    return http.get(`/api/nodes/${id}/pipelines-runtime`, options)
  },

  /** POST /api/nodes/deploy — SSH 8-step deploy state machine (§4.4). */
  deploy(input: DeployInput, options?: HttpOptions): Promise<DeployResponse> {
    return http.post('/api/nodes/deploy', input, options)
  },

  /** GET /api/nodes/{id}/deploy-log?limit=100 — time-ascending step log. */
  deployLog(id: number | string, params?: { limit?: number }, options?: HttpOptions): Promise<DeployLogList> {
    return http.get(`/api/nodes/${id}/deploy-log`, { query: params, ...options })
  },

  /** POST /api/nodes/{id}/action {"action":"stop"} → 202 {accepted:true}.
   * `upgrade` additionally requires artifactId. */
  action(id: number | string, action: NodeAction, artifactId?: number, options?: HttpOptions): Promise<ActionResponse> {
    return http.post(`/api/nodes/${id}/action`, { action, artifactId }, options)
  },
}
