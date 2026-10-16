/**
 * Handwritten HTTP core for the console management API (design doc §4.0).
 *
 * Conventions implemented here, once:
 *  - Base path `/api`.
 *  - Envelope: success returns the resource JSON as-is; failure is HTTP 4xx/5xx
 *    with `{"error":{"code":"…","message":"…"}}` → single throw point (ApiError).
 *    No "200 + error field" envelope (deliberate departure from the legacy system).
 *  - Auth: session cookie `tr_session` is the primary channel; when the auth
 *    store holds an in-memory token it is also attached as `Authorization: Bearer`.
 *  - Any 401 (except opt-out auth endpoints) redirects to /login once, globally.
 *  - `X-Operator` header passthrough (operator attribution, §4.0).
 *  - Unified error toast via naive-ui discrete API; opt out per call with `silent`.
 */

import { darkTheme, createDiscreteApi } from 'naive-ui'
import { brand, semantic } from '../theme/tokens'

export class ApiError extends Error {
  readonly status: number
  readonly code: string
  /** Raw error body, e.g. 409 IN_USE carries `refs` (§4.2). */
  readonly data: unknown

  constructor(status: number, code: string, message: string, data: unknown) {
    super(message || `${status} ${code}`)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.data = data
  }
}

export interface HttpOptions {
  /** Suppress the global error toast (call site handles the failure inline). */
  silent?: boolean
  /** Skip the global 401→/login redirect (auth endpoints probe their own 401s). */
  skipAuthRedirect?: boolean
  /** `X-Operator` override (defaults to the session username server-side, §4.0). */
  operator?: string
  /** Query params; undefined/null values are dropped. */
  query?: Record<string, string | number | boolean | undefined | null>
  /** Extra headers. */
  headers?: Record<string, string>
  /** Fetch timeout in ms (default 30s; artifact upload relaxes to 300s, §4.4). */
  timeoutMs?: number
  /** Parse as JSON even for 204-style empty bodies. */
  allowEmptyBody?: boolean
}

// Bearer token is pushed in by the auth store (memory-only, §6.1-3).
let bearerToken: string | null = null
export function setBearer(token: string | null): void {
  bearerToken = token
}

// Discrete naive-ui message API (outside the component tree). Dark-first theming.
const { message: toast } = createDiscreteApi(['message'], {
  configProviderProps: {
    theme: darkTheme,
    themeOverrides: {
      common: { ...brand.dark, ...semantic.dark },
    },
  },
})

let redirectedToLogin = false

async function redirectToLogin(): Promise<void> {
  if (redirectedToLogin) return
  redirectedToLogin = true
  // Lazy import: keeps http.ts free of a static router/store cycle.
  const [{ default: router }, { useAuthStore }] = await Promise.all([
    import('../router'),
    import('../stores/auth'),
  ])
  useAuthStore().clearSession()
  const current = router.currentRoute.value
  if (current.path !== '/login') {
    await router.push({ path: '/login', query: { redirect: current.fullPath, expired: '1' } })
  }
  redirectedToLogin = false
}

function buildUrl(path: string, query?: HttpOptions['query']): string {
  const url = path.startsWith('/') ? path : `/api/${path}`
  const qs = new URLSearchParams()
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v !== undefined && v !== null) qs.set(k, String(v))
  }
  const s = qs.toString()
  return s ? `${url}?${s}` : url
}

interface EnvelopeError {
  error?: { code?: string; message?: string }
}

async function parseErrorBody(res: Response): Promise<{ code: string; message: string; data: unknown }> {
  let body: unknown = null
  try {
    body = await res.json()
  } catch {
    /* non-JSON error body */
  }
  const env = (body ?? {}) as EnvelopeError
  return {
    code: env.error?.code ?? 'HTTP_' + res.status,
    message: env.error?.message ?? res.statusText,
    data: body,
  }
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  options: HttpOptions = {},
): Promise<T> {
  const { silent = false, skipAuthRedirect = false } = options
  const headers: Record<string, string> = { ...options.headers }
  if (body !== undefined && !(body instanceof FormData)) {
    headers['Content-Type'] = 'application/json'
  }
  if (bearerToken) headers['Authorization'] = `Bearer ${bearerToken}`
  if (options.operator) headers['X-Operator'] = options.operator

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), options.timeoutMs ?? 30_000)

  let res: Response
  try {
    res = await fetch(buildUrl(path, options.query), {
      method,
      headers,
      body: body === undefined ? undefined : body instanceof FormData ? body : JSON.stringify(body),
      signal: controller.signal,
    })
  } catch (e) {
    const err = new ApiError(0, 'NETWORK_ERROR', '无法连接 tailrace-console，请确认服务已启动', null)
    if (!silent) toast.error(err.message)
    throw err
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 401 && !skipAuthRedirect) {
    await redirectToLogin()
  }

  if (!res.ok) {
    const { code, message, data } = await parseErrorBody(res)
    if (!silent) toast.error(message || `${res.status} ${code}`)
    throw new ApiError(res.status, code, message, data)
  }

  if (res.status === 204) return undefined as T
  const text = await res.text()
  if (text === '' && options.allowEmptyBody) return undefined as T
  try {
    return JSON.parse(text) as T
  } catch {
    return (options.allowEmptyBody ? undefined : text) as unknown as T
  }
}

export const http = {
  get: <T>(path: string, options?: HttpOptions) => request<T>('GET', path, undefined, options),
  post: <T>(path: string, body?: unknown, options?: HttpOptions) => request<T>('POST', path, body, options),
  put: <T>(path: string, body?: unknown, options?: HttpOptions) => request<T>('PUT', path, body, options),
  delete: <T>(path: string, options?: HttpOptions) => request<T>('DELETE', path, undefined, options),
}
