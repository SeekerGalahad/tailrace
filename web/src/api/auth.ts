/**
 * Auth API (design doc §4.1). Types mirror the spec verbatim.
 *
 * All three calls opt out of the global 401 redirect: login/me handle their
 * own 401 semantics (inline alert / guard redirect).
 */

import { http, type HttpOptions } from './http'

export interface User {
  username: string
  displayName: string
  role: string
}

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  /** Opaque token; also delivered as Set-Cookie tr_session (HttpOnly). */
  token: string
  expiresInSec: number
  user: User
}

export interface MeResponse {
  user: User
}

/** Remaining-login-attempts lockout: 5 consecutive failures → 60s lock (§4.1). */
export const MAX_FAILED_ATTEMPTS = 5
export const LOCKOUT_SECONDS = 60

export const authApi = {
  /** POST /api/auth/login — 200 LoginResponse; 401 {code: BAD_CREDENTIALS}. */
  login(body: LoginRequest): Promise<LoginResponse> {
    return http.post('/api/auth/login', body, { silent: true, skipAuthRedirect: true })
  },

  /** POST /api/auth/logout — 204, revokes the current session. */
  logout(): Promise<void> {
    return http.post('/api/auth/logout', undefined, { silent: true, skipAuthRedirect: true })
  },

  /** GET /api/auth/me — 200 {user}; 401 when unauthenticated (guard probes it). */
  me(options?: HttpOptions): Promise<MeResponse> {
    return http.get('/api/auth/me', { silent: true, skipAuthRedirect: true, ...options })
  },
}
