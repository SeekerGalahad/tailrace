import { ref } from 'vue'
import { defineStore } from 'pinia'
import { authApi, type User } from '../api/auth'
import { setBearer } from '../api/http'
import { LS_LAST_USER } from '../theme/tokens'

/**
 * Session store (design doc §6.1): the session cookie `tr_session` is the
 * primary auth channel; the token returned by login lives in memory ONLY
 * (never localStorage — XSS hardening, §6.1-3).
 *
 * `checked` gates the router guard: the app probes GET /api/auth/me exactly
 * once on first navigation; later requests that turn 401 go through the
 * http-layer global redirect.
 */
export const useAuthStore = defineStore('auth', () => {
  const user = ref<User | null>(null)
  const token = ref<string | null>(null)
  const checked = ref(false)

  function clearSession(): void {
    user.value = null
    token.value = null
    setBearer(null)
  }

  /** Probe /api/auth/me once; caches the user on 200. */
  async function checkSession(): Promise<boolean> {
    if (checked.value) return user.value !== null
    try {
      const { user: me } = await authApi.me()
      user.value = me
      return true
    } catch {
      clearSession()
      return false
    } finally {
      checked.value = true
    }
  }

  /** POST /api/auth/login — cookie is set by the server; remember the last
   * username only (password is never persisted, §6.1-3). */
  async function login(username: string, password: string): Promise<User> {
    const res = await authApi.login({ username, password })
    user.value = res.user
    token.value = res.token
    setBearer(res.token)
    try {
      localStorage.setItem(LS_LAST_USER, username)
    } catch {
      /* non-fatal */
    }
    return res.user
  }

  /** POST /api/auth/logout — revokes the session, clears local state. */
  async function logout(): Promise<void> {
    try {
      await authApi.logout()
    } finally {
      clearSession()
    }
  }

  const isAuthenticated = () => user.value !== null

  return { user, token, checked, isAuthenticated, checkSession, login, logout, clearSession }
})
