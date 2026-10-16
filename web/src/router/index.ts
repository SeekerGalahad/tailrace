import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '../stores/auth'

/**
 * Route tree (design doc §6.0): /login outside the layout; everything else
 * nests under ConsoleLayout (single-level sidebar). Route-level code splitting.
 */

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', name: 'login', component: () => import('../pages/login/index.vue') },
    {
      path: '/',
      component: () => import('../layouts/ConsoleLayout.vue'),
      children: [
        { path: '', redirect: '/overview' },
        { path: 'overview', name: 'overview', component: () => import('../pages/overview/index.vue') },
        { path: 'datasources', name: 'datasources', component: () => import('../pages/datasources/index.vue') },
        { path: 'datasources/:id', name: 'datasource-detail', component: () => import('../pages/datasources/detail.vue') },
        { path: 'nodes', name: 'nodes', component: () => import('../pages/nodes/index.vue') },
        { path: 'pipelines', name: 'pipelines', component: () => import('../pages/pipelines/index.vue') },
        // Pipeline sub-pages (§6.5): /pipelines/edit stays static so it never
        // collides with /pipelines/:id/… (vue-router ranks static segments higher).
        { path: 'pipelines/edit', name: 'pipelines-edit', component: () => import('../pages/pipelines/edit.vue') },
        { path: 'pipelines/:id/publish', name: 'pipeline-publish', component: () => import('../pages/pipelines/publish.vue') },
        { path: 'pipelines/:id/monitor', name: 'pipeline-monitor', component: () => import('../pages/pipelines/monitor.vue') },
      ],
    },
    { path: '/:pathMatch(.*)*', redirect: '/overview' },
  ],
})

/**
 * Global guard (design doc §6.1): first navigation probes GET /api/auth/me —
 * 200 caches the user and passes; 401 on any protected target redirects to
 * /login?redirect=<fullPath>&expired=1; an authenticated visit to /login
 * bounces to /overview.
 */
router.beforeEach(async (to) => {
  const auth = useAuthStore()
  if (!auth.checked) await auth.checkSession()

  if (to.path === '/login') {
    return auth.isAuthenticated() ? { path: '/overview', replace: true } : true
  }
  if (!auth.isAuthenticated()) {
    return {
      path: '/login',
      query: { redirect: to.fullPath, expired: '1' },
      replace: true,
    }
  }
  return true
})

/**
 * Open-redirect guard (§6.1): only same-site relative paths pass
 * (`^/` and not `//`); anything else silently falls back to /overview.
 */
export function sanitizeRedirect(raw: unknown): string {
  if (typeof raw === 'string' && raw.startsWith('/') && !raw.startsWith('//')) return raw
  return '/overview'
}

export default router
