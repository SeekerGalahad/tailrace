import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Design doc §8.1: monaco worker chunking + /api proxy to the console binary.
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      // monaco's exports map ("./*.js" → "./esm/vs/*.js") has no CSS entry —
      // resolve the codicon font stylesheet (suggest-widget icons) directly.
      // URL.pathname keeps this dependency-free (no @types/node in the web pkg).
      'monaco-editor/base/browser/ui/codicons/codicon/codicon.css': decodeURIComponent(
        new URL('./node_modules/monaco-editor/esm/vs/base/browser/ui/codicons/codicon/codicon.css', import.meta.url).pathname,
      ),
    },
  },
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
  worker: {
    // monaco editor workers as ES modules
    format: 'es',
  },
  build: {
    rollupOptions: {
      output: {
        // Vite 8 bundles with rolldown: object-form manualChunks is unsupported,
        // advancedChunks.groups is the native equivalent.
        advancedChunks: {
          groups: [
            // T9: minShareCount=2 — with SqlMonaco loaded via defineAsyncComponent
            // exactly one chunk imports monaco, so the group extracts nothing and
            // monaco stays inside that async chunk. Without it (minShareCount=1)
            // the group chunk swallowed the shared preload helper, forcing the
            // entry to statically import the 4.4MB monaco chunk (login-page hit).
            { name: 'monaco', test: /node_modules[\\/]monaco-editor/, minShareCount: 2 },
          ],
        },
      },
    },
  },
})
