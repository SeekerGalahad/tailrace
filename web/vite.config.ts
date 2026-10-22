import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Design doc §8.1: editor chunking + /api proxy to the console binary.
// T17 PoC: CodeMirror 6 needs no web worker, no CSS alias and no icon font —
// the monaco-specific resolve.alias / worker.format entries are gone.
export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
  build: {
    rollupOptions: {
      output: {
        // Vite 8 bundles with rolldown: object-form manualChunks is unsupported,
        // advancedChunks.groups is the native equivalent.
        advancedChunks: {
          groups: [
            // T9 rationale kept: minShareCount=2 — with SqlMonaco loaded via
            // defineAsyncComponent exactly one chunk imports the editor, so the
            // group extracts nothing and CM6 stays inside that async chunk
            // (minShareCount=1 would swallow the shared preload helper and drag
            // the editor into the entry — the login-page hit T9 fixed).
            {
              name: 'codemirror',
              test: /node_modules[\\/](codemirror|@codemirror|@lezer|style-mod|w3c-keyname|crelt|@marijn)[\\/]/,
              minShareCount: 2,
            },
          ],
        },
      },
    },
  },
})
