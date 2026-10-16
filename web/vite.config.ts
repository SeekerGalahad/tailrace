import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Design doc §8.1: monaco worker chunking + /api proxy to the console binary.
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
          groups: [{ name: 'monaco', test: /node_modules[\\/]monaco-editor/ }],
        },
      },
    },
  },
})
