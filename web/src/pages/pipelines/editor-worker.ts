/**
 * Monaco editor-worker entry — bundled as an ES-module web worker via the
 * `?worker` suffix. Specifier goes through monaco's package `exports` map
 * ("./*.js" → "./esm/vs/*.js"); monaco 0.57 ships SQL as a basic language,
 * so the single editor worker covers tokenization (§6.5.2).
 */
import 'monaco-editor/editor/editor.worker.js'
