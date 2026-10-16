<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
// T9 entry cut: `monaco-editor` (editor.main) bundles ~80 language
// definitions + css/html/json/ts LSP features + monaco-lsp-client and their
// workers (ts.worker alone is 6.9MB). Deep specifiers resolve through the
// package exports map ("./*.js" → "./esm/vs/*.js"); the set below is
// editor.api (core API + widget) + SQL tokenizer + the editor-side contribs
// the SQL editor actually uses (completion/find/contextmenu/... — without
// them editor.api alone registers no suggest/find controller and table
// completion would render nothing). The single editor worker stays.
import * as monaco from 'monaco-editor/editor/editor.api.js'
import 'monaco-editor/languages/definitions/sql/register.js'
// editor-side contributions (subset of editor.main's list, languages/LSP cut)
import 'monaco-editor/editor/browser/coreCommands.js'
import 'monaco-editor/features/find/register.js'
import 'monaco-editor/editor/contrib/suggest/browser/suggestController.js'
import 'monaco-editor/editor/contrib/snippet/browser/snippetController2.js'
import 'monaco-editor/editor/contrib/contextmenu/browser/contextmenu.js'
import 'monaco-editor/editor/contrib/hover/browser/hoverContribution.js'
import 'monaco-editor/editor/contrib/bracketMatching/browser/bracketMatching.js'
import 'monaco-editor/editor/contrib/folding/browser/folding.js'
import 'monaco-editor/editor/contrib/wordHighlighter/browser/wordHighlighter.js'
import 'monaco-editor/editor/contrib/comment/browser/comment.js'
import 'monaco-editor/editor/contrib/linesOperations/browser/linesOperations.js'
import 'monaco-editor/editor/contrib/wordOperations/browser/wordOperations.js'
import 'monaco-editor/editor/contrib/wordPartOperations/browser/wordPartOperations.js'
import 'monaco-editor/editor/contrib/smartSelect/browser/smartSelect.js'
import 'monaco-editor/editor/contrib/multicursor/browser/multicursor.js'
import 'monaco-editor/editor/contrib/lineSelection/browser/lineSelection.js'
import 'monaco-editor/editor/contrib/indentation/browser/indentation.js'
import 'monaco-editor/editor/contrib/cursorUndo/browser/cursorUndo.js'
import 'monaco-editor/editor/contrib/caretOperations/browser/caretOperations.js'
import 'monaco-editor/editor/contrib/caretOperations/browser/transpose.js'
import 'monaco-editor/editor/contrib/clipboard/browser/clipboard.js'
import 'monaco-editor/editor/contrib/unicodeHighlighter/browser/unicodeHighlighter.js'
import 'monaco-editor/editor/contrib/unusualLineTerminators/browser/unusualLineTerminators.js'
import 'monaco-editor/editor/contrib/links/browser/links.js'
import 'monaco-editor/editor/contrib/dnd/browser/dnd.js'
import 'monaco-editor/editor/standalone/browser/quickAccess/standaloneCommandsQuickAccess.js'
import 'monaco-editor/editor/standalone/browser/quickAccess/standaloneHelpQuickAccess.js'
import 'monaco-editor/base/browser/ui/codicons/codicon/codicon.css'
import EditorWorker from './editor-worker?worker'
import { useThemeStore } from '../../stores/theme'
import { fontMono, neutral, semantic } from '../../theme/tokens'
import type { Diagnostic } from '../../api/pipelines'

/**
 * SqlMonaco (design doc §6.5.2): thin monaco-editor wrapper.
 *
 * - SQL language + `库名.` table-object completion (max 50, detail shows
 *   kind · format); applying a completion auto-inserts `-- @tag:ds` above
 *   the current line when missing (publish-gate requirement)
 * - compile diagnostics → monaco markers (wavy underlines)
 * - theme follows ThemeStore (colors from tokens, never hardcoded)
 * - monaco 0.57: SQL is a basic language (tokenizer only, no LSP worker) —
 *   the single editor worker covers tokenization; ES-module worker format
 *   per vite.config (§8.1)
 */

const props = withDefaults(
  defineProps<{
    modelValue: string
    /** Compile diagnostics to mirror as editor markers. */
    diagnostics?: Diagnostic[]
    /** Completion source: registered table objects. */
    tables?: Array<{ dbName: string; tableName: string; kind: string; format: string | null }>
    height?: number
    placeholder?: string
  }>(),
  { diagnostics: () => [], tables: () => [], height: 560, placeholder: undefined },
)

const emit = defineEmits<{ 'update:modelValue': [value: string] }>()

const theme = useThemeStore()
const hostEl = ref<HTMLDivElement | null>(null)

// --- editor worker (module scope: once per page chunk) ---
;(self as unknown as { MonacoEnvironment: monaco.Environment }).MonacoEnvironment = {
  getWorker(_workerId: string, _label: string): Worker {
    return new EditorWorker()
  },
}

// --- completion source + tag command: module-level, single editor per page ---
interface CompletionTable {
  dbName: string
  tableName: string
  kind: string
  format: string | null
}
let completionTables: CompletionTable[] = []
let tagCommandId: string | null = null
let activeModel: monaco.editor.ITextModel | null = null
let activeEditor: monaco.editor.IStandaloneCodeEditor | null = null
let providerRegistered = false

function registerProvider(): void {
  if (providerRegistered) return
  providerRegistered = true
  monaco.languages.registerCompletionItemProvider('sql', {
    triggerCharacters: ['.'],
    provideCompletionItems(model, position) {
      const lineUpToCursor = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      })
      const m = /([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z0-9_]*)$/.exec(lineUpToCursor)
      if (!m) return { suggestions: [] }
      const [, dbName, prefix] = m
      const hit = completionTables.filter((t) => t.dbName.toLowerCase() === dbName.toLowerCase())
      if (hit.length === 0) return { suggestions: [] }
      const word = model.getWordUntilPosition(position)
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      }
      return {
        suggestions: hit
          .filter((t) => t.tableName.toLowerCase().startsWith(prefix.toLowerCase()))
          .slice(0, 50)
          .map((t) => ({
            label: t.tableName,
            kind: monaco.languages.CompletionItemKind.Field,
            insertText: t.tableName,
            detail: [t.kind, t.format].filter(Boolean).join(' · '),
            range,
            command: tagCommandId
              ? { id: tagCommandId, title: 'tailrace.addDsTag', arguments: [position.lineNumber] }
              : undefined,
          })),
      }
    },
  })
}

function insertTagAbove(lineNumber: number): void {
  const model = activeModel
  const editor = activeEditor
  if (!model || !editor || lineNumber < 1 || lineNumber > model.getLineCount()) return
  const prev = lineNumber > 1 ? model.getLineContent(lineNumber - 1) : ''
  if (/^\s*--\s*@tag/.test(prev)) return // already tagged
  editor.pushUndoStop()
  editor.executeEdits('tailrace-tag', [
    { range: new monaco.Range(lineNumber, 1, lineNumber, 1), text: '-- @tag:ds\n', forceMoveMarkers: true },
  ])
  editor.pushUndoStop()
}

// --- themes from design tokens ---
function defineThemes(): void {
  monaco.editor.defineTheme('tailrace-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'comment', foreground: '5d6b7a', fontStyle: 'italic' },
      { token: 'keyword', foreground: '818CF8' },
      { token: 'string', foreground: '34D399' },
      { token: 'number', foreground: 'FBBF24' },
    ],
    colors: {
      'editor.background': neutral.dark['bg-card'],
      'editorGutter.background': neutral.dark['bg-card'],
      'editor.lineHighlightBackground': neutral.dark['bg-pop'],
      'editorLineNumber.foreground': neutral.dark['text-3'],
      'editorCursor.foreground': semantic.dark.successColor,
    },
  })
  monaco.editor.defineTheme('tailrace-light', {
    base: 'vs',
    inherit: true,
    rules: [
      { token: 'comment', foreground: '98A4B3', fontStyle: 'italic' },
      { token: 'keyword', foreground: '6366F1' },
      { token: 'string', foreground: '10B981' },
      { token: 'number', foreground: 'F59E0B' },
    ],
    colors: {
      'editor.background': neutral.light['bg-card'],
      'editorGutter.background': neutral.light['bg-card'],
      'editor.lineHighlightBackground': '#F4F6F9',
      'editorLineNumber.foreground': neutral.light['text-3'],
      'editorCursor.foreground': semantic.light.successColor,
    },
  })
}

let editor: monaco.editor.IStandaloneCodeEditor | null = null
let applying = false
let layoutRo: ResizeObserver | null = null

// T9: this component is an async chunk — its CSS (monaco base styles
// included) can apply AFTER the JS has run. An editor created against the
// unstyled DOM ends up half-built and pinned at a garbage width
// (automaticLayout observes the editor node itself, whose inline width never
// changes, so it never recovers). Gate creation on monaco's stylesheet being
// live; ResizeObserver (not rAF polling) so a background-tab mount can't wedge.
function whenLaidOut(el: HTMLElement): Promise<void> {
  return new Promise((resolve) => {
    const done = () => resolve()
    if (el.clientWidth >= 50) return done()
    const ro = new ResizeObserver(() => {
      if (el.clientWidth >= 50) {
        ro.disconnect()
        done()
      }
    })
    ro.observe(el)
    setTimeout(() => {
      ro.disconnect()
      done() // safety net: never block editor creation on layout quirks
    }, 3000)
  })
}

function whenMonacoCssReady(): Promise<void> {
  return new Promise((resolve) => {
    const ok = () =>
      Array.from(document.styleSheets).some((s) => {
        try {
          return Array.from(s.cssRules).some((r) => r.cssText.includes('.monaco-editor'))
        } catch {
          return false // cross-origin sheet: unreadable
        }
      })
    if (ok()) return resolve()
    let n = 0
    const iv = setInterval(() => {
      if (ok() || ++n > 250) {
        clearInterval(iv)
        resolve()
      }
    }, 20)
  })
}

onMounted(async () => {
  if (!hostEl.value) return
  await Promise.all([whenLaidOut(hostEl.value), whenMonacoCssReady()])
  defineThemes()
  registerProvider()
  editor = monaco.editor.create(hostEl.value, {
    value: props.modelValue,
    language: 'sql',
    theme: theme.mode === 'dark' ? 'tailrace-dark' : 'tailrace-light',
    fontFamily: fontMono,
    fontSize: 13,
    lineHeight: 20,
    minimap: { enabled: false },
    automaticLayout: true,
    scrollBeyondLastLine: false,
    tabSize: 2,
    padding: { top: 8, bottom: 8 },
    fixedOverflowWidgets: true,
    renderWhitespace: 'selection',
  })
  activeEditor = editor
  activeModel = editor.getModel()
  tagCommandId = editor.addCommand(0, (...args: unknown[]) => {
    const line = args[0]
    if (typeof line === 'number') insertTagAbove(line)
  })
  editor.onDidChangeModelContent(() => {
    if (applying) return
    emit('update:modelValue', editor!.getValue())
  })
  applyMarkers()
  // T9: belt-and-braces — force a re-measure whenever the host resizes, so a
  // late-applying stylesheet (or any container change) can't pin the editor
  // at a stale width.
  layoutRo = new ResizeObserver(() => editor?.layout())
  layoutRo.observe(hostEl.value)
})

// external value → editor (guard the echo)
watch(
  () => props.modelValue,
  (v) => {
    if (!editor || editor.getValue() === v) return
    applying = true
    editor.setValue(v)
    applying = false
  },
)

watch(
  () => props.tables,
  (t) => {
    completionTables = t
  },
  { immediate: true, deep: true },
)

function applyMarkers(): void {
  if (!activeModel) return
  monaco.editor.setModelMarkers(
    activeModel,
    'tailrace-sql',
    props.diagnostics.map((d) => ({
      severity: monaco.MarkerSeverity.Error,
      message: d.message,
      startLineNumber: Math.max(1, d.line),
      endLineNumber: Math.max(1, d.line),
      startColumn: 1,
      endColumn: 12,
    })),
  )
}
watch(() => props.diagnostics, applyMarkers, { deep: true })

watch(
  () => theme.mode,
  (mode) => monaco.editor.setTheme(mode === 'dark' ? 'tailrace-dark' : 'tailrace-light'),
)

onBeforeUnmount(() => {
  layoutRo?.disconnect()
  layoutRo = null
  editor?.dispose()
  editor = null
  activeEditor = null
  activeModel = null
})
</script>

<template>
  <div class="sql-monaco" :style="{ height: `${props.height}px` }">
    <div ref="hostEl" class="editor-host" />
    <div v-if="props.modelValue === '' && props.placeholder" class="placeholder" @click="editor?.focus()">
      <pre>{{ props.placeholder }}</pre>
    </div>
  </div>
</template>

<style scoped>
.sql-monaco {
  position: relative;
  border: 1px solid var(--tr-border);
  border-radius: var(--tr-radius-code);
  overflow: hidden;
}

.editor-host {
  width: 100%;
  height: 100%;
}

.placeholder {
  position: absolute;
  inset: 8px 0 0 16px;
  pointer-events: none;
  color: var(--tr-text-3);
}

.placeholder pre {
  margin: 0;
  font-family: var(--tr-font-mono);
  font-size: 13px;
  line-height: 20px;
}
</style>
