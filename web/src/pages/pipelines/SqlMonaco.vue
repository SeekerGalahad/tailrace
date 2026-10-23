<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
// T22 (from T17 PoC): CodeMirror 6 kernel behind the unchanged SqlMonaco
// contract (props/emits/template identical to the monaco version so edit.vue
// needs zero changes). `basicSetup` is the editor-side feature set comparable
// to the 25 monaco contribs T9 kept (history/fold/bracket-match/
// close-brackets/search/multi-selection/autocompletion/active-line/
// selection-matches). No worker, no external CSS, no icon font: CM6 injects
// its styles from JS.
import { basicSetup } from 'codemirror'
import { Compartment, EditorState, type Extension } from '@codemirror/state'
import { EditorView } from '@codemirror/view'
import { HighlightStyle, indentUnit, syntaxHighlighting, LanguageSupport } from '@codemirror/language'
import {
  StandardSQL,
  keywordCompletionSource,
  schemaCompletionSource,
  type SQLNamespace,
} from '@codemirror/lang-sql'
import {
  pickedCompletion,
  type Completion,
  type CompletionContext,
  type CompletionResult,
  type CompletionSource,
} from '@codemirror/autocomplete'
import { setDiagnostics, type Diagnostic as CmDiagnostic } from '@codemirror/lint'
import { tags } from '@lezer/highlight'
import { useThemeStore } from '../../stores/theme'
import { fontMono, neutral, semantic, type ThemeMode } from '../../theme/tokens'
import type { Diagnostic } from '../../api/pipelines'

/**
 * SqlMonaco (design doc §6.5.2) — CodeMirror 6 kernel (T22, T17 PoC base).
 *
 * - SQL language + `库名.` table-object completion (detail shows
 *   kind · format) + `库名.表名.` column completion from the registered
 *   columns (T39 ⑤); applying a table completion auto-inserts `-- @tag:ds`
 *   above the current line when missing (publish-gate requirement) as its own
 *   undo step. Keyword completion comes with lang-sql.
 * - compile diagnostics → lint diagnostics (wavy underlines)
 * - theme follows ThemeStore (colors from tokens, never hardcoded)
 */

const props = withDefaults(
  defineProps<{
    modelValue: string
    /** Compile diagnostics to mirror as editor markers. */
    diagnostics?: Diagnostic[]
    /** Completion source: registered table objects (+ their `[name, type]` columns, T39 ⑤). */
    tables?: Array<{
      dbName: string
      tableName: string
      kind: string
      format: string | null
      columns?: [string, string][]
    }>
    height?: number
    placeholder?: string
  }>(),
  { diagnostics: () => [], tables: () => [], height: 560, placeholder: undefined },
)

const emit = defineEmits<{ 'update:modelValue': [value: string] }>()

const theme = useThemeStore()
const hostEl = ref<HTMLDivElement | null>(null)

// --- completion: tables → lang-sql schema namespace ---
interface CompletionTable {
  dbName: string
  tableName: string
  kind: string
  format: string | null
  /** registered columns (console list-tables `columnsJson`); absent/empty = no column completion */
  columns?: [string, string][]
}

/** Column completions for a table (`db.table.` member slot): label = column, detail = type. */
function columnCompletions(t: CompletionTable): Completion[] {
  return (t.columns ?? []).map(([name, type]) => ({ label: name, type: 'property', detail: type }))
}

const DS_TAG = '-- @tag:ds\n'

/**
 * Insert the picked table name and, when the line above lacks it, `-- @tag:ds`.
 * Two dispatches = two undo steps (monaco pushUndoStop parity): first undo
 * removes the tag line, second removes the inserted table name.
 */
function applyTable(view: EditorView, completion: Completion, from: number, to: number): void {
  const doc = view.state.doc
  const line = doc.lineAt(from)
  const prev = line.number > 1 ? doc.line(line.number - 1).text : ''
  const tagged = /^\s*--\s*@tag/.test(prev)
  view.dispatch({
    changes: { from, to, insert: completion.label },
    selection: { anchor: from + completion.label.length },
    userEvent: 'input.complete',
    annotations: pickedCompletion.of(completion),
  })
  if (tagged) return
  view.dispatch({
    changes: { from: line.from, insert: DS_TAG },
    selection: { anchor: line.from + DS_TAG.length + (from - line.from) + completion.label.length },
    userEvent: 'input.complete',
  })
}

/**
 * `{ db: { self, children: { table: { self, children: [column…] } } } }` —
 * the `children` slot carries the table's registered columns (console
 * list-tables `columnsJson`, T39 ⑤ — the DTO hop T22 found missing), so
 * `db.table.` column completion works with zero further editor code.
 */
function buildSchema(tables: CompletionTable[]): SQLNamespace {
  const dbs: Record<string, { self: Completion; children: Record<string, SQLNamespace> }> = {}
  for (const t of tables) {
    const db = (dbs[t.dbName] ??= { self: { label: t.dbName, type: 'namespace' }, children: {} })
    db.children[t.tableName] = {
      self: {
        label: t.tableName,
        type: 'type',
        detail: [t.kind, t.format].filter(Boolean).join(' · '),
        apply: applyTable,
      },
      children: columnCompletions(t),
    }
  }
  return dbs
}

// --- T22 delta alignment: case-insensitive `db.` walk + keyword gating ---
// lang-sql's schema source looks members up by exact key, so `SHOP.` (db
// registered as `shop`) completes nothing, and its keyword source fires in
// member context too (`db.e` mixes SELECT/ELSE… in). Keep the library source
// for what it does better (FROM-alias awareness, top-level db names) and add:
// a text-driven member source that walks the chain case-insensitively, plus a
// keyword source that stays silent inside a dotted chain.

/** A trailing dotted member chain: `shop.` / `shop.orders.` + partial word. */
const MEMBER_CHAIN = /((?:[A-Za-z_][A-Za-z0-9_]*\.)+)(\w*)$/

function memberCompletion(tables: CompletionTable[]): CompletionSource {
  // lowercased-key node tree mirroring buildSchema's shape; table nodes carry
  // their registered columns as leaf children (T39 ⑤), so the chain walk
  // below serves `db.table.<col>` the same case-insensitive way as `db.<table>`
  interface MemberNode {
    completion: Completion
    children: Map<string, MemberNode>
  }
  const root = new Map<string, MemberNode>()
  for (const t of tables) {
    const dbKey = t.dbName.toLowerCase()
    let db = root.get(dbKey)
    if (!db) root.set(dbKey, (db = { completion: { label: t.dbName, type: 'namespace' }, children: new Map() }))
    db.children.set(t.tableName.toLowerCase(), {
      completion: {
        label: t.tableName,
        type: 'type',
        detail: [t.kind, t.format].filter(Boolean).join(' · '),
        apply: applyTable,
      },
      children: new Map(
        columnCompletions(t).map((c): [string, MemberNode] => [c.label.toLowerCase(), { completion: c, children: new Map() }]),
      ),
    })
  }
  return (ctx: CompletionContext): CompletionResult | null => {
    const chain = MEMBER_CHAIN.exec(ctx.state.sliceDoc(0, ctx.pos))
    if (!chain) return null
    let node = { children: root } as MemberNode // sentinel: walks db→table→column
    for (const seg of chain[1].slice(0, -1).split('.')) {
      const next = node.children.get(seg.toLowerCase())
      if (!next) return null // unknown member → offer nothing (monaco parity)
      node = next
    }
    return {
      from: ctx.pos - chain[2].length,
      options: [...node.children.values()].map((n) => n.completion),
      validFor: /^\w*$/,
    }
  }
}

/** lang-sql keywords — upper-cased, but never inside a `db.` member chain. */
function keywordCompletion(): CompletionSource {
  const keywords = keywordCompletionSource(StandardSQL, true)
  return (ctx: CompletionContext) =>
    MEMBER_CHAIN.test(ctx.state.sliceDoc(0, ctx.pos)) ? null : keywords(ctx)
}

function sqlSupport(tables: CompletionTable[]): Extension {
  // same two language-data entries sql() would register (schema + keywords),
  // with the keyword source gated, plus the case-insensitive member source
  return new LanguageSupport(StandardSQL.language, [
    StandardSQL.language.data.of({
      autocomplete: schemaCompletionSource({ dialect: StandardSQL, schema: buildSchema(tables) }),
    }),
    StandardSQL.language.data.of({ autocomplete: memberCompletion(tables) }),
    StandardSQL.language.data.of({ autocomplete: keywordCompletion() }),
  ])
}

// --- theme from design tokens ---
function makeTheme(mode: ThemeMode): Extension {
  const n = neutral[mode]
  const dark = mode === 'dark'
  const activeLine = dark ? n['bg-pop'] : '#F4F6F9'
  return [
    EditorView.theme(
      {
        '&': { height: '100%', backgroundColor: n['bg-card'], color: n['text-1'], fontSize: '13px' },
        '&.cm-focused': { outline: 'none' },
        '.cm-scroller': { overflow: 'auto', fontFamily: fontMono, lineHeight: '20px' },
        '.cm-content': { padding: '8px 0' },
        '.cm-gutters': { backgroundColor: n['bg-card'], color: n['text-3'], border: 'none' },
        '.cm-activeLine': { backgroundColor: activeLine },
        '.cm-activeLineGutter': { backgroundColor: activeLine },
        '.cm-cursor, .cm-dropCursor': { borderLeftColor: semantic[mode].successColor },
        '.cm-tooltip': { backgroundColor: n['bg-pop'], color: n['text-1'], border: `1px solid ${n.border}` },
        '.cm-tooltip.cm-tooltip-autocomplete > ul > li[aria-selected]': {
          backgroundColor: dark ? n.border : '#E8ECF2',
          color: n['text-1'],
        },
        '.cm-completionDetail': { color: n['text-3'] },
      },
      { dark },
    ),
    syntaxHighlighting(
      HighlightStyle.define([
        { tag: tags.comment, color: dark ? '#5d6b7a' : '#98A4B3', fontStyle: 'italic' },
        { tag: tags.keyword, color: dark ? '#818CF8' : '#6366F1' },
        { tag: tags.string, color: dark ? '#34D399' : '#10B981' },
        { tag: tags.number, color: dark ? '#FBBF24' : '#F59E0B' },
      ]),
    ),
  ]
}

const langCompartment = new Compartment()
const themeCompartment = new Compartment()

let view: EditorView | null = null
let applying = false

function toCmDiagnostics(state: EditorState, ds: Diagnostic[]): CmDiagnostic[] {
  return ds.map((d) => {
    const line = state.doc.line(Math.min(Math.max(1, d.line), state.doc.lines))
    // monaco version marked columns 1..12 — mirror that span, clamped to the line
    return { from: line.from, to: Math.min(line.from + 11, line.to), severity: 'error', message: d.message }
  })
}

function applyMarkers(): void {
  if (!view) return
  view.dispatch(setDiagnostics(view.state, toCmDiagnostics(view.state, props.diagnostics)))
}

onMounted(() => {
  if (!hostEl.value) return
  view = new EditorView({
    parent: hostEl.value,
    state: EditorState.create({
      doc: props.modelValue,
      extensions: [
        basicSetup,
        langCompartment.of(sqlSupport(props.tables)),
        themeCompartment.of(makeTheme(theme.mode)),
        EditorState.tabSize.of(2),
        indentUnit.of('  '),
        EditorView.updateListener.of((u) => {
          if (u.docChanged && !applying) emit('update:modelValue', u.state.doc.toString())
        }),
      ],
    }),
  })
  applyMarkers()
})

// external value → editor (guard the echo)
watch(
  () => props.modelValue,
  (v) => {
    if (!view) return
    const current = view.state.doc.toString()
    if (current === v) return
    applying = true
    view.dispatch({ changes: { from: 0, to: current.length, insert: v } })
    applying = false
  },
)

watch(
  () => props.tables,
  (t) => view?.dispatch({ effects: langCompartment.reconfigure(sqlSupport(t)) }),
  { deep: true },
)

watch(() => props.diagnostics, applyMarkers, { deep: true })

watch(
  () => theme.mode,
  (mode) => view?.dispatch({ effects: themeCompartment.reconfigure(makeTheme(mode)) }),
)

onBeforeUnmount(() => {
  view?.destroy()
  view = null
})
</script>

<template>
  <div class="sql-monaco" :style="{ height: `${props.height}px` }">
    <div ref="hostEl" class="editor-host" />
    <div v-if="props.modelValue === '' && props.placeholder" class="placeholder" @click="view?.focus()">
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
