<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { onBeforeRouteLeave, useRoute, useRouter } from 'vue-router'
import {
  useDialog,
  useMessage,
  NAlert,
  NButton,
  NCard,
  NCollapse,
  NCollapseItem,
  NDynamicInput,
  NDrawer,
  NDrawerContent,
  NForm,
  NFormItem,
  NGrid,
  NGi,
  NInput,
  NInputNumber,
  NRadioButton,
  NRadioGroup,
  NResult,
  NSelect,
  NSkeleton,
  NSwitch,
  NText,
  type SelectOption,
} from 'naive-ui'
import { ArrowBackOutline, DocumentTextOutline } from '@vicons/ionicons5'
import { pipelinesApi, type Diagnostic, type PipelineConfig, type PipelineDetail, type PipelineStatus, type PipelineTemplate } from '../../api/pipelines'
import { nodesApi, type NodeListItem } from '../../api/nodes'
import { datasourcesApi, type Datasource, type TableObject } from '../../api/datasources'
import { ApiError } from '../../api/http'
import StateTag from '../../components/StateTag.vue'
import EmptyState from '../../components/EmptyState.vue'
import SqlMonaco from './SqlMonaco.vue'

/**
 * SQL editor (design doc §6.5.2): SQL-as-pipeline authoring — basic form +
 * monaco editor with table completion + tag validation + local precheck +
 * db→datasource mapping + advanced config (5 sections) + three-layer dirty
 * guard + compile-gated save.
 */

const route = useRoute()
const router = useRouter()
const message = useMessage()
const dialog = useDialog()

const SQL_PLACEHOLDER = `-- @tag:ds
INSERT INTO ods.orders SELECT id, payload FROM shop.orders`

// --- mode / id ---
const mode = computed<'create' | 'edit'>(() => (route.query.mode === 'edit' ? 'edit' : 'create'))
const pipelineId = ref<number | null>(mode.value === 'edit' ? Number(route.query.id) || null : null)

// --- form state ---
const name = ref('')
const preferredNodeId = ref<number | null>(null)
const checkpointMs = ref(10_000)
const sql = ref('')
interface MappingRow { db: string | null; dsId: number | null }
const mappingRows = ref<MappingRow[]>([])

/** Editor-side config superset: §4.5 five sections carry two write-side fields
 * the shared PipelineConfig type omits; passed through verbatim on save. */
interface EditorConfig extends PipelineConfig {
  decodeFormat?: string | null
  labelPrefix?: string | null
  streamLoadUrl?: string | null
}
const config = ref<EditorConfig>({})
const backoffText = ref('')

// --- detail / status ---
const detail = ref<PipelineDetail | null>(null)
const status = ref<PipelineStatus | null>(null)
const loading = ref(true)
const loadError = ref<string | null>(null)
const notFound = ref(false)

// --- dependencies ---
const onlineNodes = ref<NodeListItem[]>([])
const datasources = ref<Datasource[]>([])
const tables = ref<TableObject[]>([])
const templates = ref<PipelineTemplate[]>([])
const depErrors = ref<string[]>([])

async function loadDeps(): Promise<void> {
  const errs: string[] = []
  const jobs = await Promise.allSettled([
    nodesApi.list({ silent: true }),
    datasourcesApi.list(undefined, { silent: true }),
    datasourcesApi.listTables(undefined, { silent: true }),
    pipelinesApi.templates({ silent: true }),
  ])
  if (jobs[0].status === 'fulfilled') onlineNodes.value = jobs[0].value.nodes.filter((n) => n.status === 'ONLINE')
  else errs.push('节点列表')
  if (jobs[1].status === 'fulfilled') datasources.value = jobs[1].value.items
  else errs.push('数据源列表')
  if (jobs[2].status === 'fulfilled') tables.value = jobs[2].value.items
  else errs.push('表对象')
  if (jobs[3].status === 'fulfilled') templates.value = jobs[3].value.templates
  else errs.push('SQL 模板')
  depErrors.value = errs
}

async function loadDetail(id: number): Promise<void> {
  try {
    const d = await pipelinesApi.get(id, { silent: true })
    detail.value = d
    status.value = d.status
    name.value = d.name
    sql.value = d.sqlContent
    checkpointMs.value = d.checkpointMs ?? 10_000
    preferredNodeId.value = d.preferredNodeId ?? null
    // Shell first: detail.config carries nulls for unset sections (serde has
    // no skip_serializing_if), and create/default-reset paths start from {};
    // templates below read with `?.` and write via spread (autoRestart pattern).
    config.value = { reliability: {}, recovery: {}, tuning: {}, ...(d.config ?? {}) }
    if (config.value.recovery?.restartBackoffSecs) backoffText.value = config.value.recovery.restartBackoffSecs.join(',')
    mappingRows.value = Object.entries(d.dsMapping ?? {}).map(([db, dsId]) => ({ db, dsId }))
  } catch (e) {
    if (e instanceof ApiError && (e.status === 404 || e.code === 'NOT_FOUND')) notFound.value = true
    else loadError.value = e instanceof Error ? e.message : String(e)
  }
}

onMounted(async () => {
  const jobs: Array<Promise<void>> = [loadDeps()]
  if (mode.value === 'edit' && pipelineId.value != null) jobs.push(loadDetail(pipelineId.value))
  else if (mode.value === 'edit') notFound.value = true // edit without id (§6.5.2 route contract)
  await Promise.all(jobs)
  loading.value = false
  savedSnapshot.value = snapshotCurrent()
  maybeOfferDraft()
})

// --- options ---
const nodeOptions = computed(() => onlineNodes.value.map((n) => ({ label: `${n.name}（ONLINE）`, value: n.id })))

const dbOptions = computed<SelectOption[]>(() => {
  const dbs = Array.from(new Set(tables.value.map((t) => t.dbName)))
  return dbs.map((db) => ({ label: db, value: db }))
})

const dsOptions = computed(() =>
  datasources.value.map((d) => ({
    label: d.name,
    value: d.id,
    render: () =>
      h('div', { class: 'ds-option' }, [
        h('span', null, d.name),
        h(StateTag, { domain: 'kind', value: d.type, size: 'small' }),
      ]),
  })),
)

const templateOptions = computed(() => templates.value.map((t) => ({ label: t.label || t.name, value: t.name })))

const completionTables = computed(() =>
  tables.value.map((t) => ({ dbName: t.dbName, tableName: t.tableName, kind: String(t.kind), format: t.format })),
)

// --- mapping validation (complete rows + unique db, lowercase-normalized) ---
const mappingRecord = computed<Record<string, number>>(() => {
  const out: Record<string, number> = {}
  for (const r of mappingRows.value) {
    if (r.db && r.dsId != null) out[r.db.toLowerCase()] = r.dsId
  }
  return out
})

const mappingErrors = computed<string[]>(() => {
  const errs: string[] = []
  const seen = new Set<string>()
  for (const [i, r] of mappingRows.value.entries()) {
    if (!r.db || r.dsId == null) errs.push(`第 ${i + 1} 行映射不完整（库名与数据源均必填）`)
    else {
      const key = r.db.toLowerCase()
      if (seen.has(key)) errs.push(`库名 ${r.db} 重复映射（一库一源）`)
      seen.add(key)
    }
  }
  return errs
})

// --- SQL scanning: db.table refs vs mapping (tag validation, §6.5.2 flow 3) ---
const mappedDbs = computed(() => new Set(Object.keys(mappingRecord.value).map((d) => d.toLowerCase())))

interface RefHit { line: number; db: string }
const sqlRefs = computed<RefHit[]>(() => {
  const hits: RefHit[] = []
  const lines = sql.value.split('\n')
  const re = /([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)/g
  lines.forEach((text, i) => {
    const noComment = text.replace(/--.*$/, '')
    for (const m of noComment.matchAll(re)) {
      if (mappedDbs.value.has(m[1].toLowerCase())) hits.push({ line: i + 1, db: m[1] })
    }
  })
  return hits
})

interface TagCheck { boundCount: number; missing: Array<{ line: number; dbs: string[] }> }
const tagCheck = computed<TagCheck>(() => {
  const lines = sql.value.split('\n')
  const missingMap = new Map<number, Set<string>>()
  let bound = 0
  for (const hit of sqlRefs.value) {
    const prev = hit.line > 1 ? lines[hit.line - 2] ?? '' : ''
    if (/^\s*--\s*@tag/.test(prev)) bound += 1
    else {
      const set = missingMap.get(hit.line) ?? new Set<string>()
      set.add(hit.db)
      missingMap.set(hit.line, set)
    }
  }
  return {
    boundCount: bound,
    missing: Array.from(missingMap.entries()).map(([line, dbs]) => ({ line, dbs: Array.from(dbs) })),
  }
})

function addTagLines(): void {
  // Insert `-- @tag:ds` above every statement line that references a mapped
  // db but lacks a tag line (bottom-up so line numbers stay valid).
  const lines = sql.value.split('\n')
  for (const miss of tagCheck.value.missing) lines.splice(miss.line - 1, 0, '-- @tag:ds')
  sql.value = lines.join('\n')
  message.success('已补标签行')
}

// --- local precheck (mirror of backend compile rules, §6.5.2 flow 3) ---
const precheckErrors = computed<string[]>(() => {
  const errs: string[] = []
  const lines = sql.value.split('\n')
  // statements = non-empty blocks between ';'
  let stmtStartLine = 0
  let stmt: string[] = []
  const check = (stmtLines: string[], startLine: number): void => {
    if (stmtLines.length === 0) return
    const body = stmtLines.filter((l) => !/^\s*--/.test(l)).join(' ')
    if (body.trim() === '') return
    if (!/^\s*INSERT\b/i.test(body)) {
      errs.push(`第 ${startLine} 行：仅支持 INSERT 语句（当前语句不以 INSERT 开头）`)
    }
    if (/\bJOIN\b/i.test(body.replace(/'[^']*'/g, ''))) {
      errs.push(`第 ${startLine} 行：JOIN 暂不支持（阻断保存）`)
    }
  }
  for (let i = 0; i < lines.length; i++) {
    if (stmt.length === 0) stmtStartLine = i + 1
    stmt.push(lines[i])
    if (lines[i].includes(';')) {
      check(stmt, stmtStartLine)
      stmt = []
    }
  }
  check(stmt, stmtStartLine)
  return errs
})

// --- compile diagnostics (server-side final gate) ---
const diagnostics = ref<Diagnostic[]>([])
const compileChecking = ref(false)

function diagnosticsFromError(e: unknown): Diagnostic[] {
  if (e instanceof ApiError) {
    const d = e.data as { diagnostics?: Diagnostic[]; error?: { diagnostics?: Diagnostic[] } } | null
    return d?.diagnostics ?? d?.error?.diagnostics ?? []
  }
  return []
}

async function doCompile(): Promise<boolean> {
  compileChecking.value = true
  try {
    const res = await pipelinesApi.compile({ sql: sql.value, dsMapping: mappingRecord.value })
    if (res.ok) {
      diagnostics.value = []
      return true
    }
    diagnostics.value = res.diagnostics ?? []
    return false
  } catch (e) {
    const ds = diagnosticsFromError(e)
    if (ds.length > 0) diagnostics.value = ds
    else return false // network/5xx: toast shown, no fake diagnostics
    return false
  } finally {
    compileChecking.value = false
  }
}

async function checkSql(): Promise<void> {
  const ok = await doCompile()
  if (ok) message.success(`编译通过：${statementCountHint()} 条语句`)
  else if (diagnostics.value.length > 0) message.error(`编译失败：${diagnostics.value.length} 处诊断`)
}

function statementCountHint(): number {
  return (sql.value.match(/^\s*INSERT\b/gim) ?? []).length
}

// --- payload / dirty tracking ---
function buildConfig(): PipelineConfig {
  const backoffs = backoffText.value
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0)
  const cfg: EditorConfig = { ...config.value }
  if (backoffs.length > 0) {
    cfg.recovery = { ...(cfg.recovery ?? {}), restartBackoffSecs: backoffs }
  }
  return cfg as PipelineConfig
}

function buildPayload() {
  return {
    name: name.value.trim(),
    sqlContent: sql.value,
    dsMapping: mappingRecord.value,
    checkpointMs: checkpointMs.value,
    preferredNodeId: preferredNodeId.value,
    config: buildConfig(),
  }
}

const savedSnapshot = ref('')
function snapshotCurrent(): string {
  return JSON.stringify([name.value, sql.value, mappingRows.value, checkpointMs.value, preferredNodeId.value, config.value, backoffText.value])
}
const dirty = computed(() => snapshotCurrent() !== savedSnapshot.value)

// --- save flow (§6.5.2 flow 4) ---
const saving = ref(false)
const savedPendingInfo = ref(false)

async function save(): Promise<void> {
  if (saving.value) return
  if (!name.value.trim()) {
    message.error('管道名必填')
    return
  }
  if (precheckErrors.value.length > 0) return // red bar blocks save
  if (mappingErrors.value.length > 0) {
    message.error(mappingErrors.value[0])
    return
  }
  const ok = await doCompile()
  if (!ok) {
    if (diagnostics.value.length > 0) message.error('编译诊断未通过，已标注到编辑器')
    return
  }
  saving.value = true
  try {
    if (mode.value === 'create' || pipelineId.value == null) {
      const res = await pipelinesApi.create(buildPayload())
      pipelineId.value = res.id
      savedSnapshot.value = snapshotCurrent()
      clearDraft()
      message.success(`已创建管道 #${res.id}`)
      await router.replace({ path: '/pipelines/edit', query: { mode: 'edit', id: String(res.id) } })
    } else {
      const wasDraft = status.value === 'DRAFT' || status.value == null
      await pipelinesApi.update(pipelineId.value, buildPayload())
      savedSnapshot.value = snapshotCurrent()
      clearDraft()
      if (!wasDraft) {
        savedPendingInfo.value = true
        status.value = 'PENDING_CHANGES'
        message.success('已保存')
      } else {
        message.success('已保存')
      }
    }
  } catch {
    /* 4xx toast already shown */
  } finally {
    saving.value = false
  }
}

const canPublish = computed(
  () =>
    !dirty.value &&
    pipelineId.value != null &&
    (status.value === 'DRAFT' || status.value === 'PENDING_CHANGES'),
)

function goPublish(): void {
  if (pipelineId.value != null) void router.push(`/pipelines/${pipelineId.value}/publish`)
}

// --- template insert (append to SQL end, §6.5.2) ---
const templatePick = ref<string | null>(null)

function onTemplatePick(name_: string): void {
  const tpl = templates.value.find((t) => t.name === name_)
  if (tpl) {
    sql.value = sql.value.trim() === '' ? tpl.sql : `${sql.value.replace(/\s*$/, '')}\n${tpl.sql}`
    message.success(`已追加模板「${tpl.label || tpl.name}」到 SQL 末尾`)
  }
  templatePick.value = null
}

// --- capability manual drawer (static content, §6.5.2) ---
const MANUAL_SECTIONS = [
  {
    title: '语句形态',
    items: [
      { note: '每条语句 = 一段 FROM 源到 INSERT 目标的搬运', sql: 'INSERT INTO ods.orders SELECT id, payload FROM shop.orders' },
      { note: '多语句以分号分隔，逐条独立消费组', sql: '-- @tag:ds\nINSERT INTO ods.orders SELECT id, payload FROM shop.orders;\n-- @tag:ds\nINSERT INTO ods.payments SELECT id, amount FROM shop.payments' },
    ],
  },
  {
    title: '函数面',
    items: [
      { note: '行内变换支持标量函数（以编译器诊断为准）', sql: 'INSERT INTO ods.orders SELECT id, LOWER(payload) AS payload FROM shop.orders' },
    ],
  },
  {
    title: '聚合窗口',
    items: [{ note: '聚合/窗口在微批内计算（微批口径见编译产物）', sql: 'INSERT INTO ods.orders_agg SELECT COUNT(*) AS n FROM shop.orders' }],
  },
  {
    title: '子查询 / CTE',
    items: [{ note: 'FROM 子查询可用；WITH CTE 视编译诊断而定', sql: 'INSERT INTO ods.orders SELECT * FROM (SELECT id FROM shop.orders) t' }],
  },
]
const UNSUPPORTED = ['JOIN（多表联结）', '非 INSERT 语句（SELECT/UPDATE/DELETE/DDL）', '跨数据源的星型展开']
const manualDrawer = ref(false)

// --- dirty guards: three layers (§6.5.2 flow 5) ---
function onBeforeUnload(e: BeforeUnloadEvent): void {
  if (dirty.value) e.preventDefault()
}

function onKeydown(e: KeyboardEvent): void {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 's') {
    e.preventDefault()
    void save()
  }
}

onMounted(() => {
  window.addEventListener('beforeunload', onBeforeUnload)
  window.addEventListener('keydown', onKeydown)
})
onBeforeUnmount(() => {
  window.removeEventListener('beforeunload', onBeforeUnload)
  window.removeEventListener('keydown', onKeydown)
})

onBeforeRouteLeave(async () => {
  if (!dirty.value) return true
  return await new Promise<boolean>((resolve) => {
    dialog.warning({
      title: '未保存的修改',
      content: '离开将丢失未保存的修改（草稿已自动快照到本地，可回来恢复）。',
      positiveText: '放弃修改',
      negativeText: '留下',
      onPositiveClick: () => resolve(true),
      onNegativeClick: () => resolve(false),
      onClose: () => resolve(false),
      onMaskClick: () => resolve(false),
    })
  })
})

// --- sessionStorage draft snapshot (debounced 1s, §6.5.2 flow 5) ---
const draftKey = computed(() => `tr_pipe_draft_${pipelineId.value ?? 'new'}`)

let draftTimer: ReturnType<typeof setTimeout> | null = null
watch(
  [name, sql, mappingRows, checkpointMs, preferredNodeId, config, backoffText],
  () => {
    if (loading.value || !dirty.value) return
    if (draftTimer) clearTimeout(draftTimer)
    draftTimer = setTimeout(() => {
      try {
        sessionStorage.setItem(
          draftKey.value,
          JSON.stringify({ savedAt: Date.now(), name: name.value, sql: sql.value, rows: mappingRows.value, checkpointMs: checkpointMs.value, preferredNodeId: preferredNodeId.value, config: config.value, backoffText: backoffText.value }),
        )
      } catch {
        /* storage full / disabled: non-fatal */
      }
    }, 1000)
  },
  { deep: true },
)

function clearDraft(): void {
  try {
    sessionStorage.removeItem(draftKey.value)
  } catch {
    /* non-fatal */
  }
}

// --- draft restore offer (snapshot newer than the loaded server state) ---
interface DraftSnapshot {
  savedAt?: number
  name: string
  sql: string
  rows: MappingRow[]
  checkpointMs: number
  preferredNodeId: number | null
  config: EditorConfig
  backoffText: string
}
const draftOffer = ref<DraftSnapshot | null>(null)

function maybeOfferDraft(): void {
  try {
    const raw = sessionStorage.getItem(draftKey.value)
    if (!raw) return
    const snap = JSON.parse(raw) as { savedAt: number } & Record<string, unknown>
    const serverAt = detail.value?.updatedAt ? Date.parse(detail.value.updatedAt) : 0
    if (Number.isFinite(serverAt) && snap.savedAt <= serverAt) return
    draftOffer.value = snap as NonNullable<typeof draftOffer.value>
  } catch {
    /* corrupt draft: ignore */
  }
}

function restoreDraft(): void {
  const d = draftOffer.value
  if (d) {
    name.value = d.name
    sql.value = d.sql
    mappingRows.value = d.rows
    checkpointMs.value = d.checkpointMs
    preferredNodeId.value = d.preferredNodeId
    config.value = d.config
    backoffText.value = d.backoffText ?? ''
  }
  draftOffer.value = null
}

function discardDraft(): void {
  clearDraft()
  draftOffer.value = null
}
</script>

<template>
  <main class="page">
    <!-- edit-without-id / 404 -->
    <NResult
      v-if="notFound || (loadError && mode === 'edit')"
      status="404"
      title="管道不存在"
      :description="loadError ?? '编辑模式缺少 id，或该管道已被删除'"
    >
      <template #footer>
        <NButton size="small" @click="router.push('/pipelines')">返回列表</NButton>
      </template>
    </NResult>

    <template v-else>
      <!-- top bar -->
      <div class="page-head">
        <div class="head-left">
          <NButton size="small" quaternary @click="router.push('/pipelines')">
            <template #icon><ArrowBackOutline /></template>
            返回
          </NButton>
          <h1 class="title">
            管道：{{ name || '（未命名）' }}
            <StateTag v-if="status" domain="pipeline" :value="status" class="status-tag" />
          </h1>
        </div>
        <div class="head-actions">
          <NSelect :value="templatePick" :options="templateOptions" size="small" placeholder="插入模板" class="tpl-select" :disabled="templates.length === 0" @update:value="onTemplatePick" />
          <NButton size="small" @click="manualDrawer = true">
            <template #icon><DocumentTextOutline /></template>
            SQL 能力手册
          </NButton>
          <NButton size="small" :loading="compileChecking" @click="checkSql">检查 SQL</NButton>
          <NButton size="small" :disabled="!canPublish" @click="goPublish">发布</NButton>
          <NButton size="small" type="primary" :loading="saving" @click="save">
            {{ mode === 'create' ? '创建' : '保存修改' }} ⌘S
          </NButton>
        </div>
      </div>

      <!-- draft restore banner -->
      <NAlert v-if="draftOffer" type="warning" :bordered="false" closable @close="discardDraft">
        检测到未保存草稿（{{ new Date(draftOffer.savedAt ?? 0).toLocaleString() }} 保存）
        <NButton size="tiny" type="primary" style="margin-left: 12px" @click="restoreDraft">恢复</NButton>
        <NButton size="tiny" style="margin-left: 8px" @click="discardDraft">放弃</NButton>
      </NAlert>

      <!-- dependency failures -->
      <NAlert v-if="depErrors.length > 0" type="error" :bordered="false">
        依赖加载失败：{{ depErrors.join('、') }}
        <NButton size="tiny" style="margin-left: 12px" @click="loadDeps">重试</NButton>
      </NAlert>

      <!-- loading skeletons -->
      <NSkeleton v-if="loading" text :repeat="2" />
      <template v-else>
        <!-- basic area -->
        <NGrid :cols="1" :x-gap="16" :y-gap="16" responsive="screen" item-responsive class="basic-grid">
          <NGi span="1 m:3">
            <NCard size="small" :content-style="{ padding: '16px' }">
              <NForm label-placement="top" inline :show-feedback="false" class="basic-form">
                <NFormItem label="管道名" required class="grow">
                  <NInput v-model:value="name" placeholder="orders→ods" />
                </NFormItem>
                <NFormItem label="预指定节点" class="grow">
                  <NSelect v-model:value="preferredNodeId" clearable :options="nodeOptions" placeholder="可空（发布时自动分配）" />
                </NFormItem>
                <NFormItem label="checkpoint（ms）" required>
                  <NInputNumber v-model:value="checkpointMs" :min="1000" :step="1000" style="width: 160px" />
                </NFormItem>
              </NForm>
            </NCard>
          </NGi>
        </NGrid>

        <!-- SQL editor -->
        <NCard size="small" :content-style="{ padding: '8px' }">
          <SqlMonaco v-model="sql" :diagnostics="diagnostics" :tables="completionTables" :height="560" :placeholder="SQL_PLACEHOLDER" />
          <NAlert v-if="tables.length === 0 && depErrors.includes('表对象')" type="warning" :bordered="false" class="editor-note">
            表对象加载失败——补全与映射不可用
          </NAlert>
          <NAlert v-else-if="tables.length === 0" type="info" :bordered="false" class="editor-note">
            尚无表对象——先在数据源页登记表对象，编辑器才能补全
          </NAlert>
        </NCard>

        <!-- tag validation bar -->
        <template v-if="sql.trim() !== ''">
          <NAlert v-if="precheckErrors.length > 0" type="error" :bordered="false" title="预检未通过（阻断保存）">
            <ul class="diag-list">
              <li v-for="e in precheckErrors" :key="e">{{ e }}</li>
            </ul>
          </NAlert>
          <template v-else>
            <NAlert v-if="tagCheck.missing.length === 0" type="success" :bordered="false">
              标签校验通过：{{ tagCheck.boundCount }} 处绑定
            </NAlert>
            <NAlert v-else type="warning" :bordered="false">
              <div>
                <span v-for="miss in tagCheck.missing" :key="miss.line" class="tag-miss">
                  第 {{ miss.line }} 行：使用映射库名 {{ miss.dbs.join('/') }} 但上一行缺 <code>-- @tag:ds</code> 标签
                </span>
                <NButton size="tiny" type="primary" style="margin-left: 12px" @click="addTagLines">一键补标签</NButton>
              </div>
            </NAlert>
            <NAlert v-if="diagnostics.length > 0" type="error" :bordered="false" title="编译诊断">
              <ul class="diag-list">
                <li v-for="d in diagnostics" :key="`${d.line}-${d.message}`">第 {{ d.line }} 行：{{ d.message }}</li>
              </ul>
            </NAlert>
          </template>
        </template>

        <!-- mapping -->
        <NCard size="small" title="库名 → 数据源映射（一库一源）" :content-style="{ padding: '16px' }">
          <EmptyState
            v-if="mappingRows.length === 0 && tables.length === 0"
            :description="depErrors.includes('表对象') ? '表对象加载失败' : '先在数据源页登记表对象'"
          />
          <NDynamicInput
            v-model:value="mappingRows"
            :on-create="(): MappingRow => ({ db: null, dsId: null })"
          >
            <template #default="{ value }">
              <div class="mapping-row">
                <NSelect v-model:value="value.db" :options="dbOptions" placeholder="库名" filterable class="mapping-db" />
                <NText depth="3">→</NText>
                <NSelect v-model:value="value.dsId" :options="dsOptions" placeholder="数据源" filterable class="mapping-ds" />
              </div>
            </template>
          </NDynamicInput>
          <NAlert v-if="mappingErrors.length > 0" type="error" :bordered="false" class="mapping-error">
            <ul class="diag-list">
              <li v-for="e in mappingErrors" :key="e">{{ e }}</li>
            </ul>
          </NAlert>
        </NCard>

        <!-- advanced config -->
        <NCard size="small" :content-style="{ padding: '8px 16px' }">
          <template #header>
            高级配置（空 = 引擎默认；config 仍恒下发以覆盖旧值）
          </template>
          <template #header-extra>
            <NButton size="tiny" quaternary @click="config = {}; backoffText = ''">恢复默认</NButton>
          </template>
          <NCollapse display-directive="show">
            <NCollapseItem title="读取" name="read">
              <NForm label-placement="left" label-width="150" size="small" :show-feedback="false" class="config-form">
                <NFormItem label="解码格式覆写">
                  <NSelect v-model:value="config.decodeFormat" clearable :options="['ogg', 'json', 'jsonl', 'csv', 'parquet'].map((v) => ({ label: v, value: v }))" placeholder="默认按表对象 format" class="cfg-input" />
                </NFormItem>
                <NFormItem label="消费组前缀">
                  <NInput v-model:value="config.groupPrefix" placeholder="缺省 tr.p{id}.s{n}" class="cfg-input" />
                </NFormItem>
                <NAlert type="warning" :bordered="false" class="cfg-alert">
                  修改消费组前缀 = 更换消费组丢位点，按起点重新消费。
                </NAlert>
                <NFormItem label="消费起点">
                  <NRadioGroup v-model:value="config.startOffset">
                    <NRadioButton value="earliest">从头（earliest）</NRadioButton>
                    <NRadioButton value="latest">从最新（latest）</NRadioButton>
                  </NRadioGroup>
                  <NText depth="3" class="cfg-hint">仅无已保存位点时生效</NText>
                </NFormItem>
              </NForm>
            </NCollapseItem>
            <NCollapseItem title="写入" name="write">
              <NForm label-placement="left" label-width="150" size="small" :show-feedback="false" class="config-form">
                <NFormItem label="label 前缀覆写">
                  <NInput v-model:value="config.labelPrefix" placeholder="默认引擎自动幂等 label" class="cfg-input" />
                </NFormItem>
                <NFormItem label="Stream Load URL 覆写">
                  <NInput v-model:value="config.streamLoadUrl" placeholder="默认 BE 直连 :8040" class="cfg-input" />
                </NFormItem>
              </NForm>
            </NCollapseItem>
            <NCollapseItem title="交付可靠性" name="reliability">
              <NForm label-placement="left" label-width="150" size="small" :show-feedback="false" class="config-form">
                <NFormItem label="Doris 重试次数"><NInputNumber :value="config.reliability?.dorisFlushRetries" :min="0" class="cfg-num" @update:value="(v: number | null) => { config.reliability = { ...(config.reliability ?? {}), dorisFlushRetries: v } }" /></NFormItem>
                <NFormItem label="重试退避基数（ms）"><NInputNumber :value="config.reliability?.dorisFlushBackoffBaseMs" :min="0" :step="500" class="cfg-num" @update:value="(v: number | null) => { config.reliability = { ...(config.reliability ?? {}), dorisFlushBackoffBaseMs: v } }" /></NFormItem>
                <NFormItem label="Doris 请求超时（ms）"><NInputNumber :value="config.reliability?.dorisRequestTimeoutMs" :min="1000" :step="1000" class="cfg-num" @update:value="(v: number | null) => { config.reliability = { ...(config.reliability ?? {}), dorisRequestTimeoutMs: v } }" /></NFormItem>
                <NFormItem label="Kafka 投递超时（ms）"><NInputNumber :value="config.reliability?.kafkaSendTimeoutMs" :min="1000" :step="1000" class="cfg-num" @update:value="(v: number | null) => { config.reliability = { ...(config.reliability ?? {}), kafkaSendTimeoutMs: v } }" /></NFormItem>
                <NFormItem label="逐出防护间隔（ms）"><NInputNumber :value="config.reliability?.maxPollIntervalMs" :min="1000" :step="1000" class="cfg-num" @update:value="(v: number | null) => { config.reliability = { ...(config.reliability ?? {}), maxPollIntervalMs: v } }" /></NFormItem>
              </NForm>
            </NCollapseItem>
            <NCollapseItem title="自动恢复" name="recovery">
              <NForm label-placement="left" label-width="150" size="small" :show-feedback="false" class="config-form">
                <NFormItem label="自动重启">
                  <!-- nullable config field: normalize null → undefined for NSwitch -->
                  <NSwitch
                    :value="config.recovery?.autoRestart === true"
                    @update:value="(v: boolean) => { config.recovery = { ...(config.recovery ?? {}), autoRestart: v } }"
                  />
                </NFormItem>
                <NFormItem label="重启上限"><NInputNumber :value="config.recovery?.autoRestartMax" :min="0" class="cfg-num" @update:value="(v: number | null) => { config.recovery = { ...(config.recovery ?? {}), autoRestartMax: v } }" /></NFormItem>
                <NFormItem label="重启退避（秒，逗号分隔）">
                  <NInput v-model:value="backoffText" placeholder="30,60,300" class="cfg-input" />
                </NFormItem>
              </NForm>
            </NCollapseItem>
            <NCollapseItem title="性能调优" name="tuning">
              <NForm label-placement="left" label-width="150" size="small" :show-feedback="false" class="config-form">
                <NFormItem label="攒批行数"><NInputNumber :value="config.tuning?.flushRows" :min="1" :step="1000" class="cfg-num" @update:value="(v: number | null) => { config.tuning = { ...(config.tuning ?? {}), flushRows: v } }" /></NFormItem>
                <NFormItem label="攒批间隔（ms）"><NInputNumber :value="config.tuning?.flushIntervalMs" :min="100" :step="1000" class="cfg-num" @update:value="(v: number | null) => { config.tuning = { ...(config.tuning ?? {}), flushIntervalMs: v } }" /></NFormItem>
                <NFormItem label="攒批字节"><NInputNumber :value="config.tuning?.flushBytes" :min="1024" :step="1048576" class="cfg-num" @update:value="(v: number | null) => { config.tuning = { ...(config.tuning ?? {}), flushBytes: v } }" /></NFormItem>
                <NFormItem label="拉取行数"><NInputNumber :value="config.tuning?.pollRows" :min="1" :step="500" class="cfg-num" @update:value="(v: number | null) => { config.tuning = { ...(config.tuning ?? {}), pollRows: v } }" /></NFormItem>
                <NFormItem label="拉取时间片（ms）"><NInputNumber :value="config.tuning?.pollSliceMs" :min="10" :step="10" class="cfg-num" @update:value="(v: number | null) => { config.tuning = { ...(config.tuning ?? {}), pollSliceMs: v } }" /></NFormItem>
              </NForm>
            </NCollapseItem>
          </NCollapse>
        </NCard>

        <NAlert v-if="savedPendingInfo" type="info" :bordered="false" closable @close="savedPendingInfo = false">
          已保存为待发布变更，需发布后部署才生效。
        </NAlert>
      </template>
    </template>

    <!-- capability manual drawer (§6.5.2: four sections + unsupported list) -->
    <NDrawer v-model:show="manualDrawer" :width="760">
      <NDrawerContent title="SQL 能力手册" closable>
        <section v-for="sec in MANUAL_SECTIONS" :key="sec.title" class="manual-section">
          <h3 class="section-title">{{ sec.title }}</h3>
          <div v-for="(item, i) in sec.items" :key="i" class="manual-item">
            <p class="manual-note">{{ item.note }}</p>
            <pre class="manual-sql">{{ item.sql }}</pre>
            <NButton size="tiny" @click="sql = sql.trim() === '' ? item.sql : `${sql.replace(/\s*$/, '')}\n${item.sql}`; manualDrawer = false">
              插入编辑器
            </NButton>
          </div>
        </section>
        <section class="manual-section">
          <h3 class="section-title">不支持清单</h3>
          <ul class="diag-list">
            <li v-for="u in UNSUPPORTED" :key="u">{{ u }}</li>
          </ul>
          <p class="manual-note">微批口径：语句以微批为单位执行与提交，失败按 at-least-once 重放。</p>
        </section>
      </NDrawerContent>
    </NDrawer>
  </main>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.page-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}

.head-left {
  display: flex;
  align-items: center;
  gap: 8px;
}

.title {
  font-size: var(--tr-fs-title);
  font-weight: 600;
  margin: 0;
  color: var(--tr-text-1);
  display: flex;
  align-items: center;
  gap: 8px;
}

.head-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.tpl-select {
  width: 180px;
}

.basic-form {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
}

.basic-form .grow {
  flex: 1;
  min-width: 220px;
}

.editor-note {
  margin-top: 8px;
}

.diag-list {
  margin: 4px 0 0;
  padding-left: 18px;
}

.tag-miss {
  display: block;
}

.mapping-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex: 1;
}

.mapping-db {
  width: 220px;
}

.mapping-ds {
  flex: 1;
  min-width: 220px;
}

.mapping-error {
  margin-top: 8px;
}

.config-form {
  max-width: 560px;
}

.cfg-input {
  width: 260px;
}

.cfg-num {
  width: 160px;
}

.cfg-alert {
  margin: 4px 0 8px;
  padding: 4px 12px;
}

.cfg-hint {
  margin-left: 12px;
  font-size: var(--tr-fs-caption);
}

.manual-section {
  margin-bottom: 20px;
}

.manual-section .section-title {
  font-size: var(--tr-fs-body);
  font-weight: 500;
  margin: 0 0 8px;
  color: var(--tr-text-1);
}

.manual-item {
  margin-bottom: 12px;
}

.manual-note {
  color: var(--tr-text-2);
  font-size: var(--tr-fs-body-sm);
  margin: 0 0 4px;
}

.manual-sql {
  margin: 0 0 6px;
  padding: 8px 10px;
  background: var(--tr-bg-term);
  color: var(--tr-text-term);
  border-radius: var(--tr-radius-code);
  font-family: var(--tr-font-mono);
  font-size: var(--tr-fs-caption);
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
