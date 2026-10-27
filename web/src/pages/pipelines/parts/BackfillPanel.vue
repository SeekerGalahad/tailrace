<script setup lang="ts">
import { computed, h, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import {
  NAlert,
  NButton,
  NCard,
  NDataTable,
  NDescriptions,
  NDescriptionsItem,
  NForm,
  NFormItem,
  NInputNumber,
  NPopconfirm,
  NSelect,
  NSpin,
  NTag,
  NText,
  NTooltip,
  useMessage,
  type DataTableColumns,
  type SelectOption,
} from 'naive-ui'
import { RefreshOutline } from '@vicons/ionicons5'
import { ApiError } from '../../../api/http'
import {
  pipelinesApi,
  type BackfillJob,
  type BackfillOutcome,
  type CompiledStatement,
  type Diagnostic,
  type PipelineDetail,
} from '../../../api/pipelines'
import type { Datasource } from '../../../api/datasources'
import EmptyState from '../../../components/EmptyState.vue'
import MonoField from '../../../components/MonoField.vue'
import StateTag from '../../../components/StateTag.vue'
import { useAutoRefresh } from '../../../components/useAutoRefresh'

/**
 * Backfill panel on the pipeline monitor (T-A2 ⑤, blueprint §7.3): pick a
 * statement + offset range → dryRun preview (labelPrefix / estimatedRows,
 * zero side effects) → confirm → 202 job → job progress rows (10s poll).
 *
 * Capability gate mirrored client-side (the task card's 回退门): a statement
 * is replayable only when its sink is doris/starrocks (stream-load label
 * idempotency IS the dedup mechanism — file/kafka sinks would double-write)
 * and its source is file (this wave's bound addressing); anything else is
 * greyed out with the reason. The server re-checks and 422s regardless.
 */

const props = defineProps<{
  pipelineId: number
  detail: PipelineDetail | null
  datasources: Datasource[]
}>()

const router = useRouter()
const message = useMessage()

// --- capability (compile the current SQL, resolve each statement's ends) ---
interface StatementCap {
  index: number
  source: string
  sink: string
  sourceType: string | null
  sinkType: string | null
  eligible: boolean
  /** Why it is greyed out (null when eligible). */
  reason: string | null
}

type CapState =
  | { kind: 'loading' }
  | { kind: 'unpublished' }
  | { kind: 'compile-failed'; message: string }
  | { kind: 'ok'; statements: StatementCap[] }
const capState = ref<CapState>({ kind: 'loading' })

/** `dsId.db.table` reference → the datasource's type via the list the page already holds. */
function dsTypeOf(reference: string): string | null {
  const id = Number(reference.split('.')[0])
  if (!Number.isInteger(id)) return null
  return props.datasources.find((d) => d.id === id)?.type ?? null
}

const isLabelSink = (t: string | null): boolean => t === 'doris' || t === 'starrocks'

function capOf(s: CompiledStatement): StatementCap {
  const sourceType = dsTypeOf(s.source)
  const sinkType = dsTypeOf(s.sink)
  let reason: string | null = null
  if (!isLabelSink(sinkType)) {
    reason = `汇为 ${sinkType ?? '未知类型'}——回填仅支持 doris/starrocks 汇（重放去重依赖 stream load label 幂等，file/kafka 汇重放会真重复）`
  } else if (sourceType !== 'file') {
    reason = `源为 ${sourceType ?? '未知类型'}——当前仅支持 file 源（kafka 源的 from 需分区级游标寻址，待后续批次）`
  }
  return { index: s.index, source: s.source, sink: s.sink, sourceType, sinkType, eligible: reason === null, reason }
}

function diagnosticsFromError(e: unknown): Diagnostic[] {
  if (e instanceof ApiError) {
    const d = e.data as { diagnostics?: Diagnostic[]; error?: { diagnostics?: Diagnostic[] } } | null
    return d?.diagnostics ?? d?.error?.diagnostics ?? []
  }
  return []
}

async function loadCapabilities(): Promise<void> {
  const d = props.detail
  if (!d) return
  // the server refuses an unpublished pipeline (no spec snapshot) — say so up front
  if (!d.specJson) {
    capState.value = { kind: 'unpublished' }
    return
  }
  capState.value = { kind: 'loading' }
  try {
    const res = await pipelinesApi.compile({ sql: d.sqlContent, dsMapping: d.dsMapping ?? {} })
    if (!res.ok) {
      capState.value = { kind: 'compile-failed', message: fmtDiagnostics(res.diagnostics) }
      return
    }
    capState.value = { kind: 'ok', statements: (res.statements ?? []).map(capOf) }
  } catch (e) {
    const ds = diagnosticsFromError(e)
    capState.value = {
      kind: 'compile-failed',
      message: ds.length ? fmtDiagnostics(ds) : e instanceof ApiError ? e.message : '编译请求失败',
    }
  }
}

function fmtDiagnostics(ds: Diagnostic[]): string {
  return ds.length ? ds.map((x) => `第 ${x.line} 行：${x.message}`).join('；') : '编译未通过'
}

watch([() => props.detail, () => props.datasources], () => void loadCapabilities(), { immediate: true })

const statements = computed<StatementCap[]>(() => (capState.value.kind === 'ok' ? capState.value.statements : []))
const eligible = computed(() => statements.value.filter((s) => s.eligible))

const statementOptions = computed<SelectOption[]>(() =>
  statements.value.map((s) => ({
    label: `#${s.index}  ${s.source} → ${s.sink}`,
    value: s.index,
    disabled: !s.eligible,
  })),
)

const capColumns: DataTableColumns<StatementCap> = [
  { title: '#', key: 'index', width: 44, render: (s) => `#${s.index}` },
  {
    title: '源',
    key: 'source',
    minWidth: 180,
    render: (s) =>
      h('div', { class: 'cap-cell' }, [
        h(MonoField, { value: s.source, copyable: false }),
        h(StateTag, { domain: 'kind', value: s.sourceType, label: s.sourceType ?? '?' }),
      ]),
  },
  {
    title: '汇',
    key: 'sink',
    minWidth: 180,
    render: (s) =>
      h('div', { class: 'cap-cell' }, [
        h(MonoField, { value: s.sink, copyable: false }),
        h(StateTag, { domain: 'kind', value: s.sinkType, label: s.sinkType ?? '?' }),
      ]),
  },
  {
    title: '可回填',
    key: 'eligible',
    width: 100,
    render: (s) => {
      if (s.eligible) return h(NTag, { size: 'small', type: 'success', bordered: false }, { default: () => '可回填' })
      return h(
        NTooltip,
        { style: 'max-width: 360px' },
        {
          trigger: () => h(NTag, { size: 'small', bordered: false }, { default: () => '不可回填' }),
          default: () => s.reason ?? '',
        },
      )
    },
  },
]

// --- range form ---
const form = reactive<{ statementIndex: number | null; from: number | null; to: number | null }>({
  statementIndex: null,
  from: null,
  to: null,
})

// keep the pick on a replayable statement as capabilities (re)load
watch(eligible, (list) => {
  if (list.length === 0) {
    form.statementIndex = null
  } else if (form.statementIndex == null || !list.some((s) => s.index === form.statementIndex)) {
    form.statementIndex = list[0].index
  }
})

const formError = computed<string | null>(() => {
  if (form.statementIndex == null) return '请选择可回填的语句'
  if (form.from == null || form.to == null) return '请填写 from / to（offset 边界；file 源为字节位）'
  if (!Number.isInteger(form.from) || !Number.isInteger(form.to)) return 'offset 须为整数'
  if (form.from < 0) return 'from 不能为负'
  if (form.from >= form.to) return 'from 须小于 to'
  return null
})

function rangeKey(): string {
  return `${form.statementIndex}:${form.from}:${form.to}`
}

function currentInput(dryRun: boolean) {
  // callers check formError first; the nullish fallbacks only satisfy the types
  return {
    statementIndex: form.statementIndex ?? 0,
    from: { type: 'offset' as const, value: form.from ?? 0 },
    to: { type: 'offset' as const, value: form.to ?? 0 },
    dryRun,
  }
}

// --- dryRun preview (zero side effects) ---
const previewing = ref(false)
const previewError = ref<string | null>(null)
/** Preview pinned to the range it was computed for — a changed range stales it. */
const preview = ref<{ key: string; out: BackfillOutcome } | null>(null)
const previewFresh = computed(() => preview.value != null && preview.value.key === rangeKey())

async function runDryRun(): Promise<void> {
  if (formError.value) return
  previewing.value = true
  previewError.value = null
  preview.value = null
  try {
    const out = await pipelinesApi.backfill(props.pipelineId, currentInput(true), { silent: true })
    preview.value = { key: rangeKey(), out }
  } catch (e) {
    // 422 gate messages come from the server dictionary — show inline
    previewError.value = e instanceof ApiError ? e.message : '预览失败，请稍后重试'
  } finally {
    previewing.value = false
  }
}

// --- execute (202 → pending job) ---
const executing = ref(false)

async function runExecute(): Promise<void> {
  if (formError.value || !previewFresh.value) return
  executing.value = true
  try {
    const out = await pipelinesApi.backfill(props.pipelineId, currentInput(false))
    message.success(`回填 job 已受理：${out.jobId ?? '—'}（${out.state ?? 'pending'}）`)
    preview.value = null
    jobsPoll.kick()
  } catch {
    /* toast shown */
  } finally {
    executing.value = false
  }
}

// --- jobs (progress rows, newest first; 10s poll) ---
const jobs = ref<BackfillJob[]>([])
const jobsLoading = ref(false)
const jobsError = ref<string | null>(null)

async function fetchJobs(): Promise<void> {
  jobsLoading.value = true
  try {
    const res = await pipelinesApi.backfillJobs(props.pipelineId, { silent: true })
    jobs.value = res.jobs
    jobsError.value = null
  } catch (e) {
    jobsError.value = e instanceof ApiError ? e.message : '加载失败'
  } finally {
    jobsLoading.value = false
  }
}
const jobsPoll = useAutoRefresh(fetchJobs, 10_000)

function fmtBound(b: unknown): string {
  if (typeof b === 'object' && b !== null && 'value' in b) return String((b as { value: unknown }).value)
  return b == null ? '—' : String(b)
}

function fmtTime(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  const p = (n: number): string => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

const isTerminal = (state: string): boolean => state === 'done' || state === 'failed'

const jobColumns: DataTableColumns<BackfillJob> = [
  { title: 'job', key: 'jobId', width: 120, render: (j) => h(MonoField, { value: j.jobId, headChars: 8 }) },
  { title: '语句', key: 'statementIndex', width: 60, render: (j) => `#${j.statementIndex}` },
  {
    title: '区间',
    key: 'range',
    minWidth: 140,
    render: (j) => h('span', { class: 'mono-inline' }, `${fmtBound(j.from)} → ${fmtBound(j.to)}`),
  },
  { title: '状态', key: 'state', width: 90, render: (j) => h(StateTag, { domain: 'backfillJob', value: j.state }) },
  {
    title: '写入行',
    key: 'rowsWritten',
    width: 90,
    align: 'right',
    render: (j) => (isTerminal(j.state) ? j.rowsWritten.toLocaleString() : '—'),
  },
  {
    title: '错误',
    key: 'error',
    minWidth: 160,
    ellipsis: { tooltip: true },
    render: (j) => j.error ?? '—',
  },
  { title: '创建', key: 'createdAt', width: 130, render: (j) => fmtTime(j.createdAt) },
  { title: '完成', key: 'finishedAt', width: 130, render: (j) => fmtTime(j.finishedAt) },
]

function rowKey(j: BackfillJob): string {
  return j.jobId
}

function goPublish(): void {
  void router.push(`/pipelines/${props.pipelineId}/publish`)
}
</script>

<template>
  <NCard size="small" title="回填（backfill）" :content-style="{ padding: '16px' }">
    <template #header-extra>
      <NTooltip trigger="hover">
        <template #trigger>
          <NText depth="3" class="cursor-hint">幂等口径</NText>
        </template>
        同区间重放使用确定性 label（tr-p{id}-s{idx}-bf 族）去重；回填 job 用独立台账命名空间，成功/失败均不推进主管道游标。
      </NTooltip>
    </template>

    <NSpin v-if="capState.kind === 'loading'" size="small" class="panel-spin" />

    <EmptyState
      v-else-if="capState.kind === 'unpublished'"
      description="管道尚未发布（无 spec 快照）——先发布再回填"
      cta="前往发布向导"
      @cta="goPublish"
    />

    <NAlert v-else-if="capState.kind === 'compile-failed'" type="error" :bordered="false">
      当前 SQL 编译未通过，无法判定回填能力：{{ capState.message }}
    </NAlert>

    <template v-else>
      <!-- per-statement capability: greyed-out rows carry the reason -->
      <NDataTable
        size="small"
        :columns="capColumns"
        :data="statements"
        :row-key="(s: StatementCap) => s.index"
        :bordered="false"
        class="cap-table"
      />

      <NAlert v-if="eligible.length === 0" type="warning" :bordered="false" class="gate-alert">
        该管道没有可回填的语句——回填仅支持 doris/starrocks 汇 + file 源（见各行「不可回填」原因）。
      </NAlert>

      <!-- range → dryRun → confirm -->
      <NForm label-placement="left" label-width="90" size="small" class="range-form" :disabled="eligible.length === 0">
        <NFormItem label="语句">
          <NSelect v-model:value="form.statementIndex" :options="statementOptions" placeholder="选择可回填的语句" />
        </NFormItem>
        <div class="range-row">
          <NFormItem label="from（含）">
            <NInputNumber v-model:value="form.from" :min="0" :precision="0" :show-button="false" placeholder="offset / 字节位" class="num-input" />
          </NFormItem>
          <NFormItem label="to（不含）">
            <NInputNumber v-model:value="form.to" :min="1" :precision="0" :show-button="false" placeholder="offset / 字节位" class="num-input" />
          </NFormItem>
        </div>
        <div class="range-actions">
          <NButton size="small" :loading="previewing" :disabled="formError != null || eligible.length === 0" @click="runDryRun">
            dryRun 预览
          </NButton>
          <NPopconfirm @positive-click="runExecute">
            <template #trigger>
              <NButton size="small" type="primary" :loading="executing" :disabled="!previewFresh || formError != null">
                确认执行回填
              </NButton>
            </template>
            将创建回填 job 并在控制面执行（Deterministic label，主管道游标不受影响）。确认？
          </NPopconfirm>
          <NText depth="3" class="range-hint">
            {{ formError ?? (previewFresh ? '预览与当前区间一致，可执行' : '先 dryRun 预览，再确认执行') }}
          </NText>
        </div>
      </NForm>

      <NAlert v-if="previewError" type="error" :bordered="false" class="gate-alert">{{ previewError }}</NAlert>

      <NDescriptions v-if="preview" label-placement="left" :column="3" size="small" bordered class="preview-desc">
        <NDescriptionsItem label="labelPrefix">
          <MonoField :value="preview.out.labelPrefix" />
        </NDescriptionsItem>
        <NDescriptionsItem label="estimatedRows">
          <template v-if="preview.out.estimatedRows != null">{{ preview.out.estimatedRows.toLocaleString() }}</template>
          <NText v-else depth="3">—（字节位区间无法预估；以执行后 rowsWritten 为准）</NText>
        </NDescriptionsItem>
        <NDescriptionsItem label="jobId">
          <NText depth="3">{{ preview.out.jobId ?? '未创建（dryRun 零副作用）' }}</NText>
        </NDescriptionsItem>
      </NDescriptions>
    </template>

    <!-- job progress rows — shown regardless of current capability (history) -->
    <div class="jobs-head">
      <span class="jobs-title">回填 job</span>
      <NButton size="tiny" quaternary :loading="jobsLoading" @click="jobsPoll.kick()">
        <template #icon><RefreshOutline /></template>
        刷新
      </NButton>
    </div>
    <NAlert v-if="jobsError && jobs.length === 0" type="warning" :bordered="false">job 列表加载失败：{{ jobsError }}</NAlert>
    <EmptyState v-else-if="jobs.length === 0" description="暂无回填 job" />
    <NDataTable
      v-else
      size="small"
      :columns="jobColumns"
      :data="jobs"
      :row-key="rowKey"
      :bordered="false"
      :max-height="280"
    />
  </NCard>
</template>

<style scoped>
.panel-spin {
  display: block;
  margin: 24px auto;
}

.cursor-hint {
  font-size: var(--tr-fs-caption);
  cursor: help;
}

.cap-table {
  margin-bottom: 12px;
}

:deep(.cap-cell) {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}

.gate-alert {
  margin-bottom: 12px;
}

.range-form {
  margin-top: 4px;
}

.range-row {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
}

.num-input {
  width: 220px;
}

.range-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 12px;
}

.range-hint {
  font-size: var(--tr-fs-caption);
}

.preview-desc {
  margin-bottom: 12px;
}

.jobs-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 8px 0;
}

.jobs-title {
  font-size: var(--tr-fs-body-sm);
  font-weight: 500;
  color: var(--tr-text-2);
}

.mono-inline {
  font-family: var(--tr-font-mono);
  font-variant-numeric: tabular-nums;
}
</style>
