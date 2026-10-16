<script setup lang="ts">
import { computed, h, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useMessage, NAlert, NButton, NCard, NDataTable, NInput, NPagination, NPopconfirm, NPopover, NResult, NSelect, NSkeleton, NTag, NTooltip, type DataTableColumns, type PaginationProps } from 'naive-ui'
import { AddOutline, RefreshOutline } from '@vicons/ionicons5'
import { pipelinesApi, type PipelineListItem, type PipelineStatus, type PipelineDetail } from '../../api/pipelines'
import EmptyState from '../../components/EmptyState.vue'
import StateTag from '../../components/StateTag.vue'
import MonoField from '../../components/MonoField.vue'
import { useAutoRefresh } from '../../components/useAutoRefresh'

/**
 * Pipeline list (design doc §6.5.1): server-paginated table (pageSize 20,
 * keepPreviousData), 30s polling, client-side status/search filters, action
 * visibility matrix per pipeline status machine.
 */

const route = useRoute()
const router = useRouter()
const message = useMessage()

const STATUS_OPTIONS: Array<{ label: string; value: PipelineStatus | '' }> = [
  { label: '全部', value: '' },
  { label: 'DRAFT', value: 'DRAFT' },
  { label: 'PENDING_CHANGES', value: 'PENDING_CHANGES' },
  { label: 'PUBLISHED', value: 'PUBLISHED' },
  { label: 'RUNNING', value: 'RUNNING' },
  { label: 'PAUSED', value: 'PAUSED' },
  { label: 'FAILED', value: 'FAILED' },
  { label: 'DEPLOY_FAILED', value: 'DEPLOY_FAILED' },
]

// --- list state ---
const rows = ref<PipelineListItem[]>([])
const total = ref(0)
const page = ref(Number(route.query.page) || 1)
const pageSize = 20
const loading = ref(true)
const firstLoadError = ref<string | null>(null)
const staleAt = ref<string | null>(null)
const detailCache = new Map<number, PipelineDetail>()

async function fetchPage(): Promise<void> {
  try {
    const res = await pipelinesApi.list({ page: page.value, pageSize }, { silent: rows.value.length > 0 })
    rows.value = res.pipelines
    total.value = res.total
    firstLoadError.value = null
    staleAt.value = null
  } catch (e) {
    if (rows.value.length === 0) {
      firstLoadError.value = e instanceof Error ? e.message : String(e)
    } else {
      // Poll failed but the last snapshot stays; surface a stale-data strip (§6.5.1).
      const d = new Date()
      const p = (n: number): string => String(n).padStart(2, '0')
      staleAt.value = `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
    }
  } finally {
    loading.value = false
  }
}
useAutoRefresh(fetchPage, 30_000)

// --- filters (client-side, §6.5.1: list endpoint has no server filter) ---
const statusFilter = ref(typeof route.query.status === 'string' ? route.query.status : '')
const keyword = ref('')

const filtered = computed(() => {
  let out = rows.value
  if (statusFilter.value) out = out.filter((p) => p.status === statusFilter.value)
  const kw = keyword.value.trim().toLowerCase()
  if (kw) out = out.filter((p) => `${p.id} ${p.name}`.toLowerCase().includes(kw))
  return out
})

function fmtTime(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  const p = (n: number): string => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`
}

function groupOf(p: PipelineListItem): string {
  return p.groupPrefix ? `${p.groupPrefix}.s*` : `tr.p${p.id}.s*`
}

/** Lazy per-row detail fetch for the DEPLOY_FAILED / PENDING_CHANGES hover popover. */
const hoverDetail = ref<PipelineDetail | null>(null)
const hoverLoading = ref(false)

async function loadHoverDetail(p: PipelineListItem): Promise<void> {
  if (p.status !== 'DEPLOY_FAILED' && p.status !== 'PENDING_CHANGES') return
  const cached = detailCache.get(p.id)
  if (cached) {
    hoverDetail.value = cached
    return
  }
  hoverLoading.value = true
  hoverDetail.value = null
  try {
    const d = await pipelinesApi.get(p.id, { silent: true })
    detailCache.set(p.id, d)
    hoverDetail.value = d
  } catch {
    hoverDetail.value = null
  } finally {
    hoverLoading.value = false
  }
}

function statusCell(p: PipelineListItem) {
  const tag = h(StateTag, { domain: 'pipeline', value: p.status })
  if (p.status !== 'DEPLOY_FAILED' && p.status !== 'PENDING_CHANGES') return tag
  return h(
    NPopover,
    { trigger: 'hover', width: 280, onUpdateShow: (show: boolean) => show && void loadHoverDetail(p) },
    {
      trigger: () => h('span', { class: 'status-wrap' }, [tag]),
      default: () =>
        hoverLoading.value
          ? '加载中…'
          : p.status === 'PENDING_CHANGES'
            ? '存在已保存但未发布的修改——需发布后部署才生效'
            : hoverDetail.value?.deployError || '部署失败（详情暂不可用）',
    },
  )
}

// --- actions (visibility matrix, §6.5.1) ---
const actionBusy = ref<number | null>(null)

async function runAction(p: PipelineListItem, action: 'pause' | 'resume' | 'stop'): Promise<void> {
  actionBusy.value = p.id
  try {
    const res =
      action === 'pause' ? await pipelinesApi.pause(p.id) : action === 'resume' ? await pipelinesApi.resume(p.id) : await pipelinesApi.stop(p.id)
    p.status = res.status
    message.success(`#${p.id} → ${res.status}`)
  } catch {
    /* 502 etc. — toast already shown by http layer */
  } finally {
    actionBusy.value = null
  }
}

async function removePipeline(p: PipelineListItem): Promise<void> {
  try {
    await pipelinesApi.remove(p.id)
    message.success(`已删除 #${p.id}`)
    void fetchPage()
  } catch {
    /* toast already shown */
  }
}

const columns = computed<DataTableColumns<PipelineListItem>>(() => [
  { title: 'ID', key: 'id', width: 56 },
  { title: '名称', key: 'name', minWidth: 160, ellipsis: { tooltip: true } },
  { title: '状态', key: 'status', width: 150, render: statusCell },
  { title: '版本', key: 'version', width: 60, render: (p) => (p.version > 0 ? `v${p.version}` : 'v0') },
  {
    title: '消费组',
    key: 'group',
    minWidth: 140,
    render: (p) =>
      h(
        NTooltip,
        { trigger: 'hover' },
        {
          trigger: () => h(MonoField, { value: groupOf(p) }),
          default: () => `语句组名规则：${p.groupPrefix ? `${p.groupPrefix}.s{n}（前缀已配置）` : `tr.p${p.id}.s{n}（缺省）`}`,
        },
      ),
  },
  {
    title: '起点',
    key: 'start',
    width: 90,
    render: (p) => h(StateTag, { domain: 'startOffset', value: p.consumedOffsetStart, label: p.consumedOffsetStart === 'earliest' ? '从头' : '从最新' }),
  },
  { title: '节点', key: 'node', width: 100, render: (p) => p.assignedNode?.name ?? '—' },
  { title: '更新', key: 'updatedAt', width: 100, render: (p) => fmtTime(p.updatedAt) },
  {
    title: '操作',
    key: 'ops',
    width: 300,
    render: (p) => {
      const btn = (label: string, onClick: () => void, opts?: { type?: 'primary' | 'error'; confirm?: string }): ReturnType<typeof h> => {
        // stopPropagation: row-click navigates to monitor; button clicks must not.
        const stopped = (e?: MouseEvent): void => {
          e?.stopPropagation()
          onClick()
        }
        if (opts?.confirm) {
          return h(
            NPopconfirm,
            { onPositiveClick: onClick },
            { trigger: () => h(NButton, { size: 'tiny', quaternary: true, type: opts.type, loading: actionBusy.value === p.id, onClick: stopped }, { default: () => label }), default: () => opts.confirm },
          )
        }
        return h(NButton, { size: 'tiny', quaternary: true, type: opts?.type, loading: actionBusy.value === p.id, onClick: stopped }, { default: () => label })
      }
      const cells: Array<ReturnType<typeof h> | null> = []
      const st = p.status
      // monitor
      if (['PENDING_CHANGES', 'RUNNING', 'PAUSED', 'FAILED', 'DEPLOY_FAILED'].includes(st)) {
        cells.push(btn('监控', () => router.push(`/pipelines/${p.id}/monitor`)))
      }
      // edit
      cells.push(btn('编辑', () => router.push(`/pipelines/edit?mode=edit&id=${p.id}`)))
      // publish
      if (st === 'DRAFT' || st === 'PENDING_CHANGES') {
        cells.push(btn('发布', () => router.push(`/pipelines/${p.id}/publish`), { type: 'primary' }))
      }
      // deploy / retry-deploy → wizard step 3
      if (['PUBLISHED', 'FAILED', 'DEPLOY_FAILED'].includes(st)) {
        cells.push(btn(st === 'PUBLISHED' ? '部署' : '重试部署', () => router.push(`/pipelines/${p.id}/publish?from=deploy`), { type: 'primary' }))
      }
      if (st === 'RUNNING') cells.push(btn('暂停', () => void runAction(p, 'pause')))
      if (st === 'PAUSED') cells.push(btn('恢复', () => void runAction(p, 'resume')))
      if (['RUNNING', 'PAUSED', 'PENDING_CHANGES'].includes(st)) {
        cells.push(btn('停止', () => void runAction(p, 'stop'), { confirm: '游标已提交，回已发布态可再部署' }))
      }
      cells.push(btn('删除', () => void removePipeline(p), { type: 'error', confirm: 'RUNNING 管道将先自动停止，确认删除？' }))
      return h('div', { class: 'row-actions' }, cells)
    },
  },
])

const pagination = computed<PaginationProps>(() => ({
  page: page.value,
  pageSize,
  itemCount: total.value,
}))

function onPage(next: number): void {
  page.value = next
  loading.value = true
  void fetchPage()
}

function goCreate(): void {
  router.push('/pipelines/edit?mode=create')
}
</script>

<template>
  <main class="page">
    <div class="page-head">
      <div>
        <h1 class="title">数据管道</h1>
        <p class="subtitle">SQL 即管道——写、校、发、部署、观测闭环。</p>
      </div>
      <div class="head-actions">
        <NButton size="small" @click="fetchPage()">
          <template #icon><RefreshOutline /></template>
          刷新
        </NButton>
        <NButton size="small" type="primary" @click="goCreate">
          <template #icon><AddOutline /></template>
          新建管道
        </NButton>
      </div>
    </div>

    <NResult v-if="firstLoadError" status="error" title="管道列表加载失败" :description="firstLoadError" class="block-error">
      <template #footer>
        <NButton size="small" @click="fetchPage()">重试</NButton>
      </template>
    </NResult>

    <template v-else>
      <NAlert v-if="staleAt" type="warning" :bordered="false" class="stale-strip">
        数据可能过期 · 上次成功 {{ staleAt }}
      </NAlert>

      <div class="filter-bar">
        <NSelect v-model:value="statusFilter" :options="STATUS_OPTIONS" size="small" class="status-select" />
        <NInput v-model:value="keyword" size="small" clearable placeholder="搜索当前页（ID / 名称）" class="kw-input" />
        <NTag v-if="statusFilter" size="small" closable @close="statusFilter = ''">
          状态过滤：{{ statusFilter }}
        </NTag>
      </div>

      <NCard size="small" :content-style="{ padding: '0' }" class="table-card">
        <NSkeleton v-if="loading && rows.length === 0" text :repeat="6" class="table-skeleton" />
        <EmptyState v-else-if="rows.length === 0" description="尚无管道" cta="新建管道" @cta="goCreate" />
        <NDataTable
          v-else
          :columns="columns"
          :data="filtered"
          :row-key="(p: PipelineListItem) => p.id"
          size="small"
          :loading="loading"
          :row-props="(p: PipelineListItem) => ({ style: 'cursor: pointer', onClick: () => ['PENDING_CHANGES','RUNNING','PAUSED','FAILED','DEPLOY_FAILED'].includes(p.status) && router.push(`/pipelines/${p.id}/monitor`) })"
        />
      </NCard>

      <div class="pager-row">
        <NPagination v-bind="pagination" @update:page="onPage" />
        <span class="total-note">共 {{ total }} 条 · 第 {{ page }}/{{ Math.max(1, Math.ceil(total / pageSize)) }} 页</span>
      </div>
    </template>
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
  align-items: flex-end;
  justify-content: space-between;
  gap: 16px;
}

.title {
  font-size: var(--tr-fs-title-lg);
  font-weight: 600;
  margin: 0 0 4px;
  color: var(--tr-text-1);
}

.subtitle {
  margin: 0;
  color: var(--tr-text-3);
  font-size: var(--tr-fs-body-sm);
}

.head-actions {
  display: flex;
  gap: 8px;
}

.block-error {
  margin-top: 32px;
}

.stale-strip {
  padding: 4px 12px;
}

.filter-bar {
  display: flex;
  align-items: center;
  gap: 8px;
}

.status-select {
  width: 200px;
}

.kw-input {
  width: 240px;
}

.table-card :deep(.n-card__content) {
  padding: 8px;
}

.table-skeleton {
  padding: 16px;
}

.row-actions {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  flex-wrap: wrap;
}

.status-wrap {
  display: inline-flex;
}

.pager-row {
  display: flex;
  align-items: center;
  gap: 12px;
  justify-content: flex-end;
}

.total-note {
  color: var(--tr-text-3);
  font-size: var(--tr-fs-caption);
  font-variant-numeric: tabular-nums;
}
</style>
