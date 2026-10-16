<script setup lang="ts">
import { computed, h, reactive, ref, type ComponentPublicInstance } from 'vue'
import { useRouter } from 'vue-router'
import {
  NButton,
  NCard,
  NDataTable,
  NGrid,
  NGi,
  NIcon,
  NResult,
  NSkeleton,
  NText,
  NTooltip,
  type DataTableColumns,
} from 'naive-ui'
import { ChevronForwardOutline } from '@vicons/ionicons5'
import { nodesApi, type NodeCapacity, type NodeEvent, type NodeListItem } from '../../api/nodes'
import { pipelinesApi, type PipelineListItem, type PipelineStatus } from '../../api/pipelines'
import { ApiError } from '../../api/http'
import MonoField from '../../components/MonoField.vue'
import StateTag from '../../components/StateTag.vue'
import EmptyState from '../../components/EmptyState.vue'
import KpiCard from '../../components/KpiCard.vue'
import { useAutoRefresh } from '../../components/useAutoRefresh'
import { stateTone } from '../../theme/state-map'
import { brand, semantic } from '../../theme/tokens'
import { useThemeStore } from '../../stores/theme'

/**
 * Overview page (design doc §6.2). Three parallel queries, one 60s poller for
 * all of them (§6.2 交互流程 1):
 *   ①② GET /api/nodes            → KPI cards + node-health table
 *   ③   GET /api/pipelines?pageSize=100 → client-side status distribution
 *   ④   GET /api/nodes/events?hours=24&level=ERROR → error-event feed
 * KPI③ 「24h 流入行」 stays `—` until 增补建议 A (summary.rowsIn24h) lands.
 */

const router = useRouter()
const theme = useThemeStore()

// --- per-query state: 'loading' (no data yet) | 'ok' | 'error' ---
interface QueryState<T> {
  phase: 'loading' | 'ok' | 'error'
  data: T | null
  error: string
}

interface NodesData {
  nodes: NodeListItem[]
  summary: { online: number; total: number; runningPipelines: number; errors24h: number }
}

interface PipelinesData {
  pipelines: PipelineListItem[]
  total: number
}

interface EventsData {
  items: NodeEvent[]
}

const qNodes = reactive<QueryState<NodesData>>({ phase: 'loading', data: null, error: '' })
const qPipelines = reactive<QueryState<PipelinesData>>({ phase: 'loading', data: null, error: '' })
const qEvents = reactive<QueryState<EventsData>>({ phase: 'loading', data: null, error: '' })

function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : '请求失败，请稍后重试'
}

let nodesBusy = false
async function fetchNodes(): Promise<void> {
  if (nodesBusy) return
  nodesBusy = true
  if (!qNodes.data) qNodes.phase = 'loading'
  try {
    const res = await nodesApi.list({ silent: true }) // inline degrade, no toast
    qNodes.data = { nodes: res.nodes, summary: res.summary }
    qNodes.phase = 'ok'
  } catch (e) {
    if (!qNodes.data) {
      qNodes.phase = 'error'
      qNodes.error = errorMessage(e)
    } // else: keep stale data on poll failure, next cycle retries
  } finally {
    nodesBusy = false
  }
}

let pipelinesBusy = false
async function fetchPipelines(): Promise<void> {
  if (pipelinesBusy) return
  pipelinesBusy = true
  if (!qPipelines.data) qPipelines.phase = 'loading'
  try {
    const res = await pipelinesApi.list({ page: 1, pageSize: 100 }, { silent: true })
    qPipelines.data = { pipelines: res.pipelines, total: res.total }
    qPipelines.phase = 'ok'
  } catch (e) {
    if (!qPipelines.data) {
      qPipelines.phase = 'error'
      qPipelines.error = errorMessage(e)
    }
  } finally {
    pipelinesBusy = false
  }
}

let eventsBusy = false
async function fetchEvents(): Promise<void> {
  if (eventsBusy) return
  eventsBusy = true
  if (!qEvents.data) qEvents.phase = 'loading'
  try {
    const res = await nodesApi.allEvents({ hours: 24, level: 'ERROR' }, { silent: true })
    qEvents.data = { items: res.items }
    qEvents.phase = 'ok'
  } catch (e) {
    if (!qEvents.data) {
      qEvents.phase = 'error'
      qEvents.error = errorMessage(e)
    }
  } finally {
    eventsBusy = false
  }
}

function fetchAll(): void {
  void fetchNodes()
  void fetchPipelines()
  void fetchEvents()
}
// §6.2: three queries in parallel on entry + 60s poll; shared useAutoRefresh
// (§6.6.6) fires the first round on mount, pauses while hidden, kicks on show.
useAutoRefresh(fetchAll, 60_000)

const allFailed = computed(
  () => qNodes.phase === 'error' && qPipelines.phase === 'error' && qEvents.phase === 'error',
)

// --- ① KPI four cards (§6.2 线框 ①) ---
const eventsBlockEl = ref<HTMLElement | null>(null)
function setEventsBlockEl(el: Element | ComponentPublicInstance | null): void {
  eventsBlockEl.value = el instanceof HTMLElement ? el : null
}
const eventsHighlighted = ref(false)
let highlightTimer: ReturnType<typeof setTimeout> | null = null

/** KPI④ drill: scroll to block ④ and flash it, instead of navigating. */
function focusEventsBlock(): void {
  eventsBlockEl.value?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  eventsHighlighted.value = true
  if (highlightTimer) clearTimeout(highlightTimer)
  highlightTimer = setTimeout(() => (eventsHighlighted.value = false), 2400)
}

interface KpiDef {
  label: string
  value: string
  drill: () => void
}

const kpis = computed<KpiDef[]>(() => {
  const s = qNodes.data?.summary
  return [
    {
      label: '在线节点',
      value: s ? `${s.online} / ${s.total}` : '—',
      drill: () => router.push('/nodes'),
    },
    {
      label: '运行管道',
      value: s ? String(s.runningPipelines) : '—',
      drill: () => router.push({ path: '/pipelines', query: { status: 'RUNNING' } }),
    },
    {
      label: '24h 流入行',
      // 增补建议 A (summary.rowsIn24h) not in §4.3 yet — placeholder, no error.
      value: '—',
      drill: () => router.push('/pipelines'),
    },
    {
      label: '24h 错误',
      value: s ? String(s.errors24h) : '—',
      drill: focusEventsBlock,
    },
  ]
})

// --- ② node-health table (§6.2 线框 ②) ---
// Wireframe capacity cell is 「N管·N语」; §4.3 GET /api/nodes only returns
// runningPipelines, so running statements are read tolerantly when present.
type CapacityWithRunning = NodeCapacity & { runningStatements?: number }

function capacityText(row: NodeListItem): string {
  if (row.status !== 'ONLINE') return '—'
  const c = row.capacity as CapacityWithRunning
  const pipes = `${c.runningPipelines ?? 0}管`
  return c.runningStatements != null ? `${pipes}·${c.runningStatements}语` : pipes
}

const nodeColumns: DataTableColumns<NodeListItem> = [
  {
    title: '名称',
    key: 'name',
    ellipsis: { tooltip: true },
    render: (row) => row.name || row.nodeKey,
  },
  {
    title: '状态',
    key: 'status',
    width: 110,
    render: (row) => h(StateTag, { domain: 'node', value: row.status }),
  },
  {
    title: '版本',
    key: 'engineVersion',
    width: 90,
    ellipsis: { tooltip: true },
    render: (row) => row.engineVersion || '—',
  },
  {
    title: '容量',
    key: 'capacity',
    render: capacityText,
  },
  {
    title: '24h 错误',
    key: 'errors24h',
    width: 90,
    align: 'right',
    render: (row) => String(row.errors24h ?? 0),
  },
]

const nodeRowProps = (row: NodeListItem) => ({
  style: 'cursor: pointer;',
  onClick: () => router.push('/nodes'),
})

// --- ③ pipeline status distribution (§6.2 线框 ③) ---
const STATUS_ORDER: PipelineStatus[] = [
  'RUNNING',
  'PAUSED',
  'FAILED',
  'DEPLOY_FAILED',
  'PUBLISHED',
  'PENDING_CHANGES',
  'DRAFT',
]

const PAGE_SIZE_CAP = 100

const dist = computed(() => {
  const list = qPipelines.data?.pipelines ?? []
  const counts = new Map<PipelineStatus, number>()
  for (const p of list) counts.set(p.status, (counts.get(p.status) ?? 0) + 1)
  const rows = STATUS_ORDER.filter((s) => (counts.get(s) ?? 0) > 0).map((s) => ({
    status: s,
    count: counts.get(s) ?? 0,
  }))
  const counted = rows.reduce((acc, r) => acc + r.count, 0)
  return {
    rows,
    counted,
    capped: (qPipelines.data?.total ?? 0) > PAGE_SIZE_CAP,
  }
})

function distPct(count: number): string {
  if (dist.value.counted === 0) return '0%'
  return `${Math.round((count / dist.value.counted) * 100)}%`
}

/** Bar color follows the §6.6.5 status tone, resolved through tokens (§6.6.4). */
function distBarColor(status: PipelineStatus): string {
  const sem = semantic[theme.mode]
  switch (stateTone('pipeline', status)) {
    case 'success':
      return sem.successColor
    case 'warning':
      return sem.warningColor
    case 'error':
      return sem.errorColor
    case 'info':
      return sem.infoColor
    case 'primary':
      return brand[theme.mode].primaryColor
    default:
      return 'var(--tr-text-3)'
  }
}

function goStatus(status: PipelineStatus): void {
  router.push({ path: '/pipelines', query: { status } })
}

// --- ④ error-event feed (§6.2 线框 ④) ---
function fmtTime(ts: string): string {
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return ts
  const p = (n: number) => String(n).padStart(2, '0')
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

const eventColumns: DataTableColumns<NodeEvent> = [
  {
    title: '时间',
    key: 'ts',
    width: 100,
    render: (row) => fmtTime(row.ts),
  },
  {
    title: 'node_key',
    key: 'nodeKey',
    width: 220,
    render: (row) => h(MonoField, { value: row.nodeKey, headChars: 16 }),
  },
  {
    title: '级别',
    key: 'level',
    width: 90,
    render: (row) => h(StateTag, { domain: 'eventLevel', value: row.level }),
  },
  {
    title: '类型',
    key: 'kind',
    width: 180,
    ellipsis: { tooltip: true },
  },
  {
    title: '消息',
    key: 'message',
    ellipsis: { tooltip: true }, // hover shows the full message (§6.2 交互 4)
  },
]
</script>

<template>
  <main class="page">
    <!-- whole-page error only when all three queries fail (§6.2 三态) -->
    <NResult
      v-if="allFailed"
      status="error"
      title="总览数据加载失败"
      description="三路查询（节点 / 管道 / 事件）均失败，请确认 tailrace-console 可用后重试"
    >
      <template #footer>
        <NButton size="small" @click="fetchAll">重试全部</NButton>
      </template>
    </NResult>

    <template v-else>
      <!-- ① KPI four cards -->
      <NGrid cols="1 s:2 m:4" :x-gap="16" :y-gap="16" responsive="screen" class="kpi-grid">
        <NGi v-if="qNodes.phase === 'error'" span="1 s:2 m:4">
          <NCard size="small" class="block-card">
            <NResult status="error" title="KPI 加载失败" :description="qNodes.error">
              <template #footer>
                <NButton size="small" @click="fetchNodes">重试</NButton>
              </template>
            </NResult>
          </NCard>
        </NGi>
        <NGi v-else v-for="kpi in kpis" :key="kpi.label">
          <KpiCard
            :label="kpi.label"
            :value="kpi.value"
            :loading="qNodes.phase === 'loading'"
            clickable
            class="kpi-drill"
            @click="kpi.drill"
          >
            <NIcon :size="14" class="kpi-arrow">
              <ChevronForwardOutline />
            </NIcon>
          </KpiCard>
        </NGi>
      </NGrid>

      <!-- ② node health + ③ pipeline status distribution -->
      <NGrid cols="1 m:2" :x-gap="16" :y-gap="16" responsive="screen" class="block-grid">
        <NGi>
          <NCard size="small" title="节点健康" class="block-card">
            <template #header-extra>
              <NButton text size="tiny" @click="router.push('/nodes')">全部</NButton>
            </template>
            <NSkeleton v-if="qNodes.phase === 'loading'" :repeat="3" text />
            <NResult
              v-else-if="qNodes.phase === 'error'"
              size="small"
              status="error"
              title="节点数据加载失败"
              :description="qNodes.error"
            >
              <template #footer>
                <NButton size="small" @click="fetchNodes">重试</NButton>
              </template>
            </NResult>
            <EmptyState
              v-else-if="qNodes.data && qNodes.data.nodes.length === 0"
              description="尚无引擎节点"
              cta-text="部署引擎"
              @cta="router.push('/nodes')"
            />
            <NDataTable
              v-else
              size="small"
              :columns="nodeColumns"
              :data="qNodes.data?.nodes ?? []"
              :row-props="nodeRowProps"
              :bordered="false"
            />
          </NCard>
        </NGi>

        <NGi>
          <NCard size="small" title="管道状态分布" class="block-card">
            <template #header-extra>
              <NButton text size="tiny" @click="router.push('/pipelines')">查看全部</NButton>
            </template>
            <NSkeleton v-if="qPipelines.phase === 'loading'" :repeat="3" text />
            <NResult
              v-else-if="qPipelines.phase === 'error'"
              size="small"
              status="error"
              title="管道数据加载失败"
              :description="qPipelines.error"
            >
              <template #footer>
                <NButton size="small" @click="fetchPipelines">重试</NButton>
              </template>
            </NResult>
            <EmptyState
              v-else-if="dist.rows.length === 0"
              description="尚无管道"
              cta-text="新建管道"
              @cta="router.push('/pipelines/edit?mode=create')"
            />
            <div v-else class="dist-list">
              <div
                v-for="row in dist.rows"
                :key="row.status"
                class="dist-row"
                @click="goStatus(row.status)"
              >
                <StateTag domain="pipeline" :value="row.status" class="dist-tag" />
                <NTooltip>
                  <template #trigger>
                    <div class="dist-bar-track">
                      <div
                        class="dist-bar"
                        :style="{
                          width: distPct(row.count),
                          background: distBarColor(row.status),
                        }"
                      />
                    </div>
                  </template>
                  {{ row.status }} · {{ row.count }} 条（{{ distPct(row.count) }}，点击查看）
                </NTooltip>
                <span class="dist-count">{{ row.count }}</span>
              </div>
              <div v-if="dist.capped" class="dist-capped">
                <NText depth="3" size="small">
                  管道超过 {{ PAGE_SIZE_CAP }} 条，分布仅统计前 {{ PAGE_SIZE_CAP }} 条
                </NText>
                <NButton text size="tiny" @click="router.push('/pipelines')">查看全部</NButton>
              </div>
            </div>
          </NCard>
        </NGi>
      </NGrid>

      <!-- ④ error events of the last 24h -->
      <div :ref="setEventsBlockEl" class="events-anchor">
        <NCard
          size="small"
          title="近 24h 错误事件（level=ERROR）"
          class="block-card events-block"
          :class="{ highlight: eventsHighlighted }"
        >
        <template #header-extra>
          <NButton text size="tiny" @click="router.push('/nodes')">全部</NButton>
        </template>
        <NSkeleton v-if="qEvents.phase === 'loading'" :repeat="3" text />
        <NResult
          v-else-if="qEvents.phase === 'error'"
          size="small"
          status="error"
          title="事件数据加载失败"
          :description="qEvents.error"
        >
          <template #footer>
            <NButton size="small" @click="fetchEvents">重试</NButton>
          </template>
        </NResult>
        <EmptyState v-else-if="qEvents.data && qEvents.data.items.length === 0" description="近 24h 无错误事件" positive />
        <NDataTable
          v-else
          size="small"
          :columns="eventColumns"
          :data="qEvents.data?.items ?? []"
          :bordered="false"
          :max-height="320"
        />
        </NCard>
      </div>
    </template>
  </main>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 16px; /* §6.6.3: block gap 16 */
  color: var(--tr-text-1);
}

.kpi-skeleton {
  height: 108px;
  border-radius: var(--tr-radius-card);
}

/* shared KpiCard, overview-specific styling (§6.2 ①): hover 主色描边 +
   display 字阶 28px + 右上角下钻箭头 */
.kpi-drill {
  position: relative;
  transition: border-color 0.15s ease;
}

.kpi-drill:hover {
  border-color: var(--tr-chart-1);
}

.kpi-drill :deep(.n-card__content) {
  position: relative;
}

.kpi-drill :deep(.n-statistic .n-statistic-value) {
  font-size: var(--tr-fs-display); /* §6.6.2 display 28px */
}

.kpi-arrow {
  position: absolute;
  top: 12px;
  right: 12px;
  color: var(--tr-text-3);
}

.block-card {
  border-radius: var(--tr-radius-card);
}

.dist-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 4px 0;
}

.dist-row {
  display: flex;
  align-items: center;
  gap: 12px;
  cursor: pointer;
  padding: 2px 4px;
  border-radius: var(--tr-radius-base);
}

.dist-row:hover {
  background: var(--tr-bg-pop);
}

.dist-tag {
  min-width: 108px;
}

.dist-bar-track {
  flex: 1;
  height: 10px;
  border-radius: var(--tr-radius-sharp);
  background: var(--tr-bg-pop);
  overflow: hidden;
}

.dist-bar {
  height: 100%;
  border-radius: var(--tr-radius-sharp);
  transition: width 0.3s ease;
}

.dist-count {
  min-width: 36px;
  text-align: right;
  font-variant-numeric: tabular-nums;
  color: var(--tr-text-2);
}

.dist-capped {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 4px;
}

.events-block {
  transition: border-color 0.3s ease;
}

/* KPI④ 下钻高亮（§6.2 交互 2） */
.events-block.highlight {
  border-color: var(--tr-chart-1);
}
</style>
