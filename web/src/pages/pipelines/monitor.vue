<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useMessage, NAlert, NButton, NCard, NCode, NCollapse, NCollapseItem, NDescriptions, NDescriptionsItem, NForm, NFormItem, NGrid, NGi, NModal, NPopconfirm, NRadioButton, NRadioGroup, NResult, NSelect, NSkeleton, NSpin, NTag, NText, NTooltip } from 'naive-ui'
import { ArrowBackOutline, RefreshOutline } from '@vicons/ionicons5'
import { pipelinesApi, type PipelineDetail, type PipelineStatus } from '../../api/pipelines'
import { nodesApi, type NodeListItem } from '../../api/nodes'
import { datasourcesApi, type Datasource } from '../../api/datasources'
import { ApiError } from '../../api/http'
import KpiCard from '../../components/KpiCard.vue'
import EmptyState from '../../components/EmptyState.vue'
import StateTag from '../../components/StateTag.vue'
import MonoField from '../../components/MonoField.vue'
import MetricChart from '../../components/MetricChart.vue'
import LogTerminal from '../../components/LogTerminal.vue'
import { useAutoRefresh } from '../../components/useAutoRefresh'

/**
 * Pipeline runtime monitor (design doc §6.5.4): stats KPI cards (5s poll,
 * front-end diff rates, counter-reset baseline), 24h uPlot trend, offsets
 * ledger view, SSE LogTerminal, meta/mapping/SQL/spec collapse, actions
 * (pause/resume/stop/recover).
 */

const route = useRoute()
const router = useRouter()
const message = useMessage()

const pipelineId = Number(route.params.id)

// --- detail ---
const detail = ref<PipelineDetail | null>(null)
const loading = ref(true)
const notFound = ref(false)
const loadError = ref<string | null>(null)
const status = ref<PipelineStatus | null>(null)

const datasources = ref<Datasource[]>([])
const dsName = (id: number): string => datasources.value.find((d) => d.id === id)?.name ?? `#${id}`
const dsType = (id: number): string | null => datasources.value.find((d) => d.id === id)?.type ?? null

async function loadDetail(): Promise<void> {
  try {
    const d = await pipelinesApi.get(pipelineId, { silent: true })
    detail.value = d
    status.value = d.status
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) notFound.value = true
    else loadError.value = e instanceof Error ? e.message : String(e)
  }
}

const deployed = computed(() => detail.value?.assignedNode != null)

function fmtTime(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  const p = (n: number): string => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`
}

// --- stats (5s poll, front-end diff, §6.5.4 flow 2) ---
interface EngineStatementCounters { consumed: number; flushed: number; skipped?: number; bad?: number; flushes?: number; lastFlushMs?: number }
interface EngineStats { status: string; totals: EngineStatementCounters; statements: EngineStatementCounters[] }

type StatsState =
  | { kind: 'loading' }
  | { kind: 'ok'; stats: EngineStats }
  | { kind: 'not-registered'; error: string }
  | { kind: 'unreachable'; message: string }
  | { kind: 'undeployed' }
const statsState = ref<StatsState>({ kind: 'loading' })

const rateIn = ref<number | null>(null)
const rateOut = ref<number | null>(null)
const cumIn = ref<number | null>(null)
const cumOut = ref<number | null>(null)
const inSpark: number[] = []
const outSpark: number[] = []
const cumInSpark: number[] = []
const cumOutSpark: number[] = []
const sparkVersion = ref(0) // bump to force MetricChart data refresh (arrays are plain)

let prevSample: { consumed: number; flushed: number; at: number } | null = null
const resetAlertShown = ref(false)

function pushSpark(arr: number[], v: number): void {
  arr.push(v)
  if (arr.length > 60) arr.shift()
}

function parseStats(raw: unknown): EngineStats | null {
  if (typeof raw !== 'object' || raw === null) return null
  const r = raw as Record<string, unknown>
  const t = r.totals as Record<string, unknown> | undefined
  if (!t || typeof t.consumed !== 'number' || typeof t.flushed !== 'number') return null
  return {
    status: typeof r.status === 'string' ? r.status : '',
    totals: { consumed: t.consumed, flushed: t.flushed, flushes: typeof t.flushes === 'number' ? t.flushes : 0 },
    statements: Array.isArray(r.statements)
      ? (r.statements as Array<Record<string, unknown>>).map((s) => ({
          consumed: typeof s.consumed === 'number' ? s.consumed : 0,
          flushed: typeof s.flushed === 'number' ? s.flushed : 0,
        }))
      : [],
  }
}

async function fetchStats(): Promise<void> {
  if (!detail.value) return // still loading the header — skip this tick
  if (!deployed.value) {
    statsState.value = { kind: 'undeployed' }
    return
  }
  try {
    const env = await pipelinesApi.stats(pipelineId)
    if (env.ok) {
      const parsed = parseStats(env.stats)
      if (!parsed) {
        statsState.value = { kind: 'unreachable', message: 'stats 载荷不可解析（旧版制品？）' }
        return
      }
      statsState.value = { kind: 'ok', stats: parsed }
      cumIn.value = parsed.totals.consumed
      cumOut.value = parsed.totals.flushed
      const now = Date.now()
      if (prevSample) {
        const dt = (now - prevSample.at) / 1000
        if (parsed.totals.consumed < prevSample.consumed || parsed.totals.flushed < prevSample.flushed) {
          // engine restart zeroed counters → reset the diff baseline once
          prevSample = { consumed: parsed.totals.consumed, flushed: parsed.totals.flushed, at: now }
          if (!resetAlertShown.value) resetAlertShown.value = true
          rateIn.value = null
          rateOut.value = null
        } else if (dt > 0.5) {
          rateIn.value = Math.round(((parsed.totals.consumed - prevSample.consumed) / dt) * 10) / 10
          rateOut.value = Math.round(((parsed.totals.flushed - prevSample.flushed) / dt) * 10) / 10
          prevSample = { consumed: parsed.totals.consumed, flushed: parsed.totals.flushed, at: now }
        }
      } else {
        prevSample = { consumed: parsed.totals.consumed, flushed: parsed.totals.flushed, at: now }
      }
      pushSpark(inSpark, rateIn.value ?? 0)
      pushSpark(outSpark, rateOut.value ?? 0)
      pushSpark(cumInSpark, parsed.totals.consumed)
      pushSpark(cumOutSpark, parsed.totals.flushed)
      sparkVersion.value += 1
    } else {
      statsState.value = { kind: 'not-registered', error: env.error ?? '管道不在引擎运行册' }
    }
  } catch (e) {
    statsState.value =
      e instanceof ApiError && e.status === 502
        ? { kind: 'unreachable', message: '引擎不可达或为旧版制品（无 stats 端点）——升级引擎制品后可用；历史趋势仍可查看' }
        : { kind: 'unreachable', message: e instanceof Error ? e.message : String(e) }
  }
}
useAutoRefresh(fetchStats, 5_000)

const multiStatementNote = computed(() => {
  if (statsState.value.kind !== 'ok') return ''
  const sts = statsState.value.stats.statements
  if (sts.length <= 1) return ''
  return sts.map((s, i) => `#${i} 入 ${s.consumed.toLocaleString()} / 出 ${s.flushed.toLocaleString()}`).join(' · ')
})

function sparkSeries(arr: number[]): Array<number | null> {
  void sparkVersion.value
  return arr.map((v) => v)
}

const fmtInt = (v: number | null): string => (v == null ? '—' : v.toLocaleString())

// --- trend (60s poll) ---
type TrendState = { kind: 'loading' } | { kind: 'ok' } | { kind: 'error'; message: string }
const trendState = ref<TrendState>({ kind: 'loading' })
const trendMode = ref<'rate' | 'cum'>('rate')
const trendPoints = ref<Array<{ ts: number; consumed: number; flushed: number; rateInRps: number; rateOutRps: number }>>([])

async function fetchTrend(): Promise<void> {
  try {
    const res = await pipelinesApi.trend(pipelineId, { hours: 24 }, { silent: true })
    trendPoints.value = res.points.map((p) => ({
      ts: Math.floor(new Date(p.ts).getTime() / 1000),
      consumed: p.consumed,
      flushed: p.flushed,
      rateInRps: p.rateInRps,
      rateOutRps: p.rateOutRps,
    }))
    trendState.value = { kind: 'ok' }
  } catch (e) {
    if (trendPoints.value.length === 0) trendState.value = { kind: 'error', message: e instanceof Error ? e.message : String(e) }
  }
}
useAutoRefresh(fetchTrend, 60_000)

const trendChart = computed(() => ({
  timestamps: trendPoints.value.map((p) => p.ts),
  series:
    trendMode.value === 'rate'
      ? [
          { label: '流入 行/s', points: trendPoints.value.map((p) => p.rateInRps) },
          { label: '流出 行/s', points: trendPoints.value.map((p) => p.rateOutRps) },
        ]
      : [
          { label: '累计流入', points: trendPoints.value.map((p) => p.consumed) },
          { label: '累计流出', points: trendPoints.value.map((p) => p.flushed) },
        ],
}))

// --- offsets (on enter + manual + after actions, §6.5.4 flow 6) ---
interface OffsetRow {
  statement: string
  kind: 'kafka' | 'file' | 'jdbc' | 'none'
  group?: string
  topic?: string
  partitions?: Array<[string, number]>
  path?: string
  bytePos?: number
  jdbc?: { key: string; value: string }
  lastAck?: string | null
  totals?: { consumed: number; flushed: number; flushes: number }
}

function normalizeOffsets(raw: unknown): OffsetRow[] {
  if (typeof raw !== 'object' || raw === null) return []
  const r = raw as Record<string, unknown>
  const out: OffsetRow[] = []
  const stmts = Array.isArray(r.statements) ? r.statements : []
  for (const sRaw of stmts) {
    if (typeof sRaw !== 'object' || sRaw === null) continue
    const s = sRaw as Record<string, unknown>
    // Engine ledger shape: { statement: "0", ledger: Cursor | {cursor, last_ack, totals} }
    const ledger = (s.ledger ?? s) as Record<string, unknown>
    let cursor = ledger as Record<string, unknown>
    let lastAck: string | null | undefined
    let totals: OffsetRow['totals']
    if ('cursor' in ledger && typeof ledger.cursor === 'object' && ledger.cursor !== null) {
      cursor = ledger.cursor as Record<string, unknown>
      lastAck = typeof ledger.last_ack === 'string' ? ledger.last_ack : null
      const t = ledger.totals as Record<string, unknown> | undefined
      if (t && typeof t.consumed === 'number') {
        totals = { consumed: t.consumed, flushed: typeof t.flushed === 'number' ? t.flushed : 0, flushes: typeof t.flushes === 'number' ? t.flushes : 0 }
      }
    }
    const row: OffsetRow = { statement: String(s.statement ?? out.length), kind: 'none', lastAck: lastAck ?? undefined, totals }
    if ('Kafka' in cursor) {
      const k = cursor.Kafka as Record<string, unknown>
      row.kind = 'kafka'
      row.group = typeof k.group === 'string' ? k.group : undefined
      row.topic = typeof k.topic === 'string' ? k.topic : undefined
      row.partitions = Object.entries((k.offsets ?? {}) as Record<string, number>).map(([p, o]) => [String(p), Number(o)])
    } else if ('File' in cursor) {
      const f = cursor.File as Record<string, unknown>
      row.kind = 'file'
      row.path = typeof f.path === 'string' ? f.path : undefined
      row.bytePos = typeof f.byte_pos === 'number' ? f.byte_pos : undefined
    } else if ('JdbcKey' in cursor) {
      const j = cursor.JdbcKey as Record<string, unknown>
      row.kind = 'jdbc'
      row.jdbc = { key: String(j.key ?? ''), value: String(j.value ?? '') }
    }
    // Documented passthrough shape: { group, topic, partitions, file }
    if (row.kind === 'none' && (s.group || s.topic || s.partitions || s.file)) {
      row.kind = 'kafka'
      row.group = typeof s.group === 'string' ? s.group : undefined
      row.topic = typeof s.topic === 'string' ? s.topic : undefined
      row.partitions = Object.entries((s.partitions ?? {}) as Record<string, number>).map(([p, o]) => [String(p), Number(o)])
      if (s.file && typeof s.file === 'object') {
        row.kind = 'file'
        const f = s.file as Record<string, unknown>
        row.path = typeof f.path === 'string' ? f.path : undefined
        row.bytePos = typeof f.bytePos === 'number' ? f.bytePos : undefined
      }
    }
    out.push(row)
  }
  return out
}

const offsetsLoading = ref(false)
const offsetsError = ref<string | null>(null)
const offsetRows = ref<OffsetRow[]>([])

async function fetchOffsets(): Promise<void> {
  if (!deployed.value) return
  offsetsLoading.value = true
  offsetsError.value = null
  try {
    const raw = await pipelinesApi.offsets(pipelineId, { silent: true })
    offsetRows.value = normalizeOffsets(raw)
  } catch (e) {
    offsetsError.value = e instanceof Error ? e.message : String(e)
  } finally {
    offsetsLoading.value = false
  }
}

// --- actions ---
const actionBusy = ref(false)

async function runAction(action: 'pause' | 'resume' | 'stop'): Promise<void> {
  actionBusy.value = true
  try {
    const res =
      action === 'pause' ? await pipelinesApi.pause(pipelineId) : action === 'resume' ? await pipelinesApi.resume(pipelineId) : await pipelinesApi.stop(pipelineId)
    status.value = res.status
    message.success(`#${pipelineId} → ${res.status}`)
    void fetchOffsets()
    prevSample = null // rates re-baseline after engine-side transitions
  } catch {
    /* toast shown */
  } finally {
    actionBusy.value = false
  }
}

// --- recover modal (§6.5.4) ---
const recoverModal = ref(false)
const recoverNodeId = ref<number | null>(null)
const recoverSubmitting = ref(false)
const onlineNodes = ref<NodeListItem[]>([])

function openRecover(): void {
  recoverNodeId.value = null
  recoverModal.value = true
  if (onlineNodes.value.length === 0) {
    void nodesApi
      .list({ silent: true })
      .then((res) => (onlineNodes.value = res.nodes.filter((n) => n.status === 'ONLINE')))
      .catch(() => undefined)
  }
}

const currentNodeId = computed(() => detail.value?.assignedNode?.id ?? null)

const recoverNodeOptions = computed(() => [
  { label: `当前节点${currentNodeId.value != null ? `（${detail.value?.assignedNode?.name ?? ''}）` : ''} ← 当前`, value: 0 },
  ...onlineNodes.value.filter((n) => n.id !== currentNodeId.value).map((n) => ({ label: `${n.name}（${n.apiUrl}）`, value: n.id })),
])

async function submitRecover(): Promise<void> {
  recoverSubmitting.value = true
  try {
    await pipelinesApi.recover(pipelineId, { nodeId: recoverNodeId.value })
    recoverModal.value = false
    message.success('急救恢复已下推（引擎从上次提交位置续传）')
    void fetchOffsets()
  } catch {
    /* toast shown */
  } finally {
    recoverSubmitting.value = false
  }
}

// --- boot ---
onMounted(async () => {
  await Promise.all([
    loadDetail(),
    datasourcesApi.list(undefined, { silent: true }).then((r) => (datasources.value = r.items)).catch(() => undefined),
  ])
  loading.value = false
  void fetchOffsets()
})

function goPublish(): void {
  void router.push(`/pipelines/${pipelineId}/publish`)
}

function goEdit(): void {
  void router.push(`/pipelines/edit?mode=edit&id=${pipelineId}`)
}

const groupOf = computed(() => {
  const p = detail.value
  return p ? (p.groupPrefix ? `${p.groupPrefix}.s*` : `tr.p${p.id}.s*`) : ''
})

const mappingEntries = computed(() => Object.entries(detail.value?.dsMapping ?? {}))

const logsUrl = computed(() => pipelinesApi.logsUrl(pipelineId, 200))
</script>

<template>
  <main class="page">
    <NResult v-if="notFound" status="404" title="管道不存在">
      <template #footer>
        <NButton size="small" @click="router.push('/pipelines')">返回列表</NButton>
      </template>
    </NResult>
    <NResult v-else-if="loadError" status="error" title="详情加载失败" :description="loadError">
      <template #footer>
        <NButton size="small" @click="router.go(0)">重试</NButton>
      </template>
    </NResult>

    <template v-else>
      <!-- header + action bar -->
      <div class="page-head">
        <div class="head-left">
          <NButton size="small" quaternary @click="router.push('/pipelines')">
            <template #icon><ArrowBackOutline /></template>
            返回
          </NButton>
          <h1 class="title">{{ detail?.name ?? `#${pipelineId}` }}</h1>
          <StateTag v-if="status" domain="pipeline" :value="status" />
          <NText depth="3" class="meta-line">
            v{{ detail?.version ?? 0 }} · 节点 {{ detail?.assignedNode?.name ?? '—' }} · 更新 {{ fmtTime(detail?.updatedAt ?? null) }}
          </NText>
        </div>
        <div class="head-actions">
          <NButton v-if="status === 'RUNNING'" size="small" :loading="actionBusy" @click="runAction('pause')">暂停</NButton>
          <NButton v-if="status === 'PAUSED'" size="small" :loading="actionBusy" @click="runAction('resume')">恢复</NButton>
          <NPopconfirm v-if="status === 'RUNNING' || status === 'PAUSED'" @positive-click="runAction('stop')">
            <template #trigger>
              <NButton size="small" :loading="actionBusy">停止</NButton>
            </template>
            游标已提交，回已发布态可再部署
          </NPopconfirm>
          <NButton v-if="status === 'RUNNING' || status === 'PAUSED' || status === 'FAILED'" size="small" type="error" ghost @click="openRecover">
            急救恢复…
          </NButton>
          <NButton v-if="status === 'DRAFT' || status === 'PENDING_CHANGES'" size="small" type="primary" @click="goPublish">
            发布新版本
          </NButton>
          <NButton size="small" @click="goEdit">编辑</NButton>
        </div>
      </div>

      <NSkeleton v-if="loading" text :repeat="4" />

      <template v-else>
        <!-- stats KPI cards (5s poll; degrade without blocking the rest) -->
        <template v-if="statsState.kind === 'undeployed'">
          <NCard size="small" :content-style="{ padding: '16px' }">
            <EmptyState description="管道未部署——先发布并部署" cta="前往发布向导" @cta="goPublish" />
          </NCard>
        </template>
        <template v-else-if="statsState.kind === 'loading'">
          <NGrid :cols="4" :x-gap="16" :y-gap="16">
            <NGi v-for="i in 4" :key="i"><NCard size="small" :content-style="{ padding: '16px' }"><NSkeleton text width="60%" /></NCard></NGi>
          </NGrid>
        </template>
        <template v-else-if="statsState.kind === 'ok'">
          <NAlert v-if="resetAlertShown" type="info" :bordered="false" closable @close="resetAlertShown = false">
            引擎计数已重置（重启），速率基线重新起算
          </NAlert>
          <NGrid :cols="4" :x-gap="16" :y-gap="16">
            <NGi>
              <KpiCard label="累计流入" :value="fmtInt(cumIn)">
                <MetricChart :timestamps="sparkSeries(cumInSpark).map((_, i) => i)" :series="[{ label: '累计流入', points: sparkSeries(cumInSpark) }]" spark :height="28" />
              </KpiCard>
            </NGi>
            <NGi>
              <KpiCard label="累计流出" :value="fmtInt(cumOut)">
                <MetricChart :timestamps="sparkSeries(cumOutSpark).map((_, i) => i)" :series="[{ label: '累计流出', points: sparkSeries(cumOutSpark) }]" spark :height="28" />
              </KpiCard>
            </NGi>
            <NGi>
              <KpiCard label="流入 行/s" :value="rateIn == null ? '—' : rateIn.toFixed(1)">
                <MetricChart :timestamps="sparkSeries(inSpark).map((_, i) => i)" :series="[{ label: '流入', points: sparkSeries(inSpark) }]" spark :height="28" />
              </KpiCard>
            </NGi>
            <NGi>
              <KpiCard label="流出 行/s" :value="rateOut == null ? '—' : rateOut.toFixed(1)">
                <MetricChart :timestamps="sparkSeries(outSpark).map((_, i) => i)" :series="[{ label: '流出', points: sparkSeries(outSpark) }]" spark :height="28" />
              </KpiCard>
            </NGi>
          </NGrid>
          <NText v-if="multiStatementNote" depth="3" class="stmt-note">{{ multiStatementNote }}</NText>
        </template>
        <template v-else-if="statsState.kind === 'not-registered'">
          <NAlert type="warning" :bordered="false">管道不在引擎运行册（未部署或已停止）——{{ statsState.error }}</NAlert>
        </template>
        <template v-else>
          <NAlert type="warning" :bordered="false">{{ statsState.message }}</NAlert>
        </template>

        <!-- trend + offsets -->
        <div class="trend-offset-row">
          <NCard size="small" title="24h 趋势" class="trend-card" :content-style="{ padding: '16px' }">
            <template #header-extra>
              <NRadioGroup v-model:value="trendMode" size="small">
                <NRadioButton value="rate">速率</NRadioButton>
                <NRadioButton value="cum">累计</NRadioButton>
              </NRadioGroup>
            </template>
            <NSkeleton v-if="trendState.kind === 'loading'" text :repeat="3" />
            <EmptyState v-else-if="trendState.kind === 'error'" :description="`趋势加载失败：${trendState.message}`" />
            <MetricChart v-else :timestamps="trendChart.timestamps" :series="trendChart.series" :height="220" />
          </NCard>

          <NCard size="small" class="offsets-card" :content-style="{ padding: '16px' }">
            <template #header>
              消费游标（offsets）
              <NTooltip trigger="hover">
                <template #trigger>
                  <NText depth="3" class="cursor-hint">账本口径</NText>
                </template>
                分区偏移为「下一个待读 offset」；File 源为字节位；totals 为语句累计计数。
              </NTooltip>
            </template>
            <template #header-extra>
              <NButton size="tiny" quaternary :loading="offsetsLoading" @click="fetchOffsets">
                <template #icon><RefreshOutline /></template>
                刷新
              </NButton>
            </template>
            <template v-if="!deployed">
              <EmptyState description="管道未部署——先发布并部署" cta="前往发布向导" @cta="goPublish" />
            </template>
            <NSpin v-else-if="offsetsLoading && offsetRows.length === 0" size="small" class="offsets-spin" />
            <EmptyState
              v-else-if="offsetRows.length === 0"
              :description="offsetsError ? `游标加载失败：${offsetsError}` : '无已提交游标（管道从未消费或引擎未装配）'"
            />
            <div v-else class="offset-rows">
              <div v-for="row in offsetRows" :key="row.statement" class="offset-row">
                <div class="offset-head">
                  <NTag size="small">#{{ row.statement }}</NTag>
                  <MonoField v-if="row.kind === 'kafka'" :value="`${row.group ?? '—'} @ ${row.topic ?? '—'}`" :copyable="false" />
                  <NTag v-if="row.kind === 'file'" size="small">File 源</NTag>
                  <NTag v-else-if="row.kind === 'jdbc'" size="small">JDBC 键游标</NTag>
                </div>
                <div v-if="row.kind === 'kafka'" class="partition-tags">
                  <NTag v-for="[p, o] in row.partitions ?? []" :key="p" size="small" class="mono-inline">P{{ p }}→{{ o }}</NTag>
                  <NText v-if="!row.partitions?.length" depth="3">无分区游标</NText>
                </div>
                <div v-else-if="row.kind === 'file'" class="file-cursor mono-inline">
                  {{ row.path }} · byte_pos {{ row.bytePos ?? 0 }}
                </div>
                <div v-else-if="row.kind === 'jdbc'" class="file-cursor mono-inline">
                  {{ row.jdbc?.key }} = {{ row.jdbc?.value }}
                </div>
                <NText depth="3" class="offset-foot">
                  totals 入 {{ row.totals?.consumed ?? '—' }} / 出 {{ row.totals?.flushed ?? '—' }} · lastAck {{ row.lastAck ?? '—' }}
                </NText>
              </div>
            </div>
          </NCard>
        </div>

        <!-- SSE logs: mounted only when deployed — an undeployed pipeline has
             no engine to stream from (the bridge would emit eof:not_deployed
             and every reconnect would loop) -->
        <NCard size="small" title="运行日志（SSE）" :content-style="{ padding: '8px' }">
          <LogTerminal v-if="deployed" :url="logsUrl" :height="360" :max-lines="2000" :key="pipelineId" />
          <EmptyState v-else description="管道未部署——先发布并部署" cta="前往发布向导" @cta="goPublish" />
        </NCard>

        <!-- meta collapse -->
        <NCollapse>
          <NCollapseItem title="元信息与映射" name="meta">
            <NDescriptions :column="2" size="small" bordered>
              <NDescriptionsItem label="ID">#{{ detail?.id }}</NDescriptionsItem>
              <NDescriptionsItem label="状态"><StateTag v-if="status" domain="pipeline" :value="status" /></NDescriptionsItem>
              <NDescriptionsItem label="版本">v{{ detail?.version ?? 0 }}</NDescriptionsItem>
              <NDescriptionsItem label="消费组"><MonoField :value="groupOf" /></NDescriptionsItem>
              <NDescriptionsItem label="checkpoint">{{ detail?.checkpointMs ?? '—' }} ms</NDescriptionsItem>
              <NDescriptionsItem label="部署错误" :span="2">{{ detail?.deployError || '—' }}</NDescriptionsItem>
            </NDescriptions>
            <div class="mapping-tags">
              <span v-for="[db, dsId] in mappingEntries" :key="db" class="mapping-tag">
                <NTag size="small">{{ db }}</NTag>
                <NText depth="3">→</NText>
                <NTag size="small">{{ dsName(dsId) }}</NTag>
                <StateTag domain="kind" :value="dsType(dsId)" />
              </span>
              <NText v-if="mappingEntries.length === 0" depth="3">无映射</NText>
            </div>
          </NCollapseItem>
          <NCollapseItem :title="`SQL 原文（${detail?.sqlContent?.length ?? 0} 字符）`" name="sql">
            <NCode :code="detail?.sqlContent ?? ''" class="sql-code" />
          </NCollapseItem>
          <NCollapseItem title="编译产物 spec_json" name="spec">
            <NCode v-if="detail?.specJson" :code="detail.specJson" class="sql-code" />
            <NText v-else depth="3">发布后展示</NText>
          </NCollapseItem>
        </NCollapse>
      </template>
    </template>

    <!-- recover modal -->
    <NModal v-model:show="recoverModal" preset="card" title="急救恢复" class="recover-modal">
      <NAlert type="warning" :bordered="false" class="recover-alert">
        目标节点引擎进程需在跑（plan 通道 = HTTP）且节点 state 目录有 offsets 文件；跨节点急救需先迁移 state。
      </NAlert>
      <p class="recover-text">
        急救 = 重新下推管道 plan 到目标引擎节点——引擎装配时自动从 state/offsets-{{ pipelineId }}.json
        恢复消费位点，从上次提交位置续传数据（不丢不重）。
      </p>
      <NForm label-placement="left" label-width="110" size="small">
        <NFormItem label="目标节点">
          <NSelect :value="recoverNodeId ?? 0" :options="recoverNodeOptions" size="small" @update:value="(v: number) => (recoverNodeId = v === 0 ? null : v)" />
        </NFormItem>
      </NForm>
      <template #footer>
        <div class="modal-footer">
          <NButton size="small" @click="recoverModal = false">取消</NButton>
          <NButton size="small" type="error" :loading="recoverSubmitting" @click="submitRecover">执行急救恢复</NButton>
        </div>
      </template>
    </NModal>
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
  flex-wrap: wrap;
}

.title {
  font-size: var(--tr-fs-title-lg);
  font-weight: 600;
  margin: 0;
  color: var(--tr-text-1);
}

.meta-line {
  font-size: var(--tr-fs-body-sm);
}

.head-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.stmt-note {
  font-size: var(--tr-fs-caption);
  font-variant-numeric: tabular-nums;
}

.trend-offset-row {
  display: grid;
  grid-template-columns: minmax(0, 3fr) minmax(0, 2fr);
  gap: 16px;
}

@media (max-width: 960px) {
  .trend-offset-row {
    grid-template-columns: 1fr;
  }
}

.offsets-spin {
  display: block;
  margin: 24px auto;
}

.offset-rows {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.offset-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.partition-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 6px;
}

.file-cursor {
  margin-top: 6px;
  font-size: var(--tr-fs-caption);
  word-break: break-all;
}

.offset-foot {
  display: block;
  margin-top: 4px;
  font-size: var(--tr-fs-caption);
  font-variant-numeric: tabular-nums;
}

.mono-inline {
  font-family: var(--tr-font-mono);
  font-variant-numeric: tabular-nums;
}

.mapping-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 12px;
}

.mapping-tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.sql-code {
  max-height: 320px;
  overflow: auto;
  font-size: var(--tr-fs-caption);
}

.recover-modal {
  width: 560px;
  max-width: 90vw;
}

.recover-alert {
  margin-bottom: 12px;
}

.recover-text {
  color: var(--tr-text-2);
  font-size: var(--tr-fs-body-sm);
  margin: 0 0 12px;
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.cursor-hint {
  font-size: var(--tr-fs-caption);
  margin-left: 8px;
  cursor: help;
}
</style>
