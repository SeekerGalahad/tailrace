<script setup lang="ts">
import { computed, h, ref, watch } from 'vue'
import { useMessage, useDialog, NButton, NCard, NCode, NDescriptions, NDescriptionsItem, NDropdown, NForm, NFormItem, NGrid, NGi, NInput, NInputNumber, NAlert, NDrawer, NDrawerContent, NModal, NPopconfirm, NRadioButton, NRadioGroup, NSelect, NSkeleton, NSpin, NDataTable, NText, NTimeline, NTimelineItem, NTooltip, NResult, type DataTableColumns, type DropdownOption, type FormInst } from 'naive-ui'
import { AddOutline, ChevronDownOutline, RefreshOutline } from '@vicons/ionicons5'
import { nodesApi, type NodeDetail, type NodeListItem, type NodesSummary, type DeployLogItem, type NodeEvent, type ManualRegistered } from '../../api/nodes'
import { artifactsApi, type Artifact } from '../../api/artifacts'
import { pipelinesApi, type PipelineListItem } from '../../api/pipelines'
import { ApiError } from '../../api/http'
import KpiCard from '../../components/KpiCard.vue'
import EmptyState from '../../components/EmptyState.vue'
import StateTag from '../../components/StateTag.vue'
import MonoField from '../../components/MonoField.vue'
import MetricChart from '../../components/MetricChart.vue'
import { useAutoRefresh } from '../../components/useAutoRefresh'

/**
 * Engine-node page (design doc §6.4, this segment's scope: list / detail
 * drawer / manual registration / heartbeat status / action buttons — the
 * artifact card, deploy wizard and orchestration overlay land in a later
 * segment; upgrades still pick their artifact from the registry).
 */

const message = useMessage()
const dialog = useDialog()

// --- list state (30s polling, §6.4 flow 1) ---
const nodes = ref<NodeListItem[]>([])
const summary = ref<NodesSummary | null>(null)
const listLoading = ref(true)
const listError = ref<string | null>(null)

async function fetchList(): Promise<void> {
  try {
    const res = await nodesApi.list({ silent: listLoading.value || nodes.value.length > 0 })
    nodes.value = res.nodes
    summary.value = res.summary
    listError.value = null
  } catch (e) {
    if (nodes.value.length === 0) listError.value = e instanceof Error ? e.message : String(e)
  } finally {
    listLoading.value = false
  }
}
useAutoRefresh(fetchList, 30_000)

function fmtTime(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  const p = (n: number): string => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

// --- node table ---
const columns = computed<DataTableColumns<NodeListItem>>(() => [
  {
    title: '名称',
    key: 'name',
    minWidth: 140,
    render: (row) => h('span', { class: 'node-name' }, row.name),
  },
  { title: 'node_key', key: 'nodeKey', minWidth: 150, render: (row) => h(MonoField, { value: row.nodeKey, headChars: 12 }) },
  { title: '地址', key: 'apiUrl', minWidth: 170, render: (row) => h('span', { class: 'mono-inline' }, row.apiUrl) },
  { title: '版本', key: 'engineVersion', width: 80, render: (row) => row.engineVersion || '—' },
  {
    title: '状态',
    key: 'status',
    width: 110,
    render: (row) =>
      h(
        NTooltip,
        { trigger: 'hover' },
        {
          trigger: () => h(StateTag, { domain: 'node', value: row.status }),
          default: () => `最近心跳：${row.lastHeartbeat ? fmtTime(row.lastHeartbeat) : '从未心跳'}`,
        },
      ),
  },
  { title: '部署', key: 'deployStatus', width: 120, render: (row) => h(StateTag, { domain: 'deploy', value: row.deployStatus }) },
  {
    title: '容量',
    key: 'capacity',
    width: 120,
    render: (row) => `${row.capacity?.runningPipelines ?? 0}/${row.capacity?.pipelines ?? 0} 管 · ${row.capacity?.statements ?? 0} 语`,
  },
  { title: '24h 错误', key: 'errors24h', width: 90, render: (row) => String(row.errors24h ?? 0) },
  {
    title: '操作',
    key: 'actions',
    width: 210,
    render: (row) =>
      h('div', { class: 'row-actions' }, [
        h(NButton, { size: 'tiny', quaternary: true, onClick: () => openDetail(row) }, { default: () => '详情' }),
        h(NButton, { size: 'tiny', quaternary: true, onClick: () => openLogDrawer(row) }, { default: () => '日志' }),
        h(
          NDropdown,
          { options: moreOptions(row), trigger: 'click', onSelect: (key: string) => onMoreSelect(key, row) },
          {
            trigger: () =>
              h(NButton, { size: 'tiny', quaternary: true }, {
                default: () => '更多',
                icon: () => h(ChevronDownOutline, { style: 'width:12px' }),
              }),
          },
        ),
      ]),
  },
])

/** Lifecycle three-axis menu (§6.4): run / version / governance. */
function moreOptions(row: NodeListItem): DropdownOption[] {
  const offline = row.status !== 'ONLINE'
  return [
    { label: '运行', key: 'group-run', children: [
      { label: '启动（SSH 拉起）', key: 'start' },
      { label: '停止', key: 'stop', disabled: offline },
      { label: '重启', key: 'restart', disabled: offline },
    ] },
    { label: '版本', key: 'group-version', children: [
      { label: '升级', key: 'upgrade', disabled: offline },
      { label: '重新部署', key: 'redeploy', disabled: offline },
      { label: '回滚', key: 'rollback', disabled: offline },
    ] },
    { label: '治理', key: 'group-gov', children: [
      { label: '吊销 token', key: 'revoke' },
      { label: '清理删除', key: 'delete', disabled: false },
    ] },
  ]
}

// --- node actions (confirm copy canon, §6.4 flow 4/6/7) ---
const ACTION_COPY: Record<string, { title: string; content: string }> = {
  stop: { title: '停止引擎', content: '引擎优雅停机，管道游标已提交。' },
  restart: { title: '重启引擎', content: 'stop → start，at-least-once 重放。' },
  rollback: { title: '回滚版本', content: 'current 软链回退一步，保留数据进度。' },
  revoke: { title: '吊销 token', content: '吊销后该节点凭据票据请求将被拒绝（票据联动清空）。' },
  delete: {
    title: '清理删除节点',
    content:
      'best-effort 停止引擎并清空远端安装目录（SSH 不可达跳过并警告），随后删除节点行、部署日志与事件历史——不可恢复。',
  },
}

async function runAction(row: NodeListItem, action: 'start' | 'stop' | 'restart' | 'rollback'): Promise<void> {
  await nodesApi.action(row.id, action)
  message.success(`动作已受理（202）：${action} @ ${row.name}，编排进度见日志抽屉`)
  openLogDrawer(row)
  void fetchList()
}

function onMoreSelect(key: string, row: NodeListItem): void {
  if (key === 'start') {
    dialog.info({
      title: '启动引擎',
      content: '通过 SSH 拉起远端引擎进程（节点 OFFLINE 时可用）。',
      positiveText: '启动',
      negativeText: '取消',
      onPositiveClick: () => runAction(row, 'start'),
    })
    return
  }
  if (key === 'stop' || key === 'restart' || key === 'rollback') {
    const copy = ACTION_COPY[key]
    dialog.warning({
      title: copy.title,
      content: copy.content,
      positiveText: '确认',
      negativeText: '取消',
      onPositiveClick: () => runAction(row, key),
    })
    return
  }
  if (key === 'upgrade') {
    openUpgradeModal(row)
    return
  }
  if (key === 'redeploy') {
    openRedeployModal(row)
    return
  }
  if (key === 'revoke') {
    const copy = ACTION_COPY.revoke
    dialog.warning({
      title: copy.title,
      content: copy.content,
      positiveText: '吊销',
      negativeText: '取消',
      onPositiveClick: async () => {
        await nodesApi.revoke(row.id)
        message.success(`已吊销 ${row.name} 的 token`)
        void fetchList()
      },
    })
    return
  }
  if (key === 'delete') {
    const copy = ACTION_COPY.delete
    dialog.warning({
      title: copy.title,
      content: copy.content,
      positiveText: '删除',
      negativeText: '取消',
      onPositiveClick: async () => {
        const res = await nodesApi.remove(row.id)
        if (res?.warning) {
          dialog.warning({ title: '远端清理跳过', content: `远端清理跳过：${res.warning}`, positiveText: '知道了' })
        } else {
          message.success(`已删除 ${row.name}${res?.remoteCleaned ? '（远端已清理）' : ''}`)
        }
        void fetchList()
      },
    })
  }
}

// --- upgrade / redeploy modals (artifact picker) ---
const upgradeModal = ref(false)
const upgradeNode = ref<NodeListItem | null>(null)
const upgradeArtifactId = ref<number | null>(null)
const artifacts = ref<Artifact[]>([])
const artifactsLoading = ref(false)

async function loadArtifacts(): Promise<void> {
  artifactsLoading.value = true
  try {
    artifacts.value = (await artifactsApi.list({ silent: true })).artifacts
  } catch (e) {
    message.error(e instanceof Error ? e.message : '程序包列表加载失败')
  } finally {
    artifactsLoading.value = false
  }
}

const artifactOptions = computed(() =>
  artifacts.value.map((a) => ({
    label: `${a.name} v${a.version} · ${a.sha256.slice(0, 8)}`,
    value: a.id,
  })),
)

function openUpgradeModal(row: NodeListItem): void {
  upgradeNode.value = row
  upgradeArtifactId.value = null
  upgradeModal.value = true
  if (artifacts.value.length === 0) void loadArtifacts()
}

async function submitUpgrade(): Promise<void> {
  const row = upgradeNode.value
  if (!row || upgradeArtifactId.value == null) return
  try {
    await nodesApi.action(row.id, 'upgrade', upgradeArtifactId.value)
    upgradeModal.value = false
    message.success(`升级已受理（202）：${row.name}，编排进度见日志抽屉`)
    openLogDrawer(row)
    void fetchList()
  } catch {
    /* toast already shown by http layer */
  }
}

// Redeploy = POST /api/nodes/deploy {nodeId, artifactId, clean, wipeState} (§6.4 flow 6)
const redeployModal = ref(false)
const redeployNode = ref<NodeListItem | null>(null)
const redeployArtifactId = ref<number | null>(null)
const redeployWipeState = ref(false)
const redeploySubmitting = ref(false)

function openRedeployModal(row: NodeListItem): void {
  redeployNode.value = row
  redeployArtifactId.value = row.artifactId
  redeployWipeState.value = false
  redeployModal.value = true
  if (artifacts.value.length === 0) void loadArtifacts()
}

async function submitRedeploy(): Promise<void> {
  const row = redeployNode.value
  const artifactId = redeployArtifactId.value
  const wipeState = redeployWipeState.value
  if (!row || artifactId == null) return
  const doDeploy = async (): Promise<void> => {
    redeploySubmitting.value = true
    try {
      await nodesApi.deploy({ nodeId: row.id, artifactId, clean: true, wipeState })
      redeployModal.value = false
      message.success('重新部署已提交，进度见日志抽屉')
      openLogDrawer(row)
      void fetchList()
    } finally {
      redeploySubmitting.value = false
    }
  }
  if (wipeState) {
    // wipeState risk: pipelines consuming from `latest` lose their in-flight
    // backlog at the wipe instant (§6.4 flow 6) — force a danger confirm.
    let latestCount = 0
    try {
      const res = await pipelinesApi.list({ page: 1, pageSize: 200 }, { silent: true })
      latestCount = res.pipelines.filter((p) => p.assignedNode?.id === row.id && p.consumedOffsetStart === 'latest').length
    } catch {
      /* proceed with the generic danger confirm */
    }
    if (latestCount > 0) {
      dialog.error({
        title: '清空 state 将丢失未消费数据',
        content: `该节点挂载 ${latestCount} 条从最新消费（latest）的管道：清空瞬间的未消费数据将永久丢失。`,
        positiveText: '仍要清空并重新部署',
        negativeText: '取消',
        onPositiveClick: doDeploy,
      })
      return
    }
  }
  await doDeploy()
}

// --- manual registration (§6.4 flow 7) ---
const manualModal = ref(false)
const manualFormRef = ref<FormInst | null>(null)
const manualForm = ref({ name: '', host: '', enginePort: 18100 })
const manualSubmitting = ref(false)
const manualError = ref<string | null>(null)

const manualRules = {
  name: { required: true, message: '请输入节点名称', trigger: 'input' },
  host: { required: true, message: '请输入主机 IP', trigger: 'input' },
  enginePort: { required: true, type: 'number' as const, message: '请输入引擎端口', trigger: 'input' },
}

function openManualModal(): void {
  manualForm.value = { name: '', host: '', enginePort: 18100 }
  manualError.value = null
  manualModal.value = true
}

async function submitManual(): Promise<void> {
  await manualFormRef.value?.validate()
  manualSubmitting.value = true
  manualError.value = null
  try {
    const cred = await nodesApi.registerManual({ ...manualForm.value }, { silent: true })
    manualModal.value = false
    // One-time credential echo (§4.3): show before anything else — closing
    // the dialog loses the token for good (server never returns it again).
    manualCred.value = cred
    message.success(`已注册 ${manualForm.value.name}（healthz 探活通过）`)
    void fetchList()
  } catch (e) {
    // 502 = console healthz probe failed (§6.4 flow 7)
    manualError.value =
      e instanceof ApiError
        ? `探活失败：${e.message}（检查引擎进程与端口/防火墙）`
        : e instanceof Error
          ? e.message
          : String(e)
  } finally {
    manualSubmitting.value = false
  }
}

// --- one-time credential echo (§4.3: token never shown again afterwards) ---
const manualCred = ref<ManualRegistered | null>(null)

// --- deploy/action log drawer (§6.4: NTimeline, 3s polling while open) ---
const logDrawer = ref(false)
const logNode = ref<NodeListItem | null>(null)
const logItems = ref<DeployLogItem[]>([])
const logLoading = ref(true)

function openLogDrawer(row: NodeListItem): void {
  logNode.value = row
  logItems.value = []
  logLoading.value = true
  logDrawer.value = true
}

async function fetchLog(): Promise<void> {
  if (!logDrawer.value || !logNode.value) return
  try {
    const res = await nodesApi.deployLog(logNode.value.id, { limit: 100 }, { silent: true })
    logItems.value = res.items
  } catch {
    /* keep last snapshot; the next tick retries */
  } finally {
    logLoading.value = false
  }
}
useAutoRefresh(fetchLog, 3_000)

// --- detail drawer (§6.4: identity / run / pipelines / runtime / events / metrics) ---
const detailDrawer = ref(false)
const detailRow = ref<NodeListItem | null>(null)
const detail = ref<NodeDetail | null>(null)
const detailLoading = ref(false)
const detailError = ref<string | null>(null)

const relatedPipelines = ref<PipelineListItem[]>([])
const runtimeRaw = ref<unknown>(null)
const runtimeLoading = ref(false)
const runtimeError = ref<string | null>(null)
const events = ref<NodeEvent[]>([])
const metricsWindow = ref<1 | 6 | 24>(1)
const metricsPoints = ref<Array<{ ts: number; rssBytes: number; cpuPercent: number; rxBps: number; txBps: number }>>([])

function openDetail(row: NodeListItem): void {
  detailRow.value = row
  detailDrawer.value = true
  void loadDetail(row.id)
}

async function loadDetail(id: number): Promise<void> {
  detailLoading.value = true
  detailError.value = null
  detail.value = null
  try {
    const [d, pl] = await Promise.all([
      nodesApi.get(id, { silent: true }),
      pipelinesApi.list({ page: 1, pageSize: 200 }, { silent: true }).catch(() => null),
    ])
    detail.value = d
    relatedPipelines.value = pl ? pl.pipelines.filter((p) => p.assignedNode?.id === id) : []
  } catch (e) {
    detailError.value = e instanceof Error ? e.message : String(e)
  } finally {
    detailLoading.value = false
  }
  void loadRuntime(id)
}

/** Runtime snapshot: proxied engine GET /v1/pipelines; 502 → degraded alert (§6.4). */
async function loadRuntime(id: number): Promise<void> {
  runtimeLoading.value = true
  runtimeError.value = null
  try {
    runtimeRaw.value = await nodesApi.pipelinesRuntime(id, { silent: true })
  } catch (e) {
    runtimeRaw.value = null
    runtimeError.value =
      e instanceof ApiError && e.status === 502
        ? '引擎不可达——节点离线或为旧版制品，升级引擎制品后可用'
        : e instanceof Error
          ? e.message
          : String(e)
  } finally {
    runtimeLoading.value = false
  }
}

async function fetchEventsPolling(): Promise<void> {
  if (!detailDrawer.value || !detailRow.value) return
  try {
    const ev = await nodesApi.events(detailRow.value.id, { hours: 168 }, { silent: true })
    events.value = ev.items
  } catch {
    /* keep last snapshot */
  }
}
useAutoRefresh(fetchEventsPolling, 15_000)

async function fetchMetricsPolling(): Promise<void> {
  if (!detailDrawer.value || !detailRow.value) return
  try {
    const m = await nodesApi.metrics(detailRow.value.id, { hours: metricsWindow.value }, { silent: true })
    metricsPoints.value = m.points.map((p) => ({ ...p, ts: Math.floor(new Date(p.ts).getTime() / 1000) }))
  } catch {
    /* keep last snapshot */
  }
}
useAutoRefresh(fetchMetricsPolling, 60_000)

// window switch (近1h|近6h|近24h) → immediate refetch instead of waiting a tick
watch(metricsWindow, () => void fetchMetricsPolling())

function metricsSeries(kind: 'rss-cpu' | 'net'): { timestamps: number[]; series: Array<{ label: string; points: Array<number | null>; yAxis?: 1 | 2 }> } {
  const timestamps = metricsPoints.value.map((p) => p.ts)
  if (kind === 'rss-cpu') {
    return {
      timestamps,
      series: [
        { label: 'RSS (MB)', points: metricsPoints.value.map((p) => Math.round((p.rssBytes / 1048576) * 10) / 10) },
        { label: 'CPU %', points: metricsPoints.value.map((p) => p.cpuPercent), yAxis: 2 },
      ],
    }
  }
  return {
    timestamps,
    series: [
      { label: '接收 KB/s', points: metricsPoints.value.map((p) => Math.round(p.rxBps / 1024)) },
      { label: '发送 KB/s', points: metricsPoints.value.map((p) => Math.round(p.txBps / 1024)) },
    ],
  }
}

const metricRss = computed(() => metricsSeries('rss-cpu'))
const metricNet = computed(() => metricsSeries('net'))

function groupOf(p: PipelineListItem): string {
  return p.groupPrefix ? `${p.groupPrefix}.s*` : `tr.p${p.id}.s*`
}

function onPipelineAction(p: PipelineListItem, action: 'pause' | 'resume' | 'stop'): void {
  const run = async (): Promise<void> => {
    try {
      const res = action === 'pause' ? await pipelinesApi.pause(p.id) : action === 'resume' ? await pipelinesApi.resume(p.id) : await pipelinesApi.stop(p.id)
      message.success(`#${p.id} → ${res.status}`)
      if (detailRow.value) void loadDetail(detailRow.value.id)
    } catch {
      /* toast already shown */
    }
  }
  if (action === 'stop') {
    dialog.warning({
      title: '停止管道',
      content: '游标已提交，回已发布态可再部署。',
      positiveText: '停止',
      negativeText: '取消',
      onPositiveClick: run,
    })
  } else {
    void run()
  }
}

const eventColumns: DataTableColumns<NodeEvent> = [
  { title: '时间', key: 'ts', width: 80, render: (e) => fmtTime(e.ts).slice(6) },
  { title: '级别', key: 'level', width: 70, render: (e) => h(StateTag, { domain: 'eventLevel', value: e.level }) },
  { title: '类型', key: 'kind', width: 110, render: (e) => h('span', { class: 'mono-inline' }, e.kind) },
  { title: '内容', key: 'message', ellipsis: { tooltip: true } },
]</script>

<template>
  <main class="page">
    <div class="page-head">
      <div>
        <h1 class="title">引擎节点</h1>
        <p class="subtitle">引擎 = 在服务器上常驻运行、真正搬运数据的小程序；90 秒内有心跳即在线。</p>
      </div>
      <div class="head-actions">
        <NButton size="small" @click="fetchList()">
          <template #icon><RefreshOutline /></template>
          刷新
        </NButton>
        <NButton size="small" @click="openManualModal">
          <template #icon><AddOutline /></template>
          手动注册
        </NButton>
      </div>
    </div>

    <!-- error: whole page NResult + retry (first load only; later failures keep data) -->
    <NResult
      v-if="listError && nodes.length === 0"
      status="error"
      title="节点列表加载失败"
      :description="listError"
      class="block-error"
    >
      <template #footer>
        <NButton size="small" @click="fetchList()">重试</NButton>
      </template>
    </NResult>

    <template v-else>
      <!-- ① summary cards -->
      <NGrid :cols="4" :x-gap="16" :y-gap="16" class="summary-grid">
        <NGi><KpiCard label="在线" :value="summary ? `${summary.online}/${summary.total}` : '—'" :loading="listLoading && !summary" /></NGi>
        <NGi><KpiCard label="运行管道" :value="summary?.runningPipelines ?? '—'" :loading="listLoading && !summary" /></NGi>
        <NGi><KpiCard label="24h 事件" :value="summary?.events24h ?? '—'" :loading="listLoading && !summary" /></NGi>
        <NGi><KpiCard label="24h 错误" :value="summary?.errors24h ?? '—'" :loading="listLoading && !summary" /></NGi>
      </NGrid>

      <!-- ③ node table -->
      <NCard size="small" :content-style="{ padding: '0' }" class="node-card">
        <template #header>节点表</template>
        <template #header-extra>
          <NText depth="3" class="card-note">停止/重启/升级/回滚对 OFFLINE 节点禁用；启动恒可用</NText>
        </template>
        <NSkeleton v-if="listLoading && nodes.length === 0" text :repeat="4" class="table-skeleton" />
        <EmptyState
          v-else-if="nodes.length === 0"
          description="尚无引擎节点"
          cta="手动注册"
          @cta="openManualModal"
        />
        <NDataTable
          v-else
          :columns="columns"
          :data="nodes"
          :row-key="(r: NodeListItem) => r.id"
          size="small"
          :loading="listLoading"
        />
      </NCard>
    </template>

    <!-- manual registration modal -->
    <NModal v-model:show="manualModal" preset="card" title="手动注册节点" class="modal-sm">
      <NAlert v-if="manualError" type="error" :bordered="false" class="form-alert">{{ manualError }}</NAlert>
      <NForm ref="manualFormRef" :model="manualForm" :rules="manualRules" label-placement="left" label-width="90">
        <NFormItem label="名称" path="name">
          <NInput v-model:value="manualForm.name" placeholder="edge-01" />
        </NFormItem>
        <NFormItem label="主机 IP" path="host">
          <NInput v-model:value="manualForm.host" placeholder="10.1.1.5" />
        </NFormItem>
        <NFormItem label="引擎端口" path="enginePort">
          <NInputNumber v-model:value="manualForm.enginePort" :min="1" :max="65535" style="width: 100%" />
        </NFormItem>
      </NForm>
      <p class="modal-hint">console 将先探活 <code>GET http://host:port/healthz</code>，200 才登记。</p>
      <template #footer>
        <div class="modal-footer">
          <NButton size="small" @click="manualModal = false">取消</NButton>
          <NButton size="small" type="primary" :loading="manualSubmitting" @click="submitManual">注册</NButton>
        </div>
      </template>
    </NModal>

    <!-- one-time credential echo: bootstrap_token shown exactly once (§4.3) -->
    <NModal
      :show="manualCred != null"
      preset="card"
      title="注册成功——凭据仅此一次展示"
      class="modal-sm"
      :mask-closable="false"
      @update:show="(v: boolean) => { if (!v) manualCred = null }"
    >
      <NAlert type="warning" :bordered="false" class="form-alert">
        bootstrap_token 关闭后不可再查看、无法找回（服务端不再回显）。请立即复制并配置到该节点的引擎进程。
      </NAlert>
      <NDescriptions label-placement="left" :column="1" size="small" bordered>
        <NDescriptionsItem label="node_key">
          <MonoField v-if="manualCred" :value="manualCred.nodeKey" />
        </NDescriptionsItem>
        <NDescriptionsItem label="bootstrap_token">
          <MonoField v-if="manualCred" :value="manualCred.bootstrapToken" />
        </NDescriptionsItem>
      </NDescriptions>
      <template #footer>
        <div class="modal-footer">
          <NButton size="small" type="primary" @click="manualCred = null">我已保存凭据</NButton>
        </div>
      </template>
    </NModal>

    <!-- upgrade modal -->
    <NModal v-model:show="upgradeModal" preset="card" title="升级引擎" class="modal-sm">
      <NSpin v-if="artifactsLoading" size="small" class="modal-spin" />
      <EmptyState v-else-if="artifactOptions.length === 0" description="制品库为空——请先在服务端上传程序包" />
      <NForm v-else label-placement="left" label-width="90">
        <NFormItem label="目标制品">
          <NSelect v-model:value="upgradeArtifactId" :options="artifactOptions" placeholder="选择程序包" />
        </NFormItem>
      </NForm>
      <template #footer>
        <div class="modal-footer">
          <NButton size="small" @click="upgradeModal = false">取消</NButton>
          <NButton size="small" type="primary" :disabled="upgradeArtifactId == null" @click="submitUpgrade">升级</NButton>
        </div>
      </template>
    </NModal>

    <!-- redeploy modal -->
    <NModal v-model:show="redeployModal" preset="card" title="重新部署" class="modal-sm">
      <NSpin v-if="artifactsLoading" size="small" class="modal-spin" />
      <EmptyState v-else-if="artifactOptions.length === 0" description="制品库为空——请先在服务端上传程序包" />
      <template v-else>
        <NForm label-placement="left" label-width="90">
          <NFormItem label="制品">
            <NSelect v-model:value="redeployArtifactId" :options="artifactOptions" placeholder="选择程序包" />
          </NFormItem>
        </NForm>
        <NAlert type="warning" :bordered="false" class="form-alert">
          清空 state（wipeState）将同时清除消费位点：从最新消费（latest）的管道在清空瞬间的未消费数据将永久丢失。
        </NAlert>
        <label class="wipe-check">
          <input v-model="redeployWipeState" type="checkbox" />
          清空 state（wipeState）
        </label>
      </template>
      <template #footer>
        <div class="modal-footer">
          <NButton size="small" @click="redeployModal = false">取消</NButton>
          <NButton size="small" type="error" :loading="redeploySubmitting" :disabled="redeployArtifactId == null" @click="submitRedeploy">
            重新部署
          </NButton>
        </div>
      </template>
    </NModal>

    <!-- deploy/action log drawer -->
    <NDrawer v-model:show="logDrawer" :width="560">
      <NDrawerContent :title="`部署/动作日志 · ${logNode?.name ?? ''}`" closable>
        <NSpin v-if="logLoading" size="small" class="modal-spin" />
        <EmptyState v-else-if="logItems.length === 0" description="暂无日志（动作受理后逐步落库）" positive />
        <NTimeline v-else>
          <NTimelineItem
            v-for="(item, i) in logItems"
            :key="i"
            :type="item.status === 'FAIL' ? 'error' : item.status === 'OK' || item.status === 'REGISTERED' ? 'success' : 'default'"
            :title="item.step"
            :time="`${fmtTime(item.ts)} · ${item.operator}`"
          >
            <pre class="log-output">{{ item.output }}</pre>
          </NTimelineItem>
        </NTimeline>
      </NDrawerContent>
    </NDrawer>

    <!-- detail drawer -->
    <NDrawer v-model:show="detailDrawer" :width="640" @after-leave="detailRow = null">
      <NDrawerContent :title="`节点详情 · ${detailRow?.name ?? ''}`" closable>
        <NSpin v-if="detailLoading" size="small" class="modal-spin" />
        <NResult v-else-if="detailError" status="warning" title="详情加载失败" :description="detailError">
          <template #footer>
            <NButton size="small" @click="detailRow && loadDetail(detailRow.id)">重试</NButton>
          </template>
        </NResult>
        <template v-else-if="detail">
          <!-- ① identity -->
          <section class="detail-section">
            <h3 class="section-title">身份</h3>
            <NDescriptions :column="2" size="small" label-placement="left" bordered>
              <NDescriptionsItem label="行号">#{{ detail.id }}</NDescriptionsItem>
              <NDescriptionsItem label="名称">{{ detail.name }}</NDescriptionsItem>
              <NDescriptionsItem label="node_key" :span="2">
                <MonoField :value="detail.nodeKey" />
              </NDescriptionsItem>
            </NDescriptions>
          </section>

          <!-- ② run & deploy -->
          <section class="detail-section">
            <h3 class="section-title">运行与部署</h3>
            <NDescriptions :column="2" size="small" label-placement="left" bordered>
              <NDescriptionsItem label="状态">
                <StateTag domain="node" :value="detail.status" />
                <NText depth="3" class="heartbeat">心跳 {{ fmtTime(detail.lastHeartbeat) }}</NText>
              </NDescriptionsItem>
              <NDescriptionsItem label="api">{{ detail.apiUrl }}</NDescriptionsItem>
              <NDescriptionsItem label="SSH">{{ detail.sshUser ? `${detail.sshUser}@${detail.host}:${detail.sshPort}` : '—' }}</NDescriptionsItem>
              <NDescriptionsItem label="部署"><StateTag domain="deploy" :value="detail.deployStatus" /></NDescriptionsItem>
              <NDescriptionsItem label="版本">{{ detail.engineVersion || '—' }}</NDescriptionsItem>
              <NDescriptionsItem label="容量">
                {{ detail.capacity?.runningPipelines ?? 0 }}/{{ detail.capacity?.pipelines ?? 0 }} 管 · {{ detail.capacity?.statements ?? 0 }} 语 / {{ detail.capacity?.memoryMb ?? 0 }} MB
              </NDescriptionsItem>
            </NDescriptions>
          </section>

          <!-- ③ related pipelines (client-filtered assignedNode, §6.4) -->
          <section class="detail-section">
            <h3 class="section-title">关联管道（{{ relatedPipelines.length }}）</h3>
            <EmptyState v-if="relatedPipelines.length === 0" description="本节点暂无关联管道" positive />
            <NDataTable
              v-else
              :columns="[
                { title: 'ID', key: 'id', width: 50 },
                { title: '名称', key: 'name' },
                { title: '状态', key: 'status', width: 120, render: (p: PipelineListItem) => h(StateTag, { domain: 'pipeline', value: p.status }) },
                { title: '版本', key: 'version', width: 60 },
                { title: '消费组', key: 'group', render: (p: PipelineListItem) => h(MonoField, { value: groupOf(p) }) },
                {
                  title: '操作', key: 'ops', width: 140,
                  render: (p: PipelineListItem) => h('div', { class: 'row-actions' }, [
                    p.status === 'RUNNING' ? h(NButton, { size: 'tiny', quaternary: true, onClick: () => onPipelineAction(p, 'pause') }, { default: () => '暂停' }) : null,
                    p.status === 'PAUSED' ? h(NButton, { size: 'tiny', quaternary: true, onClick: () => onPipelineAction(p, 'resume') }, { default: () => '恢复' }) : null,
                    p.status === 'RUNNING' || p.status === 'PAUSED'
                      ? h(NPopconfirm, { onPositiveClick: () => onPipelineAction(p, 'stop') }, { trigger: () => h(NButton, { size: 'tiny', quaternary: true, type: 'error' }, { default: () => '停止' }), default: () => '游标已提交，回已发布态可再部署' })
                      : null,
                  ]),
                },
              ]"
              :data="relatedPipelines"
              :row-key="(p: PipelineListItem) => p.id"
              size="small"
            />
            <NText depth="3" class="card-note">SQL 原文见各管道的运行监控页</NText>
          </section>

          <!-- ④ runtime snapshot (proxied; 502 degrades) -->
          <section class="detail-section">
            <div class="section-head">
              <h3 class="section-title">运行态快照（代理引擎 /v1/pipelines）</h3>
              <NButton size="tiny" quaternary @click="detailRow && loadRuntime(detailRow.id)">刷新</NButton>
            </div>
            <NSpin v-if="runtimeLoading" size="small" class="modal-spin" />
            <NAlert v-else-if="runtimeError" type="warning" :bordered="false">{{ runtimeError }}</NAlert>
            <NCode v-else :code="JSON.stringify(runtimeRaw, null, 2)" class="json-block" />
          </section>

          <!-- ⑤ events (7d) -->
          <section class="detail-section">
            <h3 class="section-title">事件流（近 7 天）</h3>
            <EmptyState v-if="events.length === 0" description="近 7 天无事件" positive />
            <NDataTable v-else :columns="eventColumns" :data="events" :row-key="(e: NodeEvent) => `${e.ts}:${e.kind}:${e.message.slice(0, 24)}`" size="small" :pagination="{ pageSize: 8 }" />
          </section>

          <!-- ⑥ resource trends -->
          <section class="detail-section">
            <div class="section-head">
              <h3 class="section-title">资源趋势</h3>
              <NRadioGroup v-model:value="metricsWindow" size="small">
                <NRadioButton :value="1">近 1h</NRadioButton>
                <NRadioButton :value="6">近 6h</NRadioButton>
                <NRadioButton :value="24">近 24h</NRadioButton>
              </NRadioGroup>
            </div>
            <EmptyState
              v-if="metricsPoints.length === 0"
              :description="detail.status === 'OFFLINE' ? '节点离线且无历史资源指标' : '引擎版本未上报资源指标（旧制品）'"
              positive
            />
            <template v-else>
              <MetricChart :timestamps="metricRss.timestamps" :series="metricRss.series" :height="160" />
              <MetricChart :timestamps="metricNet.timestamps" :series="metricNet.series" :height="160" class="chart-gap" />
            </template>
          </section>
        </template>
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

.node-card :deep(.n-card__content) {
  padding: 8px;
}

.card-note {
  font-size: var(--tr-fs-caption);
}

.table-skeleton {
  padding: 16px;
}

.mono-inline {
  font-family: var(--tr-font-mono);
  font-variant-numeric: tabular-nums;
  word-break: break-all;
}

.node-name {
  font-weight: 500;
}

.row-actions {
  display: inline-flex;
  align-items: center;
  gap: 2px;
}

.modal-sm {
  width: 480px;
  max-width: 90vw;
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.modal-spin {
  display: block;
  margin: 24px auto;
}

.modal-hint {
  color: var(--tr-text-3);
  font-size: var(--tr-fs-caption);
  margin: 8px 0 0;
}

.form-alert {
  margin-bottom: 12px;
}

.wipe-check {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-top: 8px;
  font-size: var(--tr-fs-body-sm);
  color: var(--tr-text-2);
  cursor: pointer;
}

.log-output {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  font-family: var(--tr-font-mono);
  font-size: var(--tr-fs-caption);
  color: var(--tr-text-2);
}

.detail-section {
  margin-bottom: 20px;
}

.section-title {
  font-size: var(--tr-fs-body);
  font-weight: 500;
  margin: 0 0 8px;
  color: var(--tr-text-1);
}

.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}

.heartbeat {
  margin-left: 8px;
  font-size: var(--tr-fs-caption);
}

.json-block {
  max-height: 240px;
  overflow: auto;
  font-size: var(--tr-fs-caption);
}

.chart-gap {
  margin-top: 12px;
}
</style>
