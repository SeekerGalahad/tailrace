<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useMessage, NAlert, NButton, NCard, NCode, NDataTable, NResult, NSelect, NSpin, NStep, NSteps, NTag, NText, NTooltip, type DataTableColumns } from 'naive-ui'
import { ArrowBackOutline } from '@vicons/ionicons5'
import { pipelinesApi, type CompiledStatement, type Diagnostic, type PipelineDetail } from '../../api/pipelines'
import { nodesApi, type NodeListItem } from '../../api/nodes'
import { ApiError } from '../../api/http'
import EmptyState from '../../components/EmptyState.vue'
import StateTag from '../../components/StateTag.vue'
import MonoField from '../../components/MonoField.vue'

/**
 * Publish wizard (design doc §6.5.3, basic version): ① compile precheck →
 * ② publish version → ③ deploy node. `?from=deploy` enters at step 3.
 * A 501 from the deploy endpoint (SSH deploy machinery not enabled) degrades
 * to manual-registration guidance instead of a dead end.
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

// --- steps ---
const fromDeploy = route.query.from === 'deploy'
const step = ref(fromDeploy ? 3 : 1)

// step 1: compile
const compiling = ref(false)
const compileOk = ref<boolean | null>(null)
const statements = ref<CompiledStatement[]>([])
const diagnostics = ref<Diagnostic[]>([])

// step 2: publish
const publishing = ref(false)
const publishedVersion = ref<number | null>(null)
const publishedSpec = ref<string | null>(null)
const publishDiagnostics = ref<Diagnostic[]>([])

// step 3: deploy
const onlineNodes = ref<NodeListItem[]>([])
const nodesError = ref<string | null>(null)
const deployNodeId = ref<number | null>(null) // null = auto-assign
const deploying = ref(false)
const deployErrorText = ref<string | null>(null)
const deployNotImplemented = ref(false)

function diagnosticsFromError(e: unknown): Diagnostic[] {
  if (e instanceof ApiError) {
    const d = e.data as { diagnostics?: Diagnostic[]; error?: { diagnostics?: Diagnostic[] } } | null
    return d?.diagnostics ?? d?.error?.diagnostics ?? []
  }
  return []
}

async function runCompile(): Promise<void> {
  if (!detail.value) return
  compiling.value = true
  try {
    const res = await pipelinesApi.compile({ sql: detail.value.sqlContent, dsMapping: detail.value.dsMapping ?? {} })
    compileOk.value = res.ok
    statements.value = res.statements ?? []
    diagnostics.value = res.diagnostics ?? []
  } catch (e) {
    const ds = diagnosticsFromError(e)
    if (ds.length > 0) {
      compileOk.value = false
      diagnostics.value = ds
    } else {
      compileOk.value = null // network error — retryable
      message.error(e instanceof Error ? e.message : '编译请求失败')
    }
  } finally {
    compiling.value = false
  }
}

async function loadNodes(): Promise<void> {
  nodesError.value = null
  try {
    const res = await nodesApi.list({ silent: true })
    onlineNodes.value = res.nodes.filter((n) => n.status === 'ONLINE')
  } catch (e) {
    nodesError.value = e instanceof Error ? e.message : String(e)
  }
}

onMounted(async () => {
  try {
    detail.value = await pipelinesApi.get(pipelineId, { silent: true })
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) notFound.value = true
    else loadError.value = e instanceof Error ? e.message : String(e)
    loading.value = false
    return
  }
  loading.value = false
  if (fromDeploy) {
    await loadNodes()
    return
  }
  await Promise.all([runCompile(), loadNodes()])
})

// --- step transitions ---
function goEdit(): void {
  void router.push(`/pipelines/edit?mode=edit&id=${pipelineId}`)
}

const isAlreadyPublished = computed(() => detail.value?.status === 'PUBLISHED')

const nextVersion = computed(() => {
  const v = detail.value?.version ?? 0
  return v + 1
})

async function doPublish(): Promise<void> {
  publishing.value = true
  publishDiagnostics.value = []
  try {
    const res = await pipelinesApi.publish(pipelineId)
    publishedVersion.value = res.version
    publishedSpec.value = res.specJson
    if (detail.value) detail.value.status = 'PUBLISHED'
    message.success(`已发布 v${res.version}`)
  } catch (e) {
    const ds = diagnosticsFromError(e)
    if (ds.length > 0) publishDiagnostics.value = ds
    /* else toast already shown (400 without diagnostics / network) */
  } finally {
    publishing.value = false
  }
}

async function doDeploy(): Promise<void> {
  deploying.value = true
  deployErrorText.value = null
  deployNotImplemented.value = false
  try {
    await pipelinesApi.deploy(pipelineId, deployNodeId.value)
    message.success('部署成功，跳转运行监控')
    await router.replace(`/pipelines/${pipelineId}/monitor`)
  } catch (e) {
    if (e instanceof ApiError) {
      if (e.status === 501) {
        // SSH deploy endpoint not implemented on this console → manual path.
        deployNotImplemented.value = true
      } else if (e.status === 502) {
        deployErrorText.value = e.message || '引擎不可达或返回 ok:false（已记 DEPLOY_FAILED）'
      } else {
        deployErrorText.value = e.message
      }
    } else {
      deployErrorText.value = e instanceof Error ? e.message : String(e)
    }
  } finally {
    deploying.value = false
  }
}

// --- statements table (§6.5.3 step 1) ---
const stmtColumns = computed<DataTableColumns<CompiledStatement>>(() => [
  { title: '#', key: 'index', width: 48, render: (s) => `#${s.index}` },
  { title: '源', key: 'source', minWidth: 200, render: (s) => h(MonoField, { value: s.source }) },
  { title: '变换下推', key: 'pushdown', minWidth: 140, render: (s) => (s.pushdown?.length ? s.pushdown.join(' / ') : '—') },
  { title: '汇入', key: 'sink', minWidth: 180, render: (s) => h(MonoField, { value: s.sink }) },
  { title: '消费组', key: 'group', minWidth: 150, render: (s) => h(MonoField, { value: s.group }) },
  {
    title: '起点',
    key: 'startOffset',
    width: 90,
    render: (s) => h(StateTag, { domain: 'startOffset', value: s.startOffset, label: s.startOffset === 'earliest' ? '从头' : '从最新' }),
  },
])

const nodeOptions = computed(() => [
  { label: '自动分配', value: 0 },
  ...onlineNodes.value.map((n) => ({ label: `${n.name}（${n.apiUrl}）`, value: n.id })),
])

function onNodePick(v: number): void {
  deployNodeId.value = v === 0 ? null : v
}

const selectedNodeValue = computed(() => deployNodeId.value ?? 0)

function goNodes(): void {
  void router.push('/nodes')
}
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
      <div class="page-head">
        <div class="head-left">
          <NButton size="small" quaternary @click="router.push('/pipelines')">
            <template #icon><ArrowBackOutline /></template>
            返回
          </NButton>
          <h1 class="title">发布向导 · {{ detail?.name ?? `#${pipelineId}` }}</h1>
          <StateTag v-if="detail" domain="pipeline" :value="detail.status" />
        </div>
      </div>

      <NSteps :current="step" size="small" class="steps">
        <NStep title="编译预检" />
        <NStep title="发布版本" />
        <NStep title="部署节点" />
      </NSteps>

      <NSpin v-if="loading" size="small" class="page-spin" />

      <!-- ① compile precheck -->
      <NCard v-else-if="step === 1" size="small" :content-style="{ padding: '16px' }">
        <NSpin v-if="compiling" size="small" class="page-spin">
          <template #description>正在编译 SQL…</template>
        </NSpin>
        <template v-else>
          <template v-if="compileOk === true">
            <NAlert type="success" :bordered="false">ok:true · 共 {{ statements.length }} 条语句</NAlert>
            <NDataTable :columns="stmtColumns" :data="statements" :row-key="(s: CompiledStatement) => s.index" size="small" class="stmt-table" />
            <div class="wizard-footer">
              <NButton size="small" @click="goEdit">返回编辑</NButton>
              <NButton size="small" type="primary" @click="step = 2">下一步</NButton>
            </div>
          </template>
          <template v-else-if="compileOk === false">
            <NAlert type="error" :bordered="false" title="编译未通过">
              <ul class="diag-list">
                <li v-for="d in diagnostics" :key="`${d.line}-${d.message}`">第 {{ d.line }} 行：{{ d.message }}</li>
              </ul>
            </NAlert>
            <div class="wizard-footer">
              <NButton size="small" type="primary" @click="goEdit">返回编辑</NButton>
              <NButton size="small" @click="runCompile">重试编译</NButton>
            </div>
          </template>
          <template v-else>
            <NAlert type="warning" :bordered="false">编译请求失败，可重试</NAlert>
            <div class="wizard-footer">
              <NButton size="small" @click="runCompile">重试</NButton>
            </div>
          </template>
        </template>
      </NCard>

      <!-- ② publish version -->
      <NCard v-else-if="step === 2" size="small" :content-style="{ padding: '16px' }">
        <template v-if="publishDiagnostics.length > 0">
          <NAlert type="error" :bordered="false" title="发布门禁失败">
            <ul class="diag-list">
              <li v-for="d in publishDiagnostics" :key="`${d.line}-${d.message}`">第 {{ d.line }} 行：{{ d.message }}</li>
            </ul>
          </NAlert>
          <div class="wizard-footer">
            <NButton size="small" @click="step = 1">上一步</NButton>
            <NButton size="small" type="primary" @click="goEdit">返回编辑</NButton>
          </div>
        </template>
        <template v-else-if="publishedVersion != null">
          <NAlert type="success" :bordered="false">已发布 v{{ publishedVersion }} —— spec 物化预览：</NAlert>
          <NCode :code="publishedSpec ?? ''" class="spec-code" />
          <div class="wizard-footer">
            <NButton size="small" @click="step = 1">上一步</NButton>
            <NButton size="small" type="primary" @click="step = 3">下一步（部署）</NButton>
          </div>
        </template>
        <template v-else>
          <template v-if="isAlreadyPublished">
            <NAlert type="info" :bordered="false">已是最新发布版本 v{{ detail?.version ?? 0 }}（无可发布变更）</NAlert>
            <div class="wizard-footer">
              <NButton size="small" @click="step = 1">上一步</NButton>
              <NButton size="small" type="primary" @click="step = 3">跳过 ▸</NButton>
            </div>
          </template>
          <template v-else>
            <p class="publish-line">将发布为版本 <NTag size="small">v{{ nextVersion }}</NTag></p>
            <p class="publish-hint">spec 物化预览在发布成功后展示——spec 中形如 <code>$&#123;ds:id.db.table&#125;</code> 的引用将替换为数据源绑定。</p>
            <div class="wizard-footer">
              <NButton size="small" @click="step = 1">上一步</NButton>
              <NButton size="small" type="primary" :loading="publishing" @click="doPublish">确认发布</NButton>
            </div>
          </template>
        </template>
      </NCard>

      <!-- ③ deploy node -->
      <NCard v-else size="small" :content-style="{ padding: '16px' }">
        <NSpin v-if="deploying" size="small" class="page-spin">
          <template #description>正在部署…</template>
        </NSpin>
        <template v-else>
          <!-- 501: SSH deploy unavailable → guide manual registration (task spec) -->
          <template v-if="deployNotImplemented">
            <NAlert type="warning" :bordered="false" title="自动部署不可用（501）">
              该 console 未启用 SSH 自动部署。请手动部署：
              在目标主机解压程序包并启动引擎（<code>nodectl.sh start</code>），然后到「引擎节点」页手动注册。
            </NAlert>
            <div class="wizard-footer">
              <NButton size="small" @click="goEdit">返回编辑</NButton>
              <NButton size="small" type="primary" @click="goNodes">前往手动注册</NButton>
            </div>
          </template>
          <template v-else>
            <NAlert v-if="deployErrorText" type="error" :bordered="false" title="部署失败（DEPLOY_FAILED）">
              {{ deployErrorText }}
              <div class="deploy-error-actions">
                <NButton size="tiny" type="primary" @click="doDeploy">重试部署</NButton>
                <NButton size="tiny" @click="router.push(`/pipelines/${pipelineId}/monitor`)">查看监控</NButton>
              </div>
            </NAlert>
            <template v-if="nodesError">
              <NAlert type="error" :bordered="false">节点列表加载失败：{{ nodesError }}</NAlert>
              <div class="wizard-footer">
                <NButton size="small" @click="loadNodes">重试</NButton>
              </div>
            </template>
            <EmptyState
              v-else-if="onlineNodes.length === 0"
              description="无可用节点，先到 /nodes 部署或等待节点上线（或手动注册已装好的引擎）"
              cta="前往引擎节点"
              @cta="goNodes"
            />
            <template v-else>
              <div class="deploy-pick">
                <NSelect :value="selectedNodeValue" :options="nodeOptions" size="small" @update:value="onNodePick" />
                <NTooltip trigger="hover" placement="top">
                  <template #trigger>
                    <NText depth="3" class="assign-hint">自动分配规则</NText>
                  </template>
                  自动分配 = 预指定节点（合格优先）→ 运行管道最少 → 节点 id 升序；部署事实唯一记录
                  assigned_node；节点 OFFLINE 后不自动迁移（v1 人工）。
                </NTooltip>
              </div>
              <div class="wizard-footer">
                <NButton size="small" @click="step = 2">上一步</NButton>
                <NButton size="small" type="primary" @click="doDeploy">部署 →</NButton>
              </div>
            </template>
          </template>
        </template>
      </NCard>
    </template>
  </main>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 16px;
  max-width: 960px;
}

.page-head {
  display: flex;
  align-items: center;
}

.head-left {
  display: flex;
  align-items: center;
  gap: 8px;
}

.title {
  font-size: var(--tr-fs-title-lg);
  font-weight: 600;
  margin: 0;
  color: var(--tr-text-1);
}

.steps {
  margin: 0 4px;
}

.page-spin {
  display: block;
  margin: 32px auto;
}

.stmt-table {
  margin-top: 12px;
}

.wizard-footer {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  margin-top: 16px;
}

.diag-list {
  margin: 4px 0 0;
  padding-left: 18px;
}

.publish-line {
  margin: 0 0 8px;
  display: flex;
  align-items: center;
  gap: 8px;
}

.publish-hint {
  color: var(--tr-text-3);
  font-size: var(--tr-fs-caption);
  margin: 0;
}

.spec-code {
  margin-top: 12px;
  max-height: 320px;
  overflow: auto;
  font-size: var(--tr-fs-caption);
}

.deploy-pick {
  display: flex;
  align-items: center;
  gap: 12px;
  max-width: 480px;
}

.deploy-error-actions {
  display: flex;
  gap: 8px;
  margin-top: 8px;
}
</style>
