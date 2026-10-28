<script setup lang="ts">
import { computed, h, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  NButton,
  NCard,
  NDataTable,
  NDescriptions,
  NDescriptionsItem,
  NIcon,
  NResult,
  NSkeleton,
  NSpin,
  NTag,
  NText,
  NTooltip,
  useMessage,
  type DataTableColumns,
} from 'naive-ui'
import { ArrowBackOutline } from '@vicons/ionicons5'
import { ApiError } from '../../api/http'
import {
  datasourcesApi,
  type DatasourceDetail,
  type DatasourceType,
  type TableObject,
} from '../../api/datasources'
import MonoField from '../../components/MonoField.vue'
import StateTag from '../../components/StateTag.vue'
import DatasourceEditDialog from './parts/DatasourceEditDialog.vue'
import TableDdlWizard from './parts/TableDdlWizard.vue'
import EmptyState from '../../components/EmptyState.vue'

/**
 * Datasource detail page `/datasources/:id` (design doc §6.3 详情页): meta
 * descriptions + catalog browser (basic version: db/table/column list via the
 * existing §4.2 endpoints — listTables + infer-schema; sample preview stays a
 * TODO slot until a P2 endpoint exists) + audit placeholder. Non-numeric or
 * unknown id → NResult 404 + back button. Doris/StarRocks datasources get a
 * per-table 「建表向导」 action (T-A1 create-table closed loop).
 */

const route = useRoute()
const router = useRouter()
const message = useMessage()

const idParam = Array.isArray(route.params.id) ? route.params.id[0] : route.params.id
const dsId = Number(idParam)
const invalidId = !Number.isInteger(dsId) || dsId <= 0

function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : '请求失败，请稍后重试'
}

function fmtDateTime(ts: string): string {
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return ts
  const p = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

// --- datasource query ---
interface DsQ {
  phase: 'loading' | 'ok' | 'error' | 'notfound'
  data: DatasourceDetail | null
  error: string
}
const dsQ = reactive<DsQ>({ phase: 'loading', data: null, error: '' })

async function fetchDs(): Promise<void> {
  if (invalidId) return
  if (!dsQ.data) dsQ.phase = 'loading'
  try {
    dsQ.data = await datasourcesApi.get(dsId, { silent: true })
    dsQ.phase = 'ok'
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) {
      dsQ.phase = 'notfound'
    } else if (!dsQ.data) {
      dsQ.phase = 'error'
      dsQ.error = errorMessage(e)
    }
  }
}

// --- table objects of this datasource ---
interface TablesQ {
  phase: 'loading' | 'ok' | 'error'
  items: TableObject[]
  error: string
}
const tablesQ = reactive<TablesQ>({ phase: 'loading', items: [], error: '' })

async function fetchTables(): Promise<void> {
  if (invalidId) return
  if (!tablesQ.items.length) tablesQ.phase = 'loading'
  try {
    const res = await datasourcesApi.listTables({ datasourceId: dsId }, { silent: true })
    tablesQ.items = res.items
    tablesQ.phase = 'ok'
  } catch (e) {
    if (!tablesQ.items.length) {
      tablesQ.phase = 'error'
      tablesQ.error = errorMessage(e)
    }
  }
}

// --- datasource types (needed by the shared edit modal) ---
interface TypesQ {
  phase: 'loading' | 'ok' | 'error'
  types: DatasourceType[]
}
const typesQ = reactive<TypesQ>({ phase: 'loading', types: [] })

async function fetchTypes(): Promise<void> {
  try {
    const res = await datasourcesApi.listTypes({ silent: true })
    typesQ.types = res.types
    typesQ.phase = 'ok'
  } catch {
    typesQ.phase = 'error'
  }
}

function fetchAll(): void {
  void fetchDs()
  void fetchTables()
  void fetchTypes()
}
if (!invalidId) fetchAll()

// --- actions: test / edit (same modal as the main page, §6.3 交互 7) ---
const testing = ref(false)

async function testDatasource(): Promise<void> {
  if (testing.value || !dsQ.data) return
  testing.value = true
  try {
    const res = await datasourcesApi.testById(dsId)
    if (dsQ.data) dsQ.data.status = res.status === 'CONN_OK' ? 'CONN_OK' : 'CONN_FAIL'
    if (res.status === 'CONN_OK') message.success(`连通正常 · ${res.latencyMs}ms`)
    else message.error(`连通失败：${res.message}`)
  } catch (e) {
    message.error(errorMessage(e))
  } finally {
    testing.value = false
  }
}

const formShow = ref(false)

function onSaved(): void {
  void fetchDs()
  void fetchTables()
}

// --- catalog browser (basic): per-table column list via infer-schema ---
interface SchemaState {
  phase: 'loading' | 'ok' | 'error'
  columns: [string, string][]
  error: string
}
const schemaCache = reactive<Record<number, SchemaState>>({})
const expandedKeys = ref<number[]>([])

async function loadSchema(row: TableObject): Promise<void> {
  schemaCache[row.id] = { phase: 'loading', columns: [], error: '' }
  try {
    const res = await datasourcesApi.inferSchema(
      dsId,
      { dbName: row.dbName, tableName: row.tableName, format: row.format },
      { silent: true },
    )
    schemaCache[row.id] = { phase: 'ok', columns: res.columns, error: '' }
  } catch (e) {
    schemaCache[row.id] = {
      phase: 'error',
      columns: [],
      error: e instanceof ApiError ? e.message : 'Schema 推断失败',
    }
  }
}

function onExpandKeys(keys: Array<string | number>): void {
  expandedKeys.value = keys.map((k) => Number(k))
  for (const key of expandedKeys.value) {
    if (!schemaCache[key]) {
      const row = tablesQ.items.find((t) => t.id === key)
      if (row) void loadSchema(row)
    }
  }
}

function renderExpand(row: TableObject) {
  const st = schemaCache[row.id]
  if (!st || st.phase === 'loading') {
    return h(NSpin, { size: 'small' })
  }
  if (st.phase === 'error') {
    return h('div', { style: 'display:flex;align-items:center;gap:12px;' }, [
      h(NText, { type: 'error', depth: 2 }, { default: () => `列清单加载失败：${st.error}` }),
      h(
        NButton,
        { size: 'tiny', onClick: () => void loadSchema(row) },
        { default: () => '重试' },
      ),
    ])
  }
  if (st.columns.length === 0) {
    return h(NText, { depth: 3 }, { default: () => '该表未推断到列信息' })
  }
  return h('div', { style: 'display:flex;flex-direction:column;gap:10px;' }, [
    h(
      'div',
      { style: 'display:flex;flex-wrap:wrap;gap:6px;' },
      st.columns.map(([name, type]) =>
        h(
          NTag,
          { size: 'small', bordered: false },
          {
            default: () =>
              h(
                'span',
                { style: `font-family: var(--tr-font-mono); font-size: var(--tr-fs-caption);` },
                `${name}: ${type}`,
              ),
          },
        ),
      ),
    ),
    // Sample preview: TODO slot — no endpoint in v1 (P2 with catalog, §4.2 裁剪).
    h('div', { style: 'display:flex;align-items:center;gap:8px;' }, [
      h(
        NButton,
        { size: 'tiny', disabled: true },
        { default: () => '样本预览' },
      ),
      h(
        NText,
        { depth: 3, style: 'font-size: var(--tr-fs-caption);' },
        { default: () => 'TODO：样本预览端点 P2 随 catalog 能力回归后提供' },
      ),
    ]),
  ])
}

const catalogBaseColumns: DataTableColumns<TableObject> = [
  { type: 'expand', renderExpand },
  { title: '逻辑库', key: 'dbName', width: 140, ellipsis: { tooltip: true } },
  {
    title: '表对象',
    key: 'tableName',
    render: (row) => h(MonoField, { value: row.tableName, headChars: 0 }),
  },
  {
    title: '类别',
    key: 'kind',
    width: 100,
    render: (row) => h(StateTag, { domain: 'kind', value: row.kind }),
  },
  {
    title: '格式',
    key: 'format',
    width: 90,
    render: (row) => row.format || '—',
  },
  {
    title: '游标语义',
    key: 'cursorSemantics',
    width: 150,
    render: (row) =>
      h(NTag, { size: 'small', bordered: false }, { default: () => row.cursorSemantics }),
  },
  { title: '来源', key: 'source', width: 110 },
]

// --- T-A1 create-table wizard: doris/starrocks datasources only (the draft
// endpoint generates Doris-dialect DDL and 400s on anything else) ---
const isDorisLike = computed(() => {
  const t = dsQ.data?.type
  return t === 'doris' || t === 'starrocks'
})

const wizardShow = ref(false)
const wizardTable = ref<TableObject | null>(null)

function openWizard(row: TableObject): void {
  wizardTable.value = row
  wizardShow.value = true
}

/** The draft needs a registered column list — disable (with the reason)
 * rather than let the wizard open onto a 400. */
function renderWizardAction(row: TableObject) {
  const noColumns = !row.columnsJson || row.columnsJson.length === 0
  const btn = h(
    NButton,
    { size: 'tiny', disabled: noColumns, onClick: () => openWizard(row) },
    { default: () => '建表向导' },
  )
  if (!noColumns) return btn
  return h(
    NTooltip,
    {},
    {
      trigger: () => btn,
      default: () => '该表对象未登记列清单（columnsJson 为空），无法生成 DDL 草稿',
    },
  )
}

const catalogColumns = computed<DataTableColumns<TableObject>>(() =>
  isDorisLike.value
    ? [...catalogBaseColumns, { title: '操作', key: 'actions', width: 100, render: renderWizardAction }]
    : catalogBaseColumns,
)

const tableCount = computed(
  () => dsQ.data?.tableCount ?? tablesQ.items.length,
)

function rowKey(row: TableObject): number {
  return row.id
}

function back(): void {
  router.push('/datasources')
}
</script>

<template>
  <main class="page">
    <!-- invalid id → 404 (§6.3 路由) -->
    <NResult
      v-if="invalidId"
      status="404"
      title="数据源不存在"
      :description="`路径参数「${idParam}」不是合法的数据源 id`"
    >
      <template #footer>
        <NButton size="small" @click="back">返回数据源列表</NButton>
      </template>
    </NResult>

    <template v-else>
      <div class="topbar">
        <NButton quaternary size="small" @click="back">
          <template #icon>
            <NIcon><ArrowBackOutline /></NIcon>
          </template>
          返回
        </NButton>
      </div>

      <NSkeleton v-if="dsQ.phase === 'loading'" class="head-skeleton" />
      <NResult
        v-else-if="dsQ.phase === 'notfound'"
        status="404"
        title="数据源不存在"
        description="该数据源可能已被删除"
      >
        <template #footer>
          <NButton size="small" @click="back">返回数据源列表</NButton>
        </template>
      </NResult>
      <NCard v-else-if="dsQ.phase === 'error'" size="small" class="block-card">
        <NResult status="error" title="数据源详情加载失败" :description="dsQ.error">
          <template #footer>
            <NButton size="small" @click="fetchDs">重试</NButton>
          </template>
        </NResult>
      </NCard>

      <template v-else-if="dsQ.data">
        <!-- header card: name + status + actions (§6.3 详情页) -->
        <NCard size="small" class="block-card">
          <template #header>
            <span class="ds-name">{{ dsQ.data.name }}</span>
          </template>
          <template #header-extra>
            <div class="header-actions">
              <StateTag domain="datasource" :value="dsQ.data.status" />
              <NButton size="small" :loading="testing" @click="testDatasource">测试连接</NButton>
              <NButton size="small" type="primary" @click="formShow = true">编辑</NButton>
            </div>
          </template>
          <NDescriptions label-placement="left" :column="3" size="small" bordered>
            <NDescriptionsItem label="类型">
              <StateTag domain="kind" :value="dsQ.data.type" />
            </NDescriptionsItem>
            <NDescriptionsItem label="状态">
              <StateTag domain="datasource" :value="dsQ.data.status" />
            </NDescriptionsItem>
            <NDescriptionsItem label="凭据">
              {{ dsQ.data.hasPassword ? '含凭据（服务端加密存储，不回显）' : '无凭据' }}
            </NDescriptionsItem>
            <NDescriptionsItem label="generation">
              <span class="mono-num">gen#{{ dsQ.data.generation }}</span>
            </NDescriptionsItem>
            <NDescriptionsItem label="表对象">{{ tableCount }}</NDescriptionsItem>
            <NDescriptionsItem label="更新时间">
              {{ fmtDateTime(dsQ.data.updatedAt) }}
            </NDescriptionsItem>
          </NDescriptions>
        </NCard>

        <!-- catalog browser (basic): db/table/column list, sample preview TODO -->
        <NCard size="small" title="库表目录" class="block-card">
          <template #header-extra>
            <NButton size="small" quaternary :loading="tablesQ.phase === 'loading'" @click="fetchTables">
              刷新
            </NButton>
          </template>
          <NResult
            v-if="tablesQ.phase === 'error'"
            size="small"
            status="error"
            title="表对象加载失败"
            :description="tablesQ.error"
          >
            <template #footer>
              <NButton size="small" @click="fetchTables">重试</NButton>
            </template>
          </NResult>
          <EmptyState
            v-else-if="tablesQ.items.length === 0"
            description="尚未登记表对象"
            cta="返回列表页发现资产"
            @cta="back"
          />
          <NDataTable
            v-else
            size="small"
            :columns="catalogColumns"
            :data="tablesQ.items"
            :loading="tablesQ.phase === 'loading'"
            :row-key="rowKey"
            :expanded-keys="expandedKeys"
            :bordered="false"
            @update:expanded-keys="onExpandKeys"
          />
        </NCard>

        <!-- audit — 增补建议 B endpoint not landed (§6.3 API 映射): placeholder -->
        <NCard size="small" title="审计流水" class="block-card">
          <EmptyState description="审计查询接口待启用" />
        </NCard>
      </template>
    </template>

    <DatasourceEditDialog
      v-model:show="formShow"
      :types="typesQ.types"
      :types-error="typesQ.phase === 'error'"
      :source="dsQ.data"
      @saved="onSaved"
      @retry-types="fetchTypes"
    />

    <!-- T-A1 create-table wizard (doris/starrocks rows only) -->
    <TableDdlWizard v-model:show="wizardShow" :datasource="dsQ.data" :table="wizardTable" />
  </main>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 16px;
  color: var(--tr-text-1);
}

.topbar {
  display: flex;
}

.head-skeleton {
  height: 220px;
  border-radius: var(--tr-radius-card);
}

.block-card {
  border-radius: var(--tr-radius-card);
}

.ds-name {
  font-size: var(--tr-fs-title-lg);
  font-weight: 600;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.mono-num {
  font-variant-numeric: tabular-nums;
}
</style>
