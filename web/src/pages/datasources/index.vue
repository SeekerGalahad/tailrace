<script setup lang="ts">
import { computed, h, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import {
  NAlert,
  NBadge,
  NButton,
  NCard,
  NDataTable,
  NGrid,
  NGi,
  NIcon,
  NInput,
  NPopconfirm,
  NResult,
  NSkeleton,
  NSelect,
  NTag,
  NText,
  useMessage,
  type DataTableColumns,
} from 'naive-ui'
import {
  AddOutline,
  RefreshOutline,
  SearchOutline,
  TrashOutline,
} from '@vicons/ionicons5'
import { ApiError } from '../../api/http'
import {
  datasourcesApi,
  type Datasource,
  type DatasourceType,
  type TableObject,
} from '../../api/datasources'
import MonoField from '../../components/MonoField.vue'
import StateTag from '../../components/StateTag.vue'
import DatasourceEditDialog from './parts/DatasourceEditDialog.vue'
import DiscoverModal from './parts/DiscoverModal.vue'
import RefsBlockModal from './parts/RefsBlockModal.vue'
import EmptyState from '../../components/EmptyState.vue'

/**
 * Datasource management main page (design doc §6.3). Full-list fetch (no
 * pagination — §4.2 has none), client-side search/type filter, NO polling:
 * status only changes through explicit test actions, each action invalidates
 * and refetches (§6.3 交互 1).
 */

const router = useRouter()
const message = useMessage()

function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : '请求失败，请稍后重试'
}

function parseRefs(data: unknown): { pipelines?: number[] } {
  const d = data as { refs?: { pipelines?: unknown } } | null
  const pipes = d?.refs?.pipelines
  return {
    pipelines: Array.isArray(pipes) ? pipes.filter((n): n is number => typeof n === 'number') : [],
  }
}

// --- queries: types / datasources / table objects ---
interface TypesQ {
  phase: 'loading' | 'ok' | 'error'
  types: DatasourceType[]
}
const typesQ = reactive<TypesQ>({ phase: 'loading', types: [] })

interface ListQ {
  phase: 'loading' | 'ok' | 'error'
  items: Datasource[]
  error: string
}
const listQ = reactive<ListQ>({ phase: 'loading', items: [], error: '' })

interface TablesQ {
  phase: 'loading' | 'ok' | 'error'
  items: TableObject[]
  error: string
}
const tablesQ = reactive<TablesQ>({ phase: 'loading', items: [], error: '' })

async function fetchTypes(): Promise<void> {
  try {
    const res = await datasourcesApi.listTypes({ silent: true })
    typesQ.types = res.types
    typesQ.phase = 'ok'
  } catch {
    typesQ.phase = 'error'
  }
}

async function fetchList(): Promise<void> {
  if (!listQ.items.length) listQ.phase = 'loading'
  try {
    const res = await datasourcesApi.list(undefined, { silent: true })
    listQ.items = res.items
    listQ.phase = 'ok'
    lastCardCount.value = Math.max(res.items.length, 1)
  } catch (e) {
    if (!listQ.items.length) {
      listQ.phase = 'error'
      listQ.error = errorMessage(e)
    }
  }
}

async function fetchTables(): Promise<void> {
  if (!tablesQ.items.length) tablesQ.phase = 'loading'
  try {
    const res = await datasourcesApi.listTables(undefined, { silent: true })
    tablesQ.items = res.items
    tablesQ.phase = 'ok'
  } catch (e) {
    if (!tablesQ.items.length) {
      tablesQ.phase = 'error'
      tablesQ.error = errorMessage(e)
    }
  }
}

function fetchAll(): void {
  void fetchTypes()
  void fetchList()
  void fetchTables()
}
fetchAll()

function refresh(): void {
  void fetchList()
  void fetchTables()
}

// --- ① toolbar: search (300ms debounce) + type filter (§6.3 工具条) ---
const searchRaw = ref('')
const searchApplied = ref('')
let searchTimer: ReturnType<typeof setTimeout> | null = null
watch(searchRaw, (v) => {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(() => (searchApplied.value = v.trim()), 300)
})
onBeforeUnmount(() => {
  if (searchTimer) clearTimeout(searchTimer)
})

const typeFilter = ref<string>('all')

const typeOptions = computed(() => [
  { label: '全部类型', value: 'all' },
  ...typesQ.types.map((t) => ({ label: t.label, value: String(t.type) })),
])

const filteredItems = computed(() => {
  const q = searchApplied.value.toLowerCase()
  return listQ.items.filter((ds) => {
    if (typeFilter.value !== 'all' && ds.type !== typeFilter.value) return false
    if (q && !ds.name.toLowerCase().includes(q)) return false
    return true
  })
})

// --- ② asset cards ---
// Skeleton count follows the last known card count (3 on first visit, §6.3).
const lastCardCount = ref(3)

const testingId = ref<number | null>(null)

async function testDatasource(ds: Datasource): Promise<void> {
  if (testingId.value !== null) return
  testingId.value = ds.id
  try {
    const res = await datasourcesApi.testById(ds.id) // silent: inline write-back
    const target = listQ.items.find((i) => i.id === ds.id)
    if (target) target.status = res.status === 'CONN_OK' ? 'CONN_OK' : 'CONN_FAIL'
    if (res.status === 'CONN_OK') message.success(`连通正常 · ${res.latencyMs}ms`)
    else message.error(`连通失败：${res.message}`)
  } catch (e) {
    message.error(errorMessage(e))
  } finally {
    testingId.value = null
  }
}

// --- register / edit modal ---
const formShow = ref(false)
const editingSource = ref<Datasource | null>(null)

function openCreate(): void {
  editingSource.value = null
  formShow.value = true
}

function openEdit(ds: Datasource): void {
  editingSource.value = ds
  formShow.value = true
}

// --- delete with 409 refs guard ---
const deletingId = ref<number | null>(null)
const refsShow = ref(false)
const refsData = ref<{ pipelines?: number[] } | null>(null)
const refsLabel = ref('')

async function removeDatasource(ds: Datasource): Promise<void> {
  if (deletingId.value !== null) return
  deletingId.value = ds.id
  try {
    await datasourcesApi.remove(ds.id, { silent: true })
    message.success(`已删除数据源 ${ds.name}`)
    refresh()
  } catch (e) {
    if (e instanceof ApiError && e.status === 409) {
      refsData.value = parseRefs(e.data)
      refsLabel.value = `数据源 ${ds.name}`
      refsShow.value = true
    } else {
      message.error(errorMessage(e))
    }
  } finally {
    deletingId.value = null
  }
}

// --- ③ table objects ---
const tableFilter = ref<number | 'all'>('all')

const tableFilterOptions = computed(() => [
  { label: '全部', value: 'all' },
  ...listQ.items.map((ds) => ({ label: ds.name, value: ds.id })),
])

const filteredTables = computed(() =>
  tableFilter.value === 'all'
    ? tablesQ.items
    : tablesQ.items.filter((t) => t.datasourceId === tableFilter.value),
)

const discoverTarget = computed<Datasource | null>(() => {
  if (tableFilter.value === 'all') return null
  return listQ.items.find((ds) => ds.id === tableFilter.value) ?? null
})

const discoverShow = ref(false)

function openDiscover(): void {
  if (!discoverTarget.value) {
    message.warning('请先在筛选中选择一个数据源')
    return
  }
  discoverShow.value = true
}

function onRegistered(): void {
  void fetchTables()
}

const deletingTableId = ref<number | null>(null)

async function removeTable(row: TableObject): Promise<void> {
  if (deletingTableId.value !== null) return
  deletingTableId.value = row.id
  try {
    await datasourcesApi.removeTable(row.id, { silent: true })
    message.success(`已删除表对象 ${row.dbName}.${row.tableName}`)
    void fetchTables()
  } catch (e) {
    if (e instanceof ApiError && e.status === 409) {
      refsData.value = parseRefs(e.data)
      refsLabel.value = `表对象 ${row.dbName}.${row.tableName}`
      refsShow.value = true
    } else {
      message.error(errorMessage(e))
    }
  } finally {
    deletingTableId.value = null
  }
}

const tableColumns: DataTableColumns<TableObject> = [
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
    render: (row) => h(NTag, { size: 'small', bordered: false }, { default: () => row.cursorSemantics }),
  },
  { title: '来源', key: 'source', width: 110 },
  {
    title: '操作',
    key: 'actions',
    width: 80,
    render: (row) =>
      h(
        NPopconfirm,
        { onPositiveClick: () => void removeTable(row) },
        {
          trigger: () =>
            h(
              NButton,
              {
                size: 'tiny',
                quaternary: true,
                type: 'error',
                loading: deletingTableId.value === row.id,
              },
              { default: () => '删除' },
            ),
          default: () => '确认删除该表对象？被已发布管道引用时将无法删除。',
        },
      ),
  },
]
</script>

<template>
  <main class="page">
    <!-- ① toolbar -->
    <div class="toolbar">
      <div class="toolbar-left">
        <NInput v-model:value="searchRaw" placeholder="搜索名称" clearable class="search-input">
          <template #prefix>
            <NIcon :size="14"><SearchOutline /></NIcon>
          </template>
        </NInput>
        <NSelect
          v-model:value="typeFilter"
          :options="typeOptions"
          :disabled="typesQ.phase === 'error'"
          class="type-select"
        />
        <NAlert v-if="typesQ.phase === 'error'" type="error" :bordered="false" class="types-error">
          类型注册表加载失败
          <NButton text size="tiny" @click="fetchTypes">重试</NButton>
        </NAlert>
        <NButton
          :loading="listQ.phase === 'loading' || tablesQ.phase === 'loading'"
          @click="refresh"
        >
          <template #icon>
            <NIcon><RefreshOutline /></NIcon>
          </template>
          刷新
        </NButton>
      </div>
      <NButton type="primary" @click="openCreate">
        <template #icon>
          <NIcon><AddOutline /></NIcon>
        </template>
        注册数据源
      </NButton>
    </div>

    <!-- ② asset cards -->
    <template v-if="listQ.phase === 'loading'">
      <NGrid cols="1 s:2 m:3 l:4" :x-gap="16" :y-gap="16" responsive="screen">
        <NGi v-for="i in lastCardCount" :key="`ds-skel-${i}`">
          <NSkeleton class="card-skeleton" />
        </NGi>
      </NGrid>
    </template>
    <NCard v-else-if="listQ.phase === 'error'" size="small" class="block-card">
      <NResult status="error" title="数据源列表加载失败" :description="listQ.error">
        <template #footer>
          <NButton size="small" @click="fetchList">重试</NButton>
        </template>
      </NResult>
    </NCard>
    <NCard v-else-if="listQ.items.length === 0" size="small" class="block-card">
      <EmptyState description="尚无数据源" cta="注册第一个数据源" @cta="openCreate" />
    </NCard>
    <NCard v-else-if="filteredItems.length === 0" size="small" class="block-card">
      <EmptyState description="没有匹配条件的数据源" />
    </NCard>
    <NGrid v-else cols="1 s:2 m:3 l:4" :x-gap="16" :y-gap="16" responsive="screen">
      <NGi v-for="ds in filteredItems" :key="ds.id">
        <NCard size="small" class="ds-card" @click="router.push(`/datasources/${ds.id}`)">
          <template #header>
            <span class="ds-name">{{ ds.name }}</span>
          </template>
          <template #header-extra>
            <StateTag domain="datasource" :value="ds.status" />
          </template>
          <div class="ds-body">
            <div class="ds-meta">
              <StateTag domain="kind" :value="ds.type" />
              <NBadge v-if="ds.hasPassword" dot type="success">
                <span class="cred-text">含凭据</span>
              </NBadge>
            </div>
            <NText depth="3" class="ds-sub">gen#{{ ds.generation }} · #{{ ds.id }}</NText>
          </div>
          <template #action>
            <div class="ds-actions" @click.stop>
              <NButton
                size="tiny"
                :loading="testingId === ds.id"
                @click="testDatasource(ds)"
              >
                测试
              </NButton>
              <NButton size="tiny" @click="openEdit(ds)">编辑</NButton>
              <NPopconfirm @positive-click="removeDatasource(ds)">
                <template #trigger>
                  <NButton size="tiny" quaternary type="error" :loading="deletingId === ds.id">
                    <template #icon>
                      <NIcon><TrashOutline /></NIcon>
                    </template>
                    删除
                  </NButton>
                </template>
                将删除资产及级联的凭据与表对象
              </NPopconfirm>
            </div>
          </template>
        </NCard>
      </NGi>
    </NGrid>

    <!-- ③ table objects -->
    <NCard size="small" class="block-card">
      <template #header>
        <span>表对象</span>
      </template>
      <template #header-extra>
        <div class="tables-toolbar">
          <NSelect
            v-model:value="tableFilter"
            :options="tableFilterOptions"
            size="small"
            class="table-select"
          />
          <NButton size="small" :disabled="!discoverTarget" @click="openDiscover">
            发现资产
          </NButton>
          <NButton size="small" quaternary @click="fetchTables">刷新</NButton>
        </div>
      </template>
      <NResult
        v-if="tablesQ.phase === 'error'"
        size="small"
        status="error"
        title="表对象加载失败"
      >
        <template #footer>
          <NButton size="small" @click="fetchTables">重试</NButton>
        </template>
      </NResult>
      <EmptyState
        v-else-if="filteredTables.length === 0"
        description="尚未登记表对象"
        cta="发现资产"
        @cta="openDiscover"
      />
      <NDataTable
        v-else
        size="small"
        :columns="tableColumns"
        :data="filteredTables"
        :loading="tablesQ.phase === 'loading'"
        :bordered="false"
        :max-height="360"
      />
    </NCard>

    <!-- ④ audit trail — 增补建议 B endpoint not landed (§6.3 API 映射): placeholder -->
    <NCard size="small" title="审计流水（最近 30 条）" class="block-card">
      <EmptyState description="审计查询接口待启用" />
    </NCard>

    <!-- modals -->
    <DatasourceEditDialog
      v-model:show="formShow"
      :types="typesQ.types"
      :types-error="typesQ.phase === 'error'"
      :source="editingSource"
      @saved="refresh"
      @retry-types="fetchTypes"
    />
    <DiscoverModal
      v-model:show="discoverShow"
      :datasource="discoverTarget"
      @registered="onRegistered"
    />
    <RefsBlockModal v-model:show="refsShow" :refs="refsData" :entity-label="refsLabel" />
  </main>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 16px; /* §6.6.3 block gap */
  color: var(--tr-text-1);
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.toolbar-left {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.search-input {
  width: 200px;
}

.type-select {
  width: 170px;
}

.types-error {
  padding: 2px 10px;
}

.card-skeleton {
  height: 150px;
  border-radius: var(--tr-radius-card);
}

.block-card {
  border-radius: var(--tr-radius-card);
}

.ds-card {
  cursor: pointer;
  transition: border-color 0.15s ease;
}

.ds-card:hover {
  border-color: var(--tr-chart-1);
}

.ds-name {
  font-size: var(--tr-fs-title);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.ds-body {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.ds-meta {
  display: flex;
  align-items: center;
  gap: 12px;
}

.cred-text {
  font-size: var(--tr-fs-caption);
  color: var(--tr-text-2);
}

.ds-sub {
  font-size: var(--tr-fs-caption);
  font-variant-numeric: tabular-nums;
}

.ds-actions {
  display: flex;
  gap: 6px;
}

.tables-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
}

.table-select {
  width: 200px;
}
</style>
