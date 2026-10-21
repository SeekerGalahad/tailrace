<script setup lang="ts">
import { computed, h, ref, watch } from 'vue'
import {
  NButton,
  NCard,
  NDataTable,
  NInput,
  NModal,
  NResult,
  NSelect,
  NSpin,
  NTag,
  NTooltip,
  useMessage,
  type DataTableColumns,
} from 'naive-ui'
import { ApiError } from '../../../api/http'
import {
  datasourcesApi,
  type Datasource,
  type DiscoveredAsset,
  type InferSchemaResponse,
} from '../../../api/datasources'
import MonoField from '../../../components/MonoField.vue'
import StateTag from '../../../components/StateTag.vue'
import EmptyState from '../../../components/EmptyState.vue'

/**
 * Discover-assets modal (design doc §6.3 交互 4, width 720): probe on open →
 * search/filter → checkbox selection → register each row via infer-schema +
 * POST /tables with a per-row result tag (green 已登记 / red 失败 + reason).
 */

const props = defineProps<{
  show: boolean
  datasource: Datasource | null
}>()

const emit = defineEmits<{
  'update:show': [boolean]
  registered: [count: number]
}>()

const message = useMessage()

// --- probe state ---
const probing = ref(false)
const probeError = ref<string | null>(null)
const assets = ref<DiscoveredAsset[]>([])

async function runDiscover(): Promise<void> {
  const ds = props.datasource
  if (!ds) return
  probing.value = true
  probeError.value = null
  assets.value = []
  rowResults.clear()
  checkedKeys.value = []
  try {
    const res = await datasourcesApi.discover(ds.id, 20, { silent: true })
    assets.value = res.assets
  } catch (e) {
    probeError.value = e instanceof ApiError ? e.message : '探测失败，请稍后重试'
  } finally {
    probing.value = false
  }
}

watch(
  () => props.show,
  (open) => {
    if (open) void runDiscover()
  },
)

// --- search (300ms debounce) + logical-db filter, both client-side ---
const searchRaw = ref('')
const searchApplied = ref('')
let searchTimer: ReturnType<typeof setTimeout> | null = null
watch(searchRaw, (v) => {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(() => (searchApplied.value = v.trim()), 300)
})

const dbFilter = ref<string | null>(null)

const dbOptions = computed(() => {
  const dbs = [...new Set(assets.value.map((a) => a.dbName))]
  return dbs.map((d) => ({ label: d, value: d }))
})

const filteredAssets = computed(() => {
  const q = searchApplied.value.toLowerCase()
  return assets.value.filter((a) => {
    if (dbFilter.value && a.dbName !== dbFilter.value) return false
    if (q && !`${a.dbName}.${a.tableName}`.toLowerCase().includes(q)) return false
    return true
  })
})

// --- row selection ---
function assetKey(a: DiscoveredAsset): string {
  return `${a.dbName}.${a.tableName}`
}

/** Kafka dot-less topics carry an empty logical db — show the bare table. */
function assetLabel(a: DiscoveredAsset): string {
  return a.dbName ? `${a.dbName}.${a.tableName}` : a.tableName
}

/**
 * T14: infer-schema also returns the decode format kafka samples suggest
 * ("ogg" | "json" | null); the shared api client type hasn't been widened
 * yet (web/src/api is outside this card's file domain) — widen locally.
 */
type InferSchemaWithFormat = InferSchemaResponse & { format?: string | null }

const checkedKeys = ref<string[]>([])

// --- per-row registration results ---
interface RowResult {
  ok: boolean
  message?: string
}
const rowResults = new Map<string, RowResult>()
// Map is not reactive: keep a bump counter so the table re-renders on updates.
const resultsVersion = ref(0)

const registering = ref(false)

async function registerSelected(): Promise<void> {
  const ds = props.datasource
  if (!ds || registering.value || checkedKeys.value.length === 0) return
  registering.value = true
  let ok = 0
  try {
    for (const asset of assets.value) {
      const key = assetKey(asset)
      if (!checkedKeys.value.includes(key)) continue
      try {
        // columns come from the no-persist inference endpoint (§4.2); the
        // same call reports the decode format kafka samples suggest (e.g.
        // ogg vs json) — persist it with the table so decode matches
        const schema = (await datasourcesApi.inferSchema(
          ds.id,
          { dbName: asset.dbName, tableName: asset.tableName, format: asset.format },
          { silent: true },
        )) as InferSchemaWithFormat
        await datasourcesApi.registerTable(
          ds.id,
          {
            dbName: asset.dbName,
            tableName: asset.tableName,
            format: schema.format ?? asset.format,
            columnsJson: schema.columns,
          },
          { silent: true },
        )
        rowResults.set(key, { ok: true })
        ok += 1
      } catch (e) {
        rowResults.set(key, {
          ok: false,
          message: e instanceof ApiError ? e.message : '登记失败',
        })
      }
      resultsVersion.value += 1
    }
  } finally {
    registering.value = false
  }
  if (ok > 0) {
    message.success(`成功登记 ${ok} 项`)
    emit('registered', ok)
  }
  checkedKeys.value = checkedKeys.value.filter((k) => !rowResults.get(k)?.ok)
}

const columns = computed<DataTableColumns<DiscoveredAsset>>(() => [
  {
    type: 'selection',
    disabled: (row) => rowResults.get(assetKey(row))?.ok === true,
  },
  {
    title: '资产',
    key: 'asset',
    render: (row) => h(MonoField, { value: assetLabel(row), headChars: 0 }),
  },
  {
    title: '逻辑库',
    key: 'dbName',
    render: (row) => (row.dbName ? row.dbName : '—'),
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
    title: '分区数',
    key: 'partitions',
    width: 80,
    align: 'right',
    render: (row) => (row.partitions != null ? String(row.partitions) : '—'),
  },
  {
    title: '登记结果',
    key: 'result',
    width: 100,
    render: (row) => {
      void resultsVersion.value
      const r = rowResults.get(assetKey(row))
      if (!r) return null
      if (r.ok) return h(NTag, { type: 'success', size: 'small', bordered: false }, { default: () => '已登记' })
      return h(
        NTooltip,
        {},
        {
          trigger: () =>
            h(NTag, { type: 'error', size: 'small', bordered: false }, { default: () => '失败' }),
          default: () => r.message ?? '登记失败',
        },
      )
    },
  },
])

function close(): void {
  emit('update:show', false)
}
</script>

<template>
  <NModal
    :show="show"
    preset="card"
    title="发现资产"
    :style="{ width: '720px' }"
    :mask-closable="false"
    @update:show="(v) => emit('update:show', v)"
  >
    <div class="discover-body">
      <NSpin v-if="probing" class="discover-spin">
        <div class="spin-placeholder">正在探测数据源资产…</div>
      </NSpin>

      <NResult
        v-else-if="probeError"
        status="error"
        title="资产探测失败"
        :description="probeError"
      >
        <template #footer>
          <NButton size="small" @click="runDiscover">重试探测</NButton>
        </template>
      </NResult>

      <template v-else>
        <div class="discover-toolbar">
          <NInput
            v-model:value="searchRaw"
            placeholder="搜索 库.表"
            clearable
            class="search-input"
          />
          <NSelect
            v-model:value="dbFilter"
            :options="dbOptions"
            placeholder="全部逻辑库"
            clearable
            class="db-select"
          />
          <NButton :loading="registering" :disabled="checkedKeys.length === 0" @click="registerSelected">
            登记所选 ({{ checkedKeys.length }})
          </NButton>
        </div>

        <EmptyState
          v-if="assets.length === 0"
          description="未发现资产（检查连接参数后重试）"
          cta="重试探测"
          @cta="runDiscover"
        />
        <EmptyState
          v-else-if="filteredAssets.length === 0"
          description="没有匹配筛选条件的资产"
        />
        <NCard v-else size="small" :bordered="true" class="discover-card">
          <NDataTable
            size="small"
            :columns="columns"
            :data="filteredAssets"
            :row-key="assetKey"
            v-model:checked-row-keys="checkedKeys"
            :bordered="false"
            :max-height="360"
          />
        </NCard>
      </template>
    </div>

    <template #footer>
      <div class="modal-footer">
        <span class="footer-hint">登记所选将逐行推断 Schema 并写入表对象</span>
        <NButton :disabled="registering" @click="close">关闭</NButton>
      </div>
    </template>
  </NModal>
</template>

<style scoped>
.discover-body {
  min-height: 160px;
}

.discover-spin {
  width: 100%;
}

.spin-placeholder {
  height: 120px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--tr-text-3);
}

.discover-toolbar {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}

.search-input {
  width: 220px;
}

.db-select {
  width: 180px;
}

.discover-card {
  border-radius: var(--tr-radius-card);
}

.modal-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.footer-hint {
  color: var(--tr-text-3);
  font-size: var(--tr-fs-caption);
}
</style>
