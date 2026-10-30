<script setup lang="ts">
import { computed, h, ref, watch } from 'vue'
import {
  NAlert,
  NButton,
  NCheckbox,
  NCode,
  NDataTable,
  NDescriptions,
  NDescriptionsItem,
  NFormItem,
  NInputNumber,
  NModal,
  NPopconfirm,
  NRadio,
  NResult,
  NSelect,
  NSpin,
  NStep,
  NSteps,
  NTag,
  NText,
  useMessage,
  type DataTableColumns,
  type SelectOption,
} from 'naive-ui'
import { ApiError } from '../../../api/http'
import {
  datasourcesApi,
  type Datasource,
  type DdlDraftResponse,
  type DdlEvolveConflictRefs,
  type DdlEvolveResponse,
  type DdlExecuteResponse,
  type DdlKeyModel,
  type TableObject,
} from '../../../api/datasources'
import MonoField from '../../../components/MonoField.vue'

/**
 * Create-table wizard (T-A1 ④, blueprint §7.2): three steps over the
 * draft/execute/evolve endpoints — ① column confirm with per-column type
 * overrides (A3: the inferred type is a dropdown over the generator's
 * vocabulary — inference saying utf8 while the human knows the column is
 * money gets fixed here, steering this draft only, never the registration)
 * → ② key model, MANDATORY, with the heuristic key column carrying its
 * 「猜测」 badge (next stays disabled until a model is picked) and the
 * partition candidate, when the generator offered one, as a keep/drop
 * checkbox → ③ read-only DDL preview + execute / evolve entries. No DDL
 * hand-editor on purpose (task-card 风险登记): confirm differently and
 * regenerate instead of editing text. Doris/StarRocks datasources only —
 * the caller hides the entry otherwise.
 */

const props = defineProps<{
  show: boolean
  datasource: Datasource | null
  table: TableObject | null
}>()

const emit = defineEmits<{
  'update:show': [boolean]
}>()

const message = useMessage()

// --- steps: 1 columns → 2 key model → 3 preview/execute ---
const step = ref(1)

// --- draft (step ① / regenerated in ②→③ transition) ---
const drafting = ref(false)
const draftError = ref<string | null>(null)
const draft = ref<DdlDraftResponse | null>(null)

// --- confirmation (step ②) ---
/** Unpicked on purpose — §7.2 「不选键模型不给执行」, mirrored by the disabled next. */
const keyModel = ref<DdlKeyModel | null>(null)
const keyColumn = ref<string | null>(null)
const replicationNum = ref<number | null>(null)
/** Partition decision: null = no candidate seen yet (hint omitted); true =
 * keep (confirms, clears the guess badge); false = drop (no PARTITION clause).
 * Seeded to true the first time a draft offers a candidate. */
const keepPartition = ref<boolean | null>(null)

// --- execute / evolve outcomes (step ③) ---
const executing = ref(false)
const execResult = ref<DdlExecuteResponse | null>(null)
const execError = ref<string | null>(null)

type EvolveState =
  | { kind: 'idle' }
  | { kind: 'ok'; res: DdlEvolveResponse }
  | { kind: 'conflict'; msg: string; refs: DdlEvolveConflictRefs | null }
  | { kind: 'error'; msg: string }
const evolveState = ref<EvolveState>({ kind: 'idle' })
const evolving = ref(false)

function reset(): void {
  step.value = 1
  draft.value = null
  draftError.value = null
  keyModel.value = null
  keyColumn.value = null
  replicationNum.value = null
  keepPartition.value = null
  columnTypeOverrides.value = {}
  execResult.value = null
  execError.value = null
  evolveState.value = { kind: 'idle' }
}

watch(
  () => props.show,
  (open) => {
    if (open) {
      reset()
      void runDraft(false)
    }
  },
)

/** First round: empty body = pure heuristics. Round two feeds the confirmed
 * hints back so the previewed DDL matches exactly what step ② shows —
 * including any column-type overrides typed in step ① (A3). */
async function runDraft(withHints: boolean): Promise<boolean> {
  const ds = props.datasource
  const t = props.table
  if (!ds || !t) return false
  drafting.value = true
  draftError.value = null
  try {
    const overrides = Object.entries(columnTypeOverrides.value).map(([name, type]) => [name, type] as [string, string])
    const res = await datasourcesApi.ddlDraft(
      ds.id,
      t.dbName,
      t.tableName,
      withHints
        ? {
            keyModel: keyModel.value ?? undefined,
            keyColumn: keyColumn.value ?? undefined,
            replicationNum: replicationNum.value ?? undefined,
            partition: keepPartition.value ?? undefined,
            columnTypes: overrides.length > 0 ? overrides : undefined,
          }
        : undefined,
      { silent: true },
    )
    draft.value = res
    // round one seeds the key-column pick (a select with a value); the key
    // model itself stays unpicked — the human must choose it
    if (!withHints) keyColumn.value = res.keyColumn
    // the first draft that offers a partition candidate seeds the keep/drop
    // checkbox to keep (the human unticks to drop; the decision then persists
    // across regenerations)
    if (res.partition && keepPartition.value == null) keepPartition.value = true
    return true
  } catch (e) {
    draftError.value = e instanceof ApiError ? e.message : '草稿生成失败，请稍后重试'
    return false
  } finally {
    drafting.value = false
  }
}

function toStep3(): void {
  void runDraft(true).then((ok) => {
    if (ok) step.value = 3
  })
}

/** Back to ② from the preview: the next DDL may differ, so the previous
 * execute outcome no longer describes it (evolve results are table-level
 * and stay). */
function regenerate(): void {
  execResult.value = null
  execError.value = null
  step.value = 2
}

// --- step ① column table ---
interface ColumnRow {
  name: string
  type: string
}

const columnRows = computed<ColumnRow[]>(() =>
  (draft.value?.columns ?? []).map(([name, type]) => ({ name, type })),
)

/**
 * Column-type dropdown options (A3): the 13 words the DDL generator maps.
 * The parametric words appear as common presets and the select is
 * tag+filterable, so `decimal(20,4)` can be typed directly.
 * list<float32>/variant/null are deliberately absent — the generator
 * refuses them until the W5 probe pass opens them.
 */
const COLUMN_TYPE_OPTIONS: SelectOption[] = [
  'utf8',
  'string',
  'bool',
  'int8',
  'int16',
  'int32',
  'int64',
  'float32',
  'float64',
  'decimal(18,2)',
  'decimal(38,9)',
  'timestamp(s)',
  'timestamp(ms)',
  'timestamp(us)',
  'timestamp(ns)',
  'date',
  'binary',
].map((v) => ({ label: v, value: v }))

/** Per-column type overrides typed in step ① (A3): `name → vocabulary
 * type`. They steer every regenerate of THIS wizard session only — the
 * registered columnsJson is never rewritten (the console applies them as
 * DdlDraftInput.columnTypes). */
const columnTypeOverrides = ref<Record<string, string>>({})

function onTypeChange(name: string, ty: string): void {
  if (!draft.value) return
  const col = draft.value.columns.find(([n]) => n === name)
  if (!col || col[1] === ty) return
  col[1] = ty
  columnTypeOverrides.value[name] = ty
}

const columnCols = computed<DataTableColumns<ColumnRow>>(() => {
  const d = draft.value
  return [
    { title: '#', key: 'idx', width: 44, render: (_, i) => String(i + 1) },
    { title: '列名', key: 'name', render: (r) => h(MonoField, { value: r.name, copyable: false }) },
    {
      title: '类型',
      key: 'type',
      width: 168,
      render: (r) =>
        h(NSelect, {
          value: r.type,
          options: COLUMN_TYPE_OPTIONS,
          size: 'small',
          filterable: true,
          tag: true,
          'onUpdate:value': (v: string) => onTypeChange(r.name, v),
        }),
    },
    {
      title: '角色',
      key: 'role',
      width: 260,
      render: (r) => {
        const tags: ReturnType<typeof h>[] = []
        if (d && r.name === d.keyColumn) {
          tags.push(h(NTag, { size: 'small', type: 'primary', bordered: false }, { default: () => '键列（分桶）' }))
          if (d.keyColumnGuessed) tags.push(guessTag())
        }
        if (d?.sequenceColumn && r.name === d.sequenceColumn) {
          tags.push(h(NTag, { size: 'small', bordered: false }, { default: () => 'sequence_column' }))
        }
        if (d?.partition && r.name === d.partition.column) {
          tags.push(h(NTag, { size: 'small', type: 'info', bordered: false }, { default: () => '分区候选' }))
          if (d.partition.guessed) tags.push(guessTag())
        }
        return tags.length ? h('div', { style: 'display:flex;gap:4px;flex-wrap:wrap;' }, tags) : '—'
      },
    },
  ]
})

/** The heuristic 「猜测」 badge — the whole point of T-A1's human-confirm gate. */
function guessTag() {
  return h(NTag, { size: 'small', type: 'warning', bordered: false }, { default: () => '猜测' })
}

// --- step ② options ---
const KEY_MODELS: readonly DdlKeyModel[] = ['unique', 'duplicate']

const KEY_MODEL_HELP: Record<DdlKeyModel, string> = {
  unique: 'UNIQUE KEY（MoW）——CDC 按键去重，同键新版本覆盖',
  duplicate: 'DUPLICATE KEY——追加明细，不去重',
}

const keyColumnOptions = computed<SelectOption[]>(() =>
  (draft.value?.columns ?? []).map(([name]) => ({
    label: name,
    value: name,
  })),
)

/** Still the heuristic pick AND still guessed → keep the badge next to the select. */
const keyColumnStillGuessed = computed(
  () => draft.value != null && draft.value.keyColumnGuessed && keyColumn.value === draft.value.keyColumn,
)

/** The partition caveat is type-dependent (A3): a BIGINT partition column
 * carries the yyyyMMdd warning; a timestamp/date column partitions on true
 * date bounds and carries none. */
const partitionColumnIsInteger = computed(() => {
  const d = draft.value
  const p = d?.partition
  if (!d || !p) return false
  return (d.columns.find(([n]) => n === p.column)?.[1] ?? '') === 'int64'
})

async function runExecute(): Promise<void> {
  const ds = props.datasource
  const t = props.table
  const d = draft.value
  const km = keyModel.value
  if (!ds || !t || !d || !km) return
  executing.value = true
  execError.value = null
  try {
    execResult.value = await datasourcesApi.ddlExecute(
      ds.id,
      t.dbName,
      t.tableName,
      { ddl: d.ddl, keyModel: km },
      { silent: true },
    )
    message.success('建表成功（已入审计 TABLE_DDL_EXECUTE）')
    // Overrides steered only this draft (A3): the registration keeps the
    // inferred types, so evolve may later suggest a retype — say so now
    // instead of leaving that as a surprise 409.
    if (Object.keys(columnTypeOverrides.value).length > 0) {
      message.info('类型改判仅作用于本次建表，未写回登记列——后续 evolve 比对可能建议改类型')
    }
  } catch (e) {
    // 502 DDL_FAILED carries the Doris error verbatim — show it inline
    execResult.value = null
    execError.value = e instanceof ApiError ? e.message : '执行失败，请稍后重试'
  } finally {
    executing.value = false
  }
}

async function runEvolve(): Promise<void> {
  const ds = props.datasource
  const t = props.table
  if (!ds || !t) return
  evolving.value = true
  try {
    const res = await datasourcesApi.ddlEvolve(ds.id, t.dbName, t.tableName, { silent: true })
    evolveState.value = { kind: 'ok', res }
  } catch (e) {
    if (e instanceof ApiError && e.status === 409) {
      evolveState.value = { kind: 'conflict', msg: e.message, refs: parseEvolveRefs(e.data) }
    } else {
      evolveState.value = { kind: 'error', msg: e instanceof ApiError ? e.message : '演进失败，请稍后重试' }
    }
  } finally {
    evolving.value = false
  }
}

/** `error.refs` of the 409 — defensive read, the server owns the shape. */
function parseEvolveRefs(data: unknown): DdlEvolveConflictRefs | null {
  const d = data as { error?: { refs?: Partial<DdlEvolveConflictRefs> } } | null
  const r = d?.error?.refs
  if (!r) return null
  return {
    suggestedDdl: Array.isArray(r.suggestedDdl) ? r.suggestedDdl : [],
    added: Array.isArray(r.added) ? r.added : [],
    retyped: Array.isArray(r.retyped) ? r.retyped : [],
    removed: Array.isArray(r.removed) ? r.removed : [],
    partialUpdateNotice: typeof r.partialUpdateNotice === 'string' ? r.partialUpdateNotice : '',
  }
}

function close(): void {
  emit('update:show', false)
}
</script>

<template>
  <NModal
    :show="show"
    preset="card"
    :title="`建表向导 · ${table ? `${table.dbName}.${table.tableName}` : ''}`"
    :style="{ width: '760px' }"
    :mask-closable="false"
    @update:show="(v: boolean) => emit('update:show', v)"
  >
    <div class="wizard-body">
      <!-- draft failed (not doris / no registered columns / …) → dead end -->
      <NResult
        v-if="draftError && !draft"
        status="warning"
        title="无法生成建表草稿"
        :description="draftError"
      >
        <template #footer>
          <NButton size="small" @click="runDraft(false)">重试</NButton>
        </template>
      </NResult>

      <template v-else-if="draft">
        <NSteps :current="step" size="small" class="steps">
          <NStep title="确认列清单" />
          <NStep title="选择键模型" />
          <NStep title="预览并执行" />
        </NSteps>

        <!-- ① column confirm -->
        <template v-if="step === 1">
          <NAlert type="info" :bordered="false" class="step-alert">
            草稿将由以下列生成（与 DDL 预览逐列对应）。列清单来自表对象登记的 columnsJson——如需增删列，请先在登记处更新；类型可在此直接改判（如推断为 utf8 的金额列改为 decimal(18,2)）——仅影响本次草稿，不写回登记。
          </NAlert>
          <NDataTable
            size="small"
            :columns="columnCols"
            :data="columnRows"
            :row-key="(r: ColumnRow) => r.name"
            :bordered="false"
            :max-height="280"
            class="col-table"
          />
          <NDescriptions v-if="draft.sequenceColumn || draft.notes.length" label-placement="left" :column="1" size="small" bordered class="notes-desc">
            <NDescriptionsItem v-if="draft.sequenceColumn" :label="`sequence_column`">
              <MonoField :value="draft.sequenceColumn" />
              <NText depth="3" class="desc-hint">——推荐 UNIQUE KEY 的依据；DUPLICATE KEY 表中它只作普通列</NText>
            </NDescriptionsItem>
            <NDescriptionsItem v-if="draft.notes.length" label="生成器备注">
              <ul class="note-list">
                <li v-for="n in draft.notes" :key="n">{{ n }}</li>
              </ul>
            </NDescriptionsItem>
          </NDescriptions>
        </template>

        <!-- ② key model (mandatory) -->
        <template v-else-if="step === 2">
          <NAlert type="info" :bordered="false" class="step-alert">
            推荐 <NTag size="small" type="primary" :bordered="false">{{ draft.keyModel === 'unique' ? 'UNIQUE KEY' : 'DUPLICATE KEY' }}</NTag>：{{ draft.keyModelReason }}
          </NAlert>
          <NFormItem label="键模型" required label-placement="left" label-width="90">
            <div class="model-radios">
              <div v-for="m in KEY_MODELS" :key="m" class="model-row">
                <NRadio :value="m" :checked="keyModel === m" @update:checked="(v: boolean) => (keyModel = v ? m : null)" />
                <span class="model-label">{{ m === 'unique' ? 'UNIQUE KEY' : 'DUPLICATE KEY' }}</span>
                <NTag v-if="draft.keyModel === m" size="small" type="primary" :bordered="false">推荐</NTag>
                <NText depth="3" class="model-help">{{ KEY_MODEL_HELP[m] }}</NText>
              </div>
            </div>
          </NFormItem>
          <NFormItem label="键列" label-placement="left" label-width="90">
            <NSelect v-model:value="keyColumn" :options="keyColumnOptions" size="small" placeholder="键列（UNIQUE/DUPLICATE KEY 与 HASH 分桶列）" />
            <NTag v-if="keyColumnStillGuessed" size="small" type="warning" :bordered="false" class="guess-inline">猜测</NTag>
          </NFormItem>
          <NFormItem label="副本数" label-placement="left" label-width="90">
            <NInputNumber v-model:value="replicationNum" :min="1" :precision="0" size="small" placeholder="留空 = 集群默认" clearable class="replica-input" />
            <NText depth="3" class="desc-hint">——单 BE 集群填 1（console 不代查 SHOW BACKENDS，缺省走集群默认副本数）</NText>
          </NFormItem>
          <!-- partition candidate: keep (confirms) or drop — the generator only
               offers shapes Doris accepts; the human still decides -->
          <NFormItem v-if="draft.partition" label="分区" label-placement="left" label-width="90">
            <div class="partition-row">
              <NCheckbox :checked="keepPartition === true" @update:checked="(v: boolean) => (keepPartition = v)">
                按天 RANGE 动态分区 · 分区列 <span class="model-label">{{ draft.partition.column }}</span>
              </NCheckbox>
              <NTag v-if="draft.partition.guessed && keepPartition !== false" size="small" type="warning" :bordered="false">猜测</NTag>
              <NText depth="3" class="desc-hint">
                ——{{
                  partitionColumnIsInteger
                    ? '分区列须为 yyyyMMdd 形式的整数日期（epoch 秒/毫秒值会落在所有分区之外被拒载）'
                    : '分区列按真实日期边界生成按天动态分区'
                }}；取消勾选则不生成 PARTITION 子句
              </NText>
            </div>
          </NFormItem>
          <NAlert v-if="keyColumnStillGuessed" type="warning" :bordered="false" class="step-alert">
            键列为启发式猜测（id / *_id / 首列）——请确认或改选后再执行。
          </NAlert>
          <!-- round-two draft refused (hint names an unknown column / type, …) -->
          <NAlert v-if="draftError" type="error" :bordered="false" class="step-alert" title="重新生成草稿失败">
            {{ draftError }}
          </NAlert>
        </template>

        <!-- ③ read-only preview + execute / evolve -->
        <template v-else>
          <NDescriptions label-placement="left" :column="2" size="small" bordered class="summary-desc">
            <NDescriptionsItem label="键模型">
              <NTag size="small" type="primary" :bordered="false">{{ keyModel === 'unique' ? 'UNIQUE KEY' : 'DUPLICATE KEY' }}</NTag>
              <NTag v-if="keyModel != null && draft.keyModel !== keyModel" size="small" :bordered="false" class="summary-tag">（覆盖推荐）</NTag>
            </NDescriptionsItem>
            <NDescriptionsItem label="键列">
              <MonoField :value="draft.keyColumn" />
              <NTag v-if="draft.keyColumnGuessed" size="small" type="warning" :bordered="false" class="summary-tag">猜测</NTag>
            </NDescriptionsItem>
            <NDescriptionsItem label="分区">
              <template v-if="draft.partition">
                {{ draft.partition.column }}（按天 RANGE 动态分区）<NTag v-if="draft.partition.guessed" size="small" type="warning" :bordered="false">猜测</NTag>
              </template>
              <NText v-else-if="keepPartition === false" depth="3">已取消（不生成 PARTITION 子句）</NText>
              <NText v-else depth="3">无</NText>
            </NDescriptionsItem>
            <NDescriptionsItem label="副本数">
              {{ replicationNum ?? '集群默认' }}
            </NDescriptionsItem>
          </NDescriptions>
          <!-- round-two notes: the reasons behind THIS text (a partition
               skipped by the chosen key model, a plain-column sequence
               column, …) — step ① showed the round-one set -->
          <NAlert v-if="draft.notes.length" type="default" :bordered="false" class="step-alert" title="生成器备注">
            <ul class="note-list">
              <li v-for="n in draft.notes" :key="n">{{ n }}</li>
            </ul>
          </NAlert>
          <div class="ddl-head">
            <span class="ddl-title">DDL 预览（只读——执行的就是这段原文）</span>
          </div>
          <NCode :code="draft.ddl" class="ddl-code" />

          <NAlert v-if="execResult" type="success" :bordered="false" class="step-alert">
            建表成功 · {{ execResult.elapsedMs }}ms · 影响行 {{ execResult.rowsAffected }} · 已入审计（TABLE_DDL_EXECUTE）
          </NAlert>
          <NAlert v-if="execError" type="error" :bordered="false" class="step-alert" title="DDL 执行失败（Doris 原文）">
            {{ execError }}
          </NAlert>

          <!-- evolve: additive follow-up on an EXISTING table -->
          <div class="evolve-block">
            <div class="evolve-head">
              <span class="ddl-title">表结构演进（evolve，对已存在的表）</span>
              <NPopconfirm @positive-click="runEvolve">
                <template #trigger>
                  <NButton size="small" :loading="evolving" :disabled="executing">比对并演进（ADD COLUMN）</NButton>
                </template>
                将 DESC 实表与登记列比对：仅补「登记有、实表无」的列（Light Schema Change）；检测到改类型/删列只给建议不执行。
              </NPopconfirm>
            </div>
            <NAlert v-if="evolveState.kind === 'idle'" type="default" :bordered="false" class="step-alert">
              适用场景：表已建好但登记列新增了若干——比对结果可安全补列。建新表不需要此步。
            </NAlert>
            <template v-else-if="evolveState.kind === 'ok'">
              <NAlert :type="evolveState.res.changed ? 'success' : 'info'" :bordered="false" class="step-alert">
                <template v-if="evolveState.res.changed">
                  已执行 {{ evolveState.res.statements.length }} 条 ADD COLUMN · {{ evolveState.res.elapsedMs }}ms · 新增列：
                  {{ evolveState.res.added.map(([n]) => n).join('、') }}
                </template>
                <template v-else>实表已含全部登记列，无需演进</template>
              </NAlert>
              <NCode v-if="evolveState.res.changed" :code="evolveState.res.statements.join('\n')" class="ddl-code" />
            </template>
            <template v-else-if="evolveState.kind === 'conflict'">
              <NAlert type="warning" :bordered="false" class="step-alert" :title="evolveState.msg">
                未执行任何语句——以下建议 DDL 供人工评估（改类型/删列不在自动演进范围）。
              </NAlert>
              <NCode v-if="evolveState.refs?.suggestedDdl?.length" :code="evolveState.refs.suggestedDdl.join('\n')" class="ddl-code" />
              <div v-if="evolveState.refs" class="conflict-detail">
                <span v-if="evolveState.refs.retyped.length">
                  改类型：{{ evolveState.refs.retyped.map((r) => `${r.column}（登记 ${r.registered} / 实表 ${r.current}）`).join('、') }}
                </span>
                <span v-if="evolveState.refs.removed.length">
                  删列：{{ evolveState.refs.removed.map(([n]) => n).join('、') }}
                </span>
              </div>
            </template>
            <NAlert v-else-if="evolveState.kind === 'error'" type="error" :bordered="false" class="step-alert">
              {{ evolveState.msg }}
            </NAlert>
            <!-- Doris red line, rides every evolve outcome (T-A1 ⑤ / T-A7) -->
            <NAlert
              v-if="evolveState.kind !== 'idle'"
              type="warning"
              :bordered="false"
              class="step-alert"
              title="互斥提示"
            >
              {{ evolveState.kind === 'ok' ? evolveState.res.partialUpdateNotice : evolveState.kind === 'conflict' ? evolveState.refs?.partialUpdateNotice : '' }}
            </NAlert>
          </div>
        </template>
      </template>

      <NSpin v-else class="wizard-spin">
        <div class="spin-placeholder">正在生成建表草稿…</div>
      </NSpin>
    </div>

    <template #footer>
      <div class="modal-footer">
        <span class="footer-hint">{{
          step === 1 ? '草稿只读生成，不触碰 Doris' : step === 2 ? '键模型为必选项——未确认不能进入执行' : '执行原文即预览原文；执行/演进均入审计'
        }}</span>
        <div class="footer-actions">
          <template v-if="step === 1">
            <NButton size="small" :disabled="drafting" @click="close">关闭</NButton>
            <NButton size="small" type="primary" :loading="drafting" @click="step = 2">下一步</NButton>
          </template>
          <template v-else-if="step === 2">
            <NButton size="small" @click="step = 1">上一步</NButton>
            <!-- §7.2 gate mirrored client-side: no key model, no next step -->
            <NButton size="small" type="primary" :disabled="keyModel == null" :loading="drafting" @click="toStep3">
              生成 DDL 预览
            </NButton>
          </template>
          <template v-else>
            <NButton size="small" :disabled="executing || evolving" @click="regenerate">重新生成</NButton>
            <NPopconfirm @positive-click="runExecute">
              <template #trigger>
                <NButton size="small" type="primary" :loading="executing" :disabled="execResult != null">
                  {{ execResult != null ? '已执行' : '执行建表' }}
                </NButton>
              </template>
              将在 {{ table?.dbName }} 上执行该 CREATE TABLE 原文（失败则原样回传 Doris 报错），确认执行？
            </NPopconfirm>
            <NButton size="small" @click="close">完成</NButton>
          </template>
        </div>
      </div>
    </template>
  </NModal>
</template>

<style scoped>
.wizard-body {
  min-height: 200px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.wizard-spin {
  width: 100%;
}

.spin-placeholder {
  height: 140px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--tr-text-3);
}

.steps {
  margin: 0 4px;
}

.step-alert {
  margin-top: 0;
}

.col-table {
  margin-top: 2px;
}

.notes-desc {
  margin-top: 10px;
}

.note-list {
  margin: 0;
  padding-left: 16px;
}

.desc-hint {
  margin-left: 8px;
  font-size: var(--tr-fs-caption);
}

.model-radios {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.model-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.model-label {
  font-family: var(--tr-font-mono);
  font-size: var(--tr-fs-body-sm);
  color: var(--tr-text-1);
}

.model-help {
  font-size: var(--tr-fs-caption);
}

.guess-inline {
  margin-left: 8px;
}

.replica-input {
  width: 180px;
}

.partition-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.summary-desc {
  margin-top: 2px;
}

.summary-tag {
  margin-left: 6px;
}

.ddl-head,
.evolve-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.ddl-title {
  font-size: var(--tr-fs-body-sm);
  font-weight: 500;
  color: var(--tr-text-2);
}

.ddl-code {
  max-height: 260px;
  overflow: auto;
  font-size: var(--tr-fs-caption);
}

.evolve-block {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 4px;
}

.conflict-detail {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: var(--tr-fs-caption);
  color: var(--tr-text-2);
}

.modal-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}

.footer-actions {
  display: flex;
  gap: 8px;
}

.footer-hint {
  color: var(--tr-text-3);
  font-size: var(--tr-fs-caption);
}
</style>
