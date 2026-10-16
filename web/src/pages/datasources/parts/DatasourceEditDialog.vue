<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from 'vue'
import {
  NAlert,
  NButton,
  NForm,
  NFormItem,
  NInput,
  NInputNumber,
  NModal,
  NRadioButton,
  NRadioGroup,
  NText,
  useDialog,
  useMessage,
  type FormInst,
  type FormItemRule,
} from 'naive-ui'
import { ApiError } from '../../../api/http'
import {
  datasourcesApi,
  type ConnTestResult,
  type Datasource,
  type DatasourceType,
  type DatasourceTypeField,
  type DatasourceUpsertInput,
} from '../../../api/datasources'

/**
 * Register / edit datasource modal (design doc §6.3 线框「注册数据源」，width 640).
 * Dynamic form driven entirely by GET /api/datasource-types (single source of
 * truth — zero hardcoded type knowledge). Test-connection probes without
 * persisting (HTTP stays 200 for CONN_FAIL, §4.2). Edit carries `generation`
 * for CAS; password left empty keeps the stored credential.
 */

const props = defineProps<{
  show: boolean
  types: DatasourceType[]
  /** Registry failed to load (page offers a retry through `retry-types`). */
  typesError?: boolean
  /** null = register; otherwise edit this datasource (CAS on generation). */
  source: Datasource | null
}>()

const emit = defineEmits<{
  'update:show': [boolean]
  saved: []
  'retry-types': []
}>()

const dialog = useDialog()
const message = useMessage()

const formRef = ref<FormInst | null>(null)

// --- form model: flat members + per-inputType buckets (§4.2 fields) ---
const model = reactive({
  name: '',
  username: '',
  password: '',
  text: {} as Record<string, string>,
  num: {} as Record<string, number | null>,
})
const typeKey = ref('')
const generation = ref(1)

const activeType = computed<DatasourceType | null>(
  () => props.types.find((t) => t.type === typeKey.value) ?? null,
)

const nameRule: FormItemRule = { required: true, message: '请输入名称', trigger: ['input', 'blur'] }

function fieldPath(f: DatasourceTypeField): string {
  return f.inputType === 'number' ? `num.${f.name}` : `text.${f.name}`
}

function fieldRule(f: DatasourceTypeField): FormItemRule {
  return { required: f.required, message: `请填写${f.label}`, trigger: ['input', 'blur'] }
}

function buildConnJson(): Record<string, unknown> {
  const fields = activeType.value?.fields
  if (!fields) return props.source ? { ...props.source.connJson } : {} // unknown type in edit: keep stored conn
  const out: Record<string, unknown> = {}
  for (const f of fields) {
    out[f.name] = f.inputType === 'number' ? (model.num[f.name] ?? null) : (model.text[f.name] ?? '')
  }
  return out
}

// --- dirty tracking (maskClosable guard, §6.3 交互/通用) ---
const dirty = ref(false)
let suppressDirty = false

watch([() => model.name, () => model.username, () => model.password, typeKey], () => {
  if (!suppressDirty) dirty.value = true
})
watch([() => model.text, () => model.num], () => {
  if (!suppressDirty) dirty.value = true
}, { deep: true })

// --- transient feedback state ---
const testing = ref(false)
const saving = ref(false)
const pulling = ref(false)
const testResult = ref<ConnTestResult | null>(null)
const testError = ref<string | null>(null)
const nameConflict = ref<string | null>(null)
const genConflict = ref(false)
const formError = ref<string | null>(null)

function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : '请求失败，请稍后重试'
}

function resetFields(conn: Record<string, unknown>): void {
  for (const key of Object.keys(model.text)) delete model.text[key]
  for (const key of Object.keys(model.num)) delete model.num[key]
  for (const f of activeType.value?.fields ?? []) {
    const v = conn[f.name]
    if (f.inputType === 'number') model.num[f.name] = typeof v === 'number' ? v : null
    else model.text[f.name] = v == null ? '' : String(v)
  }
}

/** (Re)build the form from `src` (defaults to props.source): pullLatest passes
 * the freshly fetched datasource so CAS recovery rebases on server state
 * instead of the stale object the parent page handed in. */
async function initForm(src: Datasource | null = props.source): Promise<void> {
  suppressDirty = true
  dirty.value = false
  testResult.value = null
  testError.value = null
  nameConflict.value = null
  genConflict.value = false
  formError.value = null
  formRef.value?.restoreValidation()
  model.name = src?.name ?? ''
  model.username = ''
  model.password = ''
  typeKey.value = src?.type ?? props.types[0]?.type ?? ''
  generation.value = src?.generation ?? 1
  resetFields(src?.connJson ?? {})
  await nextTick(() => {
    suppressDirty = false
  })
}

watch(
  () => props.show,
  (open) => {
    if (open) void initForm()
  },
)

function onTypeChange(t: string): void {
  typeKey.value = t
  resetFields({})
  testResult.value = null
  testError.value = null
}

// --- close flow with dirty guard ---
function closeNow(): void {
  emit('update:show', false)
}

function onUpdateShow(v: boolean): void {
  if (v) {
    emit('update:show', true)
    return
  }
  if (dirty.value) {
    dialog.warning({
      title: '放弃修改？',
      content: '表单已有未保存的修改，关闭后将丢失。',
      positiveText: '放弃修改',
      negativeText: '继续编辑',
      onPositiveClick: () => closeNow(),
    })
  } else {
    closeNow()
  }
}

// --- probe without persisting (§4.2 POST /api/datasources/test) ---
async function runTest(): Promise<void> {
  if (testing.value || !activeType.value) return
  testing.value = true
  testResult.value = null
  testError.value = null
  try {
    testResult.value = await datasourcesApi.test({
      type: typeKey.value,
      connJson: buildConnJson(),
      username: model.username || null,
      password: model.password || null,
    })
  } catch (e) {
    testError.value = errorMessage(e) // transport-level failure (probe itself never ran)
  } finally {
    testing.value = false
  }
}

// --- refresh the stored config after a 409 GENERATION_CONFLICT ---
async function pullLatest(): Promise<void> {
  if (!props.source || pulling.value) return
  pulling.value = true
  try {
    const fresh = await datasourcesApi.get(props.source.id, { silent: true })
    // Rebuild the whole form from the server-latest datasource (audit-web H5):
    // rebasing only `generation` would make the next save overwrite the other
    // editor's changes with the stale values still sitting in the form.
    await initForm(fresh)
    message.success('已拉取最新配置，表单已重建为服务端当前值，请在此基础上修改后保存')
  } catch (e) {
    message.error(errorMessage(e))
  } finally {
    pulling.value = false
  }
}

async function save(): Promise<void> {
  if (saving.value) return
  try {
    await formRef.value?.validate()
  } catch {
    return // inline field feedback is enough
  }
  if (!activeType.value) return
  nameConflict.value = null
  genConflict.value = false
  formError.value = null
  saving.value = true
  try {
    const input: DatasourceUpsertInput = {
      name: model.name.trim(),
      type: typeKey.value,
      connJson: buildConnJson(),
      username: model.username.trim() || null,
      // empty password on update = keep the stored credential (§4.2)
      password: model.password || null,
    }
    if (props.source) {
      await datasourcesApi.update(props.source.id, { ...input, generation: generation.value }, { silent: true })
    } else {
      await datasourcesApi.create(input, { silent: true })
    }
    message.success(props.source ? '已保存修改' : '数据源已注册')
    emit('saved')
    closeNow()
  } catch (e) {
    if (e instanceof ApiError) {
      if (e.status === 409 && e.code === 'GENERATION_CONFLICT') {
        genConflict.value = true
      } else if (e.status === 409) {
        nameConflict.value = e.message || '名称已存在'
      } else if (e.status >= 500 || e.status === 0) {
        message.error(errorMessage(e))
      } else {
        formError.value = errorMessage(e) // 400 etc → inline alert
      }
    } else {
      message.error('保存失败，请稍后重试')
    }
  } finally {
    saving.value = false
  }
}
</script>

<template>
  <NModal
    :show="show"
    preset="card"
    :title="source ? '编辑数据源' : '注册数据源'"
    :style="{ width: '640px' }"
    :mask-closable="true"
    class="ds-form-modal"
    @update:show="onUpdateShow"
  >
    <NAlert v-if="typesError || types.length === 0" type="error" :bordered="false">
      <div class="alert-row">
        <span>类型注册表未加载，无法渲染动态表单</span>
        <NButton size="tiny" @click="emit('retry-types')">重试</NButton>
      </div>
    </NAlert>

    <NForm v-else ref="formRef" :model="model" label-placement="left" label-width="150">
      <NFormItem
        label="名称"
        path="name"
        :rule="nameRule"
        :validation-status="nameConflict ? 'error' : undefined"
        :feedback="nameConflict ?? undefined"
      >
        <NInput v-model:value="model.name" placeholder="唯一名称，如 edge-kafka" @input="nameConflict = null" />
      </NFormItem>

      <NFormItem label="类型">
        <div class="type-row">
          <NRadioGroup :value="typeKey" :disabled="!!source" @update:value="onTypeChange">
            <NRadioButton v-for="t in types" :key="t.type" :value="t.type">
              {{ t.label }}
            </NRadioButton>
          </NRadioGroup>
          <NText v-if="activeType" depth="3" class="family-note">{{ activeType.family }}</NText>
        </div>
      </NFormItem>

      <NFormItem
        v-for="f in activeType?.fields ?? []"
        :key="f.name"
        :label="f.label"
        :path="fieldPath(f)"
        :rule="fieldRule(f)"
      >
        <NInputNumber
          v-if="f.inputType === 'number'"
          v-model:value="model.num[f.name]"
          :placeholder="f.placeholder"
          class="full-width"
        />
        <NInput
          v-else-if="f.inputType === 'password'"
          v-model:value="model.text[f.name]"
          type="password"
          show-password-on="click"
          :placeholder="f.placeholder"
        />
        <NInput
          v-else-if="f.inputType === 'textarea'"
          v-model:value="model.text[f.name]"
          type="textarea"
          :rows="2"
          :placeholder="f.placeholder"
        />
        <!-- 'select' has no option source in the §4.2 v1 registry — plain input -->
        <NInput v-else v-model:value="model.text[f.name]" :placeholder="f.placeholder" />
      </NFormItem>

      <template v-if="activeType && activeType.credentialLabel">
        <NFormItem :label="activeType.credentialLabel" path="username">
          <NInput v-model:value="model.username" placeholder="如 loader" />
        </NFormItem>
        <NFormItem label="密码" path="password">
          <NInput
            v-model:value="model.password"
            type="password"
            show-password-on="click"
            :placeholder="source ? '留空 = 保留原凭据' : undefined"
          />
        </NFormItem>
      </template>

      <NAlert
        v-if="testResult"
        :type="testResult.status === 'CONN_OK' ? 'success' : 'error'"
        :bordered="false"
        class="modal-alert"
      >
        {{
          testResult.status === 'CONN_OK'
            ? `连通正常 · ${testResult.latencyMs}ms`
            : `连通失败：${testResult.message}（耗时 ${testResult.latencyMs}ms）`
        }}
      </NAlert>
      <NAlert v-else-if="testError" type="error" :bordered="false" class="modal-alert">
        探测请求失败：{{ testError }}
      </NAlert>
      <NAlert v-if="genConflict" type="warning" :bordered="false" class="modal-alert">
        <div class="alert-row">
          <span>配置已被他人修改（GENERATION_CONFLICT），请拉取最新后重填。</span>
          <NButton size="tiny" :loading="pulling" @click="pullLatest">拉取最新</NButton>
        </div>
      </NAlert>
      <NAlert v-if="formError" type="error" :bordered="false" class="modal-alert">
        {{ formError }}
      </NAlert>
    </NForm>

    <template #footer>
      <div class="modal-footer">
        <NButton :loading="testing" :disabled="!activeType" @click="runTest">测试连接</NButton>
        <div class="footer-right">
          <NButton :disabled="saving" @click="onUpdateShow(false)">取消</NButton>
          <NButton type="primary" :loading="saving" :disabled="!activeType" @click="save">保存</NButton>
        </div>
      </div>
    </template>
  </NModal>
</template>

<style scoped>
.full-width {
  width: 100%;
}

.type-row {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.family-note {
  font-size: var(--tr-fs-caption);
}

.modal-alert {
  margin-top: 4px;
}

.alert-row {
  display: flex;
  align-items: center;
  gap: 12px;
  justify-content: space-between;
}

.modal-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.footer-right {
  display: flex;
  gap: 8px;
}
</style>
