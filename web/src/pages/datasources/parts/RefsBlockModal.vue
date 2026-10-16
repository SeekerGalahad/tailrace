<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { NButton, NIcon, NModal, NText, useMessage } from 'naive-ui'
import { AlertCircleOutline } from '@vicons/ionicons5'
import { semantic } from '../../../theme/tokens'
import { useThemeStore } from '../../../stores/theme'

/**
 * 409 IN_USE delete guard (design doc §6.3 交互 5/6): escalates the popconfirm
 * into a modal that lists the blocking pipeline refs (clickable →
 * /pipelines/{id}/monitor) plus the「先处理引用后再删除」copy.
 */

const props = defineProps<{
  show: boolean
  /** Refs from the 409 error body: `error.data.refs.pipelines`. */
  refs: { pipelines?: number[] } | null
  /** What was being deleted, e.g. the datasource or table name. */
  entityLabel: string
}>()

const emit = defineEmits<{ 'update:show': [boolean] }>()

const router = useRouter()
const message = useMessage()
const theme = useThemeStore()

const errorColor = computed(() => semantic[theme.mode].errorColor)

const pipelines = computed<number[]>(() => props.refs?.pipelines ?? [])

function goPipeline(id: number): void {
  emit('update:show', false)
  void router.push(`/pipelines/${id}/monitor`)
}

async function copyPipelineIds(): Promise<void> {
  try {
    await navigator.clipboard.writeText(pipelines.value.join(', '))
    message.success('已复制管道 ID')
  } catch {
    message.error('复制失败')
  }
}
</script>

<template>
  <NModal
    :show="show"
    preset="card"
    title="无法删除：资源被引用"
    :style="{ width: '520px' }"
    :mask-closable="true"
    @update:show="(v) => emit('update:show', v)"
  >
    <div class="refs-body">
      <div class="refs-head">
        <NIcon :size="20" :style="{ color: errorColor }">
          <AlertCircleOutline />
        </NIcon>
        <span>{{ entityLabel }} 正被以下管道引用，先处理引用后再删除：</span>
      </div>

      <div v-if="pipelines.length > 0" class="refs-list">
        <NButton
          v-for="id in pipelines"
          :key="id"
          text
          type="primary"
          size="small"
          class="ref-link"
          @click="goPipeline(id)"
        >
          管道 #{{ id }}
        </NButton>
      </div>
      <NText v-else depth="3">（服务端未返回引用明细）</NText>
    </div>

    <template #footer>
      <div class="refs-footer">
        <NButton size="tiny" quaternary @click="copyPipelineIds">复制全部管道 ID</NButton>
        <NButton size="small" @click="emit('update:show', false)">知道了</NButton>
      </div>
    </template>
  </NModal>
</template>

<style scoped>
.refs-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.refs-head {
  display: flex;
  gap: 8px;
  align-items: flex-start;
  color: var(--tr-text-1);
}

.refs-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding: 8px 12px;
  border: 1px solid var(--tr-border);
  border-radius: var(--tr-radius-base);
}

.ref-link {
  font-variant-numeric: tabular-nums;
}

.refs-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
