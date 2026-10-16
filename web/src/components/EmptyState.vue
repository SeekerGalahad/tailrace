<script setup lang="ts">
import { computed } from 'vue'
import { NButton, NEmpty, NIcon } from 'naive-ui'
import { CheckmarkCircleOutline } from '@vicons/ionicons5'
import { useThemeStore } from '../stores/theme'
import { semantic } from '../theme/tokens'

/**
 * EmptyState (design doc §6.6.6): NEmpty + optional CTA (text + caller-side
 * navigation). `positive` renders the success-tinted variant with no CTA
 * (good-news emptiness, e.g. "近 24h 无事件").
 */

const props = withDefaults(
  defineProps<{
    description?: string
    cta?: string
    positive?: boolean
  }>(),
  { description: '暂无数据', cta: undefined, positive: false },
)

const emit = defineEmits<{ cta: [] }>()

const theme = useThemeStore()
const iconColor = computed(() => semantic[theme.mode].successColor)
</script>

<template>
  <div class="empty-state">
    <NEmpty :description="props.description">
      <template #icon>
        <NIcon v-if="props.positive" :size="40" :color="iconColor">
          <CheckmarkCircleOutline />
        </NIcon>
      </template>
      <template v-if="props.cta" #extra>
        <NButton size="small" type="primary" @click="emit('cta')">{{ props.cta }}</NButton>
      </template>
    </NEmpty>
  </div>
</template>

<style scoped>
.empty-state {
  display: flex;
  justify-content: center;
  padding: 32px 0;
}
</style>
