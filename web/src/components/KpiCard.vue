<script setup lang="ts">
import { NCard, NSkeleton, NStatistic } from 'naive-ui'

/**
 * KpiCard (design doc §6.6.6): NCard + NStatistic with a built-in loading
 * skeleton and a click-through emit (pages wire it to router.push for
 * drill-down). Extra content (e.g. a sparkline) goes in the default slot.
 */

const props = withDefaults(
  defineProps<{
    label: string
    /** Pre-formatted display value (tabular-nums applied in CSS). */
    value: string | number
    loading?: boolean
    clickable?: boolean
  }>(),
  { loading: false, clickable: false },
)

const emit = defineEmits<{ click: [] }>()
</script>

<template>
  <NCard
    size="small"
    class="kpi-card"
    :class="{ clickable: props.clickable }"
    :content-style="{ padding: '16px' }"
    @click="emit('click')"
  >
    <NSkeleton v-if="props.loading" text width="40%" size="medium" />
    <NStatistic v-else :label="props.label" :value="props.value" />
    <slot />
  </NCard>
</template>

<style scoped>
.kpi-card.clickable {
  cursor: pointer;
}

.kpi-card :deep(.n-statistic-value) {
  font-variant-numeric: tabular-nums;
}

.kpi-card :deep(.n-statistic__label) {
  font-size: var(--tr-fs-caption);
}
</style>
