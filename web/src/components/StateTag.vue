<script setup lang="ts">
import { computed } from 'vue'
import { NTag } from 'naive-ui'
import {
  isKind,
  stateTone,
  type StateDomain,
  type TagTone,
} from '../theme/state-map'
import { kindColors, withAlpha } from '../theme/tokens'

/**
 * StateTag (design doc §6.6.5/§6.6.6): the ONLY place status/type → color is
 * decided. Pages must consume this component, never pick tag colors themselves.
 *
 * domain ∈ pipeline | node | deploy | datasource | eventLevel | startOffset | kind
 * (kind tags use the §6.6.1 type palette: kafka/doris/file/jdbc/mqtt/s3).
 *
 * `active` marks a deploy step currently in progress → primary tone (§6.6.5).
 */

const props = withDefaults(
  defineProps<{
    domain: StateDomain | 'kind'
    value: string | null | undefined
    /** Display text override (defaults to the raw value). */
    label?: string
    active?: boolean
    size?: 'small' | 'medium'
    bordered?: boolean
  }>(),
  { label: undefined, active: false, size: 'small', bordered: true },
)

const tone = computed<TagTone>(() => {
  if (props.active) return 'primary' // deploy step in progress (§6.6.5)
  if (props.domain === 'kind') return 'neutral'
  return stateTone(props.domain, props.value)
})

const text = computed(() => props.label ?? props.value ?? '')

/** kind domain: colored wash from the §6.6.1 type palette. */
const kindColor = computed<string | null>(() => {
  if (props.domain !== 'kind') return null
  return isKind(props.value) ? kindColors[props.value] : null
})

const tagType = computed(() => (tone.value === 'neutral' || kindColor.value ? 'default' : tone.value))

const tagColor = computed(() => {
  const c = kindColor.value
  if (!c) return undefined
  return {
    color: withAlpha(c, 0.14),
    textColor: c,
    borderColor: withAlpha(c, 0.45),
  }
})
</script>

<template>
  <NTag
    v-if="value != null || label != null"
    :type="tagType"
    :size="size"
    :bordered="bordered"
    :color="tagColor"
    class="state-tag"
  >
    {{ text }}
  </NTag>
  <!-- null/undefined renders nothing: absent state, not a fake one -->
</template>

<style scoped>
.state-tag {
  font-variant-numeric: tabular-nums;
  border-radius: var(--tr-radius-sharp); /* §6.6.3: Tag 2px sharp */
}
</style>
