<script setup lang="ts">
import { computed, ref } from 'vue'
import { NIcon, NTooltip } from 'naive-ui'
import { CheckmarkOutline, CopyOutline } from '@vicons/ionicons5'

/**
 * MonoField (design doc §6.6.6): machine identity (node_key / SQL / consumer
 * group / offset / sha256 / logs) in the mono font, one-click copy with a
 * "已复制" tooltip, optional head-truncation (e.g. sha256 → first 8 chars + …).
 */

const props = withDefaults(
  defineProps<{
    value: string
    /** Show only the first N chars + '…'; copy still copies the full value. */
    headChars?: number
    copyable?: boolean
  }>(),
  { headChars: 0, copyable: true },
)

const copied = ref(false)
let copiedTimer: ReturnType<typeof setTimeout> | null = null

const display = computed(() =>
  props.headChars > 0 && props.value.length > props.headChars
    ? `${props.value.slice(0, props.headChars)}…`
    : props.value,
)

async function copy(): Promise<void> {
  if (!props.copyable || props.value === '') return
  try {
    await navigator.clipboard.writeText(props.value)
  } catch {
    // Insecure-origin fallback (LAN HTTP deployments): hidden textarea trick.
    const ta = document.createElement('textarea')
    ta.value = props.value
    ta.style.position = 'fixed'
    ta.style.opacity = '0'
    document.body.appendChild(ta)
    ta.select()
    try {
      document.execCommand('copy')
    } finally {
      document.body.removeChild(ta)
    }
  }
  copied.value = true
  if (copiedTimer) clearTimeout(copiedTimer)
  copiedTimer = setTimeout(() => (copied.value = false), 1500)
}
</script>

<template>
  <span class="mono-field">
    <span class="mono-text">{{ display }}</span>
    <NTooltip v-if="copyable" trigger="hover" placement="top" :show-arrow="false">
      <template #trigger>
        <button class="copy-btn" type="button" :title="copied ? '已复制' : '复制'" @click="copy">
          <NIcon :size="12">
            <CheckmarkOutline v-if="copied" />
            <CopyOutline v-else />
          </NIcon>
        </button>
      </template>
      {{ copied ? '已复制' : '复制' }}
    </NTooltip>
  </span>
</template>

<style scoped>
.mono-field {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-family: var(--tr-font-mono);
  font-size: var(--tr-fs-body-sm);
  color: var(--tr-text-1);
  font-variant-numeric: tabular-nums;
  word-break: break-all;
}

.copy-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: none;
  background: transparent;
  color: var(--tr-text-3);
  cursor: pointer;
  padding: 2px;
  border-radius: var(--tr-radius-sharp);
}

.copy-btn:hover {
  color: var(--tr-text-1);
}
</style>
