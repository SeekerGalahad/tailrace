<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { NButton, NIcon, NInput, NSelect, NSpin, NText } from 'naive-ui'
import {
  ArrowDownOutline,
  PauseOutline,
  PlayOutline,
  TrashOutline,
} from '@vicons/ionicons5'
import { createSseChannel, type SseStatus } from '../api/sse'
import { ApiError } from '../api/http'
import { authApi } from '../api/auth'
import { semantic } from '../theme/tokens'

/**
 * LogTerminal (design doc §6.6.6): SSE terminal log panel.
 *
 * - always dark (brand element, §6.6.4-3) — ignores theme mode
 * - SSE via native EventSource; `event: eof` from the bridge relies on the
 *   browser's automatic reconnect ("eof 重连")
 * - virtual scrolling: 2000-line window, self-implemented visible-range
 *   rendering (~30 rows), fixed row height
 * - 80ms batched flush merges bursty arrivals into one render (§6.5 monitor)
 * - pauses on document.hidden, kicks one reconnect on visible (§6.0)
 * - 401 probe: a permanently-closed stream re-checks /api/auth/me once and
 *   bounces to /login on 401 (session death during a long-lived stream)
 */

interface LogLine {
  seq?: number
  level: string
  kind?: string
  message: string
}

const props = withDefaults(
  defineProps<{
    /** SSE endpoint, e.g. pipelinesApi.logsUrl(42). Cookie auth only. */
    url: string
    /** Panel height in px. */
    height?: number
    /** Render window cap (design: 2000). */
    maxLines?: number
  }>(),
  { height: 320, maxLines: 2000 },
)

const ROW_H = 22
const OVERSCAN = 10

// --- stream state ---
const lines = ref<LogLine[]>([])
const pausedBuffer = ref<LogLine[]>([])
const paused = ref(false)
const status = ref<SseStatus>('closed')
const received = ref(0)
const eofSeen = ref(false)

// --- toolbar state ---
const keyword = ref('')
const levelFilter = ref<string[]>([])
const autoScroll = ref(true)

const levelOptions = [
  { label: 'INFO', value: 'INFO' },
  { label: 'WARN', value: 'WARN' },
  { label: 'ERROR', value: 'ERROR' },
]

const statusText = computed(
  () => ({ connecting: '连接中', open: '已连接', closed: '已断开' })[status.value],
)
const statusColor = computed(() => {
  if (paused.value) return semantic.dark.warningColor
  return { connecting: semantic.dark.warningColor, open: semantic.dark.successColor, closed: semantic.dark.errorColor }[
    status.value
  ]
})

// --- filtering ---
const filtered = computed(() => {
  const kw = keyword.value.trim().toLowerCase()
  const levels = levelFilter.value
  return lines.value.filter((l) => {
    if (levels.length > 0 && !levels.includes(l.level)) return false
    if (kw !== '' && !`${l.kind ?? ''} ${l.message}`.toLowerCase().includes(kw)) return false
    return true
  })
})

function levelColor(level: string): string {
  const s = semantic.dark // panel is always dark — use dark palette
  if (level === 'ERROR') return s.errorColor
  if (level === 'WARN') return s.warningColor
  return s.infoColor
}

// --- 80ms batched flush (§6.5) ---
let pending: LogLine[] = []
let flushTimer: ReturnType<typeof setTimeout> | null = null

function enqueue(line: LogLine): void {
  pending.push(line)
  received.value += 1
  if (flushTimer) return
  flushTimer = setTimeout(flush, 80)
}

function pushBounded(target: LogLine[], incoming: LogLine[]): LogLine[] {
  const merged = target.concat(incoming)
  return merged.length > props.maxLines ? merged.slice(merged.length - props.maxLines) : merged
}

async function flush(): Promise<void> {
  flushTimer = null
  const incoming = pending
  pending = []
  if (incoming.length === 0) return
  if (paused.value) {
    pausedBuffer.value = pushBounded(pausedBuffer.value, incoming)
  } else {
    lines.value = pushBounded(lines.value, incoming)
    if (autoScroll.value) await nextTick(scrollToBottom)
  }
}

// --- SSE channel ---
const channel = createSseChannel(props.url, {
  events: {
    log: (data) => {
      if (typeof data === 'object' && data !== null) {
        const l = data as LogLine
        if (typeof l.message === 'string') {
          enqueue({ seq: l.seq, level: l.level ?? 'INFO', kind: l.kind, message: l.message })
        }
      }
    },
    eof: () => {
      // Bridge gave up (engine unreachable 5x, §4.5); EventSource will retry.
      eofSeen.value = true
    },
  },
  onStatus: (s) => {
    status.value = s
    if (s === 'open') eofSeen.value = false
    if (s === 'closed') void probeAuth()
  },
})

/** 401 probe — a hard-closed stream may mean the session died. */
async function probeAuth(): Promise<void> {
  try {
    await authApi.me()
  } catch (e) {
    if (e instanceof ApiError && e.status === 401) {
      const { default: router } = await import('../router')
      const current = router.currentRoute.value
      if (current.path !== '/login') {
        await router.push({ path: '/login', query: { redirect: current.fullPath, expired: '1' } })
      }
    }
  }
}

// --- virtual scroll (visible-range windowing) ---
const viewport = ref<HTMLDivElement | null>(null)
const scrollTop = ref(0)
const atBottom = ref(true)

function onScroll(): void {
  if (!viewport.value) return
  scrollTop.value = viewport.value.scrollTop
  // Follow the tail only while the user is at the bottom.
  const el = viewport.value
  atBottom.value = el.scrollTop + el.clientHeight >= el.scrollHeight - 4
}

const startIdx = computed(() => {
  const first = Math.floor(scrollTop.value / ROW_H) - OVERSCAN
  return Math.max(0, first)
})

const visibleCount = computed(() => {
  const rows = Math.ceil(props.height / ROW_H) + OVERSCAN * 2
  return Math.min(filtered.value.length - startIdx.value, rows)
})

const visibleSlice = computed(() => filtered.value.slice(startIdx.value, startIdx.value + visibleCount.value))

function scrollToBottom(): void {
  const el = viewport.value
  if (!el) return
  el.scrollTop = el.scrollHeight
  atBottom.value = true
}

watch(autoScroll, (on) => {
  if (on) scrollToBottom()
})

// --- toolbar actions ---
function togglePause(): void {
  paused.value = !paused.value
  if (!paused.value) {
    // Resume: catch up with what accumulated while frozen.
    lines.value = pushBounded(lines.value, pausedBuffer.value.splice(0))
    if (autoScroll.value) void nextTick(scrollToBottom)
  }
}

function clear(): void {
  lines.value = []
  pausedBuffer.value = []
  received.value = 0
}

// --- lifecycle + visibility (§6.0: SSE pauses while hidden, kicks on visible) ---
// NOTE: `url` is fixed for the component's lifetime — key the component by
// pipeline id so switching remounts it with a fresh channel.
function onVisibility(): void {
  if (document.hidden) {
    channel.close()
  } else if (status.value === 'closed') {
    channel.open()
  }
}

onMounted(() => {
  channel.open()
  document.addEventListener('visibilitychange', onVisibility)
})

onBeforeUnmount(() => {
  if (flushTimer) clearTimeout(flushTimer)
  document.removeEventListener('visibilitychange', onVisibility)
  channel.close()
})
</script>

<template>
  <section class="log-stream" :style="{ height: `${props.height + 40}px` }">
    <header class="toolbar">
      <div class="toolbar-left">
        <span class="status-dot" :style="{ background: statusColor }" />
        <NText class="status-text" depth="3">{{ paused ? '已暂停' : statusText }}</NText>
        <NText class="count" depth="3">{{ filtered.length }}/{{ received }} 行</NText>
      </div>
      <div class="toolbar-right">
        <NSelect
          v-model:value="levelFilter"
          size="tiny"
          multiple
          clearable
          placeholder="级别"
          :options="levelOptions"
          class="level-select"
          :consistent-menu-width="false"
        />
        <NInput
          v-model:value="keyword"
          size="tiny"
          clearable
          placeholder="关键字"
          class="keyword-input"
        />
        <NButton size="tiny" :type="paused ? 'warning' : 'default'" quaternary @click="togglePause">
          <template #icon>
            <NIcon><PauseOutline v-if="!paused" /><PlayOutline v-else /></NIcon>
          </template>
          {{ paused ? '继续' : '暂停' }}
        </NButton>
        <NButton size="tiny" :type="autoScroll ? 'primary' : 'default'" quaternary @click="autoScroll = !autoScroll">
          自动滚动
        </NButton>
        <NButton size="tiny" quaternary @click="scrollToBottom">
          <template #icon>
            <NIcon><ArrowDownOutline /></NIcon>
          </template>
          到底
        </NButton>
        <NButton size="tiny" quaternary @click="clear">
          <template #icon>
            <NIcon><TrashOutline /></NIcon>
          </template>
          清空
        </NButton>
      </div>
    </header>
    <div class="panel">
      <NSpin v-if="status === 'connecting' && lines.length === 0" size="small" class="spinner" />
      <div
        ref="viewport"
        class="viewport"
        :style="{ height: `${props.height}px` }"
        @scroll="onScroll"
      >
        <div class="spacer" :style="{ height: `${filtered.length * ROW_H}px` }">
          <div
            v-for="(line, i) in visibleSlice"
            :key="startIdx + i"
            class="row"
            :style="{ top: `${(startIdx + i) * ROW_H}px` }"
          >
            <span class="seq">{{ line.seq ?? '' }}</span>
            <span class="level" :style="{ color: levelColor(line.level) }">{{ line.level }}</span>
            <span v-if="line.kind" class="kind">{{ line.kind }}</span>
            <span class="message">{{ line.message }}</span>
          </div>
        </div>
      </div>
      <div v-if="eofSeen" class="eof-banner">日志流已中断（引擎不可达），自动重连中…</div>
      <div v-else-if="filtered.length === 0 && status === 'open'" class="eof-banner">
        暂无日志
      </div>
    </div>
  </section>
</template>

<style scoped>
.log-stream {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--tr-border);
  border-radius: var(--tr-radius-code);
  overflow: hidden;
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 4px 10px;
  background: var(--tr-bg-card);
  border-bottom: 1px solid var(--tr-border);
  flex-wrap: wrap;
}

.toolbar-left {
  display: flex;
  align-items: center;
  gap: 8px;
}

.toolbar-right {
  display: flex;
  align-items: center;
  gap: 4px;
  flex-wrap: wrap;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex: none;
}

.status-text,
.count {
  font-size: var(--tr-fs-caption);
  font-variant-numeric: tabular-nums;
}

.level-select {
  width: 130px;
}

.keyword-input {
  width: 120px;
}

/* Terminal panel: always dark, both theme modes (§6.6.4-3). */
.panel {
  position: relative;
  background: var(--tr-bg-term);
  color: var(--tr-text-term);
}

.spinner {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  z-index: 1;
}

.viewport {
  overflow-y: auto;
  overflow-x: auto;
  font-family: var(--tr-font-mono);
  font-size: var(--tr-fs-caption);
  line-height: 22px;
}

.spacer {
  position: relative;
  min-width: 100%;
}

.row {
  position: absolute;
  left: 0;
  /* max-content so long lines widen the h-scroll area instead of clipping */
  width: max-content;
  min-width: 100%;
  height: 22px;
  display: flex;
  gap: 10px;
  padding: 0 10px;
  white-space: pre;
}

.seq {
  color: var(--tr-text-3);
  min-width: 48px;
  text-align: right;
  font-variant-numeric: tabular-nums;
  flex: none;
}

.level {
  flex: none;
  min-width: 44px;
}

.kind {
  color: var(--tr-text-2);
  flex: none;
}

.message {
  color: var(--tr-text-term);
}

.eof-banner {
  position: absolute;
  bottom: 8px;
  left: 50%;
  transform: translateX(-50%);
  padding: 2px 12px;
  background: color-mix(in srgb, var(--tr-bg-term) 90%, transparent);
  border: 1px solid var(--tr-border);
  border-radius: var(--tr-radius-base);
  font-size: var(--tr-fs-caption);
  color: var(--tr-text-2);
  white-space: nowrap;
}
</style>
