<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import uPlot from 'uplot'
import { useThemeStore } from '../stores/theme'
import { chartGrid, chartSeries, fontMono, neutral, withAlpha } from '../theme/tokens'
import type { ChartSeries } from './chart-types'

/**
 * MetricChart (design doc §6.6.6): thin uPlot wrapper.
 *
 * - dark canvas, thin lines (1px) + area gradient fade (§6.0b-4)
 * - ≤4 series, colors from the fixed seq palette (§6.6.1)
 * - dual y-axis via per-series `yAxis: 2`
 * - spark mode: axes/legend/cursor hidden (60-point sparklines)
 * - subscribes to ThemeStore: theme switch rebuilds with re-read tokens (§6.6.4-3)
 * - hover tooltip via a setCursor hook
 */

const props = withDefaults(
  defineProps<{
    timestamps: number[]
    series: ChartSeries[]
    /** Plot height in px. Defaults: 200 (full), 28 (spark). */
    height?: number
    spark?: boolean
  }>(),
  { height: undefined, spark: false },
)

const theme = useThemeStore()
const wrapEl = ref<HTMLDivElement | null>(null)
const tipEl = ref<HTMLDivElement | null>(null)

const chartHeight = computed(() => props.height ?? (props.spark ? 28 : 200))
const hasY2 = computed(() => props.series.some((s) => s.yAxis === 2))

const tipVisible = ref(false)
const tipLeft = ref(0)
const tipRows = ref<Array<{ label: string; value: string; color: string }>>([])
const tipTime = ref('')

function compactNum(v: number): string {
  const abs = Math.abs(v)
  if (abs >= 1e9) return `${(v / 1e9).toFixed(1)}G`
  if (abs >= 1e6) return `${(v / 1e6).toFixed(1)}M`
  if (abs >= 1e3) return `${(v / 1e3).toFixed(1)}k`
  return `${Math.round(v * 100) / 100}`
}

function hhmm(ts: number): string {
  const d = new Date(ts * 1000)
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
}

function buildTooltip(u: uPlot): void {
  const idx = u.cursor.idx
  if (idx == null || props.spark || props.timestamps.length === 0) {
    tipVisible.value = false
    return
  }
  tipTime.value = hhmm(props.timestamps[idx] ?? 0)
  tipRows.value = props.series.map((s, i) => {
    const v = s.points[idx]
    return {
      label: s.label,
      value: v == null ? '—' : compactNum(v),
      color: chartSeries[i] ?? chartSeries[3],
    }
  })
  tipVisible.value = true
  // Keep the tooltip inside the wrapper.
  const wrapW = wrapEl.value?.clientWidth ?? 0
  const left = (u.cursor.left ?? 0) + 12
  tipLeft.value = Math.min(left, Math.max(wrapW - 130, 0))
}

function areaFill(color: string): (u: uPlot, seriesIdx: number) => CanvasRenderingContext2D['fillStyle'] {
  return (u) => {
    const grad = u.ctx.createLinearGradient(0, u.bbox.top, 0, u.bbox.top + u.bbox.height)
    grad.addColorStop(0, withAlpha(color, 0.22))
    grad.addColorStop(1, withAlpha(color, 0))
    return grad
  }
}

function buildOptions(): uPlot.Options {
  const mode = theme.mode
  const axisColor = neutral[mode]['text-2']
  const grid = { stroke: chartGrid[mode] as CanvasRenderingContext2D['strokeStyle'] }
  const font = `11px ${fontMono}`

  const seriesConfig: uPlot.Series[] = [
    {}, // x
    ...props.series.map((s, i) => {
      const color = chartSeries[i] ?? chartSeries[3]
      return {
        label: s.label,
        scale: s.yAxis === 2 ? 'y2' : 'y',
        stroke: color as CanvasRenderingContext2D['strokeStyle'],
        width: 1,
        fill: areaFill(color),
        points: { show: false },
        value: (_u: uPlot, raw: number) => (raw == null ? '—' : compactNum(raw)),
        spanGaps: true,
      } satisfies uPlot.Series
    }),
  ]

  const yAxis = (scale: string): uPlot.Axis => ({
    scale,
    side: scale === 'y' ? 3 : 1,
    stroke: axisColor,
    grid: scale === 'y' ? grid : { show: false },
    values: (_u, splits) => splits.map((v) => compactNum(v)),
    size: 44,
    font,
  })

  return {
    width: wrapEl.value?.clientWidth ?? 320,
    height: chartHeight.value,
    series: seriesConfig,
    scales: { x: { time: false }, y: {}, ...(hasY2.value ? { y2: {} } : {}) },
    axes: props.spark
      ? [{ show: false }, { show: false }]
      : [
          {
            grid,
            stroke: axisColor,
            values: (_u, splits) => splits.map((v) => hhmm(v)),
            font,
            size: 30,
          },
          yAxis('y'),
          ...(hasY2.value ? [yAxis('y2')] : []),
        ],
    legend: { show: !props.spark },
    cursor: {
      show: !props.spark,
      drag: { x: false, y: false, setScale: false },
      points: { show: false },
    },
    plugins: [
      {
        hooks: {
          setCursor: [
            (u: uPlot) => {
              buildTooltip(u)
            },
          ],
        },
      },
    ],
  }
}

function alignedData(): uPlot.AlignedData {
  return [props.timestamps, ...props.series.map((s) => s.points)] as unknown as uPlot.AlignedData
}

let up: uPlot | null = null
let ro: ResizeObserver | null = null

function rebuild(): void {
  up?.destroy()
  up = null
  if (!wrapEl.value) return
  if (props.timestamps.length === 0) return
  up = new uPlot(buildOptions(), alignedData(), wrapEl.value)
}

function applyData(): void {
  if (!up) {
    rebuild()
    return
  }
  if (props.timestamps.length === 0) {
    up?.destroy()
    up = null
    return
  }
  const seriesChanged =
    up.series.length !== props.series.length + 1 ||
    props.series.some((s, i) => (up!.series[i + 1].scale ?? 'y') !== (s.yAxis === 2 ? 'y2' : 'y'))
  if (seriesChanged) {
    rebuild()
  } else {
    up.setData(alignedData())
  }
}

onMounted(() => {
  rebuild()
  ro = new ResizeObserver(() => {
    if (up && wrapEl.value) up.setSize({ width: wrapEl.value.clientWidth, height: chartHeight.value })
  })
  if (wrapEl.value) ro.observe(wrapEl.value)
})

onBeforeUnmount(() => {
  ro?.disconnect()
  up?.destroy()
  up = null
})

watch(() => [props.timestamps, props.series], applyData, { deep: true })
watch(() => theme.mode, rebuild) // theme switch → full redraw (§6.6.4-3)
watch(chartHeight, () => up?.setSize({ width: up.width, height: chartHeight.value }))
</script>

<template>
  <div ref="wrapEl" class="metric-chart">
    <div v-if="timestamps.length === 0" class="empty">暂无数据</div>
    <div
      v-show="tipVisible"
      ref="tipEl"
      class="tooltip"
      :style="{ left: `${tipLeft}px` }"
    >
      <div class="tooltip-time">{{ tipTime }}</div>
      <div v-for="row in tipRows" :key="row.label" class="tooltip-row">
        <span class="dot" :style="{ background: row.color }" />
        <span class="label">{{ row.label }}</span>
        <span class="value">{{ row.value }}</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.metric-chart {
  position: relative;
  width: 100%;
}

.empty {
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--tr-text-3);
  font-size: var(--tr-fs-caption);
}

.tooltip {
  position: absolute;
  top: 8px;
  z-index: 2;
  min-width: 120px;
  padding: 6px 8px;
  background: var(--tr-bg-pop);
  border: 1px solid var(--tr-border);
  border-radius: var(--tr-radius-base);
  font-size: var(--tr-fs-caption);
  color: var(--tr-text-1);
  pointer-events: none;
}

.tooltip-time {
  color: var(--tr-text-3);
  font-family: var(--tr-font-mono);
  margin-bottom: 4px;
}

.tooltip-row {
  display: flex;
  align-items: center;
  gap: 6px;
  line-height: 1.6;
}

.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex: none;
}

.label {
  color: var(--tr-text-2);
  margin-right: auto;
}

.value {
  font-family: var(--tr-font-mono);
  font-variant-numeric: tabular-nums;
}
</style>
