/** Shared input type for MetricChart (see components/MetricChart.vue). */
export interface ChartSeries {
  label: string
  /** Aligned with the chart's `timestamps` prop; null = gap. */
  points: Array<number | null>
  /** 1 = left axis (default), 2 = right axis. */
  yAxis?: 1 | 2
}
