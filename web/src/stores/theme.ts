import { computed, ref, watchEffect } from 'vue'
import { defineStore } from 'pinia'
import type { GlobalThemeOverrides } from 'naive-ui'
import {
  LS_THEME,
  brand,
  chartGrid,
  chartSeries,
  fontMono,
  fontUi,
  neutral,
  radius,
  semantic,
  terminal,
  typeScale,
  type ThemeMode,
} from '../theme/tokens'

/**
 * Single-source ThemeStore (§6.6.4): drives NConfigProvider theme +
 * overrides, publishes --tr-* CSS vars on <html data-theme>.
 *
 * Dark-first (§6.0b-1): stored choice (localStorage `tr_theme`) wins; first
 * visit falls back to prefers-color-scheme; no live matchMedia listener (v1).
 */
function resolveInitialMode(): ThemeMode {
  try {
    const stored = localStorage.getItem(LS_THEME)
    if (stored === 'dark' || stored === 'light') return stored
  } catch {
    /* localStorage unavailable — fall through */
  }
  if (typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: light)').matches) {
    return 'light'
  }
  return 'dark'
}

export const useThemeStore = defineStore('theme', () => {
  const mode = ref<ThemeMode>(resolveInitialMode())

  function toggleMode(): void {
    mode.value = mode.value === 'dark' ? 'light' : 'dark'
    try {
      localStorage.setItem(LS_THEME, mode.value)
    } catch {
      /* non-fatal */
    }
  }

  const overrides = computed<GlobalThemeOverrides>(() => {
    const n = neutral[mode.value]
    return {
      common: {
        ...brand[mode.value],
        ...semantic[mode.value],
        fontFamily: fontUi,
        fontFamilyMono: fontMono,
        bodyColor: n['bg-page'],
        cardColor: n['bg-card'],
        popoverColor: n['bg-pop'],
        borderRadius: radius.base,
        fontSize: typeScale['body-sm'].size,
      },
    }
  })

  // Publish --tr-* CSS variables + data-theme for custom components.
  watchEffect(() => {
    const root = document.documentElement
    for (const [k, v] of Object.entries(neutral[mode.value])) {
      root.style.setProperty(`--tr-${k}`, v)
    }
    for (const [k, v] of Object.entries(terminal)) {
      root.style.setProperty(`--tr-${k}`, v)
    }
    chartSeries.forEach((c, i) => root.style.setProperty(`--tr-chart-${i + 1}`, c))
    root.style.setProperty('--tr-chart-grid', chartGrid[mode.value])
    root.style.setProperty('--tr-font-ui', fontUi)
    root.style.setProperty('--tr-font-mono', fontMono)
    for (const [name, t] of Object.entries(typeScale)) {
      root.style.setProperty(`--tr-fs-${name}`, t.size)
      root.style.setProperty(`--tr-lh-${name}`, t.lineHeight)
    }
    for (const [name, r] of Object.entries(radius)) {
      root.style.setProperty(`--tr-radius-${name}`, r)
    }
    root.dataset.theme = mode.value
  })

  return { mode, overrides, toggleMode }
})
