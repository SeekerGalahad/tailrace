/**
 * Design tokens — single source of truth (design doc §6.6.1/§6.6.2/§6.6.3).
 *
 * Consumers:
 *  - stores/theme.ts  → naive-ui themeOverrides + --tr-* CSS vars + <html data-theme>
 *  - custom components → import constants here or read --tr-* vars.
 *    Hard-coding hex values inside components/ is forbidden (§6.6.4-2).
 */

export type ThemeMode = 'dark' | 'light'

/** Brand + semantic colors (§6.6.1). Teal base #14B8A6, three states per mode. */
export const brand: Record<ThemeMode, BrandPalette> = {
  dark: {
    primaryColor: '#14B8A6',
    primaryColorHover: '#2DD4BF',
    primaryColorPressed: '#0FA593',
    primaryColorSuppl: '#2DD4BF',
  },
  light: {
    primaryColor: '#14B8A6',
    primaryColorHover: '#0D9488',
    primaryColorPressed: '#0B7C6F',
    primaryColorSuppl: '#0D9488',
  },
}

export interface BrandPalette {
  primaryColor: string
  primaryColorHover: string
  primaryColorPressed: string
  primaryColorSuppl: string
}

export const semantic: Record<ThemeMode, SemanticPalette> = {
  dark: {
    infoColor: '#818CF8',
    successColor: '#34D399',
    warningColor: '#FBBF24',
    errorColor: '#F87171',
  },
  light: {
    infoColor: '#6366F1',
    successColor: '#10B981',
    warningColor: '#F59E0B',
    errorColor: '#EF4444',
  },
}

export interface SemanticPalette {
  infoColor: string
  successColor: string
  warningColor: string
  errorColor: string
}

/** Neutral surfaces (§6.6.1) — also published as --tr-* CSS vars. */
export const neutral: Record<ThemeMode, Record<NeutralToken, string>> = {
  dark: {
    'bg-page': '#0B0F14',
    'bg-card': '#111820',
    'bg-pop': '#18222E',
    border: '#22303F',
    'text-1': '#E7EDF3',
    'text-2': '#93A1B0',
    'text-3': '#5D6B7A',
  },
  light: {
    'bg-page': '#F4F6F9',
    'bg-card': '#FFFFFF',
    'bg-pop': '#FFFFFF',
    border: '#E3E8EF',
    'text-1': '#1B2530',
    'text-2': '#5A6878',
    'text-3': '#98A4B3',
  },
}

export type NeutralToken =
  | 'bg-page'
  | 'bg-card'
  | 'bg-pop'
  | 'border'
  | 'text-1'
  | 'text-2'
  | 'text-3'

/** Terminal panel stays dark in both modes — brand element (§6.6.1/§6.6.4-3). */
export const terminal = { 'bg-term': '#0A0E13', 'text-term': '#C9D4DF' } as const
export type TerminalToken = keyof typeof terminal

/** Chart series colors, max 4 series per chart (§6.6.1). */
export const chartSeries: readonly string[] = ['#14B8A6', '#6366F1', '#F59E0B', '#EF4444']

/** Chart grid line color per mode (§6.6.1). */
export const chartGrid: Record<ThemeMode, string> = {
  dark: 'rgba(231,237,243,.08)',
  light: 'rgba(27,37,48,.08)',
}

/** Datasource / table-object kind colors (§6.6.1, kinds per §4.2/§4.5). */
export const kindColors: Record<Kind, string> = {
  kafka: '#8B5CF6',
  doris: '#6366F1',
  starrocks: '#6366F1',
  file: '#22D3EE',
  jdbc: '#38BDF8',
  mqtt: '#F472B6',
  s3: '#A3E635',
}

export type Kind = 'kafka' | 'file' | 'doris' | 'starrocks' | 'jdbc' | 'mqtt' | 's3'

/** Font stacks — system only, no webfonts (§6.6.2). */
export const fontUi =
  '-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", "Noto Sans CJK SC", sans-serif'
export const fontMono =
  'ui-monospace, "SF Mono", "Cascadia Code", Menlo, Consolas, "Liberation Mono", monospace'

/** Type scale (§6.6.2): size / line-height / weight. Global default body-sm=13px. */
export const typeScale = {
  display: { size: '28px', lineHeight: '1.1', weight: 600 },
  'title-lg': { size: '20px', lineHeight: '1.3', weight: 600 },
  title: { size: '16px', lineHeight: '1.3', weight: 500 },
  body: { size: '14px', lineHeight: '1.6', weight: 400 },
  'body-sm': { size: '13px', lineHeight: '1.6', weight: 400 },
  caption: { size: '12px', lineHeight: '1.5', weight: 400 },
} as const
export type TypeScaleToken = keyof typeof typeScale

/** Radius ladder (§6.6.3) — controls globally 3px via themeOverrides. */
export const radius = { sharp: '2px', base: '3px', code: '4px', card: '6px', round: '8px' } as const

/** localStorage keys. */
export const LS_THEME = 'tr_theme'
export const LS_LAST_USER = 'tr_last_user'

/** `#RRGGBB` + alpha → `rgba()` string (chart gradients, tag washes). */
export function withAlpha(hex: string, alpha: number): string {
  const m = /^#([0-9a-fA-F]{6})$/.exec(hex)
  if (!m) return hex
  const n = parseInt(m[1], 16)
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`
}
