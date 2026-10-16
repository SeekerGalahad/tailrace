<script setup lang="ts">
import { computed } from 'vue'
import {
  NConfigProvider,
  NDialogProvider,
  NMessageProvider,
  darkTheme,
  dateZhCN,
  zhCN,
  type GlobalTheme,
} from 'naive-ui'
import { useThemeStore } from './stores/theme'

// Dark-first console shell (§6.0b-1/§6.6.4): store drives darkTheme|lightTheme.
const theme = useThemeStore()
const naiveTheme = computed<GlobalTheme | null>(() => (theme.mode === 'dark' ? darkTheme : null))
</script>

<template>
  <NConfigProvider
    :theme="naiveTheme"
    :theme-overrides="theme.overrides"
    :locale="zhCN"
    :date-locale="dateZhCN"
  >
    <NMessageProvider>
      <NDialogProvider>
        <RouterView />
      </NDialogProvider>
    </NMessageProvider>
  </NConfigProvider>
</template>
