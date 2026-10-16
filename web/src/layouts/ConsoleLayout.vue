<script setup lang="ts">
import { computed, h, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  NButton,
  NDropdown,
  NIcon,
  NLayout,
  NLayoutHeader,
  NLayoutSider,
  NMenu,
  NText,
  type MenuOption,
} from 'naive-ui'
import {
  GitNetworkOutline,
  HardwareChipOutline,
  LogOutOutline,
  MoonOutline,
  ServerOutline,
  SettingsOutline,
  SpeedometerOutline,
  SunnyOutline,
} from '@vicons/ionicons5'
import { useAuthStore } from '../stores/auth'
import { useThemeStore } from '../stores/theme'

/**
 * Console layout (design doc §6.0): left single-level sidebar (four entries +
 * bottom Settings, P2) + top bar carrying the session user and logout.
 */

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()
const theme = useThemeStore()

function icon(Comp: typeof SpeedometerOutline) {
  return () => h(NIcon, null, { default: () => h(Comp) })
}

const activeKey = computed(() => (route.name as string) ?? '')

const menuOptions: MenuOption[] = [
  { label: '总览', key: 'overview', icon: icon(SpeedometerOutline) },
  { label: '数据源', key: 'datasources', icon: icon(ServerOutline) },
  { label: '引擎节点', key: 'nodes', icon: icon(HardwareChipOutline) },
  { label: '数据管道', key: 'pipelines', icon: icon(GitNetworkOutline) },
]

function onMenuSelect(key: string): void {
  router.push({ name: key })
}

// Session user menu (top bar, §6.1 logout entry).
const userMenuOptions = [
  { label: '登出', key: 'logout', icon: icon(LogOutOutline) },
] satisfies MenuOption[]

const loggingOut = ref(false)

async function onUserMenu(key: string): Promise<void> {
  if (key !== 'logout' || loggingOut.value) return
  loggingOut.value = true
  try {
    await auth.logout()
  } finally {
    loggingOut.value = false
    await router.replace({ path: '/login', query: { loggedout: '1' } })
  }
}
</script>

<template>
  <NLayout has-sider class="console-shell">
    <NLayoutSider :width="208" bordered :native-scrollbar="false" content-style="display:flex;flex-direction:column;height:100%">
      <div class="brand">
        <span class="brand-name">tailrace</span>
      </div>
      <NMenu
        :value="activeKey"
        :options="menuOptions"
        :indent="18"
        style="flex: 1"
        @update:value="onMenuSelect"
      />
      <div class="sider-footer">
        <NButton quaternary size="small" disabled class="settings-btn">
          <template #icon>
            <NIcon><SettingsOutline /></NIcon>
          </template>
          设置
        </NButton>
        <NText depth="3" class="p2-note">P2</NText>
      </div>
    </NLayoutSider>
    <NLayout class="main">
      <NLayoutHeader bordered class="topbar">
        <div />
        <div class="topbar-right">
          <NButton quaternary circle size="small" :title="theme.mode === 'dark' ? '切换亮色' : '切换暗色'" @click="theme.toggleMode()">
            <template #icon>
              <NIcon><SunnyOutline v-if="theme.mode === 'dark'" /><MoonOutline v-else /></NIcon>
            </template>
          </NButton>
          <NDropdown :options="userMenuOptions" trigger="click" @select="onUserMenu">
            <NButton quaternary size="small" class="user-btn">
              {{ auth.user?.displayName ?? auth.user?.username ?? '' }}
            </NButton>
          </NDropdown>
        </div>
      </NLayoutHeader>
      <main class="content">
        <RouterView />
      </main>
    </NLayout>
  </NLayout>
</template>

<style scoped>
.console-shell {
  height: 100vh;
}

.brand {
  height: 48px;
  display: flex;
  align-items: center;
  padding: 0 18px;
}

.brand-name {
  font-family: var(--tr-font-mono);
  font-size: var(--tr-fs-title);
  font-weight: 600;
  color: var(--tr-text-1);
}

.sider-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 10px;
  border-top: 1px solid var(--tr-border);
}

.settings-btn {
  flex: 1;
  justify-content: flex-start;
}

.p2-note {
  font-size: var(--tr-fs-caption);
}

.topbar {
  height: 48px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 16px;
}

.topbar-right {
  display: flex;
  align-items: center;
  gap: 8px;
}

.user-btn {
  font-weight: 400;
}

.content {
  padding: 24px;
  min-height: 0;
  overflow: auto;
}
</style>
