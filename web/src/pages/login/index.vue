<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  NAlert,
  NButton,
  NCard,
  NFlex,
  NForm,
  NFormItem,
  NGradientText,
  NInput,
  NText,
  useMessage,
} from 'naive-ui'
import pkg from '../../../package.json'
import { useAuthStore } from '../../stores/auth'
import { ApiError } from '../../api/http'
import { LOCKOUT_SECONDS, MAX_FAILED_ATTEMPTS } from '../../api/auth'
import { LS_LAST_USER } from '../../theme/tokens'
import { sanitizeRedirect } from '../../router'

/**
 * Login page (design doc §6.1). Zero custom components — naive primitives
 * only. No business API on open; submit is the single network call.
 */

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()
const message = useMessage()

const APP_VERSION = pkg.version

// --- banner state from guard/top-bar redirects (§6.1-5) ---
const expired = route.query.expired === '1'
const loggedoutAlert = ref(route.query.loggedout === '1')
let loggedoutTimer: ReturnType<typeof setTimeout> | null = null
if (loggedoutAlert.value) {
  loggedoutTimer = setTimeout(() => (loggedoutAlert.value = false), 5000)
}

// --- form state ---
const username = ref('')
const password = ref('')
const usernameInput = ref<InstanceType<typeof NInput> | null>(null)
const submitting = ref(false)

// Inline error state (§6.1-4): 'bad-credentials' | 'locked' | 'unreachable'.
type LoginError = 'bad-credentials' | 'locked' | 'unreachable' | null
const loginError = ref<LoginError>(null)
const failedAttempts = ref(0)
const lockCountdown = ref(0)
let lockTimer: ReturnType<typeof setInterval> | null = null

const locked = computed(() => lockCountdown.value > 0)
const remainingAttempts = computed(() => MAX_FAILED_ATTEMPTS - failedAttempts.value)

function startLockCountdown(): void {
  lockCountdown.value = LOCKOUT_SECONDS
  if (lockTimer) clearInterval(lockTimer)
  lockTimer = setInterval(() => {
    lockCountdown.value -= 1
    if (lockCountdown.value <= 0) {
      if (lockTimer) clearInterval(lockTimer)
      lockTimer = null
      failedAttempts.value = 0 // display-only; server counter is authoritative
    }
  }, 1000)
}
onBeforeUnmount(() => {
  if (loggedoutTimer) clearTimeout(loggedoutTimer)
  if (lockTimer) clearInterval(lockTimer)
})

// Prefill last username (password is never remembered); focus username (§6.1-1).
onMounted(() => {
  try {
    username.value = localStorage.getItem(LS_LAST_USER) ?? ''
  } catch {
    /* non-fatal */
  }
  usernameInput.value?.focus()
})

async function submit(): Promise<void> {
  if (submitting.value || locked.value) return
  if (!username.value || !password.value) return
  submitting.value = true
  loginError.value = null
  try {
    const user = await auth.login(username.value, password.value)
    message.success(`欢迎，${user.displayName}`)
    await router.replace(sanitizeRedirect(route.query.redirect))
  } catch (e) {
    if (e instanceof ApiError && e.status === 401 && e.code === 'BAD_CREDENTIALS') {
      failedAttempts.value += 1
      if (failedAttempts.value >= MAX_FAILED_ATTEMPTS) {
        loginError.value = 'locked'
        startLockCountdown()
      } else {
        loginError.value = 'bad-credentials'
      }
    } else {
      // Network failure / 5xx — uniform copy (§6.1-4).
      loginError.value = 'unreachable'
    }
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <main class="login-page">
    <NFlex vertical align="center" :size="0" class="login-box">
      <NGradientText :size="28" font-weight="600" class="brand">tailrace</NGradientText>
      <div class="tagline">单二进制数据接入引擎</div>

      <NCard :bordered="false" class="login-card" :content-style="{ padding: '24px' }">
        <NFlex vertical :size="0">
          <NAlert
            v-if="loggedoutAlert"
            type="info"
            :bordered="false"
            closable
            class="banner"
            @close="loggedoutAlert = false"
          >
            已登出
          </NAlert>
          <NAlert v-else-if="expired" type="warning" :bordered="false" class="banner">
            会话已过期，请重新登录
          </NAlert>

          <NAlert v-if="loginError === 'bad-credentials'" type="error" :bordered="false" class="banner">
            用户名或密码错误（还可尝试 {{ remainingAttempts }} 次）
          </NAlert>
          <NAlert v-else-if="loginError === 'locked'" type="warning" :bordered="false" class="banner">
            已锁定，请 {{ lockCountdown }}s 后重试
          </NAlert>
          <NAlert v-else-if="loginError === 'unreachable'" type="error" :bordered="false" class="banner">
            无法连接 tailrace-console，请确认服务已启动
          </NAlert>

          <NForm @submit.prevent="submit">
            <NFormItem label="用户名" label-placement="top" :show-feedback="false">
              <NInput
                ref="usernameInput"
                v-model:value="username"
                placeholder="username"
                :disabled="submitting"
                @keydown.enter.prevent="submit"
              />
            </NFormItem>
            <NFormItem label="密码" label-placement="top" :show-feedback="false" class="password-item">
              <NInput
                v-model:value="password"
                type="password"
                show-password-on="click"
                placeholder="password"
                :disabled="submitting"
                @keydown.enter.prevent="submit"
              />
            </NFormItem>
            <NButton
              type="primary"
              block
              attr-type="submit"
              :loading="submitting"
              :disabled="locked"
              class="submit-btn"
            >
              {{ locked ? `${lockCountdown}s 后重试` : '登 录' }}
            </NButton>
          </NForm>
        </NFlex>
      </NCard>

      <div class="first-boot-hint">首次启动：admin 初始密码已在 tailrace-console 启动日志一次性打印</div>

      <NText depth="3" class="footer">
        tailrace-console v{{ APP_VERSION }} · Apache-2.0 · API 文档 · 使用帮助
      </NText>
    </NFlex>
  </main>
</template>

<style scoped>
.login-page {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background:
    radial-gradient(ellipse at 50% 0%, rgba(20, 184, 166, 0.12), transparent 60%),
    var(--tr-bg-page);
}

.login-box {
  width: 360px;
}

.brand {
  font-family: var(--tr-font-mono);
  margin-bottom: 8px;
}

.tagline {
  color: var(--tr-text-2);
  font-size: var(--tr-fs-body-sm);
  margin-bottom: 24px;
}

.login-card {
  width: 100%;
  background: var(--tr-bg-card);
  border: 1px solid var(--tr-border);
  border-radius: var(--tr-radius-round);
}

.banner {
  margin-bottom: 16px;
}

.password-item {
  margin-bottom: 20px;
}

.submit-btn {
  margin-top: 4px;
}

.first-boot-hint {
  color: var(--tr-text-3);
  font-size: var(--tr-fs-caption);
  margin-top: 16px;
}

.footer {
  font-size: var(--tr-fs-caption);
  margin-top: 8px;
}
</style>
