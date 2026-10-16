import { onBeforeUnmount, onMounted } from 'vue'

/**
 * useAutoRefresh (design doc §6.6.6): interval + visibility discipline (§6.0).
 *
 * - fires once immediately on mount, then every intervalMs
 * - pauses while document.hidden; on visible: immediate kick + restart
 * - overlap-safe: a slow request never stacks ticks
 * - cleaned up on unmount; a throwing tick never kills the loop (callers own
 *   their error state)
 */

export interface PollingHandle {
  /** Manual immediate run (does not reset the timer). */
  kick(): void
  start(): void
  stop(): void
}

export function useAutoRefresh(fn: () => void | Promise<void>, intervalMs: number): PollingHandle {
  let timer: ReturnType<typeof setInterval> | null = null
  let inFlight = false

  async function run(): Promise<void> {
    if (inFlight) return
    inFlight = true
    try {
      await fn()
    } catch {
      /* swallowed on purpose — pages surface errors via their own state */
    } finally {
      inFlight = false
    }
  }

  function start(): void {
    if (timer != null || document.hidden) return
    timer = setInterval(() => void run(), intervalMs)
  }

  function stop(): void {
    if (timer != null) clearInterval(timer)
    timer = null
  }

  function onVisibility(): void {
    if (document.hidden) {
      stop()
    } else {
      void run()
      start()
    }
  }

  onMounted(() => {
    void run()
    start()
    document.addEventListener('visibilitychange', onVisibility)
  })
  onBeforeUnmount(() => {
    stop()
    document.removeEventListener('visibilitychange', onVisibility)
  })

  return {
    kick: () => void run(),
    start,
    stop,
  }
}
