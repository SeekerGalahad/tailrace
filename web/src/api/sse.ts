/**
 * SSE channel wrapper (design doc §4.0/§4.5).
 *
 * Uses the native EventSource — the session cookie `tr_session` is the only
 * credential channel that reaches it (EventSource cannot set headers; §4.0).
 * Native behavior retained on purpose:
 *  - network errors → browser retries automatically (this includes the server
 *    closing after `event: eof`: LogTerminal surfaces the state, the reconnect
 *    that follows is the documented "eof 重连", §6.6.6);
 *  - `: keepalive` comment lines are ignored by EventSource (§4.5).
 *
 * Visibility pausing is the caller's policy (LogTerminal), via open()/close().
 */

export type SseStatus = 'connecting' | 'open' | 'closed'

export interface SseOptions {
  /** Map of `event:` names → handlers. Data is JSON.parse'd when possible. */
  events: Record<string, (data: unknown, raw: MessageEvent) => void>
  /** Fires on every status transition. */
  onStatus?: (status: SseStatus) => void
}

export interface SseChannel {
  open(): void
  close(): void
  readonly status: SseStatus
}

export function createSseChannel(url: string, options: SseOptions): SseChannel {
  const { events, onStatus } = options
  let source: EventSource | null = null
  let status: SseStatus = 'closed'
  const listeners: Array<() => void> = []

  function setStatus(next: SseStatus): void {
    if (status === next) return
    status = next
    onStatus?.(next)
  }

  function bind(es: EventSource): void {
    es.onopen = () => setStatus('open')
    // Native EventSource retries on error; surface as 'connecting'. A terminal
    // close (our own close()) reports 'closed' before the source is dropped.
    es.onerror = () => {
      if (source === es && es.readyState === EventSource.CLOSED) {
        setStatus('closed')
      } else {
        setStatus('connecting')
      }
    }
    for (const name of Object.keys(events)) {
      const h = (raw: MessageEvent) => {
        let data: unknown = raw.data
        if (typeof raw.data === 'string') {
          try {
            data = JSON.parse(raw.data)
          } catch {
            /* keep raw string */
          }
        }
        events[name](data, raw)
      }
      es.addEventListener(name, h as EventListener)
      listeners.push(() => es.removeEventListener(name, h as EventListener))
    }
  }

  function open(): void {
    if (source) return
    setStatus('connecting')
    const es = new EventSource(url)
    source = es
    bind(es)
  }

  function close(): void {
    for (const off of listeners.splice(0)) off()
    if (source) {
      const es = source
      source = null
      es.close()
    }
    setStatus('closed')
  }

  return {
    open,
    close,
    get status() {
      return status
    },
  }
}
