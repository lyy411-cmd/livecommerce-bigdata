// 短轮询事件订阅 - 每3秒拉取一次
const listeners = new Set()
let timer = null
let isRunning = false
let lastIdx = 0

async function poll() {
  if (!isRunning) return
  try {
    const res = await fetch('/api/events/stream?idx=' + lastIdx, { cache: 'no-store' })
    const text = await res.text()
    const lines = text.split('\n')
    for (const line of lines) {
      if (line.startsWith('data: ')) {
        try {
          const data = JSON.parse(line.slice(6))
          if (data.type === '__idx__') {
            lastIdx = data.idx
          } else {
            listeners.forEach(h => h(data))
          }
        } catch (e) { /* ignore */ }
      }
    }
  } catch (e) { /* ignore */ }
  if (isRunning) timer = setTimeout(poll, 3000)
}

export function subscribeEvents(handler) {
  if (typeof window === 'undefined') return () => {}
  listeners.add(handler)
  if (!timer) {
    isRunning = true
    poll()
  }
  return () => {
    listeners.delete(handler)
    if (listeners.size === 0) {
      isRunning = false
      if (timer) { clearTimeout(timer); timer = null }
    }
  }
}
