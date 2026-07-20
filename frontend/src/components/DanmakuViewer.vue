<template>
  <div class="danmaku-viewer">
    <div class="danmaku-stream" ref="streamRef">
      <transition-group name="dm" tag="div">
        <div v-for="msg in visibleMessages" :key="msg._uid"
             :class="['danmaku-item', 'type-' + (msg.danmakuType || msg.danmaku_type || 'comment')]">
          <span class="dm-time" v-if="msg.eventTime || msg.event_time">{{ msg.eventTime || msg.event_time }}</span>
          <span v-if="getType(msg) === 'gift'" class="dm-badge dm-badge-gift">礼物</span>
          <span v-else-if="getType(msg) === 'like'" class="dm-badge dm-badge-like">点赞</span>
          <span v-else-if="getType(msg) === 'follow'" class="dm-badge dm-badge-follow">关注</span>
          <span v-else-if="getType(msg) === 'enter'" class="dm-badge dm-badge-enter">进场</span>
          <span class="dm-user">{{ msg.userName || msg.user_name || '匿名' }}</span>
          <span class="dm-content">{{ msg.content }}</span>
          <span v-if="getType(msg) === 'gift'" class="dm-gift-icon">🎁</span>
        </div>
      </transition-group>

      <!-- Status messages -->
      <div v-if="!visibleMessages.length && loading" class="dm-status">
        <div class="dm-status-icon">⟳</div>
        加载弹幕数据中...
      </div>
      <div v-if="!visibleMessages.length && !loading && !wsConnected && isLive" class="dm-status dm-status-wait">
        <div class="dm-status-icon">◉</div>
        等待实时弹幕接入中...
      </div>
      <div v-if="!visibleMessages.length && !loading && !isLive && !historyLoaded" class="dm-status dm-status-empty">
        <div class="dm-status-icon">◇</div>
        该直播间暂无弹幕记录
      </div>
      <div v-if="replayDone && visibleMessages.length" class="dm-replay-end">
        — 弹幕回放结束 · 共 {{ visibleMessages.length }} 条 —
      </div>
    </div>

    <!-- Controls -->
    <div class="danmaku-controls">
      <el-input v-model="filter" placeholder="过滤弹幕..." size="small" clearable style="width: 160px" />
      <el-switch v-model="showEnter" active-text="进场" inactive-text="" size="small" style="--el-switch-on-color: #909399" />
      <el-switch v-model="autoScroll" active-text="自动滚动" size="small" />
      <div class="controls-right">
        <el-tag size="small" type="info">{{ visibleMessages.length }} 条</el-tag>
        <el-tag size="small" :type="connectionType">
          {{ connectionLabel }}
        </el-tag>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, nextTick, watch } from 'vue'

const props = defineProps({
  roomId: { type: String, required: true },
  isLive: { type: Boolean, default: false },
  maxMessages: { type: Number, default: 300 },
  wsUrl: { type: String, default: '' }
})

const streamRef = ref(null)
const messages = ref([])
const filter = ref('')
const autoScroll = ref(true)
const showEnter = ref(true)
const loading = ref(true)
const wsConnected = ref(false)
const historyLoaded = ref(false)
const replayDone = ref(false)
let ws = null
let reconnectTimer = null
let reconnectDelay = 1000
let uidCounter = 0
let replayTimer = null

function getType(msg) {
  return msg.danmakuType || msg.danmaku_type || 'comment'
}

function getShortId(fullId) {
  if (!fullId) return ''
  const parts = fullId.split('_')
  if (parts.length >= 3 && parts[0] === 'CRAWL') return parts.slice(2).join('_')
  return fullId
}

const shortRoomId = computed(() => getShortId(props.roomId))
const numericRoomId = computed(() => props.roomId.replace(/\D/g, ''))

const connectionType = computed(() => {
  if (wsConnected.value) return 'success'
  if (historyLoaded.value) return 'info'
  return 'danger'
})

const connectionLabel = computed(() => {
  if (wsConnected.value) return '实时连接'
  if (historyLoaded.value) return '历史数据'
  return '未连接'
})

const visibleMessages = computed(() => {
  let msgs = messages.value
  if (!showEnter.value) msgs = msgs.filter(m => getType(m) !== 'enter')
  if (filter.value) {
    const kw = filter.value.toLowerCase()
    msgs = msgs.filter(m =>
      (m.content || '').toLowerCase().includes(kw) ||
      (m.userName || m.user_name || '').toLowerCase().includes(kw)
    )
  }
  return msgs
})

function isMessageForThisRoom(data) {
  if (!data.room_id) return true
  const msgRoom = String(data.room_id)
  return msgRoom === shortRoomId.value || msgRoom === numericRoomId.value || msgRoom === props.roomId
}

function addMessage(msg) {
  msg._uid = ++uidCounter
  messages.value.push(msg)
  if (messages.value.length > props.maxMessages) {
    messages.value.splice(0, messages.value.length - props.maxMessages)
  }
  if (autoScroll.value) {
    nextTick(() => {
      if (streamRef.value) streamRef.value.scrollTop = streamRef.value.scrollHeight
    })
  }
}

// HTTP: load historical danmaku
async function loadHistoryDanmaku() {
  loading.value = true
  const ids = [props.roomId]
  const short = shortRoomId.value
  if (short && short !== props.roomId) ids.push(short)

  for (const id of ids) {
    try {
      const res = await fetch(`/api/live/room/${encodeURIComponent(id)}/danmaku?limit=200`)
      const data = await res.json()
      if (data.code === 0 && data.data && data.data.length > 0) {
        // In history/replay mode, add messages gradually
        if (props.isLive) {
          // Live mode: load all at once as background
          data.data.forEach(d => addMessage(d))
        } else {
          // History mode: replay gradually
          await replayMessages(data.data)
        }
        historyLoaded.value = true
        loading.value = false
        return
      }
    } catch (e) {
      console.log('[DanmakuViewer] HTTP load failed for', id, e)
    }
  }
  loading.value = false
}

// Gradually show historical messages
async function replayMessages(msgs) {
  const batchSize = 5
  const interval = 80 // ms between batches
  for (let i = 0; i < msgs.length; i += batchSize) {
    const batch = msgs.slice(i, i + batchSize)
    batch.forEach(m => addMessage(m))
    if (i + batchSize < msgs.length) {
      await new Promise(r => setTimeout(r, interval))
    }
  }
  replayDone.value = true
}

// WebSocket connection (for live rooms)
function connectWebSocket() {
  if (!props.isLive) return // Don't connect WS for ended rooms

  const shortId = shortRoomId.value
  const url = props.wsUrl || (shortId
    ? `ws://localhost:8765/danmaku/${shortId}`
    : `ws://localhost:8765/danmaku/all`)

  try {
    ws = new WebSocket(url)
    ws.onopen = () => {
      wsConnected.value = true
      reconnectDelay = 1000
    }
    ws.onmessage = (event) => {
      let data
      try { data = JSON.parse(event.data) } catch { return }
      if (data.type === 'connected' || data.type === 'pong' || data.type === 'room_update') return
      if (!isMessageForThisRoom(data)) return
      addMessage(data)
    }
    ws.onclose = () => {
      wsConnected.value = false
      scheduleReconnect()
    }
    ws.onerror = () => {
      console.error('[DanmakuWS] error')
    }
  } catch {
    scheduleReconnect()
  }
}

function scheduleReconnect() {
  if (!props.isLive) return
  if (reconnectTimer) clearTimeout(reconnectTimer)
  reconnectTimer = setTimeout(() => {
    reconnectDelay = Math.min(reconnectDelay * 1.5, 10000)
    connectWebSocket()
  }, reconnectDelay)
}

let pingTimer
onMounted(() => {
  loadHistoryDanmaku()
  if (props.isLive) {
    connectWebSocket()
    pingTimer = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' }))
    }, 25000)
  }
})

onBeforeUnmount(() => {
  if (ws) ws.close()
  if (reconnectTimer) clearTimeout(reconnectTimer)
  if (pingTimer) clearInterval(pingTimer)
  if (replayTimer) clearInterval(replayTimer)
})

defineExpose({ addMessage, messages })
</script>

<style scoped>
.danmaku-viewer { display: flex; flex-direction: column; height: 100%; background: rgba(10,12,20,0.8); border-radius: 8px; border: 1px solid rgba(255,255,255,0.06); }
.danmaku-stream { flex: 1; overflow-y: auto; padding: 8px 12px; scroll-behavior: smooth; }
.danmaku-stream::-webkit-scrollbar { width: 4px; }
.danmaku-stream::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 2px; }

.danmaku-item { padding: 4px 0; font-size: 13px; line-height: 1.6; border-bottom: 1px solid rgba(255,255,255,0.03); display: flex; align-items: baseline; gap: 4px; flex-wrap: wrap; }
.dm-time { font-size: 10px; color: rgba(255,255,255,0.15); font-family: 'Courier New', monospace; flex-shrink: 0; }
.dm-user { color: #00d9ff; font-weight: 500; font-size: 12px; }
.dm-content { color: rgba(255,255,255,0.8); }
.dm-gift-icon { margin-left: 2px; }

.type-gift .dm-content { color: #ffa502; }
.type-enter { opacity: 0.5; font-size: 11px; }
.type-enter .dm-content { color: rgba(255,255,255,0.4); font-style: italic; }
.type-like .dm-content { color: #ff6b81; }
.type-follow .dm-content { color: #a855f7; }

.dm-badge { display: inline-block; font-size: 10px; padding: 0 4px; border-radius: 3px; font-weight: 600; line-height: 18px; flex-shrink: 0; }
.dm-badge-gift { background: rgba(255,165,2,0.2); color: #ffa502; }
.dm-badge-like { background: rgba(255,107,129,0.2); color: #ff6b81; }
.dm-badge-follow { background: rgba(168,85,247,0.2); color: #a855f7; }
.dm-badge-enter { background: rgba(255,255,255,0.08); color: rgba(255,255,255,0.4); }

/* Status messages */
.dm-status { text-align: center; padding: 60px 0; color: rgba(255,255,255,0.25); font-size: 13px; }
.dm-status-icon { font-size: 24px; margin-bottom: 12px; opacity: 0.3; animation: spin 2s linear infinite; }
.dm-status-wait .dm-status-icon { color: #00ffcc; animation: pulse 2s infinite; }
.dm-status-empty .dm-status-icon { animation: none; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
@keyframes pulse { 0%,100% { opacity: 0.3; } 50% { opacity: 0.8; } }

.dm-replay-end { text-align: center; padding: 12px 0; color: rgba(255,255,255,0.2); font-size: 11px; border-top: 1px solid rgba(255,255,255,0.04); margin-top: 8px; }

/* Controls */
.danmaku-controls { display: flex; gap: 6px; align-items: center; padding: 8px 10px; border-top: 1px solid rgba(255,255,255,0.06); flex-wrap: wrap; }
.controls-right { display: flex; gap: 4px; margin-left: auto; }

/* Transition */
.dm-enter-active { animation: slideIn 0.3s ease; }
@keyframes slideIn { from { opacity: 0; transform: translateX(-10px); } to { opacity: 1; transform: translateX(0); } }
</style>
