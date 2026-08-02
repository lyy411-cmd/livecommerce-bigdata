<template>
  <div class="bigscreen">
    <div class="bs-header">
      <h1>星播直播电商数据总览</h1>
      <div class="header-meta">
        <span class="clock">{{ currentTime }}</span>
        <span class="sep">|</span>
        <span class="refresh-hint">每 5 秒自动刷新</span>
      </div>
    </div>

    <div class="kpi-row">
      <div class="bs-kpi" style="--accent:#00ffcc">
        <p class="kpi-label">当前在线观众</p>
        <p class="kpi-val">{{ realtime.currentViewers?.toLocaleString() || 0 }}</p>
        <p class="kpi-sub">实时在线人数</p>
      </div>
      <div class="bs-kpi" style="--accent:#a855f7">
        <p class="kpi-label">实时订单</p>
        <p class="kpi-val">{{ realtime.currentOrders || 0 }}</p>
        <p class="kpi-sub">本小时订单数</p>
      </div>
      <div class="bs-kpi" style="--accent:#ffa502">
        <p class="kpi-label">实时GMV</p>
        <p class="kpi-val">￥{{ formatNumber(realtime.currentGmv) }}</p>
        <p class="kpi-sub">本小时成交额</p>
      </div>
      <div class="bs-kpi" style="--accent:#00d9ff">
        <p class="kpi-label">在线主播</p>
        <p class="kpi-val">{{ realtime.onlineAnchors || 0 }}</p>
        <p class="kpi-sub">正在直播中</p>
      </div>
    </div>

    <div class="content-row">
      <div class="panel chart-panel">
        <div class="panel-title">┃ 弹幕热词</div>
        <div class="hotwords" v-if="hotwords.length > 0">
          <span v-for="(word, idx) in hotwords" :key="idx" class="hw-tag"
            :style="{
              fontSize: (12 + Math.min(14, (word.count || word.heat || 1) * 0.4)) + 'px',
              opacity: 0.5 + Math.min(0.5, (word.count || word.heat || 1) * 0.015)
            }">
            {{ word.word || word.text || word }}
          </span>
        </div>
        <div v-else class="ph-text">暂无热词数据</div>
      </div>
      <div class="panel table-panel">
        <div class="panel-title">┃ 实时直播列表</div>
        <div class="room-table">
          <div class="rt-head">
            <span class="rt-col name">直播间</span>
            <span class="rt-col anchor">主播</span>
            <span class="rt-col num">观众</span>
            <span class="rt-col num">GMV</span>
          </div>
          <div class="rt-body">
            <div class="rt-row" v-for="(r, i) in liveRooms.slice(0, 12)" :key="i">
              <span class="rt-col name">{{ r.roomName }}</span>
              <span class="rt-col anchor">{{ r.anchorName }}</span>
              <span class="rt-col num">{{ formatNumber(r.viewerCount) }}</span>
              <span class="rt-col num gmv">￥{{ formatNumber(r.gmv) }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onBeforeUnmount } from 'vue'
import { getRealtimeData, getLiveRooms, getHotwords } from '@/api'
import { fallback } from '@/utils/fallback'

const realtime = reactive({})
const liveRooms = ref([])
const currentTime = ref('')
const hotwords = ref([])
let timer

const formatNumber = (n) => {
  if (!n) return 0
  const num = Number(n)
  if (num >= 10000) return (num / 10000).toFixed(1) + '万'
  return num.toLocaleString()
}

const updateTime = () => { currentTime.value = new Date().toLocaleTimeString() }

const fetchData = async () => {
  try {
    const [r, rooms, hw] = await Promise.all([
      getRealtimeData().catch(() => fallback.realtimeData()),
      getLiveRooms().catch(() => fallback.liveRooms()),
      getHotwords().catch(() => fallback.hotwords())
    ])
    Object.assign(realtime, r.data)
    liveRooms.value = rooms.data || []
    hotwords.value = hw.data || []
  } catch (e) { console.error(e) }
}

onMounted(() => {
  updateTime()
  fetchData()
  timer = setInterval(() => { updateTime(); fetchData() }, 5000)
})

onBeforeUnmount(() => { clearInterval(timer) })
</script>

<style scoped>
.bigscreen {
  background: #0a0e17;
  color: #e0e0e0; padding: 20px 24px; height: 100%;
  display: flex; flex-direction: column; gap: 16px; overflow-y: auto;
}
.bs-header { text-align: center; padding: 12px 0 4px; }
.bs-header h1 { font-size: 22px; font-weight: 700; color: #e0e0e0; margin: 0;
  text-shadow: 0 0 20px rgba(0,255,204,0.2); letter-spacing: 2px; }
.header-meta { display: flex; justify-content: center; align-items: center; gap: 10px; margin-top: 6px; }
.clock { font-size: 14px; color: #00ffcc; font-family: 'Courier New', monospace; font-weight: 600; }
.sep { color: rgba(255,255,255,0.1); }
.refresh-hint { font-size: 11px; color: rgba(255,255,255,0.25); }

.kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
.bs-kpi {
  background: rgba(15,20,30,0.6); border: 1px solid rgba(255,255,255,0.06);
  border-radius: 10px; padding: 18px 16px; text-align: center; position: relative;
  overflow: hidden; transition: all 0.3s;
}
.bs-kpi::before {
  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
  background: var(--accent); opacity: 0.6;
}
.bs-kpi:hover { border-color: color-mix(in srgb, var(--accent) 30%, transparent); transform: translateY(-2px); }
.kpi-label { font-size: 11px; color: rgba(255,255,255,0.35); letter-spacing: 1px; margin: 0 0 6px; }
.kpi-val { font-size: 28px; font-weight: 700; color: #f0f0f0; margin: 0; font-family: 'Courier New', monospace; }
.kpi-sub { font-size: 10px; color: rgba(255,255,255,0.2); margin: 4px 0 0; }

.content-row { display: grid; grid-template-columns: 1fr 1.5fr; gap: 14px; flex: 1; min-height: 0; }

.panel {
  background: rgba(15,20,30,0.5); border: 1px solid rgba(255,255,255,0.05);
  border-radius: 10px; padding: 16px; display: flex; flex-direction: column;
}
.panel-title { font-size: 13px; font-weight: 600; color: rgba(255,255,255,0.5); margin-bottom: 12px; }

.hotwords { display: flex; flex-wrap: wrap; gap: 10px; align-content: flex-start; flex: 1; overflow-y: auto; }
.hw-tag {
  display: inline-block; padding: 4px 12px;
  background: rgba(0,255,204,0.06); border: 1px solid rgba(0,255,204,0.15);
  border-radius: 16px; color: rgba(0,255,204,0.8); transition: all 0.2s; cursor: default;
}
.hw-tag:hover { background: rgba(0,255,204,0.12); color: #00ffcc; }
.ph-text { color: rgba(255,255,255,0.2); text-align: center; padding: 40px 0; }

.room-table { flex: 1; overflow-y: auto; }
.rt-head {
  display: flex; gap: 8px; padding: 6px 10px; border-bottom: 1px solid rgba(255,255,255,0.06);
  font-size: 10px; color: rgba(255,255,255,0.3); text-transform: uppercase; letter-spacing: 0.5px;
}
.rt-body { display: flex; flex-direction: column; }
.rt-row {
  display: flex; gap: 8px; padding: 8px 10px; border-bottom: 1px solid rgba(255,255,255,0.03);
  font-size: 12px; transition: background 0.2s;
}
.rt-row:hover { background: rgba(0,255,204,0.04); }
.rt-col.name { flex: 2; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: rgba(255,255,255,0.7); }
.rt-col.anchor { flex: 1; color: rgba(255,255,255,0.4); }
.rt-col.num { flex: 1; text-align: right; font-family: 'Courier New', monospace; color: rgba(255,255,255,0.6); }
.rt-col.num.gmv { color: #ffa502; }
</style>
