<template>
  <div class="realtime">
    <div class="header-strip">
      <div><h2>┃ 实时直播</h2></div>
      <button class="refresh-btn" @click="fetchData">⟳ 刷新</button>
    </div>

    <div class="kpi-row">
      <div class="kpi-card" v-for="k in kpis" :key="k.label" :style="{ borderColor: k.color }">
        <div class="kpi-glow" :style="{ boxShadow: `0 0 24px ${k.color}22` }"></div>
        <p class="kpi-label">{{ k.label }}</p>
        <p class="kpi-value">{{ k.value }}</p>
        <p class="kpi-sub">{{ k.sub }}</p>
      </div>
    </div>

    <div class="section-card">
      <div class="section-header">
        <h3>直播中 ({{ liveRooms.length }} 个)</h3>
        <div class="sort-btns">
          <button :class="{ active: sortBy === 'viewer_count' }" @click="sortBy = 'viewer_count'">观众</button>
          <button :class="{ active: sortBy === 'gmv' }" @click="sortBy = 'gmv'">GMV</button>
          <button :class="{ active: sortBy === 'order_count' }" @click="sortBy = 'order_count'">订单</button>
        </div>
      </div>

      <div class="room-grid" v-if="sortedRooms.length > 0">
        <div class="room-card" v-for="r in sortedRooms" :key="r.roomId || r.id" @click="goRoomDetail(r)" style="cursor:pointer" :style="{ borderLeft: '3px solid #00d9ff' }">
          <div class="room-top">
            <span class="room-tag live-tag">LIVE</span>
          </div>
          <h4 class="room-name">{{ r.roomName }}</h4>
          <p class="room-anchor">{{ r.anchorName }}</p>
          <div v-if="r.liveUrl" class="room-link" @click.stop="openLiveUrl(r.liveUrl)" style="font-size:10px;color:#00ffcc;cursor:pointer;margin:2px 0">🔗 跳转直播间</div>
          <div class="room-stats">
            <div><span>观众</span><strong>{{ formatNum(r.viewerCount) }}</strong></div>
            <div><span>订单</span><strong>{{ formatNum(r.orderCount) }}</strong></div>
            <div><span>GMV</span><strong>￥{{ formatNum(r.gmv) }}</strong></div>
          </div>
        </div>
      </div>
      <div v-else class="empty-state">
        <p>暂无直播中的带货直播间</p>
        <p class="empty-hint">请在带货高峰时段 (19:00-23:00) 运行爬虫刷新数据</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { useRouter } from 'vue-router'
import { getRealtimeRooms, getRoomPage } from '@/api'

const router = useRouter()
const goRoomDetail = (r) => { router.push(`/live-room/${r.roomId || r.id}`) }
const openLiveUrl = (url) => { window.open(url, '_blank') }

const liveRooms = ref([])
const sortBy = ref('viewer_count')
const kpis = ref([
  { label: '直播中', value: '...', sub: '活跃房间', color: '#00ffcc' },
  { label: '总观众', value: '...', sub: '累计在线', color: '#00d9ff' },
  { label: '总GMV', value: '...', sub: '实时统计', color: '#a855f7' },
  { label: '订单数', value: '...', sub: '本时段', color: '#ffa502' }
])

const sortedRooms = computed(() => [...liveRooms.value].sort((a, b) => Number(b[sortBy.value] || 0) - Number(a[sortBy.value] || 0)))

const formatNum = (n) => {
  const v = Number(n || 0)
  if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'
  if (v >= 1e4) return (v / 1e4).toFixed(1) + '万'
  return v.toLocaleString()
}

const getColor = () => '#00d9ff'

let timer

async function fetchData() {
  try {
    // 先尝试从 rt_room_stats 获取真实爬虫数据
    let rooms = []
    try {
      const rtRes = await getRealtimeRooms()
      if (rtRes?.code === 0 && rtRes?.data?.length > 0) {
        rooms = rtRes.data.map(r => ({
          id: r.roomId,
          roomName: r.roomName,
          anchorName: r.anchorName,
          category: r.category,
          status: r.status,
          viewerCount: r.viewerCount,
          orderCount: r.totalOrders,
          gmv: r.totalGmv,
          liveUrl: r.liveUrl,
          roomId: r.roomId
        }))
      }
    } catch {}

    // Fallback: 从 live_room 表获取
    if (rooms.length === 0) {
      const res = await getRoomPage({ page: 1, pageSize: 200 })
      const allRooms = res?.data?.records || []
      rooms = allRooms.filter(r => r.status === 'live' && r.liveUrl).map(r => ({ ...r }))
    }

    liveRooms.value = rooms
    const viewers = rooms.reduce((s, r) => s + Number(r.viewerCount || 0), 0)
    const gmv = rooms.reduce((s, r) => s + Number(r.gmv || 0), 0)
    const orders = rooms.reduce((s, r) => s + Number(r.orderCount || 0), 0)

    kpis.value[0].value = rooms.length
    kpis.value[1].value = formatNum(viewers)
    kpis.value[2].value = '￥' + formatNum(gmv)
    kpis.value[3].value = formatNum(orders)
  } catch {}
}

onMounted(() => { fetchData(); timer = setInterval(fetchData, 3000) })
onBeforeUnmount(() => clearInterval(timer))
</script>

<style scoped>
.realtime { display: flex; flex-direction: column; gap: 18px; height: 100%; overflow-y: auto; padding-bottom: 20px; }

.header-strip { display: flex; justify-content: space-between; align-items: flex-start; }
.header-strip h2 { font-size: 20px; font-weight: 700; color: #e0e0e0; margin: 0; }
.header-strip p { font-size: 12px; color: rgba(255,255,255,0.3); margin: 2px 0 0; }
.refresh-btn { background: rgba(0,255,204,0.08); color: #00ffcc; border: 1px solid rgba(0,255,204,0.2); padding: 5px 14px; border-radius: 5px; cursor: pointer; font-size: 12px; }

.kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
.kpi-card { background: rgba(15,20,30,0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; padding: 16px; position: relative; overflow: hidden; }
.kpi-glow { position: absolute; inset: 0; pointer-events: none; }
.kpi-label { font-size: 11px; color: rgba(255,255,255,0.35); letter-spacing: 1px; }
.kpi-value { font-size: 26px; font-weight: 700; color: #f0f0f0; margin: 6px 0; font-family: 'Courier New', monospace; }
.kpi-sub { font-size: 10px; color: rgba(255,255,255,0.2); }

.section-card { background: rgba(15,20,30,0.4); border: 1px solid rgba(255,255,255,0.05); border-radius: 10px; padding: 16px; }
.section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; }
.section-header h3 { font-size: 14px; color: rgba(255,255,255,0.6); font-weight: 600; margin: 0; }
.sort-btns { display: flex; gap: 4px; }
.sort-btns button { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08); color: rgba(255,255,255,0.4); padding: 3px 10px; border-radius: 4px; cursor: pointer; font-size: 11px; }
.sort-btns button.active { background: rgba(0,255,204,0.1); border-color: #00ffcc; color: #00ffcc; }

.room-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
.room-card { background: rgba(15,20,30,0.5); padding: 14px; border-radius: 8px; transition: all 0.2s; }
.room-card:hover { transform: translateY(-2px); background: rgba(15,20,30,0.7); }
.room-top { display: flex; gap: 6px; margin-bottom: 6px; }
.room-tag { font-size: 9px; padding: 1px 6px; border-radius: 3px; font-weight: 700; }
.live-tag { background: #ff4757; color: #fff; animation: blink 1.5s infinite; }
@keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }
.room-name { font-size: 13px; color: #e0e0e0; margin: 4px 0; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.room-anchor { font-size: 11px; color: rgba(255,255,255,0.35); margin-bottom: 8px; }
.room-stats { display: flex; gap: 12px; }
.room-stats div { display: flex; flex-direction: column; }
.room-stats span { font-size: 9px; color: rgba(255,255,255,0.25); }
.room-stats strong { font-size: 13px; color: rgba(255,255,255,0.7); font-family: 'Courier New', monospace; }
.empty-state { text-align: center; padding: 40px 20px; }
.empty-state p { color: rgba(255,255,255,0.4); font-size: 14px; margin: 4px 0; }
.empty-state .empty-hint { font-size: 11px; color: rgba(255,255,255,0.2); }
</style>
