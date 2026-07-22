<template>
  <div class="room-detail">
    <!-- Header -->
    <div class="detail-header">
      <div class="header-left">
        <el-button text @click="$router.back()" style="color:#aaa">← 返回</el-button>
        <h2>{{ room.roomName || '直播间详情' }}</h2>
        <el-tag :type="isLive ? 'success' : 'info'" size="small" effect="dark">
          <span v-if="isLive" class="live-pulse"></span>
          {{ isLive ? '直播中' : '已结束' }}
        </el-tag>
        <el-tag v-if="room.hasShoppingCart" type="warning" size="small" effect="plain">小黄车</el-tag>
      </div>
      <div class="header-right">
        <el-tag v-if="dmStats.total" size="small" type="info">
          {{ dmStats.total }} 条弹幕
        </el-tag>
        <el-button v-if="isLive && room.liveUrl" type="danger" size="small" @click="jumpToLive">
          跳转直播间
        </el-button>
      </div>
    </div>

    <div class="detail-body">
      <!-- 左侧: 信息 + 统计 -->
      <div class="detail-left">
        <!-- 基础信息 -->
        <div class="info-card">
          <div class="info-grid">
            <div class="info-item">
              <span>主播</span>
              <strong class="anchor-name">{{ room.anchorName || '-' }}</strong>
            </div>
            <div class="info-item">
              <span>类目</span>
              <strong>{{ room.category || '-' }}</strong>
            </div>
            <div class="info-item">
              <span>在线观众</span>
              <strong class="highlight">{{ formatNum(room.viewerCount) }}</strong>
            </div>
            <div class="info-item">
              <span>峰值在线</span>
              <strong>{{ formatNum(room.peakViewers || room.viewerCount) }}</strong>
            </div>
            <div class="info-item">
              <span>订单数</span>
              <strong>{{ formatNum(room.totalOrders || room.orderCount) }}</strong>
            </div>
            <div class="info-item">
              <span>GMV</span>
              <strong class="gmv">{{ formatGmv(room.totalGmv || room.gmv) }}</strong>
            </div>
          </div>
        </div>

        <!-- 弹幕统计面板 -->
        <div class="stats-card" v-if="dmStats.total">
          <h3>互动数据</h3>
          <div class="stats-grid">
            <div class="stat-item">
              <div class="stat-value highlight">{{ dmStats.total }}</div>
              <div class="stat-label">总弹幕</div>
            </div>
            <div class="stat-item">
              <div class="stat-value">{{ dmStats.msgPerMin }}</div>
              <div class="stat-label">条/分钟</div>
            </div>
            <div class="stat-item">
              <div class="stat-value gift-color">{{ dmStats.gifts }}</div>
              <div class="stat-label">礼物</div>
            </div>
            <div class="stat-item">
              <div class="stat-value like-color">{{ dmStats.likes }}</div>
              <div class="stat-label">点赞</div>
            </div>
            <div class="stat-item">
              <div class="stat-value follow-color">{{ dmStats.follows }}</div>
              <div class="stat-label">关注</div>
            </div>
            <div class="stat-item">
              <div class="stat-value enter-color">{{ dmStats.enters }}</div>
              <div class="stat-label">进场</div>
            </div>
          </div>

          <!-- 类型分布条 -->
          <div class="type-bar" v-if="dmStats.total">
            <div class="bar-segment comment-bar" :style="{ width: typePercent('comments') + '%' }" :title="'评论 ' + typePercent('comments') + '%'"></div>
            <div class="bar-segment gift-bar" :style="{ width: typePercent('gifts') + '%' }" :title="'礼物 ' + typePercent('gifts') + '%'"></div>
            <div class="bar-segment like-bar" :style="{ width: typePercent('likes') + '%' }" :title="'点赞 ' + typePercent('likes') + '%'"></div>
            <div class="bar-segment follow-bar" :style="{ width: typePercent('follows') + '%' }" :title="'关注 ' + typePercent('follows') + '%'"></div>
            <div class="bar-segment enter-bar" :style="{ width: typePercent('enters') + '%' }" :title="'进场 ' + typePercent('enters') + '%'"></div>
          </div>

          <!-- 热门用户 -->
          <div class="top-users" v-if="dmStats.topUsers && dmStats.topUsers.length">
            <span class="top-label">活跃用户</span>
            <div class="user-tags">
              <el-tag v-for="u in dmStats.topUsers" :key="u.name" size="small" type="info" effect="plain" class="user-tag">
                {{ u.name }} <span class="user-count">{{ u.count }}</span>
              </el-tag>
            </div>
          </div>

          <!-- 活跃时段 -->
          <div class="time-range" v-if="dmStats.firstMsg">
            <span>{{ dmStats.firstMsg }} - {{ dmStats.lastMsg }}</span>
            <span class="duration">持续 {{ dmStats.durationMin }} 分钟</span>
          </div>
        </div>

      </div>

      <!-- 右侧: 弹幕 -->
      <div class="detail-right">
        <div class="danmaku-card">
          <div class="danmaku-header">
            <h3>{{ isLive ? '实时弹幕' : '弹幕回放' }}</h3>
            <el-tag v-if="isLive" type="success" size="small" effect="plain">LIVE</el-tag>
            <el-tag v-else type="info" size="small" effect="plain">HISTORY</el-tag>
          </div>
          <DanmakuViewer v-if="room.status" :room-id="roomId" :is-live="isLive" style="flex:1; min-height:0" />
          <div v-else class="dm-loading-placeholder">
            <div class="dm-loading-spinner"></div>
            加载直播间数据...
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { useRoute } from 'vue-router'
import DanmakuViewer from '@/components/DanmakuViewer.vue'
import { getLiveRooms, getRoomPage, getRoomDanmakuStats } from '@/api'

const route = useRoute()
const roomId = ref(route.params.roomId || '')
const room = ref({})
const dmStats = ref({ total: 0, comments: 0, gifts: 0, enters: 0, likes: 0, follows: 0, msgPerMin: 0, topUsers: [], firstMsg: '', lastMsg: '', durationMin: 0 })

const isLive = computed(() => room.value.status === 'live')

const formatNum = (n) => {
  const v = Number(n || 0)
  if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'
  if (v >= 1e4) return (v / 1e4).toFixed(1) + '万'
  return v.toLocaleString()
}

const formatGmv = (n) => {
  const v = Number(n || 0)
  if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'
  if (v >= 1e4) return (v / 1e4).toFixed(1) + '万'
  return v.toLocaleString()
}

const typePercent = (key) => {
  if (!dmStats.value.total) return 0
  return Math.round((dmStats.value[key] || 0) / dmStats.value.total * 100)
}

const jumpToLive = () => {
  if (room.value.liveUrl) window.open(room.value.liveUrl, '_blank')
}

async function loadRoomInfo() {
  try {
    const parts = roomId.value.split('_')
    const shortId = parts.length >= 3 ? parts.slice(2).join('_') : roomId.value

    // Try getLiveRooms first (live_room table with status='live')
    const res = await getLiveRooms()
    if (res?.data) {
      const rooms = Array.isArray(res.data) ? res.data : []
      const found = rooms.find(r =>
        r.roomNo === roomId.value || r.roomIdExternal === shortId || r.roomIdExternal === roomId.value
      )
      if (found) {
        room.value = found
        return
      }
    }
    // Fallback: search live_room table by roomNo
    const res2 = await getRoomPage({ search: roomId.value })
    if (res2?.data?.records?.length) {
      room.value = res2.data.records[0]
    }
  } catch (e) {
    console.error('[Detail] loadRoomInfo error:', e)
  }
}

async function loadDanmakuStats() {
  try {
    const res = await getRoomDanmakuStats(roomId.value)
    if (res?.data) {
      dmStats.value = res.data
    }
  } catch {}
}

let refreshTimer
onMounted(() => {
  loadRoomInfo()
  loadDanmakuStats()
  refreshTimer = setInterval(() => {
    loadRoomInfo()
    if (isLive.value) loadDanmakuStats()
  }, 15000)
})

onBeforeUnmount(() => clearInterval(refreshTimer))
</script>

<style scoped>
.room-detail { display: flex; flex-direction: column; height: 100%; gap: 10px; }
.detail-header { display: flex; justify-content: space-between; align-items: center; padding: 0 4px; }
.header-left { display: flex; align-items: center; gap: 10px; }
.header-left h2 { font-size: 17px; color: #e0e0e0; margin: 0; max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.header-right { display: flex; align-items: center; gap: 8px; }

.live-pulse {
  display: inline-block; width: 6px; height: 6px; border-radius: 50%;
  background: #00ff88; margin-right: 4px; vertical-align: middle;
  animation: pulse 1.5s infinite;
}
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }

.detail-body { display: flex; gap: 14px; flex: 1; min-height: 0; }
.detail-left { width: 44%; display: flex; flex-direction: column; gap: 10px; overflow-y: auto; padding-right: 4px; }
.detail-left::-webkit-scrollbar { width: 3px; }
.detail-left::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 2px; }
.detail-right { flex: 1; display: flex; flex-direction: column; min-height: 0; }

.info-card, .stats-card, .danmaku-card {
  background: rgba(15,20,30,0.5); border: 1px solid rgba(255,255,255,0.06);
  border-radius: 10px; padding: 14px;
}

.info-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
.info-item { display: flex; flex-direction: column; }
.info-item span { font-size: 11px; color: rgba(255,255,255,0.3); }
.info-item strong { font-size: 16px; color: #f0f0f0; font-family: 'Courier New', monospace; margin-top: 2px; }
.info-item .anchor-name { font-family: inherit; font-size: 14px; }
.info-item .highlight { color: #00ffcc; }
.info-item .gmv { color: #ffa502; }

/* Stats panel */
.stats-card h3 { font-size: 13px; color: rgba(255,255,255,0.5); margin: 0 0 10px; font-weight: 500; }
.stats-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 10px; }
.stat-item { text-align: center; padding: 6px 4px; background: rgba(255,255,255,0.02); border-radius: 6px; }
.stat-value { font-size: 18px; font-weight: 700; font-family: 'Courier New', monospace; color: #f0f0f0; }
.stat-value.highlight { color: #00ffcc; }
.stat-value.gift-color { color: #ffa502; }
.stat-value.like-color { color: #ff6b81; }
.stat-value.follow-color { color: #a855f7; }
.stat-value.enter-color { color: #60a5fa; }
.stat-label { font-size: 10px; color: rgba(255,255,255,0.3); margin-top: 2px; }

/* Type distribution bar */
.type-bar { display: flex; height: 6px; border-radius: 3px; overflow: hidden; margin-bottom: 10px; background: rgba(255,255,255,0.03); }
.bar-segment { height: 100%; transition: width 0.5s ease; }
.comment-bar { background: #00ffcc; }
.gift-bar { background: #ffa502; }
.like-bar { background: #ff6b81; }
.follow-bar { background: #a855f7; }
.enter-bar { background: #60a5fa; }

/* Top users */
.top-users { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; flex-wrap: wrap; }
.top-label { font-size: 11px; color: rgba(255,255,255,0.35); white-space: nowrap; }
.user-tags { display: flex; gap: 4px; flex-wrap: wrap; }
.user-tag { font-size: 11px; }
.user-count { color: #00ffcc; margin-left: 2px; font-weight: 600; }

/* Time range */
.time-range { font-size: 11px; color: rgba(255,255,255,0.3); display: flex; justify-content: space-between; }
.duration { color: rgba(255,255,255,0.4); }

/* Danmaku card */
.danmaku-card { display: flex; flex-direction: column; flex: 1; min-height: 0; }
.danmaku-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
.danmaku-header h3 { font-size: 13px; color: rgba(255,255,255,0.5); margin: 0; font-weight: 500; }

/* Loading placeholder */
.dm-loading-placeholder { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: rgba(255,255,255,0.25); font-size: 13px; gap: 12px; }
.dm-loading-spinner { width: 24px; height: 24px; border: 2px solid rgba(255,255,255,0.1); border-top-color: #00ffcc; border-radius: 50%; animation: spin 1s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
</style>
