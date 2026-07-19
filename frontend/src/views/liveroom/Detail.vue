<template>
  <div class="room-detail">
    <div class="detail-header">
      <div class="header-left">
        <el-button text @click="$router.back()" style="color:#aaa">← 返回</el-button>
        <h2>{{ room.roomName || '直播间详情' }}</h2>
        <el-tag :type="room.status === 'live' ? 'success' : 'danger'" size="small">
          {{ room.status === 'live' ? '直播中' : '已结束' }}
        </el-tag>
      </div>
      <div class="header-right">
        <el-button v-if="room.liveUrl" type="danger" size="small" @click="jumpToLive">
          🔴 跳转直播间
        </el-button>
      </div>
    </div>

    <div class="detail-body">
      <!-- 左侧: 直播间信息 + 商品 -->
      <div class="detail-left">
        <div class="info-card">
          <div class="info-grid">
            <div class="info-item"><span>主播</span><strong>{{ room.anchorName }}</strong></div>
            <div class="info-item"><span>类目</span><strong>{{ room.category || '-' }}</strong></div>
            <div class="info-item"><span>在线</span><strong class="highlight">{{ formatNum(room.viewerCount) }}</strong></div>
            <div class="info-item"><span>峰值</span><strong>{{ formatNum(room.peakViewers) }}</strong></div>
            <div class="info-item"><span>订单</span><strong>{{ formatNum(room.totalOrders) }}</strong></div>
            <div class="info-item"><span>GMV</span><strong class="gmv">￥{{ formatNum(room.totalGmv) }}</strong></div>
          </div>
        </div>

        <div class="products-card">
          <h3>商品货架 ({{ products.length }})</h3>
          <div class="product-list" v-if="products.length">
            <div class="product-item" v-for="p in products" :key="p.productId">
              <img v-if="p.imageUrl" :src="p.imageUrl" class="product-img" />
              <div v-else class="product-img placeholder">📦</div>
              <div class="product-info">
                <p class="product-name">{{ p.productName }}</p>
                <div class="product-meta">
                  <span class="price">￥{{ p.price }}</span>
                  <span v-if="p.originalPrice && p.originalPrice > p.price" class="original-price">￥{{ p.originalPrice }}</span>
                  <span class="sales">已售 {{ formatNum(p.sales) }}</span>
                </div>
              </div>
              <el-button v-if="p.productUrl" text type="primary" size="small" @click="openUrl(p.productUrl)">购买</el-button>
            </div>
          </div>
          <el-empty v-else description="暂无商品数据" :image-size="60" />
        </div>
      </div>

      <!-- 右侧: 弹幕 -->
      <div class="detail-right">
        <div class="danmaku-card">
          <h3>实时弹幕</h3>
          <DanmakuViewer :room-id="roomId" style="height: calc(100vh - 280px)" />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import DanmakuViewer from '@/components/DanmakuViewer.vue'
import { getRealtimeRooms, getRoomPage, getRoomProducts } from '@/api'

const route = useRoute()
const router = useRouter()
const roomId = ref(route.params.roomId || '')
const room = ref({})
const products = ref([])

const formatNum = (n) => {
  const v = Number(n || 0)
  if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'
  if (v >= 1e4) return (v / 1e4).toFixed(1) + '万'
  return v.toLocaleString()
}

const jumpToLive = () => {
  if (room.value.liveUrl) {
    window.open(room.value.liveUrl, '_blank')
  }
}

const openUrl = (url) => window.open(url, '_blank')

async function loadRoomInfo() {
  try {
    // Extract numeric ID from route param (e.g., CRAWL_DOUYIN_123456789 -> 123456789)
    const parts = roomId.value.split('_')
    const shortId = parts.length >= 3 ? parts.slice(2).join('_') : roomId.value
    const numericId = roomId.value.replace(/\D/g, '')

    // Try rt_room_stats first (real data)
    const res = await getRealtimeRooms()
    if (res?.code === 0 && res?.data) {
      const found = res.data.find(r =>
        r.roomId === roomId.value || r.roomId === shortId || r.roomId === numericId
      )
      if (found) {
        room.value = {
          ...found,
          roomName: found.roomName,
          anchorName: found.anchorName,
          viewerCount: found.viewerCount,
          peakViewers: found.peakViewers,
          totalOrders: found.totalOrders,
          totalGmv: found.totalGmv,
          liveUrl: found.liveUrl,
          category: found.category,
          status: found.status
        }
        return
      }
    }
    // Fallback: live_room table
    const res2 = await getRoomPage({ search: roomId.value })
    if (res2?.data?.records?.length) {
      room.value = res2.data.records[0]
    }
  } catch {}
}

async function loadProducts() {
  try {
    const res = await getRoomProducts(roomId.value)
    if (res?.code === 0) {
      products.value = res.data || []
    }
  } catch {}
}

let refreshTimer
onMounted(() => {
  loadRoomInfo()
  loadProducts()
  refreshTimer = setInterval(() => {
    loadRoomInfo()
    loadProducts()
  }, 10000)
})

onBeforeUnmount(() => clearInterval(refreshTimer))
</script>

<style scoped>
.room-detail { display: flex; flex-direction: column; height: 100%; gap: 12px; }
.detail-header { display: flex; justify-content: space-between; align-items: center; padding: 0 4px; }
.header-left { display: flex; align-items: center; gap: 12px; }
.header-left h2 { font-size: 18px; color: #e0e0e0; margin: 0; }
.header-right { display: flex; align-items: center; gap: 8px; }
.detail-body { display: flex; gap: 16px; flex: 1; min-height: 0; }
.detail-left { width: 45%; display: flex; flex-direction: column; gap: 12px; overflow-y: auto; }
.detail-right { flex: 1; display: flex; flex-direction: column; }
.info-card, .products-card, .danmaku-card { background: rgba(15,20,30,0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; padding: 16px; }
.info-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
.info-item { display: flex; flex-direction: column; }
.info-item span { font-size: 11px; color: rgba(255,255,255,0.35); }
.info-item strong { font-size: 18px; color: #f0f0f0; font-family: 'Courier New', monospace; }
.info-item .highlight { color: #00ffcc; }
.info-item .gmv { color: #ffa502; }
.products-card h3, .danmaku-card h3 { font-size: 14px; color: rgba(255,255,255,0.6); margin: 0 0 12px; }
.product-list { display: flex; flex-direction: column; gap: 8px; max-height: 300px; overflow-y: auto; }
.product-item { display: flex; align-items: center; gap: 10px; padding: 8px; background: rgba(255,255,255,0.03); border-radius: 6px; }
.product-img { width: 48px; height: 48px; border-radius: 4px; object-fit: cover; }
.product-img.placeholder { display: flex; align-items: center; justify-content: center; background: rgba(255,255,255,0.05); font-size: 20px; }
.product-info { flex: 1; }
.product-name { font-size: 13px; color: #e0e0e0; margin: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.product-meta { display: flex; gap: 8px; align-items: center; margin-top: 4px; }
.price { color: #ff4757; font-weight: 700; font-size: 14px; }
.original-price { color: rgba(255,255,255,0.3); text-decoration: line-through; font-size: 11px; }
.sales { color: rgba(255,255,255,0.4); font-size: 11px; }
.danmaku-card { display: flex; flex-direction: column; flex: 1; }
</style>
