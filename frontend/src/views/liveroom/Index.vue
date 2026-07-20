<template>
  <div class="liveroom">
    <el-card>
      <div class="search-bar">
        <el-input v-model="query.search" placeholder="搜索直播间/主播名" clearable style="width:220px" @keyup.enter="onSearch" />
        <el-button type="primary" @click="onSearch">搜索</el-button>
        <el-button @click="onRefresh" class="refresh-btn">刷新</el-button>
        <el-button @click="onRotate" :loading="rotating" class="rotate-btn" type="warning" plain size="small">
          模拟直播轮换
        </el-button>
        <el-button @click="onRefreshLive" :loading="refreshing" type="success" plain size="small">
          从抖音刷新直播间
        </el-button>
        <div class="status-summary">
          <el-tag type="success" effect="dark" size="large">
            <span class="live-dot-sm"></span>
            {{ filteredLive.length }} 直播中
          </el-tag>
          <el-tag type="info" size="large" style="margin-left:8px">
            {{ finishedRooms.length }} 已结束
          </el-tag>
          <span v-if="lastUpdate" class="update-time">更新于 {{ lastUpdate }}</span>
        </div>
      </div>

      <el-tabs v-model="activeTab" class="room-tabs" @tab-change="onTabChange">
        <el-tab-pane :label="`正在直播 (${filteredLive.length})`" name="live">
          <el-table
            v-if="filteredLive.length"
            :data="filteredLive"
            stripe
            highlight-current-row
            height="calc(100vh - 340px)"
          >
            <el-table-column prop="roomNo" label="编号" width="100">
              <template #default="{ row }">
                <span style="font-size:11px;color:#888">{{ (row.roomNo || '').replace('CRAWL_DOUYIN_', '#') }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="roomName" label="直播间" min-width="180" show-overflow-tooltip />
            <el-table-column prop="anchorName" label="主播" width="120" />
            <el-table-column prop="category" label="类目" width="80" />
            <el-table-column prop="viewerCount" label="在线" width="90" sortable>
              <template #default="{ row }">
                <span style="color:#00ffcc;font-weight:700">{{ formatNum(row.viewerCount) }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="orderCount" label="订单" width="80" sortable />
            <el-table-column label="GMV" width="110">
              <template #default="{ row }">
                <span style="color:#ffa502">￥{{ formatNum(row.gmv) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="90">
              <template #default>
                <el-tag type="success" size="small" effect="dark">
                  <span class="live-dot-sm"></span>直播中
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="180" fixed="right">
              <template #default="{ row }">
                <el-button text type="primary" size="small" @click="viewDanmaku(row)">弹幕</el-button>
                <el-button text type="success" size="small" @click="jumpToLive(row)">跳转直播间</el-button>
              </template>
            </el-table-column>
          </el-table>
          <el-empty v-else description="暂无直播中的带货直播间，等待爬虫发现新房间..." :image-size="80" />
        </el-tab-pane>

        <el-tab-pane :label="`已结束 (${finishedRooms.length})`" name="finished">
          <el-table
            v-if="finishedRooms.length"
            :data="filteredFinished"
            stripe
            height="calc(100vh - 340px)"
          >
            <el-table-column prop="roomNo" label="编号" width="100">
              <template #default="{ row }">
                <span style="font-size:11px;color:#888">{{ (row.roomNo || '').replace('CRAWL_DOUYIN_', '#') }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="roomName" label="直播间" min-width="180" show-overflow-tooltip />
            <el-table-column prop="anchorName" label="主播" width="120" />
            <el-table-column prop="category" label="类目" width="80" />
            <el-table-column prop="viewerCount" label="峰值在线" width="100">
              <template #default="{ row }">
                <span style="color:#888">{{ formatNum(row.viewerCount) }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="orderCount" label="总订单" width="80" />
            <el-table-column label="总GMV" width="110">
              <template #default="{ row }">
                <span style="color:#888">￥{{ formatNum(row.gmv) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="90">
              <template #default><el-tag type="danger" size="small">已结束</el-tag></template>
            </el-table-column>
          </el-table>
          <el-empty v-else description="暂无已结束的直播间" :image-size="80" />
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onBeforeUnmount } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { getRoomPage, getLiveRooms, rotateDemoRooms, refreshLiveRooms } from '@/api'

const router = useRouter()

const query = reactive({ search: '' })
const activeTab = ref('live')
const liveRooms = ref([])
const finishedRooms = ref([])
const lastUpdate = ref('')

const formatNum = (n) => {
  const v = Number(n || 0)
  if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'
  if (v >= 1e4) return (v / 1e4).toFixed(1) + '万'
  return v.toLocaleString()
}

const filterBySearch = (rooms) => {
  if (!query.search) return rooms
  const kw = query.search.toLowerCase()
  return rooms.filter(r =>
    (r.roomName || '').toLowerCase().includes(kw) ||
    (r.anchorName || '').toLowerCase().includes(kw)
  )
}

const filteredLive = computed(() => filterBySearch(liveRooms.value))
const filteredFinished = computed(() => filterBySearch(finishedRooms.value))

const fetchLiveRooms = async () => {
  try {
    const res = await getLiveRooms()
    if (res?.code === 0 && res?.data) {
      liveRooms.value = res.data
    }
  } catch (e) { console.error('fetchLiveRooms error:', e) }
}

const fetchFinishedRooms = async () => {
  try {
    const res = await getRoomPage({ status: 'finished', pageSize: 100 })
    finishedRooms.value = res?.data?.records || []
  } catch (e) { console.error('fetchFinishedRooms error:', e) }
}

const fetchData = async () => {
  await Promise.all([fetchLiveRooms(), fetchFinishedRooms()])
  const now = new Date()
  lastUpdate.value = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`
}

const onSearch = () => fetchData()
const onRefresh = () => { query.search = ''; fetchData() }
const onTabChange = () => {}

const rotating = ref(false)
const onRotate = async () => {
  rotating.value = true
  try {
    const res = await rotateDemoRooms()
    ElMessage.success(res.msg || '轮换完成')
    await fetchData()
  } catch (e) {
    ElMessage.error('轮换失败')
  } finally {
    rotating.value = false
  }
}

const refreshing = ref(false)
const onRefreshLive = async () => {
  refreshing.value = true
  try {
    const res = await refreshLiveRooms()
    ElMessage.success(res?.msg || '刷新完成，已从抖音获取最新直播房间')
    await fetchData()
  } catch (e) {
    ElMessage.error('刷新失败，请稍后重试')
  } finally {
    refreshing.value = false
  }
}

const viewDanmaku = (row) => {
  const rid = row.roomNo || row.roomIdExternal || String(row.id)
  router.push(`/live-room/${rid}`)
}

const jumpToLive = (row) => {
  if (row.liveUrl) {
    window.open(row.liveUrl, '_blank')
  } else {
    const rid = row.roomIdExternal || (row.roomNo || '').replace('CRAWL_DOUYIN_', '')
    if (rid) window.open('https://live.douyin.com/' + rid, '_blank')
  }
}

let refreshTimer
onMounted(() => {
  fetchData()
  refreshTimer = setInterval(fetchData, 30000)
})
onBeforeUnmount(() => clearInterval(refreshTimer))
</script>

<style scoped lang="scss">
.liveroom { display: flex; flex-direction: column; height: 100%; }
.liveroom > .el-card { display: flex; flex-direction: column; flex: 1; :deep(.el-card__body) { display: flex; flex-direction: column; flex: 1; } }
.liveroom .search-bar { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
.status-summary { margin-left: auto; display: flex; align-items: center; gap: 4px; }
.update-time { font-size: 12px; color: #888; margin-left: 12px; }
.live-dot-sm {
  display: inline-block; width: 6px; height: 6px; border-radius: 50%;
  background: #00ff88; margin-right: 4px; vertical-align: middle;
  animation: pulse 1.5s infinite;
}
.room-tabs { margin-top: 8px; flex: 1; display: flex; flex-direction: column;
  :deep(.el-tabs__content) { flex: 1; overflow: hidden; }
  :deep(.el-tabs__item) { font-size: 14px; font-weight: 600; }
}
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
</style>
