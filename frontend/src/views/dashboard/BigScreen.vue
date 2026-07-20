<template>
  <div class="bigscreen">
    <div class="header">
      <h1>智慧直播电商实时数据大屏</h1>
      <p class="realtime">实时数据 · {{ currentTime }} · 每 5 秒自动刷新</p>
    </div>
    <el-row :gutter="16">
      <el-col :span="6">
        <el-card class="metric-card">
          <div class="metric">
            <el-icon :size="32" color="rgba(255,255,255,0.7)"><View /></el-icon>
            <p class="metric-label">当前在线观众</p>
            <p class="metric-value">{{ realtime.currentViewers?.toLocaleString() || 0 }}</p>
            <p class="metric-sub">实时人数</p>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card class="metric-card green">
          <div class="metric">
            <el-icon :size="32" color="rgba(255,255,255,0.7)"><List /></el-icon>
            <p class="metric-label">实时订单数</p>
            <p class="metric-value">{{ realtime.currentOrders || 0 }}</p>
            <p class="metric-sub">本小时</p>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card class="metric-card orange">
          <div class="metric">
            <el-icon :size="32" color="rgba(255,255,255,0.7)"><Money /></el-icon>
            <p class="metric-label">实时GMV</p>
            <p class="metric-value">￥{{ formatNumber(realtime.currentGmv) }}</p>
            <p class="metric-sub">本小时</p>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card class="metric-card purple">
          <div class="metric">
            <el-icon :size="32" color="rgba(255,255,255,0.7)"><VideoCamera /></el-icon>
            <p class="metric-label">在线主播数</p>
            <p class="metric-value">{{ realtime.onlineAnchors || 0 }}</p>
            <p class="metric-sub">正在直播</p>
          </div>
        </el-card>
      </el-col>
    </el-row>
    <el-row :gutter="16" class="mt-16">
      <el-col :span="12">
        <el-card class="dark-card">
          <template #header><span>┃ 地域分布 TOP10</span></template>
          <div ref="geoRef" style="height:450px"></div>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card class="dark-card">
          <template #header><span>┃ 实时直播列表</span></template>
          <el-table :data="liveRooms" stripe size="small" height="450" :row-style="{ background: 'transparent' }">
            <el-table-column prop="roomName" label="直播间" min-width="160" />
            <el-table-column prop="anchorName" label="主播" width="90" />
            <el-table-column prop="viewerCount" label="在线" width="90" sortable align="right">
              <template #default="{ row }">{{ formatNumber(row.viewerCount) }}</template>
            </el-table-column>
            <el-table-column prop="gmv" label="GMV" width="120" sortable align="right">
              <template #default="{ row }">￥{{ formatNumber(row.gmv) }}</template>
            </el-table-column>
            <el-table-column label="状态" width="80" align="center">
              <template #default="{ row }">
                <el-tag :type="row.status === 'live' ? 'success' : 'info'" size="small" effect="dark">
                  {{ row.status === 'live' ? '直播中' : '已结束' }}
                </el-tag>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>
    <el-row :gutter="16" class="mt-16">
      <el-col :span="24">
        <el-card class="dark-card">
          <template #header><span>┃ 弹幕热词</span></template>
          <div class="hotwords-container" v-if="hotwords.length > 0">
            <span v-for="(word, idx) in hotwords" :key="idx" class="hotword-tag" :style="{ fontSize: (14 + (word.count || word.heat || 1) * 0.5) + 'px', opacity: 0.6 + Math.min(0.4, (word.count || word.heat || 1) * 0.02) }">
              {{ word.word || word.text || word }}
            </span>
          </div>
          <div v-else style="text-align:center;color:rgba(255,255,255,0.3);padding:40px 0">暂无热词数据</div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { getRealtimeData, getGeoDistribution, getLiveRooms, getHotwords } from '@/api'
import { fallback } from '@/utils/fallback'

const realtime = reactive({})
const liveRooms = ref([])
const currentTime = ref('')
const geoRef = ref()
const hotwords = ref([])
let chart, timer

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

const initChart = (data) => {
  if (!geoRef.value) return
  chart = echarts.init(geoRef.value)
  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 60, right: 20, top: 20, bottom: 30 },
    xAxis: { type: 'value', axisLabel: { color: '#fff' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
    yAxis: { type: 'category', data: data.data.map(c => c.name), axisLabel: { color: '#fff' } },
    series: [{
      type: 'bar',
      data: data.data.map(c => c.value),
      itemStyle: {
        borderRadius: [0, 4, 4, 0],
        color: { type: 'linear', x: 0, y: 0, x2: 1, y2: 0, colorStops: [
          { offset: 0, color: '#1890ff' }, { offset: 1, color: '#52c41a' }] } },
      label: { show: true, position: 'right', color: '#fff', formatter: '{c}万' }
    }]
  })
}

onMounted(async () => {
  await nextTick()
  updateTime()
  await fetchData()
  const geo = await getGeoDistribution().catch(() => fallback.geoDistribution())
  initChart(geo)
  timer = setInterval(() => { updateTime(); fetchData() }, 5000)
})

onBeforeUnmount(() => {
  clearInterval(timer)
  chart?.dispose()
})
</script>

<style scoped lang="scss">
.bigscreen {
  background: linear-gradient(180deg, #001529 0%, #002140 100%);
  color: #fff; padding: 20px; border-radius: 12px;
  height: 100%; display: flex; flex-direction: column; gap: 16px; overflow-y: auto;
  .header { text-align: center; margin-bottom: 8px;
    h1 { font-size: 28px; color: #fff; margin: 0; text-shadow: 0 0 10px rgba(24,144,255,0.5); }
    .realtime { color: #1890ff; margin-top: 6px; font-size: 13px; }
  }
  :deep(.el-card) { background: rgba(0, 33, 64, 0.6) !important; color: #fff; border: 1px solid rgba(24,144,255,0.3) !important; backdrop-filter: blur(10px); }
  :deep(.el-card__header) { color: #fff; border-bottom: 1px solid rgba(24,144,255,0.2); font-weight: 600; }
  :deep(.el-table) { background: transparent !important; color: #fff; }
  :deep(.el-table tr), :deep(.el-table tr.el-table__row) { background: transparent !important; color: #fff !important; }
  :deep(.el-table th.el-table__cell) { background: rgba(24,144,255,0.2) !important; color: #fff; border-bottom: 1px solid rgba(24,144,255,0.3); }
  :deep(.el-table td.el-table__cell) { border-bottom: 1px solid rgba(255,255,255,0.06); }
  :deep(.el-table--striped .el-table__body tr.el-table__row--striped td) { background: rgba(255,255,255,0.03) !important; }
  :deep(.el-table__body tr:hover > td) { background: rgba(24,144,255,0.1) !important; }
}
.metric-card { transition: all 0.3s;
  &:hover { transform: scale(1.03); }
  .metric { text-align: center; padding: 20px 0; }
  .metric-label { color: rgba(255,255,255,0.85); font-size: 13px; margin: 8px 0 4px; }
  .metric-value { font-size: 36px; font-weight: bold; color: #fff; margin: 8px 0; text-shadow: 0 2px 8px rgba(0,0,0,0.3); }
  .metric-sub { color: rgba(255,255,255,0.7); font-size: 12px; }
  background: linear-gradient(135deg, #1890ff 0%, #096dd9 100%) !important;
  &.green { background: linear-gradient(135deg, #52c41a 0%, #389e0d 100%) !important; }
  &.orange { background: linear-gradient(135deg, #faad14 0%, #d48806 100%) !important; }
  &.purple { background: linear-gradient(135deg, #722ed1 0%, #531dab 100%) !important; }
}
.hotwords-container { display: flex; flex-wrap: wrap; gap: 12px; padding: 10px 0; justify-content: center; }
.hotword-tag { display: inline-block; padding: 4px 14px; background: rgba(24,144,255,0.15); border: 1px solid rgba(24,144,255,0.3); border-radius: 20px; color: #69c0ff; transition: all 0.3s; cursor: default; }
.hotword-tag:hover { background: rgba(24,144,255,0.3); color: #fff; transform: scale(1.05); }
</style>
