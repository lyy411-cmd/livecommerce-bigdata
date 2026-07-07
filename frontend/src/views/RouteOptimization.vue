<template>
  <div class="route-optimization">
    <el-row :gutter="16">
      <el-col :span="8">
        <el-card shadow="hover" class="control-card">
          <template #header><span>路线规划</span></template>
          <el-form label-width="80px" size="default">
            <el-form-item label="起点">
              <el-select v-model="route.from" placeholder="选择起点" id="route-from" style="width:100%">
                <el-option v-for="c in cities" :key="c" :label="c" :value="c" />
              </el-select>
            </el-form-item>
            <el-form-item label="终点">
              <el-select v-model="route.to" placeholder="选择终点" id="route-to" style="width:100%">
                <el-option v-for="c in cities" :key="c" :label="c" :value="c" />
              </el-select>
            </el-form-item>
            <el-form-item label="车辆类型">
              <el-select v-model="route.vehicleType" id="route-vehicle-type" style="width:100%">
                <el-option label="小型货车(2吨)" value="small" />
                <el-option label="中型货车(5吨)" value="medium" />
                <el-option label="大型货车(10吨)" value="large" />
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" icon="Search" id="btn-calc-route" style="width:100%" @click="calculateRoute">计算最优路线</el-button>
            </el-form-item>
          </el-form>
        </el-card>

        <el-card shadow="hover" class="result-card" v-if="routeResult">
          <template #header><span>计算结果</span></template>
          <div class="route-result">
            <div class="result-item">
              <el-icon :size="20" color="#409EFF"><MapLocation /></el-icon>
              <div>
                <p class="result-label">推荐路线</p>
                <p class="result-value">{{ routeResult.path }}</p>
              </div>
            </div>
            <div class="result-item">
              <el-icon :size="20" color="#67C23A"><Timer /></el-icon>
              <div>
                <p class="result-label">预计耗时</p>
                <p class="result-value">{{ routeResult.duration }}小时</p>
              </div>
            </div>
            <div class="result-item">
              <el-icon :size="20" color="#E6A23C"><Money /></el-icon>
              <div>
                <p class="result-label">预估成本</p>
                <p class="result-value">¥{{ routeResult.cost }}</p>
              </div>
            </div>
            <div class="result-item">
              <el-icon :size="20" color="#F56C6C"><WarningFilled /></el-icon>
              <div>
                <p class="result-label">风险等级</p>
                <p class="result-value">{{ routeResult.risk }}</p>
              </div>
            </div>
          </div>
        </el-card>
      </el-col>

      <el-col :span="16">
        <el-card shadow="hover" class="map-card">
          <template #header><span>路线可视化</span></template>
          <div ref="mapChartRef" style="height:520px"></div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" class="history-card" style="margin-top:16px">
      <template #header><span>历史路线规划记录</span></template>
      <el-table :data="historyRoutes" stripe>
        <el-table-column prop="from" label="起点" width="80" />
        <el-table-column prop="to" label="终点" width="80" />
        <el-table-column prop="vehicle_type" label="车辆类型" width="110" />
        <el-table-column prop="path" label="推荐路线" min-width="200" />
        <el-table-column prop="distance" label="距离(km)" width="90" sortable />
        <el-table-column prop="duration" label="耗时(h)" width="85" sortable />
        <el-table-column prop="cost" label="成本(元)" width="100" sortable />
        <el-table-column prop="time" label="时间" width="160" />
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onBeforeUnmount, nextTick, watch } from 'vue'
import * as echarts from 'echarts'

const cities = ['北京', '上海', '广州', '深圳', '武汉', '成都', '杭州', '南京', '重庆', '长沙']

// 城市坐标表
const cityCoords = {
  '北京': [116.4, 39.9], '天津': [117.2, 39.1], '济南': [117.0, 36.7],
  '南京': [118.8, 32.1], '上海': [121.5, 31.2], '合肥': [117.3, 31.9],
  '武汉': [114.3, 30.6], '长沙': [113.0, 28.2], '杭州': [120.2, 30.3],
  '成都': [104.1, 30.6], '重庆': [106.5, 29.5], '贵阳': [106.7, 26.6],
  '桂林': [110.3, 25.3], '广州': [113.3, 23.1], '深圳': [114.1, 22.5]
}

// 城市间中间节点
const midNodes = {
  '深圳_武汉': ['长沙'], '深圳_广州': [], '广州_深圳': [],
  '北京_上海': ['济南', '南京'], '上海_北京': ['南京', '济南'],
  '成都_广州': ['贵阳', '桂林'], '广州_成都': ['桂林', '贵阳'],
  '杭州_重庆': ['合肥', '武汉'], '重庆_杭州': ['武汉', '合肥'],
  '南京_长沙': ['合肥', '武汉'], '长沙_南京': ['武汉', '合肥'],
  '北京_武汉': ['郑州'], '上海_杭州': [], '深圳_长沙': [],
  '武汉_北京': ['郑州'], '长沙_武汉': [], '杭州_上海': []
}

const route = reactive({ from: '深圳', to: '武汉', vehicleType: 'medium' })
const routeResult = ref(null)

const historyRoutes = ref([
  { from: '深圳', to: '武汉', vehicle_type: '中型货车', path: '深圳 → 长沙 → 武汉', distance: 1050, duration: 12.5, cost: 3200, time: '2026-06-25 08:30' },
  { from: '北京', to: '上海', vehicle_type: '大型货车', path: '北京 → 济南 → 南京 → 上海', distance: 1200, duration: 14, cost: 4500, time: '2026-06-24 14:00' },
  { from: '成都', to: '广州', vehicle_type: '小型货车', path: '成都 → 贵阳 → 桂林 → 广州', distance: 1600, duration: 20, cost: 5200, time: '2026-06-23 06:00' },
  { from: '杭州', to: '重庆', vehicle_type: '中型货车', path: '杭州 → 合肥 → 武汉 → 重庆', distance: 1450, duration: 18, cost: 4800, time: '2026-06-22 10:00' },
  { from: '南京', to: '长沙', vehicle_type: '小型货车', path: '南京 → 合肥 → 武汉 → 长沙', distance: 900, duration: 11, cost: 2800, time: '2026-06-21 09:00' }
])

const mapChartRef = ref()
let mapChart

const getRouteLine = (from, to) => {
  const key = `${from}_${to}`
  const mids = midNodes[key] || []
  const line = [{ coord: cityCoords[from] || [116, 39] }]
  mids.forEach(m => { if (cityCoords[m]) line.push({ coord: cityCoords[m] }) })
  line.push({ coord: cityCoords[to] || [116, 39] })
  return line
}

const updateMapChart = (fromCity, toCity) => {
  if (!mapChart) return
  const routeLine = getRouteLine(fromCity, toCity)
  mapChart.setOption({
    series: [
      {
        type: 'scatter',
        data: Object.entries(cityCoords).map(([name, coord]) => ({
          value: coord, name,
          symbolSize: (name === fromCity || name === toCity) ? 18 : 10,
          itemStyle: {
            color: name === fromCity ? '#67C23A' : name === toCity ? '#F56C6C' : '#409EFF',
            shadowBlur: (name === fromCity || name === toCity) ? 10 : 0
          },
          label: { show: true, formatter: p => p.name, position: 'right', fontSize: 11 }
        }))
      },
      {
        type: 'lines',
        coordinateSystem: 'cartesian2d',
        polyline: true,
        data: [routeLine],
        lineStyle: { color: '#409EFF', width: 2.5, type: 'dashed' },
        effect: { show: true, period: 3, trailLength: 0.2, symbolSize: 8 }
      }
    ]
  })
}

const initMapChart = () => {
  if (!mapChartRef.value) return
  mapChart = echarts.init(mapChartRef.value)
  mapChart.setOption({
    backgroundColor: '#f5f7fa',
    tooltip: { trigger: 'item', formatter: '{b}' },
    xAxis: { type: 'value', show: false, min: 100, max: 125 },
    yAxis: { type: 'value', show: false, min: 20, max: 42 }
  })
  updateMapChart(route.from, route.to)
}

const calculateRoute = () => {
  if (route.from === route.to) {
    ElMessage.warning('起点和终点不能相同')
    return
  }
  const vehicleCost = { small: 2.5, medium: 3.0, large: 3.8 }
  const dist = Math.floor(Math.random() * 500 + 500)
  const key = `${route.from}_${route.to}`
  const mids = midNodes[key] || []
  const pathStr = mids.length > 0 ? `${route.from} → ${mids.join(' → ')} → ${route.to}` : `${route.from} → ${route.to}`

  routeResult.value = {
    path: pathStr,
    duration: (dist / 80).toFixed(1),
    cost: (dist * vehicleCost[route.vehicleType]).toFixed(0),
    risk: ['低', '中低', '中'][Math.floor(Math.random() * 3)]
  }
  historyRoutes.value.unshift({
    from: route.from, to: route.to,
    vehicle_type: { small: '小型货车', medium: '中型货车', large: '大型货车' }[route.vehicleType],
    path: pathStr,
    distance: dist,
    duration: (dist / 80).toFixed(1),
    cost: (dist * vehicleCost[route.vehicleType]).toFixed(0),
    time: new Date().toLocaleString()
  })

  // 动态更新地图上的路线
  updateMapChart(route.from, route.to)
  ElMessage.success('路线计算完成，地图已更新')
}

onMounted(async () => {
  await nextTick()
  initMapChart()
})

onBeforeUnmount(() => {
  mapChart?.dispose()
})
</script>

<style scoped lang="scss">
.route-optimization { display: flex; flex-direction: column; gap: 16px; }
.control-card, .result-card { margin-bottom: 16px; }
.route-result { display: flex; flex-direction: column; gap: 16px; }
.result-item {
  display: flex; align-items: center; gap: 12px;
  .result-label { font-size: 12px; color: #909399; }
  .result-value { font-size: 16px; font-weight: 600; color: #303133; }
}
</style>
