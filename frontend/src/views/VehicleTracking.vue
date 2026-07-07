<template>
  <div class="vehicle-tracking">
    <el-row :gutter="16" class="status-row">
      <el-col :span="6" v-for="card in vehicleStats" :key="card.label">
        <el-card shadow="hover" class="status-card">
          <div class="status-content">
            <p class="status-label">{{ card.label }}</p>
            <p class="status-value">{{ card.value }}<small>{{ card.unit }}</small></p>
            <p class="status-sub">{{ card.sub }}</p>
          </div>
          <div class="status-icon" :style="{ background: card.color }">
            <el-icon :size="28" color="#fff"><component :is="card.icon" /></el-icon>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16" class="track-row">
      <el-col :span="16">
        <el-card shadow="hover" class="map-card">
          <template #header>
            <div class="flex-between">
              <span>车辆实时追踪地图</span>
              <el-tag type="success">实时更新中 {{ updateTime }}</el-tag>
            </div>
          </template>
          <div ref="trackMapRef" style="height:480px"></div>
        </el-card>
      </el-col>

      <el-col :span="8">
        <el-card shadow="hover" class="vehicle-list-card">
          <template #header>
            <div class="flex-between">
              <span>在线车辆列表</span>
              <el-input v-model="vehicleSearch" placeholder="搜索车牌" prefix-icon="Search" size="small" clearable style="width:160px" />
            </div>
          </template>
          <div class="vehicle-scroll">
            <div
              v-for="v in filteredVehicles" :key="v.id"
              class="vehicle-item"
              :class="{ selected: selectedVehicle === v.id }"
              @click="selectVehicle(v)"
            >
              <div class="v-header">
                <span class="v-plate">{{ v.plate }}</span>
                <el-tag :type="v.status === '正常' ? 'success' : v.status === '怠速' ? 'warning' : 'danger'" size="small">
                  {{ v.status }}
                </el-tag>
              </div>
              <div class="v-info">
                <span>◆ {{ v.location }}</span>
                <span>▸ {{ v.driver }}</span>
              </div>
              <div class="v-info">
                <span>◇ {{ v.speed }} km/h</span>
                <span>□ {{ v.mileage }} km</span>
              </div>
              <el-progress :percentage="v.fuel" :color="v.fuel < 20 ? '#F56C6C' : '#409EFF'" :stroke-width="4" />
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" class="replay-card">
      <template #header>
        <div class="flex-between">
          <span>轨迹回放</span>
          <div class="replay-controls">
            <span>选择车辆：</span>
            <el-select v-model="replayVehicle" placeholder="选择车辆" size="small" style="width:180px">
              <el-option v-for="v in vehicles" :key="v.id" :label="`${v.plate} - ${v.driver}`" :value="v.id" />
            </el-select>
            <el-date-picker v-model="replayDate" type="date" placeholder="选择日期" size="small" style="width:140px" />
            <el-button type="primary" size="small" icon="VideoPlay" @click="startReplay">开始回放</el-button>
          </div>
        </div>
      </template>
      <div ref="replayChartRef" style="height:200px"></div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'

const updateTime = ref(new Date().toLocaleTimeString())
setInterval(() => { updateTime.value = new Date().toLocaleTimeString() }, 3000)

const vehicleStats = ref([
  { label: '在线车辆', value: 285, unit: '辆', sub: '总车辆 342', icon: 'Van', color: '#409EFF' },
  { label: '运行中', value: 192, unit: '辆', sub: '占比 67.3%', icon: 'Ship', color: '#67C23A' },
  { label: '怠速车辆', value: 45, unit: '辆', sub: '占比 15.8%', icon: 'Timer', color: '#E6A23C' },
  { label: '告警车辆', value: 8, unit: '辆', sub: '需立即处理', icon: 'WarningFilled', color: '#F56C6C' }
])

const vehicleSearch = ref('')
const selectedVehicle = ref(1)

const vehicles = ref([
  { id: 1, plate: '粤B·D8823', driver: '张三', status: '正常', location: '深圳市南山区', lng: 114.05, lat: 22.55, speed: 65, mileage: 128, fuel: 72 },
  { id: 2, plate: '京A·F1256', driver: '李四', status: '怠速', location: '北京市朝阳区', lng: 116.46, lat: 39.92, speed: 0, mileage: 256, fuel: 35 },
  { id: 3, plate: '沪C·H3891', driver: '王五', status: '正常', location: '上海市浦东新区', lng: 121.53, lat: 31.22, speed: 78, mileage: 112, fuel: 88 },
  { id: 4, plate: '川A·K5620', driver: '赵六', status: '告警', location: '成都市锦江区', lng: 104.07, lat: 30.67, speed: 45, mileage: 320, fuel: 15 },
  { id: 5, plate: '鄂A·M7731', driver: '孙七', status: '正常', location: '武汉市洪山区', lng: 114.35, lat: 30.55, speed: 55, mileage: 95, fuel: 60 },
  { id: 6, plate: '粤A·N9842', driver: '周八', status: '正常', location: '广州市天河区', lng: 113.3, lat: 23.1, speed: 70, mileage: 180, fuel: 45 },
  { id: 7, plate: '浙A·P1053', driver: '吴九', status: '怠速', location: '杭州市滨江区', lng: 120.2, lat: 30.3, speed: 0, mileage: 210, fuel: 28 },
  { id: 8, plate: '苏A·Q2164', driver: '郑十', status: '正常', location: '南京市鼓楼区', lng: 118.8, lat: 32.1, speed: 82, mileage: 155, fuel: 92 }
])

const filteredVehicles = computed(() => {
  if (!vehicleSearch.value) return vehicles.value
  return vehicles.value.filter(v => v.plate.includes(vehicleSearch.value))
})

const selectVehicle = (v) => {
  selectedVehicle.value = v.id
  highlightOnMap(v)
  ElMessage.info(`已选中车辆 ${v.plate} - ${v.driver}，地图已定位`)
}

// 高亮地图上的选中车辆
const highlightOnMap = (v) => {
  if (!trackMap) return
  const allPoints = vehicles.value.map(ve => ({
    value: [ve.lng, ve.lat],
    name: ve.plate,
    symbolSize: ve.plate === v.plate ? 22 : 12,
    itemStyle: {
      color: ve.plate === v.plate ? '#FF6B35' :
             ve.status === '告警' ? '#F56C6C' :
             ve.status === '怠速' ? '#E6A23C' : '#409EFF',
      shadowBlur: ve.plate === v.plate ? 20 : 5,
      shadowColor: ve.plate === v.plate ? 'rgba(255,107,53,0.6)' : 'rgba(64,158,255,0.2)'
    },
    label: {
      show: true,
      formatter: () => `${ve.plate}\n${ve.location}`,
      position: 'right',
      fontSize: 11
    }
  }))

  trackMap.setOption({
    series: [{
      type: 'scatter',
      data: allPoints
    }]
  })
}

// 追踪地图
const trackMapRef = ref()
let trackMap

const initTrackMap = () => {
  if (!trackMapRef.value) return
  trackMap = echarts.init(trackMapRef.value)
  trackMap.setOption({
    backgroundColor: '#f5f7fa',
    tooltip: {
      trigger: 'item',
      formatter: p => `<b>${p.name}</b><br/>坐标: (${p.value[0]}, ${p.value[1]})`
    },
    xAxis: { type: 'value', show: false, min: 100, max: 125 },
    yAxis: { type: 'value', show: false, min: 20, max: 42 }
  })

  // 初始化时高亮第一辆车
  const firstVehicle = vehicles.value[0]
  if (firstVehicle) highlightOnMap(firstVehicle)

  // 点击地图上的车辆
  trackMap.on('click', (params) => {
    if (params.componentType === 'series') {
      const plate = params.name
      const v = vehicles.value.find(ve => ve.plate === plate)
      if (v) selectVehicle(v)
    }
  })
}

// 轨迹回放
const replayVehicle = ref(1)
const replayDate = ref(new Date())
const replayChartRef = ref()
let replayChart

const startReplay = () => {
  const v = vehicles.value.find(ve => ve.id === replayVehicle.value)
  if (!replayChart) {
    replayChart = echarts.init(replayChartRef.value)
  }
  const hours = Array.from({ length: 24 }, (_, i) => `${i}:00`)
  const baseSpeed = v ? v.speed : 60
  const data = [0,0,0,0,0, baseSpeed-20, baseSpeed-10, baseSpeed+10, baseSpeed, baseSpeed-5, baseSpeed+15, baseSpeed+5, baseSpeed-10, baseSpeed, baseSpeed+10, baseSpeed-5, baseSpeed, baseSpeed-15, baseSpeed+5, baseSpeed-10, baseSpeed-5, baseSpeed-20, baseSpeed-30, 0]
  replayChart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: { type: 'category', data: hours },
    yAxis: { type: 'value', name: '速度(km/h)' },
    series: [{
      type: 'line', smooth: true,
      data: data,
      areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
        { offset: 0, color: 'rgba(64,158,255,0.3)' },
        { offset: 1, color: 'rgba(64,158,255,0.02)' }
      ])},
      lineStyle: { color: '#409EFF' }
    }]
  })
  ElMessage.success(`正在回放 ${v.plate} 轨迹`)
}

onMounted(async () => {
  await nextTick()
  initTrackMap()
})

onBeforeUnmount(() => {
  trackMap?.dispose()
  replayChart?.dispose()
})
</script>

<style scoped lang="scss">
.vehicle-tracking { display: flex; flex-direction: column; gap: 16px; }

.status-card {
  cursor: pointer; display: flex; justify-content: space-between; align-items: center;
  transition: transform 0.2s;
  &:hover { transform: translateY(-2px); }
}
.status-label { font-size: 13px; color: #909399; }
.status-value { font-size: 24px; font-weight: bold; color: #303133; small { font-size: 13px; font-weight: normal; margin-left: 4px; } }
.status-sub { font-size: 12px; color: #C0C4CC; margin-top: 4px; }
.status-icon { width: 48px; height: 48px; border-radius: 12px; display: flex; align-items: center; justify-content: center; }

.vehicle-scroll { max-height: 440px; overflow-y: auto; }
.vehicle-item {
  padding: 14px; border: 1px solid #EBEEF5; border-radius: 8px; margin-bottom: 10px;
  cursor: pointer; transition: all 0.2s;
  &:hover { border-color: #409EFF; }
  &.selected { border-color: #FF6B35; background: #fff7f0; }
}
.v-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
.v-plate { font-size: 15px; font-weight: bold; }
.v-info { display: flex; justify-content: space-between; font-size: 12px; color: #909399; margin-bottom: 6px; }
.replay-controls { display: flex; align-items: center; gap: 10px; }
</style>
