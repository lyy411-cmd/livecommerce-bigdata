<template>
  <div class="dashboard">
    <!-- KPI 卡片 -->
    <el-row :gutter="16" class="kpi-row">
      <el-col :span="6" v-for="card in kpiCards" :key="card.label">
        <el-card shadow="hover" class="kpi-card">
          <div class="kpi-content">
            <div class="kpi-left">
              <p class="kpi-label">{{ card.label }}</p>
              <p class="kpi-value">
                <count-to :end-val="card.value" :duration="2000" />{{ card.unit }}
              </p>
              <p class="kpi-trend" :class="card.up ? 'up' : 'down'">
                <el-icon><component :is="card.up ? 'Top' : 'Bottom'" /></el-icon>
                {{ card.rate }}% {{ card.up ? '增长' : '下降' }}
              </p>
            </div>
            <div class="kpi-icon" :style="{ background: card.color }">
              <el-icon :size="32" color="#fff"><component :is="card.icon" /></el-icon>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 趋势图 + 地理分布 -->
    <el-row :gutter="16" class="chart-row">
      <el-col :span="16">
        <el-card shadow="hover">
          <template #header>
            <div class="card-header"><span>订单量趋势（近30天）</span></div>
          </template>
          <div ref="trendChartRef" style="height:360px"></div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card shadow="hover">
          <template #header>
            <div class="card-header"><span>订单状态分布</span></div>
          </template>
          <div ref="pieChartRef" style="height:360px"></div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 最近订单 + 热力路线 -->
    <el-row :gutter="16" class="bottom-row">
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header>
            <div class="card-header flex-between">
              <span>最近订单</span>
              <el-button text type="primary" @click="router.push('/order-monitor')">查看全部</el-button>
            </div>
          </template>
          <el-table :data="recentOrders" stripe size="small">
            <el-table-column prop="order_no" label="订单号" width="140" />
            <el-table-column prop="sender" label="发货方" />
            <el-table-column prop="receiver" label="收货方" />
            <el-table-column prop="status" label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="statusType(row.status)" size="small">{{ row.status }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="amount" label="金额(元)" width="100" />
          </el-table>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header>
            <div class="card-header"><span>热门路线 TOP5</span></div>
          </template>
          <div ref="barChartRef" style="height:280px"></div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { useRouter } from 'vue-router'
import * as echarts from 'echarts'
import { getDashboardStats, getDashboardTrend, getDashboardGeo } from '@/api/dashboard'
import { getOrders } from '@/api/order'

const router = useRouter()

// KPI 数据
const kpiCards = ref([
  { label: '今日订单量', value: 2856, unit: '单', rate: 12.5, up: true, icon: 'Document', color: '#409EFF' },
  { label: '活跃车辆数', value: 342, unit: '辆', rate: 8.3, up: true, icon: 'Van', color: '#67C23A' },
  { label: '在途包裹数', value: 18920, unit: '件', rate: 3.2, up: false, icon: 'Box', color: '#E6A23C' },
  { label: '今日营收', value: 156.8, unit: '万元', rate: 15.7, up: true, icon: 'Money', color: '#F56C6C' }
])

// 最近订单
const recentOrders = ref([
  { order_no: 'LOG202606260001', sender: '深圳华南仓', receiver: '广州天河站', status: '运输中', amount: 2580 },
  { order_no: 'LOG202606260002', sender: '北京大兴仓', receiver: '上海浦东站', status: '待揽收', amount: 4320 },
  { order_no: 'LOG202606260003', sender: '成都高新仓', receiver: '重庆江北站', status: '已签收', amount: 1850 },
  { order_no: 'LOG202606260004', sender: '武汉光谷仓', receiver: '长沙岳麓站', status: '运输中', amount: 3200 },
  { order_no: 'LOG202606260005', sender: '杭州滨江仓', receiver: '南京鼓楼站', status: '异常', amount: 1680 }
])

const statusType = (s) => {
  const map = { '待揽收': 'info', '运输中': '', '已签收': 'success', '异常': 'danger' }
  return map[s] || 'info'
}

// Charts
const trendChartRef = ref()
const pieChartRef = ref()
const barChartRef = ref()
let trendChart, pieChart, barChart

const initTrendChart = () => {
  if (!trendChartRef.value) return
  trendChart = echarts.init(trendChartRef.value)
  const dates = Array.from({ length: 30 }, (_, i) => `06-${String(i + 1).padStart(2, '0')}`)
  trendChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['订单量', '签收量', '异常量'], bottom: 0 },
    grid: { left: 50, right: 20, top: 20, bottom: 40 },
    xAxis: { type: 'category', data: dates, boundaryGap: false },
    yAxis: { type: 'value' },
    series: [
      {
        name: '订单量', type: 'line', smooth: true, symbol: 'none',
        data: [120, 132, 145, 138, 155, 162, 180, 195, 210, 225, 240, 230, 250, 265, 280, 275, 290, 310, 305, 320, 330, 315, 340, 355, 370, 360, 385, 400, 390, 410],
        lineStyle: { color: '#409EFF' },
        itemStyle: { color: '#409EFF' }
      },
      {
        name: '签收量', type: 'line', smooth: true, symbol: 'none',
        data: [100, 110, 120, 115, 130, 140, 155, 165, 180, 195, 210, 200, 220, 235, 250, 245, 260, 280, 275, 290, 300, 285, 310, 325, 340, 330, 355, 370, 360, 380],
        lineStyle: { color: '#67C23A' },
        itemStyle: { color: '#67C23A' }
      },
      {
        name: '异常量', type: 'line', smooth: true, symbol: 'none',
        data: [5, 4, 6, 3, 5, 8, 4, 6, 5, 7, 4, 3, 6, 5, 4, 7, 5, 6, 4, 5, 8, 6, 5, 4, 7, 5, 6, 4, 5, 6],
        lineStyle: { color: '#F56C6C' },
        itemStyle: { color: '#F56C6C' }
      }
    ]
  })
}

const initPieChart = () => {
  if (!pieChartRef.value) return
  pieChart = echarts.init(pieChartRef.value)
  pieChart.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: { orient: 'vertical', right: 10, top: 'center' },
    series: [{
      type: 'pie', radius: ['40%', '70%'], center: ['35%', '50%'],
      label: { show: false },
      emphasis: { label: { show: true } },
      data: [
        { value: 1280, name: '运输中', itemStyle: { color: '#409EFF' } },
        { value: 856, name: '已签收', itemStyle: { color: '#67C23A' } },
        { value: 420, name: '待揽收', itemStyle: { color: '#E6A23C' } },
        { value: 200, name: '已取消', itemStyle: { color: '#909399' } },
        { value: 100, name: '异常', itemStyle: { color: '#F56C6C' } }
      ]
    }]
  })
}

const initBarChart = () => {
  if (!barChartRef.value) return
  barChart = echarts.init(barChartRef.value)
  barChart.setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 80, right: 30, top: 10, bottom: 20 },
    xAxis: { type: 'value' },
    yAxis: {
      type: 'category',
      data: ['广州 → 深圳', '上海 → 杭州', '北京 → 天津', '成都 → 重庆', '武汉 → 长沙']
    },
    series: [{
      type: 'bar', barWidth: 22,
      itemStyle: {
        borderRadius: [0, 4, 4, 0],
        color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
          { offset: 0, color: '#409EFF' },
          { offset: 1, color: '#79bbff' }
        ])
      },
      data: [2850, 2460, 1980, 1650, 1320],
      label: { show: true, position: 'right', formatter: '{c} 单' }
    }]
  })
}

// CountTo 组件
const CountTo = {
  props: { endVal: Number, duration: { type: Number, default: 2000 } },
  setup(props) {
    const displayVal = ref(0)
    let timer
    onMounted(() => {
      const startTime = Date.now()
      const step = () => {
        const elapsed = Date.now() - startTime
        const progress = Math.min(elapsed / props.duration, 1)
        displayVal.value = Math.floor(progress * props.endVal)
        if (progress < 1) {
          timer = requestAnimationFrame(step)
        } else {
          displayVal.value = props.endVal
        }
      }
      step()
    })
    onBeforeUnmount(() => cancelAnimationFrame(timer))
    return () => displayVal.value.toLocaleString()
  }
}

onMounted(async () => {
  await nextTick()
  initTrendChart()
  initPieChart()
  initBarChart()
})

onBeforeUnmount(() => {
  trendChart?.dispose()
  pieChart?.dispose()
  barChart?.dispose()
})
</script>

<style scoped lang="scss">
.dashboard { display: flex; flex-direction: column; gap: 16px; }

.kpi-row { margin-bottom: 0; }

.kpi-card {
  cursor: pointer;
  transition: transform 0.2s;
  &:hover { transform: translateY(-2px); }
}

.kpi-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.kpi-label { font-size: 13px; color: #909399; margin-bottom: 8px; }
.kpi-value { font-size: 28px; font-weight: bold; color: #303133; }
.kpi-trend { font-size: 12px; margin-top: 6px; display: flex; align-items: center; gap: 2px; }
.kpi-trend.up { color: #67C23A; }
.kpi-trend.down { color: #F56C6C; }

.kpi-icon {
  width: 56px; height: 56px; border-radius: 12px;
  display: flex; align-items: center; justify-content: center;
}

.chart-row, .bottom-row { margin-bottom: 0; }

.card-header {
  font-size: 15px; font-weight: 600;
}
</style>
