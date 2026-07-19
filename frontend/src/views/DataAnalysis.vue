<template>
  <div class="data-analysis">
    <!-- 筛选区 -->
    <el-card shadow="never" class="filter-card">
      <el-form :inline="true">
        <el-form-item label="时间维度">
          <el-select v-model="filter.period" style="width:140px">
            <el-option label="近7天" value="7d" />
            <el-option label="近30天" value="30d" />
            <el-option label="近90天" value="90d" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" icon="Refresh" @click="loadData">刷新数据</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- KPI 卡片 -->
    <el-row :gutter="16" class="kpi-row">
      <el-col :span="6" v-for="card in kpiCards" :key="card.label">
        <el-card shadow="hover" class="kpi-card">
          <div class="kpi-value">{{ card.value }}</div>
          <div class="kpi-label">{{ card.label }}</div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 图表区域 -->
    <el-row :gutter="16" class="chart-row">
      <el-col :span="14">
        <el-card shadow="hover">
          <template #header><span>GMV 与订单趋势</span></template>
          <div ref="trendChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
      <el-col :span="10">
        <el-card shadow="hover">
          <template #header><span>类目订单分布</span></template>
          <div ref="categoryOrderChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16" class="chart-row">
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>商品类目 GMV 排行</span></template>
          <div ref="categoryChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>TOP 带货主播</span></template>
          <div ref="anchorChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 类目销售明细表 -->
    <el-card shadow="never">
      <template #header><span>类目销售明细</span></template>
      <el-table :data="categoryTable" stripe>
        <el-table-column prop="category" label="类目" width="120" />
        <el-table-column prop="rooms" label="直播间数" sortable />
        <el-table-column prop="viewers" label="总观众" sortable>
          <template #default="{ row }">{{ formatNum(row.viewers) }}</template>
        </el-table-column>
        <el-table-column prop="orders" label="订单数" sortable>
          <template #default="{ row }">{{ formatNum(row.orders) }}</template>
        </el-table-column>
        <el-table-column prop="gmv" label="GMV (元)" sortable>
          <template #default="{ row }">¥{{ formatMoney(row.gmv) }}</template>
        </el-table-column>
        <el-table-column prop="convRate" label="转化率" sortable>
          <template #default="{ row }">{{ row.convRate }}%</template>
        </el-table-column>
        <el-table-column prop="trend" label="趋势" width="120">
          <template #default="{ row }">
            <el-tag :type="row.convRate > 4 ? 'success' : 'warning'" size="small">
              {{ row.convRate > 4 ? '高转化' : '待提升' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onBeforeUnmount, nextTick, computed } from 'vue'
import * as echarts from 'echarts'
import { getDashboardKpi, getGmvTrend, getCategoryDistribution, getCategoryRank, getAnchorRank, getRoomPage } from '@/api'

const filter = reactive({ period: '30d' })

const trendChartRef = ref()
const categoryOrderChartRef = ref()
const categoryChartRef = ref()
const anchorChartRef = ref()
let charts = []

// KPI 数据
const kpi = reactive({
  totalGmv: 0, totalOrders: 0, totalRooms: 0, totalViewers: 0
})

const kpiCards = computed(() => [
  { label: '总 GMV (元)', value: `¥${formatMoney(kpi.totalGmv)}` },
  { label: '总订单', value: formatNum(kpi.totalOrders) },
  { label: '直播间数', value: formatNum(kpi.totalRooms) },
  { label: '总观众', value: formatNum(kpi.totalViewers) },
])

// 类目表格
const categoryTable = ref([])

function formatNum(n) {
  if (!n) return '0'
  if (n >= 10000) return (n / 10000).toFixed(1) + '万'
  return n.toLocaleString()
}
function formatMoney(n) {
  if (!n) return '0'
  if (n >= 100000000) return (n / 100000000).toFixed(2) + '亿'
  if (n >= 10000) return (n / 10000).toFixed(2) + '万'
  return n.toLocaleString(undefined, { minimumFractionDigits: 2 })
}

const initCharts = () => {
  // 趋势图
  if (trendChartRef.value) {
    const c1 = echarts.init(trendChartRef.value)
    c1.setOption({
      tooltip: { trigger: 'axis' },
      legend: { data: ['GMV (元)', '订单量'], bottom: 0 },
      grid: { left: 60, right: 60, top: 20, bottom: 40 },
      xAxis: { type: 'category', data: [] },
      yAxis: [
        { type: 'value', name: 'GMV (元)' },
        { type: 'value', name: '订单量' }
      ],
      series: [
        { name: 'GMV (元)', type: 'bar', data: [], yAxisIndex: 0, itemStyle: { color: '#409EFF' } },
        { name: '订单量', type: 'line', smooth: true, data: [], yAxisIndex: 1, itemStyle: { color: '#67C23A' } }
      ]
    })
    charts.push(c1)
  }

  // 类目订单分布
  if (categoryOrderChartRef.value) {
    const c2 = echarts.init(categoryOrderChartRef.value)
    c2.setOption({
      tooltip: { trigger: 'item', formatter: '{b}: {c}单 ({d}%)' },
      series: [{
        type: 'pie', radius: ['40%', '70%'],
        label: { formatter: '{b}\n{d}%' },
        data: []
      }]
    })
    charts.push(c2)
  }

  // 类目排行
  if (categoryChartRef.value) {
    const c3 = echarts.init(categoryChartRef.value)
    c3.setOption({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      grid: { left: 80, right: 30, top: 10, bottom: 20 },
      xAxis: { type: 'value', name: 'GMV (元)' },
      yAxis: { type: 'category', data: [] },
      series: [{
        type: 'bar', barWidth: 20,
        itemStyle: {
          borderRadius: [0, 4, 4, 0],
          color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
            { offset: 0, color: '#409EFF' }, { offset: 1, color: '#79bbff' }
          ])
        },
        data: [],
        label: { show: true, position: 'right', formatter: p => '¥' + formatMoney(p.value) }
      }]
    })
    charts.push(c3)
  }

  // TOP主播
  if (anchorChartRef.value) {
    const c4 = echarts.init(anchorChartRef.value)
    c4.setOption({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      grid: { left: 100, right: 30, top: 10, bottom: 20 },
      xAxis: { type: 'value', name: 'GMV' },
      yAxis: { type: 'category', data: [], inverse: true },
      series: [{
        type: 'bar', barWidth: 16,
        itemStyle: {
          borderRadius: [0, 4, 4, 0],
          color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
            { offset: 0, color: '#67C23A' }, { offset: 1, color: '#95d475' }
          ])
        },
        data: [],
        label: { show: true, position: 'right', formatter: p => '¥' + formatMoney(p.value) }
      }]
    })
    charts.push(c4)
  }
}

const loadData = async () => {
  try {
    // 并行请求所有 API
    const [kpiRes, trendRes, categoryDistRes, categoryRes, anchorRes] = await Promise.all([
      getDashboardKpi(),
      getGmvTrend(),
      getCategoryDistribution(),
      getCategoryRank(),
      getAnchorRank(10),
    ])

    // KPI
    if (kpiRes?.data) {
      const d = kpiRes.data
      kpi.totalGmv = d.totalGmv || 0
      kpi.totalOrders = d.totalOrders || 0
      kpi.totalRooms = d.totalRooms || 0
      kpi.totalViewers = d.totalViewers || 0
    }

    // GMV 趋势
    if (trendRes?.data && charts[0]) {
      const td = trendRes.data
      const dates = td.map(d => (d.date || '').slice(5))
      const values = td.map(d => d.value || 0)
      // 估算订单量 (GMV / 均价约80元)
      const orders = values.map(v => Math.round(v / 80))
      charts[0].setOption({
        xAxis: { data: dates },
        series: [{ data: values }, { data: orders }]
      })
    }

    // 类目订单分布
    if (categoryDistRes?.data && charts[1]) {
      const colors = ['#409EFF', '#67C23A', '#E6A23C', '#F56C6C', '#909399', '#00bcd4', '#ff9800', '#9c27b0']
      const catOrderData = categoryDistRes.data.map((d, i) => ({
        value: d.value, name: d.name, itemStyle: { color: colors[i % colors.length] }
      }))
      charts[1].setOption({ series: [{ data: catOrderData }] })
    }

    // 类目排行
    if (categoryRes?.data && charts[2]) {
      const catData = categoryRes.data.reverse()
      charts[2].setOption({
        yAxis: { data: catData.map(d => d.name) },
        series: [{ data: catData.map(d => d.value) }]
      })
    }

    // TOP 主播
    if (anchorRes?.data && charts[3]) {
      const topAnchors = (anchorRes.data || []).slice(0, 10).reverse()
      charts[3].setOption({
        yAxis: { data: topAnchors.map(d => d.name) },
        series: [{ data: topAnchors.map(d => d.totalGmv || 0) }]
      })
    }

    // 类目表格 - 从 room page 按类目聚合
    const roomRes = await getRoomPage({ pageSize: 200 })
    if (roomRes?.data?.records) {
      const catMap = {}
      for (const r of roomRes.data.records) {
        const cat = r.category || '未分类'
        if (!catMap[cat]) catMap[cat] = { category: cat, rooms: 0, viewers: 0, orders: 0, gmv: 0, convSum: 0 }
        catMap[cat].rooms++
        catMap[cat].viewers += r.viewerCount || 0
        catMap[cat].orders += r.orderCount || 0
        catMap[cat].gmv += r.gmv || 0
      }
      categoryTable.value = Object.values(catMap).map(c => ({
        ...c,
        convRate: c.viewers > 0 ? (c.orders / c.viewers * 100).toFixed(2) : '0.00'
      })).sort((a, b) => b.gmv - a.gmv)
    }

    ElMessage.success('数据已刷新')
  } catch (e) {
    console.error('加载数据失败', e)
    ElMessage.warning('部分数据加载失败，请检查后端服务')
  }
}

onMounted(async () => {
  await nextTick()
  initCharts()
  loadData()
})

onBeforeUnmount(() => {
  charts.forEach(c => c?.dispose())
})
</script>

<style scoped lang="scss">
.data-analysis { display: flex; flex-direction: column; gap: 16px; }
.kpi-row { margin-bottom: 0; }
.kpi-card {
  text-align: center;
  .kpi-value { font-size: 24px; font-weight: 700; color: #409EFF; margin-bottom: 4px; }
  .kpi-label { font-size: 13px; color: #909399; }
}
.chart-row { margin-bottom: 0; }
</style>
