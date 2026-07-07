<template>
  <div class="data-analysis">
    <!-- 分析维度选择 -->
    <el-card shadow="never" class="filter-card">
      <el-form :inline="true">
        <el-form-item label="时间维度">
          <el-select v-model="filter.period" style="width:140px">
            <el-option label="近7天" value="7d" />
            <el-option label="近30天" value="30d" />
            <el-option label="近90天" value="90d" />
            <el-option label="今年" value="year" />
          </el-select>
        </el-form-item>
        <el-form-item label="区域">
          <el-select v-model="filter.region" style="width:140px">
            <el-option label="全国" value="all" />
            <el-option label="华东" value="east" />
            <el-option label="华南" value="south" />
            <el-option label="华北" value="north" />
            <el-option label="西南" value="southwest" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" icon="Search" @click="doAnalysis">开始分析</el-button>
          <el-button icon="Download" @click="exportReport">导出报表</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- 图表区域 -->
    <el-row :gutter="16" class="chart-row">
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>订单量与收入趋势</span></template>
          <div ref="trendChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>各区域订单占比</span></template>
          <div ref="regionChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16" class="chart-row">
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>延误原因分析</span></template>
          <div ref="delayChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>下季度订单预测</span></template>
          <div ref="predictChartRef" style="height:340px"></div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 成本分析表格 -->
    <el-card shadow="never">
      <template #header><span>物流成本明细分析</span></template>
      <el-table :data="costData" stripe>
        <el-table-column prop="category" label="成本类别" width="140" />
        <el-table-column prop="jan" label="1月(万元)" sortable />
        <el-table-column prop="feb" label="2月(万元)" sortable />
        <el-table-column prop="mar" label="3月(万元)" sortable />
        <el-table-column prop="apr" label="4月(万元)" sortable />
        <el-table-column prop="may" label="5月(万元)" sortable />
        <el-table-column prop="jun" label="6月(万元)" sortable />
        <el-table-column prop="total" label="合计(万元)" sortable>
          <template #default="{ row }">
            <strong>{{ row.jan + row.feb + row.mar + row.apr + row.may + row.jun }}</strong>
          </template>
        </el-table-column>
        <el-table-column prop="trend" label="趋势" width="120">
          <template #default="{ row }">
            <el-tag :type="row.jun > row.may ? 'danger' : 'success'" size="small">
              {{ row.jun > row.may ? '↑ 上升' : '↓ 下降' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'

const filter = reactive({ period: '30d', region: 'all' })

const trendChartRef = ref()
const regionChartRef = ref()
const delayChartRef = ref()
const predictChartRef = ref()
let charts = []

const costData = ref([
  { category: '运输成本', jan: 85.2, feb: 78.5, mar: 92.1, apr: 88.7, may: 95.3, jun: 102.5 },
  { category: '仓储成本', jan: 45.8, feb: 43.2, mar: 46.5, apr: 44.9, may: 47.8, jun: 46.2 },
  { category: '人力成本', jan: 62.3, feb: 60.1, mar: 63.8, apr: 62.5, may: 65.2, jun: 64.8 },
  { category: '管理成本', jan: 28.6, feb: 27.9, mar: 29.2, apr: 28.3, may: 30.1, jun: 29.5 },
  { category: 'IT运维', jan: 18.5, feb: 17.8, mar: 19.2, apr: 18.6, may: 20.3, jun: 21.5 },
  { category: '保险费用', jan: 22.1, feb: 21.5, mar: 22.8, apr: 22.2, may: 23.5, jun: 23.8 }
])

const initCharts = () => {
  if (trendChartRef.value) {
    const c1 = echarts.init(trendChartRef.value)
    c1.setOption({
      tooltip: { trigger: 'axis' },
      legend: { data: ['订单量', '收入'], bottom: 0 },
      grid: { left: 60, right: 60, top: 20, bottom: 40 },
      xAxis: { type: 'category', data: ['6/1', '6/5', '6/10', '6/15', '6/20', '6/25'] },
      yAxis: [
        { type: 'value', name: '订单量' },
        { type: 'value', name: '万元' }
      ],
      series: [
        { name: '订单量', type: 'bar', data: [420, 450, 480, 510, 530, 560], yAxisIndex: 0, itemStyle: { color: '#409EFF' } },
        { name: '收入', type: 'line', smooth: true, data: [45, 48, 52, 55, 58, 62], yAxisIndex: 1, itemStyle: { color: '#67C23A' } }
      ]
    })
    charts.push(c1)
  }

  if (regionChartRef.value) {
    const c2 = echarts.init(regionChartRef.value)
    c2.setOption({
      tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      series: [{
        type: 'pie', radius: ['45%', '75%'],
        label: { formatter: '{b}\n{d}%' },
        data: [
          { value: 1280, name: '华东', itemStyle: { color: '#409EFF' } },
          { value: 956, name: '华南', itemStyle: { color: '#67C23A' } },
          { value: 720, name: '华北', itemStyle: { color: '#E6A23C' } },
          { value: 540, name: '西南', itemStyle: { color: '#F56C6C' } },
          { value: 360, name: '其他', itemStyle: { color: '#909399' } }
        ]
      }]
    })
    charts.push(c2)
  }

  if (delayChartRef.value) {
    const c3 = echarts.init(delayChartRef.value)
    c3.setOption({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      grid: { left: 130, right: 30, top: 10, bottom: 20 },
      xAxis: { type: 'value', name: '次数' },
      yAxis: {
        type: 'category',
        data: ['交通拥堵', '天气原因', '车辆故障', '人员不足', '系统调度', '客户变更']
      },
      series: [{
        type: 'bar', barWidth: 20,
        itemStyle: {
          borderRadius: [0, 4, 4, 0],
          color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
            { offset: 0, color: '#F56C6C' }, { offset: 1, color: '#fab6b6' }
          ])
        },
        data: [256, 180, 95, 72, 58, 42],
        label: { show: true, position: 'right' }
      }]
    })
    charts.push(c3)
  }

  if (predictChartRef.value) {
    const c4 = echarts.init(predictChartRef.value)
    c4.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 60, right: 30, top: 20, bottom: 30 },
      xAxis: { type: 'category', data: ['7月', '8月', '9月'] },
      yAxis: { type: 'value', name: '订单量' },
      series: [
        {
          name: '保守预估', type: 'bar',
          data: [1800, 1950, 2100],
          itemStyle: { color: '#A0CFFF' }
        },
        {
          name: '乐观预估', type: 'bar',
          data: [2200, 2450, 2800],
          itemStyle: { color: '#409EFF' }
        }
      ]
    })
    charts.push(c4)
  }
}

const doAnalysis = () => {
  // 根据筛选条件生成不同的数据
  const multiplier = filter.period === '7d' ? 0.3 : filter.period === '90d' ? 2.5 : filter.period === 'year' ? 8 : 1
  const regionFilter = filter.region

  // 更新趋势图
  if (charts[0]) {
    charts[0].setOption({
      xAxis: { data: ['6/1', '6/5', '6/10', '6/15', '6/20', '6/25'] },
      series: [
        { data: [420, 450, 480, 510, 530, 560].map(v => Math.round(v * multiplier)) },
        { data: [45, 48, 52, 55, 58, 62].map(v => Math.round(v * multiplier)) }
      ]
    })
  }

  // 更新区域占比
  if (charts[1]) {
    charts[1].setOption({
      series: [{
        data: [
          { value: Math.round(1280 * multiplier), name: '华东' },
          { value: Math.round(956 * multiplier), name: '华南' },
          { value: Math.round(720 * multiplier), name: '华北' },
          { value: Math.round(540 * multiplier), name: '西南' },
          { value: Math.round(360 * multiplier), name: '其他' }
        ]
      }]
    })
  }

  // 更新延误原因
  if (charts[2]) {
    charts[2].setOption({
      series: [{
        data: [256, 180, 95, 72, 58, 42].map(v => Math.round(v * multiplier))
      }]
    })
  }

  // 更新预测
  if (charts[3]) {
    charts[3].setOption({
      series: [
        { data: [1800, 1950, 2100].map(v => Math.round(v * multiplier)) },
        { data: [2200, 2450, 2800].map(v => Math.round(v * multiplier)) }
      ]
    })
  }

  // 更新成本表格
  costData.value = costData.value.map(row => ({
    ...row,
    jan: +(row.jan * (0.8 + Math.random() * 0.4)).toFixed(1),
    feb: +(row.feb * (0.8 + Math.random() * 0.4)).toFixed(1),
    mar: +(row.mar * (0.8 + Math.random() * 0.4)).toFixed(1),
    apr: +(row.apr * (0.8 + Math.random() * 0.4)).toFixed(1),
    may: +(row.may * (0.8 + Math.random() * 0.4)).toFixed(1),
    jun: +(row.jun * (0.8 + Math.random() * 0.4)).toFixed(1)
  }))

  const regionLabel = regionFilter === 'all' ? '全国' : regionFilter === 'east' ? '华东' : regionFilter === 'south' ? '华南' : regionFilter === 'north' ? '华北' : '西南'
  ElMessage.success(`分析完成：${filter.period} / ${regionLabel}（图表已更新）`)
}

const exportReport = () => {
  ElMessage.success('报表已开始导出，请稍候...')
}

onMounted(async () => {
  await nextTick()
  initCharts()
})

onBeforeUnmount(() => {
  charts.forEach(c => c?.dispose())
})
</script>

<style scoped lang="scss">
.data-analysis { display: flex; flex-direction: column; gap: 16px; }
.chart-row { margin-bottom: 0; }
</style>
