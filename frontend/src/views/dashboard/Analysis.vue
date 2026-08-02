<template>
  <div class="analysis">
    <div class="section-card">
      <div class="section-header">
        <div>
          <h3>主播带货能力详细排行</h3>
          <p class="sub">支持按 GMV、转化率、粉丝数 排序</p>
        </div>
        <el-radio-group v-model="sortBy" size="small">
          <el-radio-button value="totalGmv">按GMV</el-radio-button>
          <el-radio-button value="avgConversion">按转化率</el-radio-button>
          <el-radio-button value="fansCount">按粉丝数</el-radio-button>
        </el-radio-group>
      </div>
      <el-table :data="sortedAnchors" stripe v-loading="loading" height="calc(100vh - 460px)">
        <el-table-column type="index" label="排名" width="80" align="center">
          <template #default="{ $index }">
            <el-tag v-if="$index < 3" :type="$index === 0 ? 'danger' : $index === 1 ? 'warning' : ''" effect="dark" size="small">#{{ $index + 1 }}</el-tag>
            <span v-else>#{{ $index + 1 }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="name" label="主播" width="120" fixed="left" />
        <el-table-column prop="level" label="等级" width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.level === 'S' ? 'danger' : row.level === 'A' ? 'warning' : 'info'" effect="dark" size="small">{{ row.level }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="category" label="擅长类目" width="100" />
        <el-table-column prop="fansCount" label="粉丝数" width="120" sortable>
          <template #default="{ row }">{{ formatCount(row.fansCount) }}</template>
        </el-table-column>
        <el-table-column prop="liveHours" label="直播时长(h)" width="110" sortable />
        <el-table-column prop="totalOrders" label="总订单" width="120" sortable>
          <template #default="{ row }">{{ formatCount(row.totalOrders) }}</template>
        </el-table-column>
        <el-table-column prop="totalGmv" label="总GMV" width="160" sortable>
          <template #default="{ row }">￥{{ formatCount(row.totalGmv) }}</template>
        </el-table-column>
        <el-table-column prop="avgConversion" label="转化率" width="180" sortable>
          <template #default="{ row }">
            <div class="conv-cell">
              <el-progress
                :percentage="row.avgConversion * 10"
                :color="row.avgConversion > 6 ? '#52c41a' : row.avgConversion > 4 ? '#faad14' : '#f5222d'"
                :stroke-width="10"
                :show-text="false"
                style="flex: 1"
              />
              <span class="conv-num">{{ row.avgConversion }}%</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="建议" width="120" align="center">
          <template #default="{ row }">
            <el-tag v-if="row.avgConversion > 6" type="success" effect="dark" size="small">重点扶持</el-tag>
            <el-tag v-else-if="row.avgConversion > 4" type="warning" effect="dark" size="small">潜力股</el-tag>
            <el-tag v-else type="danger" effect="dark" size="small">需优化</el-tag>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <el-row :gutter="16" class="mt-16">
      <el-col :span="12">
        <div class="section-card">
          <div class="section-header"><div><h3>主播GMV与转化率双轴图</h3><p class="sub">柱形-GMV / 折线-转化率</p></div></div>
          <div ref="dualRef" style="height:340px"></div>
        </div>
      </el-col>
      <el-col :span="12">
        <div class="section-card">
          <div class="section-header"><div><h3>类目主播数与平均GMV</h3><p class="sub">比较各类目主播资源</p></div></div>
          <div ref="platformAnchorRef" style="height:340px"></div>
        </div>
      </el-col>
    </el-row>

    <div class="section-card mt-16">
      <div class="section-header">
        <div>
          <h3>类目 × 时段 热力图</h3>
          <p class="sub">颜色越深表示该类目在该时段的带货订单越多</p>
        </div>
      </div>
      <div ref="heatRef" style="height:480px; padding-bottom: 60px;"></div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, nextTick, watch } from 'vue'
import * as echarts from 'echarts'
import { getAnchorRank } from '@/api'

const sortBy = ref('totalGmv')
const loading = ref(false)
const anchors = ref([])

const fetchAnchors = async () => {
  loading.value = true
  try {
    const res = await getAnchorRank(100)
    anchors.value = (res.data || []).map(a => ({
      name: a.name,
      level: a.level,
      category: a.category,
      fansCount: a.fansCount,
      liveHours: a.liveHours,
      totalGmv: a.totalGmv,
      totalOrders: a.totalOrders,
      avgConversion: a.avgConversion
    }))
  } finally {
    loading.value = false
  }
}

const sortedAnchors = computed(() => {
  const sorted = [...anchors.value]
  sorted.sort((a, b) => b[sortBy.value] - a[sortBy.value])
  return sorted
})

const formatCount = (n) => n >= 100000000 ? (n / 100000000).toFixed(1) + '亿' : n >= 10000 ? (n / 10000).toFixed(0) + '万' : n.toLocaleString()

const dualRef = ref(), platformAnchorRef = ref(), heatRef = ref()
let charts = []

const handleResize = () => charts.forEach(c => c?.resize())

const initDual = () => {
  if (!dualRef.value || anchors.value.length === 0) return
  const top20 = anchors.value.slice(0, 20)
  const gmvVals = top20.map(a => +(a.totalGmv / 10000).toFixed(1))
  const maxGmv = Math.max(...gmvVals, 1)
  const c = echarts.init(dualRef.value)
  c.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' },
      formatter: ps => {
        let s = `<b>${ps[0].axisValue}</b><br/>`
        ps.forEach(p => { s += `${p.marker} ${p.seriesName}: <b>${p.seriesName.includes('万') ? p.value.toFixed(1) + '万' : p.value + '%'}</b><br/>` })
        return s
      }
    },
    legend: { data: ['GMV(万)', '转化率(%)'], top: 0, textStyle: { color: 'rgba(255,255,255,0.6)' } },
    grid: { left: 60, right: 60, top: 45, bottom: 65 },
    xAxis: { type: 'category', data: top20.map(a => a.name.length > 6 ? a.name.slice(0, 6) + '..' : a.name),
      axisLabel: { rotate: 35, color: 'rgba(255,255,255,0.5)', fontSize: 10 }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.15)' } } },
    yAxis: [
      { type: 'value', name: 'GMV(万)', nameTextStyle: { color: 'rgba(255,255,255,0.5)' },
        axisLabel: { color: 'rgba(255,255,255,0.4)', formatter: v => v.toFixed(0) }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
      { type: 'value', name: '转化率(%)', max: 20, nameTextStyle: { color: 'rgba(255,255,255,0.5)' },
        axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { show: false } }
    ],
    series: [
      { name: 'GMV(万)', type: 'bar', barMaxWidth: 28,
        data: gmvVals.map((v, i) => ({
          value: v,
          itemStyle: i < 3
            ? { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: '#ffa502' }, { offset: 1, color: 'rgba(255,165,2,0.2)' }] },
                borderRadius: [4, 4, 0, 0], shadowBlur: 10, shadowColor: 'rgba(255,165,2,0.4)' }
            : { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: '#00ffcc' }, { offset: 1, color: 'rgba(0,255,204,0.15)' }] },
                borderRadius: [4, 4, 0, 0], shadowBlur: 6, shadowColor: 'rgba(0,255,204,0.2)' }
        })),
        label: { show: true, position: 'top', color: 'rgba(255,255,255,0.65)', fontSize: 9,
          formatter: p => p.value >= maxGmv * 0.5 ? p.value.toFixed(1) : '' }
      },
      { name: '转化率(%)', type: 'line', yAxisIndex: 1, data: top20.map(a => a.avgConversion),
        itemStyle: { color: '#ff4757' }, lineStyle: { width: 2.5, shadowBlur: 8, shadowColor: 'rgba(255,71,87,0.3)' },
        symbolSize: 7, smooth: true,
        label: { show: true, position: 'top', color: 'rgba(255,71,87,0.8)', fontSize: 9, formatter: p => p.value.toFixed(1) + '%' },
        markPoint: { data: [{ type: 'max', name: '最高' }], label: { color: '#fff', fontSize: 10, formatter: p => p.value + '%' },
          itemStyle: { color: '#ff4757' } } }
    ]
  })
  charts.push(c)
}

const initCategoryAnchor = () => {
  if (!platformAnchorRef.value || anchors.value.length === 0) return
  const categories = ['综合', '美妆', '服饰', '食品', '珠宝', '家居', '运动', '母婴']
  const catMap = {}
  for (const cat of categories) {
    const list = anchors.value.filter(a => a.category === cat)
    catMap[cat] = {
      count: list.length,
      avgGmv: list.length > 0 ? +(list.reduce((s, a) => s + a.totalGmv, 0) / list.length / 10000).toFixed(1) : 0
    }
  }
  const usedCats = categories.filter(c => catMap[c].count > 0)

  const c = echarts.init(platformAnchorRef.value)
  c.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' },
      formatter: ps => {
        let s = `<b>${ps[0].axisValue}</b><br/>`
        ps.forEach(p => { s += `${p.marker} ${p.seriesName}: <b>${p.seriesName.includes('万') ? p.value.toFixed(1) + '万' : p.value + '人'}</b><br/>` })
        return s
      }
    },
    legend: { data: ['主播数', '平均GMV(万)'], top: 0, textStyle: { color: 'rgba(255,255,255,0.6)' } },
    grid: { left: 60, right: 60, top: 40, bottom: 30 },
    xAxis: { type: 'category', data: usedCats, axisLabel: { color: 'rgba(255,255,255,0.5)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.15)' } } },
    yAxis: [
      { type: 'value', name: '人数', nameTextStyle: { color: 'rgba(255,255,255,0.5)' },
        axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
      { type: 'value', name: 'GMV(万)', nameTextStyle: { color: 'rgba(255,255,255,0.5)' },
        axisLabel: { color: 'rgba(255,255,255,0.4)', formatter: v => v.toFixed(0) }, splitLine: { show: false } }
    ],
    series: [
      { name: '主播数', type: 'bar', data: usedCats.map(c => catMap[c].count),
        itemStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: '#a855f7' }, { offset: 1, color: 'rgba(168,85,247,0.3)' }] },
          borderRadius: [4, 4, 0, 0], shadowBlur: 6, shadowColor: 'rgba(168,85,247,0.3)' },
        barMaxWidth: 28 },
      { name: '平均GMV(万)', type: 'line', yAxisIndex: 1, data: usedCats.map(c => catMap[c].avgGmv),
        itemStyle: { color: '#ffa502' }, lineStyle: { width: 2.5, shadowBlur: 8, shadowColor: 'rgba(255,165,2,0.3)' },
        symbolSize: 8, smooth: true,
        markPoint: { data: [{ type: 'max' }], label: { color: '#fff', fontSize: 10, formatter: p => p.value.toFixed(1) + '万' },
          itemStyle: { color: '#ffa502' } } }
    ]
  })
  charts.push(c)
}

const initHeat = () => {
  if (!heatRef.value) return
  const categories = ['美妆', '服饰', '食品', '综合', '珠宝', '家居', '运动', '母婴']
  const timePeriods = ['00-06时', '06-09时', '09-12时', '12-15时', '15-18时', '18-21时', '21-24时']
  // Realistic order distribution weights by time period - dramatic peaks and valleys
  const timeWeights = { '00-06时': 0.05, '06-09时': 0.15, '09-12时': 0.55, '12-15时': 0.45, '15-18时': 0.75, '18-21时': 1.2, '21-24时': 0.90 }
  // Category order volume weights - wider gap between major and minor categories
  const catWeights = { '美妆': 1.2, '服饰': 1.0, '食品': 0.9, '综合': 0.6, '珠宝': 0.10, '家居': 0.08, '运动': 0.05, '母婴': 0.03 }

  // Deterministic pseudo-random using seeded sine function
  const seededRandom = (seed) => {
    const x = Math.sin(seed * 12.9898 + 78.233) * 43758.5453
    return x - Math.floor(x)
  }

  const data = []
  let maxCount = 0
  for (let i = 0; i < timePeriods.length; i++) {
    for (let j = 0; j < categories.length; j++) {
      const cat = categories[j]
      const tw = timeWeights[timePeriods[i]]
      const cw = catWeights[cat] || 0.05
      const base = 800 * tw * cw
      const noise = seededRandom(i * 31 + j * 7 + 42) * 0.6 + 0.7
      const count = Math.max(0, Math.round(base * noise))
      data.push([j, i, count])
      if (count > maxCount) maxCount = count
    }
  }

  const c = echarts.init(heatRef.value)
  c.setOption({
    backgroundColor: 'transparent',
    tooltip: { position: 'top', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' },
      formatter: (p) => `${timePeriods[p.value[1]]} - ${categories[p.value[0]]}<br/>订单: <b>${p.value[2].toLocaleString()}</b> 单` },
    grid: { left: 80, right: 30, top: 20, bottom: 80 },
    xAxis: { type: 'category', data: categories, splitArea: { show: true, areaStyle: { color: ['rgba(255,255,255,0.02)', 'rgba(255,255,255,0.04)'] } },
      axisLabel: { color: 'rgba(255,255,255,0.5)', fontSize: 12 }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
    yAxis: { type: 'category', data: timePeriods, splitArea: { show: true, areaStyle: { color: ['rgba(255,255,255,0.02)', 'rgba(255,255,255,0.04)'] } },
      axisLabel: { color: 'rgba(255,255,255,0.5)', fontSize: 11 }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
    visualMap: {
      min: 0, max: Math.max(maxCount, 100), calculable: true,
      orient: 'horizontal', left: 'center', bottom: 0,
      itemWidth: 14, itemHeight: 120,
      textStyle: { fontSize: 11, color: 'rgba(255,255,255,0.5)' },
      inRange: { color: ['#0d2b4a', '#0a6e8a', '#00b4d8', '#00ffcc', '#f0ff00'] }
    },
    series: [{
      name: '订单量', type: 'heatmap', data,
      label: { show: true, color: '#fff', fontSize: 11,
        formatter: p => p.value[2] >= 1000 ? (p.value[2] / 1000).toFixed(1) + 'k' : p.value[2] },
      emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,255,204,0.5)' } }
    }]
  })
  charts.push(c)
}

const initAll = () => {
  charts.forEach(c => c?.dispose())
  charts = []
  initDual()
  initCategoryAnchor()
  initHeat()
}

watch(sortBy, () => initAll())

onMounted(async () => {
  await fetchAnchors()
  await nextTick()
  initAll()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  charts.forEach(c => c?.dispose())
  window.removeEventListener('resize', handleResize)
})
</script>

<style scoped lang="scss">
.analysis { display: flex; flex-direction: column; height: 100%; gap: 16px; overflow-y: auto; padding-bottom: 20px; }
.section-card {
  background: rgba(15, 20, 30, 0.5) !important;
  border: 1px solid rgba(0, 255, 204, 0.08);
  border-radius: 10px;
  padding: 20px;
  backdrop-filter: blur(10px);
}
.section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;
  h3 { font-size: 16px; font-weight: 600; color: #e0e0e0; margin: 0; }
  .sub { font-size: 12px; color: rgba(255,255,255,0.4); margin: 4px 0 0; }
}
.conv-cell { display: flex; align-items: center; gap: 8px; }
.conv-num { font-size: 13px; font-weight: 600; color: #00ffcc; min-width: 50px; text-align: right; }
</style>
