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
import { getAnchorPage } from '@/api'

const sortBy = ref('totalGmv')
const loading = ref(false)
const anchors = ref([])

const fetchAnchors = async () => {
  loading.value = true
  try {
    const res = await getAnchorPage({ page: 1, pageSize: 100 })
    anchors.value = (res.data.records || []).map(a => ({
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

const initDual = () => {
  if (!dualRef.value || anchors.value.length === 0) return
  const c = echarts.init(dualRef.value)
  c.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' } },
    legend: { data: ['GMV(亿)', '转化率(%)'], top: 0, textStyle: { color: 'rgba(255,255,255,0.6)' } },
    grid: { left: 60, right: 60, top: 40, bottom: 30 },
    xAxis: { type: 'category', data: anchors.value.map(a => a.name), axisLabel: { rotate: 30, color: 'rgba(255,255,255,0.5)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.15)' } } },
    yAxis: [
      { type: 'value', name: 'GMV(亿)', nameTextStyle: { color: 'rgba(255,255,255,0.5)' }, axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
      { type: 'value', name: '转化率(%)', max: 10, nameTextStyle: { color: 'rgba(255,255,255,0.5)' }, axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } }
    ],
    series: [
      { name: 'GMV(亿)', type: 'bar', data: anchors.value.map(a => +(a.totalGmv / 100000000).toFixed(1)),
        itemStyle: { color: '#00ffcc', borderRadius: [4, 4, 0, 0], shadowBlur: 6, shadowColor: 'rgba(0,255,204,0.3)' } },
      { name: '转化率(%)', type: 'line', yAxisIndex: 1, data: anchors.value.map(a => a.avgConversion),
        itemStyle: { color: '#ff4757' }, lineStyle: { width: 3, shadowBlur: 8, shadowColor: 'rgba(255,71,87,0.3)' }, symbolSize: 8,
        markPoint: { data: [{ type: 'max', name: '最高' }], label: { color: '#fff' } } }
    ]
  })
  charts.push(c)
}

const initCategoryAnchor = () => {
  if (!platformAnchorRef.value || anchors.value.length === 0) return
  const categories = ['美妆', '服饰', '食品', '数码', '家居', '母婴', '珠宝', '运动']
  const catMap = {}
  for (const cat of categories) {
    const list = anchors.value.filter(a => a.category === cat)
    catMap[cat] = {
      count: list.length,
      avgGmv: list.length > 0 ? +(list.reduce((s, a) => s + a.totalGmv, 0) / list.length / 100000000).toFixed(1) : 0
    }
  }
  const usedCats = categories.filter(c => catMap[c].count > 0)

  const c = echarts.init(platformAnchorRef.value)
  c.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' } },
    legend: { data: ['主播数', '平均GMV(亿)'], top: 0, textStyle: { color: 'rgba(255,255,255,0.6)' } },
    grid: { left: 60, right: 60, top: 40, bottom: 30 },
    xAxis: { type: 'category', data: usedCats, axisLabel: { color: 'rgba(255,255,255,0.5)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.15)' } } },
    yAxis: [
      { type: 'value', name: '人数', nameTextStyle: { color: 'rgba(255,255,255,0.5)' }, axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
      { type: 'value', name: 'GMV(亿)', nameTextStyle: { color: 'rgba(255,255,255,0.5)' }, axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } }
    ],
    series: [
      { name: '主播数', type: 'bar', data: usedCats.map(c => catMap[c].count),
        itemStyle: { color: '#a855f7', borderRadius: [4, 4, 0, 0], shadowBlur: 6, shadowColor: 'rgba(168,85,247,0.3)' } },
      { name: '平均GMV(亿)', type: 'line', yAxisIndex: 1, data: usedCats.map(c => catMap[c].avgGmv),
        itemStyle: { color: '#ffa502' }, lineStyle: { width: 3, shadowBlur: 8, shadowColor: 'rgba(255,165,2,0.3)' },
        markPoint: { data: [{ type: 'max' }], label: { color: '#fff' } } }
    ]
  })
  charts.push(c)
}

const initHeat = () => {
  if (!heatRef.value) return
  const categories = ['美妆', '全品类', '食品', '数码', '服饰', '运动', '家居', '母婴']
  const timePeriods = ['00-06时', '06-09时', '09-12时', '12-15时', '15-18时', '18-21时', '21-24时']
  const data = []
  for (let i = 0; i < timePeriods.length; i++) {
    for (let j = 0; j < categories.length; j++) {
      const count = anchors.value.filter(a => a.category === categories[j]).length * Math.floor(Math.random() * 200 + 50)
      data.push([j, i, count])
    }
  }
  const c = echarts.init(heatRef.value)
  c.setOption({
    backgroundColor: 'transparent',
    tooltip: { position: 'top', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' },
      formatter: (p) => `${timePeriods[p.value[1]]} - ${categories[p.value[0]]}<br/>订单: ${p.value[2]}单` },
    grid: { left: 80, right: 30, top: 20, bottom: 80 },
    xAxis: { type: 'category', data: categories, splitArea: { show: true, areaStyle: { color: ['rgba(255,255,255,0.02)', 'rgba(255,255,255,0.04)'] } }, axisLabel: { color: 'rgba(255,255,255,0.5)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
    yAxis: { type: 'category', data: timePeriods, splitArea: { show: true, areaStyle: { color: ['rgba(255,255,255,0.02)', 'rgba(255,255,255,0.04)'] } }, axisLabel: { color: 'rgba(255,255,255,0.5)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
    visualMap: {
      min: 0, max: 500, calculable: true,
      orient: 'horizontal', left: 'center', bottom: 0,
      itemWidth: 12, itemHeight: 100,
      textStyle: { fontSize: 11, color: 'rgba(255,255,255,0.5)' },
      inRange: { color: ['#0d2b4a', '#0a6e8a', '#00b4d8', '#00ffcc', '#f0ff00'] }
    },
    series: [{
      name: '订单量', type: 'heatmap', data,
      label: { show: true, color: '#fff', fontSize: 11 },
      emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,255,204,0.5)' } }
    }]
  })
  charts.push(c)
}

const initAll = () => {
  charts = []
  initDual()
  initCategoryAnchor()
  initHeat()
}

watch(sortBy, () => { charts.forEach(c => c?.dispose()); initAll() })

onMounted(async () => {
  await fetchAnchors()
  await nextTick()
  initAll()
  window.addEventListener('resize', () => charts.forEach(c => c?.resize()))
})

onBeforeUnmount(() => {
  charts.forEach(c => c?.dispose())
  window.removeEventListener('resize', () => charts.forEach(c => c?.resize()))
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
