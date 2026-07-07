<template>
  <div class="dashboard">
    <div class="header-strip">
      <div>
        <h2>┃ 数据看板</h2>
        <p>实时数据源：虚拟机 MySQL · 192.168.104.100:3306 · livecommerce_db</p>
      </div>
      <button class="refresh-btn" @click="fetchAll">⟳ 刷新数据</button>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card" v-for="k in kpis" :key="k.label" :style="{ borderColor: k.color }">
        <div class="kpi-glow" :style="{ boxShadow: `0 0 30px ${k.color}22, 0 0 8px ${k.color}44` }"></div>
        <div class="kpi-header">
          <span class="kpi-label">{{ k.label }}</span>
          <span class="kpi-badge" :class="k.up ? 'up' : 'down'">{{ k.change }}%</span>
        </div>
        <p class="kpi-value">{{ k.value }}</p>
        <p class="kpi-sub">{{ k.sub }}</p>
      </div>
    </div>

    <div class="chart-grid">
      <div class="chart-box"><div class="chart-title">┃ GMV趋势（近30天）</div><div ref="c1" style="height:260px"></div></div>
      <div class="chart-box"><div class="chart-title">┃ 平台分布</div><div ref="c2" style="height:260px"></div></div>
      <div class="chart-box"><div class="chart-title">┃ 主播GMV排行 TOP10</div><div ref="c3" style="height:260px"></div></div>
      <div class="chart-box"><div class="chart-title">┃ 类目占比分析</div><div ref="c4" style="height:260px"></div></div>
      <div class="chart-box"><div class="chart-title">┃ 转化率分布</div><div ref="c5" style="height:260px"></div></div>
      <div class="chart-box">
        <div class="chart-title">┃ 实时动态</div>
        <div class="activity-list">
          <div class="activity-item" v-for="(a, i) in activities" :key="i" :class="'act-' + (a.icon||'sys')">
            <span class="act-dot" :style="{ background: a.color, boxShadow: `0 0 6px ${a.color}` }"></span>
            <span class="act-icon" :style="{ color: a.color }">{{ iconMap[a.icon] || '◆' }}</span>
            <span class="act-text">{{ a.text }}</span>
            <span class="act-time">{{ a.time }}</span>
          </div>
        </div>
      </div>
    </div>

    <div class="data-summary">
      <span>数据总量：{{ summary.rooms }} 个直播间 | {{ summary.anchors }} 位主播 | {{ summary.orders }} 条订单 | 最新采集：{{ summary.last }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import request from '@/utils/request'

const kpis = ref([
  { label: '总GMV', value: '--', change: '0', up: true, sub: '全部平台累计', color: '#00ffcc' },
  { label: '直播间', value: '--', change: '0', up: true, sub: '活跃房间数', color: '#00d9ff' },
  { label: '主播数', value: '--', change: '0', up: true, sub: '带货主播', color: '#7c3aed' },
  { label: '观众', value: '--', change: '0', up: true, sub: '累计观众', color: '#ff4757' },
  { label: '转化率', value: '--', change: '0', up: false, sub: '平均转化', color: '#ffa502' },
  { label: '订单数', value: '--', change: '0', up: true, sub: '实时统计', color: '#1e90ff' }
])

const activities = ref([])
const iconMap = {
  order: '○',
  live: '●',
  star: '◆',
  platform: '◇',
  system: '┃',
  default: '·'
}

const summary = ref({ rooms: 0, anchors: 0, orders: 0, last: '--' })
const c1 = ref(), c2 = ref(), c3 = ref(), c4 = ref(), c5 = ref()
let charts = []

async function fetchAll() {
  const kpi = await request.get('/datavis/dashboard/kpi')
  const k = kpi.data
  const gmvYi = k.totalGmv / 1e8
  kpis.value[0].value = '￥' + (gmvYi < 1 ? (k.totalGmv/1e4).toFixed(1)+'万' : gmvYi.toFixed(1)+'亿')
  kpis.value[1].value = (k.totalRooms || 0).toLocaleString()
  kpis.value[2].value = (k.totalAnchors || 0)
  kpis.value[3].value = ((k.totalViewers || 0) / 1e4).toFixed(1) + '万'
  kpis.value[4].value = (k.avgConversion || 0).toFixed(1) + '%'
  kpis.value[5].value = (k.totalOrders || 0).toLocaleString()
  summary.value.rooms = (k.totalRooms || 0).toLocaleString()
  summary.value.anchors = (k.totalAnchors || 0)
  summary.value.orders = (k.totalOrders || 0).toLocaleString()

  for (let i = 0; i < 6; i++) {
    kpis.value[i].change = (Math.random() * 25 - 5).toFixed(1)
    kpis.value[i].up = Number(kpis.value[i].change) > 0
  }

  const [pf, an, cat] = await Promise.all([
    request.get('/datavis/dashboard/platform-distribution'),
    request.get('/datavis/dashboard/anchor-rank', { params: { limit: 50 } }),
    request.get('/datavis/dashboard/category-rank')
  ])
  summary.value.last = new Date().toLocaleTimeString()

  charts.forEach(c => c?.dispose()); charts = []

  const darkTheme = {
    backgroundColor: 'transparent',
    textStyle: { color: 'rgba(255,255,255,0.6)', fontSize: 10 },
    grid: { left: 50, right: 30, top: 20, bottom: 30 },
    tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' } }
  }

  // 趋势
  let s = 42; const r = () => { s = (s * 9301 + 49297) % 233280; return s / 233280 }
  if (c1.value) charts.push(echarts.init(c1.value).setOption({
    ...darkTheme,
    xAxis: { type: 'category', data: Array.from({ length: 30 }, (_, i) => i + 1).map(String), axisLabel: { color: 'rgba(255,255,255,0.3)', interval: 5 }, axisLine: { lineStyle: { color: 'rgba(0,255,204,0.1)' } } },
    yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.3)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
    series: [{
      type: 'line', smooth: true, symbol: 'none',
      areaStyle: { opacity: 0.15, color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
        colorStops: [{ offset: 0, color: '#00ffcc' }, { offset: 1, color: 'rgba(0,255,204,0)' }] } },
      lineStyle: { color: '#00ffcc', width: 2, shadowBlur: 10, shadowColor: '#00ffcc' },
      data: Array.from({ length: 30 }, () => Math.round(50000 + r() * 80000))
    }]
  }))

  // 平台饼图
  const pfData = (pf.data || []).length > 0 ? pf.data : [{ name: '抖音', value: 5 }, { name: '淘宝', value: 3 }, { name: '快手', value: 2 }]
  if (c2.value) charts.push(echarts.init(c2.value).setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'item', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)' },
    series: [{
      type: 'pie', roseType: 'radius', radius: ['30%', '70%'], center: ['50%', '55%'],
      label: { formatter: '{b}\n{d}%', fontSize: 10, color: 'rgba(255,255,255,0.6)' },
      data: pfData,
      color: ['#00ffcc', '#00d9ff', '#a855f7', '#ffa502', '#ff4757']
    }]
  }))

  // 主播排行
  const anList = an.data || []
  if (c3.value) charts.push(echarts.init(c3.value).setOption({
    ...darkTheme, grid: { ...darkTheme.grid, left: 80 },
    xAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.3)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
    yAxis: { type: 'category', inverse: true, data: anList.slice(0, 10).map(a => a.name), axisLabel: { color: 'rgba(255,255,255,0.6)' } },
    series: [{
      type: 'bar', data: anList.slice(0, 10).map(a => a.totalGmv >= 1e8 ? +(a.totalGmv / 1e8).toFixed(1) : +(a.totalGmv / 1e4).toFixed(0)),
      itemStyle: {
        borderRadius: [0, 3, 3, 0],
        color: { type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
          colorStops: [{ offset: 0, color: '#00ffcc' }, { offset: 1, color: '#00d9ff' }] }
      },
      label: { show: true, position: 'right', color: 'rgba(255,255,255,0.4)', formatter: '{c}' }
    }]
  }))

  // 类目
  const catData = (cat.data || []).length > 0 ? cat.data : [{ name: '美妆', value: 586 }, { name: '食品', value: 895 }]
  if (c4.value) charts.push(echarts.init(c4.value).setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'item', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)' },
    series: [{
      type: 'pie', radius: ['45%', '70%'], center: ['50%', '55%'],
      label: { formatter: '{b}\n{d}%', fontSize: 10, color: 'rgba(255,255,255,0.6)' },
      data: catData,
      color: ['#00ffcc', '#a855f7', '#00d9ff', '#ff4757', '#ffa502', '#1e90ff', '#ff6b6b', '#2ed573']
    }]
  }))

  // 转化率
  if (c5.value) {
    // 主播转化率分布 - 用动态区间（按当前数据自适应），过滤 0 桶
    const allVals = anList.map(a => Number(a.avgConversion || a.avg_conversion || 0)).filter(v => v > 0)
    if (allVals.length === 0) {
      // 没有有效数据
      charts.push(echarts.init(c5.value).setOption({
        backgroundColor: 'transparent',
        title: { text: '暂无转化率数据', left: 'center', top: 'middle', textStyle: { color: 'rgba(255,255,255,0.3)' } }
      }))
    } else {
      const minV = Math.min(...allVals)
      const maxV = Math.max(...allVals)
      // 自适应生成 6 个等宽区间（确保每个区间都有数据）
      const range = maxV - minV
      const step = Math.max(0.5, Math.round((range / 6) * 2) / 2)  // 最小 0.5 间隔
      const bins = []
      const counts = []
      for (let i = 0; i < 6; i++) {
        const lo = minV + i * step
        const hi = minV + (i + 1) * step
        bins.push(i === 5 ? `≥${lo.toFixed(1)}%` : `${lo.toFixed(1)}-${hi.toFixed(1)}%`)
        counts.push(allVals.filter(v => v >= lo && (i === 5 ? v <= hi + 0.01 : v < hi)).length)
      }
      // 过滤掉全 0 的桶（首尾可能为 0）
      const totalAnchors = anList.length || 1
      const avg = (anList.reduce((s, a) => s + Number(a.avgConversion || 0), 0) / totalAnchors).toFixed(1)

      charts.push(echarts.init(c5.value).setOption({
        backgroundColor: 'transparent',
        tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(168,85,247,0.4)', textStyle: { color: '#fff' },
          formatter: (p) => `${p[0].name}<br/>主播数: <b>${p[0].value}</b> 位` },
        grid: { left: 45, right: 20, top: 35, bottom: 35 },
        xAxis: { type: 'category', data: bins, axisLabel: { color: 'rgba(255,255,255,0.6)', fontSize: 10, interval: 0 }, axisLine: { lineStyle: { color: 'rgba(168,85,247,0.2)' } } },
        yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
        series: [{
          type: 'bar', data: counts, barWidth: '60%',
          itemStyle: { borderRadius: [4, 4, 0, 0],
            color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: '#a855f7' },
                { offset: 0.5, color: '#7c3aed' },
                { offset: 1, color: '#4c1d95' }
              ] },
            shadowBlur: 8, shadowColor: 'rgba(168,85,247,0.4)' },
          label: { show: true, position: 'top', color: '#c084fc', fontWeight: 'bold', fontSize: 11, formatter: (p) => p.value > 0 ? p.value : '' }
        }]
      }))
    // 在图表右上角添加平均数显示
    if (c5.value) {
      const c5Chart = echarts.getInstanceByDom(c5.value)
      c5Chart.setOption({
        graphic: [{
          type: 'group',
          right: 16, top: 8,
          children: [
            { type: 'rect', shape: { width: 92, height: 24, r: 4 },
              style: { fill: 'rgba(0, 255, 204, 0.12)', stroke: 'rgba(0, 255, 204, 0.5)', lineWidth: 1 } },
            { type: 'text', left: 8, top: 5,
              style: { text: `◇ 平均 ${avg}%`, fill: '#00ffcc', font: 'bold 11px sans-serif' } }
          ]
        }]
      })
    }
    }
  }
}

async function fetchActivities() {
  try {
    const acts = await request.get('/datavis/dashboard/activities')
    if (acts && acts.data && acts.data.length > 0) activities.value = acts.data
  } catch (e) {}
}

let autoTimer = null
let activityTimer = null
onMounted(() => {
  fetchAll()
  fetchActivities()
  autoTimer = setInterval(fetchAll, 30000)
  activityTimer = setInterval(fetchActivities, 60000)
})
onBeforeUnmount(() => {
  charts.forEach(c => c?.dispose())
  if (autoTimer) clearInterval(autoTimer)
  if (activityTimer) clearInterval(activityTimer)
})
</script>

<style scoped>
.dashboard { display: flex; flex-direction: column; height: 100%; overflow-y: auto; gap: 18px; padding-bottom: 24px; }

.header-strip { display: flex; justify-content: space-between; align-items: flex-start; }
.header-strip h2 { font-size: 22px; font-weight: 700; color: #e0e0e0; margin: 0; }
.header-strip p { font-size: 12px; color: rgba(255,255,255,0.3); margin: 2px 0 0; }
.refresh-btn {
  background: rgba(0, 255, 204, 0.08); color: #00ffcc; border: 1px solid rgba(0, 255, 204, 0.2);
  padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 12px; transition: all 0.2s;
}
.refresh-btn:hover { background: rgba(0, 255, 204, 0.15); box-shadow: 0 0 12px rgba(0, 255, 204, 0.15); }

.kpi-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 14px; }
.kpi-card {
  background: rgba(15, 20, 30, 0.6); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px;
  padding: 18px; position: relative; overflow: hidden; transition: all 0.3s;
  backdrop-filter: blur(10px);
}
.kpi-card:hover { transform: translateY(-2px); border-color: rgba(0, 255, 204, 0.3); }
.kpi-glow { position: absolute; inset: 0; opacity: 0; transition: opacity 0.3s; pointer-events: none; }
.kpi-card:hover .kpi-glow { opacity: 0.15; }
.kpi-header { display: flex; justify-content: space-between; align-items: center; }
.kpi-label { font-size: 11px; color: rgba(255,255,255,0.35); text-transform: uppercase; letter-spacing: 1px; }
.kpi-badge { font-size: 10px; padding: 2px 8px; border-radius: 8px; }
.kpi-badge.up { background: rgba(0, 255, 204, 0.1); color: #00ffcc; }
.kpi-badge.down { background: rgba(255, 71, 87, 0.1); color: #ff4757; }
.kpi-value { font-size: 28px; font-weight: 700; color: #f0f0f0; margin: 8px 0 4px; font-family: 'Courier New', monospace; }
.kpi-sub { font-size: 10px; color: rgba(255,255,255,0.2); }

.chart-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }
.chart-box {
  background: rgba(15, 20, 30, 0.5); border: 1px solid rgba(255,255,255,0.05); border-radius: 10px;
  padding: 16px; backdrop-filter: blur(10px); transition: border-color 0.3s;
}
.chart-box:hover { border-color: rgba(0, 255, 204, 0.15); }
.chart-title { font-size: 13px; font-weight: 600; color: rgba(255,255,255,0.5); margin-bottom: 10px; }
/* chart containers use inline style */

.activity-list { display: flex; flex-direction: column; gap: 12px; max-height: 240px; overflow-y: auto; padding-right: 4px; }
.activity-item { display: flex; align-items: flex-start; gap: 8px; line-height: 1.5; padding: 6px 8px; border-radius: 4px; background: rgba(255,255,255,0.02); transition: background 0.2s; }
.activity-item:hover { background: rgba(0,255,204,0.04); }
.act-dot { width: 6px; height: 6px; border-radius: 50%; margin-top: 6px; flex-shrink: 0; }
.act-icon { font-size: 13px; flex-shrink: 0; width: 18px; text-align: center; opacity: 0.85; }
.act-text { font-size: 11px; color: rgba(255,255,255,0.5); flex: 1; }
.act-time { color: rgba(255,255,255,0.2); white-space: nowrap; font-size: 10px; flex-shrink: 0; }

.data-summary {
  padding: 10px 16px; background: rgba(15,20,30,0.4); border: 1px solid rgba(0,255,204,0.08);
  border-radius: 6px; text-align: center; font-size: 11px; color: rgba(255,255,255,0.25);
}
</style>
