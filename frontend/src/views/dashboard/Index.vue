<template>
  <div class="dashboard">
    <div class="header-strip">
      <div>
        <h2>┃ 数据看板</h2>
        <p>实时数据源：虚拟机 MySQL · 192.168.104.100:3306 · livecommerce_db</p>
      </div>
      <button class="refresh-btn" @click="fetchAll">
        ⟳ 刷新数据 <span class="countdown" v-if="refreshCountdown > 0">{{ refreshCountdown }}s</span>
      </button>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card" v-for="k in kpis" :key="k.label" :style="{ borderColor: k.color }">
        <div class="kpi-glow" :style="{ boxShadow: `0 0 30px ${k.color}22, 0 0 8px ${k.color}44` }"></div>
        <div class="kpi-header">
          <span class="kpi-label">{{ k.label }}</span>
        </div>
        <p class="kpi-value">{{ k.value }}</p>
        <p class="kpi-sub">{{ k.sub }}</p>
      </div>
    </div>

    <div class="chart-grid">
      <div class="chart-box"><div class="chart-title">┃ GMV趋势（近30天）</div><div ref="c1" style="height:260px"></div></div>
      <div class="chart-box"><div class="chart-title">┃ 类目订单分布</div><div ref="c2" style="height:260px"></div></div>
      <div class="chart-box"><div class="chart-title">┃ 主播GMV排行 TOP10</div><div ref="c3" style="height:260px"></div></div>
      <div class="chart-box"><div class="chart-title">┃ 类目观众排行</div><div ref="c4" style="height:260px"></div></div>
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

    <div class="demo-banner" v-if="isDemoMode">
      <span>◇ 后端服务不可用，当前显示示例数据 · 启动后端后自动切换为真实数据</span>
    </div>

    <div class="data-summary">
      <span>数据总量：{{ summary.rooms }} 个直播间 | {{ summary.anchors }} 位主播 | {{ summary.orders }} 条订单<span v-if="summary.danmaku"> | {{ summary.danmaku.toLocaleString() }} 条弹幕</span> | 最新采集：{{ summary.last }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import { getDashboardKpi, getCategoryDistribution, getAnchorRank, getCategoryRank, getGmvTrend, getActivities, getDanmakuSummary } from '@/api'
import { fallback } from '@/utils/fallback'

const isDemoMode = ref(false)
const refreshCountdown = ref(30)

const kpis = ref([
  { label: '总GMV', value: '--', sub: '抖音带货累计成交额', color: '#00ffcc' },
  { label: '直播间', value: '--', sub: '已收录带货直播间', color: '#00d9ff' },
  { label: '主播数', value: '--', sub: '带货主播总数', color: '#7c3aed' },
  { label: '观众', value: '--', sub: '累计观看人次', color: '#ff4757' },
  { label: '转化率', value: '--', sub: '观众-下单平均转化', color: '#ffa502' },
  { label: '订单数', value: '--', sub: '累计成交订单', color: '#1e90ff' }
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

const summary = ref({ rooms: 0, anchors: 0, orders: 0, danmaku: 0, last: '--' })
const c1 = ref(), c2 = ref(), c3 = ref(), c4 = ref(), c5 = ref()
let charts = []

function fmtNum(n) {
  if (n >= 1e8) return (n / 1e8).toFixed(1) + '亿'
  if (n >= 1e4) return (n / 1e4).toFixed(1) + '万'
  return n.toLocaleString()
}

async function fetchAll() {
  refreshCountdown.value = 30
  try {
    // KPI data
    const kpi = await getDashboardKpi().catch(() => { isDemoMode.value = true; return fallback.kpi() })
    const k = kpi.data
    const gmvYi = k.totalGmv / 1e8
    kpis.value[0].value = '￥' + (gmvYi < 1 ? (k.totalGmv/1e4).toFixed(1)+'万' : gmvYi.toFixed(1)+'亿')
    kpis.value[1].value = (k.totalRooms || 0).toLocaleString()
    kpis.value[2].value = (k.totalAnchors || 0).toLocaleString()
    kpis.value[3].value = fmtNum(k.totalViewers || 0)
    kpis.value[4].value = (k.avgConversion || 0).toFixed(1) + '%'
    kpis.value[5].value = (k.totalOrders || 0).toLocaleString()
    summary.value.rooms = (k.totalRooms || 0).toLocaleString()
    summary.value.anchors = (k.totalAnchors || 0).toLocaleString()
    summary.value.orders = (k.totalOrders || 0).toLocaleString()

    // Fetch chart data in parallel - request 100 anchors for better distribution
    const [pf, an, cat] = await Promise.all([
      getCategoryDistribution().catch(() => fallback.categoryDistribution()),
      getAnchorRank(100).catch(() => fallback.anchorRank()),
      getCategoryRank().catch(() => fallback.categoryRank())
    ])
    summary.value.last = new Date().toLocaleTimeString()

    // Try to get danmaku count
    try {
      const dmStats = await getDanmakuSummary().catch(() => null)
      if (dmStats && dmStats.data) {
        summary.value.danmaku = dmStats.data.totalMessages || dmStats.data.total || 0
      }
    } catch { /* ignore */ }

    charts.forEach(c => c?.dispose()); charts = []

    const darkTheme = {
      backgroundColor: 'transparent',
      textStyle: { color: 'rgba(255,255,255,0.6)', fontSize: 10 },
      grid: { left: 50, right: 30, top: 20, bottom: 30 },
      tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)', textStyle: { color: '#e0e0e0' } }
    }

    // ── c1: GMV趋势 (line chart) ──
    try {
      const gmvRes = await getGmvTrend().catch(() => fallback.gmvTrend())
      const gmvData = gmvRes.data || []
      if (c1.value) charts.push(echarts.init(c1.value).setOption({
        ...darkTheme,
        xAxis: { type: 'category', data: gmvData.map(d => d.date?.slice(5) || ''), axisLabel: { color: 'rgba(255,255,255,0.3)', interval: 5 }, axisLine: { lineStyle: { color: 'rgba(0,255,204,0.1)' } } },
        yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.3)', formatter: v => v >= 1e4 ? (v/1e4).toFixed(0)+'万' : v }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
        series: [{
          type: 'line', smooth: true, symbol: 'none',
          areaStyle: { opacity: 0.15, color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [{ offset: 0, color: '#00ffcc' }, { offset: 1, color: 'rgba(0,255,204,0)' }] } },
          lineStyle: { color: '#00ffcc', width: 2, shadowBlur: 10, shadowColor: '#00ffcc' },
          data: gmvData.map(d => d.value || 0)
        }]
      }))
    } catch (e) {
      if (c1.value) charts.push(echarts.init(c1.value).setOption({
        backgroundColor: 'transparent',
        title: { text: '暂无趋势数据', left: 'center', top: 'middle', textStyle: { color: 'rgba(255,255,255,0.3)' } }
      }))
    }

    // ── c2: 类目订单分布 (rose pie) ──
    const pfData = pf.data || []
    if (c2.value) {
      if (pfData.length === 0) {
        charts.push(echarts.init(c2.value).setOption({
          backgroundColor: 'transparent',
          title: { text: '暂无数据', left: 'center', top: 'middle', textStyle: { color: 'rgba(255,255,255,0.3)' } }
        }))
      } else {
        charts.push(echarts.init(c2.value).setOption({
          backgroundColor: 'transparent',
          tooltip: { trigger: 'item', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,255,204,0.3)',
            formatter: p => `${p.name}<br/>直播间: <b>${p.value}</b> 个 (${p.percent}%)` },
          series: [{
            type: 'pie', roseType: 'radius', radius: ['30%', '70%'], center: ['50%', '55%'],
            label: { formatter: '{b}\n{d}%', fontSize: 10, color: 'rgba(255,255,255,0.6)' },
            data: pfData,
            color: ['#00ffcc', '#00d9ff', '#a855f7', '#ffa502', '#ff4757']
          }]
        }))
      }
    }

    // ── c3: 主播GMV排行 TOP10 (horizontal bar) ──
    const anList = an.data || []
    if (c3.value) {
      const top10 = anList.slice(0, 10)
      const maxGmv = top10.length > 0 ? top10[0].totalGmv : 1
      const useYi = maxGmv >= 1e8
      charts.push(echarts.init(c3.value).setOption({
        ...darkTheme, grid: { ...darkTheme.grid, left: 80, right: 60 },
        xAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.3)', formatter: v => useYi ? (v/1e8).toFixed(1)+'亿' : (v/1e4).toFixed(0)+'万' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
        yAxis: { type: 'category', inverse: true, data: top10.map(a => a.name), axisLabel: { color: 'rgba(255,255,255,0.6)', width: 70, overflow: 'truncate' } },
        series: [{
          type: 'bar', data: top10.map(a => +(a.totalGmv || 0).toFixed(2)),
          itemStyle: {
            borderRadius: [0, 3, 3, 0],
            color: { type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
              colorStops: [{ offset: 0, color: '#00ffcc' }, { offset: 1, color: '#00d9ff' }] }
          },
          label: { show: true, position: 'right', color: 'rgba(255,255,255,0.5)', fontSize: 10,
            formatter: p => useYi ? (p.value/1e8).toFixed(1)+'亿' : (p.value/1e4).toFixed(0)+'万' }
        }]
      }))
    }

    // ── c4: 类目观众排行 (horizontal bar - replaced donut) ──
    const catData = cat.data || []
    if (c4.value) {
      if (catData.length === 0) {
        charts.push(echarts.init(c4.value).setOption({
          backgroundColor: 'transparent',
          title: { text: '暂无数据', left: 'center', top: 'middle', textStyle: { color: 'rgba(255,255,255,0.3)' } }
        }))
      } else {
        const catTop = catData.slice(0, 8)
        charts.push(echarts.init(c4.value).setOption({
          ...darkTheme, grid: { ...darkTheme.grid, left: 80, right: 55 },
          tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(0,215,255,0.3)', textStyle: { color: '#e0e0e0' },
            formatter: p => `${p[0].name}<br/>累计观众: <b>${fmtNum(p[0].value)}</b>` },
          xAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.3)', formatter: v => fmtNum(v) }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
          yAxis: { type: 'category', inverse: true, data: catTop.map(c => c.name), axisLabel: { color: 'rgba(255,255,255,0.6)', width: 70, overflow: 'truncate' } },
          series: [{
            type: 'bar', data: catTop.map((c, i) => ({
              value: c.value || 0,
              itemStyle: {
                borderRadius: [0, 3, 3, 0],
                color: { type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
                  colorStops: [
                    { offset: 0, color: ['#00ffcc','#a855f7','#00d9ff','#ff4757','#ffa502','#1e90ff','#ff6b6b','#2ed573'][i % 8] },
                    { offset: 1, color: ['#00d9ff','#7c3aed','#00ffcc','#ffa502','#ff4757','#a855f7','#ff4757','#00ffcc'][i % 8] }
                  ]
                }
              }
            })),
            barWidth: '55%',
            label: { show: true, position: 'right', color: 'rgba(255,255,255,0.45)', fontSize: 10,
              formatter: p => fmtNum(p.value) }
          }]
        }))
      }
    }

    // ── c5: 转化率分布 (bar with fixed business-meaningful bins) ──
    if (c5.value) {
      const allVals = anList.map(a => Number(a.avgConversion || a.avg_conversion || 0)).filter(v => v > 0)
      if (allVals.length === 0) {
        charts.push(echarts.init(c5.value).setOption({
          backgroundColor: 'transparent',
          title: { text: '暂无转化率数据', left: 'center', top: 'middle', textStyle: { color: 'rgba(255,255,255,0.3)' } }
        }))
      } else {
        // Fixed bins that make business sense for live commerce
        const binDefs = [
          { label: '0-1%', lo: 0, hi: 1 },
          { label: '1-2%', lo: 1, hi: 2 },
          { label: '2-4%', lo: 2, hi: 4 },
          { label: '4-6%', lo: 4, hi: 6 },
          { label: '6-10%', lo: 6, hi: 10 },
          { label: '>10%', lo: 10, hi: 9999 }
        ]
        const counts = binDefs.map(b => allVals.filter(v => v >= b.lo && v < b.hi).length)
        const totalAnchors = allVals.length
        const avg = (allVals.reduce((s, v) => s + v, 0) / totalAnchors).toFixed(1)
        const median = allVals.sort((a, b) => a - b)[Math.floor(allVals.length / 2)].toFixed(1)

        charts.push(echarts.init(c5.value).setOption({
          backgroundColor: 'transparent',
          tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,20,30,0.95)', borderColor: 'rgba(168,85,247,0.4)', textStyle: { color: '#fff' },
            formatter: p => `转化率 ${p[0].name}<br/>主播数: <b>${p[0].value}</b> 位 (${(p[0].value/totalAnchors*100).toFixed(0)}%)` },
          grid: { left: 45, right: 20, top: 40, bottom: 35 },
          xAxis: { type: 'category', data: binDefs.map(b => b.label), axisLabel: { color: 'rgba(255,255,255,0.6)', fontSize: 10, interval: 0 }, axisLine: { lineStyle: { color: 'rgba(168,85,247,0.2)' } } },
          yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.4)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } } },
          series: [{
            type: 'bar', data: counts, barWidth: '55%',
            itemStyle: { borderRadius: [4, 4, 0, 0],
              color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
                colorStops: [
                  { offset: 0, color: '#a855f7' },
                  { offset: 0.5, color: '#7c3aed' },
                  { offset: 1, color: '#4c1d95' }
                ] },
              shadowBlur: 8, shadowColor: 'rgba(168,85,247,0.4)' },
            label: { show: true, position: 'top', color: '#c084fc', fontWeight: 'bold', fontSize: 11, formatter: p => p.value > 0 ? p.value : '' }
          }],
          graphic: [{
            type: 'group', right: 10, top: 6,
            children: [
              { type: 'rect', shape: { width: 130, height: 28, r: 4 },
                style: { fill: 'rgba(0, 255, 204, 0.10)', stroke: 'rgba(0, 255, 204, 0.4)', lineWidth: 1 } },
              { type: 'text', left: 8, top: 7,
                style: { text: `均值 ${avg}%  |  中位 ${median}%`, fill: '#00ffcc', font: '11px sans-serif' } }
            ]
          }]
        }))
      }
    }
  } catch (e) { console.error('[Dashboard] fetch error:', e) }
}

async function fetchActivities() {
  try {
    const acts = await getActivities().catch(() => fallback.activities())
    if (acts && acts.data && acts.data.length > 0) activities.value = acts.data
  } catch (e) {}
}

let autoTimer = null
let activityTimer = null
let countdownTimer = null
onMounted(() => {
  fetchAll()
  fetchActivities()
  autoTimer = setInterval(fetchAll, 30000)
  activityTimer = setInterval(fetchActivities, 60000)
  countdownTimer = setInterval(() => {
    if (refreshCountdown.value > 0) refreshCountdown.value--
  }, 1000)
})
onBeforeUnmount(() => {
  charts.forEach(c => c?.dispose())
  if (autoTimer) clearInterval(autoTimer)
  if (activityTimer) clearInterval(activityTimer)
  if (countdownTimer) clearInterval(countdownTimer)
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
.countdown { font-size: 10px; color: rgba(0,255,204,0.4); margin-left: 4px; font-family: 'Courier New', monospace; }

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

.activity-list { display: flex; flex-direction: column; gap: 10px; max-height: 280px; overflow-y: auto; padding-right: 4px; }
.activity-list::-webkit-scrollbar { width: 3px; }
.activity-list::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 2px; }
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
.demo-banner {
  padding: 8px 16px; background: rgba(255,165,2,0.1); border: 1px solid rgba(255,165,2,0.3);
  border-radius: 6px; text-align: center; font-size: 12px; color: #ffa502; animation: pulse-border 2s infinite;
}
@keyframes pulse-border { 0%,100% { border-color: rgba(255,165,2,0.3); } 50% { border-color: rgba(255,165,2,0.6); } }
</style>
