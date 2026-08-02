<template>
  <div class="anchor-search">
    <el-card class="search-card">
      <div class="search-header">
        <h2 class="search-title">带货主播搜索</h2>
        <p class="search-desc">搜索数据库中的带货主播，查看直播状态和弹幕数据</p>
      </div>
      <div class="stats-row">
        <div class="stat-chip"><span class="stat-num">{{ stats.totalAnchors }}</span> 位主播</div>
        <div class="stat-chip"><span class="stat-num">{{ stats.totalRooms }}</span> 个房间</div>
        <div class="stat-chip live" v-if="stats.liveRooms"><span class="stat-num">{{ stats.liveRooms }}</span> 直播中</div>
        <div style="flex:1"></div>
        <el-button type="success" :loading="crawling" @click="startCrawl" :disabled="crawling">
          {{ crawling ? '爬取中...' : '发现更多主播' }}
        </el-button>
        <span v-if="crawlMsg" class="crawl-msg" :class="crawlOk?'ok':'info'">{{ crawlMsg }}</span>
      </div>
      <div class="search-bar">
        <el-input v-model="keyword" placeholder="输入主播名称或直播间关键词..." clearable size="large" style="flex:1" @keyup.enter="doSearch" @clear="results=[]" />
        <el-select v-model="tier" placeholder="主播等级" clearable size="large" style="width:140px">
          <el-option label="S级 (万人以上)" value="S" />
          <el-option label="A级 (500+)" value="A" />
          <el-option label="B级 (成长中)" value="B" />
          <el-option label="C级 (新锐)" value="C" />
        </el-select>
        <el-button type="primary" size="large" @click="doSearch" :loading="loading">搜索</el-button>
      </div>
      <div v-if="searched && !results.length && !loading" class="empty-state">
        <el-empty description="未找到匹配的主播，换个关键词试试" />
      </div>
      <div v-if="results.length" class="results-info">
        <span>找到 <strong>{{ results.length }}</strong> 位主播</span>
        <span v-if="liveCount > 0" class="live-count">{{ liveCount }} 位正在直播</span>
      </div>
      <div class="results-grid" v-if="results.length">
        <div v-for="(a, i) in results" :key="i" class="anchor-card" :class="{'is-live':a.isLive}" @click="openAnchor(a)">
          <div class="card-avatar">
            <div class="avatar-circle" :style="{background:getColor(a.anchorName)}">{{ a.anchorName.charAt(0) }}</div>
            <div v-if="a.isLive" class="live-badge">LIVE</div>
          </div>
          <div class="card-info">
            <div class="card-name">
              {{ a.anchorName }}
              <span class="tier-tag" :class="'tier-' + getTier(a)">{{ getTier(a) }}</span>
            </div>
            <div class="card-meta">
              <el-tag size="small" :type="a.isLive?'danger':'info'" effect="plain">{{ a.isLive?'直播中':'未开播' }}</el-tag>
              <span class="meta-viewers">{{ fmtN(a.maxViewers) }} 观看</span>
            </div>
            <div class="card-stats">
              <span class="stat">{{ a.roomCount }} 场直播</span>
              <span class="stat">GMV {{ fmtM(a.totalGmv) }}</span>
            </div>
          </div>
          <div class="card-action">
            <el-button v-if="a.isLive" type="danger" size="small" circle>▶</el-button>
            <el-button v-else type="info" size="small" circle disabled>▶</el-button>
          </div>
        </div>
      </div>
    </el-card>

    <!-- 主播详情弹窗 -->
    <el-dialog v-model="dlg" :title="cur?(cur.anchorName+' - 直播详情'):'直播详情'" width="92%" top="4vh" :close-on-click-modal="false" @close="closeDlg" class="anchor-dialog">
      <div class="dialog-body" v-if="cur">
        <div class="live-panel">
          <div class="live-header">
            <div class="header-avatar" :style="{background:getColor(cur.anchorName)}">{{ cur.anchorName.charAt(0) }}</div>
            <div class="header-info">
              <div class="header-name">
                {{ cur.anchorName }}
                <span class="tier-tag" :class="'tier-' + getTier(cur)">{{ getTier(cur) }}级主播</span>
              </div>
              <div class="header-status">
                <el-tag type="danger" effect="dark" size="small" v-if="cur.isLive">直播中</el-tag>
                <el-tag type="info" effect="dark" size="small" v-else>未开播</el-tag>
                <span class="status-text" v-if="cur.isLive && cur.liveUrl">
                  <a :href="cur.liveUrl" target="_blank" class="open-link">打开直播间 →</a>
                </span>
              </div>
            </div>
          </div>
          <div class="live-metrics">
            <div class="metric-item">
              <div class="metric-value highlight">{{ fmtN(cur.maxViewers) }}</div>
              <div class="metric-label">观看人数</div>
            </div>
            <div class="metric-item">
              <div class="metric-value">{{ cur.roomCount }}</div>
              <div class="metric-label">直播场次</div>
            </div>
            <div class="metric-item">
              <div class="metric-value gmv-color">{{ fmtM(cur.totalGmv) }}</div>
              <div class="metric-label">累计GMV</div>
            </div>
            <div class="metric-item">
              <div class="metric-value">{{ fmtN(cur.totalOrders) }}</div>
              <div class="metric-label">累计订单</div>
            </div>
          </div>
          <div class="live-status-area">
            <div v-if="cur.isLive" class="live-indicator">
              <div class="pulse-ring"></div>
              <span>直播进行中 · 弹幕数据实时采集中</span>
              <span class="live-hint">因浏览器安全限制，直播画面需在新标签页查看</span>
            </div>
            <div v-else class="offline-indicator">
              <span>该主播当前未开播，可查看历史弹幕数据</span>
            </div>
          </div>
        </div>
        <div class="dm-panel">
          <div class="dm-title">
            <span>弹幕数据</span>
            <el-tag size="small" :type="cur.isLive?'success':'info'" effect="plain">{{ cur.isLive?'实时采集':'历史回放' }}</el-tag>
          </div>
          <div v-if="cur.liveRoomId" class="dm-area">
            <DanmakuViewer :room-id="cur.liveRoomId" :max-messages="150" />
          </div>
          <div v-else class="dm-ph">
            <div class="dm-ph-text">暂无弹幕数据</div>
            <div class="dm-ph-hint">该直播间尚未接入弹幕采集</div>
          </div>
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { searchAnchors, crawlAnchors, getAnchorStats } from '@/api'
import DanmakuViewer from '@/components/DanmakuViewer.vue'
const keyword = ref('')
const tier = ref('')
const loading = ref(false)
const searched = ref(false)
const results = ref([])
const dlg = ref(false)
const cur = ref(null)
const stats = ref({ totalAnchors: 0, totalRooms: 0, liveRooms: 0 })
const crawling = ref(false)
const crawlMsg = ref('')
const crawlOk = ref(false)
let crawlTimer = null
const liveCount = computed(() => results.value.filter(r => r.isLive).length)

const getTier = (a) => {
  const v = Number(a.maxViewers || 0)
  if (v >= 10000) return 'S'
  if (v >= 500) return 'A'
  if (v >= 50) return 'B'
  return 'C'
}

const loadStats = async () => {
  try { const r = await getAnchorStats(); if (r?.code === 0) stats.value = r.data } catch {}
}
const startCrawl = async () => {
  crawling.value = true; crawlMsg.value = ''; crawlOk.value = false
  if (crawlTimer) { clearTimeout(crawlTimer); crawlTimer = null }
  try {
    const r = await crawlAnchors()
    if (r?.code === 0) {
      if (r.data?.status === 'running') {
        crawlMsg.value = '爬虫正在运行中，请稍后再试'
        crawlTimer = setTimeout(() => { crawlMsg.value = '' }, 8000)
      } else {
        crawlMsg.value = '已启动主播发现，后台运行中...（约2-5分钟）'
        crawlOk.value = true
        crawlTimer = setTimeout(() => { crawlMsg.value = '' }, 12000)
        setTimeout(loadStats, 30000); setTimeout(loadStats, 120000); setTimeout(loadStats, 300000)
      }
    } else {
      crawlMsg.value = r?.msg || '启动失败'
      crawlTimer = setTimeout(() => { crawlMsg.value = '' }, 8000)
    }
  } catch (e) {
    crawlMsg.value = '启动失败: ' + (e.message || e)
    crawlTimer = setTimeout(() => { crawlMsg.value = '' }, 8000)
  } finally { crawling.value = false }
}
onMounted(loadStats)
const doSearch = async () => {
  if (!keyword.value.trim() && !tier.value) { results.value = []; searched.value = false; return }
  loading.value = true; searched.value = true
  try {
    const p = {}
    if (keyword.value.trim()) p.keyword = keyword.value.trim()
    if (tier.value) p.tier = tier.value
    const res = await searchAnchors(p)
    results.value = (res?.code === 0 && res?.data) ? res.data : []
  } catch (e) { console.error(e); results.value = [] }
  finally { loading.value = false }
}
const openAnchor = (a) => { cur.value = a; dlg.value = true }
const closeDlg = () => { cur.value = null; dlg.value = false }
const fmtN = (n) => { if (!n) return '0'; if (n >= 10000) return (n/10000).toFixed(1)+'万'; if (n >= 1000) return (n/1000).toFixed(1)+'k'; return String(n) }
const fmtM = (n) => { if (!n) return '0'; if (n >= 1e8) return (n/1e8).toFixed(1)+'亿'; if (n >= 1e4) return (n/1e4).toFixed(1)+'万'; return n.toLocaleString() }
const getColor = (name) => {
  const cs = ['#6366f1','#8b5cf6','#a78bfa','#c084fc','#e879f9','#f472b6','#fb7185','#f97316','#22d3ee','#34d399']
  return cs[(name||'').charCodeAt(0) % cs.length]
}
</script>

<style scoped>
.anchor-search{padding:0}
.search-card{background:rgba(15,23,42,.85);border:1px solid rgba(99,102,241,.2);border-radius:12px}
.search-header{margin-bottom:16px}
.search-title{color:#e2e8f0;font-size:20px;margin:0 0 4px}
.search-desc{color:#94a3b8;font-size:13px;margin:0}
.stats-row{display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap}
.stat-chip{background:rgba(30,41,59,.7);border:1px solid rgba(99,102,241,.2);border-radius:20px;padding:4px 14px;color:#94a3b8;font-size:13px}
.stat-chip .stat-num{color:#e2e8f0;font-weight:700;margin-right:2px}
.stat-chip.live{border-color:rgba(248,113,113,.4);color:#f87171}
.crawl-msg{font-size:12px;margin-left:8px}
.crawl-msg.ok{color:#34d399}
.crawl-msg.info{color:#fbbf24}
.search-bar{display:flex;gap:12px;align-items:center}
.results-info{margin-top:16px;color:#94a3b8;font-size:13px;display:flex;gap:16px;align-items:center}
.live-count{color:#f87171;font-weight:600}
.results-grid{margin-top:16px;display:flex;flex-direction:column;gap:8px;max-height:calc(100vh - 340px);overflow-y:auto}
.results-grid::-webkit-scrollbar{width:3px}
.results-grid::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.08);border-radius:2px}
.anchor-card{display:flex;align-items:center;gap:16px;padding:14px 18px;background:rgba(30,41,59,.7);border:1px solid rgba(99,102,241,.15);border-radius:10px;cursor:pointer;transition:all .2s}
.anchor-card:hover{background:rgba(49,56,80,.8);border-color:rgba(99,102,241,.4);transform:translateX(4px)}
.anchor-card.is-live{border-color:rgba(248,113,113,.4)}
.anchor-card.is-live:hover{border-color:rgba(248,113,113,.7)}
.card-avatar{position:relative;flex-shrink:0}
.avatar-circle{width:48px;height:48px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:700;color:#fff}
.live-badge{position:absolute;bottom:-2px;right:-2px;background:#ef4444;color:#fff;font-size:9px;font-weight:700;padding:1px 5px;border-radius:6px;letter-spacing:.5px;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
.card-info{flex:1;min-width:0}
.card-name{color:#e2e8f0;font-size:15px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:flex;align-items:center;gap:8px}
.tier-tag{font-size:10px;padding:1px 6px;border-radius:4px;font-weight:600;flex-shrink:0}
.tier-S{background:rgba(251,191,36,.15);color:#fbbf24}
.tier-A{background:rgba(168,85,247,.15);color:#a855f7}
.tier-B{background:rgba(96,165,250,.15);color:#60a5fa}
.tier-C{background:rgba(148,163,184,.15);color:#94a3b8}
.card-meta{margin-top:4px;display:flex;gap:8px;align-items:center}
.meta-viewers{color:#94a3b8;font-size:12px}
.card-stats{margin-top:6px;display:flex;gap:16px}
.stat{color:#64748b;font-size:12px}
.card-action{flex-shrink:0}
.empty-state{margin-top:40px}
.dialog-body{display:flex;gap:16px;height:72vh}
.live-panel{flex:1;display:flex;flex-direction:column;gap:14px}
.live-header{display:flex;align-items:center;gap:14px}
.header-avatar{width:52px;height:52px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:700;color:#fff;flex-shrink:0}
.header-info{flex:1}
.header-name{color:#e2e8f0;font-size:18px;font-weight:600;display:flex;align-items:center;gap:8px}
.header-status{margin-top:4px;display:flex;align-items:center;gap:8px}
.status-text{font-size:12px;color:#94a3b8}
.open-link{color:#22d3ee;text-decoration:none;font-weight:500}
.open-link:hover{text-decoration:underline}
.live-metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.metric-item{background:rgba(30,41,59,.6);border:1px solid rgba(99,102,241,.12);border-radius:8px;padding:14px;text-align:center}
.metric-value{font-size:20px;font-weight:700;color:#e2e8f0;font-family:'Courier New',monospace}
.metric-value.highlight{color:#22d3ee}
.metric-value.gmv-color{color:#fbbf24}
.metric-label{font-size:11px;color:#64748b;margin-top:4px}
.live-status-area{flex:1;display:flex;flex-direction:column;justify-content:center;align-items:center;background:rgba(15,23,42,.5);border:1px solid rgba(99,102,241,.15);border-radius:10px;padding:24px}
.live-indicator{display:flex;flex-direction:column;align-items:center;gap:8px;color:#e2e8f0;font-size:14px}
.pulse-ring{width:48px;height:48px;border-radius:50%;border:3px solid #ef4444;animation:ring-pulse 2s infinite;margin-bottom:8px}
@keyframes ring-pulse{0%{box-shadow:0 0 0 0 rgba(239,68,68,.5)}70%{box-shadow:0 0 0 16px rgba(239,68,68,0)}100%{box-shadow:0 0 0 0 rgba(239,68,68,0)}}
.live-hint{font-size:12px;color:#64748b;margin-top:4px}
.offline-indicator{color:#94a3b8;font-size:14px;text-align:center}
.dm-panel{width:380px;flex-shrink:0;display:flex;flex-direction:column;background:rgba(15,23,42,.6);border-radius:10px;border:1px solid rgba(99,102,241,.15);overflow:hidden}
.dm-title{padding:12px 16px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(99,102,241,.12);color:#e2e8f0;font-weight:600;font-size:14px}
.dm-area{flex:1;overflow:hidden;padding:8px}
.dm-ph{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center}
.dm-ph-text{color:#94a3b8;font-size:14px}
.dm-ph-hint{color:#64748b;font-size:12px;margin-top:4px}
</style>
