<template>
  <div class="anchor-search">
    <el-card class="search-card">
      <div class="search-header">
        <h2 class="search-title">&#128269; 带货主播搜索</h2>
        <p class="search-desc">搜索数据库中的带货主播，查看直播状态和弹幕</p>
      </div>
      <div class="stats-row">
        <div class="stat-chip"><span class="stat-num">{{ stats.totalAnchors }}</span> 位主播</div>
        <div class="stat-chip"><span class="stat-num">{{ stats.totalRooms }}</span> 个房间</div>
        <div class="stat-chip live" v-if="stats.liveRooms"><span class="stat-num">{{ stats.liveRooms }}</span> 直播中</div>
        <div style="flex:1"></div>
        <el-button type="success" :loading="crawling" @click="startCrawl" :disabled="crawling">
          {{ crawling ? '爬取中...' : '&#128640; 发现更多主播' }}
        </el-button>
        <span v-if="crawlMsg" class="crawl-msg" :class="crawlOk?'ok':'info'">{{ crawlMsg }}</span>
      </div>
      <div class="search-bar">
        <el-input v-model="keyword" placeholder="输入主播名称或直播间关键词..." clearable size="large" style="flex:1" @keyup.enter="doSearch" @clear="results=[]" />
        <el-select v-model="category" placeholder="类目筛选" clearable size="large" style="width:140px">
          <el-option v-for="c in categories" :key="c" :label="c" :value="c" />
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
            <div class="card-name">{{ a.anchorName }}</div>
            <div class="card-meta">
              <el-tag size="small" :type="a.isLive?'danger':'info'" effect="plain">{{ a.isLive?'直播中':'未开播' }}</el-tag>
              <span v-if="a.category" class="meta-item">{{ a.category }}</span>
            </div>
            <div class="card-stats">
              <span class="stat">&#128065; {{ fmtN(a.maxViewers) }}</span>
              <span class="stat">&#127909; {{ a.roomCount }}场</span>
              <span class="stat">&#128176; {{ fmtM(a.totalGmv) }}</span>
            </div>
          </div>
          <div class="card-action">
            <el-button v-if="a.isLive" type="danger" size="small" circle>&#9654;</el-button>
            <el-button v-else type="info" size="small" circle disabled>&#9654;</el-button>
          </div>
        </div>
      </div>
    </el-card>
    <el-dialog v-model="dlg" :title="cur?(cur.anchorName+' - 直播间'):'直播间'" width="90%" top="5vh" :close-on-click-modal="false" @close="closeDlg">
      <div class="dialog-body" v-if="cur">
        <div class="live-panel">
          <div class="live-header">
            <el-tag type="danger" effect="dark" size="large" v-if="cur.isLive">LIVE</el-tag>
            <el-tag type="info" effect="dark" size="large" v-else>未开播</el-tag>
            <span class="live-name">{{ cur.anchorName }}</span>
            <span class="live-cat" v-if="cur.category">{{ cur.category }}</span>
          </div>
          <div class="live-content">
            <div v-if="cur.isLive" class="frame-area">
              <div class="frame-ph">
                <div style="font-size:48px;margin-bottom:12px">&#128250;</div>
                <p style="color:#e2e8f0;font-size:16px;font-weight:600">{{ cur.anchorName }}</p>
                <p class="ph-hint">由于浏览器安全策略，无法直接嵌入抖音页面</p>
                <a :href="cur.liveUrl" target="_blank" class="open-btn">&#128279; 在新标签页打开直播间</a>
                <p class="ph-hint2">右侧弹幕面板实时更新</p>
              </div>
            </div>
            <div v-else class="offline-area">
              <el-empty description="该主播当前未开播">
                <template #description><p style="color:#94a3b8">可查看历史直播数据</p></template>
              </el-empty>
              <div class="off-stats">
                <div class="off-stat"><span class="sl">历史最高观看</span><span class="sv">{{ fmtN(cur.maxViewers) }}</span></div>
                <div class="off-stat"><span class="sl">直播场次</span><span class="sv">{{ cur.roomCount }}</span></div>
                <div class="off-stat"><span class="sl">累计GMV</span><span class="sv">{{ fmtM(cur.totalGmv) }}</span></div>
                <div class="off-stat"><span class="sl">累计订单</span><span class="sv">{{ fmtN(cur.totalOrders) }}</span></div>
              </div>
            </div>
          </div>
        </div>
        <div class="dm-panel">
          <div class="dm-title"><span>实时弹幕</span><el-tag size="small" :type="cur.isLive?'success':'info'">{{ cur.isLive?'实时':'历史' }}</el-tag></div>
          <div v-if="cur.isLive && cur.liveRoomId" class="dm-area">
            <DanmakuViewer :room-id="cur.liveRoomId" :max-messages="150" />
          </div>
          <div v-else class="dm-ph"><el-empty description="暂无弹幕数据" :image-size="60" /></div>
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
const category = ref('')
const loading = ref(false)
const searched = ref(false)
const results = ref([])
const dlg = ref(false)
const cur = ref(null)
const stats = ref({ totalAnchors: 0, totalRooms: 0, liveRooms: 0 })
const crawling = ref(false)
const crawlMsg = ref('')
const crawlOk = ref(false)
const categories = ['美妆','服饰','食品','数码','家居','母婴','珠宝','运动','综合']
const liveCount = computed(() => results.value.filter(r => r.isLive).length)
const loadStats = async () => {
  try { const r = await getAnchorStats(); if (r?.code === 0) stats.value = r.data } catch {}
}
const startCrawl = async () => {
  crawling.value = true; crawlMsg.value = ''; crawlOk.value = false
  try {
    const r = await crawlAnchors()
    if (r?.code === 0) {
      if (r.data?.status === 'running') { crawlMsg.value = '爬虫正在运行中，请稍后再试' }
      else { crawlMsg.value = '已启动主播发现，后台运行中...（约2-5分钟）'; crawlOk.value = true; setTimeout(loadStats, 30000); setTimeout(loadStats, 120000); setTimeout(loadStats, 300000) }
    } else { crawlMsg.value = r?.msg || '启动失败' }
  } catch (e) { crawlMsg.value = '启动失败: ' + (e.message || e) }
  finally { crawling.value = false }
}
onMounted(loadStats)
const doSearch = async () => {
  if (!keyword.value.trim() && !category.value) { results.value = []; searched.value = false; return }
  loading.value = true; searched.value = true
  try {
    const p = {}
    if (keyword.value.trim()) p.keyword = keyword.value.trim()
    if (category.value) p.category = category.value
    const res = await searchAnchors(p)
    results.value = (res?.code === 0 && res?.data) ? res.data : []
  } catch (e) { console.error(e); results.value = [] }
  finally { loading.value = false }
}
const openAnchor = (a) => { cur.value = a; dlg.value = true }
const closeDlg = () => { cur.value = null; dlg.value = false }
const fmtN = (n) => { if (!n) return '0'; if (n >= 10000) return (n/10000).toFixed(1)+'w'; if (n >= 1000) return (n/1000).toFixed(1)+'k'; return String(n) }
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
.anchor-card{display:flex;align-items:center;gap:16px;padding:14px 18px;background:rgba(30,41,59,.7);border:1px solid rgba(99,102,241,.15);border-radius:10px;cursor:pointer;transition:all .2s}
.anchor-card:hover{background:rgba(49,56,80,.8);border-color:rgba(99,102,241,.4);transform:translateX(4px)}
.anchor-card.is-live{border-color:rgba(248,113,113,.4)}
.anchor-card.is-live:hover{border-color:rgba(248,113,113,.7)}
.card-avatar{position:relative;flex-shrink:0}
.avatar-circle{width:48px;height:48px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:700;color:#fff}
.live-badge{position:absolute;bottom:-2px;right:-2px;background:#ef4444;color:#fff;font-size:9px;font-weight:700;padding:1px 5px;border-radius:6px;letter-spacing:.5px;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
.card-info{flex:1;min-width:0}
.card-name{color:#e2e8f0;font-size:15px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card-meta{margin-top:4px;display:flex;gap:8px;align-items:center}
.meta-item{color:#94a3b8;font-size:12px}
.card-stats{margin-top:6px;display:flex;gap:16px}
.stat{color:#64748b;font-size:12px}
.card-action{flex-shrink:0}
.dialog-body{display:flex;gap:16px;height:70vh}
.live-panel{flex:1;display:flex;flex-direction:column}
.live-header{display:flex;align-items:center;gap:10px;margin-bottom:12px}
.live-name{color:#e2e8f0;font-size:18px;font-weight:600}
.live-cat{color:#94a3b8;font-size:13px}
.live-content{flex:1}
.frame-area{height:100%}
.frame-ph{height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;background:rgba(15,23,42,.6);border-radius:10px;border:1px solid rgba(99,102,241,.2)}
.ph-hint{font-size:12px;color:#94a3b8;margin:4px 0}
.ph-hint2{font-size:12px;color:#64748b;margin-top:12px}
.open-btn{display:inline-block;margin-top:12px;padding:10px 24px;background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;transition:opacity .2s}
.open-btn:hover{opacity:.85}
.offline-area{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%}
.off-stats{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:20px}
.off-stat{background:rgba(30,41,59,.7);border-radius:8px;padding:14px;text-align:center;border:1px solid rgba(99,102,241,.15)}
.sl{display:block;color:#94a3b8;font-size:12px;margin-bottom:4px}
.sv{display:block;color:#e2e8f0;font-size:18px;font-weight:700}
.dm-panel{width:360px;flex-shrink:0;display:flex;flex-direction:column;background:rgba(15,23,42,.6);border-radius:10px;border:1px solid rgba(99,102,241,.2)}
.dm-title{padding:12px 16px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(99,102,241,.15);color:#e2e8f0;font-weight:600}
.dm-area{flex:1;overflow:hidden;padding:8px}
.dm-ph{flex:1;display:flex;align-items:center;justify-content:center}
.empty-state{margin-top:40px}
</style>
