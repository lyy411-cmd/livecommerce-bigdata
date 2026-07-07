<template>
  <div class="order">
    <el-card>
      <el-row :gutter="16" class="stats">
        <el-col :span="4"><el-card class="stat-card cyan">
          <div><p class="lbl">总订单数</p><p class="val">{{ stats.totalOrders || 0 }}</p></div>
        </el-card></el-col>
        <el-col :span="4"><el-card class="stat-card orange">
          <div><p class="lbl">待支付</p><p class="val">{{ stats.pendingOrders || 0 }}</p></div>
        </el-card></el-col>
        <el-col :span="4"><el-card class="stat-card green">
          <div><p class="lbl">已支付</p><p class="val">{{ stats.paidOrders || 0 }}</p></div>
        </el-card></el-col>
        <el-col :span="4"><el-card class="stat-card blue">
          <div><p class="lbl">已发货</p><p class="val">{{ stats.shippedOrders || 0 }}</p></div>
        </el-card></el-col>
        <el-col :span="4"><el-card class="stat-card purple">
          <div><p class="lbl">已签收</p><p class="val">{{ stats.deliveredOrders || 0 }}</p></div>
        </el-card></el-col>
        <el-col :span="4"><el-card class="stat-card red">
          <div><p class="lbl">总金额</p><p class="val">￥{{ formatMoney(stats.totalAmount) }}</p></div>
        </el-card></el-col>
      </el-row>

      <div class="search-bar mt-16">
        <el-select v-model="query.status" placeholder="状态" clearable style="width:140px">
          <el-option label="待支付" value="pending" />
          <el-option label="已支付" value="paid" />
          <el-option label="已发货" value="shipped" />
          <el-option label="已签收" value="delivered" />
          <el-option label="已取消" value="cancelled" />
        </el-select>
        <el-select v-model="query.platform" placeholder="平台" clearable style="width:130px">
          <el-option label="抖音" value="douyin" />
          <el-option label="快手" value="kuaishou" />
          <el-option label="淘宝" value="taobao" />
        </el-select>
        <el-button type="primary" @click="onSearch">搜索</el-button>
        <el-button @click="onRefresh" class="refresh-btn">刷新</el-button>
        <div class="sse-status" :class="{ active: sseConnected }">
          <span class="dot"></span>
          {{ sseConnected ? '实时同步中' : '连接中...' }}
          <span class="order-hint"> 订单自动模拟生成中</span>
        </div>
      </div>

      <el-table
        :data="filteredData"
        stripe
        v-loading="initialLoading"
        element-loading-text="加载中..."
        height="calc(100vh - 420px)"
        style="margin-top:16px"
        :row-class-name="rowClassName"
      >
        <el-table-column prop="orderNo" label="订单号" width="180" fixed="left" sortable />
        <el-table-column prop="productName" label="商品" min-width="160" show-overflow-tooltip />
        <el-table-column prop="roomName" label="直播间" width="140" show-overflow-tooltip />
        <el-table-column prop="username" label="买家" width="110" />
        <el-table-column prop="quantity" label="数量" width="60" align="center" />
        <el-table-column prop="totalAmount" label="金额" width="110" align="right">
          <template #default="{ row }">￥{{ Number(row.totalAmount).toLocaleString() }}</template>
        </el-table-column>
        <el-table-column prop="platform" label="平台" width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.platform === 'taobao' ? 'warning' : row.platform === 'douyin' ? 'primary' : 'success'" size="small">
              {{ {taobao:'淘宝', douyin:'抖音', kuaishou:'快手'}[row.platform] }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100" align="center">
          <template #default="{ row }">
            <el-tag :type="statusType(row.status)" size="small">
              {{ statusLabel(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="createTime" label="下单时间" width="160" sortable />
        
      </el-table>
    </el-card>

    

    <!-- 实时事件滚动条 -->
    <div class="event-ticker" v-if="recentEvents.length > 0">
      <div class="ticker-wrap">
        <span class="ticker-label">实时订单流</span>
        <div class="ticker-list">
          <span v-for="(e, i) in recentEvents" :key="i" class="ticker-item" :style="{ color: e.color }">
            {{ e.text }}  <span style="color:rgba(255,255,255,0.2)">{{ e.time }}</span>
          </span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onBeforeUnmount } from 'vue'
import { getOrderPage, getOrderOverview } from '@/api'
import { subscribeEvents } from '@/utils/sse'

const query = reactive({ page: 1, pageSize: 100, status: '', platform: '' })
const tableData = ref([])
const initialLoading = ref(true)
const stats = ref({})
const sseConnected = ref(false)
const recentEvents = ref([])
let unsubscribe = null
let pollTimer = null


const filteredData = computed(() => {
  let list = [...tableData.value]
  const priority = { pending: 0, paid: 1, shipped: 2, delivered: 3, cancelled: 4, refunded: 5 }
  list.sort((a, b) => {
    const pa = priority[a.status] ?? 9, pb = priority[b.status] ?? 9
    if (pa !== pb) return pa - pb
    return b.id - a.id
  })
  return list
})

const statusLabel = (s) => ({ pending:'待支付', paid:'已支付', shipped:'已发货', delivered:'已签收', cancelled:'已取消', refunded:'已退款' })[s] || s
const statusType = (s) => ({ pending:'warning', paid:'primary', shipped:'success', delivered:'info', cancelled:'danger', refunded:'danger' })[s] || ''
const formatMoney = (v) => Number(v || 0).toLocaleString()

const rowClassName = ({ row }) => {
  if (row.status === 'delivered') return 'delivered-row'
  if (row.status === 'cancelled' || row.status === 'refunded') return 'cancelled-row'
  return row.status === 'pending' ? 'highlight-row' : ''
}

const addEvent = (text, color = '#00ffcc') => {
  const now = new Date()
  const time = now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0') + ':' + now.getSeconds().toString().padStart(2,'0')
  recentEvents.value.unshift({ text, time, color })
  if (recentEvents.value.length > 30) recentEvents.value.pop()
}

const incStat = (key, delta) => { stats.value[key] = (stats.value[key] || 0) + delta }

const handleSSE = (event) => {
  sseConnected.value = true
  const colors = { new_order:'#00ffcc', order_paid:'#ffa502', order_shipped:'#00d9ff', order_delivered:'#2ed573', order_cancelled:'#ff6b6b' }
  addEvent(event.msg || event.orderNo, colors[event.type] || '#00ffcc')

  if (event.type === 'new_order') {
    incStat('totalOrders', 1)
    incStat('pendingOrders', 1)
    stats.value.totalAmount = (stats.value.totalAmount || 0) + (event.totalAmount || event.amount || 0)
    if (activeTab.value === 'all' || activeTab.value === 'pending' || activeTab.value === 'active') {
      tableData.value.unshift({
        id: event.oid, orderNo: event.orderNo, productName: event.productName || event.product,
        roomName: event.roomName || '', username: event.username || event.user || '',
        quantity: event.quantity || 1, totalAmount: event.totalAmount || event.amount || 0,
        platform: event.platform || 'douyin', status: 'pending', createTime: event.createTime || ''
      })
    }
  } else if (event.type === 'order_paid') {
    incStat('pendingOrders', -1)
    incStat('paidOrders', 1)
    stats.value.totalAmount = (stats.value.totalAmount || 0) + (event.amount || 0)
    updateRow(event.orderNo, 'paid')
  } else if (event.type === 'order_shipped') {
    incStat('paidOrders', -1)
    incStat('shippedOrders', 1)
    updateRow(event.orderNo, 'shipped')
  } else if (event.type === 'order_delivered') {
    incStat('shippedOrders', -1)
    incStat('deliveredOrders', 1)
    updateRow(event.orderNo, 'delivered')
  } else if (event.type === 'order_cancelled') {
    incStat('pendingOrders', -1)
    incStat('cancelledOrders', 1)
    updateRow(event.orderNo, 'cancelled')
  } else if (event.type === 'order_status_changed') {
    const statMap = { pending:'pendingOrders', paid:'paidOrders', shipped:'shippedOrders', delivered:'deliveredOrders', cancelled:'cancelledOrders', refunded:'cancelledOrders' }
    if (event.oldStatus) incStat(statMap[event.oldStatus] || '', -1)
    if (event.newStatus) incStat(statMap[event.newStatus] || '', 1)
    updateRow(event.orderNo, event.newStatus)
  }
}

const updateRow = (orderNo, newStatus) => {
  const idx = tableData.value.findIndex(r => r.orderNo === orderNo)
  if (idx >= 0) {
    tableData.value[idx] = { ...tableData.value[idx], status: newStatus }
  }
}

const fetchData = async () => {
  initialLoading.value = true
  try {
    const [r, s] = await Promise.all([getOrderPage(query), getOrderOverview()])
    tableData.value = r.data.records || []
    stats.value = s.data
  } finally { initialLoading.value = false }
}

const silentRefresh = async () => {
  try {
    const [r, s] = await Promise.all([getOrderPage(query), getOrderOverview()])
    tableData.value = r.data.records || []
    stats.value = s.data
  } catch {}
}

const onSearch = () => fetchData()
const onRefresh = () => { query.page = 1; query.status = ''; query.platform = ''; fetchData() }
const onTabChange = () => {}

const handleAction = async (row, action) => {
  try {
    const res = await fetch(`/api/livecommerce/order/${action}?id=${row.id}`, { method: 'POST' })
    const data = await res.json()
    if (data.code === 0) {
      if (action === 'pay') { incStat('pendingOrders', -1); incStat('paidOrders', 1) }
      else if (action === 'ship') { incStat('paidOrders', -1); incStat('shippedOrders', 1) }
      else if (action === 'confirm') { incStat('shippedOrders', -1); incStat('deliveredOrders', 1) }
      else if (action === 'cancel') {
        if (row.status === 'pending') incStat('pendingOrders', -1)
        else if (row.status === 'paid') incStat('paidOrders', -1)
        else incStat('shippedOrders', -1)
        incStat('cancelledOrders', 1)
      }
      updateRow(row.orderNo, { pay:'paid', ship:'shipped', confirm:'delivered', cancel:'cancelled' }[action])
      ElMessage.success(data.msg || '操作成功')
    } else {
      ElMessage.warning(data.msg)
    }
  } catch { ElMessage.error('操作失败') }
}

const handleRefund = (row) => {
  refundForm.orderNo = row.orderNo
  refundForm.productName = row.productName
  refundForm.reason = ''
  refundForm.note = ''
  refundVisible.value = true
}

onMounted(() => {
  fetchData()
  unsubscribe = subscribeEvents(handleSSE)
  pollTimer = setInterval(async () => {
    sseConnected.value = true
    await silentRefresh()
  }, 3000)
})

onBeforeUnmount(() => {
  if (unsubscribe) unsubscribe()
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
})
</script>

<style scoped lang="scss">
.order { display: flex; flex-direction: column; height: 100%; }
.order > .el-card { display: flex; flex-direction: column; flex: 1; :deep(.el-card__body) { display: flex; flex-direction: column; flex: 1; } }
.order .stats { margin-bottom: 16px; }
.stat-card { text-align: center; background: rgba(15,20,30,0.5) !important; border: 1px solid rgba(255,255,255,0.06) !important;
  .lbl { color: rgba(255,255,255,0.4); font-size: 11px; letter-spacing: 0.5px; }
  .val { font-size: 22px; font-weight: bold; color: #e0e0e0; margin-top: 4px; font-family: 'Courier New', monospace; }
  &.cyan .val { color: #00ffcc; } &.green .val { color: #2ed573; } &.orange .val { color: #ffa502; }
  &.purple .val { color: #a855f7; } &.blue .val { color: #00d9ff; } &.red .val { color: #ff6b6b; }
}
.search-bar { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
.refresh-btn { background: rgba(0,217,255,0.1) !important; border-color: rgba(0,217,255,0.3) !important; color: #00d9ff !important; }
.sse-status {
  margin-left: auto; display: flex; align-items: center; gap: 6px; font-size: 11px; color: rgba(255,255,255,0.3);
  .dot { width: 6px; height: 6px; border-radius: 50%; background: rgba(255,255,255,0.2); }
  &.active { color: #00ffcc; .dot { background: #00ffcc; box-shadow: 0 0 6px #00ffcc; animation: pulse 2s infinite; } }
}
.order-hint { color: rgba(255,255,255,0.2); font-size: 10px; margin-left: 4px; }
@keyframes pulse { 0%,100% { box-shadow: 0 0 4px #00ffcc; } 50% { box-shadow: 0 0 12px #00ffcc; } }
::deep(.delivered-row) { opacity: 0.5; td { background: rgba(46,213,115,0.03) !important; } }
::deep(.cancelled-row) { opacity: 0.4; td { background: rgba(255,107,107,0.03) !important; text-decoration: line-through; } }
::deep(.highlight-row) { background: rgba(0,255,204,0.03) !important; }

.event-ticker {
  margin-top: 12px; background: rgba(15,20,30,0.4);
  border: 1px solid rgba(0,255,204,0.08); border-radius: 8px;
  padding: 8px 16px; overflow: hidden;
}
.ticker-wrap { display: flex; align-items: center; gap: 12px; }
.ticker-label { font-size: 11px; color: #00ffcc; white-space: nowrap; font-weight: 600; letter-spacing: 1px; }
.ticker-list { display: flex; gap: 32px; overflow-x: auto; flex: 1; scroll-behavior: smooth;
  &::-webkit-scrollbar { height: 0; } }
.ticker-item { font-size: 12px; white-space: nowrap; }
</style>
