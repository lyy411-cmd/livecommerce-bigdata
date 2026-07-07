<template>
  <div class="customer-center">
    <el-row :gutter="16" class="welcome-row">
      <el-col :span="24">
        <el-card class="welcome-card">
          <div class="welcome-content">
            <el-avatar :size="64" icon="UserFilled" />
            <div class="welcome-text">
              <h2>欢迎回来，{{ userStore.userInfo?.username }}</h2>
              <p>您可以在平台创建和管理您的物流订单</p>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16" class="stats-row">
      <el-col :span="6" v-for="card in statsCards" :key="card.label">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-content">
            <div class="stat-left">
              <p class="stat-label">{{ card.label }}</p>
              <p class="stat-value">{{ card.value }}</p>
            </div>
            <div class="stat-icon" :style="{ background: card.color }">
              <el-icon :size="24" color="#fff"><component :is="card.icon" /></el-icon>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card class="order-card">
      <template #header>
        <div class="card-header flex-between">
          <span>我的订单</span>
          <el-button type="primary" size="small" icon="Plus" id="btn-create-order" @click="showCreateDialog">新建订单</el-button>
        </div>
      </template>

      <el-table :data="orders" stripe>
        <el-table-column prop="order_no" label="订单号" width="170" />
        <el-table-column prop="sender" label="发货方" width="120" />
        <el-table-column prop="sender_address" label="发货地址" min-width="140" />
        <el-table-column prop="receiver" label="收货方" width="120" />
        <el-table-column prop="receiver_address" label="收货地址" min-width="140" />
        <el-table-column prop="amount" label="金额(元)" width="95" />
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="statusType(row.status)" size="small">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="created_at" label="创建时间" width="165" />
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{ row }">
            <el-button
              v-if="row.status === '已签收'"
              text type="success" size="small"
              id="btn-sign-order"
              @click="confirmSign(row)"
            >
              确认签收
            </el-button>
            <span v-else style="color:#C0C4CC;font-size:12px">{{ row.status === '运输中' ? '运输中...' : '等待揽收' }}</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 新建订单弹窗 -->
    <el-dialog v-model="dialogVisible" title="新建物流订单" width="550px" center>
      <el-form ref="formRef" :model="form" :rules="formRules" label-width="90px">
        <el-form-item label="发货方" prop="sender">
          <el-input v-model="form.sender" placeholder="您的姓名或公司名称" id="order-sender" />
        </el-form-item>
        <el-form-item label="发货地址" prop="sender_address">
          <el-input v-model="form.sender_address" placeholder="详细发货地址" id="order-sender-addr" />
        </el-form-item>
        <el-form-item label="收货方" prop="receiver">
          <el-input v-model="form.receiver" placeholder="收件人姓名或公司名称" id="order-receiver" />
        </el-form-item>
        <el-form-item label="收货地址" prop="receiver_address">
          <el-input v-model="form.receiver_address" placeholder="详细收货地址" id="order-receiver-addr" />
        </el-form-item>
        <el-form-item label="预估金额" prop="amount">
          <el-input-number v-model="form.amount" :min="0" :precision="2" style="width:100%" placeholder="预估物流费用" id="order-amount" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitLoading" id="btn-submit-order" @click="submitOrder">提交订单</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { useUserStore } from '@/stores/user'
import { getMyOrders, createMyOrder, getMyOrderStats } from '@/api/customer'
import { signOrder } from '@/api/order'

const userStore = useUserStore()
const orders = ref([])
const dialogVisible = ref(false)
const formRef = ref()
const submitLoading = ref(false)

const statsCards = ref([
  { label: '我的订单', value: 0, icon: 'Document', color: '#409EFF' },
  { label: '运输中', value: 0, icon: 'Van', color: '#E6A23C' },
  { label: '已签收', value: 0, icon: 'CircleCheckFilled', color: '#67C23A' },
  { label: '待揽收', value: 0, icon: 'Clock', color: '#909399' }
])

const form = reactive({ sender: '', sender_address: '', receiver: '', receiver_address: '', amount: 0 })
const formRules = {
  sender: [{ required: true, message: '请输入发货方', trigger: 'blur' }],
  sender_address: [{ required: true, message: '请输入发货地址', trigger: 'blur' }],
  receiver: [{ required: true, message: '请输入收货方', trigger: 'blur' }],
  receiver_address: [{ required: true, message: '请输入收货地址', trigger: 'blur' }]
}

const statusType = (s) => {
  const m = { '待揽收': 'info', '运输中': '', '已签收': 'success', '异常': 'danger' }
  return m[s] || 'info'
}

const fetchOrders = () => {
  getMyOrders({ page_size: 50 }).then(res => {
    orders.value = res.data || []
  }).catch(() => {})
}

const fetchStats = () => {
  getMyOrderStats().then(res => {
    const d = res.data || {}
    statsCards.value[0].value = d.total || 0
    statsCards.value[1].value = d.in_transit || 0
    statsCards.value[2].value = d.delivered || 0
    statsCards.value[3].value = d.pending || 0
  }).catch(() => {})
}

const showCreateDialog = () => {
  Object.assign(form, { sender: '', sender_address: '', receiver: '', receiver_address: '', amount: 0 })
  dialogVisible.value = true
}

const submitOrder = async () => {
  await formRef.value.validate()
  submitLoading.value = true
  try {
    await createMyOrder({ ...form })
    ElMessage.success('订单创建成功！物流人员将尽快处理')
    dialogVisible.value = false
    fetchOrders()
    fetchStats()
  } catch { /* 拦截器已处理 */ }
  finally { submitLoading.value = false }
}

const confirmSign = async (row) => {
  try {
    await ElMessageBox.confirm(
      `确认已收到订单 ${row.order_no} 的货物？签收后状态将更新。`,
      '确认签收',
      { confirmButtonText: '确认签收', cancelButtonText: '取消', type: 'success' }
    )
    await signOrder(row.id)
    ElMessage.success('签收成功！感谢您的使用')
    fetchOrders()
    fetchStats()
  } catch { /* 用户取消 */ }
}

onMounted(() => {
  fetchOrders()
  fetchStats()
})
</script>

<style scoped lang="scss">
.customer-center { display: flex; flex-direction: column; gap: 16px; }
.welcome-card {
  background: linear-gradient(135deg, #409EFF 0%, #337ecc 100%);
  :deep(.el-card__body) { padding: 24px; }
}
.welcome-content {
  display: flex; align-items: center; gap: 20px;
  h2 { color: #fff; margin: 0; font-size: 20px; }
  p { color: rgba(255,255,255,0.8); margin: 6px 0 0; font-size: 14px; }
}
.stats-row { margin-bottom: 0; }
.stat-card { cursor: pointer; transition: transform 0.2s; &:hover { transform: translateY(-2px); } }
.stat-content { display: flex; justify-content: space-between; align-items: center; }
.stat-label { font-size: 13px; color: #909399; }
.stat-value { font-size: 24px; font-weight: bold; color: #303133; margin-top: 4px; }
.stat-icon { width: 44px; height: 44px; border-radius: 10px; display: flex; align-items: center; justify-content: center; }
</style>
