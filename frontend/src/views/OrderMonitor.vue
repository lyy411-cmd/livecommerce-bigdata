<template>
  <div class="order-monitor">
    <!-- 搜索栏 -->
    <el-card shadow="never" class="search-card">
      <el-form :inline="true" :model="query">
        <el-form-item label="订单号">
          <el-input v-model="query.orderNo" placeholder="输入订单号" id="order-search-no" clearable />
        </el-form-item>
        <el-form-item label="状态">
          <el-select v-model="query.status" placeholder="全部" id="order-search-status" clearable style="width:130px">
            <el-option label="待揽收" value="pending" />
            <el-option label="运输中" value="in_transit" />
            <el-option label="已签收" value="delivered" />
            <el-option label="异常" value="abnormal" />
          </el-select>
        </el-form-item>
        <el-form-item label="日期">
          <el-date-picker v-model="query.dateRange" type="daterange" range-separator="至"
            start-placeholder="开始" end-placeholder="结束" value-format="YYYY-MM-DD" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" id="btn-order-search" icon="Search" @click="handleSearch">搜索</el-button>
          <el-button icon="Refresh" id="btn-order-reset" @click="resetQuery">重置</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- 统计卡片 -->
    <el-row :gutter="16" class="stats-row">
      <el-col :span="6" v-for="st in stats" :key="st.label">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-content">
            <span class="stat-dot" :style="{ background: st.color }"></span>
            <div>
              <p class="stat-label">{{ st.label }}</p>
              <p class="stat-number">{{ st.value }}</p>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 订单列表表格 -->
    <el-card shadow="never">
      <template #header>
        <div class="flex-between">
          <span>订单列表</span>
          <el-button type="primary" size="small" icon="Plus" id="btn-order-create" @click="showCreateDialog">新建订单</el-button>
        </div>
      </template>
      <el-table :data="tableData" stripe v-loading="tableLoading" style="width:100%">
        <el-table-column prop="order_no" label="订单号" width="160" />
        <el-table-column prop="sender" label="发货方" width="140" />
        <el-table-column prop="sender_address" label="发货地址" min-width="180" />
        <el-table-column prop="receiver" label="收货方" width="140" />
        <el-table-column prop="receiver_address" label="收货地址" min-width="180" />
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="statusTagType(row.status)">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="amount" label="金额(元)" width="95" sortable />
        <el-table-column prop="created_at" label="创建时间" width="170" />
        <el-table-column label="操作" width="180" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="showDetail(row)">详情</el-button>
            <el-button text type="warning" size="small" @click="showEdit(row)">编辑</el-button>
            <el-popconfirm title="确定删除?" @confirm="handleDelete(row.id)">
              <template #reference>
                <el-button text type="danger" size="small">删除</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>
      <div class="pagination-wrap">
        <el-pagination
          v-model:current-page="pagination.page"
          v-model:page-size="pagination.pageSize"
          :page-sizes="[10, 20, 50]"
          layout="total, sizes, prev, pager, next"
          :total="pagination.total"
          @change="fetchData"
        />
      </div>
    </el-card>

    <!-- 创建/编辑弹窗 -->
    <el-dialog v-model="dialogVisible" :title="isEdit ? '编辑订单' : '新建订单'" width="550px" center>
      <el-form ref="formRef" :model="form" :rules="formRules" label-width="90px">
        <el-form-item label="发货方" prop="sender">
          <el-input v-model="form.sender" />
        </el-form-item>
        <el-form-item label="发货地址" prop="sender_address">
          <el-input v-model="form.sender_address" />
        </el-form-item>
        <el-form-item label="收货方" prop="receiver">
          <el-input v-model="form.receiver" />
        </el-form-item>
        <el-form-item label="收货地址" prop="receiver_address">
          <el-input v-model="form.receiver_address" />
        </el-form-item>
        <el-form-item label="金额(元)" prop="amount">
          <el-input-number v-model="form.amount" :min="0" :precision="2" style="width:100%" />
        </el-form-item>
        <el-form-item label="状态" prop="status">
          <el-select v-model="form.status" style="width:100%">
            <el-option label="待揽收" value="待揽收" />
            <el-option label="运输中" value="运输中" />
            <el-option label="已签收" value="已签收" />
            <el-option label="异常" value="异常" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitLoading" @click="submitForm">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { getOrders, createOrder, updateOrder, deleteOrder } from '@/api/order'

const query = reactive({ orderNo: '', status: '', dateRange: [] })
const tableData = ref([])
const tableLoading = ref(false)
const pagination = reactive({ page: 1, pageSize: 10, total: 0 })

const stats = ref([
  { label: '总订单', value: 2856, color: '#409EFF' },
  { label: '运输中', value: 1280, color: '#E6A23C' },
  { label: '已签收', value: 856, color: '#67C23A' },
  { label: '异常订单', value: 100, color: '#F56C6C' }
])

const statusTagType = (s) => {
  const m = { '待揽收': 'info', '运输中': '', '已签收': 'success', '异常': 'danger' }
  return m[s] || ''
}

// 模拟数据
const mockOrders = Array.from({ length: 25 }, (_, i) => ({
  id: i + 1,
  order_no: `LOG202606${String(26 + Math.floor(i / 10)).padStart(2, '0')}${String(1000 + i).padStart(4, '0')}`,
  sender: ['深圳华南仓', '北京大兴仓', '成都高新仓', '武汉光谷仓'][i % 4],
  sender_address: ['深圳市南山区科技园', '北京市大兴区物流园', '成都市高新区天府大道', '武汉市光谷大道'][i % 4],
  receiver: ['广州天河站', '上海浦东站', '重庆江北站', '长沙岳麓站'][i % 4],
  receiver_address: ['广州市天河区体育西路', '上海市浦东新区陆家嘴', '重庆市江北区观音桥', '长沙市岳麓区麓谷'][i % 4],
  status: ['运输中', '待揽收', '已签收', '运输中', '异常'][i % 5],
  amount: (Math.random() * 5000 + 500).toFixed(2),
  created_at: `2026-06-${String(26 - i % 25).padStart(2, '0')} ${String(8 + i % 12).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}:00`
}))

const fetchData = () => {
  tableLoading.value = true
  setTimeout(() => {
    tableData.value = mockOrders.slice(
      (pagination.page - 1) * pagination.pageSize,
      pagination.page * pagination.pageSize
    )
    pagination.total = mockOrders.length
    tableLoading.value = false
  }, 300)
}

const handleSearch = () => fetchData()
const resetQuery = () => { Object.assign(query, { orderNo: '', status: '', dateRange: [] }); fetchData() }

// CRUD
const dialogVisible = ref(false)
const isEdit = ref(false)
const editId = ref(null)
const formRef = ref()
const submitLoading = ref(false)
const form = reactive({ sender: '', sender_address: '', receiver: '', receiver_address: '', amount: 0, status: '待揽收' })
const formRules = {
  sender: [{ required: true, message: '必填', trigger: 'blur' }],
  sender_address: [{ required: true, message: '必填', trigger: 'blur' }],
  receiver: [{ required: true, message: '必填', trigger: 'blur' }],
  receiver_address: [{ required: true, message: '必填', trigger: 'blur' }]
}

const showCreateDialog = () => {
  isEdit.value = false; editId.value = null
  Object.assign(form, { sender: '', sender_address: '', receiver: '', receiver_address: '', amount: 0, status: '待揽收' })
  dialogVisible.value = true
}

const showEdit = (row) => {
  isEdit.value = true; editId.value = row.id
  Object.assign(form, { sender: row.sender, sender_address: row.sender_address, receiver: row.receiver, receiver_address: row.receiver_address, amount: row.amount, status: row.status })
  dialogVisible.value = true
}

const showDetail = (row) => {
  ElMessageBox.alert(
    `<div><p><b>订单号:</b> ${row.order_no}</p><p><b>发货方:</b> ${row.sender}</p><p><b>收货方:</b> ${row.receiver}</p><p><b>金额:</b> ¥${row.amount}</p><p><b>状态:</b> ${row.status}</p><p><b>时间:</b> ${row.created_at}</p></div>`,
    '订单详情', { dangerouslyUseHTMLString: true }
  )
}

const submitForm = async () => {
  await formRef.value.validate()
  submitLoading.value = true
  try {
    if (isEdit.value) {
      // await updateOrder(editId.value, form)
      const idx = mockOrders.findIndex(o => o.id === editId.value)
      if (idx > -1) Object.assign(mockOrders[idx], form)
      ElMessage.success('更新成功')
    } else {
      // await createOrder(form)
      mockOrders.unshift({ id: Date.now(), order_no: 'LOG' + Date.now(), ...form, created_at: new Date().toLocaleString() })
      ElMessage.success('创建成功')
    }
    dialogVisible.value = false
    fetchData()
  } catch { ElMessage.error('操作失败') }
  finally { submitLoading.value = false }
}

const handleDelete = (id) => {
  const idx = mockOrders.findIndex(o => o.id === id)
  if (idx > -1) mockOrders.splice(idx, 1)
  ElMessage.success('删除成功')
  fetchData()
}

onMounted(() => fetchData())
</script>

<style scoped lang="scss">
.order-monitor { display: flex; flex-direction: column; gap: 16px; }
.search-card { :deep(.el-card__body) { padding-bottom: 0; } }
.stats-row { margin-bottom: 0; }
.stat-card { cursor: pointer; }
.stat-content { display: flex; align-items: center; gap: 12px; }
.stat-dot { width: 10px; height: 10px; border-radius: 50%; }
.stat-label { font-size: 13px; color: #909399; }
.stat-number { font-size: 22px; font-weight: bold; color: #303133; }
.pagination-wrap { display: flex; justify-content: flex-end; margin-top: 16px; }
</style>
