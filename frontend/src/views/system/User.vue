<template>
  <div class="user-mgmt">
    <el-card>
      <div class="search-bar">
        <el-input v-model="search" placeholder="搜索用户名/邮箱" clearable style="width:240px" @keyup.enter="onSearch" />
        <el-button type="primary" @click="onSearch">搜索</el-button>
        <el-button @click="onRefresh" class="refresh-btn">刷新</el-button>
        <el-button type="primary" @click="openAdd">添加员工</el-button>
      </div>
      <el-table :data="tableData" stripe v-loading="loading" height="calc(100vh - 380px)" style="margin-top:16px">
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column prop="username" label="用户名" width="120" />
        <el-table-column prop="email" label="邮箱" width="220" />
        <el-table-column prop="phone" label="手机号" width="140" />
        <el-table-column prop="role" label="角色" width="100">
          <template #default="{ row }">
            <el-tag :type="row.role === 'admin' ? 'danger' : row.role === 'operator' ? 'warning' : 'primary'" size="small">
              {{ roleLabel(row.role) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="department" label="部门" width="120" />
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 1 ? 'success' : 'info'" size="small">
              {{ row.status === 1 ? '启用' : '禁用' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="createTime" label="创建时间" width="170" />
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="openEdit(row)">编辑</el-button>
            <el-button text type="warning" size="small" @click="resetPwd(row)">重置密码</el-button>
            <el-button text type="danger" size="small" :disabled="row.username === 'admin'" @click="handleDelete(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-pagination class="pagination" v-model:current-page="query.page" :page-size="query.pageSize"
        :total="total" @current-change="fetchData" layout="total, prev, pager, next" />
    </el-card>

    <el-dialog v-model="showForm" :title="form.id ? '编辑员工' : '添加员工'" width="500px">
      <el-form :model="form" :rules="rules" ref="formRef" label-width="100px">
        <el-form-item label="用户名" prop="username"><el-input v-model="form.username" :disabled="!!form.id" /></el-form-item>
        <el-form-item label="邮箱" prop="email"><el-input v-model="form.email" /></el-form-item>
        <el-form-item label="手机号"><el-input v-model="form.phone" placeholder="选填" /></el-form-item>
        <el-form-item v-if="!form.id" label="初始密码" prop="password">
          <el-input v-model="form.password" type="password" show-password placeholder="至少6位" />
        </el-form-item>
        <el-form-item label="角色" prop="role">
          <el-select v-model="form.role" style="width:100%">
            <el-option label="管理员" value="admin" />
            <el-option label="运营员" value="operator" />
            <el-option label="分析师" value="analyst" />
          </el-select>
        </el-form-item>
        <el-form-item label="部门"><el-input v-model="form.department" placeholder="选填，如：运营部" /></el-form-item>
        <el-form-item v-if="form.id" label="状态">
          <el-switch v-model="form.status" :active-value="1" :inactive-value="0" active-text="启用" inactive-text="禁用" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showForm = false">取消</el-button>
        <el-button type="primary" @click="handleSave">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { getUserPage, createUser, updateUser, resetUserPassword, deleteUser } from '@/api'

const search = ref('')
const query = reactive({ page: 1, pageSize: 10 })
const tableData = ref([])
const total = ref(0)
const loading = ref(false)
const showForm = ref(false)
const formRef = ref()
const form = reactive({ id: null, username: '', email: '', phone: '', password: '123456', role: 'operator', department: '', status: 1 })
const rules = {
  username: [{ required: true, min: 3, max: 20, message: '用户名3-20字符', trigger: 'blur' }],
  email: [{ required: true, type: 'email', message: '邮箱格式错误', trigger: 'blur' }],
  password: [{ required: true, min: 6, message: '至少6位', trigger: 'blur' }],
  role: [{ required: true, message: '必选', trigger: 'change' }]
}

const roleLabel = (r) => ({ admin: '管理员', operator: '运营员', analyst: '分析师' })[r] || r

const resetForm = () => {
  Object.assign(form, { id: null, username: '', email: '', phone: '', password: '123456', role: 'operator', department: '', status: 1 })
}

const fetchData = async () => {
  loading.value = true
  try {
    const res = await getUserPage({ page: query.page, pageSize: query.pageSize, search: search.value })
    if (res.code === 0) {
      tableData.value = res.data.records || []
      total.value = Number(res.data.total) || 0
    }
  } catch (e) {
    ElMessage.error('加载员工列表失败')
  } finally { loading.value = false }
}

const onSearch = () => {
  query.page = 1
  fetchData()
}

const onRefresh = () => {
  search.value = ''
  query.page = 1
  fetchData()
}

const openAdd = () => {
  resetForm()
  showForm.value = true
}

const openEdit = (row) => {
  Object.assign(form, { ...row, password: '' })
  form.status = row.status
  showForm.value = true
}

const handleSave = async () => {
  await formRef.value.validate()
  try {
    if (form.id) {
      await updateUser({ id: form.id, email: form.email, phone: form.phone, role: form.role, department: form.department, status: form.status })
      ElMessage.success('员工信息已更新')
    } else {
      await createUser(form)
      ElMessage.success('员工添加成功')
    }
    showForm.value = false
    fetchData()
  } catch (e) {
    const msg = e?.response?.data?.msg || e?.message || '保存失败'
    ElMessage.error(msg)
  }
}

const resetPwd = async (row) => {
  try {
    await ElMessageBox.confirm(`确认将「${row.username}」的密码重置为 123456？`, '重置密码', { type: 'warning' })
    const res = await resetUserPassword({ id: row.id, password: '123456' })
    if (res.code === 0) {
      ElMessage.success(res.msg || '密码已重置')
    } else {
      ElMessage.error(res.msg || '重置失败')
    }
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('重置失败')
  }
}

const handleDelete = async (row) => {
  if (row.username === 'admin') {
    ElMessage.warning('超级管理员不可删除')
    return
  }
  try {
    await ElMessageBox.confirm(`确认删除员工「${row.username}」？此操作不可恢复`, '删除确认', { type: 'warning' })
    await deleteUser(row.id)
    ElMessage.success('员工已删除')
    if (tableData.value.length === 1 && query.page > 1) query.page--
    fetchData()
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('删除失败')
  }
}

onMounted(fetchData)
</script>

<style scoped lang="scss">
.user-mgmt { display: flex; flex-direction: column; height: 100%; }
.user-mgmt > .el-card { display: flex; flex-direction: column; flex: 1; :deep(.el-card__body) { display: flex; flex-direction: column; flex: 1; } }
.user-mgmt .search-bar { display: flex; gap: 12px; align-items: center; }
.refresh-btn { background: rgba(0, 217, 255, 0.1) !important; border-color: rgba(0, 217, 255, 0.3) !important; color: #00d9ff !important; }
.refresh-btn:hover { background: rgba(0, 217, 255, 0.2) !important; box-shadow: 0 0 12px rgba(0, 217, 255, 0.2) !important; }
.pagination { margin-top: 16px; text-align: right; }
</style>
