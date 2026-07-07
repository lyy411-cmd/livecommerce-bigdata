<template>
  <div class="user-management">
    <el-card shadow="never">
      <template #header>
        <div class="flex-between">
          <span>用户管理</span>
          <el-button type="primary" size="small" icon="Plus" id="btn-add-staff" @click="showAddDialog">添加用户</el-button>
        </div>
      </template>

      <!-- 搜索 -->
      <div class="search-bar">
        <el-input v-model="search" placeholder="搜索用户名或邮箱" id="user-search" prefix-icon="Search" clearable style="width:280px" />
        <el-select v-model="roleFilter" placeholder="角色筛选" id="user-role-filter" clearable style="width:140px;margin-left:12px">
          <el-option label="管理员" value="admin" />
          <el-option label="调度员" value="dispatcher" />
          <el-option label="司机" value="driver" />
          <el-option label="仓管员" value="warehouse_keeper" />
        </el-select>
      </div>

      <el-table :data="filteredUsers" stripe v-loading="loading" style="width:100%;margin-top:16px">
        <el-table-column prop="id" label="ID" width="60" />
        <el-table-column prop="username" label="用户名" width="120" />
        <el-table-column prop="email" label="邮箱" width="200" />
        <el-table-column prop="role" label="角色" width="120">
          <template #default="{ row }">
            <el-tag :type="roleTagType(row.role)">{{ roleMap[row.role] }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="phone" label="手机号" width="140" />
        <el-table-column prop="department" label="部门" width="120" />
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-switch
              v-model="row.status"
              :active-value="1"
              :inactive-value="0"
              @change="toggleStatus(row)"
            />
          </template>
        </el-table-column>
        <el-table-column prop="created_at" label="创建时间" width="170" />
        <el-table-column label="操作" width="180" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="showEditDialog(row)">编辑</el-button>
            <el-button text type="warning" size="small" @click="resetPwd(row)">重置密码</el-button>
            <el-popconfirm title="确定删除?" @confirm="doDelete(row.id)">
              <template #reference>
                <el-button text type="danger" size="small">删除</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>

      <div class="pagination-wrap">
        <el-pagination
          v-model:current-page="page"
          :page-size="10"
          layout="total, prev, pager, next"
          :total="users.length"
        />
      </div>
    </el-card>

    <!-- 添加/编辑用户弹窗 -->
    <el-dialog v-model="dialogVisible" :title="isEdit ? '编辑用户' : '添加用户'" width="500px" center>
      <el-form ref="formRef" :model="form" :rules="formRules" label-width="90px">
        <el-form-item label="用户名" prop="username">
          <el-input v-model="form.username" :disabled="isEdit" id="staff-username" />
        </el-form-item>
        <el-form-item label="邮箱" prop="email">
          <el-input v-model="form.email" id="staff-email" />
        </el-form-item>
        <el-form-item label="角色" prop="role">
          <el-select v-model="form.role" id="staff-role" style="width:100%">
            <el-option label="管理员" value="admin" />
            <el-option label="调度员" value="dispatcher" />
            <el-option label="司机" value="driver" />
            <el-option label="仓管员" value="warehouse_keeper" />
          </el-select>
        </el-form-item>
        <el-form-item label="手机号" prop="phone">
          <el-input v-model="form.phone" id="staff-phone" />
        </el-form-item>
        <el-form-item label="部门" prop="department">
          <el-input v-model="form.department" id="staff-department" />
        </el-form-item>
        <el-form-item v-if="!isEdit" label="密码" prop="password">
          <el-input v-model="form.password" type="password" id="staff-password" show-password />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitLoading" id="btn-submit-staff" @click="submitForm">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed } from 'vue'

const search = ref('')
const roleFilter = ref('')
const page = ref(1)
const loading = ref(false)

const roleMap = { admin: '管理员', dispatcher: '调度员', driver: '司机', warehouse_keeper: '仓管员' }
const roleTagType = (r) => ({ admin: 'danger', dispatcher: '', driver: 'success', warehouse_keeper: 'warning' }[r])

const users = ref([
  { id: 1, username: 'admin', email: 'admin@logistics.com', role: 'admin', phone: '13800138000', department: '技术部', status: 1, created_at: '2026-01-15 09:00:00' },
  { id: 2, username: 'zhangsan', email: 'zhangsan@logistics.com', role: 'dispatcher', phone: '13900139001', department: '调度中心', status: 1, created_at: '2026-02-20 14:30:00' },
  { id: 3, username: 'lisi', email: 'lisi@logistics.com', role: 'driver', phone: '13700137002', department: '运输部', status: 1, created_at: '2026-03-10 11:00:00' },
  { id: 4, username: 'wangwu', email: 'wangwu@logistics.com', role: 'warehouse_keeper', phone: '13600136003', department: '仓储部', status: 0, created_at: '2026-03-15 08:30:00' },
  { id: 5, username: 'zhaoliu', email: 'zhaoliu@logistics.com', role: 'driver', phone: '13500135004', department: '运输部', status: 1, created_at: '2026-04-05 10:15:00' },
  { id: 6, username: 'sunqi', email: 'sunqi@logistics.com', role: 'dispatcher', phone: '13400134005', department: '调度中心', status: 1, created_at: '2026-04-20 16:00:00' },
  { id: 7, username: 'zhouba', email: 'zhouba@logistics.com', role: 'warehouse_keeper', phone: '13300133006', department: '仓储部', status: 1, created_at: '2026-05-10 09:45:00' },
  { id: 8, username: 'wujiu', email: 'wujiu@logistics.com', role: 'driver', phone: '13200132007', department: '运输部', status: 0, created_at: '2026-05-18 13:20:00' }
])

const filteredUsers = computed(() => {
  let list = users.value
  if (search.value) list = list.filter(u => u.username.includes(search.value) || u.email.includes(search.value))
  if (roleFilter.value) list = list.filter(u => u.role === roleFilter.value)
  return list
})

const dialogVisible = ref(false)
const isEdit = ref(false)
const editUserId = ref(null)
const formRef = ref()
const submitLoading = ref(false)

const form = reactive({
  username: '', email: '', role: 'dispatcher', phone: '', department: '', password: ''
})

const formRules = {
  username: [{ required: true, message: '必填', trigger: 'blur' }],
  email: [{ required: true, message: '必填', trigger: 'blur' }, { type: 'email', message: '格式不正确', trigger: 'blur' }],
  role: [{ required: true, message: '必选', trigger: 'change' }]
}

const showAddDialog = () => {
  isEdit.value = false; editUserId.value = null
  Object.assign(form, { username: '', email: '', role: 'dispatcher', phone: '', department: '', password: '' })
  dialogVisible.value = true
}

const showEditDialog = (row) => {
  isEdit.value = true; editUserId.value = row.id
  Object.assign(form, {
    username: row.username, email: row.email, role: row.role,
    phone: row.phone, department: row.department, password: ''
  })
  dialogVisible.value = true
}

const submitForm = async () => {
  await formRef.value.validate()
  submitLoading.value = true
  try {
    if (isEdit.value) {
      const idx = users.value.findIndex(u => u.id === editUserId.value)
      if (idx > -1) Object.assign(users.value[idx], { email: form.email, role: form.role, phone: form.phone, department: form.department })
      ElMessage.success('更新成功')
    } else {
      const maxId = Math.max(...users.value.map(u => u.id), 0)
      users.value.unshift({
        id: maxId + 1, username: form.username, email: form.email,
        role: form.role, phone: form.phone, department: form.department,
        status: 1, created_at: new Date().toLocaleString()
      })
      ElMessage.success('添加成功')
    }
    dialogVisible.value = false
  } finally { submitLoading.value = false }
}

const toggleStatus = (row) => {
  ElMessage.success(`用户 ${row.username} ${row.status === 1 ? '已启用' : '已禁用'}`)
}

const resetPwd = (row) => {
  ElMessage.success(`已重置 ${row.username} 的密码为默认密码`)
}

const doDelete = (id) => {
  users.value = users.value.filter(u => u.id !== id)
  ElMessage.success('删除成功')
}
</script>

<style scoped lang="scss">
.user-management { .search-bar { margin-top: 8px; } }
.pagination-wrap { display: flex; justify-content: flex-end; margin-top: 16px; }
</style>
