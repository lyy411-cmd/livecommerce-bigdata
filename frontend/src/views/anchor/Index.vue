<template>
  <div class="anchor">
    <el-card>
      <div class="search-bar">
        <el-input v-model="query.search" placeholder="搜索姓名/昵称/类目" clearable style="width:200px" @keyup.enter="onSearch" />
        <el-select v-model="query.level" placeholder="等级" clearable style="width:140px">
          <el-option label="S级" value="S" />
          <el-option label="A级" value="A" />
          <el-option label="B级" value="B" />
          <el-option label="C级" value="C" />
        </el-select>
        <el-button type="primary" @click="onSearch">搜索</el-button>
        <el-button @click="onRefresh" class="refresh-btn">刷新</el-button>
        <el-button @click="showCreate = true">新增主播</el-button>
      </div>
      <el-table :data="tableData" stripe v-loading="loading" height="calc(100vh - 380px)" style="margin-top:16px">
        <el-table-column prop="name" label="姓名" width="100" />
        <el-table-column prop="nickname" label="昵称" width="100" />
        <el-table-column prop="level" label="等级" width="80">
          <template #default="{ row }">
            <el-tag :type="row.level === 'S' ? 'danger' : row.level === 'A' ? 'warning' : 'info'">{{ row.level }}级</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="category" label="擅长类目" width="120" />
        <el-table-column prop="fansCount" label="粉丝数" width="120" sortable>
          <template #default="{ row }">{{ Number(row.fansCount || 0).toLocaleString() }}</template>
        </el-table-column>
        <el-table-column prop="liveHours" label="直播时长" width="100" />
        <el-table-column prop="totalGmv" label="总GMV" width="140" sortable>
          <template #default="{ row }">￥{{ Number(row.totalGmv || 0).toLocaleString() }}</template>
        </el-table-column>
        <el-table-column prop="totalOrders" label="总订单" width="100" sortable />
        <el-table-column label="操作" width="160" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="handleEdit(row)">编辑</el-button>
            <el-button text type="danger" size="small" @click="handleDelete(row.id)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-pagination class="pagination" v-model:current-page="query.page" :page-size="query.pageSize"
        :total="total" @current-change="fetchData" layout="total, prev, pager, next" />
    </el-card>

    <el-dialog v-model="showCreate" :title="editing.id ? '编辑主播' : '新增主播'" width="600px">
      <el-form :model="editing" label-width="100px">
        <el-form-item label="姓名"><el-input v-model="editing.name" /></el-form-item>
        <el-form-item label="昵称"><el-input v-model="editing.nickname" /></el-form-item>
        <el-form-item label="等级">
          <el-select v-model="editing.level" style="width:100%">
            <el-option label="S级" value="S" />
            <el-option label="A级" value="A" />
            <el-option label="B级" value="B" />
            <el-option label="C级" value="C" />
          </el-select>
        </el-form-item>
        <el-form-item label="擅长类目">
          <el-select v-model="editing.category" style="width:100%">
            <el-option v-for="c in ['美妆', '服饰', '食品', '数码', '家居']" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item label="粉丝数"><el-input-number v-model="editing.fansCount" :min="0" style="width:100%" /></el-form-item>
        <el-form-item label="简介"><el-input v-model="editing.intro" type="textarea" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showCreate = false">取消</el-button>
        <el-button type="primary" @click="handleSave">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { getAnchorPage, createAnchor, updateAnchor, deleteAnchor } from '@/api'

const query = reactive({ page: 1, pageSize: 10, level: '', search: '' })
const tableData = ref([])
const total = ref(0)
const loading = ref(false)
const showCreate = ref(false)
const editing = reactive({ id: null, name: '', nickname: '', level: 'A', category: '美妆', fansCount: 0, intro: '' })

const fetchData = async () => {
  loading.value = true
  try {
    const res = await getAnchorPage(query)
    tableData.value = res.data.records || []
    total.value = Number(res.data.total) || 0
  } finally { loading.value = false }
}

const onSearch = () => { query.page = 1; fetchData() }
const onRefresh = () => { query.search = ''; query.level = ''; query.page = 1; fetchData() }

const handleEdit = (row) => { Object.assign(editing, row); showCreate.value = true }
const handleSave = async () => {
  try {
    if (editing.id) await updateAnchor(editing)
    else await createAnchor(editing)
    showCreate.value = false
    fetchData()
    ElMessage.success('保存成功')
  } catch { ElMessage.error('保存失败') }
}
const handleDelete = async (id) => {
  await ElMessageBox.confirm('确认删除？', '提示', { type: 'warning' })
  await deleteAnchor(id)
  fetchData()
  ElMessage.success('删除成功')
}

onMounted(fetchData)
</script>

<style scoped lang="scss">
.anchor { display: flex; flex-direction: column; height: 100%; }
.anchor > .el-card { display: flex; flex-direction: column; flex: 1; :deep(.el-card__body) { display: flex; flex-direction: column; flex: 1; } }
.anchor .search-bar { display: flex; gap: 12px; }
.pagination { margin-top: 16px; text-align: right; }
</style>
