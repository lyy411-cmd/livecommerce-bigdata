<template>
  <div class="liveroom">
    <el-card>
      <div class="search-bar">
        <el-input v-model="query.search" placeholder="搜索直播间/主播名" clearable style="width:200px" @keyup.enter="onSearch" />
        <el-select v-model="query.platform" placeholder="平台" clearable style="width:140px">
          <el-option label="抖音" value="douyin" />
          <el-option label="快手" value="kuaishou" />
          <el-option label="淘宝" value="taobao" />
        </el-select>
        <el-select v-model="query.status" placeholder="状态" clearable style="width:140px">
          <el-option label="直播中" value="live" />
          <el-option label="暂停" value="paused" />
          <el-option label="已结束" value="finished" />
        </el-select>
        <el-button type="primary" @click="onSearch">搜索</el-button>
        <el-button @click="onRefresh" class="refresh-btn">刷新</el-button>
        <el-button @click="showCreate = true">新增直播间</el-button>
      </div>
      <el-table :data="tableData" stripe v-loading="loading" height="calc(100vh - 380px)" style="margin-top:16px">
        <el-table-column prop="roomNo" label="编号" width="100" />
        <el-table-column prop="roomName" label="直播间" />
        <el-table-column prop="anchorName" label="主播" width="100" />
        <el-table-column prop="platform" label="平台" width="90">
          <template #default="{ row }">
            <el-tag :type="row.platform === 'taobao' ? 'warning' : row.platform === 'douyin' ? '' : 'success'" size="small">
              {{ {taobao:'淘宝', douyin:'抖音', kuaishou:'快手'}[row.platform] || row.platform }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="category" label="类目" width="100" />
        <el-table-column prop="viewerCount" label="在线" width="100" sortable />
        <el-table-column prop="orderCount" label="订单" width="80" sortable />
        <el-table-column prop="gmv" label="GMV" width="120">
          <template #default="{ row }">￥{{ Number(row.gmv || 0).toLocaleString() }}</template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 'live' ? 'success' : 'info'">
              {{ row.status === 'live' ? '直播中' : row.status === 'paused' ? '暂停' : '已结束' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="180" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="handleEdit(row)">编辑</el-button>
            <el-button text type="danger" size="small" @click="handleDelete(row.id)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-pagination class="pagination" v-model:current-page="query.page" :page-size="query.pageSize"
        :total="total" @current-change="fetchData" layout="total, prev, pager, next" />
    </el-card>

    <el-dialog v-model="showCreate" :title="editing.id ? '编辑直播间' : '新增直播间'" width="600px">
      <el-form :model="editing" label-width="100px">
        <el-form-item label="直播间名"><el-input v-model="editing.roomName" /></el-form-item>
        <el-form-item label="主播"><el-input v-model="editing.anchorName" /></el-form-item>
        <el-form-item label="平台">
          <el-select v-model="editing.platform" style="width:100%">
            <el-option label="抖音" value="douyin" />
            <el-option label="快手" value="kuaishou" />
            <el-option label="淘宝" value="taobao" />
          </el-select>
        </el-form-item>
        <el-form-item label="类目">
          <el-select v-model="editing.category" style="width:100%">
            <el-option v-for="c in ['美妆', '服饰', '食品', '数码', '家居']" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item label="状态">
          <el-select v-model="editing.status" style="width:100%">
            <el-option label="直播中" value="live" />
            <el-option label="暂停" value="paused" />
            <el-option label="已结束" value="finished" />
          </el-select>
        </el-form-item>
        <el-form-item label="在线人数"><el-input-number v-model="editing.viewerCount" :min="0" style="width:100%" /></el-form-item>
        <el-form-item label="订单数"><el-input-number v-model="editing.orderCount" :min="0" style="width:100%" /></el-form-item>
        <el-form-item label="GMV"><el-input-number v-model="editing.gmv" :min="0" style="width:100%" /></el-form-item>
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
import { getRoomPage, createRoom, updateRoom, deleteRoom } from '@/api'

const query = reactive({ page: 1, pageSize: 10, platform: '', status: '', search: '' })
const tableData = ref([])
const total = ref(0)
const loading = ref(false)
const showCreate = ref(false)
const editing = reactive({ id: null, roomName: '', anchorName: '', platform: 'douyin', category: '美妆', status: 'live', viewerCount: 0, orderCount: 0, gmv: 0 })

const fetchData = async () => {
  loading.value = true
  try {
    const res = await getRoomPage(query)
    tableData.value = res.data.records || []
    total.value = Number(res.data.total) || 0
  } catch (e) { console.error(e) } finally { loading.value = false }
}

const onSearch = () => { query.page = 1; fetchData() }
const onRefresh = () => { query.search = ''; query.platform = ''; query.status = ''; query.page = 1; fetchData() }

const handleEdit = (row) => {
  Object.assign(editing, row)
  showCreate.value = true
}

const handleSave = async () => {
  try {
    if (editing.id) {
      await updateRoom(editing)
    } else {
      await createRoom(editing)
    }
    showCreate.value = false
    fetchData()
    ElMessage.success('保存成功')
  } catch (e) { ElMessage.error('保存失败') }
}

const handleDelete = async (id) => {
  await ElMessageBox.confirm('确认删除？', '提示', { type: 'warning' })
  await deleteRoom(id)
  fetchData()
  ElMessage.success('删除成功')
}

onMounted(fetchData)
</script>

<style scoped lang="scss">
.liveroom { display: flex; flex-direction: column; height: 100%; }
.liveroom > .el-card { display: flex; flex-direction: column; flex: 1; :deep(.el-card__body) { display: flex; flex-direction: column; flex: 1; } }
.liveroom .search-bar { display: flex; gap: 12px; }
.pagination { margin-top: 16px; text-align: right; }
</style>
