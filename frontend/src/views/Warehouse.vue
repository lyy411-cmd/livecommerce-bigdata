<template>
  <div class="warehouse">
    <el-row :gutter="16" class="overview-row">
      <el-col :span="6" v-for="wh in warehouses" :key="wh.id">
        <el-card shadow="hover" class="wh-card" :class="{ active: activeWh === wh.id }" @click="activeWh = wh.id">
          <div class="wh-header">
            <span class="wh-name">{{ wh.name }}</span>
            <el-tag :type="wh.status === '正常' ? 'success' : 'warning'" size="small">{{ wh.status }}</el-tag>
          </div>
          <div class="wh-stats">
            <div class="wh-stat">
              <p>存储容量</p>
              <el-progress :percentage="wh.usage" :color="wh.usage > 80 ? '#F56C6C' : '#409EFF'" />
              <span class="usage-text">已用 {{ wh.used_m3 }}m³ / 总 {{ wh.capacity_m3 }}m³</span>
            </div>
            <div class="wh-meta">
              <span>SKU: {{ wh.sku_count }}</span>
              <span>人员: {{ wh.staff_count }}人</span>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16" class="operation-row">
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>┃ 入库操作</span></template>
          <el-form label-width="90px" size="default">
            <el-form-item label="仓库">
              <el-select v-model="inboundForm.warehouse_id" id="wh-inbound-select" style="width:100%">
                <el-option v-for="wh in warehouses" :key="wh.id" :label="wh.name" :value="wh.id" />
              </el-select>
            </el-form-item>
            <el-form-item label="商品名称">
              <el-input v-model="inboundForm.product_name" placeholder="如：电子产品、日用品" id="wh-inbound-name" />
            </el-form-item>
            <el-form-item label="数量">
              <el-input-number v-model="inboundForm.quantity" :min="1" id="wh-inbound-qty" style="width:100%" />
            </el-form-item>
            <el-form-item label="体积(m³)">
              <el-input-number v-model="inboundForm.volume" :min="0" :precision="2" style="width:100%" placeholder="单件体积" />
            </el-form-item>
            <el-form-item>
              <el-button type="primary" id="btn-inbound" @click="doInbound">确认入库</el-button>
            </el-form-item>
          </el-form>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header><span>┃ 出库操作</span></template>
          <el-form label-width="90px" size="default">
            <el-form-item label="仓库">
              <el-select v-model="outboundForm.warehouse_id" id="wh-outbound-select" style="width:100%">
                <el-option v-for="wh in warehouses" :key="wh.id" :label="wh.name" :value="wh.id" />
              </el-select>
            </el-form-item>
            <el-form-item label="商品名称">
              <el-input v-model="outboundForm.product_name" placeholder="输入要出库的商品名" id="wh-outbound-name" />
            </el-form-item>
            <el-form-item label="数量">
              <el-input-number v-model="outboundForm.quantity" :min="1" id="wh-outbound-qty" style="width:100%" />
            </el-form-item>
            <el-form-item label="目的地">
              <el-input v-model="outboundForm.destination" placeholder="如：广州白云机场 / 上海市浦东新区" id="wh-outbound-dest" />
            </el-form-item>
            <el-form-item>
              <el-button type="warning" id="btn-outbound" @click="doOutbound">确认出库</el-button>
            </el-form-item>
          </el-form>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" class="inventory-table">
      <template #header>
        <div class="flex-between">
          <span>库存列表</span>
          <el-input v-model="inventorySearch" placeholder="搜索商品名称" id="inventory-search" prefix-icon="Search" clearable style="width:240px" @clear="inventorySearch = ''" />
        </div>
      </template>
      <el-table :data="filteredInventory" stripe>
        <el-table-column prop="product_name" label="商品名称" min-width="140" />
        <el-table-column prop="warehouse" label="所在仓库" width="130" />
        <el-table-column prop="sku" label="SKU" width="120" />
        <el-table-column prop="quantity" label="数量" width="80" sortable />
        <el-table-column prop="volume_total" label="总体积(m³)" width="110" />
        <el-table-column prop="last_update" label="最后更新" width="170" />
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag :type="row.quantity < 100 ? 'danger' : row.quantity < 500 ? 'warning' : 'success'" size="small">
              {{ row.quantity < 100 ? '低库存' : row.quantity < 500 ? '正常' : '充足' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, computed } from 'vue'

const activeWh = ref(1)
const warehouses = ref([
  { id: 1, name: '深圳华南仓', status: '正常', usage: 72, used_m3: 3600, capacity_m3: 5000, sku_count: 2850, staff_count: 42 },
  { id: 2, name: '北京大兴仓', status: '正常', usage: 58, used_m3: 2900, capacity_m3: 5000, sku_count: 2100, staff_count: 35 },
  { id: 3, name: '成都高新仓', status: '预警', usage: 85, used_m3: 2975, capacity_m3: 3500, sku_count: 1680, staff_count: 28 },
  { id: 4, name: '武汉光谷仓', status: '正常', usage: 65, used_m3: 2600, capacity_m3: 4000, sku_count: 1950, staff_count: 30 }
])

const inboundForm = reactive({ warehouse_id: 1, product_name: '', quantity: 1, volume: 0 })
const outboundForm = reactive({ warehouse_id: 1, product_name: '', quantity: 1, destination: '' })
const inventorySearch = ref('')

const inventory = ref([
  { product_name: '电子产品A型', warehouse: '深圳华南仓', sku: 'ELC-001', quantity: 1200, volume_total: '36.0', last_update: '2026-06-26 14:30' },
  { product_name: '家电配件B型', warehouse: '深圳华南仓', sku: 'HAP-002', quantity: 350, volume_total: '52.5', last_update: '2026-06-26 10:15' },
  { product_name: '汽车零部件C', warehouse: '北京大兴仓', sku: 'APT-003', quantity: 80, volume_total: '24.0', last_update: '2026-06-25 16:00' },
  { product_name: '日用品D系列', warehouse: '成都高新仓', sku: 'DCG-004', quantity: 650, volume_total: '45.5', last_update: '2026-06-26 08:45' },
  { product_name: '食品E类', warehouse: '武汉光谷仓', sku: 'FD-005', quantity: 45, volume_total: '6.8', last_update: '2026-06-25 20:00' },
  { product_name: '医疗器械F型', warehouse: '深圳华南仓', sku: 'MD-006', quantity: 520, volume_total: '15.6', last_update: '2026-06-26 11:20' },
  { product_name: '服装G品牌', warehouse: '北京大兴仓', sku: 'CL-007', quantity: 2100, volume_total: '84.0', last_update: '2026-06-24 09:00' },
  { product_name: '建材H类', warehouse: '成都高新仓', sku: 'BC-008', quantity: 180, volume_total: '90.0', last_update: '2026-06-26 13:10' }
])

const filteredInventory = computed(() => {
  if (!inventorySearch.value) return inventory.value
  return inventory.value.filter(item =>
    item.product_name.includes(inventorySearch.value) ||
    item.sku.toLowerCase().includes(inventorySearch.value.toLowerCase())
  )
})

const doInbound = () => {
  const whName = warehouses.value.find(w => w.id === inboundForm.warehouse_id)?.name || ''
  // 检查同名商品是否已存在，存在则合并
  const existing = inventory.value.find(
    item => item.warehouse === whName && item.product_name === inboundForm.product_name
  )
  if (existing) {
    existing.quantity += inboundForm.quantity
    existing.volume_total = (parseFloat(existing.volume_total) + inboundForm.volume).toFixed(2)
    existing.last_update = new Date().toLocaleString()
    ElMessage.success(`${inboundForm.product_name} 入库成功，数量已合并为 ${existing.quantity}`)
  } else {
    inventory.value.unshift({
      product_name: inboundForm.product_name,
      warehouse: whName,
      sku: 'SKU-' + Date.now().toString(36).toUpperCase(),
      quantity: inboundForm.quantity,
      volume_total: inboundForm.volume.toString(),
      last_update: new Date().toLocaleString()
    })
    ElMessage.success(`入库成功：${inboundForm.product_name} x${inboundForm.quantity}`)
  }

  // 更新仓库使用量
  const wh = warehouses.value.find(w => w.id === inboundForm.warehouse_id)
  if (wh) {
    wh.used_m3 += inboundForm.volume
    wh.usage = Math.round(wh.used_m3 / wh.capacity_m3 * 100)
    wh.sku_count += 1
  }
}

const doOutbound = () => {
  const whName = warehouses.value.find(w => w.id === outboundForm.warehouse_id)?.name || ''
  const item = inventory.value.find(
    i => i.warehouse === whName && i.product_name === outboundForm.product_name
  )
  if (!item) {
    ElMessage.warning(`仓库中未找到 "${outboundForm.product_name}"`)
    return
  }
  if (item.quantity < outboundForm.quantity) {
    ElMessage.warning(`库存不足，当前只有 ${item.quantity} 件`)
    return
  }
  item.quantity -= outboundForm.quantity
  item.volume_total = (parseFloat(item.volume_total) * (1 - outboundForm.quantity / (item.quantity + outboundForm.quantity))).toFixed(2)
  item.last_update = new Date().toLocaleString()
  ElMessage.success(`出库成功：${outboundForm.product_name} x${outboundForm.quantity} → ${outboundForm.destination}`)
}
</script>

<style scoped lang="scss">
.warehouse { display: flex; flex-direction: column; gap: 16px; }

.wh-card {
  cursor: pointer; border: 2px solid transparent; transition: all 0.3s;
  &.active { border-color: #409EFF; }
}
.wh-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
.wh-name { font-size: 15px; font-weight: 600; }
.wh-stat { margin-bottom: 10px; .usage-text { font-size: 12px; color: #909399; margin-top: 4px; display: block; } }
.wh-meta { display: flex; justify-content: space-between; font-size: 13px; color: #606266; }
.operation-row { margin-bottom: 0; }
</style>
