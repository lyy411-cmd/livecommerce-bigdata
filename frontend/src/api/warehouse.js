import request from '@/utils/request'

export function getWarehouses(params) {
  return request.get('/warehouses', { params })
}

export function getWarehouseDetail(id) {
  return request.get(`/warehouses/${id}`)
}

export function getInventory(params) {
  return request.get('/warehouses/inventory', { params })
}

export function inbound(data) {
  return request.post('/warehouses/inbound', data)
}

export function outbound(data) {
  return request.post('/warehouses/outbound', data)
}

export function getWarehouseStats() {
  return request.get('/warehouses/stats')
}
