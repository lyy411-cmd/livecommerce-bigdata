import request from '@/utils/request'

// 客户查看自己的订单
export function getMyOrders(params) {
  return request.get('/orders', { params })
}

// 客户创建订单
export function createMyOrder(data) {
  return request.post('/orders', data)
}

// 客户查看订单统计
export function getMyOrderStats() {
  return request.get('/orders/my-stats')
}

// 客户查看订单详情
export function getMyOrderDetail(id) {
  return request.get(`/orders/${id}`)
}
