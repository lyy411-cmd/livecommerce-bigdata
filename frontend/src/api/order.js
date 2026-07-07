import request from '@/utils/request'

export function getOrders(params) {
  return request.get('/orders', { params })
}

export function getOrderDetail(id) {
  return request.get(`/orders/${id}`)
}

export function createOrder(data) {
  return request.post('/orders', data)
}

export function updateOrder(id, data) {
  return request.put(`/orders/${id}`, data)
}

export function deleteOrder(id) {
  return request.delete(`/orders/${id}`)
}

export function getOrderStats() {
  return request.get('/orders/stats/summary')
}

// 客户签收订单
export function signOrder(orderId) {
  return request.post(`/orders/${orderId}/sign`)
}
