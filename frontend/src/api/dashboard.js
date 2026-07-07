import request from '@/utils/request'

export function getDashboardStats() {
  return request.get('/dashboard/stats')
}

export function getDashboardTrend() {
  return request.get('/dashboard/trend')
}

export function getDashboardGeo() {
  return request.get('/dashboard/geo')
}
