import request from '@/utils/request'

export function getVehicles(params) {
  return request.get('/vehicles', { params })
}

export function getVehicleTrack(id) {
  return request.get(`/vehicles/${id}/track`)
}

export function createVehicle(data) {
  return request.post('/vehicles', data)
}

export function updateVehicle(id, data) {
  return request.put(`/vehicles/${id}`, data)
}
