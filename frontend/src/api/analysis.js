import request from '@/utils/request'

export function getHotRouteAnalysis() {
  return request.get('/analysis/hot-routes')
}

export function getDelayAnalysis() {
  return request.get('/analysis/delay')
}

export function getCostAnalysis(params) {
  return request.get('/analysis/cost', { params })
}

export function getPrediction() {
  return request.get('/analysis/prediction')
}
