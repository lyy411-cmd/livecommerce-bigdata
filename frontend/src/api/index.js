import request from '@/utils/request'

// ===== 认证 =====
export const login = (data) => request.post('/auth/login', data)
export const register = (data) => request.post('/auth/register', data)
export const getMe = () => request.get('/auth/me')
export const logout = () => request.post('/auth/logout')

// ===== 用户管理 =====
export const getUserPage = (params) => request.get('/system/user/page', { params })
export const createUser = (data) => request.post('/system/user/create', data)
export const updateUser = (data) => request.put('/system/user/update', data)
export const resetUserPassword = (data) => request.post('/system/user/reset-password', data)
export const deleteUser = (id) => request.delete('/system/user/delete', { params: { id } })

// ===== 直播间 =====
export const getRoomPage = (params) => request.get('/livecommerce/room/page', { params })
export const getRoomOverview = () => request.get('/livecommerce/room/overview')
export const getLiveRooms = () => request.get('/livecommerce/room/live')
export const getRoomDetail = (id) => request.get('/livecommerce/room/detail', { params: { id } })
export const createRoom = (data) => request.post('/livecommerce/room/create', data)
export const updateRoom = (data) => request.put('/livecommerce/room/update', data)
export const deleteRoom = (id) => request.delete('/livecommerce/room/delete', { params: { id } })

// ===== 主播 =====
export const getAnchorPage = (params) => request.get('/livecommerce/anchor/page', { params })
export const getAnchorTop = (limit = 10) => request.get('/livecommerce/anchor/top', { params: { limit } })
export const getAnchorDetail = (id) => request.get('/livecommerce/anchor/detail', { params: { id } })
export const createAnchor = (data) => request.post('/livecommerce/anchor/create', data)
export const updateAnchor = (data) => request.put('/livecommerce/anchor/update', data)
export const deleteAnchor = (id) => request.delete('/livecommerce/anchor/delete', { params: { id } })

// ===== 订单 =====
export const getOrderPage = (params) => request.get('/livecommerce/order/page', { params })
export const getMyOrders = (userId) => request.get('/livecommerce/order/my', { params: { userId } })
export const getOrderDetail = (id) => request.get('/livecommerce/order/detail', { params: { id } })
export const getOrderOverview = () => request.get('/livecommerce/order/overview')
export const createOrder = (data) => request.post('/livecommerce/order/create', data)
export const payOrder = (id) => request.post('/livecommerce/order/pay', null, { params: { id } })
export const shipOrder = (id) => request.post('/livecommerce/order/ship', null, { params: { id } })
export const confirmOrder = (id) => request.post('/livecommerce/order/confirm', null, { params: { id } })

// ===== 数据管道 =====
export const getPipelineStatus = () => request.get('/datapipeline/status')
export const simulateEvent = (data) => request.post('/datapipeline/simulate', data)
export const batchSimulate = (count) => request.post('/datapipeline/batch-simulate', null, { params: { count } })

// ===== 数据可视化 =====
export const getDashboardKpi = () => request.get('/datavis/dashboard/kpi')
export const getGmvTrend = (months = 12) => request.get('/datavis/dashboard/gmv-trend', { params: { months } })
export const getPlatformDistribution = () => request.get('/datavis/dashboard/platform-distribution')
export const getCategoryRank = () => request.get('/datavis/dashboard/category-rank')
export const getAnchorRank = (limit = 10) => request.get('/datavis/dashboard/anchor-rank', { params: { limit } })
export const getGeoDistribution = () => request.get('/datavis/dashboard/geo-distribution')
export const getRealtimeData = () => request.get('/datavis/dashboard/realtime')
