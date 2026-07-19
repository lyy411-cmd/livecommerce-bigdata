import { createRouter, createWebHashHistory } from 'vue-router'

const routes = [
  { path: '/login', component: () => import('@/views/Login.vue'), meta: { noAuth: true } },
  {
    path: '/',
    component: () => import('@/layout/Index.vue'),
    redirect: '/dashboard',
    children: [
      { path: 'dashboard', name: 'Dashboard', component: () => import('@/views/dashboard/Index.vue'), meta: { title: '数据看板', icon: 'Odometer' } },
      { path: 'architecture', name: 'Architecture', component: () => import('@/views/dashboard/Architecture.vue'), meta: { title: '系统架构', icon: 'Connection' } },
      { path: 'live-room', name: 'LiveRoom', component: () => import('@/views/liveroom/Index.vue'), meta: { title: '直播间', icon: 'VideoCamera' } },
      { path: 'live-room/:roomId', name: 'LiveRoomDetail', component: () => import('@/views/liveroom/Detail.vue'), meta: { title: '直播间详情' } },
      { path: 'anchor', name: 'Anchor', component: () => import('@/views/anchor/Index.vue'), meta: { title: '主播', icon: 'UserFilled' } },
      { path: 'order', name: 'Order', component: () => import('@/views/order/Index.vue'), meta: { title: '订单', icon: 'List' } },
      { path: 'realtime', name: 'Realtime', component: () => import('@/views/dashboard/Realtime.vue'), meta: { title: '实时直播', icon: 'VideoPlay' } },
      { path: 'bigscreen', name: 'BigScreen', component: () => import('@/views/dashboard/BigScreen.vue'), meta: { title: '数据大屏', icon: 'DataLine' } },
      { path: 'analysis', name: 'Analysis', component: () => import('@/views/dashboard/Analysis.vue'), meta: { title: '深度分析', icon: 'TrendCharts' } },
      { path: 'user', name: 'UserMgmt', component: () => import('@/views/system/User.vue'), meta: { title: '员工管理', icon: 'User', adminOnly: true } }
    ]
  }
]

const router = createRouter({
  history: createWebHashHistory(),
  routes
})

router.beforeEach((to, from, next) => {
  const token = localStorage.getItem('lc_token')
  if (to.path === '/login') return next()
  if (!token) return next('/login')
  if (to.meta.adminOnly) {
    const userInfo = JSON.parse(localStorage.getItem('lc_user') || '{}')
    if (userInfo.role !== 'admin') {
      ElMessage.warning('仅管理员可访问该页面')
      return next('/dashboard')
    }
  }
  next()
})

export default router
