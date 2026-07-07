<template>
  <div class="layout">
    <aside class="sidebar" :style="{ width: collapsed ? '60px' : '220px' }">
      <div class="logo" @click="$router.push('/dashboard')">
        <span class="logo-icon">⬡</span>
        <span v-if="!collapsed" class="logo-text">直播大数据平台</span>
      </div>
      <nav class="nav">
        <router-link to="/dashboard" class="nav-item" :class="{ active: $route.path === '/dashboard' }">
          <span class="nav-icon icon-c1">◆</span>
          <span v-if="!collapsed">数据看板</span>
        </router-link>
        <router-link to="/live-room" class="nav-item">
          <span class="nav-icon icon-c2">◈</span>
          <span v-if="!collapsed">直播间管理</span>
        </router-link>
        <router-link to="/anchor" class="nav-item">
          <span class="nav-icon icon-c3">◉</span>
          <span v-if="!collapsed">主播管理</span>
        </router-link>
        <router-link to="/order" class="nav-item">
          <span class="nav-icon icon-c4">▣</span>
          <span v-if="!collapsed">订单管理</span>
        </router-link>
        <router-link to="/realtime" class="nav-item">
          <span class="nav-icon icon-c5">◎</span>
          <span v-if="!collapsed">实时直播</span>
        </router-link>
        <router-link to="/analysis" class="nav-item">
          <span class="nav-icon icon-c6">◈</span>
          <span v-if="!collapsed">深度分析</span>
        </router-link>
        <router-link to="/architecture" class="nav-item">
          <span class="nav-icon icon-c7">◫</span>
          <span v-if="!collapsed">系统架构</span>
        </router-link>
        <router-link to="/bigscreen" class="nav-item">
          <span class="nav-icon icon-c8">◈</span>
          <span v-if="!collapsed">数据大屏</span>
        </router-link>
        <router-link v-if="isAdmin" to="/user" class="nav-item">
          <span class="nav-icon icon-c9">▣</span>
          <span v-if="!collapsed">员工管理</span>
        </router-link>
      </nav>
      <div class="sidebar-footer">
        <button class="collapse-btn" @click="collapsed = !collapsed">{{ collapsed ? '⟩' : '⟨' }}</button>
      </div>
    </aside>

    <main class="main-content">
      <header class="topbar">
        <div class="topbar-left">
          <h1>{{ $route.meta.title || '数据看板' }}</h1>
          <el-breadcrumb separator="›">
            <el-breadcrumb-item :to="{ path: '/' }">首页</el-breadcrumb-item>
            <el-breadcrumb-item>{{ $route.meta.title }}</el-breadcrumb-item>
          </el-breadcrumb>
        </div>
        <div class="topbar-right">
          <el-dropdown @command="handleCmd">
            <div class="user-badge">
              <el-avatar :size="32" icon="UserFilled" />
              <span>{{ userStore.userInfo?.username || '管理员' }}</span>
              <span class="role-tag" :class="userStore.userInfo?.role">{{ roleLabel }}</span>
            </div>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="logout">退出登录</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
      </header>
      <div class="page-content">
        <router-view v-slot="{ Component }">
          <transition name="fade" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </div>
    </main>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useUserStore } from '@/stores/user'
const userStore = useUserStore()
const collapsed = ref(false)
const isAdmin = computed(() => userStore.userInfo?.role === 'admin')
const roleLabel = computed(() => {
  const r = userStore.userInfo?.role
  if (r === 'admin') return '管理员'
  if (r === 'operator') return '运营'
  if (r === 'analyst') return '分析师'
  return '用户'
})
const handleCmd = async (cmd) => {
  if (cmd === 'logout') {
    await ElMessageBox.confirm('确认退出？', '提示', { type: 'warning' })
    userStore.logout()
  }
}
</script>

<style>
.layout { display: flex; height: 100vh; overflow: hidden; background: #0a0e17; }
.sidebar {
  background: linear-gradient(180deg, #0d1117 0%, #0f1923 100%);
  border-right: 1px solid rgba(0, 255, 255, 0.1);
  display: flex; flex-direction: column; transition: width 0.3s; overflow: hidden; flex-shrink: 0;
}
.logo {
  padding: 18px 14px; display: flex; align-items: center; gap: 10px;
  border-bottom: 1px solid rgba(0, 255, 255, 0.06); cursor: pointer;
}
.logo-icon { font-size: 22px; filter: drop-shadow(0 0 8px rgba(0, 255, 255, 0.6)); }
.logo-text { font-size: 14px; font-weight: 700; color: #00ffcc; letter-spacing: 1px; white-space: nowrap;
  text-shadow: 0 0 20px rgba(0, 255, 204, 0.4); }
.nav { flex: 1; padding: 8px 0; overflow-y: auto; }
.nav-item {
  display: flex; align-items: center; gap: 10px; padding: 8px 14px; margin: 2px 8px;
  color: rgba(255, 255, 255, 0.5); text-decoration: none; font-size: 13px;
  transition: all 0.2s; border-radius: 6px; white-space: nowrap;
}
.nav-item:hover { color: #00ffcc; background: rgba(0, 255, 204, 0.06); }
.nav-item.active { color: #0a0e17; background: linear-gradient(90deg, #00ffcc, #00d9ff); font-weight: 600;
  box-shadow: 0 0 12px rgba(0, 255, 204, 0.2); }
.nav-icon { font-size: 15px; width: 20px; text-align: center; transition: all 0.2s; }
/* 统一风格：每个菜单图标使用青绿+蓝紫渐变色系（暗黑科技风） */
.nav-icon.icon-c1 { color: #00ffcc; }     /* 数据看板 - 青绿 */
.nav-icon.icon-c2 { color: #00d9ff; }     /* 直播间 - 蓝青 */
.nav-icon.icon-c3 { color: #7dd3fc; }     /* 主播 - 天蓝 */
.nav-icon.icon-c4 { color: #c084fc; }     /* 订单 - 紫 */
.nav-icon.icon-c5 { color: #f472b6; }     /* 实时直播 - 粉 */
.nav-icon.icon-c6 { color: #fb923c; }     /* 深度分析 - 橙 */
.nav-icon.icon-c7 { color: #facc15; }     /* 系统架构 - 黄 */
.nav-icon.icon-c8 { color: #4ade80; }     /* 数据大屏 - 绿 */
.nav-icon.icon-c9 { color: #ff6b6b; }     /* 员工管理 - 珊瑚红 */
.nav-item:hover .nav-icon { transform: scale(1.15); filter: drop-shadow(0 0 6px currentColor); }
.nav-item.active .nav-icon { color: #0a0e17 !important; filter: drop-shadow(0 0 4px rgba(0,255,204,0.4)); }
.sidebar-footer { padding: 10px; border-top: 1px solid rgba(0, 255, 255, 0.06); text-align: center; }
.collapse-btn { background: none; border: 1px solid rgba(0,255,255,0.15); color: #00ffcc; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 14px; }

.main-content { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
.topbar {
  height: 56px; background: rgba(13, 17, 23, 0.8); backdrop-filter: blur(10px);
  border-bottom: 1px solid rgba(0, 255, 255, 0.1); padding: 0 20px;
  display: flex; justify-content: space-between; align-items: center; flex-shrink: 0;
}
.topbar-left { display: flex; align-items: center; gap: 16px; }
.topbar-left h1 { font-size: 16px; font-weight: 600; color: #e0e0e0; margin: 0; }
.topbar-right { display: flex; align-items: center; gap: 20px; }
.vm-status { display: flex; align-items: center; gap: 6px; font-size: 11px; color: rgba(255,255,255,0.45); }
.vm-dot { width: 5px; height: 5px; background: #00ffcc; border-radius: 50%; }
.vm-dot.pulse { animation: pulse 2s infinite; }
@keyframes pulse { 0%,100% { box-shadow: 0 0 4px #00ffcc; } 50% { box-shadow: 0 0 12px #00ffcc; } }
.vm-sep { color: rgba(0,255,255,0.1); }
.user-badge { display: flex; align-items: center; gap: 8px; cursor: pointer; color: rgba(255,255,255,0.7); font-size: 13px; }
.role-tag {
  font-size: 9px; padding: 2px 6px; border-radius: 8px; letter-spacing: 0.5px;
  background: rgba(0, 255, 204, 0.12); color: #00ffcc;
}
.role-tag.admin { background: rgba(255, 71, 87, 0.15); color: #ff4757; }
.role-tag.operator { background: rgba(255, 165, 2, 0.15); color: #ffa502; }
.role-tag.analyst { background: rgba(168, 85, 247, 0.15); color: #a855f7; }

.page-content { flex: 1; overflow-y: auto; padding: 20px; background: #0a0e17; }

.fade-enter-active, .fade-leave-active { transition: opacity 0.15s; }
.fade-enter-from, .fade-leave-to { opacity: 0; }

/* 全局覆盖 Element Plus 深色主题 */
:deep(.el-breadcrumb) { font-size: 12px; }
:deep(.el-breadcrumb__inner) { color: rgba(255,255,255,0.35) !important; }
:deep(.el-breadcrumb__item:last-child .el-breadcrumb__inner) { color: rgba(255,255,255,0.6) !important; }
:deep(.el-dropdown-menu) { background: #151a24 !important; border: 1px solid rgba(0,255,255,0.1) !important; }
:deep(.el-dropdown-menu__item) { color: rgba(255,255,255,0.7) !important; }
:deep(.el-dropdown-menu__item:hover) { background: rgba(0,255,204,0.08) !important; }
</style>
