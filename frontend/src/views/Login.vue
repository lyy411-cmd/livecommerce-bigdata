<template>
  <div class="login-container">
    <div class="bg-grid"></div>
    <div class="bg-glow glow-1"></div>
    <div class="bg-glow glow-2"></div>
    <div class="bg-glow glow-3"></div>

    <div class="login-card">
      <div class="brand">
        <div class="brand-icon">
          <span class="icon-ring"></span>
          <span class="icon-core"></span>
        </div>
        <h1 class="brand-title">星播大数据分析平台</h1>
        <p class="brand-sub">STARCAST · LIVE COMMERCE · BIG DATA</p>
        <div class="brand-line"></div>
      </div>

      <el-form ref="formRef" :model="form" :rules="rules" size="large" @submit.prevent>
        <el-form-item prop="username">
          <el-input v-model="form.username" placeholder="请输入用户名" id="login-username" :prefix-icon="User" />
        </el-form-item>
        <el-form-item prop="password">
          <el-input v-model="form.password" placeholder="请输入密码" id="login-password" type="password" show-password :prefix-icon="Lock" @keyup.enter="handleLogin" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" id="btn-login" :loading="loading" class="btn-login" @click="handleLogin">
            <span v-if="!loading">登 录</span>
            <span v-else>登录中...</span>
          </el-button>
        </el-form-item>
      </el-form>

      <div class="tip">
        <div class="tip-line">演示账号：admin / 123456</div>
        <div class="tip-line muted">内部管理系统 · 账号由管理员统一分配</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { User, Lock } from '@element-plus/icons-vue'

const router = useRouter()
const userStore = useUserStore()
const formRef = ref()
const loading = ref(false)

const form = reactive({ username: '', password: '' })
const rules = {
  username: [{ required: true, message: '请输入用户名', trigger: 'blur' }],
  password: [{ required: true, message: '请输入密码', trigger: 'blur' }]
}

const handleLogin = async () => {
  await formRef.value.validate()
  loading.value = true
  try {
    await userStore.loginAction(form)
    ElMessage.success('登录成功')
    router.push('/dashboard')
  } catch (e) {
    ElMessage.error(e?.response?.data?.msg || e.message || '用户名或密码错误')
  } finally { loading.value = false }
}
</script>

<style scoped lang="scss">
.login-container {
  position: relative; height: 100vh; display: flex; align-items: center; justify-content: center;
  background: radial-gradient(ellipse at top, #0a1628 0%, #050810 50%, #02050b 100%);
  overflow: hidden;
}
.bg-grid {
  position: absolute; inset: 0;
  background-image:
    linear-gradient(rgba(0, 255, 204, 0.04) 1px, transparent 1px),
    linear-gradient(90deg, rgba(0, 255, 204, 0.04) 1px, transparent 1px);
  background-size: 40px 40px;
  mask-image: radial-gradient(ellipse at center, rgba(0,0,0,0.8) 0%, transparent 70%);
}
.bg-glow {
  position: absolute; border-radius: 50%; filter: blur(80px); opacity: 0.5; pointer-events: none;
  animation: float 8s ease-in-out infinite;
}
.glow-1 { width: 500px; height: 500px; top: -150px; left: -100px; background: radial-gradient(circle, rgba(0,255,204,0.4) 0%, transparent 70%); }
.glow-2 { width: 400px; height: 400px; bottom: -100px; right: -50px; background: radial-gradient(circle, rgba(0,217,255,0.3) 0%, transparent 70%); animation-delay: -3s; }
.glow-3 { width: 300px; height: 300px; top: 40%; left: 50%; background: radial-gradient(circle, rgba(168,85,247,0.25) 0%, transparent 70%); animation-delay: -5s; }
@keyframes float {
  0%, 100% { transform: translate(0, 0) scale(1); }
  50% { transform: translate(20px, -20px) scale(1.05); }
}
.login-card {
  position: relative; z-index: 1; width: 440px;
  padding: 40px 40px 32px;
  background: rgba(10, 18, 32, 0.75);
  border: 1px solid rgba(0, 255, 204, 0.15);
  border-radius: 12px;
  backdrop-filter: blur(20px);
}
.brand { text-align: center; margin-bottom: 32px; }
.brand-icon {
  position: relative; width: 64px; height: 64px; margin: 0 auto 16px;
  display: flex; align-items: center; justify-content: center;
}
.icon-ring {
  position: absolute; inset: 0; border: 2px solid rgba(0, 255, 204, 0.6);
  border-radius: 50%; border-top-color: transparent; border-right-color: transparent;
  animation: spin 3s linear infinite;
  box-shadow: 0 0 20px rgba(0, 255, 204, 0.4);
}
.icon-core {
  width: 28px; height: 28px; background: linear-gradient(135deg, #00ffcc, #00d9ff);
  border-radius: 50%; box-shadow: 0 0 16px rgba(0, 255, 204, 0.8);
  animation: pulse 2s ease-in-out infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
@keyframes pulse { 0%, 100% { transform: scale(1); opacity: 1; } 50% { transform: scale(1.1); opacity: 0.7; } }
.brand-title { font-size: 22px; font-weight: 600; color: #f0f8ff; margin: 0 0 8px; letter-spacing: 2px; }
.brand-sub { font-size: 10px; color: rgba(0, 255, 204, 0.5); margin: 0 0 16px; letter-spacing: 3px; font-weight: 500; }
.brand-line { width: 60px; height: 2px; margin: 0 auto; background: linear-gradient(90deg, transparent, #00ffcc, transparent); }
:deep(.el-form-item) { margin-bottom: 18px; }
:deep(.el-input__wrapper) {
  background: rgba(0, 0, 0, 0.4) !important;
  box-shadow: inset 0 0 0 1px rgba(0, 255, 204, 0.2) !important;
  border-radius: 6px; padding: 4px 12px; height: 44px;
}
:deep(.el-input__wrapper:hover) { box-shadow: inset 0 0 0 1px rgba(0, 255, 204, 0.4) !important; }
:deep(.el-input__wrapper.is-focus) { box-shadow: inset 0 0 0 1px #00ffcc, 0 0 12px rgba(0, 255, 204, 0.2) !important; }
:deep(.el-input__inner) { color: #f0f8ff !important; font-size: 14px; }
:deep(.el-input__inner::placeholder) { color: rgba(255, 255, 255, 0.35) !important; }
:deep(.el-input__prefix) { color: rgba(0, 255, 204, 0.6) !important; }
.btn-login {
  width: 100%; height: 44px; font-size: 14px; font-weight: 600; letter-spacing: 6px;
  background: linear-gradient(90deg, #00ffcc 0%, #00d9ff 100%) !important;
  border: none !important; color: #02050b !important;
  border-radius: 6px !important;
  box-shadow: 0 4px 16px rgba(0, 255, 204, 0.3) !important;
}
.btn-login:hover { transform: translateY(-1px); box-shadow: 0 6px 20px rgba(0, 255, 204, 0.5) !important; }
.btn-login:active { transform: translateY(0); }
.tip { text-align: center; margin-top: 20px; }
.tip-line { color: rgba(255, 255, 255, 0.5); font-size: 12px; line-height: 1.8; letter-spacing: 0.5px; }
.tip-line.muted { color: rgba(255, 255, 255, 0.3); font-size: 11px; }
</style>
