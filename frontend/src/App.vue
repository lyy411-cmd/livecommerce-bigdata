<template>
  <router-view />
</template>

<script setup>
import { onMounted } from 'vue'

onMounted(() => {
  // 每次启动应用时清除登录状态，强制弹出登录页
  localStorage.removeItem('lc_token')
  localStorage.removeItem('lc_user')
})
</script>
