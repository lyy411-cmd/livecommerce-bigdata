import { defineStore } from 'pinia'
import { login, getMe, logout as logoutApi } from '@/api'
import router from '@/router'

export const useUserStore = defineStore('user', {
  state: () => ({
    token: localStorage.getItem('lc_token') || '',
    userInfo: JSON.parse(localStorage.getItem('lc_user') || 'null')
  }),
  actions: {
    async loginAction(form) {
      const res = await login(form)
      this.token = res.data.token
      this.userInfo = res.data.user
      localStorage.setItem('lc_token', this.token)
      localStorage.setItem('lc_user', JSON.stringify(this.userInfo))
      return res.data
    },
    async fetchUserInfo() {
      const res = await getMe()
      this.userInfo = res.data
      localStorage.setItem('lc_user', JSON.stringify(this.userInfo))
    },
    async logout() {
      await logoutApi()
      this.token = ''
      this.userInfo = null
      localStorage.removeItem('lc_token')
      localStorage.removeItem('lc_user')
      router.push('/login')
    }
  }
})
