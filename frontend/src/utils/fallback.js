/**
 * Graceful degradation: sample fallback data when backend is unavailable.
 * Inspired by OfflineSampleDataService pattern from bigdata-platform reference project.
 *
 * Usage: import { fallback } from '@/utils/fallback'
 *        const kpi = await getDashboardKpi().catch(() => fallback.kpi())
 */

// KPI overview
const kpi = () => ({
  code: 0,
  data: {
    totalGmv: 128560000,
    totalRooms: 10662,
    totalAnchors: 3850,
    totalViewers: 28900000,
    avgConversion: 3.8,
    totalOrders: 487200
  }
})

// Category distribution (pie chart)
const categoryDistribution = () => ({
  code: 0,
  data: [
    { name: '美妆', value: 1850 },
    { name: '服饰', value: 2340 },
    { name: '食品', value: 1620 },
    { name: '数码', value: 980 },
    { name: '家居', value: 760 },
    { name: '母婴', value: 520 },
    { name: '珠宝', value: 380 },
    { name: '运动', value: 290 }
  ]
})

// Anchor rank (bar chart)
const anchorRank = () => ({
  code: 0,
  data: [
    { name: '疯狂小杨哥', totalGmv: 28500000, avgConversion: 5.2 },
    { name: '广东夫妇', totalGmv: 19800000, avgConversion: 4.1 },
    { name: '交个朋友', totalGmv: 15600000, avgConversion: 3.8 },
    { name: '董先生', totalGmv: 12400000, avgConversion: 4.5 },
    { name: '琦儿Leo', totalGmv: 9800000, avgConversion: 3.2 },
    { name: '与辉同行', totalGmv: 8500000, avgConversion: 6.1 },
    { name: '多余和毛毛姐', totalGmv: 7200000, avgConversion: 2.9 },
    { name: '朱瓜瓜', totalGmv: 5600000, avgConversion: 3.5 },
    { name: '大狼狗郑建鹏', totalGmv: 4800000, avgConversion: 4.0 },
    { name: '衣哥', totalGmv: 3900000, avgConversion: 3.7 }
  ]
})

// Category rank (pie chart)
const categoryRank = () => ({
  code: 0,
  data: [
    { name: '美妆护肤', value: 3200 },
    { name: '女装服饰', value: 2800 },
    { name: '零食特产', value: 1950 },
    { name: '数码家电', value: 1100 },
    { name: '家居日用', value: 860 },
    { name: '母婴用品', value: 580 },
    { name: '珠宝饰品', value: 420 },
    { name: '运动户外', value: 310 }
  ]
})

// GMV trend (line chart - last 30 days)
const gmvTrend = () => {
  const data = []
  const now = new Date()
  for (let i = 29; i >= 0; i--) {
    const d = new Date(now)
    d.setDate(d.getDate() - i)
    const dateStr = d.toISOString().slice(0, 10)
    const base = 3500000 + Math.sin(i * 0.3) * 800000
    const weekend = (d.getDay() === 0 || d.getDay() === 6) ? 1200000 : 0
    data.push({ date: dateStr, value: Math.round(base + weekend + Math.random() * 500000) })
  }
  return { code: 0, data }
}

// Activities feed
const activities = () => ({
  code: 0,
  data: [
    { icon: 'order', text: '[示例] 用户***8821 下单 花西子蜜粉 ¥169', time: '刚刚', color: '#00ffcc' },
    { icon: 'live', text: '[示例] 疯狂小杨哥 开始直播 在线 12.5万', time: '2分钟前', color: '#00d9ff' },
    { icon: 'star', text: '[示例] 广东夫妇 GMV突破 1000万', time: '5分钟前', color: '#a855f7' },
    { icon: 'order', text: '[示例] 用户***3356 下单 三只松鼠坚果 ¥89', time: '8分钟前', color: '#00ffcc' },
    { icon: 'platform', text: '[示例] 系统采集到新直播间 3 个', time: '10分钟前', color: '#ffa502' }
  ]
})

// Geo distribution (BigScreen)
const geoDistribution = () => ({
  code: 0,
  data: [
    { name: '广东', value: 18.5 },
    { name: '浙江', value: 15.2 },
    { name: '江苏', value: 12.8 },
    { name: '上海', value: 9.6 },
    { name: '北京', value: 8.4 },
    { name: '四川', value: 6.3 },
    { name: '山东', value: 5.1 },
    { name: '湖北', value: 4.2 },
    { name: '福建', value: 3.8 },
    { name: '河南', value: 3.1 }
  ]
})

// Realtime data (BigScreen KPIs)
const realtimeData = () => ({
  code: 0,
  data: {
    currentViewers: 1250000,
    currentOrders: 8520,
    currentGmv: 4560000,
    onlineAnchors: 156
  }
})

// Live rooms (BigScreen table + Realtime page)
const liveRooms = () => ({
  code: 0,
  data: [
    { roomId: 'demo1', roomName: '花西子官方旗舰店', anchorName: '花西子主播', category: '美妆', status: 'live', viewerCount: 85000, gmv: 1250000, totalOrders: 3200, liveUrl: '' },
    { roomId: 'demo2', roomName: '三只松鼠零食铺', anchorName: '松鼠小妹', category: '食品', status: 'live', viewerCount: 62000, gmv: 890000, totalOrders: 5100, liveUrl: '' },
    { roomId: 'demo3', roomName: 'URBAN REVIVO', anchorName: 'UR穿搭师', category: '服饰', status: 'live', viewerCount: 45000, gmv: 680000, totalOrders: 2100, liveUrl: '' },
    { roomId: 'demo4', roomName: '华为官方旗舰店', anchorName: '华为小助手', category: '数码', status: 'live', viewerCount: 38000, gmv: 2100000, totalOrders: 850, liveUrl: '' },
    { roomId: 'demo5', roomName: '珀莱雅官方直播', anchorName: '珀莱雅达人', category: '美妆', status: 'live', viewerCount: 28000, gmv: 560000, totalOrders: 1800, liveUrl: '' }
  ]
})

// Hotwords
const hotwords = () => ({
  code: 0,
  data: [
    { word: '好用', count: 320 }, { word: '下单了', count: 280 },
    { word: '多少钱', count: 210 }, { word: '链接', count: 190 },
    { word: '优惠', count: 165 }, { word: '包邮', count: 140 },
    { word: '回购', count: 120 }, { word: '推荐', count: 95 },
    { word: '划算', count: 80 }, { word: '质量', count: 72 }
  ]
})

export const fallback = {
  kpi,
  categoryDistribution,
  anchorRank,
  categoryRank,
  gmvTrend,
  activities,
  geoDistribution,
  realtimeData,
  liveRooms,
  hotwords
}
