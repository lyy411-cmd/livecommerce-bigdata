# 星播弹幕捕获器 - Chrome 扩展安装指南

本扩展用于从抖音直播间捕获实时弹幕（评论/进入/点赞/礼物/关注），推送到星播大数据分析平台。

## 工作原理

1. 扩展在页面加载时，通过 monkey-patch `WebSocket.prototype` 拦截抖音弹幕 WebSocket 的二进制帧（Protobuf 格式）
2. 由于脚本运行在真实 Chrome 的"主世界"（main world），享有与抖音原生 JS 完全相同的 TLS 指纹、cookie、签名
3. 抖音的反爬无法区分"真浏览器"和"注入"，因此不会 DEVICE_BLOCKED
4. 捕获的帧经 base64 编码后批量 POST 到后端 `/api/danmaku/ingest`
5. 后端解码并写入 Kafka + WebSocket 推送，前端立即可见

## 安装步骤

### 1. 启动后端

先确保 VM 已启动（MySQL/Kafka 在 192.168.104.100），然后：

```
双击 crawl_and_estimate.bat
```

后端会在 8080 端口启动，并自动发现带货直播间。

### 2. 加载扩展到 Chrome

1. 打开 Chrome，地址栏输入：`chrome://extensions/`
2. 右上角打开"开发者模式"开关
3. 点击"加载已解压的扩展程序"
4. 选择项目目录下的 `chrome_extension` 文件夹：
   ```
   C:\Users\MECHREVO\Desktop\星播大数据分析平台\chrome_extension
   ```
5. 安装成功后，Chrome 工具栏会出现"星播弹幕捕获器"图标

### 3. 打开抖音直播间

1. 在 Chrome 打开任意抖音带货直播间，例如：
   - 主页：https://live.douyin.com/category/100102（综合带货）
   - 或直接进入某个带货直播间：`https://live.douyin.com/<room_id>`
2. 扩展会自动注入到该页面（仅对 `live.douyin.com` 域名生效）
3. 在页面按 F12 打开控制台，应能看到：
   ```
   [星播] 弹幕捕获器已激活
   [星播] 内容脚本已加载，room_id=7661749148059831075
   ```
4. 等待几秒后，抖音页面自身会建立 WebSocket（这次是真浏览器，不会被封）
5. 扩展开始捕获弹幕，每 2 秒批量推送到后端

### 4. 查看弹幕

- 前端：http://localhost:5173（登录 admin/123456）
- 进入"直播监控"或"弹幕分析"页面
- 应能看到实时滚动的弹幕、词云、进入提示等

### 5. 扩展弹窗

点击 Chrome 工具栏的"星播弹幕捕获器"图标，可以查看：

- **捕获状态**：采集中 / 等待连接 / 已暂停
- **直播间 ID**：当前房间的抖音数字 ID
- **已捕获帧**：累计从 WebSocket 捕获的帧数
- **已推送后端**：累计成功推送到后端的帧数
- **后端连接**：后端是否可达（`http://localhost:8080`）

## 常见问题

### Q: 控制台看不到"[星播] 弹幕捕获器已激活"

- 检查扩展是否正确加载（`chrome://extensions/`）
- 检查 URL 是否确实是 `live.douyin.com`（不是 `douyin.com` 主站）
- 刷新页面

### Q: 显示"等待连接"，捕获帧数为 0

- 抖音页面需要几秒钟建立 WebSocket
- 确认后端已启动（`crawl_and_estimate.bat`）
- 尝试刷新页面

### Q: 显示"后端未连接"

- 检查 `crawl_and_estimate.bat` 是否正在运行
- 浏览器访问 http://localhost:8080/api/live/rooms 看是否能返回 JSON

### Q: 控制台看到捕获但前端没数据

- 可能是 Kafka 未连通（VM 未启动），数据仍然会通过 WebSocket 推送
- 刷新前端页面

## 卸载

在 `chrome://extensions/` 找到"星播弹幕捕获器"，点击"移除"。

## 文件结构

```
chrome_extension/
├── manifest.json     # 扩展清单（Manifest V3）
├── inject.js         # 注入主世界的脚本（monkey-patch WebSocket）
├── content.js        # 隔离世界的内容脚本（接收帧 + 推送到后端）
├── background.js     # Service Worker（状态跟踪）
├── popup.html        # 扩展弹窗
├── popup.js          # 弹窗逻辑
└── README.md         # 本文档
```
