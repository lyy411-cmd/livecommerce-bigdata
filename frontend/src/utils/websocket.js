/**
 * 弹幕 WebSocket 客户端工具
 */
export class DanmakuSocket {
  constructor(roomId, options = {}) {
    this.roomId = roomId
    this.wsUrl = options.wsUrl || `ws://localhost:8765/danmaku/${roomId}`
    this.onMessage = options.onMessage || (() => {})
    this.onConnect = options.onConnect || (() => {})
    this.onDisconnect = options.onDisconnect || (() => {})
    this.ws = null
    this.reconnectDelay = 1000
    this.maxReconnectDelay = 10000
    this._reconnectTimer = null
    this._pingTimer = null
    this._closed = false
  }

  connect() {
    this._closed = false
    try {
      this.ws = new WebSocket(this.wsUrl)
      this.ws.onopen = () => {
        console.log(`[DanmakuSocket] Connected to ${this.wsUrl}`)
        this.reconnectDelay = 1000
        this.onConnect()
        // Start ping
        this._pingTimer = setInterval(() => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({ type: 'ping' }))
          }
        }, 25000)
      }
      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          if (data.type !== 'connected' && data.type !== 'pong') {
            this.onMessage(data)
          }
        } catch {}
      }
      this.ws.onclose = () => {
        this._cleanup()
        if (!this._closed) {
          this.onDisconnect()
          this._scheduleReconnect()
        }
      }
      this.ws.onerror = () => this.ws.close()
    } catch {
      this._scheduleReconnect()
    }
  }

  _scheduleReconnect() {
    if (this._closed) return
    this._reconnectTimer = setTimeout(() => {
      this.reconnectDelay = Math.min(this.reconnectDelay * 1.5, this.maxReconnectDelay)
      this.connect()
    }, this.reconnectDelay)
  }

  _cleanup() {
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null }
  }

  close() {
    this._closed = true
    this._cleanup()
    if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null }
    if (this.ws) { this.ws.close(); this.ws = null }
  }
}
