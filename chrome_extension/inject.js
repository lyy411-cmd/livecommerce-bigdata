// =============================================================================
// inject.js - 注入到抖音直播间页面的"主世界"脚本
// ----------------------------------------------------------------------------
// 作用：
//   1. 在页面加载前 monkey-patch WebSocket.prototype，拦截所有进入页面的
//      WebSocket 二进制帧（包括抖音弹幕 WS）。
//   2. 将二进制帧转为 base64，通过 window.postMessage 发送给
//      content script（隔离世界）。
//
// 由于此脚本运行在页面的主世界（main world），它享有与抖音原生 JS 完全
// 相同的 TLS 指纹、cookie、签名等，因此抖音的反爬无法区分"真浏览器"和"注入"。
// =============================================================================
(function () {
  'use strict';

  // 防止重复注入
  if (window.__xbCastInjected) return;
  window.__xbCastInjected = true;

  // ---------- 状态统计 ----------
  window.__xbCast = {
    capturedFrames: 0,
    capturedBytes: 0,
    connectedSockets: 0,
    lastFrameAt: 0,
    startedAt: Date.now(),
  };

  // ---------- 拦截 WebSocket ----------
  const _OrigWS = window.WebSocket;
  const _origAddEventListener = _OrigWS.prototype.addEventListener;
  const _origRemoveEventListener = _OrigWS.prototype.removeEventListener;

  // 重写 WebSocket 构造函数
  function PatchedWebSocket(url, protocols) {
    const ws = new _OrigWS(url, protocols);

    // 只拦截抖音弹幕相关的 WebSocket
    const isDanmaku =
      typeof url === 'string' &&
      (url.indexOf('webcast/im/push') !== -1 ||
        url.indexOf('webcast5-ws') !== -1);

    if (isDanmaku) {
      window.__xbCast.connectedSockets++;
      const wsId = 'ws_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);

      // 通知 content script：新的弹幕 WS 已建立
      window.postMessage({
        __xbCast: true,
        type: 'ws_open',
        wsId: wsId,
        url: url,
      }, '*');

      // 拦截 addEventListener('message', ...) —— 抖音 SDK 通常用这种方式
      const origAdd = ws.addEventListener.bind(ws);
      ws.addEventListener = function (type, listener, options) {
        if (type === 'message' && typeof listener === 'function') {
          const wrappedListener = function (event) {
            // 调用原 listener
            try { listener.call(ws, event); } catch (e) { /* ignore */ }
            // 同时捕获帧
            captureFrame(event.data, wsId, url);
          };
          return origAdd(type, wrappedListener, options);
        }
        return origAdd(type, listener, options);
      };

      // 同时监听 onmessage（防止某些代码用 onmessage 属性赋值）
      let _onmessageHandler = null;
      Object.defineProperty(ws, 'onmessage', {
        get: function () { return _onmessageHandler; },
        set: function (handler) {
          _onmessageHandler = handler;
          if (typeof handler === 'function') {
            ws.addEventListener('message', function (event) {
              try { handler.call(ws, event); } catch (e) { /* ignore */ }
              captureFrame(event.data, wsId, url);
            });
          }
        },
        configurable: true,
      });

      // 监听关闭
      ws.addEventListener('close', function (e) {
        window.__xbCast.connectedSockets = Math.max(0, window.__xbCast.connectedSockets - 1);
        window.postMessage({
          __xbCast: true,
          type: 'ws_close',
          wsId: wsId,
          code: e.code,
          reason: e.reason,
        }, '*');
      });

      ws.addEventListener('error', function () {
        window.postMessage({
          __xbCast: true,
          type: 'ws_error',
          wsId: wsId,
        }, '*');
      });
    }

    return ws;
  }

  // 复制静态属性
  PatchedWebSocket.CONNECTING = _OrigWS.CONNECTING;
  PatchedWebSocket.OPEN = _OrigWS.OPEN;
  PatchedWebSocket.CLOSING = _OrigWS.CLOSING;
  PatchedWebSocket.CLOSED = _OrigWS.CLOSED;
  PatchedWebSocket.prototype = _OrigWS.prototype;

  // 替换全局 WebSocket
  window.WebSocket = PatchedWebSocket;

  // ---------- 帧捕获逻辑 ----------
  function captureFrame(data, wsId, url) {
    try {
      if (data instanceof ArrayBuffer) {
        const bytes = new Uint8Array(data);
        window.__xbCast.capturedFrames++;
        window.__xbCast.capturedBytes += bytes.length;
        window.__xbCast.lastFrameAt = Date.now();

        // 转 base64（主世界可用 btoa）
        let binary = '';
        const chunkSize = 8192;
        for (let i = 0; i < bytes.length; i += chunkSize) {
          const chunk = bytes.subarray(i, Math.min(i + chunkSize, bytes.length));
          binary += String.fromCharCode.apply(null, chunk);
        }
        const b64 = btoa(binary);

        // 发送给 content script
        window.postMessage({
          __xbCast: true,
          type: 'ws_frame',
          wsId: wsId,
          roomId: extractRoomId(url),
          data: b64,
          size: bytes.length,
          timestamp: Date.now(),
        }, '*');
      }
    } catch (e) {
      // 静默失败
    }
  }

  // ---------- 工具函数 ----------
  function extractRoomId(url) {
    const match = url.match(/room_id[=:](\d+)/);
    return match ? match[1] : '';
  }

  // 输出日志（可在控制台查看）
  console.log('[星播] 弹幕捕获器已激活');
})();
