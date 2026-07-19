// =============================================================================
// content.js - 隔离世界的内容脚本 (v1.1)
// ----------------------------------------------------------------------------
// 作用：
//   1. 监听 inject.js（MAIN world）通过 window.postMessage 发出的 WebSocket 帧。
//   2. 累积 base64 帧，每 2 秒批量 POST 到后端 http://localhost:8080/api/danmaku/ingest
// inject.js 由 background.js 通过 chrome.scripting + world:MAIN 直接注入。
// =============================================================================
(function () {
  'use strict';

  if (window.__xbContentLoaded) return;
  window.__xbContentLoaded = true;

  console.log('[星播] content.js 已加载 (隔离世界), url=' + location.href.slice(0, 60));

  // ---------- 配置 ----------
  const FLUSH_INTERVAL_MS = 2000;
  const MAX_BATCH_FRAMES = 50;

  // ---------- 状态 ----------
  const state = {
    enabled: true,
    frames: [],
    currentRoomId: '',
    connected: false,
    totalCaptured: 0,
    totalPosted: 0,
    backendReachable: false,
    lastPostAt: 0,
  };

  // 从 URL 提取 room_id
  function detectRoomIdFromUrl() {
    const m = location.pathname.match(/\/(\d{10,})/);
    return m ? m[1] : '';
  }
  state.currentRoomId = detectRoomIdFromUrl();

  // ---------- 接收来自 inject.js (MAIN world) 的消息 ----------
  window.addEventListener('message', (event) => {
    if (event.source !== window) return;
    const msg = event.data;
    if (!msg || !msg.__xbCast) return;

    if (msg.type === 'ws_frame') {
      const rid = msg.roomId || state.currentRoomId;
      if (!rid) return;
      state.frames.push({
        roomId: rid,
        data: msg.data,
        size: msg.size,
        timestamp: msg.timestamp,
      });
      state.totalCaptured++;
      state.connected = true;

      if (state.frames.length >= MAX_BATCH_FRAMES) {
        flushFrames();
      }
    } else if (msg.type === 'ws_open') {
      state.connected = true;
      console.log('[星播] 捕获到弹幕 WebSocket: ' + (msg.url || '').slice(0, 80) + '...');
    } else if (msg.type === 'ws_close') {
      state.connected = false;
      console.log('[星播] 弹幕 WebSocket 关闭: code=' + msg.code);
    }
  });

  // ---------- 批量推送到后端（通过 background service worker 中转，绕过 PNA） ----------
  function flushFrames() {
    if (!state.enabled || state.frames.length === 0) return;
    const batch = state.frames.splice(0, MAX_BATCH_FRAMES);
    const payload = {
      roomId: state.currentRoomId,
      platform: 'douyin',
      frames: batch,
      capturedAt: Date.now(),
    };

    chrome.runtime.sendMessage(
      { type: 'DANMAKU_INGEST', payload: payload },
      function (response) {
        if (chrome.runtime.lastError) {
          // Service worker 不可达，放回队列
          state.frames = batch.concat(state.frames).slice(-200);
          state.backendReachable = false;
          return;
        }
        if (response && response.ok) {
          state.totalPosted += batch.length;
          state.backendReachable = true;
          state.lastPostAt = Date.now();
        } else {
          state.frames = batch.concat(state.frames).slice(-200);
          state.backendReachable = false;
        }
      }
    );
  }

  setInterval(flushFrames, FLUSH_INTERVAL_MS);

  // 响应 service worker 查询
  chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.type === 'GET_STATUS') {
      sendResponse({
        enabled: state.enabled,
        connected: state.connected,
        currentRoomId: state.currentRoomId,
        pendingFrames: state.frames.length,
        totalCaptured: state.totalCaptured,
        totalPosted: state.totalPosted,
        backendReachable: state.backendReachable,
        url: location.href,
      });
      return true;
    } else if (request.type === 'TOGGLE') {
      state.enabled = !!request.enabled;
      sendResponse({ enabled: state.enabled });
      return true;
    }
  });
})();
