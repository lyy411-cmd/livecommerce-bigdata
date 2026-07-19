// =============================================================================
// background.js - 扩展的 Service Worker (v1.1)
// =============================================================================

console.log('[星播] background service worker 已启动 (v1.1)');

const lastStatus = new Map();
const injectedTabs = new Set(); // 已注入的 tabId 集合

// ---------- 主动注入 inject.js 到 MAIN world ----------
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  const url = tab.url || '';
  if (!url.includes('live.douyin.com/')) return;

  // 在页面 loading 阶段注入（早于抖音 JS 建立 WebSocket）
  if (changeInfo.status === 'loading') {
    // 新导航时清除旧的注入标记
    injectedTabs.delete(tabId);
  }

  if (changeInfo.status === 'loading' && !injectedTabs.has(tabId)) {
    injectedTabs.add(tabId);
    console.log('[星播] 页面 loading, 注入脚本, tab=' + tabId);

    // 先注入 inject.js 到 MAIN world（monkey-patch WebSocket）
    chrome.scripting.executeScript({
      target: { tabId: tabId },
      world: 'MAIN',
      files: ['inject.js'],
    }, function (results) {
      if (chrome.runtime.lastError) {
        console.error('[星播] MAIN world 注入失败:', chrome.runtime.lastError.message);
        injectedTabs.delete(tabId);
      } else {
        console.log('[星播] inject.js 注入成功 (MAIN world)');
      }
    });

    // 再注入 content.js 到 ISOLATED world（消息监听 + 后端推送）
    chrome.scripting.executeScript({
      target: { tabId: tabId },
      files: ['content.js'],
    }, function (results) {
      if (chrome.runtime.lastError) {
        console.error('[星播] content.js 注入失败:', chrome.runtime.lastError.message);
      } else {
        console.log('[星播] content.js 注入成功 (ISOLATED world)');
      }
    });
  }
});

// 监听来自 content script 的消息
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message && message.type === 'DANMAKU_INGEST') {
    // content.js 通过 service worker 中转请求到 localhost（绕过 PNA）
    fetch('http://localhost:8080/api/danmaku/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(message.payload),
    }).then((resp) => {
      if (resp.ok) {
        sendResponse({ ok: true });
      } else {
        sendResponse({ ok: false, status: resp.status });
      }
    }).catch((err) => {
      console.error('[星播] 后端请求失败:', err.message);
      sendResponse({ ok: false, error: err.message });
    });
    return true; // 保持消息通道打开以支持异步 sendResponse

  } else if (message && message.type === 'STATUS_REPORT' && sender.tab) {
    lastStatus.set(sender.tab.id, {
      ...message.status,
      tabId: sender.tab.id,
      tabUrl: sender.tab.url,
      updatedAt: Date.now(),
    });
  } else if (message && message.type === 'QUERY_ACTIVE') {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
      const tab = tabs && tabs[0];
      if (tab && lastStatus.has(tab.id)) {
        sendResponse(lastStatus.get(tab.id));
      } else if (tab) {
        chrome.tabs.sendMessage(tab.id, { type: 'GET_STATUS' }, (resp) => {
          if (chrome.runtime.lastError || !resp) {
            sendResponse(null);
          } else {
            const entry = { ...resp, tabId: tab.id, tabUrl: tab.url, updatedAt: Date.now() };
            lastStatus.set(tab.id, entry);
            sendResponse(entry);
          }
        });
      } else {
        sendResponse(null);
      }
    });
    return true;
  }
  return false;
});

chrome.tabs.onRemoved.addListener((tabId) => {
  lastStatus.delete(tabId);
  injectedTabs.delete(tabId);
});

chrome.runtime.onInstalled.addListener(() => {
  console.log('[星播] 扩展已安装/更新 (v1.1)');
});
