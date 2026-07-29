// Background service worker - manages room navigation
const CAPTURE_API = 'http://localhost:8888';
const ROOM_INTERVAL = 90000;  // Navigate every 90s

let currentTabId = null;
let roomQueue = [];
let lastNavigation = 0;

async function fetchRoomList() {
    try {
        const resp = await fetch(CAPTURE_API + '/rooms');
        if (resp.ok) {
            const data = await resp.json();
            return data.rooms || [];
        }
    } catch(e) {
        console.log('[BG] Fetch rooms error:', e.message);
    }
    return [];
}

async function navigateToRoom(room) {
    if (!currentTabId) return;
    const rid = (typeof room === 'object') ? (room.web_rid || room.id) : room;
    if (!rid) return;
    try {
        const url = 'https://live.douyin.com/' + rid;
        await chrome.tabs.update(currentTabId, { url: url });
        lastNavigation = Date.now();
        console.log('[BG] Navigated to:', url);
    } catch(e) {
        console.log('[BG] Nav error:', e.message);
    }
}

async function navigationLoop() {
    while (true) {
        await new Promise(r => setTimeout(r, 5000));
        const elapsed = Date.now() - lastNavigation;
        if (elapsed < ROOM_INTERVAL) continue;
        
        if (roomQueue.length === 0) {
            roomQueue = await fetchRoomList();
        }
        
        if (roomQueue.length > 0 && currentTabId) {
            const rid = roomQueue.shift();
            await navigateToRoom('https://live.douyin.com/' + rid);
        }
    }
}

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
    if (tab.url && tab.url.includes('live.douyin.com')) {
        currentTabId = tabId;
    }
});

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (msg.type === 'ws_frame') {
        const tabUrl = (sender.tab && sender.tab.url) || '';
        fetch(CAPTURE_API + '/ws_capture', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ frames: msg.frames, room_url: tabUrl }),
        }).catch(() => {});
        sendResponse({ok: true});
    } else if (msg.type === 'room_list') {
        roomQueue = roomQueue.concat(msg.rooms || []);
        console.log('[BG] Got', msg.rooms.length, 'rooms from content');
    }
    return true;
});

navigationLoop();
console.log('[BG] Danmaku capture background started');
