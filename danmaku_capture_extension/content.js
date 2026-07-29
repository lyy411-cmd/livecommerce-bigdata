// Content Script - Injects WS hook and relays data
(function() {
    function injectScript() {
        const script = document.createElement('script');
        script.src = chrome.runtime.getURL('inject.js');
        script.onload = function() { this.remove(); };
        (document.head || document.documentElement).appendChild(script);
    }
    injectScript();
    
    // Relay WS frames from page to background
    window.addEventListener('message', function(event) {
        if (event.source !== window) return;
        if (!event.data || event.data.type !== '__ws_capture__') return;
        if (event.data.frames) {
            chrome.runtime.sendMessage({
                type: 'ws_frame',
                frames: event.data.frames,
            }).catch(() => {});
        }
    });
    
    // Extract room links from directory page
    if (location.pathname === '/' || location.pathname === '') {
        setTimeout(function() {
            const links = [];
            document.querySelectorAll('a[href*="live.douyin.com/"]').forEach(function(a) {
                const match = a.href.match(/live\.douyin\.com\/(\d{6,})/);
                if (match && !links.includes(match[1])) {
                    links.push(match[1]);
                }
            });
            if (links.length > 0) {
                chrome.runtime.sendMessage({ type: 'room_list', rooms: links }).catch(() => {});
                console.log('[DanmakuCapture] Found', links.length, 'rooms');
            }
        }, 10000);
    }
    
    console.log('[DanmakuCapture] Content script active on', location.href);
})();
