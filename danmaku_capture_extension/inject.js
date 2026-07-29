// Danmaku WS Capture - Page-level WebSocket hook
(function() {
    'use strict';
    var OrigWS = window.WebSocket;
    if (!OrigWS) return;
    
    var _buffer = [];
    
    function flush() {
        if (_buffer.length === 0) return;
        var batch = _buffer.splice(0);
        window.postMessage({ type: '__ws_capture__', frames: batch }, '*');
    }
    setInterval(flush, 2000);
    
    function capture(url, data) {
        if (!(data instanceof ArrayBuffer)) return;
        var bytes = new Uint8Array(data);
        if (bytes.length < 4) return;
        var bin = '';
        for (var i = 0; i < bytes.length; i++) {
            bin += String.fromCharCode(bytes[i]);
        }
        _buffer.push({ url: url.substring(0, 200), data: btoa(bin), size: bytes.length, ts: Date.now() });
        if (_buffer.length >= 20) flush();
    }
    
    window.WebSocket = new Proxy(OrigWS, {
        construct: function(Target, args) {
            var ws = new Target(args[0], args[1]);
            var url = String(args[0] || '');
            if (url.indexOf('webcast5-ws-web') >= 0 || url.indexOf('im/push') >= 0) {
                console.log('[DanmakuCapture] Hooking WS:', url.substring(0, 120));
                ws.addEventListener('message', function(e) { capture(url, e.data); }, true);
                ws.addEventListener('close', function(e) { console.log('[DanmakuCapture] WS closed:', e.code); flush(); });
                window.postMessage({ type: '__ws_capture__', event: 'ws_created', url: url.substring(0, 200) }, '*');
            }
            return ws;
        }
    });
    
    ['CONNECTING','OPEN','CLOSING','CLOSED'].forEach(function(k) {
        try { window.WebSocket[k] = OrigWS[k]; } catch(e) {}
    });
    
    window.__danmaku_capture__ = true;
    console.log('[DanmakuCapture] WebSocket hook installed');
})();
