"""
WebSocket Capture Server for Chrome Extension danmaku relay.
Receives base64-encoded protobuf frames from the Chrome extension,
decodes them, and feeds into the danmaku pipeline.
"""
import json, base64, logging, threading, time, sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
logger = logging.getLogger("ws_capture")

_on_danmaku_callback = None
_stats = {"frames_received": 0, "messages_decoded": 0, "errors": 0}

def set_danmaku_callback(callback):
    global _on_danmaku_callback
    _on_danmaku_callback = callback

def get_stats():
    return dict(_stats)

class WSCaptureHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/ws_capture":
            self._handle_capture()
        elif path == "/status":
            self._handle_status()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_capture(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                self.send_response(200)
                self.end_headers()
                return
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.end_headers()
                return
            frames = data if isinstance(data, list) else [data]
            _stats["frames_received"] += len(frames)
            decoded = 0
            for frame in frames:
                if self._process_frame(frame):
                    decoded += 1
            _stats["messages_decoded"] += decoded
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._cors()
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "frames": len(frames), "decoded": decoded}).encode())
        except Exception as e:
            _stats["errors"] += 1
            logger.error(f"Capture error: {e}")
            try:
                self.send_response(500)
                self.end_headers()
            except:
                pass

    def _process_frame(self, frame):
        if not frame or not isinstance(frame, dict):
            return False
        b64 = frame.get("data")
        if not b64:
            return False
        try:
            raw = base64.b64decode(b64)
        except:
            return False
        if len(raw) < 4:
            return False
        try:
            # Import decoder
            sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from data_pipeline.proto.douyin_decoder import decode_websocket_frame
            _, messages, _, need_ack, iext = decode_websocket_frame(raw)
            if not messages:
                return False
            url = frame.get("url", "")
            room_id = self._extract_room_id(url)
            any_decoded = False
            for msg in messages:
                if _on_danmaku_callback and room_id:
                    try:
                        _on_danmaku_callback(msg, room_id, "douyin")
                        any_decoded = True
                    except Exception as cb_err:
                        pass
            return any_decoded
        except ImportError:
            return False
        except Exception:
            return False

    def _extract_room_id(self, url):
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            rids = params.get("room_id", [])
            return rids[0] if rids else None
        except:
            return None

    def _handle_status(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self._cors()
        self.end_headers()
        self.wfile.write(json.dumps({"status": "running", "callback_set": _on_danmaku_callback is not None, **_stats}).encode())

class WSCaptureServer:
    def __init__(self, port=8888):
        self.port = port
        self._server = None
        self._thread = None

    def set_callback(self, callback):
        set_danmaku_callback(callback)

    def start(self):
        self._server = HTTPServer(("0.0.0.0", self.port), WSCaptureHandler)
        self._server.timeout = 1
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        print(f"  [OK] WS Capture Server started on http://localhost:{self.port}/ws_capture")

    def _run(self):
        while True:
            self._server.handle_request()

    def stop(self):
        if self._server:
            self._server.server_close()

    def get_stats(self):
        return get_stats()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    def test_cb(msg, rid, plat):
        print(f"[Danmaku] room={rid} msg={msg.get("content", "?")[:50]}")
    server = WSCaptureServer(port=8888)
    server.set_callback(test_cb)
    server.start()
    print("WS Capture Server running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()
