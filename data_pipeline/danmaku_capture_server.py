#!/usr/bin/env python3
"""
danmaku_capture_server.py - 独立弹幕捕获服务器

接收 Chrome 扩展推送的 base64 protobuf 帧，解码后写入 MySQL 并推送到前端 WebSocket。

用法:
    python danmaku_capture_server.py

端口:
    HTTP  : 8888 (接收扩展数据)
    WS    : 8765 (推送到前端，可选)
    MySQL : 192.168.104.100:3306
"""
import sys, os, json, time, base64, threading, traceback
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# =============================================================================
# 配置
# =============================================================================
HTTP_PORT = 8888
WS_PORT = 8765
MYSQL_HOST = "192.168.104.100"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "123456"
MYSQL_DB = "livecommerce_db"
FLUSH_INTERVAL = 8  # 秒
BUFFER_MAX = 5000

# 项目路径 (用于导入 protobuf 解码器)
PROJECT_DIR = r"C:\Users\MECHREVO\Desktop\星播大数据分析平台"

# =============================================================================
# 全局状态
# =============================================================================
_last_room_id = ""   # Remember last valid room_id
_dm_buffer = []       # [(room_id, mapped_dict), ...]
_dm_buffer_lock = threading.Lock()
_stats = {
    "frames_received": 0,
    "messages_decoded": 0,
    "messages_flushed": 0,
    "mysql_writes": 0,
    "mysql_errors": 0,
    "ws_pushes": 0,
    "ws_errors": 0,
    "started_at": time.time(),
    "last_frame_at": 0,
    "last_flush_at": 0,
}

# =============================================================================
# Protobuf 解码器
# =============================================================================
_decode_fn = None

def _ensure_decoder():
    global _decode_fn
    if _decode_fn is not None:
        return True
    try:
        sys.path.insert(0, PROJECT_DIR)
        from data_pipeline.proto.douyin_decoder import decode_websocket_frame
        _decode_fn = decode_websocket_frame
        print("[Decoder] Protobuf decoder loaded OK")
        return True
    except Exception as e:
        print(f"[Decoder] ERROR loading decoder: {e}")
        return False

# =============================================================================
# MySQL 连接
# =============================================================================
def _get_db():
    import pymysql
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS,
        database=MYSQL_DB, charset="utf8mb4",
        connect_timeout=5, read_timeout=10, write_timeout=10,
        autocommit=True,
    )

def _init_db():
    """Ensure rt_danmaku table exists."""
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rt_danmaku (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                event_id VARCHAR(64),
                room_id VARCHAR(50),
                platform VARCHAR(30),
                user_id VARCHAR(50),
                user_name VARCHAR(100),
                content TEXT,
                danmaku_type VARCHAR(20) DEFAULT 'comment',
                event_time DATETIME(3),
                INDEX idx_room_time (room_id, event_time),
                INDEX idx_event_id (event_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.close()
        print("[MySQL] rt_danmaku table ready")
    except Exception as e:
        print(f"[MySQL] Table init error: {e}")

# =============================================================================
# WebSocket 推送 (异步)
# =============================================================================
_ws_loop = None
_ws_pusher = None

def _start_ws_server():
    """Start WebSocket server for frontend push."""
    global _ws_loop, _ws_pusher
    try:
        import asyncio
        import websockets

        _ws_loop = asyncio.new_event_loop()
        clients = {}  # room_id -> set
        all_clients = set()

        async def handler(websocket):
            path = websocket.path if hasattr(websocket, 'path') else ''
            room_id = ''
            if '/danmaku/' in path:
                room_id = path.split('/danmaku/')[-1].strip('/')
            elif path == '/danmaku/all':
                room_id = 'all'

            if room_id == 'all':
                all_clients.add(websocket)
            elif room_id:
                clients.setdefault(room_id, set()).add(websocket)
            else:
                all_clients.add(websocket)
                room_id = 'all'

            try:
                async for msg in websocket:
                    pass  # Keep connection alive
            except:
                pass
            finally:
                if room_id == 'all':
                    all_clients.discard(websocket)
                else:
                    clients.get(room_id, set()).discard(websocket)

        async def _run_server():
            async with websockets.serve(handler, '0.0.0.0', WS_PORT):
                print(f"[WS] WebSocket server on port {WS_PORT}")
                await asyncio.Future()  # Run forever

        def _push_danmaku_sync(room_id, data):
            """Push danmaku from sync context."""
            if _ws_loop is None or _ws_loop.is_closed():
                return
            import json as _json
            payload = _json.dumps(data, ensure_ascii=False)
            targets = set()
            # Normalize room_id
            rid = room_id.replace('CRAWL_DOUYIN_', '')
            targets.update(clients.get(rid, set()))
            targets.update(all_clients)
            if not targets:
                return
            async def _do_push():
                dead = set()
                for ws in targets:
                    try:
                        await ws.send(payload)
                    except:
                        dead.add(ws)
                for ws in dead:
                    all_clients.discard(ws)
                    for s in clients.values():
                        s.discard(ws)
            try:
                asyncio.run_coroutine_threadsafe(_do_push(), _ws_loop)
            except:
                pass

        _ws_pusher = _push_danmaku_sync

        t = threading.Thread(target=_ws_loop.run_until_complete, args=(_run_server(),), daemon=True)
        t.start()
        time.sleep(1)
        return True
    except ImportError:
        print("[WS] websockets not installed, WS push disabled")
        return False
    except Exception as e:
        print(f"[WS] Start error: {e}")
        return False

# =============================================================================
# 帧解码 + 消息映射
# =============================================================================
def _map_message(msg, room_id):
    """Map decoded protobuf message to standard danmaku dict."""
    msg_type = msg.get("type", "unknown")
    user = msg.get("user", {})
    user_name = ""
    user_id = ""
    if isinstance(user, dict):
        user_name = user.get("nickname", "") or user.get("name", "")
        user_id = str(user.get("id", ""))
    content = msg.get("content", "")

    if msg_type == "gift":
        gift = msg.get("gift", {})
        gift_name = gift.get("name", "礼物") if isinstance(gift, dict) else "礼物"
        repeat = msg.get("repeat_count", 1) or msg.get("count", 1)
        content = f"送出 {gift_name} x{repeat}"
    elif msg_type in ("enter", "member"):
        content = "进入直播间"
    elif msg_type == "like":
        count = msg.get("count", 1)
        content = f"点赞了 x{count}"
    elif msg_type in ("follow", "social"):
        content = "关注了主播"
    elif msg_type == "stats":
        return None  # Skip stats messages
    elif msg_type == "unknown":
        return None

    return {
        "user_id": user_id,
        "user_name": user_name or "匿名",
        "content": content or "",
        "danmaku_type": msg_type,
        "room_id": room_id,
        "timestamp": msg.get("timestamp", time.time() * 1000),
    }

# =============================================================================
# MySQL 批量写入
# =============================================================================
def _flush_to_mysql():
    """Flush buffered danmaku to MySQL."""
    with _dm_buffer_lock:
        if not _dm_buffer:
            return
        batch = _dm_buffer[:]
        _dm_buffer.clear()

    if not batch:
        return

    try:
        conn = _get_db()
        cur = conn.cursor()
        sql = (
            "INSERT INTO rt_danmaku (event_id, room_id, platform, user_id, user_name, "
            "content, danmaku_type, event_time) VALUES "
            "(UUID(), %s, %s, %s, %s, %s, %s, NOW(3))"
        )
        rows = []
        for room_id, dm in batch:
            rows.append((
                room_id,
                "douyin",
                dm.get("user_id", ""),
                dm.get("user_name", ""),
                dm.get("content", ""),
                dm.get("danmaku_type", "comment"),
            ))
        cur.executemany(sql, rows)

        # Update rt_room_stats total_danmaku counter
        room_counts = {}
        for room_id, dm in batch:
            room_counts[room_id] = room_counts.get(room_id, 0) + 1
        for rid, cnt in room_counts.items():
            try:
                cur.execute(
                    "UPDATE rt_room_stats SET total_danmaku = COALESCE(total_danmaku,0) + %s "
                    "WHERE room_id = %s", (cnt, rid)
                )
            except:
                pass

        conn.close()
        _stats["mysql_writes"] += len(batch)
        _stats["messages_flushed"] += len(batch)
        _stats["last_flush_at"] = time.time()
        print(f"[MySQL] Flushed {len(batch)} danmaku ({len(room_counts)} rooms)")
    except Exception as e:
        _stats["mysql_errors"] += 1
        print(f"[MySQL] Flush error: {e}")
        # Put failed items back (up to limit)
        with _dm_buffer_lock:
            _dm_buffer.extend(batch[:BUFFER_MAX - len(_dm_buffer)])

def _flush_loop():
    """Periodic flush thread."""
    while True:
        time.sleep(FLUSH_INTERVAL)
        _flush_to_mysql()

# =============================================================================
# HTTP Handler
# =============================================================================
class CaptureHandler(BaseHTTPRequestHandler):

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path == "/status":
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            uptime = time.time() - _stats["started_at"]
            data = {**_stats, "uptime_seconds": int(uptime), "buffer_size": len(_dm_buffer),
                    "last_room_id": _last_room_id}
            self.wfile.write(json.dumps(data, default=str).encode())
        elif self.path == "/health":
            self.send_response(200)
            self._cors()
            self.end_headers()
            self.wfile.write(b"OK")
        elif self.path == "/rooms" or self.path.startswith("/rooms?"):
            self._handle_get_rooms()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_get_rooms(self):
        """Return LIVE commerce rooms for extension auto-navigation."""
        try:
            conn = _get_db()
            cur = conn.cursor()
            cur.execute(
                "SELECT room_id_external, room_name, anchor_name, viewer_count "
                "FROM live_room WHERE has_shopping_cart=1 AND status='live' "
                "ORDER BY viewer_count DESC LIMIT 200"
            )
            rows = cur.fetchall()
            conn.close()
            rooms = []
            for r in rows:
                rid = r[0] or ""
                # Strip CRAWL_DOUYIN_ prefix if present
                if rid.startswith("CRAWL_DOUYIN_"):
                    rid = rid.replace("CRAWL_DOUYIN_", "")
                if rid:
                    rooms.append(rid)
            self._send_json(200, {"rooms": rooms, "count": len(rooms)})
        except Exception as e:
            self._send_json(500, {"rooms": [], "count": 0, "error": str(e)})

    def do_POST(self):
        if self.path == "/api/danmaku/ingest" or self.path == "/ws_capture":
            self._handle_ingest()
        elif self.path == "/api/danmaku/text":
            self._handle_text_danmaku()
        elif self.path == "/ping":
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_ingest(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            data = json.loads(body)
        except Exception as e:
            self._send_json(400, {"code": -1, "msg": f"Parse error: {e}"})
            return

        # Extract frames - support both formats
        frames = data.get("frames", [])
        body_room_id = data.get("roomId", "")
        platform = data.get("platform", "douyin")
        
        # Also support room_url format (from danmaku_ext)
        room_url = data.get("room_url", "")
        if not body_room_id and room_url:
            import re
            m = re.search(r'live\.douyin\.com/(\d+)', room_url)
            if m:
                body_room_id = m.group(1)

        global _last_room_id
        # Use last known room_id as fallback if tab URL didn't have one
        if not body_room_id and _last_room_id:
            body_room_id = _last_room_id

        if not _ensure_decoder():
            self._send_json(500, {"code": -1, "msg": "Decoder unavailable"})
            return

        processed = 0
        failed = 0
        type_counts = {"chat": 0, "gift": 0, "member": 0, "like": 0, "follow": 0}

        for frame in frames:
            if isinstance(frame, dict):
                b64_data = frame.get("data", "")
                frame_room = frame.get("roomId", "") or frame.get("room_id", "")
                # Try extracting from frame.url (WS URL or page URL)
                if not frame_room:
                    frame_url = frame.get("url", "")
                    if frame_url:
                        import re as _re
                        # Match room_id=\d+ in WS URL or live.douyin.com/\d+ in page URL
                        _m = _re.search(r'room_id[=/](\d+)', frame_url) or _re.search(r'live\.douyin\.com/(\d+)', frame_url)
                        if _m:
                            frame_room = _m.group(1)
                if not frame_room:
                    frame_room = body_room_id
                # Update last known room_id
                if frame_room:
                    _last_room_id = frame_room
            elif isinstance(frame, str):
                b64_data = frame
                frame_room = body_room_id
            else:
                failed += 1
                continue

            if not b64_data:
                failed += 1
                continue

            try:
                raw = base64.b64decode(b64_data)
            except:
                failed += 1
                continue

            _stats["frames_received"] += 1
            _stats["last_frame_at"] = time.time()

            try:
                result = _decode_fn(raw)
                # result can be tuple (log_id, messages, cursor, need_ack, internal_ext)
                # or just list of messages
                if isinstance(result, tuple) and len(result) >= 2:
                    messages = result[1]
                elif isinstance(result, list):
                    messages = result
                else:
                    failed += 1
                    continue
            except Exception as e:
                failed += 1
                continue

            for msg in messages:
                mapped = _map_message(msg, frame_room)
                if mapped is None:
                    continue

                _stats["messages_decoded"] += 1
                processed += 1
                mt = mapped.get("danmaku_type", "")
                if mt in type_counts:
                    type_counts[mt] += 1

                # Buffer for MySQL
                with _dm_buffer_lock:
                    if len(_dm_buffer) < BUFFER_MAX:
                        _dm_buffer.append((frame_room, mapped))

                # Push to WebSocket immediately
                if _ws_pusher:
                    try:
                        _ws_pusher(frame_room, mapped)
                        _stats["ws_pushes"] += 1
                    except:
                        _stats["ws_errors"] += 1

        self._send_json(200, {
            "code": 0,
            "data": {
                "received": len(frames),
                "processed": processed,
                "failed": failed,
                **type_counts,
            },
            "msg": f"processed {processed} messages",
        })

    def _handle_text_danmaku(self):
        """Accept pre-decoded text danmaku from CDP DOM scraper."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            data = json.loads(body)
        except Exception as e:
            self._send_json(400, {"code": -1, "msg": f"Parse error: {e}"})
            return

        room_id = data.get("roomId", "") or data.get("room_id", "")
        messages = data.get("messages", [])
        if not room_id or not messages:
            self._send_json(400, {"code": -1, "msg": "Missing roomId or messages"})
            return

        global _last_room_id
        _last_room_id = room_id

        processed = 0
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            content = (msg.get("content", "") or "").strip()
            if not content:
                continue
            mapped = {
                "user_id": msg.get("user_id", ""),
                "user_name": msg.get("user_name", "") or "匿名",
                "content": content,
                "danmaku_type": msg.get("danmaku_type", "comment"),
                "room_id": room_id,
                "timestamp": time.time() * 1000,
            }
            _stats["messages_decoded"] += 1
            processed += 1

            with _dm_buffer_lock:
                if len(_dm_buffer) < BUFFER_MAX:
                    _dm_buffer.append((room_id, mapped))

            if _ws_pusher:
                try:
                    _ws_pusher(room_id, mapped)
                    _stats["ws_pushes"] += 1
                except:
                    _stats["ws_errors"] += 1

        self._send_json(200, {
            "code": 0,
            "data": {"processed": processed, "roomId": room_id},
            "msg": f"accepted {processed} text danmaku",
        })

    def _send_json(self, code, data):
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def log_message(self, fmt, *args):
        # Suppress default logging
        pass

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

# =============================================================================
# Main
# =============================================================================
def main():
    print("=" * 60)
    print("  星播弹幕捕获服务器")
    print(f"  HTTP: {HTTP_PORT}  |  WS: {WS_PORT}  |  MySQL: {MYSQL_HOST}:{MYSQL_PORT}")
    print("=" * 60)

    # Init decoder
    _ensure_decoder()

    # Init MySQL
    _init_db()

    # Start WebSocket server
    _start_ws_server()

    # Start flush thread
    flush_thread = threading.Thread(target=_flush_loop, daemon=True)
    flush_thread.start()
    print(f"[Flush] MySQL flush every {FLUSH_INTERVAL}s")

    # Start HTTP server
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), CaptureHandler)
    print(f"[HTTP] Listening on port {HTTP_PORT}")
    print("[Ready] Waiting for Chrome extension data...\n")

    # Periodic stats print
    def _stats_printer():
        while True:
            time.sleep(30)
            buf = len(_dm_buffer)
            print(f"[Stats] frames={_stats['frames_received']} decoded={_stats['messages_decoded']} "
                  f"flushed={_stats['messages_flushed']} ws_push={_stats['ws_pushes']} "
                  f"buffer={buf} mysql_err={_stats['mysql_errors']}", flush=True)

    stats_thread = threading.Thread(target=_stats_printer, daemon=True)
    stats_thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Shutdown] Stopping...")
        server.shutdown()

if __name__ == "__main__":
    main()
