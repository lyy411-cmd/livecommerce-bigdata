# -*- coding: utf-8 -*-
"""
WebSocket 弹幕推送服务器 (websockets v16+ 兼容)
从 Kafka danmaku_events 消费弹幕消息，实时推送给前端 WebSocket 客户端

前端连接方式:
    ws://localhost:8765/danmaku/{room_id}  - 订阅特定房间弹幕
    ws://localhost:8765/danmaku/all        - 订阅所有房间弹幕
"""

import asyncio
import json
import logging
import threading
from collections import defaultdict

logger = logging.getLogger('WebSocketServer')


class DanmakuWebSocketServer:
    """WebSocket 弹幕推送服务器 (websockets v16+ 兼容)"""

    def __init__(self, host='0.0.0.0', port=8765, bootstrap_servers=None):
        self.host = host
        self.port = port
        self.bootstrap_servers = bootstrap_servers or ['192.168.104.100:9092']
        self.clients = defaultdict(set)     # room_id -> set of ws connections
        self.all_clients = set()            # clients subscribed to 'all'
        self.server = None
        self._loop = None
        self._thread = None
        self.running = False
        self.stats = {
            'messages_sent': 0,
            'active_connections': 0,
            'rooms_tracked': 0
        }

    def _get_path(self, websocket):
        """兼容 websockets v10~v16 获取请求路径"""
        # v16+: websocket.request.path
        try:
            req = getattr(websocket, 'request', None)
            if req is not None:
                p = getattr(req, 'path', '')
                if p:
                    return p
        except Exception:
            pass
        # v10~v12: websocket.path
        try:
            p = getattr(websocket, 'path', '')
            if p:
                return p
        except Exception:
            pass
        return ''

    @staticmethod
    def _normalize_room_id(room_id):
        """归一化 room_id：去除 CRAWL_DOUYIN_ / CRAWL_ 等前缀，统一为短 ID"""
        if not room_id or room_id == 'all':
            return room_id
        # CRAWL_DOUYIN_123456789 -> 123456789
        parts = room_id.split('_', 2)
        if len(parts) >= 3 and parts[0].upper() == 'CRAWL':
            return parts[2]
        return room_id

    async def handler(self, websocket):
        """处理 WebSocket 连接"""
        path = self._get_path(websocket)
        parts = path.strip('/').split('/')
        raw_room_id = parts[-1] if len(parts) > 1 else 'all'
        room_id = self._normalize_room_id(raw_room_id)

        if room_id == 'all':
            self.all_clients.add(websocket)
        else:
            self.clients[room_id].add(websocket)

        self.stats['active_connections'] = len(self.all_clients) + sum(len(v) for v in self.clients.values())
        self.stats['rooms_tracked'] = len(self.clients)

        logger.info(f"Client connected: room={room_id}, total={self.stats['active_connections']}")
        print(f"  [WS-CONN] + CONNECTED room={room_id} total_clients={self.stats['active_connections']} "
              f"(all={len(self.all_clients)} room_map={dict((k,len(v)) for k,v in self.clients.items())})",
              flush=True)

        try:
            await websocket.send(json.dumps({
                'type': 'connected',
                'room_id': room_id,
                'message': f'已连接弹幕服务器，房间: {room_id}'
            }, ensure_ascii=False))

            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get('type') == 'ping':
                        await websocket.send(json.dumps({'type': 'pong'}))
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            if room_id == 'all':
                self.all_clients.discard(websocket)
            else:
                self.clients[room_id].discard(websocket)
                if not self.clients[room_id]:
                    del self.clients[room_id]
            self.stats['active_connections'] = len(self.all_clients) + sum(len(v) for v in self.clients.values())
            self.stats['rooms_tracked'] = len(self.clients)
            logger.info(f"Client disconnected: room={room_id}, total={self.stats['active_connections']}")
            print(f"  [WS-CONN] - DISCONNECTED room={room_id} total_clients={self.stats['active_connections']}",
                  flush=True)
    
    _async_push_count = 0

    async def push_danmaku(self, room_id, data):
        """推送弹幕到订阅了该房间的客户端"""
        DanmakuWebSocketServer._async_push_count += 1
        _cnt = DanmakuWebSocketServer._async_push_count
        # 归一化 room_id，确保与客户端订阅的 key 一致
        room_id = self._normalize_room_id(room_id)
        payload = json.dumps(data, ensure_ascii=False)
        targets = set()
        targets.update(self.clients.get(room_id, set()))
        targets.update(self.all_clients)

        _should_log = _cnt <= 3 or _cnt % 100 == 0 or len(targets) > 0
        if _should_log:
            print(f"  [WS-ASYNC] push #{_cnt} room={room_id} targets={len(targets)} "
                  f"(all={len(self.all_clients)} room_cl={len(self.clients.get(room_id, set()))})",
                  flush=True)

        dead = set()
        for ws in targets:
            try:
                await ws.send(payload)
                self.stats['messages_sent'] += 1
            except:
                dead.add(ws)
        
        for ws in dead:
            for room_set in self.clients.values():
                room_set.discard(ws)
            self.all_clients.discard(ws)
        
        if _should_log and len(targets) > 0:
            print(f"  [WS-ASYNC] push #{_cnt} sent to {len(targets)-len(dead)} "
                  f"clients (dead={len(dead)})", flush=True)

    async def push_room_event(self, data):
        """推送直播间状态更新到所有客户端"""
        payload = json.dumps({'type': 'room_update', **data}, ensure_ascii=False)
        dead = set()
        for ws in self.all_clients:
            try:
                await ws.send(payload)
                self.stats['messages_sent'] += 1
            except:
                dead.add(ws)
        for ws in dead:
            self.all_clients.discard(ws)
        
        # Also push to specific room subscribers
        room_id = data.get('room_id', '')
        if room_id and room_id in self.clients:
            room_payload = json.dumps({'type': 'room_update', **data}, ensure_ascii=False)
            dead2 = set()
            for ws in self.clients[room_id]:
                try:
                    await ws.send(room_payload)
                except:
                    dead2.add(ws)
            for ws in dead2:
                self.clients[room_id].discard(ws)
    
    def _kafka_consumer_thread(self):
        """在独立线程中从 Kafka 消费弹幕并推送 (避免阻塞 async 事件循环)"""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                'danmaku_events', 'live_room_events',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.value.decode('utf-8')),
                group_id='ws-danmaku-group',
                auto_offset_reset='latest',
                consumer_timeout_ms=1000,
                request_timeout_ms=5000,
                session_timeout_ms=10000,
            )
            logger.info("WebSocket Kafka consumer started")

            while self.running:
                try:
                    records = consumer.poll(timeout_ms=500)
                    for tp, messages in records.items():
                        for msg in messages:
                            data = msg.value
                            if not self._loop or not self.running:
                                break
                            if msg.topic == 'danmaku_events':
                                asyncio.run_coroutine_threadsafe(
                                    self.push_danmaku(data.get('room_id', ''), data),
                                    self._loop
                                )
                            elif msg.topic == 'live_room_events':
                                asyncio.run_coroutine_threadsafe(
                                    self.push_room_event(data),
                                    self._loop
                                )
                except Exception as e:
                    if self.running:
                        logger.debug(f"Kafka consume error: {e}")
                        import time
                        time.sleep(1)

            consumer.close()
        except ImportError:
            logger.info("kafka-python not installed, WebSocket server runs without Kafka")
        except Exception as e:
            logger.info(f"WebSocket Kafka consumer failed (non-fatal): {e}")

    async def _start_async(self):
        """异步启动 WebSocket 服务器"""
        try:
            import websockets
            
            # 尝试绑定端口，最多重试3次
            for attempt in range(3):
                try:
                    self.server = await websockets.serve(
                        self.handler, self.host, self.port,
                        ping_interval=30, ping_timeout=10,
                        close_timeout=5,
                    )
                    break
                except OSError as bind_err:
                    if attempt < 2 and '10048' in str(bind_err):
                        print(f"  [WS] Port {self.port} busy, killing stale processes... (attempt {attempt+1}/3)",
                              flush=True)
                        try:
                            import subprocess
                            result = subprocess.run(
                                ['netstat', '-ano'], capture_output=True, text=True, timeout=5)
                            for line in result.stdout.splitlines():
                                if f':{self.port}' in line and 'LISTENING' in line:
                                    parts = line.strip().split()
                                    pid = int(parts[-1])
                                    if pid > 0:
                                        import os, signal
                                        os.kill(pid, signal.SIGTERM)
                                        print(f"  [WS] Killed PID {pid} on port {self.port}", flush=True)
                        except Exception as kill_err:
                            print(f"  [WS] Kill failed: {kill_err}", flush=True)
                        await asyncio.sleep(3)
                    else:
                        raise
            
            logger.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            print(f"  [WS] Server listening on ws://{self.host}:{self.port}", flush=True)

            # Kafka consumer 已禁用 — 避免 Kafka 超时重试造成 VM I/O 压力
            # 弹幕已通过 DanmakuDirectPusher 直接推送，无需 Kafka 中转

            await self.server.wait_closed()
        except ImportError:
            logger.error("websockets library not installed! Run: pip install websockets")
        except Exception as e:
            logger.error(f"WebSocket server error: {e}")
            print(f"  [WS] FATAL: WebSocket server failed to start: {e}", flush=True)

    def start(self):
        """在后台线程启动 WebSocket 服务器"""
        self.running = True

        def run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._start_async())
            except Exception as e:
                logger.error(f"WebSocket server thread error: {e}")

        self._thread = threading.Thread(target=run, daemon=True, name='ws-server')
        self._thread.start()
        # 给服务器一点时间完成启动
        import time
        time.sleep(0.5)
        logger.info(f"WebSocket server thread started on port {self.port}")

    def stop(self):
        """停止服务器"""
        self.running = False
        if self.server:
            try:
                self.server.close()
            except Exception:
                pass
        if self._loop and self._loop.is_running():
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass
        logger.info("WebSocket server stopped")


# ============================================================
# 直接推送模式 (不依赖 Kafka，供后端直接调用)
# ============================================================
class DanmakuDirectPusher:
    """供后端直接推送弹幕 (不经过 Kafka)
    由 run_cluster.py 中的爬虫回调使用
    """
    
    def __init__(self, ws_server):
        self.ws_server = ws_server
        self._push_count = 0
    
    def push_danmaku(self, room_id, data):
        """同步调用：从非异步线程推送弹幕"""
        self._push_count += 1
        if not self.ws_server._loop or not self.ws_server.running:
            if self._push_count <= 3:
                print(f"  [PUSHER] SKIP #{self._push_count}: loop={bool(self.ws_server._loop)} "
                      f"running={self.ws_server.running}", flush=True)
            return
        try:
            _should_log = (self._push_count <= 3 or self._push_count % 100 == 0)
            if _should_log:
                _n_all = len(self.ws_server.all_clients)
                _n_room = len(self.ws_server.clients.get(room_id, set()))
                print(f"  [PUSHER] #{self._push_count} room={room_id} "
                      f"all={_n_all} room_cl={_n_room} "
                      f"type={data.get('danmaku_type','?')} "
                      f"user={data.get('user_name','?')[:10]}", flush=True)
            future = asyncio.run_coroutine_threadsafe(
                self.ws_server.push_danmaku(room_id, data),
                self.ws_server._loop
            )
            # 仅对前 2 条消息等待 future 完成，确认推送成功
            if self._push_count <= 2:
                try:
                    future.result(timeout=2)
                except Exception:
                    pass
        except Exception as e:
            if self._push_count <= 5 or self._push_count % 200 == 0:
                print(f"  [PUSHER] ERR #{self._push_count}: {e}", flush=True)
    
    def push_room_event(self, data):
        """同步调用：推送房间状态更新"""
        if not self.ws_server._loop or not self.ws_server.running:
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self.ws_server.push_room_event(data),
                self.ws_server._loop
            )
        except Exception as e:
            logger.debug(f"Direct push room error: {e}")
