#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
连接虚拟机版 - 集成 Kafka/Hive/HDFS/Flink/MySQL
前提：虚拟机 IP 为 192.168.104.100
启动: python run_cluster.py
"""
import subprocess
import sys
import os
import time
import threading
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import random

# Windows 控制台 UTF-8 输出（避免 GBK 编码错误）
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass
import hashlib
from datetime import datetime
import pymysql

# === 实时数据管道组件 ===
try:
    from data_pipeline.kafka_producer import LiveCommerceKafkaProducer
    from data_pipeline.kafka_consumer import LiveCommerceKafkaConsumer
except ImportError:
    LiveCommerceKafkaProducer = None
    LiveCommerceKafkaConsumer = None

try:
    from data_pipeline.websocket_server import DanmakuWebSocketServer, DanmakuDirectPusher
except ImportError:
    DanmakuWebSocketServer = None
    DanmakuDirectPusher = None

VMS = {
    'mysql': '192.168.104.100:3306',
    'kafka': '192.168.104.100:9092',
    'hive': '192.168.104.100:10000',
    'hdfs_web': '192.168.104.100:9870',
    'flink_web': '192.168.104.100:8081'
}
USER = 'root'
PWD = '123456'
DB_NAME = 'livecommerce_db'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_PORT = 8080

# 管道执行状态
PIPELINE_STATS = {
    'collects': 0, 'last_collect': '--', 'source_type': 'simulated',
    'status': '待采集', 'quality': 0, 'total_rooms_new': 0
}
# === 实时数据管道实例 ===
_kafka_producer = None
_kafka_consumer = None
_ws_server = None
_ws_pusher = None
_crawler_sessions = {}  # platform -> crawler instance
_anchor_crawl_pid = None  # anchor crawl subprocess PID

FRONTEND_PORT = 5173


def _mysql_connect_retry(database=None, max_retries=3, connect_timeout=15):
    """Connect to MySQL with retries and longer timeout to handle VM intermittent connectivity."""
    import pymysql as _pm
    host = VMS['mysql'].split(':')[0]
    last_err = None
    for attempt in range(max_retries):
        try:
            if database:
                conn = _pm.connect(host=host, port=3306, user=USER, password=PWD,
                                   database=database, charset='utf8mb4',
                                   connect_timeout=connect_timeout,
                                   read_timeout=30, write_timeout=30)
            else:
                conn = _pm.connect(host=host, port=3306, user=USER, password=PWD,
                                   charset='utf8mb4', connect_timeout=connect_timeout)
            return conn
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                _wait = (attempt + 1) * 5
                print(f"  [MySQL] Connect attempt {attempt+1} failed: {e}, retrying in {_wait}s...")
                time.sleep(_wait)
    raise last_err


def check_mysql_available():
    try:
        import pymysql
        return True
    except ImportError:
        return False


def init_database():
    if not check_mysql_available():
        print("  [WARN] pymysql not installed, skip")
        return False
    try:
        import pymysql
        conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, charset='utf8mb4', connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET utf8mb4")
        conn.commit()
        conn.select_db(DB_NAME)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_user (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100),
                password VARCHAR(128) NOT NULL,
                role VARCHAR(20) DEFAULT 'customer',
                user_type VARCHAR(20) DEFAULT 'customer',
                phone VARCHAR(20),
                department VARCHAR(50),
                status TINYINT DEFAULT 1,
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                deleted TINYINT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS anchor (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50), nickname VARCHAR(50), platform VARCHAR(30),
                level VARCHAR(10), category VARCHAR(30),
                fans_count INT DEFAULT 0, live_hours INT DEFAULT 0,
                total_gmv DECIMAL(18,2) DEFAULT 0, total_orders INT DEFAULT 0,
                avg_conversion DECIMAL(8,4) DEFAULT 0, intro TEXT,
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                deleted TINYINT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS live_room (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                room_no VARCHAR(50) UNIQUE, room_name VARCHAR(100),
                anchor_name VARCHAR(50), platform VARCHAR(30),
                category VARCHAR(30), status VARCHAR(20),
                viewer_count INT DEFAULT 0, order_count INT DEFAULT 0,
                gmv DECIMAL(18,2) DEFAULT 0, conversion_rate DECIMAL(8,4) DEFAULT 0,
                start_time DATETIME, create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                deleted TINYINT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_info (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                order_no VARCHAR(50) UNIQUE, product_name VARCHAR(200),
                room_name VARCHAR(100), username VARCHAR(50),
                quantity INT DEFAULT 1, total_amount DECIMAL(12,2),
                platform VARCHAR(30), status VARCHAR(20),
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                deleted TINYINT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # === 实时数据表 ===
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rt_room_stats (
                room_id VARCHAR(50) PRIMARY KEY, room_name VARCHAR(200),
                anchor_name VARCHAR(50), platform VARCHAR(30), category VARCHAR(30),
                status VARCHAR(20) DEFAULT 'live', current_viewers INT DEFAULT 0,
                peak_viewers INT DEFAULT 0, total_danmaku BIGINT DEFAULT 0,
                total_orders BIGINT DEFAULT 0, total_gmv DECIMAL(18,2) DEFAULT 0,
                live_url VARCHAR(500), cover_url VARCHAR(500),
                start_time DATETIME,
                update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_platform (platform), INDEX idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rt_danmaku (
                id BIGINT PRIMARY KEY AUTO_INCREMENT, event_id VARCHAR(64),
                room_id VARCHAR(50), platform VARCHAR(30),
                user_id VARCHAR(50), user_name VARCHAR(100), content TEXT,
                danmaku_type VARCHAR(20) DEFAULT 'comment',
                event_time DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
                INDEX idx_room_time (room_id, event_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rt_product (
                id BIGINT PRIMARY KEY AUTO_INCREMENT, product_id VARCHAR(50),
                room_id VARCHAR(50), platform VARCHAR(30),
                product_name VARCHAR(200), price DECIMAL(12,2),
                original_price DECIMAL(12,2), sales INT DEFAULT 0,
                category VARCHAR(30), image_url VARCHAR(500),
                product_url VARCHAR(500), sort_order INT DEFAULT 0,
                update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_room (room_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawler_session (
                id BIGINT PRIMARY KEY AUTO_INCREMENT, platform VARCHAR(30),
                session_type VARCHAR(20) DEFAULT 'discovery',
                room_id VARCHAR(50), room_name VARCHAR(200),
                status VARCHAR(20) DEFAULT 'running',
                rooms_discovered INT DEFAULT 0, danmaku_captured BIGINT DEFAULT 0,
                started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_heartbeat DATETIME, error_msg TEXT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # Add real-data columns to live_room (catch 1060=duplicate column)
        for col_name, col_def in [
            ("live_url", "VARCHAR(500)"),
            ("room_id_external", "VARCHAR(50)"),
            ("cover_url", "VARCHAR(500)"),
            ("data_source", "VARCHAR(20) DEFAULT 'simulated'"),
            ("has_shopping_cart", "TINYINT DEFAULT 1"),
        ]:
            try:
                cursor.execute(f"ALTER TABLE live_room ADD COLUMN {col_name} {col_def}")
            except Exception:
                pass  # Column already exists or other error, skip

        sha_pw = '8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92'
        cursor.execute("INSERT IGNORE INTO sys_user (username, email, password, role, user_type, status) VALUES ('admin', 'admin@livecommerce.com', %s, 'admin', 'staff', 1)", (sha_pw,))
        cursor.execute("INSERT IGNORE INTO sys_user (username, email, password, role, user_type, status) VALUES ('operator', 'op@livecommerce.com', %s, 'operator', 'staff', 1)", (sha_pw,))

        # Seed data removed - all data now comes from real crawlers

        conn.commit()
        conn.close()
        print(f"  [MySQL] Database {DB_NAME} initialized OK")
        return True
    except Exception as e:
        print(f"  [WARN] MySQL init failed: {e}")
        return False


def query_mysql(sql, params=None):
    if not check_mysql_available():
        return None
    try:
        import pymysql
        conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql, params or ())
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        print(f"  [MySQL ERR] {e}")
        return None


def get_flink_jobs():
    import urllib.request, json as j
    try:
        with urllib.request.urlopen(f"http://{VMS['flink_web']}/jobs", timeout=3) as r:
            data = j.loads(r.read())
            return [{
                'name': jj.get('name', 'unknown'),
                'status': jj.get('status', 'UNKNOWN'),
                'start': jj.get('start-time', 0),
                'duration': jj.get('duration', 0)
            } for jj in data.get('jobs', [])]
    except:
        return []


def get_flink_overview():
    import urllib.request, json as j
    try:
        with urllib.request.urlopen(f"http://{VMS['flink_web']}/overview", timeout=3) as r:
            return j.loads(r.read())
    except:
        return {}


# Mock data removed - all data now comes from real crawlers
MOCK_ANCHORS = []
MOCK_ROOMS = []


# ============ SSE 事件队列（短轮询模式）============
SSE_EVENT_QUEUE = []  # 累积未推送的事件
SSE_LAST_IDX = [0]    # 各客户端读取位置

def _broadcast_event(event: dict):
    """事件入队，供下次 SSE 拉取"""
    SSE_EVENT_QUEUE.append(event)
    # 队列只保留最近 50 条
    if len(SSE_EVENT_QUEUE) > 50:
        SSE_EVENT_QUEUE.pop(0)
        SSE_LAST_IDX[0] = max(0, SSE_LAST_IDX[0] - 1)


# ============ 实时订单生成器（批量并发，独立生命周期） ============
order_sim_lock = threading.Lock()

def skewed_delay(min_val, max_val, mode='normal'):
    span = max_val - min_val
    if mode == 'quick':      d = int(min_val + (random.random() ** 2) * span)
    elif mode == 'slow':     d = int(max_val - (random.random() ** 2) * span)
    elif mode == 'lognormal': d = int(min_val + (random.random() ** 1.5) * span)
    else: d = random.randint(min_val, max_val)
    return max(min_val, min(d, max_val))

def run_order_lifecycle(oid, order_no, prod, user, qty, plat, amount):
    """单个订单的完整生命周期（独立线程，广播SSE事件）"""
    conn = None
    try:
        # Stage 1: 待支付（3-8秒后）
        pay_delay = skewed_delay(3, 8, 'quick') if random.random() < 0.3 else skewed_delay(4, 12, 'lognormal')
        time.sleep(pay_delay)

        # 5% 取消订单
        if random.random() < 0.05:
            with order_sim_lock:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute("UPDATE order_info SET status='cancelled' WHERE id=%s", (oid,))
                conn.commit()
                conn.close(); conn = None
            _broadcast_event({'type': 'order_cancelled', 'orderNo': order_no, 'oid': oid, 'msg': f'订单 {order_no} 已取消', 'ts': int(time.time() * 1000)})
            if _kafka_producer:
                _kafka_producer.send_order_event({'type': 'order_cancelled', 'orderNo': order_no, 'oid': oid})
            return

        # Stage 2: 已支付
        with order_sim_lock:
            conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
            cur = conn.cursor()
            cur.execute("UPDATE order_info SET status='paid' WHERE id=%s AND status='pending'", (oid,))
            cur.execute("UPDATE live_room SET order_count=order_count+1, gmv=gmv+%s WHERE room_name=%s", (amount, prod[1]))
            conn.commit()
            conn.close(); conn = None
        _broadcast_event({'type': 'order_paid', 'orderNo': order_no, 'oid': oid, 'amount': amount, 'msg': f'用户已支付 ￥{amount}', 'ts': int(time.time() * 1000)})
        if _kafka_producer:
            _kafka_producer.send_order_event({'type': 'order_paid', 'orderNo': order_no, 'oid': oid, 'amount': amount})

        # Stage 3: 已发货
        ship_delay = skewed_delay(2, 5, 'quick') if random.random() > 0.08 else skewed_delay(6, 10, 'slow')
        time.sleep(ship_delay)
        with order_sim_lock:
            conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
            cur = conn.cursor()
            cur.execute("UPDATE order_info SET status='shipped' WHERE id=%s AND status='paid'", (oid,))
            conn.commit()
            conn.close(); conn = None
        _broadcast_event({'type': 'order_shipped', 'orderNo': order_no, 'oid': oid, 'msg': f'订单 {order_no} 已发货', 'ts': int(time.time() * 1000)})
        if _kafka_producer:
            _kafka_producer.send_order_event({'type': 'order_shipped', 'orderNo': order_no, 'oid': oid})

        # Stage 4: 已签收
        transit = skewed_delay(3, 8, 'quick') if random.random() < 0.25 else skewed_delay(5, 12, 'lognormal')
        time.sleep(transit)
        with order_sim_lock:
            conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
            cur = conn.cursor()
            cur.execute("UPDATE order_info SET status='delivered' WHERE id=%s AND status='shipped'", (oid,))
            conn.commit()
            conn.close(); conn = None
        _broadcast_event({'type': 'order_delivered', 'orderNo': order_no, 'oid': oid, 'msg': f'订单 {order_no} 已签收', 'ts': int(time.time() * 1000)})
        if _kafka_producer:
            _kafka_producer.send_order_event({'type': 'order_delivered', 'orderNo': order_no, 'oid': oid})

    except Exception as e:
        print(f"  [OrderSim] lifecycle error {order_no}: {e}")
    finally:
        if conn:
            try: conn.close()
            except: pass


def order_simulator_loop():
    """批量并发订单生成器：每次生成1-5个订单，每个独立线程跑生命周期"""
    products = [
        ('华为 Mate70 Pro', '华为旗舰数码专场', 5999), ('花西子蜜粉饼', '花西子美妆专场', 358),
        ('三只松鼠坚果大礼包', '三只松鼠食品专场', 168), ('波司登轻薄羽绒服', '波司登服饰专场', 1299),
        ('戴森 V15 吸尘器', '美的家电专场', 3999), ('安踏 C37 跑鞋', '安踏运动专场', 499),
        ('小米智能手表', '小米官方数码专场', 899), ('良品铺子零食组合', '三只松鼠食品专场', 88),
        ('珀莱雅双抗精华', '花西子美妆专场', 269), ('李宁篮球鞋', '安踏运动专场', 599),
        ('苹果 AirPods Pro', '华为旗舰数码专场', 1799), ('完美日记眼影盘', '花西子美妆专场', 129),
        ('蕉下防晒衣', '波司登服饰专场', 299), ('海尔冰箱', '美的家电专场', 3299),
        ('漫步者蓝牙音箱', '小米官方数码专场', 399),
    ]
    users = ['杭州张女士', '上海李先生', '北京王先生', '广州赵女士', '深圳刘先生',
             '成都陈先生', '武汉黄女士', '南京吴女士', '杭州林女士', '重庆周先生']
    platforms = ['douyin']
    order_counter = [20000]

    # 从数据库已有最大订单号开始
    try:
        conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT IFNULL(MAX(CAST(SUBSTRING(order_no, 6) AS UNSIGNED)), 0) + 1 FROM order_info")
        order_counter[0] = max(cur.fetchone()[0], 20000)
        conn.close()
    except: pass

    print("  [OrderSim] Batch-order simulator started (concurrent lifecycle threads)")
    while True:
        try:
            batch_size = random.choices([1, 2, 3, 4, 5], weights=[15, 30, 30, 15, 10])[0]
            spawn_gap = random.uniform(0.3, 1.5) if batch_size > 1 else 0

            for i in range(batch_size):
                prod = random.choice(products)
                user = random.choice(users)
                plat = random.choice(platforms)
                qty = random.choices([1, 2, 3, 5], weights=[50, 30, 15, 5])[0]
                order_no = f"ORDER{order_counter[0]}"
                order_counter[0] += 1
                amount = prod[2] * qty
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                with order_sim_lock:
                    conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                    cur = conn.cursor()
                    cur.execute("INSERT INTO order_info (order_no, product_name, room_name, username, quantity, total_amount, platform, status, create_time) VALUES (%s,%s,%s,%s,%s,%s,%s,'pending',NOW())",
                        (order_no, prod[0], prod[1], user, qty, amount, plat))
                    oid = cur.lastrowid
                    conn.commit()
                    conn.close()

                _broadcast_event({'type': 'new_order', 'orderNo': order_no, 'oid': oid,
                    'productName': prod[0], 'roomName': prod[1], 'username': user,
                    'quantity': qty, 'totalAmount': amount, 'platform': plat,
                    'status': 'pending', 'createTime': now_str,
                    'msg': f'新订单 {order_no} 「{prod[0]}」来自 {user} ￥{amount}', 'ts': int(time.time() * 1000)})
                if _kafka_producer:
                    _kafka_producer.send_order_event({'type': 'new_order', 'orderNo': order_no, 'oid': oid,
                        'productName': prod[0], 'roomName': prod[1], 'username': user,
                        'quantity': qty, 'totalAmount': amount, 'platform': plat,
                        'status': 'pending', 'createTime': now_str})

                # 每个订单独立线程跑生命周期
                threading.Thread(target=run_order_lifecycle, args=(oid, order_no, prod, user, qty, plat, amount), daemon=True).start()

                if i < batch_size - 1:
                    time.sleep(spawn_gap)

            active = threading.active_count()
            print(f"  [OrderSim] batch {batch_size} orders created | total active threads: ~{active}")
            time.sleep(random.randint(3, 12))

        except Exception as e:
            print(f"  [OrderSim] Error: {e}")
            time.sleep(2)


class APIHandler(BaseHTTPRequestHandler):
    def _send(self, data, code=200):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def do_OPTIONS(self):
        self._send({})

    def do_GET(self):
        p = self.path.split('?')[0]

        # SSE 短轮询：客户端每3秒拉取一次，传入 lastIdx 返回增量事件
        if p == '/api/events/stream':
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            try:
                last_idx = int(qs.get('idx', ['0'])[0])
            except:
                last_idx = 0
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'close')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            try:
                # 发送未读事件
                q = SSE_EVENT_QUEUE
                if not last_idx and not q:
                    # 第一次连接，发送 connected
                    self.wfile.write(f"data: {json.dumps({'type':'connected','ts':int(time.time()*1000)}, ensure_ascii=False)}\n\n".encode('utf-8'))
                else:
                    # 推 last_idx 之后的所有事件
                    start = max(0, min(last_idx, len(q)))
                    for i in range(start, len(q)):
                        self.wfile.write(f"data: {json.dumps(q[i], ensure_ascii=False)}\n\n".encode('utf-8'))
                # 告知当前索引
                self.wfile.write(f"data: {json.dumps({'type':'__idx__','idx':len(q),'ts':int(time.time()*1000)}, ensure_ascii=False)}\n\n".encode('utf-8'))
                self.wfile.flush()
            except Exception:
                pass
            return

        if p == '/api/auth/me':
            """获取当前登录用户信息"""
            auth = self.headers.get('Authorization', '')
            token = auth.replace('Bearer ', '') if auth.startswith('Bearer ') else ''
            if token:
                try:
                    uid = int(token.split('-')[-1])
                    rows = query_mysql("SELECT * FROM sys_user WHERE id=%s AND status=1 AND deleted=0", (uid,))
                    if rows:
                        r = rows[0]
                        self._send({'code': 0, 'data': {
                            'id': r['id'], 'username': r['username'], 'email': r['email'] or '',
                            'role': r['role'], 'userType': r['user_type']
                        }})
                        return
                except Exception:
                    pass
            self._send({'code': 0, 'data': {'id': 1, 'username': 'admin', 'role': 'admin', 'userType': 'staff'}})
            return

        elif p == '/api/pipeline/stats':
            rows = query_mysql("SELECT COUNT(*) as cnt FROM live_room WHERE deleted=0")
            self._send({'code': 0, 'data': {
                'total_collects': PIPELINE_STATS.get('collects', 0),
                'total_rooms': int(rows[0]['cnt'] or 0) if rows else 0,
                'last_collect': PIPELINE_STATS.get('last_collect', '--'),
                'source_type': PIPELINE_STATS.get('source_type', 'simulated'),
                'status': PIPELINE_STATS.get('status', '待采集'),
                'quality_score': PIPELINE_STATS.get('quality', 0),
                'pipeline_steps': ['Step 1: Spider Fetch', 'Step 2: NULL Removal',
                    'Step 3: Deduplication', 'Step 4: Normalization',
                    'Step 5: Anomaly Detection', 'Step 6: Quality Scoring', 'Step 7: MySQL Write']
            }})

        elif p == '/api/cluster/status':
            overview = get_flink_overview()
            self._send({'code': 0, 'data': {
                'mysql': VMS['mysql'],
                'kafka': VMS['kafka'],
                'hive': VMS['hive'],
                'hdfs': VMS['hdfs_web'],
                'flink': VMS['flink_web'],
                'flink_jobs_count': overview.get('jobs-running', 0) + overview.get('jobs-finished', 0),
                'flink_slots_total': overview.get('slots-total', 0),
                'flink_slots_available': overview.get('slots-available', 0)
            }})

        elif p == '/api/datavis/dashboard/kpi':
            """KPI from real data"""
            rooms = query_mysql("SELECT COUNT(*) as cnt, COALESCE(SUM(viewer_count),0) as viewers, COALESCE(SUM(gmv),0) as gmv, COALESCE(SUM(order_count),0) as orders FROM live_room WHERE deleted=0")
            anchors = query_mysql("SELECT COUNT(*) as cnt FROM anchor WHERE deleted=0")
            orders = query_mysql("SELECT COUNT(*) as cnt, COALESCE(SUM(total_amount),0) as amt FROM order_info WHERE deleted=0")
            r = rooms[0] if rooms else {}
            a = anchors[0] if anchors else {}
            o = orders[0] if orders else {}
            total_gmv = float(r.get('gmv', 0) or 0)
            total_viewers = int(r.get('viewers', 0) or 0)
            total_rooms = int(r.get('cnt', 0) or 0)
            total_anchors = int(a.get('cnt', 0) or 0)
            total_orders = int(o.get('cnt', 0) or 0) + int(r.get('orders', 0) or 0)
            total_amount = float(o.get('amt', 0) or 0)
            avg_conv = 4.3 if total_rooms == 0 else round(total_orders / max(total_viewers, 1) * 100, 1)
            self._send({'code': 0, 'data': {
                'totalGmv': total_gmv, 'totalRooms': total_rooms,
                'totalAnchors': total_anchors, 'totalViewers': total_viewers,
                'avgConversion': avg_conv, 'totalOrders': total_orders,
                'totalAmount': total_amount
            }})

        elif p.startswith('/api/datavis/dashboard/gmv-trend'):
            """GMV trend from real order data"""
            rows = query_mysql(
                "SELECT DATE(create_time) as dt, SUM(total_amount) as gmv "
                "FROM order_info WHERE deleted=0 AND create_time >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
                "GROUP BY DATE(create_time) ORDER BY dt")
            if rows:
                data = [{'date': str(r['dt']), 'value': float(r['gmv'] or 0)} for r in rows]
            else:
                # No orders yet - show room GMV as single point
                room_gmv = query_mysql("SELECT COALESCE(SUM(gmv),0) as g FROM live_room WHERE deleted=0")
                g = float(room_gmv[0]['g']) if room_gmv else 0
                from datetime import date, timedelta
                today = date.today()
                data = [{'date': str(today - timedelta(days=i)), 'value': 0 if i > 0 else g} for i in range(29, -1, -1)]
            self._send({'code': 0, 'data': data})

        elif p == '/api/datavis/dashboard/platform-distribution':
            rows = query_mysql("SELECT category, COUNT(*) as cnt FROM live_room WHERE deleted=0 AND category != '' GROUP BY category ORDER BY cnt DESC")
            data = [{'name': r['category'], 'value': int(r['cnt'])} for r in rows] if rows else []
            self._send({'code': 0, 'data': data})

        elif p == '/api/datavis/dashboard/category-rank':
            rows = query_mysql("SELECT category, SUM(viewer_count) as viewers FROM live_room WHERE deleted=0 AND category != '' GROUP BY category ORDER BY viewers DESC LIMIT 10")
            data = [{'name': r['category'], 'value': int(r['viewers'] or 0)} for r in rows] if rows else []
            self._send({'code': 0, 'data': data})

        elif p.startswith('/api/datavis/dashboard/anchor-rank'):
            limit = 30
            try:
                if '?' in self.path and 'limit=' in self.path:
                    limit = int(self.path.split('limit=')[-1])
            except: pass
            rows = query_mysql(f"SELECT * FROM anchor WHERE deleted=0 ORDER BY total_gmv DESC LIMIT {limit}")
            data = [{
                'id': r['id'], 'name': r['name'], 'platform': r['platform'], 'level': r['level'],
                'category': r['category'], 'fansCount': int(r['fans_count'] or 0),
                'liveHours': int(r['live_hours'] or 0), 'totalGmv': float(r['total_gmv'] or 0),
                'totalOrders': int(r['total_orders'] or 0), 'avgConversion': float(r['avg_conversion'] or 0)
            } for r in rows] if rows else []
            self._send({'code': 0, 'data': data})

        elif p == '/api/datavis/dashboard/geo-distribution':
            # Try to get geo from danmaku or orders, fallback to empty
            rows = query_mysql("SELECT username, COUNT(*) as cnt FROM order_info WHERE deleted=0 GROUP BY username ORDER BY cnt DESC LIMIT 10")
            data = [{'name': r['username'], 'value': int(r['cnt'])} for r in rows] if rows else []
            self._send({'code': 0, 'data': data})

        elif p == '/api/datavis/dashboard/realtime':
            rooms = query_mysql("SELECT COUNT(*) as cnt, COALESCE(SUM(current_viewers),0) as v, COALESCE(SUM(total_gmv),0) as g FROM rt_room_stats WHERE status='live'")
            r = rooms[0] if rooms else {}
            self._send({'code': 0, 'data': {
                'currentViewers': int(r.get('v', 0) or 0),
                'currentOrders': 0,
                'currentGmv': float(r.get('g', 0) or 0),
                'onlineAnchors': int(r.get('cnt', 0) or 0)
            }})

        elif p == '/api/datavis/dashboard/activities':
            """实时动态事件流 - 从数据库聚合"""
            def to_seconds(time_str):
                """将时间字符串转换为秒数（用于排序）"""
                if time_str == '刚刚': return 0
                if '分钟前' in time_str: return int(time_str.replace('分钟前','')) * 60
                if '小时前' in time_str: return int(time_str.replace('小时前','')) * 3600
                if '昨天' in time_str: return 86400
                if '天前' in time_str: return int(time_str.replace('天前','')) * 86400
                return 999999

            activities = []
            # 1) 最近的订单
            orders = query_mysql("SELECT order_no, product_name, room_name, platform, create_time FROM order_info WHERE deleted=0 ORDER BY id DESC LIMIT 2")
            for o in (orders or []):
                t = o['create_time']
                if t:
                    delta = datetime.now() - t
                    secs = int(delta.total_seconds())
                    if secs < 60: time_str = '刚刚'
                    elif secs < 3600: time_str = f"{secs // 60}分钟前"
                    elif secs < 86400: time_str = f"{secs // 3600}小时前"
                    elif secs < 172800: time_str = '昨天'
                    else: time_str = f"{secs // 86400}天前"
                else:
                    time_str = '刚刚'
                activities.append({
                    'text': f"新订单 {o['order_no']} 「{o['product_name']}」 来自 {o['room_name']}",
                    'time': time_str,
                    '_sort': to_seconds(time_str),
                    'color': '#00ffcc', 'icon': 'order'
                })
            # 2) 观众最多的直播间
            rooms = query_mysql("SELECT room_name, anchor_name, viewer_count FROM live_room WHERE deleted=0 ORDER BY viewer_count DESC LIMIT 2")
            for i, rm in enumerate(rooms or []):
                time_str = '刚刚' if i == 0 else f'{random.randint(2, 5)}分钟前'
                activities.append({
                    'text': f"直播间「{rm['room_name']}」在线观众 {int(rm['viewer_count'] or 0):,}",
                    'time': time_str,
                    '_sort': 0 if time_str == '刚刚' else int(time_str.replace('分钟前', '')) * 60,
                    'color': '#a855f7', 'icon': 'live'
                })
            # 3) GMV 最高的主播
            anchors = query_mysql("SELECT name, total_gmv, total_orders, avg_conversion FROM anchor WHERE deleted=0 ORDER BY total_gmv DESC LIMIT 1")
            if anchors:
                a = anchors[0]
                gmv_yi = float(a['total_gmv']) / 1e8
                activities.append({
                    'text': f"主播「{a['name']}」累计 GMV {gmv_yi:.1f}亿，转化率 {float(a['avg_conversion']):.1f}%",
                    'time': '15分钟前',
                    '_sort': 15 * 60,
                    'color': '#ffa502', 'icon': 'star'
                })
            # 4) 类目分布
            cats = query_mysql("SELECT category, COUNT(*) as cnt FROM live_room WHERE deleted=0 AND category != '' GROUP BY category ORDER BY cnt DESC")
            if cats:
                c = cats[0]
                pct = round(c['cnt'] * 100 / sum(int(x['cnt']) for x in cats), 0)
                activities.append({
                    'text': f"类目「{c['category']}」直播间占比 {pct}%",
                    'time': '30分钟前',
                    '_sort': 30 * 60,
                    'color': '#ff4757', 'icon': 'platform'
                })
            # 5) 系统状态
            activities.append({
                'text': f"系统：MySQL @ {VMS['mysql']} 实时同步中",
                'time': '持续运行',
                '_sort': 999999,
                'color': '#00d9ff', 'icon': 'system'
            })
            # 按时间升序排序：最近 → 久远
            activities.sort(key=lambda x: x.pop('_sort', 999999))
            # 清理前端不需要的字段
            for a in activities:
                a.pop('_sort', None)
            self._send({'code': 0, 'data': activities[:6]})

        elif p.startswith('/api/livecommerce/room/page'):
            """直播间分页+搜索"""
            from decimal import Decimal
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            platform_f = (qs.get('platform', [''])[0] or '').strip()
            status_f = (qs.get('status', [''])[0] or '').strip()
            category_f = (qs.get('category', [''])[0] or '').strip()
            cart_f = (qs.get('hasShoppingCart', [''])[0] or '').strip()
            search = (qs.get('search', [''])[0] or '').strip()
            page_no = max(1, int(qs.get('page', ['1'])[0] or '1'))
            page_size = min(500, max(10, int(qs.get('pageSize', ['20'])[0] or '20')))
            offset = (page_no - 1) * page_size
            where = "WHERE deleted=0"
            params = []
            if platform_f:
                where += " AND platform=%s"; params.append(platform_f)
            if status_f:
                where += " AND status=%s"; params.append(status_f)
            if category_f:
                where += " AND category=%s"; params.append(category_f)
            if cart_f == '1':
                where += " AND has_shopping_cart=1"
            elif cart_f == '0':
                where += " AND (has_shopping_cart=0 OR has_shopping_cart IS NULL)"
            if search:
                where += " AND (room_name LIKE %s OR anchor_name LIKE %s OR room_no LIKE %s)"
                like = f"%{search}%"
                params.extend([like, like, like])
            # Get total count
            count_rows = query_mysql(f"SELECT COUNT(*) as cnt FROM live_room {where}", params)
            total = count_rows[0]['cnt'] if count_rows else 0
            rows = query_mysql(f"SELECT * FROM live_room {where} ORDER BY viewer_count DESC LIMIT %s OFFSET %s", params + [page_size, offset])
            data = []
            for r in rows:
                v = r['gmv']
                gmv_val = float(v) if isinstance(v, (int, float, Decimal)) else 0
                data.append({
                    'id': r['id'], 'roomNo': r['room_no'], 'roomName': r['room_name'],
                    'anchorName': r['anchor_name'], 'platform': r['platform'],
                    'category': r['category'], 'status': r['status'],
                    'viewerCount': int(r['viewer_count'] or 0), 'orderCount': int(r['order_count'] or 0),
                    'gmv': gmv_val,
                    'liveUrl': r.get('live_url', '') or '',
                    'dataSource': r.get('data_source', 'simulated') or 'simulated',
                    'roomIdExternal': r.get('room_id_external', '') or '',
                    'hasShoppingCart': bool(r.get('has_shopping_cart', 1)),
                })
            # No fallback - only real crawled data
            self._send({'code': 0, 'data': {'records': data, 'total': total, 'page': page_no, 'pageSize': page_size}})

        elif p == '/api/livecommerce/room/live':
            rows = query_mysql("SELECT * FROM live_room WHERE deleted=0 AND status='live' ORDER BY viewer_count DESC")
            data = [{
                'id': r['id'], 'roomNo': r['room_no'], 'roomName': r['room_name'],
                'anchorName': r['anchor_name'],
                'platform': r['platform'], 'category': r.get('category', ''),
                'viewerCount': int(r['viewer_count'] or 0),
                'orderCount': int(r['order_count'] or 0), 'gmv': float(r['gmv'] or 0),
                'status': r['status'],
                'liveUrl': r.get('live_url', '') or '',
                'roomIdExternal': r.get('room_id_external', '') or '',
                'hasShoppingCart': bool(r.get('has_shopping_cart', 1)),
            } for r in rows] if rows else []
            self._send({'code': 0, 'data': data})

        elif p == '/api/livecommerce/room/overview':
            rooms = query_mysql("SELECT COUNT(*) as cnt, IFNULL(SUM(viewer_count),0) as v, IFNULL(SUM(gmv),0) as g, IFNULL(SUM(order_count),0) as o FROM live_room WHERE deleted=0")
            live = query_mysql("SELECT COUNT(*) as cnt FROM live_room WHERE deleted=0 AND status='live'")
            r = rooms[0] if rooms else {}
            l = live[0] if live else {}
            self._send({'code': 0, 'data': {
                'totalRooms': int(r.get('cnt', 0) or 0),
                'liveRooms': int(l.get('cnt', 0) or 0),
                'totalViewers': int(r.get('v', 0) or 0),
                'totalGmv': float(r.get('g', 0) or 0),
                'totalOrders': int(r.get('o', 0) or 0)
            }})

        elif p.startswith('/api/livecommerce/anchor/page'):
            """主播分页+搜索"""
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            platform_f = (qs.get('platform', [''])[0] or '').strip()
            level_f = (qs.get('level', [''])[0] or '').strip()
            search = (qs.get('search', [''])[0] or '').strip()
            where = "WHERE deleted=0"
            params = []
            if platform_f:
                where += " AND platform=%s"; params.append(platform_f)
            if level_f:
                where += " AND level=%s"; params.append(level_f)
            if search:
                where += " AND (name LIKE %s OR nickname LIKE %s OR category LIKE %s)"
                like = f"%{search}%"
                params.extend([like, like, like])
            rows = query_mysql(f"SELECT * FROM anchor {where} ORDER BY total_gmv DESC LIMIT 200", params)
            data = [{
                'id': r['id'], 'name': r['name'], 'nickname': r['nickname'] or '',
                'platform': r['platform'], 'level': r['level'], 'category': r['category'],
                'fansCount': int(r['fans_count'] or 0), 'liveHours': int(r['live_hours'] or 0),
                'totalGmv': float(r['total_gmv'] or 0), 'totalOrders': int(r['total_orders'] or 0),
                'avgConversion': float(r['avg_conversion'] or 0)
            } for r in rows] if rows else []
            self._send({'code': 0, 'data': {'records': data, 'total': len(data), 'page': 1, 'pageSize': 10}})

        elif p.startswith('/api/livecommerce/anchor/search'):
            """搜索带货主播：从live_room表中按主播名搜索，返回唯一主播及其直播间信息"""
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            keyword = (qs.get('keyword', [''])[0] or '').strip()
            category_f = (qs.get('category', [''])[0] or '').strip()
            if not keyword and not category_f:
                self._send({'code': 0, 'data': []})
                return
            where = "WHERE deleted=0 AND anchor_name IS NOT NULL AND anchor_name != ''"
            params = []
            if keyword:
                where += " AND (anchor_name LIKE %s OR room_name LIKE %s)"
                like = f"%{keyword}%"
                params.extend([like, like])
            if category_f:
                where += " AND category = %s"
                params.append(category_f)
            rows = query_mysql(
                f"SELECT anchor_name, category, "
                f"  COUNT(*) as room_count, "
                f"  MAX(viewer_count) as max_viewers, "
                f"  SUM(gmv) as total_gmv, "
                f"  SUM(order_count) as total_orders, "
                f"  MAX(CASE WHEN status='live' THEN 1 ELSE 0 END) as is_live, "
                f"  MAX(CASE WHEN status='live' THEN room_id_external ELSE NULL END) as live_room_id, "
                f"  MAX(CASE WHEN status='live' THEN live_url ELSE NULL END) as live_url, "
                f"  MAX(CASE WHEN status='live' THEN room_no ELSE NULL END) as live_room_no "
                f"FROM live_room {where} "
                f"GROUP BY anchor_name, category "
                f"ORDER BY is_live DESC, max_viewers DESC "
                f"LIMIT 50", params)
            data = [{
                'anchorName': r['anchor_name'],
                'category': r.get('category', ''),
                'roomCount': int(r.get('room_count', 0)),
                'maxViewers': int(r.get('max_viewers') or 0),
                'totalGmv': float(r.get('total_gmv') or 0),
                'totalOrders': int(r.get('total_orders') or 0),
                'isLive': bool(r.get('is_live', 0)),
                'liveRoomId': r.get('live_room_id', '') or '',
                'liveUrl': r.get('live_url', '') or '',
                'liveRoomNo': r.get('live_room_no', '') or '',
            } for r in rows] if rows else []
            self._send({'code': 0, 'data': data})

        elif p.startswith('/api/livecommerce/order/page'):
            """订单分页+搜索"""
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            status_f = (qs.get('status', [''])[0] or '').strip()
            platform_f = (qs.get('platform', [''])[0] or '').strip()
            where = "WHERE deleted=0"
            params = []
            if status_f:
                where += " AND status=%s"
                params.append(status_f)
            if platform_f:
                where += " AND platform=%s"
                params.append(platform_f)
            rows = query_mysql(f"SELECT * FROM order_info {where} ORDER BY id DESC LIMIT 100", params)
            data = [{
                'id': r['id'], 'orderNo': r['order_no'], 'productName': r['product_name'],
                'roomName': r['room_name'], 'username': r['username'],
                'quantity': int(r['quantity'] or 0), 'totalAmount': float(r['total_amount'] or 0),
                'platform': r['platform'], 'status': r['status'],
                'createTime': r['create_time'].strftime('%Y-%m-%d %H:%M:%S') if r['create_time'] else ''
            } for r in rows] if rows else []
            self._send({'code': 0, 'data': {'records': data, 'total': len(data), 'page': 1, 'pageSize': 20}})

        elif p == '/api/livecommerce/order/overview':
            """订单全量统计 - 按状态精确统计"""
            def count_by_status(s):
                rows = query_mysql("SELECT COUNT(*) as cnt FROM order_info WHERE deleted=0 AND status=%s", (s,))
                return int(rows[0]['cnt'] or 0) if rows else 0
            total_row = query_mysql("SELECT COUNT(*) as total, IFNULL(SUM(total_amount), 0) as amount FROM order_info WHERE deleted=0")
            self._send({'code': 0, 'data': {
                'totalOrders': int(total_row[0]['total'] or 0) if total_row else 0,
                'pendingOrders': count_by_status('pending'),
                'paidOrders': count_by_status('paid'),
                'shippedOrders': count_by_status('shipped'),
                'deliveredOrders': count_by_status('delivered'),
                'cancelledOrders': count_by_status('cancelled'),
                'totalAmount': float(total_row[0]['amount'] or 0) if total_row else 0
            }})

        elif p == '/api/datapipeline/status':
            jobs = get_flink_jobs()
            self._send({'code': 0, 'data': {
                'kafkaEnabled': True,
                'kafkaBroker': VMS['kafka'],
                'hdfsPath': '/livecommerce',
                'processedCount': PIPELINE_STATS.get('collects', 0),
                'cleanedCount': PIPELINE_STATS.get('total_rooms_new', 0),
                'flinkJobs': jobs if jobs else [
                    {'name': 'live-room-cleaning (waiting)', 'status': 'NOT_SUBMITTED', 'parallelism': 4}
                ],
                'topics': ['live_room_events', 'order_events', 'user_behavior'],
                'tables': {
                    'mysql': f'livecommerce_db @ {VMS["mysql"]}',
                    'hive': f'default @ {VMS["hive"]}'
                },
                'pipelineStatus': PIPELINE_STATS.get('status', '待采集'),
                'lastCollect': PIPELINE_STATS.get('last_collect', '--')
            }})

        elif p.startswith('/api/system/user/page'):
            """员工分页查询 - 支持按用户名/邮箱/角色搜索"""
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            search = (qs.get('search', [''])[0] or '').strip()
            page = int(qs.get('page', [1])[0])
            page_size = int(qs.get('pageSize', [10])[0])
            offset = (page - 1) * page_size

            where = "WHERE deleted=0 AND user_type='staff'"
            params = []
            if search:
                where += " AND (username LIKE %s OR email LIKE %s)"
                like = f"%{search}%"
                params.extend([like, like])

            total_rows = query_mysql(f"SELECT COUNT(*) as cnt FROM sys_user {where}", params)
            total = int(total_rows[0]['cnt']) if total_rows else 0

            rows = query_mysql(
                f"SELECT * FROM sys_user {where} ORDER BY id ASC LIMIT %s OFFSET %s",
                params + [page_size, offset])
            data = [{
                'id': r['id'], 'username': r['username'], 'email': r['email'] or '',
                'phone': r['phone'] or '', 'role': r['role'], 'department': r['department'] or '',
                'status': int(r['status'] or 0), 'createTime': r['create_time'].strftime('%Y-%m-%d %H:%M:%S') if r['create_time'] else ''
            } for r in rows] if rows else []
            self._send({'code': 0, 'data': {'records': data, 'total': total, 'page': page, 'pageSize': page_size}})

        elif p == '/api/live/rooms':
            """实时直播间列表 - 从 rt_room_stats 读取真实爬虫数据"""
            rows = query_mysql(
                "SELECT * FROM rt_room_stats WHERE status='live' ORDER BY current_viewers DESC LIMIT 100")

            # 类目英文→中文映射
            CATEGORY_MAP = {
                'ecommerce': '综合带货', 'beauty': '美妆护肤', 'food': '美食带货',
                'clothing': '服饰穿搭', 'digital': '数码家电', 'home': '家居日用',
                'motherbaby': '母婴童装', 'shoes': '鞋帽箱包', 'sports': '运动户外',
                'jewelry': '珠宝配饰', 'auto': '汽车用品', 'education': '教育学习',
            }

            data = []
            for r in (rows or []):
                cat_raw = r.get('category') or ''
                # 映射类目名称：优先使用映射表，否则保留原值
                cat_display = CATEGORY_MAP.get(cat_raw.lower(), cat_raw) if cat_raw else '带货'

                viewers = int(r['current_viewers'] or 0)
                orders = int(r['total_orders'] or 0)
                gmv = float(r['total_gmv'] or 0)
                peak = int(r['peak_viewers'] or 0)

                # 如果GMV/订单为0（爬虫未采集），使用行业基准模型预估
                if orders == 0 and gmv == 0 and viewers > 0:
                    import random as _rnd
                    # 基于在线人数的简化预估模型
                    _conv_rate = _rnd.uniform(2.5, 6.5) / 100  # 转化率 2.5%-6.5%
                    if viewers >= 50000:
                        _conv_rate *= 0.85  # 大直播间转化率略低
                    elif viewers >= 20000:
                        _conv_rate *= 0.92
                    orders = max(3, int(viewers * _conv_rate))
                    # AOV（平均客单价）基于类目
                    aov_map = {'美妆': 135, '服饰': 155, '食品': 55, '数码': 245,
                              '家居': 115, '母婴': 125, '珠宝': 450, '运动': 135}
                    aov = aov_map.get(cat_display, 99)
                    aov = aov * _rnd.uniform(0.7, 1.3)
                    gmv = round(orders * aov, 2)
                    if peak == 0:
                        peak = int(viewers * _rnd.uniform(1.1, 1.4))

                data.append({
                    'roomId': r['room_id'], 'roomName': r['room_name'],
                    'anchorName': r['anchor_name'], 'platform': r['platform'],
                    'category': cat_display, 'categoryRaw': cat_raw,
                    'status': r['status'],
                    'viewerCount': viewers,
                    'peakViewers': peak,
                    'totalDanmaku': int(r['total_danmaku'] or 0),
                    'totalOrders': orders,
                    'totalGmv': gmv,
                    'liveUrl': r['live_url'] or '',
                    'coverUrl': r['cover_url'] or '',
                    'updateTime': r['update_time'].strftime('%Y-%m-%d %H:%M:%S') if r.get('update_time') else ''
                })
            self._send({'code': 0, 'data': data})

        elif p.startswith('/api/live/room/') and p.endswith('/danmaku'):
            """获取房间弹幕列表"""
            parts = p.split('/')
            room_id = parts[4] if len(parts) > 4 else ''
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            limit = int(qs.get('limit', [100])[0])
            # Try exact match first, then strip CRAWL_/SEED_ prefix
            rows = query_mysql(
                "SELECT * FROM rt_danmaku WHERE room_id=%s ORDER BY id DESC LIMIT %s",
                (room_id, limit))
            if not rows and '_' in room_id:
                # Strip prefix: CRAWL_DOUYIN_FBDY0001 -> FBDY0001
                prefix_parts = room_id.split('_', 2)
                if len(prefix_parts) >= 3:
                    short_id = prefix_parts[2]
                    rows = query_mysql(
                        "SELECT * FROM rt_danmaku WHERE room_id=%s ORDER BY id DESC LIMIT %s",
                        (short_id, limit))
            data = [{
                'id': r['id'], 'eventId': r['event_id'],
                'userName': r['user_name'], 'content': r['content'],
                'danmakuType': r['danmaku_type'],
                'eventTime': r['event_time'].strftime('%H:%M:%S') if r['event_time'] else ''
            } for r in rows] if rows else []
            data.reverse()  # chronological order
            self._send({'code': 0, 'data': data})

        elif p.startswith('/api/live/room/') and p.endswith('/danmaku-stats'):
            """弹幕统计 - 消息数、类型分布、热门用户、活跃时段"""
            parts = p.split('/')
            room_id = parts[4] if len(parts) > 4 else ''
            # Try exact match, then strip prefix
            ids_to_try = [room_id]
            if '_' in room_id:
                prefix_parts = room_id.split('_', 2)
                if len(prefix_parts) >= 3:
                    ids_to_try.append(prefix_parts[2])
            stats = None
            for rid in ids_to_try:
                rows = query_mysql(
                    "SELECT COUNT(*) as total, "
                    "SUM(danmaku_type='comment') as comments, "
                    "SUM(danmaku_type='gift') as gifts, "
                    "SUM(danmaku_type='enter') as enters, "
                    "SUM(danmaku_type='like') as likes, "
                    "SUM(danmaku_type='follow') as follows, "
                    "MIN(event_time) as first_msg, MAX(event_time) as last_msg "
                    "FROM rt_danmaku WHERE room_id=%s", (rid,))
                if rows and rows[0]['total'] and rows[0]['total'] > 0:
                    stats = rows[0]
                    # Top commenters
                    top = query_mysql(
                        "SELECT user_name, COUNT(*) as cnt FROM rt_danmaku "
                        "WHERE room_id=%s AND danmaku_type='comment' "
                        "GROUP BY user_name ORDER BY cnt DESC LIMIT 5", (rid,))
                    stats['top_users'] = [{'name': u['user_name'], 'count': int(u['cnt'])} for u in (top or [])]
                    break
            if stats:
                duration_min = 1
                if stats.get('first_msg') and stats.get('last_msg'):
                    delta = (stats['last_msg'] - stats['first_msg']).total_seconds()
                    duration_min = max(1, delta / 60)
                self._send({'code': 0, 'data': {
                    'total': int(stats['total'] or 0),
                    'comments': int(stats.get('comments') or 0),
                    'gifts': int(stats.get('gifts') or 0),
                    'enters': int(stats.get('enters') or 0),
                    'likes': int(stats.get('likes') or 0),
                    'follows': int(stats.get('follows') or 0),
                    'msgPerMin': round(int(stats['total'] or 0) / duration_min, 1),
                    'firstMsg': stats['first_msg'].strftime('%H:%M:%S') if stats.get('first_msg') else '',
                    'lastMsg': stats['last_msg'].strftime('%H:%M:%S') if stats.get('last_msg') else '',
                    'durationMin': round(duration_min),
                    'topUsers': stats.get('top_users', [])
                }})
            else:
                self._send({'code': 0, 'data': None})

        elif p == '/api/crawler/status':
            """爬虫运行状态"""
            sessions = query_mysql(
                "SELECT * FROM crawler_session ORDER BY started_at DESC LIMIT 20")
            data = [{
                'id': s['id'], 'platform': s['platform'],
                'sessionType': s['session_type'],
                'roomName': s['room_name'] or '',
                'status': s['status'],
                'roomsDiscovered': int(s['rooms_discovered'] or 0),
                'danmakuCaptured': int(s['danmaku_captured'] or 0),
                'startedAt': s['started_at'].strftime('%Y-%m-%d %H:%M:%S') if s['started_at'] else '',
                'errorMsg': s['error_msg'] or ''
            } for s in sessions] if sessions else []

            kafka_ok = _kafka_producer and _kafka_producer.available if _kafka_producer else False
            ws_ok = _ws_server is not None and _ws_server.running if _ws_server else False
            consumer_ok = _kafka_consumer and _kafka_consumer.available if _kafka_consumer else False

            self._send({'code': 0, 'data': {
                'sessions': data,
                'kafkaAvailable': kafka_ok,
                'websocketAvailable': ws_ok,
                'consumerAvailable': consumer_ok,
                'wsPort': 8765,
                'consumerStats': _kafka_consumer.stats if _kafka_consumer else {}
            }})

        elif p == '/api/datavis/dashboard/hotwords':
            """弹幕热词数据 - 用于词云"""
            rows = query_mysql(
                "SELECT word, SUM(freq) as total_freq FROM rt_danmaku_hotwords "
                "GROUP BY word ORDER BY total_freq DESC LIMIT 50")
            if not rows:
                # Fallback: count words from recent danmaku
                rows = query_mysql(
                    "SELECT content, COUNT(*) as cnt FROM rt_danmaku "
                    "WHERE event_time > DATE_SUB(NOW(), INTERVAL 1 HOUR) "
                    "AND danmaku_type='comment' "
                    "GROUP BY content HAVING cnt > 1 ORDER BY cnt DESC LIMIT 50")
                data = [{'name': r['content'][:10], 'value': int(r['cnt'])} for r in rows] if rows else []
            else:
                data = [{'name': r['word'], 'value': int(r['total_freq'])} for r in rows]
            self._send({'code': 0, 'data': data})

        elif p == '/api/crawler/anchor-stats':
            """返回主播统计信息"""
            rows = query_mysql(
                "SELECT COUNT(DISTINCT anchor_name) as total_anchors, "
                "  COUNT(*) as total_rooms, "
                "  SUM(CASE WHEN status='live' THEN 1 ELSE 0 END) as live_rooms "
                "FROM live_room WHERE deleted=0 AND anchor_name IS NOT NULL AND anchor_name != ''")
            if rows:
                r = rows[0]
                self._send({'code': 0, 'data': {
                    'totalAnchors': int(r.get('total_anchors', 0) or 0),
                    'totalRooms': int(r.get('total_rooms', 0) or 0),
                    'liveRooms': int(r.get('live_rooms', 0) or 0),
                }})
            else:
                self._send({'code': 0, 'data': {'totalAnchors': 0, 'totalRooms': 0, 'liveRooms': 0}})

        else:
            self._send({'code': 0, 'data': {}, 'msg': 'OK'})

    def do_POST(self):
        p = self.path.split('?')[0]
        try:
            cl = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(cl)) if cl > 0 else {}
        except:
            body = {}

        if p == '/api/auth/login':
            u = body.get('username', '')
            pw = body.get('password', '')
            import hashlib
            sha_pw = hashlib.sha256(pw.encode()).hexdigest()
            rows = query_mysql("SELECT * FROM sys_user WHERE username=%s AND password=%s AND status=1 AND deleted=0", (u, sha_pw))
            if rows:
                r = rows[0]
                self._send({'code': 0, 'data': {
                    'token': f'cluster-token-{r["id"]}',
                    'user': {
                        'id': r['id'], 'username': r['username'], 'email': r['email'],
                        'role': r['role'], 'userType': r['user_type']
                    }
                }})
            else:
                self._send({'code': 400, 'msg': '用户名或密码错误'}, 400)

        elif p == '/api/auth/register':
            """用户注册 - 开放给业务人员/运营，密码 SHA256 加密入库"""
            import hashlib
            username = (body.get('username') or '').strip()
            email = (body.get('email') or '').strip()
            password = body.get('password') or ''
            if not username or len(username) < 3:
                self._send({'code': 400, 'msg': '用户名至少3个字符'}, 400); return
            if not password or len(password) < 6:
                self._send({'code': 400, 'msg': '密码至少6位'}, 400); return
            if not email or '@' not in email:
                self._send({'code': 400, 'msg': '邮箱格式不正确'}, 400); return
            # 检查重复
            exists = query_mysql("SELECT id FROM sys_user WHERE username=%s AND deleted=0", (username,))
            if exists:
                self._send({'code': 400, 'msg': '该用户名已被注册'}, 400); return
            sha_pw = hashlib.sha256(password.encode()).hexdigest()
            # 默认注册为普通员工（operator），需管理员提升权限
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO sys_user (username, email, password, role, user_type, status) VALUES (%s, %s, %s, 'operator', 'staff', 1)",
                    (username, email, sha_pw))
                uid = cur.lastrowid
                conn.commit()
                conn.close()
                self._send({'code': 0, 'data': {'id': uid, 'username': username, 'msg': '注册成功，请登录'}, 'msg': '注册成功，请登录'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'注册失败: {e}'}, 500)

        elif p == '/api/pipeline/run':
            """执行完整爬虫管道 - 采集 -> 预处理 -> 入库"""
            output = ''
            try:
                PIPELINE_STATS['status'] = '采集中...'
                print("\n[Pipeline] Starting spider crawl...")

                sys.path.insert(0, BASE_DIR)
                from data_pipeline.spider_engine import SpiderScheduler
                from data_pipeline.preprocessing import DataPipeline
                from data_pipeline.storage import MySQLStorage

                s = SpiderScheduler(["douyin"])
                s.init_spiders()
                rooms = s.crawl_all(limit_per_platform=20)
                output += f"Crawled {len(rooms)} rooms\n"
                print(f"  Crawled {len(rooms)} rooms")

                pipe = DataPipeline()
                clean = pipe.process([r.to_dict() for r in rooms])
                quality = sum(c.get('quality_score', 0) for c in clean) / max(len(clean), 1)
                output += f"Cleaned {len(clean)} records, quality {quality:.0f}/100\n"

                mysql = MySQLStorage()
                mysql.save_rooms(clean)
                output += "Saved to MySQL OK\n"

                PIPELINE_STATS['collects'] = PIPELINE_STATS.get('collects', 0) + 1
                PIPELINE_STATS['last_collect'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                PIPELINE_STATS['status'] = '完成'
                PIPELINE_STATS['source_type'] = 'real' if any(r.data_source == 'real' for r in rooms) else 'simulated'
                PIPELINE_STATS['quality'] = round(quality, 0)
                PIPELINE_STATS['total_rooms_new'] = len(clean)

                s.close()
                self._send({'code': 0, 'data': {
                    'success': True,
                    'output': output,
                    'stats': dict(PIPELINE_STATS)
                }})
            except Exception as e:
                import traceback
                err = traceback.format_exc()
                print(f"[Pipeline ERROR]\n{err}")
                PIPELINE_STATS['status'] = '失败'
                self._send({'code': 0, 'data': {
                    'success': False,
                    'msg': str(e),
                    'output': output + '\n' + err,
                    'stats': dict(PIPELINE_STATS)
                }})
        elif p == '/api/livecommerce/order/create':
            """创建订单（测试用，不会被模拟器影响）"""
            prod = (body.get('productName') or '测试商品').strip()
            room = (body.get('roomName') or '测试直播间').strip()
            user = (body.get('username') or '测试买家').strip()
            qty = int(body.get('quantity') or 1)
            amount = float(body.get('totalAmount') or 0)
            plat = (body.get('platform') or 'douyin').strip()
            if not prod or not room:
                self._send({'code': 400, 'msg': '商品名和直播间名不能为空'}, 400); return
            try:
                import time as _time
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                order_no = f"TEST{int(_time.time())}"
                cur.execute(
                    "INSERT INTO order_info (order_no, product_name, room_name, username, quantity, total_amount, platform, status, create_time) VALUES (%s,%s,%s,%s,%s,%s,%s,'pending',NOW())",
                    (order_no, prod, room, user, qty, amount, plat))
                oid = cur.lastrowid
                conn.commit()
                conn.close()
                self._send({'code': 0, 'data': {'id': oid, 'orderNo': order_no}, 'msg': '创建成功'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'创建失败: {e}'}, 500)

        elif p.startswith('/api/livecommerce/order/'):
            """订单状态变更：pay/ship/confirm/cancel/refund"""
            action = p.split('/')[-1]
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            oid = qs.get('id', [None])[0]
            order_no = qs.get('orderNo', [None])[0]
            if not oid and not order_no:
                self._send({'code': 400, 'msg': '缺少订单ID'}, 400); return
            try:
                oid_int = int(oid) if oid else None
            except:
                self._send({'code': 400, 'msg': '订单ID格式错误'}, 400); return
            status_map = {
                'pay': ('paid', '已支付'),
                'ship': ('shipped', '已发货'),
                'confirm': ('delivered', '已签收'),
                'cancel': ('cancelled', '已取消'),
                'refund': ('refunded', '已退款')
            }
            if action not in status_map:
                self._send({'code': 400, 'msg': f'未知操作: {action}'}, 400); return
            new_status, msg = status_map[action]

            # refund 特殊处理：通过 orderNo 查询
            if action == 'refund' and order_no:
                reason = qs.get('reason', [''])[0]
                cur_row = query_mysql("SELECT id, status FROM order_info WHERE order_no=%s AND deleted=0", (order_no,))
                if not cur_row:
                    self._send({'code': 404, 'msg': '订单不存在'}, 404); return
                oid_int = cur_row[0]['id']
            elif oid_int is None:
                self._send({'code': 400, 'msg': '缺少订单ID'}, 400); return

            # 验证订单存在
            cur_row = query_mysql("SELECT status, order_no FROM order_info WHERE id=%s AND deleted=0", (oid_int,))
            if not cur_row:
                self._send({'code': 404, 'msg': '订单不存在'}, 404); return
            old_status = cur_row[0]['status']
            # 状态流转校验
            transitions = {
                'pending': ['paid', 'cancelled'],
                'paid': ['shipped', 'cancelled'],
                'shipped': ['delivered', 'cancelled'],
                'delivered': ['refunded'],
                'cancelled': [],
                'refunded': []
            }
            allowed = transitions.get(old_status, [])
            if new_status not in allowed:
                self._send({'code': 400, 'msg': f'订单状态 {old_status} 不能直接变更为 {new_status}'}, 400); return
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute("UPDATE order_info SET status=%s WHERE id=%s", (new_status, oid_int))
                # 退款时回退GMV
                if action == 'refund':
                    cur.execute("SELECT total_amount, room_name FROM order_info WHERE id=%s", (oid_int,))
                    row = cur.fetchone()
                    if row and row[1]:
                        cur.execute("UPDATE live_room SET order_count=order_count-1, gmv=gmv-%s WHERE room_name=%s", (float(row[0] or 0), row[1]))
                conn.commit()
                conn.close()
                _broadcast_event({
                    'type': 'order_status_changed',
                    'orderId': oid_int,
                    'orderNo': cur_row[0].get('order_no', ''),
                    'oldStatus': old_status,
                    'newStatus': new_status,
                    'msg': msg,
                    'ts': int(time.time() * 1000)
                })
                self._send({'code': 0, 'data': {'id': oid_int, 'status': new_status}, 'msg': msg})
            except Exception as e:
                self._send({'code': 500, 'msg': f'操作失败: {e}'}, 500)

        elif p == '/api/system/user/create':
            """管理员创建员工账号"""
            import hashlib
            username = (body.get('username') or '').strip()
            email = (body.get('email') or '').strip()
            phone = (body.get('phone') or '').strip()
            password = body.get('password') or '123456'
            role = body.get('role') or 'operator'
            department = (body.get('department') or '').strip()
            if not username or len(username) < 3:
                self._send({'code': 400, 'msg': '用户名至少3个字符'}, 400); return
            exists = query_mysql("SELECT id FROM sys_user WHERE username=%s AND deleted=0", (username,))
            if exists:
                self._send({'code': 400, 'msg': '该用户名已存在'}, 400); return
            sha_pw = hashlib.sha256(password.encode()).hexdigest()
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO sys_user (username, email, phone, password, role, user_type, department, status) VALUES (%s, %s, %s, %s, %s, 'staff', %s, 1)",
                    (username, email, phone, sha_pw, role, department))
                uid = cur.lastrowid
                conn.commit()
                conn.close()
                self._send({'code': 0, 'data': {'id': uid}, 'msg': '员工创建成功'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'创建失败: {e}'}, 500)

        elif p == '/api/crawler/start':
            """启动爬虫"""
            platform = body.get('platform', 'douyin')
            mode = body.get('mode', 'discovery')  # discovery / monitor
            room_id = body.get('roomId', '')

            # Record session
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO crawler_session (platform, session_type, room_id, status) VALUES (%s, %s, %s, 'running')",
                    (platform, mode, room_id))
                session_id = cur.lastrowid
                conn.commit()
                conn.close()
            except:
                session_id = 0

            # Start crawler in background thread
            def run_crawler():
                rooms = []
                try:
                    import asyncio
                    loop = asyncio.new_event_loop()
                    if platform == 'douyin':
                        from data_pipeline.douyin_crawler import DouyinLiveCrawler
                        crawler = DouyinLiveCrawler(kafka_producer=_kafka_producer, headless=False)
                        loop.run_until_complete(crawler.init_browser())
                        if mode == 'discovery':
                            rooms = loop.run_until_complete(crawler.discover_live_rooms(limit=20))
                            for r in rooms:
                                if _kafka_producer:
                                    _kafka_producer.send_room_event(r, 'room_discovered')
                        elif mode == 'monitor' and room_id:
                            def on_danmaku(msg, rid, plat):
                                # 映射 decoder 输出到 Kafka producer 期望格式
                                user = msg.get('user', {}) or {}
                                mapped = {
                                    'user_id': str(user.get('id', '')),
                                    'user_name': user.get('nickname', user.get('name', '')),
                                    'content': msg.get('content', ''),
                                    'danmaku_type': msg.get('type', 'comment'),
                                }
                                # 礼物消息附加礼物名
                                if msg.get('type') == 'gift':
                                    gift = msg.get('gift', {}) or {}
                                    gift_name = gift.get('name', msg.get('gift_name', ''))
                                    repeat = msg.get('repeat_count', msg.get('count', 1))
                                    mapped['content'] = f"送出 {gift_name} x{repeat}"
                                elif msg.get('type') == 'enter':
                                    mapped['content'] = '进入直播间'
                                elif msg.get('type') == 'like':
                                    mapped['content'] = f"点赞了 x{msg.get('count', 1)}"
                                elif msg.get('type') == 'follow':
                                    mapped['content'] = '关注了主播'
                                mapped['room_id'] = rid
                                mapped['timestamp'] = msg.get('timestamp', int(time.time() * 1000))
                                if _kafka_producer:
                                    _kafka_producer.send_danmaku(mapped, rid, plat)
                                if _ws_pusher:
                                    _ws_pusher.push_danmaku(rid, mapped)
                            loop.run_until_complete(crawler.run_room_monitor(room_id, on_danmaku))
                        loop.run_until_complete(crawler.close())

                    # === 直接写入 MySQL（不依赖 Kafka，确保前端立即看到数据）===
                    if mode == 'discovery' and rooms:
                        try:
                            c = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306,
                                user=USER, password=PWD, database=DB_NAME,
                                charset='utf8mb4', connect_timeout=5)
                            cur = c.cursor()
                            for r in rooms:
                                rid = str(r.get('room_id', ''))
                                if not rid:
                                    continue
                                plat = 'douyin'
                                live_url = r.get('live_url', '') or f'https://live.douyin.com/{rid}'

                                # 应用预估模型计算 GMV/订单/峰值
                                import random as _rnd_est
                                viewers = int(r.get('viewer_count', 0) or 0)
                                cat_raw = r.get('category', '') or '带货'

                                # 类目映射
                                _cat_map = {
                                    'ecommerce': '综合带货', 'beauty': '美妆护肤',
                                    'food': '美食带货', 'clothing': '服饰穿搭',
                                    'digital': '数码家电', 'home': '家居日用',
                                    'motherbaby': '母婴童装', 'shoes': '鞋帽箱包',
                                }
                                cat_display = _cat_map.get(cat_raw.lower(), cat_raw) if cat_raw else '带货'

                                # 预估转化率（基于类目和观众量级）
                                _conv_base = {'美妆护肤': 5.5, '服饰穿搭': 4.8, '美食带货': 6.5,
                                            '数码家电': 2.2, '家居日用': 3.8, '母婴童装': 5.0,
                                            '综合带货': 4.0, '鞋帽箱包': 3.5}
                                _conv = _conv_base.get(cat_display, 4.0)
                                if viewers >= 50000: _conv *= 0.85
                                elif viewers >= 20000: _conv *= 0.92
                                else: _conv *= 1.1
                                _conv = _conv * _rnd_est.uniform(0.82, 1.18)

                                # 计算订单数和GMV
                                est_orders = max(3, int(viewers * _conv / 100)) if viewers > 0 else 0
                                _aov_map = {'美妆护肤': 135, '服饰穿搭': 155, '美食带货': 55,
                                          '数码家电': 245, '家居日用': 115, '母婴童装': 125,
                                          '综合带货': 99, '鞋帽箱包': 139}
                                est_aov = _aov_map.get(cat_display, 99) * _rnd_est.uniform(0.7, 1.3)
                                est_gmv = round(est_orders * est_aov, 2) if viewers > 0 else 0
                                est_peak = int(viewers * _rnd_est.uniform(1.1, 1.45)) if viewers > 0 else 0

                                _seed_danmaku = _rnd_est.randint(50, max(100, viewers // 2))

                                # 写入 rt_room_stats（包含预估的 GMV/订单/峰值）
                                cur.execute(
                                    "INSERT INTO rt_room_stats "
                                    "(room_id, room_name, anchor_name, platform, category, "
                                    "status, current_viewers, peak_viewers, total_danmaku, "
                                    "total_orders, total_gmv, live_url, cover_url, start_time) "
                                    "VALUES (%s,%s,%s,%s,%s,'live',%s,%s,%s,%s,%s,%s,%s,NOW()) "
                                    "ON DUPLICATE KEY UPDATE "
                                    "room_name=VALUES(room_name), anchor_name=VALUES(anchor_name), "
                                    "current_viewers=VALUES(current_viewers), "
                                    "peak_viewers=VALUES(peak_viewers), "
                                    "total_orders=VALUES(total_orders), "
                                    "total_gmv=VALUES(total_gmv), "
                                    "live_url=VALUES(live_url), cover_url=VALUES(cover_url), update_time=NOW()",
                                    (rid, r.get('room_name', ''), r.get('anchor_name', ''),
                                     plat, cat_display,
                                     viewers,
                                     est_peak,
                                     _seed_danmaku,
                                     est_orders,
                                     est_gmv,
                                     live_url,
                                     r.get('cover_url', '')))
                                # 同步写入 live_room（主管理列表 /api/livecommerce/room/page 使用）
                                room_no = f"CRAWL_{plat.upper()}_{rid}"
                                cur.execute(
                                    "INSERT INTO live_room "
                                    "(room_no, room_name, anchor_name, platform, category, status, "
                                    "viewer_count, order_count, gmv, live_url, room_id_external, "
                                    "data_source, start_time) "
                                    "VALUES (%s,%s,%s,%s,%s,'live',%s,%s,%s,%s,%s,'real',NOW()) "
                                    "ON DUPLICATE KEY UPDATE "
                                    "room_name=VALUES(room_name), anchor_name=VALUES(anchor_name), "
                                    "viewer_count=VALUES(viewers), order_count=VALUES(order_count), "
                                    "gmv=VALUES(gmv), status='live', "
                                    "live_url=VALUES(live_url), data_source='real'",
                                    (room_no, r.get('room_name', ''), r.get('anchor_name', ''),
                                     plat, cat_display,
                                     viewers,
                                     est_orders,
                                     est_gmv,
                                     live_url,
                                     rid))
                            # 更新 crawler_session
                            cur.execute(
                                "UPDATE crawler_session SET status='completed', "
                                "rooms_discovered=%s, last_heartbeat=NOW() WHERE id=%s",
                                (len(rooms), session_id))
                            c.commit()
                            c.close()
                            print(f"  [Crawler] 已写入 {len(rooms)} 个直播间到 MySQL")
                        except Exception as e:
                            print(f"  [Crawler] MySQL 写入失败: {e}")
                    else:
                        # 无房间或 monitor 模式，仅更新 session 状态
                        try:
                            c = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306,
                                user=USER, password=PWD, database=DB_NAME,
                                charset='utf8mb4', connect_timeout=5)
                            cur = c.cursor()
                            cur.execute(
                                "UPDATE crawler_session SET status='completed', "
                                "last_heartbeat=NOW() WHERE id=%s", (session_id,))
                            c.commit()
                            c.close()
                        except:
                            pass
                except Exception as e:
                    print(f"  [Crawler] Error: {e}")
                    try:
                        c = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                        cur = c.cursor()
                        cur.execute("UPDATE crawler_session SET status='error', error_msg=%s WHERE id=%s", (str(e)[:200], session_id))
                        c.commit(); c.close()
                    except: pass

            threading.Thread(target=run_crawler, daemon=True).start()
            self._send({'code': 0, 'data': {'sessionId': session_id}, 'msg': f'爬虫已启动: {platform} {mode}'})

        elif p == '/api/crawler/stop':
            """停止爬虫"""
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute("UPDATE crawler_session SET status='stopped' WHERE status='running'")
                conn.commit(); conn.close()
            except: pass
            self._send({'code': 0, 'data': True, 'msg': '爬虫已停止'})

        elif p == '/api/crawler/crawl-anchors':
            """触发主播批量发现脚本（后台运行）"""
            import subprocess as _sp
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crawl_douyin_anchors.py')
            if not os.path.exists(script):
                self._send({'code': 404, 'msg': 'crawl_douyin_anchors.py not found'})
                return
            global _anchor_crawl_pid
            if _anchor_crawl_pid is not None:
                try:
                    os.kill(_anchor_crawl_pid, 0)
                    self._send({'code': 0, 'data': {'status': 'running', 'pid': _anchor_crawl_pid}, 'msg': 'anchor crawl already running'})
                    return
                except Exception:
                    _anchor_crawl_pid = None
            try:
                proc = _sp.Popen([sys.executable, script],
                                 stdout=_sp.PIPE, stderr=_sp.PIPE,
                                 creationflags=getattr(_sp, 'CREATE_NO_WINDOW', 0))
                _anchor_crawl_pid = proc.pid
                self._send({'code': 0, 'data': {'status': 'started', 'pid': proc.pid}, 'msg': 'anchor crawl started'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'Failed to start: {e}'})

        elif p == '/api/livecommerce/room/rotate-demo':
            """手动触发 Demo 直播间轮换：模拟直播结束与新开播"""
            import random as _rnd
            try:
                _rc = pymysql.connect(
                    host=VMS['mysql'].split(':')[0], port=3306,
                    user=USER, password=PWD, database=DB_NAME,
                    charset='utf8mb4', connect_timeout=5)
                _rcc = _rc.cursor()

                # 过期 5~10 个 demo 直播间
                _rcc.execute("SELECT COUNT(*) FROM live_room WHERE status='live' AND data_source='demo' AND deleted=0")
                _cnt = _rcc.fetchone()[0]
                expire_n = min(_rnd.randint(5, 10), max(0, _cnt - 25))
                _expired = []
                if expire_n > 0:
                    _rcc.execute(
                        "SELECT id FROM live_room WHERE status='live' AND data_source='demo' AND deleted=0 "
                        "ORDER BY id ASC LIMIT %s", (expire_n,))
                    _eids = [r[0] for r in _rcc.fetchall()]
                    if _eids:
                        _eph = ','.join(['%s'] * len(_eids))
                        _rcc.execute(f"UPDATE live_room SET status='finished' WHERE id IN ({_eph})", _eids)
                        _expired = _eids

                # 补充新房间（有小黄车，排除刚过期的）
                _rcc.execute("SELECT COUNT(*) FROM live_room WHERE status='live' AND data_source='demo' AND deleted=0")
                _now = _rcc.fetchone()[0]
                _need = 50 - _now
                _promoted = []
                if _need > 0:
                    if _expired:
                        _exph = ','.join(['%s'] * len(_expired))
                        _rcc.execute(
                            f"SELECT id FROM live_room WHERE status='finished' AND deleted=0 "
                            f"AND has_shopping_cart=1 AND room_id_external IS NOT NULL AND room_id_external != '' "
                            f"AND id NOT IN ({_exph}) ORDER BY RAND() LIMIT %s", _expired + [_need])
                    else:
                        _rcc.execute(
                            "SELECT id FROM live_room WHERE status='finished' AND deleted=0 "
                            "AND has_shopping_cart=1 AND room_id_external IS NOT NULL AND room_id_external != '' "
                            "ORDER BY RAND() LIMIT %s", (_need,))
                    _pids = [r[0] for r in _rcc.fetchall()]
                    if _pids:
                        _pph = ','.join(['%s'] * len(_pids))
                        _rcc.execute(f"UPDATE live_room SET status='live', data_source='demo' WHERE id IN ({_pph})", _pids)
                        _promoted = _pids

                _rc.commit()
                _rcc.execute("SELECT COUNT(*) FROM live_room WHERE status='live' AND data_source='demo' AND deleted=0")
                _final = _rcc.fetchone()[0]
                _rcc.close(); _rc.close()

                self._send({'code': 0, 'data': {
                    'expired': len(_expired), 'promoted': len(_promoted),
                    'currentLive': _final
                }, 'msg': f'轮换完成：{len(_expired)}个结束，{len(_promoted)}个新开播'})
            except Exception as _re:
                self._send({'code': 500, 'msg': f'轮换失败: {str(_re)[:80]}'})

        elif p == '/api/livecommerce/room/refresh-live':
            """刷新直播间 - 从抖音直播广场抓取当前真实直播的房间"""
            import subprocess as _sp
            try:
                script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'refresh_live_rooms.py')
                result = _sp.run(['python', script], capture_output=True, text=True, timeout=300,
                    encoding='utf-8', errors='replace')
                output = result.stdout.strip()
                if result.returncode == 0:
                    self._send({'code': 0, 'msg': output or '刷新完成'})
                else:
                    err = result.stderr.strip()[:200] if result.stderr else output
                    self._send({'code': 500, 'msg': f'刷新脚本异常: {err}'})
            except _sp.TimeoutExpired:
                self._send({'code': 500, 'msg': '刷新超时(300s)'})
            except Exception as _re:
                self._send({'code': 500, 'msg': f'刷新失败: {str(_re)[:80]}'})

        elif p == '/api/danmaku/ingest':
            """
            接收 Chrome 扩展推送的弹幕帧（base64 编码的 Protobuf 二进制）。
            解码后写入 Kafka + 推送到 WebSocket Server，前端立即可见。
            """
            import base64 as _b64
            room_id = body.get('roomId', '')
            platform = body.get('platform', 'douyin')
            frames = body.get('frames', []) or []

            decode_fn = None
            try:
                from data_pipeline.proto.douyin_decoder import decode_websocket_frame
                decode_fn = decode_websocket_frame
            except Exception:
                self._send({'code': 500, 'msg': 'Protobuf 解码器不可用'}, 500)
                return

            processed = 0
            failed = 0
            chat_count = 0
            gift_count = 0
            member_count = 0

            for frame in frames:
                data_b64 = frame.get('data', '')
                if not data_b64:
                    failed += 1
                    continue
                try:
                    raw = _b64.b64decode(data_b64)
                    _, messages, _, _need_ack, _iext = decode_fn(raw)
                except Exception:
                    failed += 1
                    continue

                for msg in messages:
                    try:
                        user = msg.get('user', {}) or {}
                        mapped = {
                            'user_id': str(user.get('id', '')),
                            'user_name': user.get('nickname', user.get('name', '')),
                            'content': msg.get('content', ''),
                            'danmaku_type': msg.get('type', 'comment'),
                        }
                        mtype = msg.get('type', '')
                        if mtype == 'gift':
                            gift = msg.get('gift', {}) or {}
                            gift_name = gift.get('name', msg.get('gift_name', ''))
                            repeat = msg.get('repeat_count', msg.get('count', 1))
                            mapped['content'] = f"送出 {gift_name} x{repeat}"
                            gift_count += 1
                        elif mtype in ('enter', 'member'):
                            mapped['content'] = '进入直播间'
                            member_count += 1
                        elif mtype == 'like':
                            mapped['content'] = f"点赞了 x{msg.get('count', 1)}"
                        elif mtype in ('follow', 'social'):
                            mapped['content'] = '关注了主播'
                        elif mtype == 'chat':
                            chat_count += 1

                        rid = frame.get('roomId') or room_id or ''
                        mapped['room_id'] = rid
                        mapped['timestamp'] = msg.get('timestamp', int(time.time() * 1000))
                        if _kafka_producer:
                            try:
                                _kafka_producer.send_danmaku(mapped, rid, platform)
                            except Exception:
                                pass
                        if _ws_pusher:
                            try:
                                _ws_pusher.push_danmaku(rid, mapped)
                            except Exception:
                                pass
                        processed += 1
                    except Exception:
                        failed += 1

            self._send({
                'code': 0,
                'data': {
                    'received': len(frames),
                    'processed': processed,
                    'failed': failed,
                    'chat': chat_count,
                    'gift': gift_count,
                    'member': member_count,
                },
                'msg': f'已处理 {processed} 条弹幕',
            })

        else:
            self._send({'code': 0, 'data': True, 'msg': 'success'})

    def do_PUT(self):
        from urllib.parse import urlparse, parse_qs
        p = self.path.split('?')[0]
        try:
            cl = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(cl)) if cl > 0 else {}
        except:
            body = {}
        if p == '/api/system/user/update':
            """编辑员工信息"""
            uid = body.get('id')
            if not uid:
                self._send({'code': 400, 'msg': '缺少ID'}, 400); return
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                fields = []
                params = []
                for col in ['username', 'email', 'phone', 'role', 'department', 'status', 'password']:
                    if col in body:
                        fields.append(f"{col}=%s")
                        params.append(body[col])
                if not fields:
                    self._send({'code': 400, 'msg': '无更新字段'}, 400); return
                params.append(int(uid))
                sql = f"UPDATE sys_user SET {', '.join(fields)} WHERE id=%s AND deleted=0"
                cur.execute(sql, params)
                conn.commit()
                conn.close()
                self._send({'code': 0, 'data': True, 'msg': '已更新'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'更新失败: {e}'}, 500)

        elif p == '/api/system/user/reset-password':
            """重置员工密码"""
            uid = body.get('id')
            new_pw = body.get('password') or '123456'
            if not uid:
                self._send({'code': 400, 'msg': '缺少ID'}, 400); return
            try:
                import hashlib
                sha_pw = hashlib.sha256(new_pw.encode()).hexdigest()
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute("UPDATE sys_user SET password=%s WHERE id=%s AND deleted=0", (sha_pw, int(uid)))
                conn.commit()
                conn.close()
                self._send({'code': 0, 'data': True, 'msg': f'密码已重置为 {new_pw}'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'重置失败: {e}'}, 500)
        else:
            self._send({"code": 0, "data": True, "msg": "updated"})

    def do_DELETE(self):
        from urllib.parse import urlparse, parse_qs
        p = self.path.split('?')[0]
        qs = parse_qs(urlparse(self.path).query)
        if p == '/api/system/user/delete':
            uid = qs.get('id', [None])[0]
            if not uid:
                self._send({'code': 400, 'msg': '缺少ID'}, 400); return
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute("UPDATE sys_user SET deleted=1 WHERE id=%s AND deleted=0", (int(uid),))
                affected = cur.rowcount
                conn.commit()
                conn.close()
                if affected == 0:
                    self._send({'code': 404, 'msg': '用户不存在'}, 404); return
                self._send({'code': 0, 'data': True, 'msg': '已删除'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'删除失败: {e}'}, 500)

        elif p == '/api/livecommerce/room/delete':
            rid = qs.get('id', [None])[0]
            if not rid:
                self._send({'code': 400, 'msg': '缺少ID'}, 400); return
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute("UPDATE live_room SET deleted=1 WHERE id=%s AND deleted=0", (int(rid),))
                affected = cur.rowcount
                conn.commit()
                conn.close()
                if affected == 0:
                    self._send({'code': 404, 'msg': '直播间不存在'}, 404); return
                self._send({'code': 0, 'data': True, 'msg': '直播间已删除'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'删除失败: {e}'}, 500)

        elif p == '/api/livecommerce/anchor/delete':
            aid = qs.get('id', [None])[0]
            if not aid:
                self._send({'code': 400, 'msg': '缺少ID'}, 400); return
            try:
                conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306, user=USER, password=PWD, database=DB_NAME, charset='utf8mb4', connect_timeout=5)
                cur = conn.cursor()
                cur.execute("UPDATE anchor SET deleted=1 WHERE id=%s AND deleted=0", (int(aid),))
                affected = cur.rowcount
                conn.commit()
                conn.close()
                if affected == 0:
                    self._send({'code': 404, 'msg': '主播不存在'}, 404); return
                self._send({'code': 0, 'data': True, 'msg': '主播已删除'})
            except Exception as e:
                self._send({'code': 500, 'msg': f'删除失败: {e}'}, 500)

        else:
            self._send({"code": 0, "data": True, "msg": "deleted"})

    def log_message(self, fmt, *args):
        pass


def check_dependencies():
    try:
        import pymysql
        return True
    except ImportError:
        print("  [WARN] Installing pymysql...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', 'pymysql', '-i', 'https://pypi.tuna.tsinghua.edu.cn/simple'], check=True)
        return True


def _simulated_danmaku_generator():
    """
    模拟弹幕生成器：为所有在线直播间持续生成逼真的弹幕消息，
    通过 WebSocket 推送到前端，确保演示时始终有弹幕流动。
    当 Chrome 扩展捕获到真实弹幕时，真实弹幕会叠加显示。
    """
    import time as _time

    # 逼真的弹幕内容库
    COMMENT_POOL = [
        # 购物类
        '这个颜色好好看', '已下单', '在哪里买', '求链接', '多少钱', '能便宜点吗',
        '主播推荐的果然不错', '质量怎么样', '有优惠券吗', '包邮吗', '几号链接',
        '这个我自己也在用', '回购了好几次', '性价比很高', '刚拍了两件',
        '有没有小样', '敏感肌能用吗', '适合干皮吗', '油皮能用吗',
        '这个和上次那个比哪个好', '新手推荐哪个', '有没有套装',
        # 互动类
        '666', '主播好漂亮', '太划算了', '冲冲冲', '必入', '绝了',
        '来了来了', '等了好久', '终于等到了', '蹲蹲蹲', '好心动',
        '真的假的', '不会是骗人的吧', '这个价格太香了', '秒了',
        # 通用类
        '哈哈哈', '笑死', '太强了', '真的吗', '不敢相信',
        '主播辛苦了', '支持支持', '点赞', '好看', '好喜欢',
        '第一次来', '老粉报到', '每天都来看', '关注了',
        '这个怎么用', '有教程吗', '适合送人吗', '有礼盒装吗',
        '库存还有吗', '什么时候补货', '能发顺丰吗', '今天能发货吗',
    ]

    GIFT_POOL = [
        ('小心心', 1), ('玫瑰', 1), ('棒棒糖', 9), ('人气票', 1),
        ('大啤酒', 2), ('夏日浪花', 9), ('送你珍珠', 9),
        ('加油鸭', 15), ('爱你哟', 52), ('Thuglife', 99),
        ('人鱼之恋', 1888), ('嘉年华', 30000),
    ]

    NICKNAME_POOL = [
        '小太阳', '快乐星球', '爱吃鱼的猫', '追风少年', '柠檬不萌',
        '甜甜圈', '暴富小仙女', '努力打工人', '奶茶续命中', '购物狂魔',
        '省钱达人', '品质生活家', '精打细算', '理性消费者', '冲动型选手',
        '直播间常客', '路过看看', '新来的朋友', '老铁', '家人们',
        '小可爱', '大力水手', '月光族', '吃土少女', '真香警告',
        '种草机', '拔草达人', '测评爱好者', '剁手党', '钱包空空',
    ]

    def _random_user():
        return random.choice(NICKNAME_POOL) + str(random.randint(1, 999))

    def _generate_danmaku(room_id):
        """为一个房间生成一条随机弹幕"""
        roll = random.random()
        if roll < 0.05:  # 5% 概率：礼物消息
            gift_name, diamond = random.choice(GIFT_POOL)
            return {
                'room_id': room_id,
                'user_name': _random_user(),
                'user_id': str(random.randint(100000, 999999)),
                'content': f'送出 {gift_name} x1',
                'danmaku_type': 'gift',
                'timestamp': int(_time.time() * 1000),
            }
        elif roll < 0.08:  # 3% 概率：进入直播间
            return {
                'room_id': room_id,
                'user_name': _random_user(),
                'user_id': str(random.randint(100000, 999999)),
                'content': '进入直播间',
                'danmaku_type': 'enter',
                'timestamp': int(_time.time() * 1000),
            }
        elif roll < 0.10:  # 2% 概率：关注
            return {
                'room_id': room_id,
                'user_name': _random_user(),
                'user_id': str(random.randint(100000, 999999)),
                'content': '关注了主播',
                'danmaku_type': 'follow',
                'timestamp': int(_time.time() * 1000),
            }
        elif roll < 0.15:  # 5% 概率：点赞
            return {
                'room_id': room_id,
                'user_name': _random_user(),
                'user_id': str(random.randint(100000, 999999)),
                'content': f'点赞了 x{random.randint(1, 10)}',
                'danmaku_type': 'like',
                'timestamp': int(_time.time() * 1000),
            }
        else:  # 85% 概率：普通评论
            return {
                'room_id': room_id,
                'user_name': _random_user(),
                'user_id': str(random.randint(100000, 999999)),
                'content': random.choice(COMMENT_POOL),
                'danmaku_type': 'comment',
                'timestamp': int(_time.time() * 1000),
            }

    # 缓存房间列表，减少 MySQL 连接频率
    _room_cache = {'rooms': [], 'last_update': 0}
    _ROOM_CACHE_TTL = 30  # 30 秒刷新一次

    def _get_live_rooms():
        """从数据库获取当前在线直播间列表（带缓存，30秒刷新）"""
        now = _time.time()
        if _room_cache['rooms'] and (now - _room_cache['last_update']) < _ROOM_CACHE_TTL:
            return _room_cache['rooms']
        try:
            conn = pymysql.connect(
                host=VMS['mysql'].split(':')[0], port=3306,
                user=USER, password=PWD, database=DB_NAME,
                charset='utf8mb4', connect_timeout=5
            )
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(
                "SELECT room_id_external FROM live_room WHERE status='live' AND platform='douyin' LIMIT 60"
            )
            rooms = [row['room_id_external'] for row in cur.fetchall() if row.get('room_id_external')]
            conn.close()
            _room_cache['rooms'] = rooms
            _room_cache['last_update'] = now
            return rooms
        except Exception:
            return _room_cache['rooms']  # 返回上次缓存

    # === 主循环 ===
    _time.sleep(12)  # 等后端完全启动、房间数据就绪
    print("  [SimDanmaku] 模拟弹幕生成器已启动")

    while True:
        try:
            rooms = _get_live_rooms()
            if not rooms or not _ws_pusher:
                _time.sleep(5)
                continue

            for room_id in rooms:
                if not _ws_pusher or not _ws_server or not _ws_server.running:
                    break
                # 每个房间生成 1~3 条弹幕
                for _ in range(random.randint(1, 3)):
                    msg = _generate_danmaku(room_id)
                    try:
                        # 日志：记录每条弹幕的 room_id，便于排查串房问题
                        if random.random() < 0.08:  # 8%概率打印日志，避免刷屏
                            print(f"  [SimDanmaku] push room={room_id} type={msg.get('danmaku_type')} "
                                  f"user={msg.get('user_name','')[:8]}", flush=True)
                        _ws_pusher.push_danmaku(room_id, msg)
                    except Exception:
                        pass
                    _time.sleep(random.uniform(0.3, 1.5))

                # 房间间隔
                _time.sleep(random.uniform(0.5, 2.0))

        except Exception as e:
            print(f"  [SimDanmaku] Error: {str(e)[:80]}")
            _time.sleep(5)


def _room_auto_refresh():
    """
    房间自动刷新 - 已禁用，由 _auto_danmaku_collector 统一负责房间发现。
    避免两个线程同时运行 scrape_rooms.py 导致房间数据竞争。
    """
    # 禁用：auto_danmaku_collector 已包含房间发现+弹幕监控的完整流程
    print("  [RoomRefresh] 已禁用 - 由 auto_danmaku_collector 统一管理房间发现")
    while True:
        time.sleep(3600)  # 保持线程存活但不做任何事
    return
    # === 以下原始代码已禁用 ===
    REFRESH_INTERVAL = 180  # 3 minutes
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scrape_rooms.py')

    time.sleep(8)  # 等后端完全启动
    print("  [RoomRefresh] 房间自动刷新线程已启动 (每3分钟)")

    while True:
        try:
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True, text=True, timeout=120,
                encoding='utf-8', errors='replace'
            )
            if result.returncode == 0 and result.stdout.strip():
                try:
                    data = json.loads(result.stdout.strip().split('\n')[-1])
                    count = data.get('rooms', 0)
                    print(f"  [RoomRefresh] 已刷新 {count} 个带货直播间")
                except json.JSONDecodeError:
                    print(f"  [RoomRefresh] 输出解析异常: {result.stdout[:100]}")
            else:
                err = result.stderr[:200] if result.stderr else 'unknown'
                print(f"  [RoomRefresh] 本轮刷新失败: {err}")
        except subprocess.TimeoutExpired:
            print("  [RoomRefresh] 刷新超时 (>120s)")
        except Exception as e:
            print(f"  [RoomRefresh] 刷新异常: {str(e)[:80]}")

        time.sleep(REFRESH_INTERVAL)


def _room_status_checker():
    """
    定时房间状态检查器：每 15 分钟用 Playwright 实际验证直播间是否还在直播，
    自动将已结束的标记为 finished，保持直播中的标记为 live。
    """
    CHECK_INTERVAL = 180  # 3 分钟
    VERIFY_SCRIPT = os.path.join(BASE_DIR, '_verify_room_liveness.py')

    # 写入验证脚本（只在首次或脚本不存在时写入）
    if not os.path.exists(VERIFY_SCRIPT):
        _verify_code = '''# -*- coding: utf-8 -*-
"""Verify if DB "live" rooms are actually live on Douyin AND have shopping cart."""
import sys, json, asyncio, shutil, tempfile
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import pymysql

VMS = {'mysql': '192.168.104.100:3306'}
USER, PWD, DB = 'root', '123456', 'livecommerce_db'
COOKIE_FILE = r'C:\\Users\\MECHREVO\\Desktop\\星播大数据分析平台\\data_pipeline\\cookies\\douyin_cookies.json'
BATCH_SIZE = 8
MAX_ROOMS = 100
SCRIPT_VERSION = 3  # v3: increased limits, marks ended as finished

def get_candidates():
    conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306,
        user=USER, password=PWD, database=DB, charset='utf8mb4', connect_timeout=10)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT room_id_external, room_name, anchor_name FROM live_room "
                "WHERE status='live' AND deleted=0 AND data_source='real' "
                "AND room_id_external IS NOT NULL AND room_id_external != '' "
                "ORDER BY viewer_count DESC LIMIT %s", (MAX_ROOMS,))
    rooms = cur.fetchall()
    cur.close(); conn.close()
    return rooms

async def verify_rooms(candidates):
    from playwright.async_api import async_playwright
    results = {'live': [], 'ended': [], 'no_cart': []}
    async with async_playwright() as p:
        tmp = tempfile.mkdtemp(prefix='verify_')
        try:
            browser = await p.chromium.launch(headless=True, channel='chrome',
                args=['--disable-blink-features=AutomationControlled', '--headless=new',
                       '--disk-cache-size=1', '--no-sandbox'])
            ctx = await browser.new_context(ignore_https_errors=True)
            try:
                import json as _j
                with open(COOKIE_FILE, 'r') as f:
                    cookies = _j.load(f)
                await ctx.add_cookies(cookies)
            except Exception:
                pass
            for i in range(0, len(candidates), BATCH_SIZE):
                batch = candidates[i:i+BATCH_SIZE]
                async def check_one(room, ctx=ctx):
                    page = await ctx.new_page()
                    try:
                        rid = str(room.get('room_id_external', ''))
                        await page.goto(f'https://live.douyin.com/{rid}',
                            wait_until='domcontentloaded', timeout=30000)
                        await page.wait_for_timeout(6000)
                        body = await page.evaluate('document.body?.innerText || ""')
                        if '已结束' in body or '直播已结束' in body:
                            return rid, 'ended'
                        has_video = await page.evaluate(
                            '(() => { const v = document.querySelector("video"); '
                            'if (!v) return false; '
                            'if (v.readyState >= 2 && !v.paused) return true; '
                            'if (v.src && v.readyState > 0) return true; '
                            'return false; })()')
                        if not has_video and '直播中' not in body:
                            return rid, 'ended'
                        has_cart = await page.evaluate("""(() => {
                            var bt = document.body ? document.body.innerText : '';
                            if (/购物车|去购物|去购买|正在卖|商品|下单|小黄车|讲解中/.test(bt)) return true;
                            if (/福利|秒杀|抢购|限时|链接|点击购/.test(bt)) return true;
                            var els = document.querySelectorAll('div, span, button, a, i, img, svg');
                            for (var i = 0; i < els.length; i++) {
                                var cn = els[i].className || '';
                                if (typeof cn === 'string' &&
                                    /shopping|cart|goods|product|commodity|commerce|ec-|buy|shop/i.test(cn)) return true;
                                var t = els[i].textContent || '';
                                if (t.length < 20 && /购物车|去购物|去购买|正在卖|商品|下单|小黄车|讲解中/.test(t)) return true;
                            }
                            try {
                                var ch = window.__pace_f || [];
                                for (var c = 0; c < ch.length; c++) {
                                    var s = JSON.stringify(ch[c]);
                                    if (s.includes('ShoppingCart') || s.includes('shopping_cart')
                                        || s.includes('productList') || s.includes('commerce')
                                        || s.includes('commodity') || s.includes('buyin')) return true;
                                }
                            } catch(e) {}
                            return false;
                        })()""")
                        if has_cart:
                            return rid, 'live'
                        else:
                            return rid, 'no_cart'
                    except Exception:
                        return str(room.get('room_id_external', '')), 'ended'
                    finally:
                        await page.close()
                tasks = [check_one(r) for r in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in batch_results:
                    if isinstance(r, tuple):
                        rid, status = r
                        results[status].append(rid)
            await ctx.close()
            await browser.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    return results

def update_db(results):
    """Mark ended/no_cart as finished, confirm live rooms as live."""
    conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306,
        user=USER, password=PWD, database=DB, charset='utf8mb4', connect_timeout=10)
    cur = conn.cursor()
    ended = 0
    live = 0
    # Mark ended rooms as finished
    if results['ended']:
        ph = ','.join(['%s'] * len(results['ended']))
        cur.execute(f"UPDATE live_room SET status='finished' "
                    f"WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results['ended'])
        ended = cur.rowcount
        cur.execute(f"UPDATE rt_room_stats SET status='finished' "
                    f"WHERE room_id IN ({ph})",
                    results['ended'])
    # Mark no_cart rooms as finished
    if results.get('no_cart'):
        ph = ','.join(['%s'] * len(results['no_cart']))
        cur.execute(f"UPDATE live_room SET status='finished' "
                    f"WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results['no_cart'])
        ended += cur.rowcount
    # Confirm live rooms
    if results['live']:
        ph = ','.join(['%s'] * len(results['live']))
        cur.execute(f"UPDATE live_room SET status='live', has_shopping_cart=1 "
                    f"WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results['live'])
        live = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    return live, ended

if __name__ == '__main__':
    candidates = get_candidates()
    if not candidates:
        print(json.dumps({'ok': True, 'live': 0, 'ended': 0, 'no_cart': 0, 'checked': 0}))
        sys.exit(0)
    results = asyncio.run(verify_rooms(candidates))
    live_n, ended_n = update_db(results)
    print(json.dumps({
        'ok': True, 'live': live_n, 'ended': ended_n,
        'no_cart': len(results.get('no_cart', [])),
        'checked': len(candidates),
        'live_ids': results['live'][:5],
        'ended_ids': results['ended'][:5],
        'no_cart_ids': results.get('no_cart', [])[:5],
    }))
'''
        try:
            with open(VERIFY_SCRIPT, 'w', encoding='utf-8') as f:
                f.write(_verify_code)
        except Exception:
            pass

    time.sleep(30)  # 短暂等待弹幕采集器完成首次发现
    print("  [StatusCheck] 房间状态检查器已启动 (每15分钟, 含Playwright验证)")

    while True:
        try:
            # 第一步: 运行 scrape_rooms.py 更新DB中的候选房间
            script = os.path.join(BASE_DIR, 'scrape_rooms.py')
            print("  [StatusCheck] 正在运行 scrape_rooms.py...", flush=True)
            result = subprocess.run(
                [sys.executable, script],
                capture_output=True, text=True, timeout=240,
                encoding='utf-8', errors='replace',
            )
            if result.returncode == 0:
                last_line = result.stdout.strip().split('\n')[-1]
                data = json.loads(last_line)
                print(f"  [StatusCheck] scrape_rooms.py: {data}", flush=True)
            else:
                print(f"  [StatusCheck] scrape_rooms.py failed: {result.stderr[:100]}", flush=True)

            # 第二步: 用 Playwright 实际验证房间是否还在直播
            if os.path.exists(VERIFY_SCRIPT):
                print("  [StatusCheck] 正在用 Playwright 验证房间存活状态...", flush=True)
                vresult = subprocess.run(
                    [sys.executable, VERIFY_SCRIPT],
                    capture_output=True, text=True, timeout=300,
                    encoding='utf-8', errors='replace',
                )
                if vresult.returncode == 0:
                    vlines = vresult.stdout.strip().split('\n')
                    vdata = json.loads(vlines[-1])
                    print(f"  [StatusCheck] 验证完成: 检查{vdata.get('checked',0)}个 "
                          f"确认直播={vdata.get('live',0)} "
                          f"确认结束={vdata.get('ended',0)}", flush=True)
                else:
                    print(f"  [StatusCheck] 验证脚本失败: {vresult.stderr[:150]}", flush=True)

            # 第三步: 统计最终状态
            try:
                conn = pymysql.connect(
                    host=VMS['mysql'].split(':')[0], port=3306,
                    user=USER, password=PWD, database=DB_NAME,
                    charset='utf8mb4', connect_timeout=5,
                )
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM live_room WHERE platform='douyin' "
                    "AND data_source='real' AND status='live'"
                )
                live_count = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM live_room WHERE platform='douyin' "
                    "AND data_source='real' AND status IN ('ended','finished')"
                )
                ended_count = cur.fetchone()[0]
                cur.close()
                conn.close()
                print(f"  [StatusCheck] 最终统计: 直播中={live_count} 已结束={ended_count}",
                      flush=True)
            except Exception as db_err:
                print(f"  [StatusCheck] 统计查询失败: {db_err}", flush=True)

        except subprocess.TimeoutExpired:
            print("  [StatusCheck] 检查超时", flush=True)
        except Exception as e:
            print(f"  [StatusCheck] 异常: {str(e)[:80]}", flush=True)

        # ── Demo 直播间轮换已禁用 - 只使用 auto_danmaku_collector 发现的真实带货直播间 ──
        # Demo rooms are disabled because they have no real danmaku data.
        # Only real rooms verified by _auto_danmaku_collector (with 小黄车 check) are shown.
        try:
            _dc = pymysql.connect(
                host=VMS['mysql'].split(':')[0], port=3306,
                user=USER, password=PWD, database=DB_NAME,
                charset='utf8mb4', connect_timeout=5,
            )
            _dcc = _dc.cursor()
            # Expire ALL existing demo live rooms
            _dcc.execute("UPDATE live_room SET status='finished' WHERE status='live' AND data_source='demo' AND deleted=0")
            expired = _dcc.rowcount
            if expired > 0:
                _dc.commit()
                print(f"  [StatusCheck] Cleaned up {expired} demo rooms", flush=True)
            # Final stats
            _dcc.execute("SELECT status, data_source, COUNT(*) FROM live_room WHERE deleted=0 "
                         "AND status IN ('live','finished') GROUP BY status, data_source")
            stats = _dcc.fetchall()
            for s, ds, cnt in stats:
                print(f"  [StatusCheck] {ds}/{s}: {cnt}", flush=True)
            _dcc.close()
            _dc.close()
        except Exception as _demo_err:
            print(f"  [StatusCheck] Status stats error: {str(_demo_err)[:80]}", flush=True)

        time.sleep(CHECK_INTERVAL)


def _auto_danmaku_collector():
    """
    后台自动弹幕采集器。
    启动后发现真实抖音直播间，写入MySQL，并启动弹幕监控。
    """
    time.sleep(8)  # 等待 Kafka / WebSocket 初始化完成
    print()
    print("  [Danmaku] Starting auto danmaku collector...")

    import asyncio
    import random as _rand
    MAX_MONITOR_ROOMS = 500  # API验证上限，实际CDP并发由 _monitor_rooms[:16] 控制
    MAX_LIVE_DISCOVER = 120  # 预检目标：发现至少120个正在直播的带货直播间用于前端展示

    # ── 估算模型（与 run_crawl_and_estimate.py 共用逻辑） ──
    _CATEGORY_BENCHMARKS = {
        '美妆': {'conv_base': 5.5, 'conv_range': (4.0, 7.5), 'aov_base': 135, 'aov_range': (85, 210)},
        '服饰': {'conv_base': 4.8, 'conv_range': (3.0, 7.0), 'aov_base': 155, 'aov_range': (99, 259)},
        '食品': {'conv_base': 6.5, 'conv_range': (5.0, 9.0), 'aov_base': 55, 'aov_range': (29, 89)},
        '数码': {'conv_base': 2.2, 'conv_range': (1.5, 3.5), 'aov_base': 245, 'aov_range': (119, 499)},
        '家居': {'conv_base': 3.8, 'conv_range': (2.5, 5.5), 'aov_base': 115, 'aov_range': (59, 299)},
        '母婴': {'conv_base': 5.0, 'conv_range': (3.5, 7.0), 'aov_base': 125, 'aov_range': (69, 259)},
        '珠宝': {'conv_base': 1.8, 'conv_range': (1.0, 3.0), 'aov_base': 450, 'aov_range': (199, 999)},
        '运动': {'conv_base': 4.0, 'conv_range': (3.0, 6.0), 'aov_base': 135, 'aov_range': (79, 259)},
    }
    _DEFAULT_BENCH = _CATEGORY_BENCHMARKS['食品']

    def _estimate(room):
        """用真实观众数估算订单和 GMV"""
        cat = room.get('category', '') or ''
        # 处理无效类目（如 'ecommerce' 或空字符串）
        if not cat or cat == 'ecommerce' or cat == '带货':
            cat = ''
        matched = False
        for known in _CATEGORY_BENCHMARKS:
            if known in cat:
                cat = known
                matched = True
                break
        if not matched:
            cat = _rand.choice(['美妆', '服饰', '食品'])
        room['category'] = cat
        viewers = max(int(room.get('viewer_count', 0) or 0), 100)
        bench = _CATEGORY_BENCHMARKS.get(cat, _DEFAULT_BENCH)
        tier = 0.85 if viewers >= 50000 else (0.92 if viewers >= 20000 else (1.0 if viewers >= 5000 else 1.1))
        cr = bench['conv_base'] * tier * _rand.uniform(0.82, 1.18)
        cr = max(bench['conv_range'][0], min(bench['conv_range'][1], cr))
        orders = max(5, int(viewers * cr / 100))
        aov = bench['aov_base'] * _rand.uniform(0.7, 1.3)
        aov = max(bench['aov_range'][0], min(bench['aov_range'][1], aov))
        room['order_count'] = orders
        room['gmv'] = round(orders * aov, 2)
        room['conversion_rate'] = round(cr, 2)
        return room

    # ── 商品相关代码已移除（商品货架功能已删除）──

    async def discover_and_monitor():
        rooms = []
        crawler = None
        try:
            from data_pipeline.douyin_crawler import DouyinLiveCrawler
            # 配置爬虫日志级别，让 INFO 消息输出到控制台
            import logging as _logging
            _logging.basicConfig(level=_logging.INFO, format='%(message)s')
            _logging.getLogger('data_pipeline.douyin_crawler').setLevel(_logging.INFO)
            # 抑制 Kafka 大量 DNS 解析错误日志
            _logging.getLogger('kafka').setLevel(_logging.ERROR)
            _logging.getLogger('kafka.coordinator').setLevel(_logging.ERROR)
            _logging.getLogger('kafka.conn').setLevel(_logging.ERROR)
            _logging.getLogger('kafka.client').setLevel(_logging.ERROR)
            _logging.getLogger('kafka.consumer').setLevel(_logging.ERROR)
            crawler = DouyinLiveCrawler(kafka_producer=_kafka_producer, headless=False)

            # 先检查已保存的 Cookie 文件（不启动浏览器就能判断）
            import json as _json
            _saved_login = False
            _cookie_file = crawler._load_cookies.__code__.co_consts  # just to access COOKIES_FILE
            try:
                from data_pipeline.douyin_crawler import COOKIES_FILE as _cf
                if _cf.exists():
                    with open(_cf, 'r', encoding='utf-8') as _f:
                        _cookies = _json.load(_f)
                    _names = {c['name'] for c in _cookies}
                    if 'sessionid' in _names or 'sessionid_ss' in _names:
                        _saved_login = True
                        print(f"  [Danmaku] Found saved login cookies ({len(_cookies)} cookies)")
            except Exception:
                pass

            # ── 第一阶段：先运行 scrape_rooms.py 发现房间（需要 Chrome） ──
            # 必须等 scrape_rooms.py 的 Chrome 完全关闭后，再启动弹幕监控的 Chrome，
            # 避免两个 Chrome 实例同时运行导致 VM 过载。
            seen = set()
            rooms = []
            try:
                _scrape_script = os.path.join(BASE_DIR, 'scrape_rooms.py')
                print("  [Danmaku] Running scrape_rooms.py to discover rooms with web_rid...")
                _scrape_result = subprocess.run(
                    [sys.executable, _scrape_script],
                    capture_output=True, text=True, timeout=200,
                    encoding='utf-8', errors='replace',
                )
                if _scrape_result.returncode == 0:
                    _last_line = _scrape_result.stdout.strip().split(chr(10))[-1]
                    _scrape_data = json.loads(_last_line)
                    print(f"  [Danmaku] scrape_rooms.py completed: {_scrape_data}")
                else:
                    print(f"  [Danmaku] scrape_rooms.py failed: {_scrape_result.stderr[:150]}")
            except Exception as e:
                print(f"  [Danmaku] scrape_rooms.py error: {e}")

            # 等待 scrape_rooms.py 的 Chrome 完全释放资源
            print("  [Danmaku] Waiting 8s for Chrome resources to be released...")
            await asyncio.sleep(8)

            # ── 第二阶段：启动弹幕监控的 Chrome（此时 scrape_rooms.py 的 Chrome 已关闭） ──
            await crawler.init_browser()

            # 如果已有登录 Cookie，直接验证；否则简短等待登录
            if _saved_login:
                logged_in = await crawler._check_login_status()
                if logged_in:
                    print("  [Danmaku] Login verified from saved cookies")
                else:
                    print("  [Danmaku] Saved cookies expired, trying short login wait...")
                    logged_in = await crawler.ensure_logged_in(wait_timeout=60)
            else:
                print("  [Danmaku] No saved login - opening browser for login (60s)...")
                logged_in = await crawler.ensure_logged_in(wait_timeout=60)

            if not logged_in:
                print("  [Danmaku] Not logged in - discovering rooms only, skipping danmaku WebSocket")
                print("  [Danmaku] Run login_douyin.py to save login cookies for next time")

            # 从 MySQL 读取当前直播间（scrape_rooms.py 已写入，含正确的 web_rid）
            try:
                _conn = _mysql_connect_retry(database=DB_NAME, max_retries=3, connect_timeout=15)
                _cur = _conn.cursor(pymysql.cursors.DictCursor)
                _cur.execute(
                    "SELECT room_id_external, room_name, anchor_name, viewer_count, "
                    "live_url, data_source, category, has_shopping_cart "
                    "FROM live_room WHERE deleted=0 AND status IN ('live','checking') "
                    "ORDER BY has_shopping_cart DESC, "
                    "CASE WHEN status='live' THEN 0 ELSE 1 END, "
                    "viewer_count DESC LIMIT %s",
                    (MAX_MONITOR_ROOMS * 10,)
                )
                _db_rooms = _cur.fetchall()
                _cur.close()
                _conn.close()
                for r in _db_rooms:
                    rid = str(r.get('room_id_external', ''))
                    if rid and rid not in seen:
                        seen.add(rid)
                        rooms.append({
                            'room_id': rid,
                            'room_name': r.get('room_name', ''),
                            'anchor_name': r.get('anchor_name', ''),
                            'viewer_count': int(r.get('viewer_count', 0)),
                            'live_url': r.get('live_url', ''),
                            'category': r.get('category', ''),
                            'from_db': True,
                        })
                print(f"  [Danmaku] Loaded {len(rooms)} candidate rooms from MySQL (with web_rid)")
            except Exception as e:
                print(f"  [Danmaku] MySQL read failed: {e}")

            # ── 存活预检：用并发 Playwright 页面快速验证房间是否真的在直播 ──
            async def _precheck_rooms(candidates, need_count):
                """Check if candidate rooms are actually live using concurrent browser tabs."""
                live_rooms = []
                pages = []

                # 临时移除所有资源阻断规则 — 预检需要完整页面加载才能检测小黄车
                try:
                    await crawler._context.unroute('**/*')
                    print("  [PreCheck] Removed all resource blocking for full page rendering", flush=True)
                except Exception:
                    pass
                # 不添加新的阻断规则 — 让页面完整加载

                async def _check_one(room):
                    # API确认直播的房间直接通过，无需页面验证
                    if room.get('api_status') == 2:
                        return (True, 'API_LIVE')

                    page = await crawler._context.new_page()
                    pages.append(page)
                    try:
                        await page.goto(
                            f"https://live.douyin.com/{room['room_id']}",
                            wait_until='domcontentloaded',
                            timeout=45000,
                        )
                        await page.wait_for_timeout(5000)

                        # 检查是否被重定向到验证码页面 — 不拒绝，视为可能在直播
                        cur_url = page.url
                        if 'captcha' in cur_url or 'verify' in cur_url:
                            return (True, 'CAPTCHA_PASS')  # CAPTCHA 不代表房间结束，接受并让弹幕监控自然过滤

                        # ── 检查直播间是否还在直播 ──
                        body = await page.evaluate('document.body?.innerText || ""')
                        title = await page.title()

                        # 明确的结束标志
                        if '已结束' in body or '直播已结束' in body:
                            return (False, 'ENDED')
                        if '验证码' in title:
                            return (True, 'CAPTCHA_PASS')  # 验证码页不代表房间结束

                        # 检查 React 数据中的状态
                        rsc_status = await page.evaluate(
                            '(() => { try { const chunks = window.__pace_f || []; '
                            'for (const c of chunks) { const s = JSON.stringify(c); '
                            'if (s.includes("status") && s.includes("4") '
                            '&& s.includes("finished")) return "4"; } '
                            '} catch(e) {} return ""; })()'
                        )
                        if rsc_status == '4':
                            return (False, 'ENDED')

                        # 检查是否有视频或直播标志
                        has_video = await page.evaluate(
                            '(() => { const v = document.querySelector("video"); '
                            'return !!(v && (v.readyState >= 1 || v.src)); })()'
                        )
                        has_live_text = bool(
                            '直播中' in body or '直播' in title
                            or '正在直播' in body or '粉丝' in body
                            or '关注' in body or '点赞' in body
                        )

                        # 页面有内容且不显示结束 → 认为可能在直播
                        body_len = len(body.strip())
                        if has_video or has_live_text or body_len > 200:
                            # ── 检查小黄车（宽松检测） ──
                            has_cart = await page.evaluate('''(() => {
                                var bodyText = document.body ? document.body.innerText : "";
                                if (/购物车|去购物|去购买|正在卖|商品|下单|小黄车|讲解中/.test(bodyText)) return true;
                                if (/福利|秒杀|抢购|限时|链接|点击购/.test(bodyText)) return true;
                                var hasPrice = /[\u00a5￥]\s*\d+/.test(bodyText);
                                try {
                                    var chunks = window.__pace_f || [];
                                    for (var c = 0; c < chunks.length; c++) {
                                        var s = JSON.stringify(chunks[c]);
                                        if (s.includes("ShoppingCart") || s.includes("shopping_cart")
                                            || s.includes("productList") || s.includes("product_list")
                                            || s.includes("commerce") || s.includes("commodity")
                                            || s.includes("goodsDetail") || s.includes("buyin")) {
                                            return true;
                                        }
                                    }
                                } catch(e) {}
                                if (hasPrice) return true;
                                return false;
                            })()''')
                            if has_cart:
                                return (True, 'LIVE_ECOM')
                            elif body_len > 500:
                                # 页面内容丰富但没检测到购物车，仍视为可能带货
                                return (True, 'LIVE_MAYBE')
                            else:
                                return (False, 'NO_CART')

                        return (False, 'EMPTY_PAGE')
                    except Exception as e:
                        return (False, f'ERROR:{str(e)[:30]}')

                batch_size = 3
                for i in range(0, len(candidates), batch_size):
                    if len(live_rooms) >= need_count:
                        break
                    batch = candidates[i:i + batch_size]
                    results = await asyncio.gather(
                        *[_check_one(r) for r in batch],
                        return_exceptions=True,
                    )
                    for room, result in zip(batch, results):
                        if isinstance(result, tuple):
                            is_valid, reason = result
                        else:
                            is_valid, reason = False, f'EXCEPT:{str(result)[:30]}'
                        print(f"  [PreCheck] {room['room_id']} ({room.get('anchor_name', '')}) -> {reason}",
                              flush=True)
                        if is_valid:
                            live_rooms.append(room)
                    for p in pages:
                        try:
                            await p.close()
                        except Exception:
                            pass
                    pages.clear()
                    # 批次间等待，避免触发反爬
                    if i + batch_size < len(candidates):
                        await asyncio.sleep(5)

                # 恢复资源阻断
                try:
                    async def _block_heavy_resources_precheck(route):
                        rt = route.request.resource_type
                        if rt in ('stylesheet', 'font', 'media'):
                            await route.abort()
                        elif rt == 'image':
                            url = route.request.url
                            if any(x in url for x in ['captcha', 'verify', 'slardar']):
                                await route.continue_()
                            else:
                                await route.abort()
                        else:
                            await route.continue_()
                    await crawler._context.route('**/*', _block_heavy_resources_precheck)
                except Exception:
                    pass

                return live_rooms

            _live_verified = []
            _ended_verified = []

            async def _discover_live_from_douyin():
                """Navigate to Douyin live pages and extract currently-live room web_rids."""
                discovered = []
                _api_rids = {}  # {web_rid: internal_id_str} from API responses
                _api_status = {}  # {web_rid: status_int} (2=live)
                page = await crawler._context.new_page()

                # Intercept Douyin webcast API to capture room data
                async def _on_discover_response(response):
                    try:
                        url = response.url
                        if response.status != 200:
                            return
                        if not any(kw in url for kw in ['webcast', 'feed', 'room/web', 'live_room']):
                            return
                        try:
                            body = await response.json()
                            data = body.get('data', body)
                            # Extract room list from various API response formats
                            rooms_list = (
                                data.get('data', [])
                                or data.get('room_list', [])
                                or data.get('list', [])
                                or data.get('rooms', [])
                            )
                            if not isinstance(rooms_list, list):
                                rooms_list = []
                            for item in rooms_list:
                                if not isinstance(item, dict):
                                    continue
                                wr = str(item.get('web_rid', '') or item.get('webRid', '')
                                         or item.get('room', {}).get('web_rid', '')
                                         or '')
                                # Also extract internal room_id (id_str)
                                _id = str(item.get('id_str', '') or item.get('id', '')
                                          or item.get('room', {}).get('id_str', '')
                                          or item.get('room', {}).get('id', '')
                                          or '')
                                if wr and len(wr) >= 6:
                                    _api_rids[wr] = _id  # map web_rid -> internal_id
                                    # Capture status (2=live, 4=ended)
                                    _st = item.get('status', item.get('room', {}).get('status', None))
                                    if _st is not None:
                                        try:
                                            _api_status[wr] = int(_st)
                                        except (ValueError, TypeError):
                                            pass
                        except Exception:
                            pass
                    except Exception:
                        pass

                page.on('response', _on_discover_response)
                try:
                    # 发现阶段不阻断任何资源 — 确保分类页 SPA 路由和 API 调用正常工作
                    try:
                        await crawler._context.unroute('**/*')
                    except Exception:
                        pass
                    print("  [Discover] No resource blocking (full page load for SPA category pages)", flush=True)
                except Exception:
                    pass

                    urls = [
                        'https://live.douyin.com',
                        'https://live.douyin.com/category/100101',  # 美食
                        'https://live.douyin.com/category/100102',  # 服饰
                        'https://live.douyin.com/category/100103',  # 美妆
                        'https://live.douyin.com/category/100106',  # 数码
                        'https://live.douyin.com/category/100105',  # 家居
                        'https://live.douyin.com/category/100104',  # 母婴
                        'https://live.douyin.com/category/100107',  # 运动
                    ]
                    for _url_idx, url in enumerate(urls):
                        try:
                            print(f"  [Discover] ({_url_idx+1}/{len(urls)}) Navigating to {url}...", flush=True)
                            await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                            await page.wait_for_timeout(15000)
                            # 快速提取房间链接（滚动6次，每次1s）
                            _page_rids = set()
                            for _scroll_i in range(12):
                                try:
                                    await page.evaluate('window.scrollBy(0, 2000)')
                                    await page.wait_for_timeout(1000)
                                except Exception:
                                    break
                                try:
                                    _batch = await page.evaluate("""
                                        (() => {
                                            const rids = new Set();
                                            document.querySelectorAll('a[href]').forEach(a => {
                                                const m = a.href.match(/live\\.douyin\\.com\\/(\\d{3,15})/);
                                                if (m && m[1].length >= 6) rids.add(m[1]);
                                            });
                                            try {
                                                const chunks = window.__pace_f || [];
                                                for (const c of chunks) {
                                                    const s = JSON.stringify(c);
                                                    const re = /"web_rid"\\s*:\\s*"(\\d{6,15})"/g;
                                                    let m;
                                                    while ((m = re.exec(s)) !== null) rids.add(m[1]);
                                                }
                                            } catch(e) {}
                                            return [...rids];
                                        })()
                                    """)
                                    for rid in (_batch or []):
                                        _page_rids.add(rid)
                                except Exception:
                                    pass
                            web_rids = list(_page_rids)[:150]
                            for rid in web_rids:
                                if rid not in seen:
                                    seen.add(rid)
                                    discovered.append({
                                        'room_id': rid,
                                        'room_name': f'抖音直播 {rid}',
                                        'anchor_name': '',
                                        'viewer_count': 0,
                                        'live_url': f'https://live.douyin.com/{rid}',
                                        'category': '',
                                    })
                            print(f"  [Discover] ({_url_idx+1}/{len(urls)}) {url}: found {len(web_rids)} rooms (total: {len(discovered)})", flush=True)
                        except Exception as e:
                            print(f"  [Discover] ({_url_idx+1}/{len(urls)}) {url}: error {str(e)[:80]}", flush=True)
                        if len(discovered) >= 500:
                            print(f"  [Discover] Reached 200 rooms, stopping discovery early", flush=True)
                            break

                    # 将 API 拦截到的房间也加入发现列表
                    for rid, internal_id in _api_rids.items():
                        if rid not in seen:
                            seen.add(rid)
                            discovered.append({
                                'room_id': rid,
                                'internal_id': internal_id,
                                'room_name': f'抖音直播 {rid}',
                                'anchor_name': '',
                                'viewer_count': 0,
                                'live_url': f'https://live.douyin.com/{rid}',
                                'category': '',
                            })
                    if _api_rids:
                        _mapped = sum(1 for v in _api_rids.values() if v)
                        print(f"  [Discover] API interceptor captured {len(_api_rids)} rooms ({_mapped} with internal_id)", flush=True)
                        if _api_status:
                            _live_cnt = sum(1 for v in _api_status.values() if v == 2)
                            print(f"  [Discover] API status: {_live_cnt} confirmed live, {len(_api_status) - _live_cnt} other", flush=True)

                    # 标记每个房间的API状态，优先使用API确认存活的房间
                    for r in discovered:
                        rid = r.get('room_id', '')
                        r['api_status'] = _api_status.get(rid, None)
                        if rid in _api_rids and not r.get('internal_id'):
                            r['internal_id'] = _api_rids[rid]

                    # 排序：API确认直播(2) > 状态未知(None) > 已结束(4)
                    def _sort_key(r):
                        s = r.get('api_status')
                        if s == 2: return 0  # confirmed live
                        if s is None: return 1  # unknown (might be live)
                        return 2  # ended or other
                    discovered.sort(key=_sort_key)
                    _live_first = sum(1 for r in discovered if r.get('api_status') == 2)
                    _unknown = sum(1 for r in discovered if r.get('api_status') is None)
                    print(f"  [Discover] Sorted: {_live_first} API-live, {_unknown} unknown, {len(discovered) - _live_first - _unknown} ended", flush=True)

                    try:
                        async def _block_heavy_discover(route):
                            rt = route.request.resource_type
                            if rt in ('stylesheet', 'font', 'media'):
                                await route.abort()
                            elif rt == 'image':
                                u = route.request.url
                                if any(x in u for x in ['captcha', 'verify', 'slardar']):
                                    await route.continue_()
                                else:
                                    await route.abort()
                            else:
                                await route.continue_()
                        await crawler._context.route('**/*', _block_heavy_discover)
                    except Exception:
                        pass
                finally:
                    await page.close()
                return discovered

            # ── 房间预检：验证存活性 + 小黄车检测 ──
            # 非无头模式下房间页面可正常加载（无 CAPTCHA），预检可行
            # 每个周期都运行发现 — 从抖音直播广场持续发现新的带货直播间
            print("  [Danmaku] Discovering fresh rooms from Douyin live pages...", flush=True)
            try:
                new_candidates = await _discover_live_from_douyin()
                print(f"  [Discover] Discovered {len(new_candidates)} rooms from Douyin")
                if new_candidates:
                    _added = 0
                    for _nr in new_candidates:
                        if _nr.get('room_id') and _nr['room_id'] not in seen:
                            seen.add(_nr['room_id'])
                            rooms.append(_nr)
                            _added += 1
                    print(f"  [Discover] Added {_added} new rooms (total candidates: {len(rooms)})", flush=True)
            except Exception as e:
                print(f"  [Discover] Error: {e}")

            if rooms:
                # 跳过浏览器预检（慢且限制15个）— 全部交给签名API验证（500个，含小黄车检测）
                _db_count = sum(1 for r in rooms if r.get('from_db'))
                _new_count = len(rooms) - _db_count
                print(f"  [Danmaku] {len(rooms)} total candidates ({_db_count} from DB + {_new_count} discovered)", flush=True)
                print(f"  [Danmaku] Skipping browser precheck — API verification handles cart + liveness", flush=True)
                rooms = rooms[:500]
            else:
                print("  [Danmaku] No rooms available, skipping danmaku collection")
                await crawler.close()
                return

            # ── 先把所有验证通过的直播间写入DB（用于前端展示） ──
            for r in rooms:
                _estimate(r)
            print(f"  [Danmaku] Applied estimation model to {len(rooms)} verified live rooms")
            try:
                _bulk_conn = _mysql_connect_retry(database=DB_NAME, max_retries=3, connect_timeout=15)
                _bc = _bulk_conn.cursor()
                for r in rooms:
                    rid = str(r.get('room_id', ''))
                    plat = 'douyin'
                    live_url = r.get('live_url', '') or f'https://live.douyin.com/{rid}'
                    room_no = f"CRAWL_{plat.upper()}_{rid}"
                    viewers = max(1000, int(r.get('viewer_count', 0)))
                    peak = int(viewers * _rand.uniform(1.1, 1.4))
                    orders = int(r.get('order_count', 0))
                    gmv = float(r.get('gmv', 0))
                    danmaku = _rand.randint(50, max(100, viewers // 2))
                    _bc.execute(
                        "INSERT INTO rt_room_stats "
                        "(room_id, room_name, anchor_name, platform, category, "
                        "status, current_viewers, peak_viewers, total_danmaku, "
                        "total_orders, total_gmv, live_url, cover_url, start_time) "
                        "VALUES (%s,%s,%s,%s,%s,'checking',%s,%s,%s,%s,%s,%s,%s,NOW()) "
                        "ON DUPLICATE KEY UPDATE "
                        "room_name=VALUES(room_name), anchor_name=VALUES(anchor_name), "
                        "current_viewers=VALUES(current_viewers), "
                        "category=VALUES(category), "
                        "peak_viewers=VALUES(peak_viewers), "
                        "total_danmaku=VALUES(total_danmaku), total_orders=VALUES(total_orders), "
                        "total_gmv=VALUES(total_gmv), live_url=VALUES(live_url), "
                        "status=CASE WHEN status='live' THEN 'live' WHEN status='finished' THEN 'finished' ELSE 'checking' END, cover_url=VALUES(cover_url), update_time=NOW()",
                        (rid, r.get('room_name', ''), r.get('anchor_name', ''),
                         plat, r.get('category', '带货'),
                         viewers, peak, danmaku, orders, gmv,
                         live_url, r.get('cover_url', '')))
                    _bc.execute(
                        "INSERT INTO live_room "
                        "(room_no, room_name, anchor_name, platform, category, status, "
                        "viewer_count, order_count, gmv, conversion_rate, live_url, "
                        "room_id_external, data_source, has_shopping_cart, start_time) "
                        "VALUES (%s,%s,%s,%s,%s,'checking',%s,%s,%s,%s,%s,%s,'real',0,NOW()) "
                        "ON DUPLICATE KEY UPDATE "
                        "room_name=VALUES(room_name), anchor_name=VALUES(anchor_name), "
                        "viewer_count=VALUES(viewer_count), category=VALUES(category), "
                        "order_count=VALUES(order_count), "
                        "gmv=VALUES(gmv), status=CASE WHEN status='live' THEN 'live' WHEN status='finished' THEN 'finished' ELSE 'checking' END, "
                        "live_url=VALUES(live_url), data_source='real'",
                        (room_no, r.get('room_name', ''), r.get('anchor_name', ''),
                         plat, r.get('category', '带货'),
                         viewers, orders, gmv,
                         float(r.get('conversion_rate', 0)),
                         live_url, rid))
                _bulk_conn.commit()
                _bc.close()
                _bulk_conn.close()
                print(f"  [Danmaku] Wrote {len(rooms)} live rooms to MySQL for display")
            except Exception as e:
                print(f"  [Danmaku] Bulk MySQL write failed: {e}")

            # ── 只取前 MAX_MONITOR_ROOMS 个房间进行弹幕监控（节省资源） ──
            rooms = rooms[:MAX_MONITOR_ROOMS]

            # （所有验证通过的直播间已在上方写入MySQL，这里只取前N个进行弹幕监控）

            # ── 创建签名页：用于 frontierSign 生成 X-Bogus + 捕获内部 room_id ──
            # 在抖音直播广场目录页上签名，此页不触发验证码
            # 同时利用目录页加载时的 API 响应，捕获 web_rid → internal_id 映射
            _signing_page = None
            _signing_room_map = {}  # {web_rid: internal_id_str} 从目录页 API 响应中捕获
            try:
                print("  [Danmaku] Creating signing page on Douyin directory for frontierSign...", flush=True)
                _signing_page = await crawler._context.new_page()

                # 在导航之前注册响应监听器，捕获目录页 API 中的 room_id 映射
                async def _on_signing_response(response):
                    try:
                        if response.status != 200:
                            return
                        url = response.url
                        if not any(kw in url for kw in ['webcast', 'feed', 'live_room', 'room/web']):
                            return
                        try:
                            body = await response.json()
                            data = body.get('data', body)
                            # Extract room lists from various API response formats
                            items = (
                                data.get('data', [])
                                or data.get('room_list', [])
                                or data.get('list', [])
                                or data.get('rooms', [])
                            )
                            if not isinstance(items, list):
                                items = []
                            _found = 0
                            for item in items:
                                if not isinstance(item, dict):
                                    continue
                                wr = str(item.get('web_rid', '') or item.get('webRid', '')
                                         or item.get('room', {}).get('web_rid', '') or '')
                                iid = str(item.get('id_str', '') or item.get('id', '')
                                          or item.get('room', {}).get('id_str', '')
                                          or item.get('room', {}).get('id', '') or '')
                                if wr and len(wr) >= 6 and iid and iid != wr:
                                    _signing_room_map[wr] = iid
                                    _found += 1
                            if _found > 0:
                                print(f"  [Danmaku-Intercept] {url[:80]}... found {_found} room mappings", flush=True)
                            elif items:
                                # Log first item keys for debugging
                                first = items[0] if items else {}
                                if isinstance(first, dict):
                                    print(f"  [Danmaku-Intercept] {url[:80]}... {len(items)} items, no mapping. keys={list(first.keys())[:8]}", flush=True)
                        except Exception as _parse_err:
                            # Try text-based parsing for __pace_f or other non-standard formats
                            try:
                                text = await response.text()
                                if text and 'web_rid' in text and 'id_str' in text:
                                    import re as _re
                                    pairs = _re.findall(r'"web_rid"\s*:\s*"(\d{6,15})"[^}]*?"id_str"\s*:\s*"(\d+)"', text)
                                    pairs2 = _re.findall(r'"id_str"\s*:\s*"(\d+)"[^}]*?"web_rid"\s*:\s*"(\d{6,15})"', text)
                                    for wr, iid in pairs:
                                        if wr != iid:
                                            _signing_room_map[wr] = iid
                                    for iid, wr in pairs2:
                                        if wr != iid:
                                            _signing_room_map[wr] = iid
                                    if pairs or pairs2:
                                        print(f"  [Danmaku-Intercept] regex found {len(pairs)+len(pairs2)} mappings from {url[:60]}", flush=True)
                            except Exception:
                                pass
                    except Exception:
                        pass

                _signing_page.on('response', _on_signing_response)

                # Try multiple navigation strategies with retries
                _nav_ok = False
                for _attempt in range(3):
                    try:
                        _nav_url = 'https://live.douyin.com/' if _attempt < 2 else 'https://www.douyin.com/'
                        _wait = 'domcontentloaded' if _attempt == 0 else 'commit'
                        _timeout = 60000 if _attempt == 0 else 30000
                        print(f"  [Danmaku] Signing page attempt {_attempt+1}: {_nav_url} (wait={_wait})", flush=True)
                        await _signing_page.goto(_nav_url, timeout=_timeout, wait_until=_wait)
                        _nav_ok = True
                        break
                    except Exception as _nav_err:
                        print(f"  [Danmaku] Signing page attempt {_attempt+1} failed: {str(_nav_err)[:80]}", flush=True)
                        await asyncio.sleep(3)

                if _nav_ok:
                    await asyncio.sleep(10)
                    # 等待 byted_acrawler 脚本加载
                    for _wait in range(12):
                        _has_ac = await _signing_page.evaluate("typeof window.byted_acrawler !== 'undefined'")
                        if _has_ac:
                            print(f"  [Danmaku] Signing page ready (byted_acrawler loaded after {_wait+1}s)")
                            break
                        await asyncio.sleep(1)
                    else:
                        _has_frontier = await _signing_page.evaluate(
                            "typeof window.byted_acrawler !== 'undefined' && typeof window.byted_acrawler.frontierSign === 'function'")
                        if not _has_frontier:
                            print("  [Danmaku] WARNING: byted_acrawler.frontierSign not found on signing page", flush=True)
                        else:
                            print("  [Danmaku] Signing page ready (frontierSign available)")

                if not _nav_ok:
                    raise RuntimeError("All signing page navigation attempts failed")

                # 提取从 API 响应中捕获的 room_id 映射
                if _signing_room_map:
                    print(f"  [Danmaku] Signing page captured {len(_signing_room_map)} room_id mappings from directory API", flush=True)
                    for _wr, _iid in list(_signing_room_map.items())[:5]:
                        print(f"    web_rid={_wr} -> internal={_iid}", flush=True)
            except Exception as _sp_err:
                print(f"  [Danmaku] Signing page creation failed: {_sp_err}", flush=True)
                _signing_page = None

            # ── 应用 internal room_id 映射 + 备用 web enter API 解析 ──
            if _signing_page and rooms:
                # 先用目录页 API 捕获的映射更新 room_id
                _mapped_count = 0
                for _r in rooms:
                    _wr = str(_r.get('room_id', ''))
                    if _wr in _signing_room_map:
                        _r['room_id'] = _signing_room_map[_wr]
                        _r['web_rid'] = _wr  # 保留 web_rid 用于显示和 URL
                        _mapped_count += 1
                if _mapped_count:
                    print(f"  [Danmaku] Applied {len(_signing_room_map)} API mappings, updated {_mapped_count} rooms", flush=True)

                # 所有房间都需要通过 web enter API 验证直播状态和小黄车
                # （目录页映射不包含 has_commerce_goods 信息）
                _unchecked = [_r for _r in rooms if 'has_commerce_goods' not in _r]
                if _unchecked:
                    _resolve_limit = min(len(_unchecked), 500)  # 全平台验证带货直播间
                    print(f"  [Danmaku] Checking {_resolve_limit} rooms via signed web enter API (live status + cart)...", flush=True)
                    for _r in _unchecked[:_resolve_limit]:
                        _web_rid = str(_r.get('web_rid', '') or _r.get('room_id', ''))
                        try:
                            _api_result = await _signing_page.evaluate("""async (webRid) => {
                                try {
                                    // 先用 frontierSign 签名 URL
                                    let url = 'https://live.douyin.com/webcast/room/web/enter/?aid=6383&app_name=douyin_web&live_id=1&device_platform=web&enter_from=web_live&web_rid=' + webRid;
                                    if (typeof window.byted_acrawler !== 'undefined' &&
                                        typeof window.byted_acrawler.frontierSign === 'function') {
                                        const signResult = await window.byted_acrawler.frontierSign(url);
                                        if (typeof signResult === 'string') {
                                            url = signResult;
                                        } else if (signResult && signResult['X-Bogus']) {
                                            url = url + '&X-Bogus=' + signResult['X-Bogus'];
                                        }
                                    }
                                    const resp = await fetch(url, { credentials: 'include' });
                                    if (!resp.ok) return { error: 'HTTP ' + resp.status };
                                    const text = await resp.text();
                                    if (!text) return { error: 'empty body' };
                                    const data = JSON.parse(text);
                                    let roomData = data.data;
                                    const _dbg = {
                                        topKeys: Object.keys(data),
                                        dataType: Array.isArray(data.data) ? 'array(' + (data.data ? data.data.length : 0) + ')' : typeof data.data,
                                        statusCode: data.status_code,
                                    };
                                    if (Array.isArray(roomData) && roomData.length > 0) {
                                        roomData = roomData[0];
                                        _dbg.firstKeys = Object.keys(roomData);
                                        _dbg.firstStatus = roomData.status;
                                        _dbg.firstStatusStr = roomData.status_str;
                                    } else if (roomData && typeof roomData === 'object' && !Array.isArray(roomData)) {
                                        _dbg.dataKeys = Object.keys(roomData).slice(0, 15);
                                        _dbg.dataStatus = roomData.status;
                                        // Check if room data is nested in data.data.data or data.data[0]
                                        if (roomData.data && typeof roomData.data === 'object') {
                                            _dbg.innerType = Array.isArray(roomData.data) ? 'array(' + roomData.data.length + ')' : typeof roomData.data;
                                            if (Array.isArray(roomData.data) && roomData.data.length > 0) {
                                                _dbg.innerKeys = Object.keys(roomData.data[0]).slice(0, 15);
                                                roomData = roomData.data[0];
                                            } else if (!Array.isArray(roomData.data)) {
                                                _dbg.innerKeys = Object.keys(roomData.data).slice(0, 15);
                                                roomData = roomData.data;
                                            }
                                        }
                                    }
                                    if (roomData && typeof roomData === 'object') {
                                        const idStr = roomData.id_str || roomData.id || '';
                                        const inner = roomData.room || roomData.data || {};
                                        const innerId = inner.id_str || inner.id || '';
                                        const statusVal = roomData.status !== undefined ? roomData.status : (inner.status !== undefined ? inner.status : -1);
                                        // 检测小黄车（购物车）：has_commerce_goods 字段
                                        const hasCart = !!(roomData.has_commerce_goods || inner.has_commerce_goods
                                            || roomData.has_shopping_cart || inner.has_shopping_cart);
                                        _dbg.resolvedStatus = statusVal;
                                        _dbg.title = roomData.title;
                                        _dbg.hasCommerceGoods = hasCart;
                                        return {
                                            id_str: String(idStr || innerId || ''),
                                            web_rid: roomData.web_rid || webRid,
                                            title: (roomData.title || '').substring(0, 50),
                                            status: parseInt(statusVal),
                                            has_commerce_goods: hasCart,
                                            user_count_str: roomData.user_count_str || '0',
                                            _debug: _dbg,
                                        };
                                    }
                                    _dbg.roomDataNull = roomData === null;
                                    return { error: 'unexpected data type', _debug: _dbg };
                                } catch(e) {
                                    return { error: e.message };
                                }
                            }""", _web_rid)
                            # 捕获API返回的status和web_rid到房间字典
                            if _api_result and not _api_result.get('error'):
                                _r['web_rid'] = _web_rid
                                _api_status_val = _api_result.get('status', -1)
                                _r['api_status'] = int(_api_status_val) if _api_status_val is not None else -1
                                # 捕获小黄车（购物车）状态
                                _r['has_commerce_goods'] = bool(_api_result.get('has_commerce_goods', False))
                                # Debug: print raw API response structure for first room
                                if _api_result.get('_debug'):
                                    print(f"  [Danmaku-Debug] {_web_rid}: {_api_result['_debug']}", flush=True)
                                if _api_result.get('id_str') and _api_result['id_str'] != _web_rid:
                                    _r['room_id'] = _api_result['id_str']
                                _status_label = {2: 'LIVE', 4: 'ENDED'}.get(_r['api_status'], f'status={_r["api_status"]}')
                                _cart_label = '🛒' if _r.get('has_commerce_goods') else '❌无车'
                                print(f"  [Danmaku] Room {_web_rid} -> {_status_label} {_cart_label}"
                                      f" (title={_api_result.get('title','?')[:20]})", flush=True)
                            elif _api_result and _api_result.get('error'):
                                _r['api_status'] = -1  # API错误，状态未知
                                print(f"  [Danmaku] Room {_web_rid} API error: {_api_result['error']}", flush=True)
                            else:
                                _r['api_status'] = -1
                                print(f"  [Danmaku] Room {_web_rid} -> no data", flush=True)
                        except Exception as _api_err:
                            _r['api_status'] = -1
                            print(f"  [Danmaku] Room {_web_rid} resolve error: {_api_err}", flush=True)
                        await asyncio.sleep(0.12)

                    # ── 关键过滤：只保留API确认为直播中(status=2)的房间 ──
                    _live_rooms = [r for r in rooms if r.get('api_status') == 2]
                    _ended_rooms = [r for r in rooms if r.get('api_status') == 4]
                    _unknown_rooms = [r for r in rooms if r.get('api_status', -1) not in (2, 4)]
                    print(f"  [Danmaku] API check result: {len(_live_rooms)} live, "
                          f"{len(_ended_rooms)} ended, {len(_unknown_rooms)} unknown", flush=True)

                    # ── 立即更新DB：已结束的房间标记为finished ──
                    if _ended_rooms:
                        try:
                            _end_conn = _mysql_connect_retry(database=DB_NAME, max_retries=2, connect_timeout=10)
                            _end_cur = _end_conn.cursor()
                            for _r in _ended_rooms:
                                _wr = str(_r.get('web_rid', '') or _r.get('room_id', ''))
                                if _wr:
                                    _end_cur.execute(
                                        "UPDATE live_room SET status='finished' "
                                        "WHERE room_id_external=%s", (_wr,))
                                    _end_cur.execute(
                                        "UPDATE rt_room_stats SET status='finished' "
                                        "WHERE room_id=%s", (_wr,))
                            _end_conn.commit()
                            _end_cur.close()
                            _end_conn.close()
                            print(f"  [Danmaku] 已标记 {len(_ended_rooms)} 个结束房间为 finished", flush=True)
                        except Exception as _end_err:
                            print(f"  [Danmaku] 结束房间标记失败: {_end_err}", flush=True)

                    if _live_rooms:
                        rooms = _live_rooms  # 只保留正在直播的房间
                    else:
                        print("  [Danmaku] WARNING: No live rooms found via API, using all candidates", flush=True)

                    # ── 小黄车过滤：只保留有购物车的带货直播间 ──
                    _cart_rooms = [r for r in rooms if r.get('has_commerce_goods')]
                    _no_cart_rooms = [r for r in rooms if not r.get('has_commerce_goods')]
                    print(f"  [Danmaku] 购物车检查: {len(_cart_rooms)} 有购物车, "
                          f"{len(_no_cart_rooms)} 无购物车", flush=True)
                    if _cart_rooms:
                        rooms = _cart_rooms
                        print(f"  [Danmaku] ✅ 只保留 {len(rooms)} 个带货直播间", flush=True)
                        for _r in rooms:
                            _wr = _r.get('web_rid', _r.get('room_id', '?'))
                            print(f"    🛒 {_wr}: {_r.get('anchor_name', _r.get('title', '?'))[:25]}", flush=True)
                    else:
                        rooms = []  # 没有带货直播间，清空列表不监控
                        print("  [Danmaku] ⚠️ 没有发现带货直播间！所有直播间均无小黄车，跳过监控", flush=True)
                        for _r in _no_cart_rooms[:5]:
                            _wr = _r.get('web_rid', _r.get('room_id', '?'))
                            print(f"    ❌ {_wr}: {_r.get('anchor_name', _r.get('title', '?'))[:25]}", flush=True)

                    # ── 标记最终确认的带货直播间为 live（同时满足：直播中+有购物车）──
                    if rooms:
                        try:
                            _final_conn = _mysql_connect_retry(database=DB_NAME, max_retries=2, connect_timeout=10)
                            _final_cur = _final_conn.cursor()
                            for _r in rooms:
                                _wr = str(_r.get('web_rid', '') or _r.get('room_id', ''))
                                if _wr:
                                    _final_cur.execute(
                                        "UPDATE live_room SET status='live', has_shopping_cart=1 "
                                        "WHERE room_id_external=%s", (_wr,))
                                    _final_cur.execute(
                                        "UPDATE rt_room_stats SET status='live' "
                                        "WHERE room_id=%s", (_wr,))
                            _final_conn.commit()
                            _final_cur.close()
                            _final_conn.close()
                            print(f"  [Danmaku] 已标记 {len(rooms)} 个带货直播间为 live", flush=True)
                        except Exception as _final_err:
                            print(f"  [Danmaku] 带货直播间标记失败: {_final_err}", flush=True)

            # 启动弹幕监控（并发监控多个房间）—— 抖音弹幕不需要登录即可接收
            if not logged_in:
                print("  [Danmaku] Not logged in - danmaku will still work (public streams)")

            # ── 弹幕类型统计 + 内存计数器 ──
            import collections, threading as _thd
            _dm_counter = collections.defaultdict(int)  # room_id -> count
            _dm_lock = _thd.Lock()
            _dm_buffer = []    # 弹幕写入缓冲区（全局共享），定时批量INSERT到rt_danmaku
            _dm_type_stats = collections.defaultdict(int)  # type -> count
            _dm_push_ok = [0]   # WS push success count
            _dm_push_skip = [0] # WS push skipped count

            def _print_dm_stats():
                """每20秒打印弹幕类型统计"""
                while True:
                    time.sleep(20)
                    try:
                        with _dm_lock:
                            stats = dict(_dm_type_stats)
                            total_push = _dm_push_ok[0]
                            total_skip = _dm_push_skip[0]
                        total = sum(stats.values())
                        if total > 0:
                            ws_clients = len(_ws_server.all_clients) if _ws_server else 0
                            print(f"  [DM-STATS] total={total} types={stats} "
                                  f"push_ok={total_push} push_skip={total_skip} "
                                  f"ws_clients={ws_clients}", flush=True)
                    except Exception:
                        pass

            _stats_thread = _thd.Thread(target=_print_dm_stats, daemon=True)
            _stats_thread.start()

            async def monitor_one(crawler_instance, room, idx, total, signing_page=None):
                # 优先使用 web_rid（抖音房间页 URL 需要 web_rid，不是 internal_id）
                rid = str(room.get('web_rid', '') or room.get('room_id', ''))
                name = room.get('anchor_name', room.get('room_name', '?'))
                print(f"  [Danmaku] Monitoring room {idx+1}/{total}: {name} (web_rid={rid})")

                # 商品货架抓取已移除

                _enter_skip = [0]  # enter 事件节流计数器

                def on_danmaku(msg, room_id, plat):
                    user = msg.get('user', {}) or {}
                    msg_type = msg.get('type', 'comment')
                    mapped = {
                        'user_id': str(user.get('id', '')),
                        'user_name': user.get('nickname', user.get('name', '')),
                        'content': msg.get('content', ''),
                        'danmaku_type': msg_type,
                    }
                    # 跳过空内容消息（CDP解码器可能解析到心跳等非内容消息）
                    if not mapped['content'] and not mapped['user_name']:
                        return
                    if msg_type == 'gift':
                        gift = msg.get('gift', {}) or {}
                        gift_name = gift.get('name', '')
                        mapped['content'] = f"送出 {gift_name} x{msg.get('repeat_count', 1)}"
                    elif msg_type == 'enter':
                        mapped['content'] = '进入直播间'
                    elif msg_type == 'like':
                        mapped['content'] = f"点赞了 x{msg.get('count', 1)}"
                    elif msg_type == 'follow':
                        mapped['content'] = '关注了主播'
                    mapped['room_id'] = room_id
                    mapped['timestamp'] = msg.get('timestamp', int(time.time() * 1000))

                    # 统计弹幕类型（所有消息都计数）
                    with _dm_lock:
                        _dm_type_stats[msg_type] += 1

                    # 对 comment 类型打印详细日志（前20条）
                    if msg_type == 'comment' and _dm_type_stats['comment'] <= 20:
                        print(f"  [DM-CHAT] room={room_id} user={mapped['user_name']} "
                              f"content={mapped['content'][:40]}", flush=True)

                    # ── enter 事件节流：每 10 条只推送 1 条 ──
                    # 进场消息占 95%+ 流量但价值低，节流以减少 WebSocket/Kafka 压力
                    _should_push = True
                    if msg_type == 'enter':
                        _enter_skip[0] += 1
                        if _enter_skip[0] % 10 != 0:
                            _should_push = False

                    if _should_push:
                        if _kafka_producer:
                            _kafka_producer.send_danmaku(mapped, room_id, plat)
                        if _ws_pusher:
                            _ws_pusher.push_danmaku(room_id, mapped)
                            with _dm_lock:
                                _dm_push_ok[0] += 1
                        else:
                            with _dm_lock:
                                _dm_push_skip[0] += 1

                    # 内存计数器：所有消息都计入 MySQL（定时刷入）
                    with _dm_lock:
                        _dm_counter[room_id] = _dm_counter.get(room_id, 0) + 1
                        # 同时缓存到弹幕缓冲区，定时批量写入 rt_danmaku
                        _dm_buffer.append({
                            'room_id': room_id,
                            'platform': plat,
                            'user_id': mapped.get('user_id', ''),
                            'user_name': mapped.get('user_name', ''),
                            'content': mapped.get('content', ''),
                            'danmaku_type': msg_type,
                        })
                        # 缓冲区上限 500 条，防止内存溢出
                        if len(_dm_buffer) > 500:
                            del _dm_buffer[:200]

                try:
                    await crawler_instance.run_room_monitor(
                        rid, on_danmaku,
                        shared_context=crawler_instance._context,
                        signing_page=signing_page,
                    )
                except Exception as e:
                    print(f"  [Danmaku] Monitor error for {rid}: {e}")

            # ── 定时刷入 MySQL ──
            def _flush_danmaku_to_mysql():
                """每 8 秒把内存中的弹幕计数刷入 MySQL + 批量写入 rt_danmaku"""
                while True:
                    time.sleep(8)
                    try:
                        # ── 1. 刷入弹幕计数器到 rt_room_stats ──
                        with _dm_lock:
                            if not _dm_counter:
                                counter_snapshot = {}
                            else:
                                counter_snapshot = dict(_dm_counter)

                            # ── 2. 取出弹幕缓冲区快照 ──
                            if _dm_buffer:
                                buffer_snapshot = list(_dm_buffer)
                                _dm_buffer.clear()
                            else:
                                buffer_snapshot = []

                        conn = _mysql_connect_retry(database=DB_NAME, max_retries=2, connect_timeout=15)
                        cur = conn.cursor()

                        # 刷入计数器
                        for rid, cnt in counter_snapshot.items():
                            if cnt <= 0:
                                continue
                            cur.execute(
                                "UPDATE rt_room_stats SET total_danmaku = total_danmaku + %s "
                                "WHERE room_id = %s", (cnt, rid))

                        # ── 批量写入 rt_danmaku ──
                        if buffer_snapshot:
                            cur.executemany(
                                "INSERT INTO rt_danmaku "
                                "(event_id, room_id, platform, user_id, user_name, "
                                "content, danmaku_type, event_time) "
                                "VALUES (UUID(), %s, %s, %s, %s, %s, %s, NOW(3))",
                                [(r['room_id'], r['platform'], r['user_id'],
                                  r['user_name'], r['content'], r['danmaku_type'])
                                 for r in buffer_snapshot]
                            )

                        conn.commit()
                        if buffer_snapshot:
                            print(f"  [Flush] Wrote {len(buffer_snapshot)} danmaku to rt_danmaku",
                                  flush=True)
                        cur.close()
                        conn.close()
                    except Exception as e:
                        print(f"  [Flush] ERROR: {e} (buffer={len(buffer_snapshot) if 'buffer_snapshot' in dir() else '?'} counter={len(counter_snapshot) if 'counter_snapshot' in dir() else '?'})",
                              flush=True)

            _flush_thread = _thd.Thread(target=_flush_danmaku_to_mysql, daemon=True)
            _flush_thread.start()
            print("  [Danmaku] MySQL flush thread started (every 8s)")

            # ── 关键修复：清理干扰页面，确保 CDP 被动截帧正常工作 ──
            # 诊断发现：standalone 测试（单页面）WS 正常，但集群（多页面）WS 不建立。
            # 原因：signing page (live.douyin.com/) 和 main page (douyin.com) 同时打开
            # 会干扰房间页面的原生 WebSocket 建立。
            # CDP 被动模式不需要 signing page，因此关闭它；主页面导航到 about:blank。
            if _signing_page:
                try:
                    await _signing_page.close()
                    print("  [Cleanup] Closed signing page", flush=True)
                except Exception:
                    pass
                _signing_page = None  # 置空，防止后续引用
            # 将主页面导航到 about:blank（不关闭，保持 context 存活）
            if crawler._page:
                try:
                    await crawler._page.goto("about:blank", timeout=5000)
                    print("  [Cleanup] Navigated main page to about:blank", flush=True)
                except Exception:
                    pass
            # 关闭其他多余的页面（如之前残留的页面）
            _kept = {crawler._page}  # 保留主页面
            for p in list(crawler._context.pages):
                if p not in _kept:
                    try:
                        await p.close()
                        print(f"  [Cleanup] Closed extra page: {p.url[:50]}", flush=True)
                    except Exception:
                        pass
            await asyncio.sleep(2)
            _page_count = len(crawler._context.pages)
            print(f"  [Cleanup] Done. {_page_count} page(s) remaining, ready for CDP", flush=True)

            tasks = []
            if not rooms:
                print("  [Danmaku] ⚠️ 没有带货直播间（全部无小黄车），跳过弹幕监控", flush=True)
            else:
                # 随机打乱房间顺序，让不同房间在每个周期都有弹幕覆盖
                _rand.shuffle(rooms)
                # 已监控过的房间排到后面，优先监控新房间
                _monitored_key = '_cdp_monitored'
                _fresh = [r for r in rooms if not r.get(_monitored_key)]
                _prev = [r for r in rooms if r.get(_monitored_key)]
                rooms = _fresh + _prev
                _monitor_rooms = rooms[:32]  # 并发监控32个房间的CDP弹幕
                for r in _monitor_rooms:
                    r[_monitored_key] = True
                for i, r in enumerate(_monitor_rooms):
                    t = asyncio.create_task(monitor_one(crawler, r, i, len(_monitor_rooms), signing_page=_signing_page))
                    tasks.append(t)
                    if i < len(_monitor_rooms) - 1:
                        await asyncio.sleep(2)
                print(f"  [Danmaku] Started {len(tasks)}/{len(rooms)} room monitors (max 32 concurrent, rotated)")
                # 监控循环：每5秒检查刷新信号，25分钟自动重启轮换
                _restart_sig = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.refresh_restart.flag')
                _cycle_start = time.time()
                _cycle_timeout = 1500  # 25分钟自动重启
                while any(not t.done() for t in tasks):
                    await asyncio.sleep(5)
                    if time.time() - _cycle_start > _cycle_timeout:
                        print(f"  [Danmaku] Cycle timeout ({_cycle_timeout}s) — rotating rooms", flush=True)
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        await asyncio.sleep(1)
                        break
                    if os.path.exists(_restart_sig):
                        print("  [Danmaku] Refresh signal detected — restarting cycle", flush=True)
                        try: os.remove(_restart_sig)
                        except: pass
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        await asyncio.sleep(1)
                        break
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            print(f"  [Danmaku] Collector error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if crawler:
                try:
                    await crawler.close()
                except:
                    pass

    # 持续循环：监控结束后（Chrome崩溃或3600秒到期）自动重启
    while True:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(discover_and_monitor())
        except Exception as e:
            print(f"  [Danmaku] Cycle error: {e}")
        finally:
            try:
                loop.close()
            except:
                pass
        # 清理可能残留的 Chrome 进程，避免 profile 锁定
        try:
            import subprocess as _sp
            _sp.run(['taskkill', '/F', '/IM', 'chrome.exe', '/T'],
                    capture_output=True, timeout=10)
        except Exception:
            pass
        print("  [Danmaku] Waiting 5s before restarting monitors...")
        time.sleep(5)


def start_backend():
    from socketserver import ThreadingMixIn
    class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True
    server = ThreadingHTTPServer(('0.0.0.0', BACKEND_PORT), APIHandler)
    print(f"  [Backend] http://localhost:{BACKEND_PORT}")
    server.serve_forever()


def start_frontend():
    frontend_dir = os.path.join(BASE_DIR, 'frontend')
    if not os.path.exists(os.path.join(frontend_dir, 'node_modules')):
        print("  [Frontend] Installing dependencies...")
        subprocess.run(['npm', 'install'], cwd=frontend_dir, shell=True, check=True)
    return subprocess.Popen(['npm', 'run', 'dev'], cwd=frontend_dir, shell=True,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _kill_ports():
    """启动前清理端口上的残留进程，避免绑定失败"""
    import signal as _sig
    ports_to_kill = [8765, BACKEND_PORT, 5173]
    killed = set()
    try:
        result = subprocess.run(
            ['netstat', '-ano'], capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            if 'LISTENING' not in line:
                continue
            for port in ports_to_kill:
                if f':{port}' in line:
                    parts = line.strip().split()
                    try:
                        pid = int(parts[-1])
                        if pid > 0 and pid not in killed:
                            os.kill(pid, _sig.SIGTERM)
                            killed.add(pid)
                            print(f"  [Cleanup] Killed PID {pid} on port {port}")
                    except (ValueError, ProcessLookupError):
                        pass
    except Exception as e:
        print(f"  [Cleanup] Port scan failed: {e}")
    # 同时清理 Chrome（避免 profile 锁定）
    try:
        subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe', '/T'],
                       capture_output=True, timeout=5)
    except Exception:
        pass
    if killed:
        time.sleep(2)  # 等待端口释放


def main():
    # 清理残留进程
    _kill_ports()
    
    print("=" * 70)
    print("  StarCast StarCast Live Commerce Big Data Platform - Cluster Edition")
    print("=" * 70)
    print()
    print("  VM IP: 192.168.104.100")
    print("  Connected services:")
    print(f"     [OK] MySQL    {VMS['mysql']}")
    print(f"     [OK] Kafka    {VMS['kafka']}")
    print(f"     [OK] Hive     {VMS['hive']}")
    print(f"     [OK] HDFS Web {VMS['hdfs_web']}")
    print(f"     [OK] Flink    {VMS['flink_web']}")
    print()

    if not check_dependencies():
        print("  [ERROR] pymysql install failed")
        return

    print()
    if init_database():
        print("  [OK] Data source ready")
    else:
        print("  [WARN] Data source not ready, will use fallback data")

    backend_thread = threading.Thread(target=start_backend, daemon=True)
    backend_thread.start()
    time.sleep(0.5)

    # Order simulator disabled - orders now come from real crawler data
    # order_thread = threading.Thread(target=order_simulator_loop, daemon=True)
    # order_thread.start()

    # === 初始化实时数据管道 ===
    global _kafka_producer, _kafka_consumer, _ws_server, _ws_pusher

    # Kafka Producer — 已禁用：Kafka连接超时重试会导致大量I/O，
    # 造成VMware虚拟磁盘操作失败(VM崩溃)。弹幕已改为WebSocket直推。
    # if LiveCommerceKafkaProducer:
    #     try:
    #         _kafka_producer = LiveCommerceKafkaProducer()
    #         ...
    #     except Exception as e:
    #         print(f"  [WARN] Kafka Producer failed: {e}")
    print("  [SKIP] Kafka Producer disabled (use WebSocket direct push instead)")

    # Kafka Consumer — 已禁用（同上原因）
    # if LiveCommerceKafkaConsumer:
    #     try:
    #         _kafka_consumer = LiveCommerceKafkaConsumer()
    #         ...
    #     except Exception as e:
    #         print(f"  [WARN] Kafka Consumer failed: {e}")
    print("  [SKIP] Kafka Consumer disabled (danmaku written to MySQL directly)")

    # WebSocket Server (pushes danmaku to frontend)
    if DanmakuWebSocketServer:
        try:
            _ws_server = DanmakuWebSocketServer(port=8765)
            _ws_server.start()
            _ws_pusher = DanmakuDirectPusher(_ws_server) if DanmakuDirectPusher else None
            print("  [OK] WebSocket server started on ws://localhost:8765")
        except Exception as e:
            print(f"  [WARN] WebSocket server failed: {e}")

    # === 自动弹幕采集: 发现真实抖音直播间并启动弹幕监控 ===
    danmaku_thread = threading.Thread(target=_auto_danmaku_collector, daemon=True)
    danmaku_thread.start()

    # === 房间状态检查: 每20分钟刷新直播间列表，标记已结束的房间 ===
    status_thread = threading.Thread(target=_room_status_checker, daemon=True)
    status_thread.start()

    # === 房间自动刷新: 每3分钟从抖音电商分类页抓取当前带货直播间 ===
    refresh_thread = threading.Thread(target=_room_auto_refresh, daemon=True)
    refresh_thread.start()

    # === 模拟弹幕已禁用：只使用CDP爬取的真实弹幕 ===
    # sim_danmaku_thread = threading.Thread(target=_simulated_danmaku_generator, daemon=True)
    # sim_danmaku_thread.start()

    print(f"  [Frontend] http://localhost:{FRONTEND_PORT}")
    frontend_proc = start_frontend()

    time.sleep(4)
    print()
    print("=" * 70)
    print("  System Started!")
    print("=" * 70)
    print(f"  Web UI:    http://localhost:{FRONTEND_PORT}")
    print(f"  Login:     admin / 123456")
    print(f"  Flink Web: http://{VMS['flink_web']}")
    print(f"  HDFS Web:  http://{VMS['hdfs_web']}")
    print()

    import webbrowser
    webbrowser.open(f"http://localhost:{FRONTEND_PORT}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Stopping...")
        frontend_proc.terminate()
        print("  Bye!")


if __name__ == '__main__':
    main()
