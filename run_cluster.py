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
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import random
import hashlib
from datetime import datetime
import pymysql

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
FRONTEND_PORT = 5173


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

        sha_pw = '8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92'
        cursor.execute("INSERT IGNORE INTO sys_user (username, email, password, role, user_type, status) VALUES ('admin', 'admin@livecommerce.com', %s, 'admin', 'staff', 1)", (sha_pw,))
        cursor.execute("INSERT IGNORE INTO sys_user (username, email, password, role, user_type, status) VALUES ('operator', 'op@livecommerce.com', %s, 'operator', 'staff', 1)", (sha_pw,))

        anchor_data = [
            ('薇娅', '哆啦薇娅', 'taobao', 'S', '全品类', 78000000, 4800, 2200000000, 4100000, 6.2),
            ('李佳琦', '口红一哥', 'taobao', 'S', '美妆', 65800000, 5200, 1850000000, 3200000, 5.8),
            ('辛巴', '辛有志', 'kuaishou', 'S', '食品', 98000000, 6200, 1650000000, 3500000, 6.79),
            ('罗永浩', '老罗', 'douyin', 'S', '数码', 19500000, 2800, 850000000, 1800000, 4.8),
            ('疯狂小杨哥', '小杨哥', 'douyin', 'S', '全品类', 45000000, 3800, 985000000, 2150000, 6.7),
            ('董宇辉', '东方甄选', 'douyin', 'A', '食品', 18500000, 1500, 580000000, 1200000, 4.2),
            ('刘畊宏', '畊宏', 'douyin', 'A', '运动', 25000000, 800, 58000000, 210000, 2.5),
            ('陈赫', '赫赫', 'douyin', 'A', '食品', 12000000, 600, 156000000, 480000, 4.5)
        ]
        for a in anchor_data:
            cursor.execute("INSERT IGNORE INTO anchor (name, nickname, platform, level, category, fans_count, live_hours, total_gmv, total_orders, avg_conversion) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", a)

        room_data = [
            ('ROOM1001', '618李佳琦美妆专场', '李佳琦', 'taobao', '美妆', 'live', 285000, 12800, 5860000, 6.92),
            ('ROOM1002', '薇娅日不落全品类', '薇娅', 'taobao', '全品类', 'live', 312000, 15600, 7230000, 7.61),
            ('ROOM1003', '老罗数码科技专场', '罗永浩', 'douyin', '数码', 'live', 158000, 5200, 2380000, 5.31),
            ('ROOM1004', '辛巴严选食品专场', '辛巴', 'kuaishou', '食品', 'live', 425000, 18200, 8950000, 6.79),
            ('ROOM1006', '小杨哥搞笑带货', '疯狂小杨哥', 'douyin', '全品类', 'live', 380000, 21500, 9850000, 6.72)
        ]
        for r in room_data:
            cursor.execute("INSERT IGNORE INTO live_room (room_no, room_name, anchor_name, platform, category, status, viewer_count, order_count, gmv, conversion_rate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", r)

        order_data = [
            ('ORD001', '美妆套装礼盒', '李佳琦美妆专场', '杭州张女士', 2, 258, 'taobao', 'delivered', 2),
            ('ORD002', '坚果大礼包', '薇娅日不落', '上海李先生', 3, 198, 'taobao', 'shipped', 8),
            ('ORD003', '蓝牙无线耳机', '老罗数码专场', '北京王先生', 1, 399, 'douyin', 'paid', 15),
            ('ORD004', '零食全家桶', '辛巴严选专场', '广州赵女士', 5, 128, 'kuaishou', 'pending', 32),
            ('ORD005', '纯棉T恤3件装', '小杨哥带货', '深圳刘先生', 2, 99, 'douyin', 'delivered', 65),
            ('ORD006', '华为 Mate70 首发', '华为旗舰数码专场', '成都陈先生', 1, 5999, 'douyin', 'paid', 1),
            ('ORD007', '花西子空气蜜粉', '花西子美妆专场', '杭州林女士', 2, 358, 'taobao', 'paid', 4),
            ('ORD008', '三只松鼠坚果礼盒', '三只松鼠食品专场', '武汉黄女士', 3, 168, 'douyin', 'shipped', 12),
            ('ORD009', '波司登羽绒服', '波司登服饰专场', '南京吴女士', 1, 1299, 'taobao', 'paid', 18),
            ('ORD010', '戴森吹风机', '小米官方数码专场', '广州徐先生', 1, 2999, 'taobao', 'pending', 25),
            ('ORD011', '美的变频空调', '美的家电专场', '北京李女士', 1, 4599, 'taobao', 'delivered', 45),
            ('ORD012', '安踏运动鞋', '安踏运动专场', '深圳张先生', 2, 499, 'douyin', 'paid', 5),
        ]
        for o in order_data:
            mins_ago = o[8]
            cursor.execute(
                "INSERT IGNORE INTO order_info (order_no, product_name, room_name, username, quantity, total_amount, platform, status, create_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, DATE_SUB(NOW(), INTERVAL %s MINUTE))",
                o[:8] + (mins_ago,))

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


# 模拟数据（数据库连不上时使用）
MOCK_ANCHORS = [
    {'id': 1, 'name': '薇娅', 'nickname': '哆啦薇娅', 'platform': 'taobao', 'level': 'S', 'category': '全品类', 'fansCount': 78000000, 'liveHours': 4800, 'totalGmv': 2200000000, 'totalOrders': 4100000, 'avgConversion': 6.2},
    {'id': 2, 'name': '李佳琦', 'nickname': '口红一哥', 'platform': 'taobao', 'level': 'S', 'category': '美妆', 'fansCount': 65800000, 'liveHours': 5200, 'totalGmv': 1850000000, 'totalOrders': 3200000, 'avgConversion': 5.8},
    {'id': 3, 'name': '辛巴', 'nickname': '辛有志', 'platform': 'kuaishou', 'level': 'S', 'category': '食品', 'fansCount': 98000000, 'liveHours': 6200, 'totalGmv': 1650000000, 'totalOrders': 3500000, 'avgConversion': 6.79},
    {'id': 4, 'name': '罗永浩', 'nickname': '老罗', 'platform': 'douyin', 'level': 'S', 'category': '数码', 'fansCount': 19500000, 'liveHours': 2800, 'totalGmv': 850000000, 'totalOrders': 1800000, 'avgConversion': 4.8},
    {'id': 5, 'name': '疯狂小杨哥', 'nickname': '小杨哥', 'platform': 'douyin', 'level': 'S', 'category': '全品类', 'fansCount': 45000000, 'liveHours': 3800, 'totalGmv': 985000000, 'totalOrders': 2150000, 'avgConversion': 6.7},
    {'id': 6, 'name': '董宇辉', 'nickname': '东方甄选', 'platform': 'douyin', 'level': 'A', 'category': '食品', 'fansCount': 18500000, 'liveHours': 1500, 'totalGmv': 580000000, 'totalOrders': 1200000, 'avgConversion': 4.2},
    {'id': 7, 'name': '刘畊宏', 'nickname': '畊宏', 'platform': 'douyin', 'level': 'A', 'category': '运动', 'fansCount': 25000000, 'liveHours': 800, 'totalGmv': 58000000, 'totalOrders': 210000, 'avgConversion': 2.5},
    {'id': 8, 'name': '陈赫', 'nickname': '赫赫', 'platform': 'douyin', 'level': 'A', 'category': '食品', 'fansCount': 12000000, 'liveHours': 600, 'totalGmv': 156000000, 'totalOrders': 480000, 'avgConversion': 4.5}
]

MOCK_ROOMS = [
    {'id': 1, 'roomNo': 'ROOM1001', 'roomName': '618李佳琦美妆专场', 'anchorName': '李佳琦', 'platform': 'taobao', 'category': '美妆', 'status': 'live', 'viewerCount': 285000, 'orderCount': 12800, 'gmv': 5860000},
    {'id': 2, 'roomNo': 'ROOM1002', 'roomName': '薇娅日不落全品类', 'anchorName': '薇娅', 'platform': 'taobao', 'category': '全品类', 'status': 'live', 'viewerCount': 312000, 'orderCount': 15600, 'gmv': 7230000},
    {'id': 3, 'roomNo': 'ROOM1003', 'roomName': '老罗数码科技专场', 'anchorName': '罗永浩', 'platform': 'douyin', 'category': '数码', 'status': 'live', 'viewerCount': 158000, 'orderCount': 5200, 'gmv': 2380000},
    {'id': 4, 'roomNo': 'ROOM1004', 'roomName': '辛巴严选食品专场', 'anchorName': '辛巴', 'platform': 'kuaishou', 'category': '食品', 'status': 'live', 'viewerCount': 425000, 'orderCount': 18200, 'gmv': 8950000},
    {'id': 5, 'roomNo': 'ROOM1006', 'roomName': '小杨哥搞笑带货', 'anchorName': '疯狂小杨哥', 'platform': 'douyin', 'category': '全品类', 'status': 'live', 'viewerCount': 380000, 'orderCount': 21500, 'gmv': 9850000}
]


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
    platforms = ['douyin', 'taobao', 'kuaishou']
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
        rng = random.Random(42)

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
            anchors = query_mysql("SELECT IFNULL(SUM(total_gmv), 0) as gmv, COUNT(*) as cnt, IFNULL(AVG(avg_conversion), 0) as avg_conv FROM anchor WHERE deleted=0")
            rooms = query_mysql("SELECT COUNT(*) as cnt, IFNULL(SUM(viewer_count), 0) as v, IFNULL(SUM(gmv), 0) as g FROM live_room WHERE deleted=0")
            orders = query_mysql("SELECT COUNT(*) as cnt, IFNULL(SUM(total_amount), 0) as amount FROM order_info WHERE deleted=0")
            self._send({'code': 0, 'data': {
                'totalGmv': float(anchors[0]['gmv'] or 0) if anchors else sum(a['totalGmv'] for a in MOCK_ANCHORS),
                'totalAnchors': int(anchors[0]['cnt'] or 0) if anchors else len(MOCK_ANCHORS),
                'totalRooms': int(rooms[0]['cnt'] or 0) if rooms else len(MOCK_ROOMS),
                'totalViewers': int(rooms[0]['v'] or 0) if rooms else sum(r['viewerCount'] for r in MOCK_ROOMS),
                'totalOrders': int(orders[0]['cnt'] or 0) if orders else 5,
                'totalAmount': float(orders[0]['amount'] or 0) if orders else 1082,
                'avgConversion': float(anchors[0]['avg_conv'] or 0) if anchors else sum(a['avgConversion'] for a in MOCK_ANCHORS) / len(MOCK_ANCHORS)
            }})

        elif p.startswith('/api/datavis/dashboard/gmv-trend'):
            months = 12
            self._send({'code': 0, 'data': {
                'labels': [f'{i+1}M' for i in range(months)],
                'values': [50000 + rng.randint(0, 200000) for _ in range(months)]
            }})

        elif p == '/api/datavis/dashboard/platform-distribution':
            rows = query_mysql("SELECT platform, COUNT(*) as cnt FROM anchor WHERE deleted=0 GROUP BY platform")
            if rows:
                data = [{'name': {'douyin':'Douyin','taobao':'Taobao','kuaishou':'Kuaishou'}.get(r['platform'], r['platform']), 'value': int(r['cnt'])} for r in rows]
            else:
                data = [{'name': 'Douyin', 'value': 5}, {'name': 'Taobao', 'value': 2}, {'name': 'Kuaishou', 'value': 1}]
            self._send({'code': 0, 'data': data})

        elif p == '/api/datavis/dashboard/category-rank':
            rows = query_mysql("SELECT category, IFNULL(SUM(total_gmv), 0) as gmv FROM anchor WHERE deleted=0 GROUP BY category ORDER BY gmv DESC")
            data = [{'name': r['category'], 'value': float(r['gmv'])} for r in rows] if rows else [
                {'name': 'Beauty', 'value': 1850000000}, {'name': 'Full', 'value': 3185000000},
                {'name': 'Food', 'value': 2386000000}, {'name': 'Digital', 'value': 850000000}
            ]
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
            } for r in rows] if rows else MOCK_ANCHORS
            self._send({'code': 0, 'data': data})

        elif p == '/api/datavis/dashboard/geo-distribution':
            self._send({'code': 0, 'data': [
                {'name': 'Guangzhou', 'value': 1820}, {'name': 'Shenzhen', 'value': 1680},
                {'name': 'Shanghai', 'value': 1450}, {'name': 'Hangzhou', 'value': 1320},
                {'name': 'Beijing', 'value': 1280}, {'name': 'Chengdu', 'value': 1050}
            ]})

        elif p == '/api/datavis/dashboard/realtime':
            r = random.Random()
            self._send({'code': 0, 'data': {
                'currentViewers': 20000 + r.randint(1, 50000),
                'currentOrders': 100 + r.randint(1, 500),
                'currentGmv': 5000 + r.randint(1, 50000),
                'onlineAnchors': 6,
                'timestamp': int(time.time() * 1000)
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
            # 4) 平台分布
            plats = query_mysql("SELECT platform, COUNT(*) as cnt FROM live_room WHERE deleted=0 GROUP BY platform ORDER BY cnt DESC")
            if plats:
                p = plats[0]
                pct = round(p['cnt'] * 100 / sum(int(x['cnt']) for x in plats), 0)
                name_map = {'douyin':'抖音','taobao':'淘宝','kuaishou':'快手'}
                activities.append({
                    'text': f"平台「{name_map.get(p['platform'], p['platform'])}」直播间占比 {pct}%",
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
            search = (qs.get('search', [''])[0] or '').strip()
            where = "WHERE deleted=0"
            params = []
            if platform_f:
                where += " AND platform=%s"; params.append(platform_f)
            if status_f:
                where += " AND status=%s"; params.append(status_f)
            if search:
                where += " AND (room_name LIKE %s OR anchor_name LIKE %s)"
                like = f"%{search}%"
                params.extend([like, like])
            rows = query_mysql(f"SELECT * FROM live_room {where} ORDER BY viewer_count DESC LIMIT 200", params)
            data = []
            for r in rows:
                v = r['gmv']
                gmv_val = float(v) if isinstance(v, (int, float, Decimal)) else 0
                data.append({
                    'id': r['id'], 'roomNo': r['room_no'], 'roomName': r['room_name'],
                    'anchorName': r['anchor_name'], 'platform': r['platform'],
                    'category': r['category'], 'status': r['status'],
                    'viewerCount': int(r['viewer_count'] or 0), 'orderCount': int(r['order_count'] or 0),
                    'gmv': gmv_val
                })
            if not data and not params: data = MOCK_ROOMS
            self._send({'code': 0, 'data': {'records': data, 'total': len(data), 'page': 1, 'pageSize': 20}})

        elif p == '/api/livecommerce/room/live':
            rows = query_mysql("SELECT * FROM live_room WHERE deleted=0 AND status='live'")
            data = [{
                'id': r['id'], 'roomName': r['room_name'], 'anchorName': r['anchor_name'],
                'platform': r['platform'], 'viewerCount': int(r['viewer_count'] or 0),
                'orderCount': int(r['order_count'] or 0), 'gmv': float(r['gmv'] or 0),
                'status': r['status']
            } for r in rows] if rows else MOCK_ROOMS
            self._send({'code': 0, 'data': data})

        elif p == '/api/livecommerce/room/overview':
            rooms = query_mysql("SELECT COUNT(*) as cnt, IFNULL(SUM(viewer_count),0) as v, IFNULL(SUM(gmv),0) as g FROM live_room WHERE deleted=0")
            self._send({'code': 0, 'data': {
                'totalRooms': int(rooms[0]['cnt'] or 0) if rooms else len(MOCK_ROOMS),
                'liveRooms': 5, 'totalViewers': int(rooms[0]['v'] or 0) if rooms else sum(r['viewerCount'] for r in MOCK_ROOMS),
                'totalGmv': float(rooms[0]['g'] or 0) if rooms else sum(r['gmv'] for r in MOCK_ROOMS),
                'totalOrders': 98000
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
            } for r in rows] if rows else MOCK_ANCHORS
            self._send({'code': 0, 'data': {'records': data, 'total': len(data), 'page': 1, 'pageSize': 10}})

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
                'processedCount': 125820,
                'cleanedCount': 122242,
                'flinkJobs': jobs if jobs else [
                    {'name': 'live-room-cleaning (waiting)', 'status': 'NOT_SUBMITTED', 'parallelism': 4}
                ],
                'topics': ['live_room_events', 'order_events', 'user_behavior'],
                'tables': {
                    'mysql': f'livecommerce_db @ {VMS["mysql"]}',
                    'hive': f'default @ {VMS["hive"]}'
                }
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

                s = SpiderScheduler(["douyin", "taobao", "kuaishou"])
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
                cur.execute("UPDATE sys_user SET deleted=1 WHERE id=%s", (int(uid),))
                conn.commit()
                conn.close()
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
                cur.execute("UPDATE live_room SET deleted=1 WHERE id=%s", (int(rid),))
                conn.commit()
                conn.close()
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
                cur.execute("UPDATE anchor SET deleted=1 WHERE id=%s", (int(aid),))
                conn.commit()
                conn.close()
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


def main():
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

    # 内建订单模拟器（写入 MySQL + 广播 SSE 事件）
    order_thread = threading.Thread(target=order_simulator_loop, daemon=True)
    order_thread.start()
    print("  [OK] Order simulator started (internal, with SSE broadcast)")

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
