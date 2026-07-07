#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""真实订单模拟器 - 模拟直播间真实下单行为
特性：
  - 每3-8秒可能触发 1~5 个订单（直播带货高峰期）
  - 每个订单独立线程运行，状态时长各异
  - 待支付/已支付/已发货/已签收 都可能同时存在多个
  - 偶尔有订单被取消（5%概率）
  - 发货时长随机（模拟不同物流速度）
"""
import pymysql
import time
import random
import threading
from datetime import datetime

MYSQL = {'host': '192.168.104.100', 'port': 3306, 'user': 'root', 'password': '123456',
         'database': 'livecommerce_db', 'charset': 'utf8mb4'}

products = [
    ('华为 Mate70 Pro', '华为旗舰数码专场', 5999),
    ('花西子蜜粉饼', '花西子美妆专场', 358),
    ('三只松鼠坚果大礼包', '三只松鼠食品专场', 168),
    ('波司登轻薄羽绒服', '波司登服饰专场', 1299),
    ('戴森 V15 吸尘器', '美的家电专场', 3999),
    ('安踏 C37 跑鞋', '安踏运动专场', 499),
    ('小米智能手表', '小米官方数码专场', 899),
    ('良品铺子零食组合', '三只松鼠食品专场', 88),
    ('珀莱雅双抗精华', '花西子美妆专场', 269),
    ('李宁篮球鞋', '安踏运动专场', 599),
    ('苹果 AirPods Pro', '华为旗舰数码专场', 1799),
    ('完美日记眼影盘', '花西子美妆专场', 129),
    ('蕉下防晒衣', '波司登服饰专场', 299),
    ('海尔冰箱', '美的家电专场', 3299),
    ('漫步者蓝牙音箱', '小米官方数码专场', 399),
    ('全聚德烤鸭礼盒', '三只松鼠食品专场', 198),
    ('Chanel 口红', '花西子美妆专场', 350),
    ('Redmi K70 至尊版', '华为旗舰数码专场', 2999),
    ('海澜之家 Polo 衫', '波司登服饰专场', 159),
    ('九阳豆浆机', '美的家电专场', 499),
]
users = ['杭州张女士', '上海李先生', '北京王先生', '广州赵女士', '深圳刘先生',
         '成都陈先生', '武汉黄女士', '南京吴女士', '杭州林女士', '重庆周先生',
         '西安冯先生', '长沙蒋女士', '厦门沈女士', '青岛韩先生', '天津杨女士']
platforms = ['douyin', 'taobao', 'kuaishou']

conn_lock = threading.Lock()
order_counter = [0]

def get_conn():
    return pymysql.connect(**MYSQL)

def init_counter():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT IFNULL(MAX(id), 0) + 1000 FROM order_info")
    n = cur.fetchone()[0]
    conn.close()
    order_counter[0] = n
    return n

def db_exec(sql, params=(), commit=True):
    with conn_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        if commit: conn.commit()
        conn.close()

def insert_order(order_no, prod, user, qty, platform):
    """返回实际 MySQL 自增 ID"""
    with conn_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO order_info (order_no, product_name, room_name, username, quantity, total_amount, platform, status, create_time) VALUES (%s,%s,%s,%s,%s,%s,%s,'pending',NOW())",
            (order_no, prod[0], prod[1], user, qty, prod[2] * qty, platform))
        oid = cur.lastrowid
        conn.commit()
        conn.close()
        return oid

def update_status(oid, new_status, room_name=None, amount=0):
    with conn_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE order_info SET status=%s WHERE id=%s", (new_status, oid))
        if room_name and new_status == 'paid':
            cur.execute("UPDATE live_room SET order_count=order_count+1, gmv=gmv+%s WHERE room_name=%s", (amount, room_name))
        conn.commit()
        conn.close()

def skewed_delay(min_val, max_val, mode='normal'):
    """生成偏态分布的延迟（秒），模拟真实用户行为"""
    span = max_val - min_val
    if mode == 'quick':
        # 偏向短的延迟（秒拍秒付，快速处理）
        d = int(min_val + (random.random() ** 2) * span)
    elif mode == 'slow':
        # 偏向长的延迟（犹豫型买家，偏远物流）
        d = int(max_val - (random.random() ** 2) * span)
    elif mode == 'lognormal':
        # 长尾分布：大部分在低区间，偶尔很长
        d = int(min_val + (random.random() ** 1.5) * span)
    else:
        d = random.randint(min_val, max_val)
    return max(min_val, min(d, max_val))


def run_order_lifecycle(oid, order_no, prod, user, qty, platform, amount):
    """单个订单的完整生命周期（独立线程）"""
    try:
        # ---- Stage 1: 待支付 ----
        # 大部分用户5-60秒支付，10%的用户犹豫很久（1-3分钟）
        if random.random() < 0.10:
            pay_delay = skewed_delay(60, 180, 'slow')  # 犹豫型
        elif random.random() < 0.30:
            pay_delay = skewed_delay(10, 45, 'quick')  # 秒拍
        else:
            pay_delay = skewed_delay(15, 60, 'lognormal')  # 普通
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] PENDING: {order_no} {prod[0]} ({user}) will pay in {pay_delay}s")
        time.sleep(pay_delay)

        # 5% 取消订单（超时未付/改变主意）
        if random.random() < 0.05:
            update_status(oid, 'cancelled')
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] CANCELLED: {order_no}")
            return

        # ---- Stage 2: 已支付 ----
        update_status(oid, 'paid', prod[1], amount)
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] PAID: {order_no} ￥{amount}")

        # ---- Stage 3: 待发货（商家处理） ----
        # 商家处理时间：大部分5-30秒，偶尔很慢（模拟库存不足/大促爆单）
        if random.random() < 0.08:
            ship_delay = skewed_delay(40, 120, 'slow')  # 爆单延迟
        else:
            ship_delay = skewed_delay(5, 30, 'quick')
        time.sleep(ship_delay)

        update_status(oid, 'shipped')
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] SHIPPED: {order_no}")

        # ---- Stage 4: 运输中 ----
        # 物流时间：10-90秒不等（同城快/跨省慢）
        if random.random() < 0.20:
            transit = skewed_delay(60, 180, 'lognormal')  # 偏远地区
        elif random.random() < 0.25:
            transit = skewed_delay(10, 30, 'quick')  # 同城/顺丰
        else:
            transit = skewed_delay(20, 80, 'lognormal')  # 普通快递
        time.sleep(transit)

        # ---- Stage 5: 已签收 ----
        update_status(oid, 'delivered')
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] DELIVERED: {order_no}")

    except Exception as e:
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] [ERR] {order_no}: {e}")


if __name__ == '__main__':
    print("=" * 60)
    print("  直播电商实时订单模拟器 v2")
    print("  模拟真实下单→支付→发货→签收流程")
    print("=" * 60)
    init_counter()
    active_threads = []

    while True:
        try:
            # 批次生成：1~5个订单同时产生（直播间高峰期）
            batch_size = random.choices([1, 2, 3, 4, 5], weights=[15, 30, 30, 15, 10])[0]

            # 如果是大单，同期持续创建（模拟直播间爆款时刻）
            spawn_interval = random.uniform(0.3, 1.5) if batch_size > 1 else 0

            for i in range(batch_size):
                prod = random.choice(products)
                user = random.choice(users)
                plat = random.choice(platforms)
                qty = random.choices([1, 2, 3, 5], weights=[50, 30, 15, 5])[0]
                order_no = f"ORDER{order_counter[0]}"
                order_counter[0] += 1
                amount = prod[2] * qty

                oid = insert_order(order_no, prod, user, qty, plat)

                # 每个订单独立线程运行生命周期
                t = threading.Thread(target=run_order_lifecycle,
                                     args=(oid, order_no, prod, user, qty, plat, amount),
                                     daemon=True)
                t.start()
                active_threads.append(t)
                # 清理已完成的线程
                active_threads = [t for t in active_threads if t.is_alive()]

                if batch_size > 1 and i < batch_size - 1:
                    time.sleep(spawn_interval)

            # 随机等待3-12秒后生成下一批
            wait = random.randint(3, 12)
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] batch of {batch_size} created, {len(active_threads)} orders active, waiting {wait}s...")
            time.sleep(wait)

        except KeyboardInterrupt:
            print(f"\n[Stop] Shutting down ({len(active_threads)} orders in progress)...")
            break
        except Exception as e:
            print(f"  [Error] {e}")
            time.sleep(2)
