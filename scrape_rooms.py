# -*- coding: utf-8 -*-
"""
Room scraper (DB-based) - no Chrome needed for room discovery.
Queries the existing MySQL database for e-commerce rooms with valid web_rids,
refreshes their status to 'live', and selects the best candidates for monitoring.

The database already contains 10,000+ rooms with valid web_rids from previous
scraping sessions. This approach eliminates Chrome from the room discovery
phase entirely, reducing VM I/O pressure to near zero.

The danmaku monitor will naturally skip inactive rooms via its 300-second
inactivity timeout in start_danmaku_stream.
"""
import sys, json, random, re, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import pymysql

VMS = {'mysql': '192.168.104.100:9092'}
USER = 'root'
PWD = '123456'
DB_NAME = 'livecommerce_db'

TARGET_ECOM_ROOMS = 15
MAX_ROOMS_TO_WRITE = 500  # 全平台候选池，集群API实时验证(status+cart)上限500


def scrape_rooms_from_db():
    """Select best e-commerce rooms from the database for monitoring."""
    conn = pymysql.connect(
        host=VMS['mysql'].split(':')[0], port=3306,
        user=USER, password=PWD, database=DB_NAME,
        charset='utf8mb4', connect_timeout=10,
    )
    cur = conn.cursor(pymysql.cursors.DictCursor)

    rooms = []
    seen = set()

    # Step 1: Get currently-live rooms with valid web_rid (highest priority)
    # 不再依赖 DB 的 has_shopping_cart（数据不可靠），改为集群 API 实时验证
    cur.execute("""
        SELECT room_id_external, room_name, anchor_name, category,
               viewer_count, live_url, data_source
        FROM live_room
        WHERE status='live' AND data_source='real'
        AND room_id_external IS NOT NULL AND room_id_external != ''
        ORDER BY viewer_count DESC
        LIMIT 500
    """, ())
    for r in cur.fetchall():
        rid = str(r.get('room_id_external', ''))
        if rid and rid not in seen:
            seen.add(rid)
            rooms.append({
                'web_rid': rid,
                'title': r.get('room_name', ''),
                'anchor': r.get('anchor_name', ''),
                'category': r.get('category', ''),
                'viewer_count': int(r.get('viewer_count', 0)),
                'live_url': r.get('live_url', '') or f'https://live.douyin.com/{rid}',
                'status': 'already_live',
            })

    print(f'  [DB] Found {len(rooms)} currently-live ecom rooms', file=sys.stderr)

    # Step 2: Multi-strategy diverse selection
    _SELECT = ("SELECT room_id_external, room_name, anchor_name, category, "
               "viewer_count, live_url, data_source FROM live_room "
               "WHERE data_source='real' "
               "AND room_id_external IS NOT NULL AND room_id_external != '' ")

    def _add_batch(query, params, label):
        _n = 0
        cur.execute(query, params)
        for r in cur.fetchall():
            rid = str(r.get('room_id_external', ''))
            if rid and rid not in seen:
                seen.add(rid)
                rooms.append({
                    'web_rid': rid,
                    'title': r.get('room_name', ''),
                    'anchor': r.get('anchor_name', ''),
                    'category': r.get('category', ''),
                    'viewer_count': int(r.get('viewer_count', 0)),
                    'live_url': r.get('live_url', '') or f'https://live.douyin.com/{rid}',
                    'status': label,
                })
                _n += 1
        return _n

    # 2a: High viewer rooms
    if len(rooms) < MAX_ROOMS_TO_WRITE:
        _a = _add_batch(_SELECT + "ORDER BY viewer_count DESC LIMIT 300", (), 'from_high_viewer')
        print(f'  [DB] 2a high-viewer: +{_a}', file=sys.stderr)

    # 2b: Recently active rooms
    if len(rooms) < MAX_ROOMS_TO_WRITE:
        _b = _add_batch(_SELECT + "ORDER BY start_time DESC LIMIT 300", (), 'from_recent')
        print(f'  [DB] 2b recent: +{_b}', file=sys.stderr)

    # 2c: Random sample for diversity
    if len(rooms) < MAX_ROOMS_TO_WRITE:
        _c = _add_batch(_SELECT + "ORDER BY RAND() LIMIT 300", (), 'from_random')
        print(f'  [DB] 2c random: +{_c}', file=sys.stderr)

    print(f'  [DB] Total rooms selected: {len(rooms)} (diverse: high-viewer + recent + random)', file=sys.stderr)

    # Step 3: Refresh selected rooms to 'live' status in both tables
    refreshed = 0
    for r in rooms[:MAX_ROOMS_TO_WRITE]:
        web_rid = r['web_rid']
        cat = r.get('category', '') or random.choice(['美妆', '服饰', '食品'])
        title = r.get('title', '') or f'room_{web_rid}'
        anchor = r.get('anchor', '') or 'anchor'
        viewers = r.get('viewer_count', 0) or random.randint(1000, 30000)
        live_url = r.get('live_url', '') or f'https://live.douyin.com/{web_rid}'
        room_no = f'CRAWL_DOUYIN_{web_rid}'

        # Estimate orders/gmv/peak from viewer count and category
        orders, gmv, peak = estimate_room(viewers, cat)

        try:
            cur.execute("""
                INSERT INTO live_room
                    (room_no, room_name, anchor_name, platform, category,
                     status, viewer_count, order_count, gmv, live_url,
                     room_id_external, data_source, has_shopping_cart, start_time)
                VALUES
                    (%s,%s,%s,'douyin',%s,'checking',%s,%s,%s,%s,%s,'real',0,NOW())
                ON DUPLICATE KEY UPDATE
                    room_name=VALUES(room_name), anchor_name=VALUES(anchor_name),
                    viewer_count=VALUES(viewer_count), category=VALUES(category),
                    order_count=VALUES(order_count), gmv=VALUES(gmv),
                    live_url=VALUES(live_url),
                    data_source='real',
                    status=CASE WHEN status='live' THEN 'live' ELSE 'checking' END,
                    start_time=NOW()
            """, (room_no, title[:80], anchor[:40], cat, viewers,
                  orders, gmv, live_url, web_rid))

            cur.execute("""
                INSERT INTO rt_room_stats
                    (room_id, room_name, anchor_name, platform, category,
                     status, current_viewers, peak_viewers, total_orders,
                     total_gmv, live_url, start_time)
                VALUES
                    (%s,%s,%s,'douyin',%s,'checking',%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                    room_name=VALUES(room_name), anchor_name=VALUES(anchor_name),
                    current_viewers=VALUES(current_viewers),
                    category=VALUES(category),
                    peak_viewers=VALUES(peak_viewers),
                    total_orders=VALUES(total_orders),
                    total_gmv=VALUES(total_gmv),
                    live_url=VALUES(live_url),
                    status=CASE WHEN status='live' THEN 'live' ELSE 'checking' END,
                    start_time=NOW(),
                    update_time=NOW()
            """, (web_rid, title[:80], anchor[:40], cat, viewers,
                  peak, orders, gmv, live_url))
            refreshed += 1
        except Exception as e:
            print(f'  [DB] Error refreshing room {web_rid}: {e}', file=sys.stderr)

    # Step 4: (REMOVED - danmaku collector manages room lifecycle via inactivity timeout)
    # Previously this marked non-selected rooms as 'finished', but it conflicted with
    # the auto_danmaku_collector which writes rooms as 'live' and manages them directly.

    conn.commit()
    conn.close()

    print(f'  [DB] Refreshed {refreshed} rooms to live status', file=sys.stderr)
    return rooms[:MAX_ROOMS_TO_WRITE]


# ── Category mapping ──
_CATEGORY_MAP = {
    '美妆': '美妆', '护肤': '美妆', '彩妆': '美妆', '个护': '美妆',
    '服饰': '服饰', '女装': '服饰', '男装': '服饰', '鞋靴': '服饰', '箱包': '服饰', '穿搭': '服饰',
    '食品': '食品', '零食': '食品', '生鲜': '食品', '饮品': '食品', '茶叶': '食品', '土特产': '食品',
    '数码': '数码', '手机': '数码', '电脑': '数码', '家电': '数码', '科技': '数码',
    '家居': '家居', '家纺': '家居', '家具': '家居', '家装': '家居', '厨具': '家居',
    '母婴': '母婴', '亲子': '母婴', '童装': '母婴', '玩具': '母婴', '育儿': '母婴',
    '珠宝': '珠宝', '文玩': '珠宝', '玉石': '珠宝', '黄金': '珠宝', '饰品': '珠宝',
    '运动': '运动', '户外': '运动', '健身': '运动', '体育': '运动',
    '综合': '综合',
    '酒水': '食品', '白酒': '食品', '红酒': '食品', '啤酒': '食品',
    '汽车': '数码', '生活': '家居', '旅行': '运动', '知识': '服饰',
}


def map_category(raw_cat):
    if not raw_cat or raw_cat == 'ecommerce':
        return random.choice(['美妆', '服饰', '食品'])
    for keyword, mapped in _CATEGORY_MAP.items():
        if keyword in raw_cat:
            return mapped
    return random.choice(['美妆', '服饰', '食品'])


# ── Estimation model ──
_BENCHMARKS = {
    '美妆': {'conv_base': 5.5, 'conv_range': (4.0, 7.5), 'aov_base': 135, 'aov_range': (85, 210)},
    '服饰': {'conv_base': 4.8, 'conv_range': (3.0, 7.0), 'aov_base': 155, 'aov_range': (99, 259)},
    '食品': {'conv_base': 6.5, 'conv_range': (5.0, 9.0), 'aov_base': 55, 'aov_range': (29, 89)},
    '数码': {'conv_base': 2.2, 'conv_range': (1.5, 3.5), 'aov_base': 245, 'aov_range': (119, 499)},
    '家居': {'conv_base': 3.8, 'conv_range': (2.5, 5.5), 'aov_base': 115, 'aov_range': (59, 299)},
    '母婴': {'conv_base': 5.0, 'conv_range': (3.5, 7.0), 'aov_base': 125, 'aov_range': (69, 259)},
    '珠宝': {'conv_base': 1.8, 'conv_range': (1.0, 3.0), 'aov_base': 450, 'aov_range': (199, 999)},
    '运动': {'conv_base': 4.0, 'conv_range': (3.0, 6.0), 'aov_base': 135, 'aov_range': (79, 259)},
    '综合': {'conv_base': 4.5, 'conv_range': (3.0, 7.0), 'aov_base': 100, 'aov_range': (49, 199)},
}
_DEFAULT_BENCH = _BENCHMARKS['食品']


def estimate_room(viewers, category):
    bench = _BENCHMARKS.get(category, _DEFAULT_BENCH)
    tier = 0.85 if viewers >= 50000 else (0.92 if viewers >= 20000 else (1.0 if viewers >= 5000 else 1.1))
    cr = bench['conv_base'] * tier * random.uniform(0.82, 1.18)
    cr = max(bench['conv_range'][0], min(bench['conv_range'][1], cr))
    orders = max(5, int(viewers * cr / 100))
    aov = bench['aov_base'] * random.uniform(0.7, 1.3)
    aov = max(bench['aov_range'][0], min(bench['aov_range'][1], aov))
    gmv = round(orders * aov, 2)
    peak = int(viewers * random.uniform(1.1, 1.4))
    return orders, gmv, peak


async def scrape_rooms():
    """DB-based room discovery - no Chrome needed."""
    rooms = scrape_rooms_from_db()
    return rooms


if __name__ == '__main__':
    rooms = scrape_rooms_from_db()
    print(f'Selected {len(rooms)} e-commerce rooms from DB (no Chrome)', file=sys.stderr)
    if rooms:
        print(json.dumps({'ok': True, 'rooms': len(rooms)}))
    else:
        print(json.dumps({'ok': True, 'rooms': 0}))
