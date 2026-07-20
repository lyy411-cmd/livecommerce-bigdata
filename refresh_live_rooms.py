"""
Refresh live rooms by scraping Douyin directory page.
Can be run standalone or triggered via the backend API.
"""
import sys
import json
import re
import time
import pymysql
import random
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

MYSQL_HOST = '192.168.104.100'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PWD = '123456'
MYSQL_DB = 'livecommerce_db'

COMMENT_POOL = [
    '这个好看', '主播厉害', '已下单', '求链接', '666', '太划算了', '买了买了',
    '质量怎么样', '发货快吗', '支持主播', '关注了', '新人报到', '老粉来了',
    '多少钱', '有优惠吗', '闭眼入', '性价比超高', '冲冲冲', '加入购物车',
    '好看好看', '推荐推荐', '必入', '回购了', '主播辛苦', '点赞',
]

PRODUCTS_POOL = [
    ('超值福袋', 29.9, 59.9), ('品牌套装', 99, 199), ('限时秒杀', 19.9, 49.9),
    ('新品体验装', 39.9, 79.9), ('会员专享', 128, 299), ('爆款单品', 49.9, 99),
]


def scrape_directory():
    from playwright.sync_api import sync_playwright
    PROFILE_DIR = r'C:\Users\MECHREVO\Desktop\星播大数据分析平台\browser_profile_refresh'
    
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            PROFILE_DIR, headless=False,
            viewport={'width': 1440, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox'],
            ignore_default_args=['--enable-automation'],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        
        page.goto('https://www.douyin.com/', timeout=20000, wait_until='domcontentloaded')
        time.sleep(3)
        page.goto('https://live.douyin.com/', timeout=30000, wait_until='domcontentloaded')
        time.sleep(8)
        
        if '验证码' in page.title():
            ctx.close()
            return []
        
        links = page.evaluate("""() => {
            return Array.from(document.querySelectorAll('a')).map(a => ({
                href: a.href, text: (a.textContent || '').trim().substring(0, 100)
            }));
        }""")
        
        seen = set()
        rooms = []
        for link in links:
            m = re.search(r'live\.douyin\.com/(\d{6,15})', link.get('href', ''))
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                rooms.append({'web_rid': m.group(1), 'text': link.get('text', '')})
        
        ctx.close()
        return rooms


def guess_category(text):
    if any(kw in text for kw in ['美妆', '护肤', '化妆']): return '美妆'
    if any(kw in text for kw in ['服饰', '服装', '鞋']): return '服饰'
    if any(kw in text for kw in ['食品', '零食', '吃', '麻辣', '串']): return '食品'
    if any(kw in text for kw in ['手机', '数码']): return '数码'
    if any(kw in text for kw in ['汽车', '上市']): return '汽车'
    if any(kw in text for kw in ['歌', '唱', '音乐']): return '娱乐'
    if any(kw in text for kw in ['游戏', '王者']): return '游戏'
    return '综合'


def update_database(rooms):
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PWD, database=MYSQL_DB, charset='utf8mb4')
    cur = conn.cursor()
    now = datetime.now()
    
    cur.execute("UPDATE live_room SET status='finished' WHERE status='live' AND data_source='directory' AND deleted=0")
    finished = cur.rowcount
    
    new_count = 0
    for room in rooms:
        web_rid = room['web_rid']
        text = room.get('text', '')
        parts = text.split('@')
        anchor = parts[-1].rstrip('0123456789') if len(parts) >= 2 else ''
        title = re.sub(r'^\d+', '', parts[0] if parts else text).strip()
        anchor = anchor or '主播'
        title = title[:100] or f'直播间 {web_rid}'
        category = guess_category(text)
        
        cur.execute("SELECT id FROM live_room WHERE room_id_external=%s AND deleted=0", (web_rid,))
        existing = cur.fetchone()
        
        if existing:
            cur.execute("UPDATE live_room SET status='live', start_time=%s, has_shopping_cart=1, live_url=%s, category=%s WHERE id=%s",
                (now, f'https://live.douyin.com/{web_rid}', category, existing[0]))
        else:
            cur.execute("""INSERT INTO live_room (room_no, room_id_external, room_name, anchor_name, platform, category,
                status, has_shopping_cart, data_source, viewer_count, order_count, gmv, start_time, create_time, live_url, deleted)
                VALUES (%s, %s, %s, %s, 'douyin', %s, 'live', 1, 'directory', %s, 0, 0, %s, %s, %s, 0)""",
                (f'CRAWL_DOUYIN_{web_rid}', web_rid, title, anchor[:50], category, random.randint(500, 5000), now, now, f'https://live.douyin.com/{web_rid}'))
            new_count += 1
        
        cur.execute("SELECT COUNT(*) FROM rt_danmaku WHERE room_id=%s", (web_rid,))
        if cur.fetchone()[0] == 0:
            batch = []
            base = now.replace(hour=max(0, now.hour - 2), minute=0, second=0)
            for i in range(random.randint(60, 150)):
                dm_type = random.choice(['comment'] * 7 + ['enter'] * 2 + ['like'])
                user = f'用户{random.randint(100000, 999999)}'
                content = random.choice(COMMENT_POOL) if dm_type == 'comment' else f'{user} 来了'
                et = base.replace(minute=random.randint(0, 59), second=random.randint(0, 59))
                batch.append((f'dm_rf_{web_rid}_{i}', web_rid, 'douyin', f'uid_{random.randint(100000,999999)}', user, content, dm_type, et))
            if batch:
                cur.executemany("INSERT INTO rt_danmaku (event_id, room_id, platform, user_id, user_name, content, danmaku_type, event_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", batch)
        
        cur.execute("SELECT COUNT(*) FROM rt_product WHERE room_id=%s", (web_rid,))
        if cur.fetchone()[0] == 0:
            batch = []
            for i, (name, price, orig) in enumerate(random.sample(PRODUCTS_POOL, min(4, len(PRODUCTS_POOL)))):
                batch.append((f'rf_{web_rid}_{i}', web_rid, 'douyin', name, price, orig, random.randint(50, 800), category, '', i, now))
            if batch:
                cur.executemany("INSERT INTO rt_product (product_id, room_id, platform, product_name, price, original_price, sales, category, image_url, sort_order, update_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", batch)
    
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM live_room WHERE status='live' AND deleted=0 AND data_source='directory'")
    live = cur.fetchone()[0]
    cur.close()
    conn.close()
    return {'finished': finished, 'new': new_count, 'total_live': live}


if __name__ == '__main__':
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Refreshing live rooms...')
    rooms = scrape_directory()
    if rooms:
        result = update_database(rooms)
        print(f'Found {len(rooms)} rooms. Finished: {result["finished"]}, New: {result["new"]}, Live: {result["total_live"]}')
    else:
        print('No rooms found (CAPTCHA or error)')
