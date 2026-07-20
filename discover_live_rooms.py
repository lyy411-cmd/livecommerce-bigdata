"""Quick live room discovery - find 50+ live shopping rooms on Douyin NOW."""
import asyncio, json, os, sys, shutil, time, re
from pathlib import Path

PROJECT_DIR = Path(r"C:\Users\MECHREVO\Desktop\星播大数据分析平台")
SEARCH_PROFILE = Path.home() / ".qoderworkcn" / "douyin_search_profile"
MAIN_PROFILE = Path.home() / ".qoderworkcn" / "douyin_browser_profile"
COOKIE_JSON = PROJECT_DIR / "data_pipeline" / "cookies" / "douyin_cookies.json"
MYSQL_HOST, MYSQL_USER, MYSQL_PWD, DB_NAME = "192.168.104.100", "root", "123456", "livecommerce_db"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
TARGET_LIVE = 50

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def _safe_copy(src, dst):
    try: shutil.copy2(src, dst)
    except: pass

def prepare_profile():
    SEARCH_PROFILE.mkdir(parents=True, exist_ok=True)
    if MAIN_PROFILE.exists():
        ls = MAIN_PROFILE / "Local State"
        if ls.exists(): _safe_copy(ls, SEARCH_PROFILE / "Local State")
        def_src, def_dst = MAIN_PROFILE / "Default", SEARCH_PROFILE / "Default"
        if def_src.is_dir() and not def_dst.exists():
            try: shutil.copytree(def_src, def_dst, dirs_exist_ok=True, copy_function=_safe_copy,
                ignore=shutil.ignore_patterns('Cache*','Code Cache*','GPUCache*','Service Worker*',
                    'blob_storage*','IndexedDB*','Session Storage*','Local Storage*'))
            except: pass
    for f in ['lockfile','SingletonLock','SingletonSocket','SingletonCookie']:
        p = SEARCH_PROFILE / f
        if p.exists():
            try: os.remove(p)
            except: pass

def load_cookies():
    if not COOKIE_JSON.exists(): return []
    try:
        with open(COOKIE_JSON, 'r', encoding='utf-8') as f: return json.load(f)
    except: return []

def mysql_conn():
    import pymysql
    return pymysql.connect(host=MYSQL_HOST, port=3306, user=MYSQL_USER, password=MYSQL_PWD,
        database=DB_NAME, charset='utf8mb4', connect_timeout=15, read_timeout=30, write_timeout=30)

LIVE_URLS = [
    'https://live.douyin.com',
    'https://live.douyin.com/category/100106',
    'https://live.douyin.com/category/100102',
    'https://live.douyin.com/category/100104',
    'https://live.douyin.com/category/100101',
    'https://live.douyin.com/category/100103',
    'https://live.douyin.com/category/100105',
    'https://live.douyin.com/category/100107',
    'https://live.douyin.com/category/100108',
    'https://live.douyin.com/category/100109',
    'https://live.douyin.com/category/100110',
    'https://live.douyin.com/category/100111',
    'https://live.douyin.com/category/100112',
    'https://live.douyin.com/category/100113',
]

EXTRACT_WEBRIDS_JS = """(() => {
    const rids = new Set();
    document.querySelectorAll('a[href*="live.douyin.com/"]').forEach(a => {
        const m = (a.href || '').match(/live\\.douyin\\.com\\/(\\d+)/);
        if (m) rids.add(m[1]);
    });
    document.querySelectorAll('script').forEach(s => {
        const text = s.textContent || '';
        (text.match(/"web_rid"\\s*:\\s*"(\\d+)"/g) || []).forEach(m => {
            const r = m.match(/(\\d+)/); if(r) rids.add(r[1]);
        });
        (text.match(/"roomId"\\s*:\\s*"(\\d+)"/g) || []).forEach(m => {
            const r = m.match(/(\\d+)/); if(r) rids.add(r[1]);
        });
    });
    return [...rids];
})()"""

EXTRACT_ROOM_JS = """(() => {
    const title = document.title || '';
    const body = document.body ? document.body.innerText : '';
    const aEl = document.querySelector('[class*="anchor"] [class*="name"], [class*="host"] [class*="name"], a[class*="avatar"] + *, [class*="nickname"]');
    const anchorName = aEl ? aEl.textContent.trim() : '';
    const hasCart = !!(document.querySelector('[class*="cart"], [class*="shopping"], [class*="product"], [class*="goods"]') ||
        body.includes('购物车') || body.includes('商品') || body.includes('去购买') || body.includes('小黄车') || body.includes('下单'));
    const vEl = document.querySelector('[class*="viewer"], [class*="watch"], [class*="count"]');
    const viewText = vEl ? vEl.textContent.trim() : '';
    const cEl = document.querySelector('[class*="category"], [class*="tag"], [class*="label"]');
    return { title, anchorName, hasCart, viewText, category: cEl ? cEl.textContent.trim() : '', bodyLen: body.length };
})()"""

def parse_viewers(t):
    if not t: return 0
    t = t.replace(',','').strip()
    m = re.search(r'([\d.]+)\s*万', t)
    if m: return int(float(m.group(1))*10000)
    m = re.search(r'(\d+)', t)
    return int(m.group(1)) if m else 0


async def discover_rids(context):
    all_rids = set()
    page = await context.new_page()
    try:
        async def block(route):
            rt = route.request.resource_type
            if rt in ('stylesheet','font','image'): await route.abort()
            else: await route.continue_()
        await context.route('**/*', block)
    except: pass
    for url in LIVE_URLS:
        try:
            log(f"  {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)
            for _ in range(8):
                try:
                    await page.evaluate("window.scrollBy(0, 1500)")
                    await page.wait_for_timeout(1200)
                except: break
            rids = await page.evaluate(EXTRACT_WEBRIDS_JS)
            new = len(set(rids) - all_rids)
            all_rids.update(rids)
            log(f"    -> {len(rids)} rids, +{new} new (total {len(all_rids)})")
        except Exception as e:
            log(f"    FAIL: {e}")
    try: await context.unroute('**/*')
    except: pass
    await page.close()
    return list(all_rids)


async def precheck(context, rids):
    results = []
    page = await context.new_page()
    for i, rid in enumerate(rids):
        if len(results) >= TARGET_LIVE * 2: break
        try:
            await page.goto(f"https://live.douyin.com/{rid}", wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(3000)
            info = await page.evaluate(EXTRACT_ROOM_JS)
            if not info or info.get('bodyLen',0) < 50: continue
            title = info.get('title','')
            if '结束' in title or '回放' in title: continue
            if info.get('bodyLen',0) < 100: continue
            anchor = info.get('anchorName','') or ''
            if not anchor:
                m = re.match(r'(.+?)的直播间', title)
                if m: anchor = m.group(1)
            viewers = parse_viewers(info.get('viewText',''))
            cat = info.get('category','') or '带货'
            results.append({
                'room_id_external': rid,
                'room_name': title.replace(' - 抖音直播','').replace('_抖音直播','').strip()[:80],
                'anchor_name': anchor or f'主播{rid[-4:]}',
                'viewer_count': viewers, 'category': cat,
                'live_url': f'https://live.douyin.com/{rid}',
                'has_shopping_cart': 1 if info.get('hasCart') else 0,
                'status': 'live',
            })
            tag = 'CART' if info.get('hasCart') else '----'
            log(f"    [{i+1}/{len(rids)}] {tag} {rid} | {anchor or title[:15]} | {viewers}人")
        except: pass
    await page.close()
    return results


def write_rooms(rooms):
    if not rooms: return 0
    conn = mysql_conn()
    cur = conn.cursor()
    n = 0
    for r in rooms:
        try:
            cur.execute(
                "INSERT INTO live_room "
                "(room_id_external,room_name,anchor_name,category,platform,"
                "viewer_count,live_url,status,has_shopping_cart,data_source,"
                "created_at,updated_at,deleted) "
                "VALUES(%s,%s,%s,%s,'douyin',%s,%s,'live',%s,'live_discover',"
                "NOW(),NOW(),0) "
                "ON DUPLICATE KEY UPDATE status='live',"
                "has_shopping_cart=GREATEST(has_shopping_cart,VALUES(has_shopping_cart)),"
                "room_name=COALESCE(NULLIF(VALUES(room_name),''),room_name),"
                "anchor_name=COALESCE(NULLIF(VALUES(anchor_name),''),anchor_name),"
                "category=COALESCE(NULLIF(VALUES(category),''),category),"
                "live_url=COALESCE(NULLIF(VALUES(live_url),''),live_url),"
                "viewer_count=GREATEST(viewer_count,VALUES(viewer_count)),updated_at=NOW()",
                (r['room_id_external'], r['room_name'], r['anchor_name'],
                 r['category'], r['viewer_count'], r['live_url'],
                 r['has_shopping_cart']))
            n += 1
        except: pass
    conn.commit()
    cur.close()
    conn.close()
    return n


async def main():
    log("=" * 50)
    log("Live room discovery - target 50 rooms")
    log("=" * 50)

    prepare_profile()

    from playwright.async_api import async_playwright
    pw = await async_playwright().start()
    context = await pw.chromium.launch_persistent_context(
        user_data_dir=str(SEARCH_PROFILE), channel="chrome", headless=False,
        viewport={"width": 1920, "height": 1080}, user_agent=UA,
        locale="zh-CN", timezone_id="Asia/Shanghai",
        args=["--disable-blink-features=AutomationControlled",
              "--no-first-run", "--no-default-browser-check",
              "--disk-cache-size=0"],
        ignore_https_errors=True)

    cookies = load_cookies()
    if cookies:
        try:
            await context.add_cookies(cookies)
            log(f"  {len(cookies)} cookies loaded")
        except: pass

    await context.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

    init_p = await context.new_page()
    try:
        await init_p.goto("https://live.douyin.com",
                          wait_until="domcontentloaded", timeout=30000)
        await init_p.wait_for_timeout(3000)
    except: pass
    await init_p.close()

    log("Discovering rooms from live directory...")
    rids = await discover_rids(context)
    log(f"Found {len(rids)} unique room IDs")

    log("Prechecking rooms...")
    rooms = await precheck(context, rids)
    cart = [r for r in rooms if r['has_shopping_cart']]
    no_cart = [r for r in rooms if not r['has_shopping_cart']]
    log(f"\nResults: {len(rooms)} live, {len(cart)} with cart, {len(no_cart)} without")

    to_write = cart
    if len(cart) < TARGET_LIVE and no_cart:
        need = TARGET_LIVE - len(cart)
        log(f"  Cart rooms < {TARGET_LIVE}, adding {min(len(no_cart), need)} without cart")
        to_write = cart + no_cart[:need]

    written = write_rooms(to_write)
    log(f"Written {written} rooms (status=live)")

    conn = mysql_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM live_room WHERE status='live' AND deleted=0")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()
    log(f"Total live rooms in DB: {total}")

    try: await context.close()
    except: pass
    await pw.stop()
    log("Done!")


if __name__ == "__main__":
    if sys.platform == 'win32':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    asyncio.run(main())
