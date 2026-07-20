"""
抖音带货主播批量发现脚本
通过 Playwright 模拟搜索，从抖音搜索页面提取带货主播信息并写入 MySQL。
使用方法：双击 crawl_anchors.bat 或在命令行运行 python crawl_douyin_anchors.py
"""
import asyncio, json, os, sys, shutil, time, re
from pathlib import Path

# ── 路径 & 常量 ──
PROJECT_DIR = Path(__file__).parent
MAIN_PROFILE = Path.home() / ".qoderworkcn" / "douyin_browser_profile"
SEARCH_PROFILE = Path.home() / ".qoderworkcn" / "douyin_search_profile"
COOKIE_JSON  = PROJECT_DIR / "data_pipeline" / "cookies" / "douyin_cookies.json"
MYSQL_HOST   = "192.168.104.100"
MYSQL_USER   = "root"
MYSQL_PWD    = "123456"
DB_NAME      = "livecommerce_db"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# ── 搜索关键词库 ──
KEYWORDS = [
    # 品类通用词
    "带货主播", "直播间", "好物推荐", "种草", "测评",
    # 美妆
    "美妆", "护肤", "化妆品", "口红", "面膜", "粉底液", "眼影", "防晒",
    # 服饰
    "女装", "男装", "穿搭", "鞋子", "包包", "运动鞋", "连衣裙",
    # 食品
    "零食", "美食", "水果", "茶叶", "坚果", "牛奶", "咖啡",
    # 数码
    "手机", "电脑", "耳机", "平板", "充电宝", "键盘",
    # 家居
    "家居", "家纺", "厨具", "收纳", "清洁",
    # 母婴
    "母婴", "奶粉", "纸尿裤", "童装", "玩具",
    # 珠宝
    "珠宝", "黄金", "翡翠", "银饰",
    # 运动
    "运动", "健身", "瑜伽", "跑步",
    # 品牌词
    "花西子", "完美日记", "珀莱雅", "欧莱雅", "兰蔻", "雅诗兰黛",
    "Nike", "Adidas", "李宁", "安踏", "鸿星尔克",
    "三只松鼠", "良品铺子", "百草味", "蒙牛", "伊利",
    "华为", "小米", "OPPO", "vivo", "苹果",
]

# ═══════════════════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════════════════

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def _safe_copy(src, dst):
    try:
        shutil.copy2(src, dst)
    except (PermissionError, OSError):
        pass

def prepare_search_profile():
    """从主 profile 拷贝关键文件到搜索专用 profile"""
    if not MAIN_PROFILE.exists():
        log(f"主 profile 不存在: {MAIN_PROFILE}")
        SEARCH_PROFILE.mkdir(parents=True, exist_ok=True)
        return
    SEARCH_PROFILE.mkdir(parents=True, exist_ok=True)
    # 拷贝 Local State
    ls_src = MAIN_PROFILE / "Local State"
    if ls_src.exists():
        _safe_copy(ls_src, SEARCH_PROFILE / "Local State")
    # 拷贝 Default 目录（含 Cookies、Login Data 等）
    def_src = MAIN_PROFILE / "Default"
    def_dst = SEARCH_PROFILE / "Default"
    if def_src.is_dir():
        if not def_dst.exists():
            try:
                shutil.copytree(def_src, def_dst, dirs_exist_ok=True,
                                copy_function=_safe_copy,
                                ignore=shutil.ignore_patterns(
                                    'Cache*', 'Code Cache*', 'GPUCache*',
                                    'DawnCache*', 'Service Worker*',
                                    'VideoDecodeStats*', 'blob_storage*',
                                    'File System*', 'IndexedDB*',
                                    'Session Storage*', 'Local Storage*'))
            except Exception as e:
                log(f"  拷贝 Default 目录失败: {e}")
        else:
            # 已有目标目录，只拷贝 Network/Cookies
            net_src = def_src / "Network" / "Cookies"
            net_dst = def_dst / "Network"
            if net_src.exists():
                net_dst.mkdir(parents=True, exist_ok=True)
                _safe_copy(net_src, net_dst / "Cookies")
    log(f"搜索 profile 准备完毕: {SEARCH_PROFILE}")


def load_cookies_from_json():
    """从 JSON 文件加载 cookie 列表"""
    if not COOKIE_JSON.exists():
        log(f"Cookie JSON 不存在: {COOKIE_JSON}")
        return []
    try:
        with open(COOKIE_JSON, 'r', encoding='utf-8') as f:
            cookies = json.load(f)
        log(f"从 JSON 加载了 {len(cookies)} 条 cookie")
        return cookies
    except Exception as e:
        log(f"加载 cookie JSON 失败: {e}")
        return []


def mysql_conn():
    import pymysql
    return pymysql.connect(host=MYSQL_HOST, port=3306, user=MYSQL_USER,
                           password=MYSQL_PWD, database=DB_NAME,
                           charset='utf8mb4', connect_timeout=15,
                           read_timeout=30, write_timeout=30)


def upsert_anchors(anchors):
    """将发现的主播写入 live_room 表（去重）"""
    if not anchors:
        return 0
    conn = mysql_conn()
    cur = conn.cursor()
    inserted = 0
    for a in anchors:
        name = (a.get('anchor_name') or '').strip()
        if not name:
            continue
        room_ext = a.get('room_id_external', '') or ''
        room_name = a.get('room_name', '') or ''
        category = a.get('category', '') or ''
        live_url = a.get('live_url', '') or ''
        viewers = int(a.get('viewer_count', 0) or 0)
        status = a.get('status', 'ended')

        # 生成唯一 room_id_external: 如果已有就用，否则用 anchor 名哈希
        if not room_ext:
            import hashlib
            room_ext = "anchor_" + hashlib.md5(name.encode()).hexdigest()[:12]

        if not live_url and room_ext and not room_ext.startswith("anchor_"):
            live_url = f"https://live.douyin.com/{room_ext}"

        try:
            cur.execute(
                "INSERT INTO live_room "
                "(room_id_external, room_name, anchor_name, category, platform, "
                " viewer_count, live_url, status, has_shopping_cart, data_source, "
                " created_at, updated_at, deleted) "
                "VALUES (%s,%s,%s,%s,'douyin',%s,%s,%s,0,'anchor_crawl',NOW(),NOW(),0) "
                "ON DUPLICATE KEY UPDATE "
                " anchor_name=VALUES(anchor_name), "
                " category=COALESCE(NULLIF(VALUES(category),''), category), "
                " room_name=COALESCE(NULLIF(VALUES(room_name),''), room_name), "
                " live_url=COALESCE(NULLIF(VALUES(live_url),''), live_url), "
                " viewer_count=GREATEST(viewer_count, VALUES(viewer_count)), "
                " updated_at=NOW()",
                (room_ext, room_name, name, category, viewers, live_url, status))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            pass  # 静默忽略单条错误
    conn.commit()
    cur.close()
    conn.close()
    return inserted


# ═══════════════════════════════════════════════════
#  核心爬取逻辑
# ═══════════════════════════════════════════════════

# 提取搜索结果的 JS 脚本（简洁版：提取所有用户链接和直播链接）
EXTRACT_SEARCH_JS = """(() => {
    const results = [];
    const seen = new Set();
    // 策略1: 提取所有 /user/ 链接
    document.querySelectorAll('a[href*="/user/"]').forEach(a => {
        const name = (a.textContent || '').trim();
        if (!name || name.length < 1 || name.length > 30 || seen.has(name)) return;
        seen.add(name);
        const href = a.href || '';
        const uidM = href.match(/\\/user\\/([^?&/]+)/);
        results.push({
            anchor_name: name,
            profile_url: href,
            sec_uid: uidM ? uidM[1] : '',
            live_url: '', web_rid: '',
            is_live: false, fans_text: '0', desc: ''
        });
    });
    // 策略2: 提取所有 live.douyin.com 链接（正在直播的）
    document.querySelectorAll('a[href*="live.douyin.com"]').forEach(a => {
        const name = (a.textContent || a.title || '').trim();
        const href = a.href || '';
        const ridM = href.match(/live\\.douyin\\.com\\/(\\d+)/);
        if (!ridM) return;
        const rid = ridM[1];
        // 找到同名结果并标记 live，或新增
        const existing = results.find(r => r.anchor_name === name);
        if (existing) {
            existing.is_live = true;
            existing.live_url = href;
            existing.web_rid = rid;
        } else if (name && !seen.has(name)) {
            seen.add(name);
            results.push({
                anchor_name: name, profile_url: '', sec_uid: '',
                live_url: href, web_rid: rid,
                is_live: true, fans_text: '0', desc: ''
            });
        }
    });
    // 策略3: 提取页面内嵌的 JSON 数据（SSR 数据）
    try {
        const scripts = document.querySelectorAll('script[id*="RENDER"], script[type="application/json"]');
        scripts.forEach(s => {
            try {
                const text = s.textContent || '';
                // 查找用户名和 sec_uid
                const userMatches = text.match(/"nickname":"([^"]{1,30})"/g);
                if (userMatches) {
                    userMatches.forEach(m => {
                        const nm = m.replace(/"nickname":"/, '').replace(/"/, '');
                        if (nm && !seen.has(nm) && nm.length >= 2) {
                            seen.add(nm);
                            results.push({
                                anchor_name: nm, profile_url: '', sec_uid: '',
                                live_url: '', web_rid: '',
                                is_live: false, fans_text: '0', desc: ''
                            });
                        }
                    });
                }
            } catch(e) {}
        });
    } catch(e) {}
    return results;
})()"""


def parse_fans(text):
    """解析粉丝数文本为整数：'12.5万' -> 125000, '1.2亿' -> 120000000"""
    if not text:
        return 0
    text = text.replace(',', '').replace(' ', '')
    m = re.search(r'([\d.]+)\s*万', text)
    if m:
        return int(float(m.group(1)) * 10000)
    m = re.search(r'([\d.]+)\s*亿', text)
    if m:
        return int(float(m.group(1)) * 100000000)
    m = re.search(r'(\d+)', text)
    return int(m.group(1)) if m else 0


def guess_category(anchor):
    """根据描述和名称猜测类目"""
    desc = (anchor.get('desc', '') + ' ' + anchor.get('anchor_name', '')).lower()
    rules = [
        (['美妆','护肤','化妆','口红','面膜','粉底','眼影','防晒','美白','精华','洗面奶','乳液','粉底液'], '美妆'),
        (['女装','男装','穿搭','鞋','包','裙','外套','裤','t恤','卫衣','大衣','风衣'], '服饰'),
        (['零食','美食','水果','茶','坚果','奶','咖啡','蛋糕','饼干','辣条','牛肉','鸡'], '食品'),
        (['手机','电脑','耳机','平板','充电','键盘','鼠标','数码','电脑','ipad'], '数码'),
        (['家居','家纺','厨具','收纳','清洁','床上','枕头','被子','窗帘','灯'], '家居'),
        (['母婴','奶粉','纸尿裤','童装','玩具','宝宝','婴儿','儿童','孕妇'], '母婴'),
        (['珠宝','黄金','翡翠','银饰','钻石','宝石','玉','珍珠'], '珠宝'),
        (['运动','健身','瑜伽','跑步','篮球','足球','哑铃','跳绳'], '运动'),
    ]
    for kws, cat in rules:
        for kw in kws:
            if kw in desc:
                return cat
    return '综合'


async def search_one_keyword(context, keyword, seen_names):
    """搜索一个关键词并返回发现的主播列表"""
    page = None
    anchors = []
    try:
        page = await context.new_page()
        url = f"https://www.douyin.com/search/{keyword}?type=general"
        log(f"  搜索: {keyword}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception:
            # 重试一次
            await asyncio.sleep(2)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except Exception as e2:
                log(f"    导航失败: {e2}")
                return anchors
        await page.wait_for_timeout(5000)

        # 滚动加载更多结果
        for scroll_i in range(6):
            try:
                await page.evaluate("window.scrollBy(0, 1200)")
                await page.wait_for_timeout(1500)
            except Exception:
                break

        # 提取搜索结果
        try:
            raw = await page.evaluate(EXTRACT_SEARCH_JS)
        except Exception:
            raw = []

        for item in (raw or []):
            name = (item.get('anchor_name') or '').strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            cat = guess_category(item)
            viewers = parse_fans(item.get('fans_text', ''))
            web_rid = item.get('web_rid', '') or ''
            anchors.append({
                'anchor_name': name,
                'room_id_external': web_rid,
                'room_name': f"{name}的直播间",
                'category': cat,
                'live_url': item.get('live_url', '') or (f"https://live.douyin.com/{web_rid}" if web_rid else ''),
                'viewer_count': viewers,
                'status': 'live' if item.get('is_live') else 'ended',
            })

        log(f"    -> {keyword}: 提取 {len(raw or [])} 条, 新增 {len(anchors)} 位主播")

        # 尝试 "用户" 类型搜索（专门搜人）
        try:
            url2 = f"https://www.douyin.com/search/{keyword}?type=user"
            await page.goto(url2, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            for _ in range(4):
                try:
                    await page.evaluate("window.scrollBy(0, 1000)")
                    await page.wait_for_timeout(1200)
                except Exception:
                    break

            try:
                raw2 = await page.evaluate(EXTRACT_SEARCH_JS)
            except Exception:
                raw2 = []
            extra = 0
            for item in (raw2 or []):
                name = (item.get('anchor_name') or '').strip()
                if not name or name in seen_names:
                    continue
                seen_names.add(name)
                cat = guess_category(item)
                viewers = parse_fans(item.get('fans_text', ''))
                web_rid = item.get('web_rid', '') or ''
                anchors.append({
                    'anchor_name': name,
                    'room_id_external': web_rid,
                    'room_name': f"{name}的直播间",
                    'category': cat,
                    'live_url': item.get('live_url', '') or (f"https://live.douyin.com/{web_rid}" if web_rid else ''),
                    'viewer_count': viewers,
                    'status': 'live' if item.get('is_live') else 'ended',
                })
                extra += 1
            if extra:
                log(f"    -> {keyword} (用户搜索): 新增 {extra} 位主播")
        except Exception as e:
            log(f"    用户搜索 '{keyword}' 出错: {e}")

    except Exception as e:
        log(f"    搜索 '{keyword}' 出错: {e}")
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass
    return anchors


async def visit_anchor_profiles(context, anchors, batch_size=10):
    """访问部分主播主页，补充直播间信息和类目"""
    enriched = []
    # 只访问有 profile_url 的前 N 个
    to_visit = [a for a in anchors if not a.get('room_id_external')][:batch_size]
    if not to_visit:
        return enriched

    log(f"访问 {len(to_visit)} 位主播主页补充信息...")
    page = await context.new_page()
    for a in to_visit:
        try:
            # 从 profile_url 访问
            purl = a.get('profile_url', '')
            if not purl:
                continue
            await page.goto(purl, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(3000)

            info = await page.evaluate("""(() => {
                // 检查是否在直播
                const liveEl = document.querySelector('[class*="live"], [class*="LIVE"]');
                const liveLink = document.querySelector('a[href*="live.douyin.com"]');
                const liveUrl = liveLink ? liveLink.href : '';
                const liveMatch = liveUrl.match(/live\\.douyin\\.com\\/(\\d+)/);
                // 提取粉丝数
                const fansEl = document.querySelector('[class*="fans"] [class*="count"], [class*="follower"] [class*="count"]');
                // 提取简介
                const bioEl = document.querySelector('[class*="bio"], [class*="desc"], [class*="signature"]');
                return {
                    is_live: !!(liveEl || liveUrl),
                    live_url: liveUrl,
                    web_rid: liveMatch ? liveMatch[1] : '',
                    fans_text: fansEl ? fansEl.textContent.trim() : '',
                    bio: bioEl ? bioEl.textContent.trim().slice(0, 200) : ''
                };
            })()""")
            if info:
                if info.get('web_rid'):
                    a['room_id_external'] = info['web_rid']
                    a['live_url'] = info.get('live_url', '') or f"https://live.douyin.com/{info['web_rid']}"
                if info.get('is_live'):
                    a['status'] = 'live'
                if info.get('fans_text'):
                    a['viewer_count'] = parse_fans(info['fans_text'])
                if info.get('bio'):
                    a['category'] = guess_category({**a, 'desc': info['bio']})
                enriched.append(a['anchor_name'])
        except Exception:
            pass
    await page.close()
    if enriched:
        log(f"  补充了 {len(enriched)} 位主播的详细信息")
    return enriched


# ═══════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════

async def main():
    log("=" * 50)
    log("抖音带货主播批量发现脚本")
    log("=" * 50)

    # 1. 准备 profile
    log("[1/5] 准备浏览器 profile...")
    prepare_search_profile()

    # 2. 查 MySQL 已有主播（去重）
    log("[2/5] 查询已有主播...")
    conn = mysql_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT anchor_name FROM live_room WHERE deleted=0 AND anchor_name IS NOT NULL AND anchor_name != ''")
    existing = set(row[0] for row in cur.fetchall())
    cur.close()
    conn.close()
    log(f"  数据库已有 {len(existing)} 位主播")
    seen_names = set(existing)

    # 3. 启动 Playwright
    log("[3/5] 启动浏览器...")
    from playwright.async_api import async_playwright
    pw = await async_playwright().start()

    # 清理 lockfile
    lockfile = SEARCH_PROFILE / "lockfile"
    if lockfile.exists():
        try:
            os.remove(lockfile)
        except Exception:
            pass

    context = await pw.chromium.launch_persistent_context(
        user_data_dir=str(SEARCH_PROFILE),
        channel="chrome",
        headless=False,
        viewport={"width": 1920, "height": 1080},
        user_agent=UA,
        locale="zh-CN",
        timezone_id="Asia/Shanghai",
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--no-first-run",
            "--no-default-browser-check",
            "--disk-cache-size=0",
        ],
        ignore_https_errors=True,
    )

    # 注入 cookie
    cookies = load_cookies_from_json()
    if cookies:
        try:
            await context.add_cookies(cookies)
            log(f"  已注入 {len(cookies)} 条 cookie")
        except Exception as e:
            log(f"  Cookie 注入部分失败: {e}")

    # 反检测
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        window.chrome = {runtime: {}};
    """)

    # 先访问首页建立 session
    log("  访问抖音首页建立 session...")
    init_page = await context.new_page()
    try:
        await init_page.goto("https://www.douyin.com", wait_until="domcontentloaded", timeout=30000)
        await init_page.wait_for_timeout(3000)
        body_len = await init_page.evaluate("document.body ? document.body.innerText.length : 0")
        log(f"  首页加载: body 长度 {body_len}")
        if body_len < 100:
            log("  ⚠ 首页加载异常，可能被反爬检测，继续尝试...")
    except Exception as e:
        log(f"  首页访问失败: {e}")
    await init_page.close()

    # 4. 逐个关键词搜索
    log(f"[4/5] 开始搜索 ({len(KEYWORDS)} 个关键词)...")
    all_anchors = []
    total_new = 0

    for i, kw in enumerate(KEYWORDS):
        batch = await search_one_keyword(context, kw, seen_names)
        all_anchors.extend(batch)
        total_new += len(batch)

        # 每 5 个关键词批量入库一次
        if (i + 1) % 5 == 0 and all_anchors:
            written = upsert_anchors(all_anchors)
            log(f"  ── 批量入库: {written} 条 (累计新增 {total_new} 位主播)")
            all_anchors = []

        # 搜索间隔，避免频率过高
        await asyncio.sleep(3)

        # 每10个关键词做一次健康检查
        if (i + 1) % 10 == 0:
            try:
                _hp = await context.new_page()
                await _hp.close()
            except Exception:
                log("  ⚠ 浏览器上下文异常，尝试重建...")
                try:
                    await context.close()
                except Exception:
                    pass
                context = await pw.chromium.launch_persistent_context(
                    user_data_dir=str(SEARCH_PROFILE),
                    channel="chrome", headless=False,
                    viewport={"width": 1920, "height": 1080},
                    user_agent=UA, locale="zh-CN", timezone_id="Asia/Shanghai",
                    args=["--disable-blink-features=AutomationControlled",
                          "--no-first-run", "--no-default-browser-check",
                          "--disk-cache-size=0"],
                    ignore_https_errors=True)
                cookies = load_cookies_from_json()
                if cookies:
                    try:
                        await context.add_cookies(cookies)
                    except Exception:
                        pass
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                """)

    # 5. 最后一批入库
    if all_anchors:
        written = upsert_anchors(all_anchors)
        log(f"  ── 最终入库: {written} 条")

    # 统计
    conn = mysql_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(DISTINCT anchor_name) FROM live_room WHERE deleted=0 AND anchor_name IS NOT NULL AND anchor_name != ''")
    total_anchors = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM live_room WHERE deleted=0")
    total_rooms = cur.fetchone()[0]
    cur.close()
    conn.close()

    log("[5/5] 完成!")
    log(f"  本次新增: {total_new} 位主播")
    log(f"  数据库总计: {total_anchors} 位主播, {total_rooms} 个房间")

    try:
        await context.close()
    except Exception:
        pass
    await pw.stop()
    log("浏览器已关闭，脚本执行完毕。")


if __name__ == "__main__":
    # Windows 控制台中文
    if sys.platform == 'win32':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    asyncio.run(main())
