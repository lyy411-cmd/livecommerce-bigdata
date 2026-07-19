"""
快手直播间爬虫模块
基于 Playwright 实现快手直播间数据采集，包括：
- 直播间发现（从首页/带货分类 API 拦截房间列表）
- 直播间详情抓取
- Cookie 持久化与反检测机制
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# 基础路径配置
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
BROWSER_PROFILES_DIR = BASE_DIR / "browser_profiles" / "kuaishou"
COOKIES_DIR = BASE_DIR / "cookies"
COOKIES_FILE = COOKIES_DIR / "kuaishou_cookies.json"

logger = logging.getLogger(__name__)

REALISTIC_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {} };
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
"""


def _parse_count(text: str) -> int:
    """解析人数文本，如 '1.2万' -> 12000"""
    if not text:
        return 0
    text = str(text).strip().lower()
    m = re.match(r'([\d.]+)\s*[万w]', text)
    if m:
        return int(float(m.group(1)) * 10000)
    m = re.match(r'([\d.]+)\s*[亿]', text)
    if m:
        return int(float(m.group(1)) * 100000000)
    m = re.match(r'([\d.]+)\s*[k千]', text)
    if m:
        return int(float(m.group(1)) * 1000)
    digits = re.sub(r'[^\d]', '', text)
    return int(digits) if digits else 0


NON_COMMERCE_KEYWORDS = [
    # ── 游戏 ──
    '游戏', 'CF', '穿越火线', '梦幻西游', '王者荣耀', '和平精英',
    '蛋仔派对', 'LOL', '英雄联盟', '吃鸡', '原神', '电竞', 'CSGO',
    'CS:GO', 'CS2', 'DOTA', 'APEX', '瓦罗兰特', '永劫无间', '使命召唤', 'PUBG',
    '暗黑', '传奇', 'DNF', '火影', '海贼王', '棋牌', '象棋', '围棋',
    '斗地主', '麻将', '游戏解说', '打游戏', '开黑', '上分', '排位',
    'MC', '我的世界', '迷你世界', 'Roblox', '第五人格', '光遇',
    '三国杀', '炉石', '云顶', '金铲铲', '暗区突围', '元梦之星',
    '星穹铁道', '崩坏', '绝区零', '幻兽帕鲁', '黑神话', '悟空',
    'FIFA', 'NBA2K', '实况足球', '跑跑卡丁车', 'QQ飞车',
    '植物大战僵尸', '自定义战斗', '盲盒', '出兵', '皇帝', '大清', '大明',
    '方舟', '无畏契约', 'Valorant', '斗罗大陆', '龙岛', '异兽',
    '明日方舟', '命运方舟', '逆战', '枪战', '坦克', '飞机大战',
    '捕鱼', '国服', '小极品', '身法', '傲视', '传奇私服',
    '刺激战场', '荒野行动', '球球大作战', '猫和老鼠', '泡泡堂',
    '问道', '大话西游', '新天龙', '倩女幽魂', '剑网', '天涯明月刀',
    '武林外传', '诛仙', '剑灵', '流放之路', '暗黑破坏神',
    '摸金', '血影', '外传', 'KPL', '对战', '水友赛', '试玩',
    '亡者世界', '无视配置', '锐评', '新版本', '强度测试',
    # ── 娱乐/才艺 ──
    '唱歌', '跳舞', '才艺', 'DJ', '打碟', '喊麦', '脱口秀', '相声',
    '音乐', '演奏', '钢琴', '吉他', '绘画', '书法', '手工',
    '八卦', '娱乐', '搞笑', '段子', '综艺',
    '星秀', '秀场', '颜值', '情感', '相亲', '交友', '占卜', '星座',
    # ── 聊天/ASMR ──
    '聊天', '陪聊', '哄睡', 'ASMR',
    # ── 户外/宠物 ──
    '钓鱼', '户外', '旅行', '旅游', '酒吧', '夜店', '蹦迪', 'KTV',
    '陪玩', '代练', '宠物', '萌宠', '猫咪', '狗狗',
    '动漫', '二次元', 'cosplay',
    # ── 个人直播/非带货 ──
    '新人首播', '第一天', '第一次', '正在直播',
    '认识你', '很高兴', '来了', '来啦', '来人', '出来玩',
    '御姐', '温柔', '白给', '心态好', '扎起来', '对手呢', '迷路了',
    '战斗', '两分钟不笑',
    # ── 教育/培训 ──
    '培训', '课程', '学习', '考试', '教学', '辅导',
    # ── 新闻/体育 ──
    '新闻', '资讯', '体育', '足球', '篮球', 'NBA', 'CBA',
    # ── 电竞选手/主播对战 ──
    '电竞选手', '主播对战', '水友赛', '锦标赛', '经典单机', '街机',
    # ── 垃圾频道/刷量 ──
    '蓝光', '蓝光Plus', '蓝光8M', '蓝光 质臻',
]

# 正面带货信号 - 房间必须包含至少一个才能通过过滤
COMMERCE_POSITIVE_KEYWORDS = [
    '官方', '旗舰', '带货', '好物', '秒杀', '福利', '优惠', '特卖',
    '品牌', '正品', '下单', '购物车', '小黄车', '商品', '店铺',
    '食品', '美妆', '护肤', '服饰', '穿搭', '零食', '家居',
    '珠宝', '首饰', '母婴', '运动', '数码', '家电',
    '女装', '男装', '童装', '内衣', '化妆', '口红', '面膜',
    '坚果', '茶', '酒', '生鲜', '水果', '粮油',
    '床品', '家纺', '厨具', '收纳', '清洁', '日用',
    '工厂', '源头', '直发', '专场',
]

# 垃圾主播/刷量频道名模式（正则匹配）
SPAM_ANCHOR_PATTERNS = [
    r'蓝光', r'蓝光\s*质臻', r'蓝光\s*Plus', r'蓝光\s*\d',
    r'超清$', r'^\d+[Kk]$', r'^用户\d+',
    r'^undefined$',
]


def _is_commerce(room: dict) -> bool:
    """严格过滤：排除非带货 + 排除垃圾频道 + 要求正面带货信号"""
    import re as _re
    name = room.get('room_name', '') or ''
    anchor = room.get('anchor_name', '') or ''
    cat = room.get('category', '') or ''
    title = room.get('title', '') or ''
    caption = room.get('caption', '') or ''
    combined = f"{name} {anchor} {cat} {title} {caption}".lower()

    # 第一关：排除垃圾频道名模式
    for pat in SPAM_ANCHOR_PATTERNS:
        if _re.search(pat, anchor, _re.IGNORECASE):
            return False

    # 第二关：排除包含非带货关键词的房间
    for kw in NON_COMMERCE_KEYWORDS:
        if kw.lower() in combined:
            return False

    # 第三关：要求至少一个正面带货信号（在名字或主播名中）
    name_anchor = f"{name} {anchor} {title} {caption}".lower()
    for kw in COMMERCE_POSITIVE_KEYWORDS:
        if kw.lower() in name_anchor:
            return True

    # 没找到任何带货信号 → 拒绝
    return False


class KuaishouLiveCrawler:
    """快手直播间 Playwright 爬虫"""

    def __init__(self, kafka_producer=None):
        self._kafka_producer = kafka_producer
        self._playwright = None
        self._context = None
        self._page = None
        self._stealth_available = False

    async def init_browser(self):
        """初始化 Playwright 浏览器"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError("请先安装 playwright: pip install playwright && playwright install chromium")

        logger.info("正在初始化快手 Playwright 浏览器...")
        BROWSER_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        COOKIES_DIR.mkdir(parents=True, exist_ok=True)

        self._playwright = await async_playwright().start()

        try:
            from playwright_stealth import stealth_async
            self._stealth_async = stealth_async
            self._stealth_available = True
        except ImportError:
            pass

        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_PROFILES_DIR),
            headless=False,
            viewport={"width": 1366, "height": 768},
            user_agent=REALISTIC_USER_AGENT,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
            ],
        )
        await self._context.add_init_script(STEALTH_JS)

        pages = self._context.pages
        self._page = pages[0] if pages else await self._context.new_page()

        if COOKIES_FILE.exists():
            try:
                with open(COOKIES_FILE, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                if cookies:
                    await self._context.add_cookies(cookies)
                    logger.info(f"已加载 {len(cookies)} 个 Cookie")
            except Exception as e:
                logger.warning(f"加载 Cookie 失败: {e}")

        logger.info("快手浏览器初始化完成")

    async def _save_cookies(self):
        try:
            cookies = await self._context.cookies()
            with open(COOKIES_FILE, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            logger.info(f"已保存 {len(cookies)} 个 Cookie")
        except Exception as e:
            logger.warning(f"保存 Cookie 失败: {e}")

    async def _random_delay(self, min_s=1.0, max_s=3.0):
        await asyncio.sleep(random.uniform(min_s, max_s))

    async def _variable_scroll(self, page, direction="down"):
        delta = random.randint(200, 500)
        if direction == "up":
            delta = -delta
        await page.evaluate(f"window.scrollBy(0, {delta})")

    async def discover_live_rooms(self, limit: int = 20) -> list[dict]:
        """发现快手带货直播间"""
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        rooms: list[dict] = []
        api_intercepted = asyncio.Event()

        async def _on_response(response):
            url = response.url
            if response.status < 200 or response.status >= 300:
                return
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type and "javascript" not in content_type:
                return

            if any(pattern in url for pattern in [
                "live/list", "liveSquare", "live/square", "live/recommend",
                "live/homepage", "graphql", "api/live", "rest/n/live",
            ]):
                try:
                    body = await response.json()
                    candidates = []
                    if isinstance(body, dict):
                        data = body.get("data", body)
                        for key in ("liveList", "list", "feeds", "liveCards",
                                    "resultList", "recommendList"):
                            val = data.get(key, []) if isinstance(data, dict) else []
                            if isinstance(val, list) and len(val) > 0:
                                candidates = val
                                break
                        if not candidates and "result" in body:
                            result = body["result"]
                            if isinstance(result, dict):
                                for key in result:
                                    val = result[key]
                                    if isinstance(val, dict):
                                        inner = val.get("data", val)
                                        for k2 in ("liveList", "list", "feeds"):
                                            v2 = inner.get(k2, []) if isinstance(inner, dict) else []
                                            if isinstance(v2, list) and len(v2) > 0:
                                                candidates = v2
                                                break
                                    if candidates:
                                        break

                    for item in candidates:
                        room_info = self._extract_room_from_api(item)
                        if room_info:
                            rooms.append(room_info)

                    if rooms:
                        api_intercepted.set()
                        logger.info(f"从 API 拦截到 {len(rooms)} 个快手直播间")
                except Exception as e:
                    logger.debug(f"解析快手 API 响应失败: {e}")

        self._page.on("response", _on_response)

        try:
            # ── 优先爬购物分类页（这个页面本身就是带货直播）──
            shopping_url = "https://live.kuaishou.com/cate/shopping"
            logger.info(f"正在导航到购物分类页 {shopping_url} ...")
            await self._page.goto(shopping_url, wait_until="domcontentloaded", timeout=35000)
            await self._random_delay(5, 8)

            try:
                await asyncio.wait_for(api_intercepted.wait(), timeout=15)
            except asyncio.TimeoutError:
                logger.info(f"购物分类页 API 拦截超时，使用 DOM 降级...")

            # 在购物页面上多滚动几次，加载更多房间
            for _ in range(6):
                await self._variable_scroll(self._page, "down")
                await self._random_delay(2, 4)

            if not rooms:
                logger.info(f"在购物分类页上尝试 DOM 降级方案...")
                rooms = await self._scrape_rooms_from_dom()
                # 购物分类页的房间信任为带货（标记来源）
                for r in rooms:
                    r['source_page'] = 'shopping_category'

            # ── 搜索页作为补充 ──
            if len(rooms) < 5:
                search_urls = [
                    "https://live.kuaishou.com/search/%E5%B8%A6%E8%B4%A7",
                    "https://live.kuaishou.com/search/%E5%A5%BD%E7%89%A9",
                ]
                for search_url in search_urls:
                    if len(rooms) >= 5:
                        break
                    logger.info(f"正在导航到 {search_url} ...")
                    api_intercepted.clear()
                    await self._page.goto(search_url, wait_until="domcontentloaded", timeout=35000)
                    await self._random_delay(4, 7)

                    try:
                        await asyncio.wait_for(api_intercepted.wait(), timeout=10)
                    except asyncio.TimeoutError:
                        logger.info(f"API 拦截超时 ({search_url})")

                    for _ in range(3):
                        await self._variable_scroll(self._page, "down")
                        await self._random_delay(2, 4)

                    if not rooms:
                        logger.info(f"在 {search_url} 上尝试 DOM 降级方案...")
                        search_rooms = await self._scrape_rooms_from_dom()
                        rooms.extend(search_rooms)

            # ── 首页作为最后手段（严格过滤）──
            if len(rooms) < 3:
                logger.info("购物页和搜索页未获取足够房间，尝试首页...")
                api_intercepted.clear()
                await self._page.goto("https://live.kuaishou.com", wait_until="domcontentloaded", timeout=35000)
                await self._random_delay(4, 6)

                try:
                    await asyncio.wait_for(api_intercepted.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

                for _ in range(5):
                    await self._variable_scroll(self._page, "down")
                    await self._random_delay(2, 3)

                if not rooms:
                    logger.info("在首页上尝试 DOM 降级方案...")
                    home_rooms = await self._scrape_rooms_from_dom()
                    # 首页的房间需要严格过滤（不标记 source_page）
                    rooms.extend(home_rooms)

        except Exception as e:
            logger.error(f"发现快手直播间时出错: {e}")
            if not rooms:
                try:
                    rooms = await self._scrape_rooms_from_dom()
                except Exception:
                    pass
        finally:
            self._page.remove_listener("response", _on_response)

        await self._save_cookies()
        # 过滤非带货直播间（购物分类页的房间信任为带货，跳过过滤）
        before = len(rooms)
        rooms = [r for r in rooms if r.get('source_page') == 'shopping_category' or _is_commerce(r)]
        if before > len(rooms):
            logger.info(f"过滤掉 {before - len(rooms)} 个非带货直播间")
        result = rooms[:limit]
        logger.info(f"共发现 {len(result)} 个快手带货直播间")
        return result

    def _extract_room_from_api(self, item: dict) -> Optional[dict]:
        try:
            live_info = item.get("liveInfo", item.get("live", item))
            if not isinstance(live_info, dict):
                live_info = item
            author_info = live_info.get("author", live_info.get("user", {}))
            if not isinstance(author_info, dict):
                author_info = {}

            room_id = str(live_info.get("id", "") or live_info.get("liveId", "")
                          or live_info.get("streamId", "") or item.get("id", "") or "")
            if not room_id:
                return None

            anchor_name = (author_info.get("name", "") or author_info.get("userName", "")
                           or author_info.get("nickname", "") or live_info.get("caption", ""))
            room_name = (live_info.get("caption", "") or live_info.get("title", "")
                         or live_info.get("name", "") or anchor_name)
            viewer_count = _parse_count(str(live_info.get("viewCount", "")
                                            or live_info.get("displayViewCount", "")
                                            or live_info.get("onlineCount", "") or "0"))

            cover_urls = live_info.get("coverUrl", "")
            cover_url = cover_urls[0] if isinstance(cover_urls, list) and cover_urls else (
                cover_urls if isinstance(cover_urls, str) else "")

            author_id = str(author_info.get("id", "") or author_info.get("userId", "") or "")
            live_url = f"https://live.kuaishou.com/u/{author_id}" if author_id else f"https://live.kuaishou.com/u/{room_id}"

            category = "带货"
            cat_val = live_info.get("category", "")
            if isinstance(cat_val, dict):
                category = cat_val.get("name", "带货") or "带货"
            elif isinstance(cat_val, str) and cat_val:
                category = cat_val

            return {
                "room_id": room_id, "room_name": room_name, "anchor_name": anchor_name,
                "viewer_count": viewer_count, "viewer_count_text": "",
                "category": category, "cover_url": cover_url, "live_url": live_url,
                "source": "api", "platform": "kuaishou",
            }
        except Exception as e:
            logger.warning(f"提取快手房间数据失败: {e}")
            return None

    async def _scrape_rooms_from_dom(self) -> list[dict]:
        rooms = []
        try:
            all_links = await self._page.evaluate("""
                () => {
                    const links = document.querySelectorAll('a[href]');
                    const results = [];
                    for (const a of links) {
                        const href = a.href || '';
                        const match = href.match(/live\\.kuaishou\\.com\\/u\\/([^/?#]+)/);
                        if (match && match[1]) {
                            const card = a.closest('div') || a;
                            const text = card.innerText || '';
                            const img = card.querySelector('img');
                            results.push({
                                userId: match[1],
                                text: text, imgUrl: img ? img.src : ''
                            });
                        }
                    }
                    const seen = new Set();
                    return results.filter(r => {
                        if (seen.has(r.userId)) return false;
                        seen.add(r.userId); return true;
                    });
                }
            """)
            if all_links:
                logger.info(f"DOM 降级：从链接中提取到 {len(all_links)} 个快手直播间")
                for item in all_links:
                    user_id = item.get("userId", "")
                    if not user_id:
                        continue
                    raw_text = (item.get("text", "") or "").strip()
                    text_lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
                    anchor_name = text_lines[0] if text_lines else ""
                    room_name = text_lines[1] if len(text_lines) > 1 else anchor_name
                    # Store title separately for filter
                    title = room_name
                    rooms.append({
                        "room_id": user_id, "room_name": room_name, "anchor_name": anchor_name,
                        "title": title, "viewer_count": 0, "category": "带货",
                        "cover_url": item.get("imgUrl", ""),
                        "live_url": f"https://live.kuaishou.com/u/{user_id}",
                        "source": "dom", "platform": "kuaishou",
                    })
        except Exception as e:
            logger.warning(f"DOM 降级抓取失败: {e}")
        return rooms

    async def close(self):
        logger.info("正在关闭快手浏览器...")
        await self._save_cookies()
        if self._context:
            await self._context.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("快手浏览器已关闭")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    parser = argparse.ArgumentParser(description="快手直播间爬虫")
    parser.add_argument("mode", choices=["discover"], help="运行模式")
    parser.add_argument("--limit", type=int, default=20, help="最大房间数")
    args = parser.parse_args()

    async def run():
        crawler = KuaishouLiveCrawler()
        await crawler.init_browser()
        try:
            rooms = await crawler.discover_live_rooms(limit=args.limit)
            for r in rooms:
                print(f"  [{r.get('source')}] {r.get('anchor_name')} | {r.get('room_name')} | {r.get('live_url')}")
            print(f"\\n共 {len(rooms)} 个直播间")
        finally:
            await crawler.close()

    asyncio.run(run())
