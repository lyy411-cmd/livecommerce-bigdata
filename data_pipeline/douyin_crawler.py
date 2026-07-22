"""
抖音直播间爬虫模块
基于 Playwright 实现抖音直播间数据采集，包括：
- 直播间发现（从首页 API 拦截房间列表）
- 直播间详情抓取（从进入房间 API 拦截详情数据）
- 弹幕流实时抓取（通过 WebSocket 拦截 Protobuf 弹幕消息）
- 商品列表抓取（购物车商品接口）
- Cookie 持久化与反检测机制
"""

import asyncio
import base64
import json
import logging
import os
import random
import re
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from typing import Callable, Optional

try:
    import websockets
    _HAS_WEBSOCKETS = True
except ImportError:
    _HAS_WEBSOCKETS = False

# ---------------------------------------------------------------------------
# 基础路径配置
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
# Chrome crashes with non-ASCII user-data-dir on Windows, use home dir path
BROWSER_PROFILES_DIR = Path.home() / ".qoderworkcn" / "douyin_browser_profile"
COOKIES_DIR = BASE_DIR / "cookies"
COOKIES_FILE = COOKIES_DIR / "douyin_cookies.json"

# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 反检测配置
# ---------------------------------------------------------------------------
REALISTIC_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

STEALTH_JS = """
// 隐藏 webdriver 标志
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// 伪造 Chrome 运行环境
window.chrome = {
    runtime: {},
    loadTimes: function() {},
    csi: function() {},
    app: { isInstalled: false },
};

// 伪造权限查询
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) =>
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters);

// 伪造 plugins 数组
Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5],
});

// 伪造 languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['zh-CN', 'zh', 'en-US', 'en'],
});
"""


def _parse_count(text: str) -> int:
    """
    将抖音显示的人数文本转为整数。
    示例:
        '3.2万' -> 32000
        '12万'  -> 120000
        '5234'  -> 5234
        ''      -> 0
    """
    if not text:
        return 0
    text = text.strip()
    # 匹配 "数字万" 格式
    match = re.match(r"([\d.]+)\s*万", text)
    if match:
        return int(float(match.group(1)) * 10000)
    # 匹配 "数字亿" 格式
    match = re.match(r"([\d.]+)\s*亿", text)
    if match:
        return int(float(match.group(1)) * 100000000)
    # 尝试直接解析为整数
    try:
        return int(text.replace(",", ""))
    except (ValueError, AttributeError):
        return 0


# ---------------------------------------------------------------------------
# 非带货关键词过滤
# ---------------------------------------------------------------------------
NON_COMMERCE_KEYWORDS = [
    # ── 游戏 ──
    '游戏', 'CF', '穿越火线', '梦幻西游', '王者荣耀', '和平精英',
    '蛋仔派对', 'LOL', '英雄联盟', '吃鸡', '原神', '电竞', 'CSGO',
    'CS2', 'DOTA', 'APEX', '瓦罗兰特', '永劫无间', '使命召唤', 'PUBG',
    '暗黑', '传奇', 'DNF', '火影', '海贼王', '棋牌', '象棋', '围棋',
    '斗地主', '麻将', '游戏解说', '打游戏', '开黑', '上分', '排位',
    'MC', '我的世界', '迷你世界', 'Roblox', '第五人格', '光遇',
    '三国杀', '炉石', '云顶', '金铲铲', '暗区突围', '元梦之星',
    '星穹铁道', '崩坏', '绝区零', '幻兽帕鲁', '黑神话', '悟空',
    'FIFA', 'NBA2K', '实况足球', '跑跑卡丁车', 'QQ飞车',
    '植物大战僵尸', '方舟', '无畏契约', 'Valorant', '捕鱼',
    '国服', '小极品', '身法', '傲视', '刺激战场', '荒野行动',
    '问道', '大话西游', '诛仙', '剑灵', '天涯明月刀',
    '摸金', '血影', '外传', 'KPL', '对战', '水友赛',
    # ── 娱乐/才艺 ──
    '唱歌', '跳舞', '才艺', 'DJ', '打碟', '喊麦', '脱口秀', '相声',
    '音乐', '演奏', '钢琴', '吉他', '绘画', '书法', '手工',
    '八卦', '娱乐', '搞笑', '段子', '综艺',
    '星秀', '秀场', '颜值', '情感', '相亲', '交友', '占卜', '星座',
    # ── 聊天/ASMR ──
    '聊天', '陪聊', '哄睡', 'ASMR',
    # ── 本地服务（非电商带货）──
    '防水', '补漏', '足道', 'SPA', '驾校', '装修', '搬家', '开锁',
    '维修', '家政', '保洁', '疏通', '婚庆', '摄影', '纹身', '美甲店',
    '理发', '美发', 'spa', '按摩', '养生馆', '诊所', '律师',
    '包工', '建房', '施工', '工地', '自建房', '俱乐部', '解惑',
    # ── 户外/宠物 ──
    '钓鱼', '户外', '旅行', '旅游', '酒吧', '夜店', '蹦迪', 'KTV',
    '陪玩', '代练', '宠物', '萌宠', '猫咪', '狗狗',
    '动漫', '二次元', 'cosplay',
    # ── 个人直播/非带货 ──
    '新人首播', '第一天', '第一次', '正在直播',
    '认识你', '很高兴', '来了', '来啦', '来人',
    '御姐', '温柔', '白给', '心态好', '锐评', '试玩',
    '扎起来', '对手呢', '迷路了', '战斗', '新版本', '强度测试',
    # ── 教育/培训 ──
    '培训', '课程', '学习', '考试', '教学', '辅导',
    '志愿', '表单', '认知', '成长', '发展中心',
    # ── 新闻/体育 ──
    '新闻', '资讯', '体育', '足球', '篮球', 'NBA', 'CBA',
]

# 正面带货信号 - 房间必须包含至少一个才能通过过滤
COMMERCE_POSITIVE_KEYWORDS = [
    '官方', '旗舰', '带货', '好物', '秒杀', '福利', '优惠', '特卖',
    '品牌', '正品', '下单', '购物车', '小黄车', '商品', '店铺',
    '食品', '美妆', '护肤', '服饰', '穿搭', '零食', '家居',
    '珠宝', '首饰', '母婴', '运动', '数码', '家电', '鞋',
    '包', '手表', '包邮', '专场', '工厂', '源头', '直发', '限时',
    '女装', '男装', '童装', '内衣', '化妆', '口红', '面膜',
    '零食', '坚果', '茶', '酒', '生鲜', '水果', '粮油',
    '床品', '家纺', '厨具', '收纳', '清洁', '日用',
    '玩具', '文具', '图书', '宠物粮', '猫粮', '狗粮',
]


def _is_commerce(room: dict) -> bool:
    """严格过滤：排除非带货 + 要求正面带货信号"""
    name = room.get('room_name', '') or ''
    anchor = room.get('anchor_name', '') or ''
    cat = room.get('category', '') or ''
    title = room.get('title', '') or ''
    combined = f"{name} {anchor} {cat} {title}".lower()

    # 第一关：排除包含非带货关键词的房间
    for kw in NON_COMMERCE_KEYWORDS:
        if kw.lower() in combined:
            return False

    # 第二关：要求至少一个正面带货信号（在名字或主播名中）
    # 分类字段不算（分类可能只是页面分类，不代表房间本身是带货）
    name_anchor = f"{name} {anchor} {title}".lower()
    for kw in COMMERCE_POSITIVE_KEYWORDS:
        if kw.lower() in name_anchor:
            return True

    # 没找到任何带货信号 → 拒绝
    return False


# ============================================================================
#  PlaywrightBrowserPool - 浏览器上下文池管理器
# ============================================================================
class PlaywrightBrowserPool:
    """
    管理 Playwright 浏览器上下文的连接池。
    限制同时打开的浏览器上下文数量，避免资源耗尽。
    """

    def __init__(self, max_size: int = 3):
        self.max_size = max_size
        self._semaphore = asyncio.Semaphore(max_size)
        self._contexts: list = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        """获取一个信号量，阻塞直到有空位"""
        await self._semaphore.acquire()

    def release(self):
        """释放一个信号量"""
        self._semaphore.release()

    async def add_context(self, context):
        """将上下文加入池进行跟踪"""
        async with self._lock:
            self._contexts.append(context)

    async def remove_context(self, context):
        """从池中移除上下文"""
        async with self._lock:
            if context in self._contexts:
                self._contexts.remove(context)

    @property
    def active_count(self) -> int:
        return len(self._contexts)

    async def close_all(self):
        """关闭所有跟踪的上下文"""
        async with self._lock:
            for ctx in self._contexts:
                try:
                    await ctx.close()
                except Exception:
                    pass
            self._contexts.clear()


# ============================================================================
#  DouyinLiveCrawler - 抖音直播间爬虫主类
# ============================================================================
class DouyinLiveCrawler:
    """
    抖音直播间爬虫，基于 Playwright 异步实现。
    支持：房间发现、房间详情、弹幕流抓取、商品列表获取。
    """

    def __init__(self, kafka_producer=None, headless: bool = False):
        """
        初始化爬虫实例。

        Args:
            kafka_producer: Kafka 生产者实例（可选），用于消息推送。
            headless:       是否无头模式（默认 False，有头模式更不易被检测）。
        """
        self.kafka_producer = kafka_producer
        self.headless = headless
        self.max_concurrent_rooms = 3

        # 浏览器池
        self.browser_pool = PlaywrightBrowserPool(max_size=self.max_concurrent_rooms)

        # Playwright 实例引用
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

        # 延迟导入 protobuf 解码器（可能尚未安装）
        self._decoder = None
        try:
            from data_pipeline.proto.douyin_decoder import (
                decode_websocket_frame,
                build_ack_frame,
            )
            self._decode_websocket_frame = decode_websocket_frame
            self._build_ack_frame = build_ack_frame
            self._decoder = True
            logger.info("已加载抖音 Protobuf 解码器")
        except ImportError:
            logger.warning(
                "未找到 proto.douyin_decoder 模块，弹幕解码功能不可用。"
                "请确保已编译 .proto 文件并放置到 data_pipeline/proto/ 目录。"
            )
            self._decode_websocket_frame = None
            self._build_ack_frame = None

        # 反检测：尝试加载 playwright-stealth
        self._stealth_available = False
        try:
            from playwright_stealth import stealth_async
            self._stealth_async = stealth_async
            self._stealth_available = True
            logger.info("已加载 playwright-stealth 反检测插件")
        except ImportError:
            logger.info("playwright-stealth 未安装，将使用内置反检测脚本")
            self._stealth_async = None

        # 确保目录存在
        BROWSER_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        COOKIES_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    #  Cookie 持久化
    # ------------------------------------------------------------------
    def _load_cookies(self) -> list[dict]:
        """从文件加载已保存的 Cookie"""
        if COOKIES_FILE.exists():
            try:
                with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                    cookies = json.load(f)
                logger.info(f"已加载 {len(cookies)} 条 Cookie")
                return cookies
            except Exception as e:
                logger.warning(f"加载 Cookie 失败: {e}")
        return []

    async def _save_cookies(self):
        """保存当前浏览器上下文的 Cookie 到文件"""
        if self._context:
            try:
                cookies = await self._context.cookies()
                with open(COOKIES_FILE, "w", encoding="utf-8") as f:
                    json.dump(cookies, f, ensure_ascii=False, indent=2)
                logger.info(f"已保存 {len(cookies)} 条 Cookie")
            except Exception as e:
                logger.warning(f"保存 Cookie 失败: {e}")

    # ------------------------------------------------------------------
    #  反检测辅助方法
    # ------------------------------------------------------------------
    async def _random_delay(self, min_s: float = 2.0, max_s: float = 5.0):
        """随机延迟，模拟人类操作节奏"""
        await asyncio.sleep(random.uniform(min_s, max_s))

    async def _mouse_jitter(self, page):
        """鼠标随机微动，模拟人类行为"""
        try:
            x = random.randint(100, 800)
            y = random.randint(100, 600)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
        except Exception:
            pass

    async def _variable_scroll(self, page, direction: str = "down"):
        """变速滚动，模拟真实滚动行为"""
        delta = random.randint(300, 800)
        if direction == "up":
            delta = -delta
        steps = random.randint(3, 6)
        per_step = delta // steps
        for _ in range(steps):
            await page.mouse.wheel(0, per_step)
            await asyncio.sleep(random.uniform(0.05, 0.15))

    # ------------------------------------------------------------------
    #  浏览器初始化
    # ------------------------------------------------------------------
    async def init_browser(self):
        """
        初始化 Playwright 浏览器实例。
        - 使用持久化上下文保存登录状态
        - 注入反检测脚本
        - 加载已保存的 Cookie
        """
        from playwright.async_api import async_playwright

        logger.info("正在启动 Playwright 浏览器...")

        self._playwright = await async_playwright().start()

        # 使用持久化上下文，保存浏览器数据到 user_data_dir
        # 先尝试清理残留 lockfile（上次崩溃可能未释放）
        lockfile_path = os.path.join(str(BROWSER_PROFILES_DIR), 'lockfile')
        if os.path.exists(lockfile_path):
            try:
                os.remove(lockfile_path)
            except PermissionError:
                pass  # 文件被占用，后续 fallback 会处理

        try:
            self._context = await self._playwright.chromium.launch_persistent_context(
                user_data_dir=str(BROWSER_PROFILES_DIR),
                channel="chrome",
                headless=self.headless,
                viewport={"width": 1920, "height": 1080},
                user_agent=REALISTIC_USER_AGENT,
                locale="zh-CN",
                timezone_id="Asia/Shanghai",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disk-cache-size=1",
                    "--disable-background-networking",
                ],
                ignore_https_errors=True,
            )
        except Exception as pw_err:
            logger.warning(f"Persistent context failed: {pw_err}")
            logger.info("Falling back to temporary profile directory...")
            import tempfile, shutil
            tmp_profile = tempfile.mkdtemp(prefix="douyin_crawl_")
            # 尝试拷贝已保存的 Cookie/状态到新临时目录
            # 自定义 copy 函数：跳过被锁定的文件 (WinError 32)
            def _safe_copy(src_f, dst_f):
                try:
                    shutil.copy2(src_f, dst_f)
                except (PermissionError, OSError):
                    pass  # 文件被占用，跳过
            for item in ('Local State', 'Default'):
                src = os.path.join(str(BROWSER_PROFILES_DIR), item)
                if os.path.exists(src):
                    dst = os.path.join(tmp_profile, item)
                    if os.path.isdir(src):
                        try:
                            shutil.copytree(src, dst, dirs_exist_ok=True, copy_function=_safe_copy)
                        except Exception:
                            pass  # 部分文件被锁定，尽力而为
                    else:
                        try:
                            shutil.copy2(src, dst)
                        except (PermissionError, OSError):
                            pass
            self._context = await self._playwright.chromium.launch_persistent_context(
                user_data_dir=tmp_profile,
                channel="chrome",
                headless=self.headless,
                viewport={"width": 1920, "height": 1080},
                user_agent=REALISTIC_USER_AGENT,
                locale="zh-CN",
                timezone_id="Asia/Shanghai",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disk-cache-size=1",
                    "--disable-background-networking",
                ],
                ignore_https_errors=True,
            )
            self._temp_profile = tmp_profile

        # ── 资源阻断已禁用 ──
        # 弹幕 SDK 需要完整加载所有资源（CSS/图片等）才能正确建立 WebSocket。
        # 之前尝试阻断资源导致 WS 无法建立。
        # async def _block_heavy_resources(route):
        #     rt = route.request.resource_type
        #     if rt in ('stylesheet', 'font', 'media'):
        #         await route.abort()
        #     elif rt == 'image':
        #         url = route.request.url
        #         if any(x in url for x in ['captcha', 'verify', 'slardar']):
        #             await route.continue_()
        #         else:
        #             await route.abort()
        #     else:
        #         await route.continue_()
        # await self._context.route('**/*', _block_heavy_resources)
        logger.info("资源阻断已禁用（弹幕SDK需要完整加载）")

        # 注入反检测 JS 到每个新页面
        await self._context.add_init_script(STEALTH_JS)

        # 如果 playwright-stealth 可用，对现有页面应用
        if self._stealth_available and self._context.pages:
            for page in self._context.pages:
                await self._stealth_async(page)

        # 获取或创建页面
        if self._context.pages:
            self._page = self._context.pages[0]
        else:
            self._page = await self._context.new_page()

        # 加载已保存的 Cookie
        saved_cookies = self._load_cookies()
        if saved_cookies:
            try:
                await self._context.add_cookies(saved_cookies)
                logger.info("已恢复保存的 Cookie")
            except Exception as e:
                logger.warning(f"恢复 Cookie 失败（可能已过期）: {e}")

        # 先访问抖音主站建立会话（CDN cookie、反检测验证等）
        # 这样后续访问 live.douyin.com 房间页时不容易被 ERR_ABORTED
        try:
            await self._page.goto("https://www.douyin.com", wait_until="domcontentloaded", timeout=20000)
            await self._random_delay(2, 4)
            logger.info("已访问抖音主站建立会话")
        except Exception as e:
            logger.warning(f"访问抖音主站失败（不影响后续）: {e}")

        logger.info("浏览器初始化完成")

    async def _check_login_status(self) -> bool:
        """检查当前是否已登录抖音"""
        try:
            cookies = await self._context.cookies()
            cookie_names = {c['name'] for c in cookies}
            # 登录态 cookie: sessionid, sid_guard, passport_auth_status 等
            login_indicators = {'sessionid', 'sessionid_ss', 'sid_guard',
                                'passport_auth_status', 'passport_csrf_token'}
            return bool(login_indicators & cookie_names)
        except Exception:
            return False

    async def ensure_logged_in(self, wait_timeout: int = 300) -> bool:
        """
        确保已登录抖音。
        如未登录且浏览器可见，打开登录页等待用户手动登录。

        Args:
            wait_timeout: 最长等待秒数（默认 5 分钟）

        Returns:
            True 表示已登录，False 表示未登录
        """
        if await self._check_login_status():
            logger.info("已检测到登录状态")
            return True

        if self.headless:
            logger.warning("未登录且处于 headless 模式，无法手动登录")
            return False

        logger.info("未检测到登录状态，请在浏览器中手动登录抖音...")
        print("\n" + "=" * 50)
        print("  [登录提示] 请在打开的浏览器中登录抖音账号")
        print(f"  等待最长 {wait_timeout} 秒...")
        print("=" * 50 + "\n")

        try:
            await self._page.goto(
                "https://www.douyin.com",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await self._random_delay(2, 4)

            # 尝试点击登录按钮
            try:
                login_btn = await self._page.query_selector(
                    'button:has-text("登录"), [class*="login-button"], '
                    '[data-e2e="user-login"]'
                )
                if login_btn:
                    await login_btn.click()
                    logger.info("已点击登录按钮")
            except Exception:
                pass

            # 轮询等待登录状态
            start = time.time()
            while time.time() - start < wait_timeout:
                await asyncio.sleep(5)
                if await self._check_login_status():
                    logger.info("登录成功！正在保存 Cookie...")
                    await self._save_cookies()
                    print("  [登录提示] 登录成功，Cookie 已保存")
                    return True
                elapsed = int(time.time() - start)
                if elapsed % 30 == 0 and elapsed > 0:
                    print(f"  [登录提示] 等待登录中... ({elapsed}s)")

            logger.warning(f"登录等待超时 ({wait_timeout}s)")
            print("  [登录提示] 登录等待超时，弹幕采集将跳过")
            return False

        except Exception as e:
            logger.error(f"登录流程出错: {e}")
            return False

    # ------------------------------------------------------------------
    #  直播间发现
    # ------------------------------------------------------------------
    async def discover_live_rooms(self, limit: int = 20) -> list[dict]:
        """
        从抖音直播首页发现正在直播的房间。

        通过拦截 XHR 响应获取房间列表 API 数据，
        如果 API 拦截失败则降级到 DOM 元素抓取。

        Args:
            limit: 最多返回的房间数量。

        Returns:
            房间信息字典列表，每个字典包含:
            room_id, room_name, anchor_name, viewer_count, category,
            cover_url, live_url
        """
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        rooms: list[dict] = []
        api_intercepted = asyncio.Event()

        async def _on_response(response):
            """拦截直播间列表 API 响应"""
            url = response.url
            # 跳过重定向和非成功响应
            if response.status < 200 or response.status >= 300:
                return
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type and "javascript" not in content_type:
                return
            # 匹配房间列表 API
            if "webcast/web/partition/detail/room" in url:
                try:
                    body = await response.json()
                    data = body.get("data", {})
                    room_list = data.get("room_list", [])
                    if not room_list:
                        # 尝试其他可能的数据结构
                        room_list = data.get("rooms", [])
                        # 再尝试嵌套结构
                        if not room_list and isinstance(data, dict):
                            for key in data:
                                if isinstance(data[key], list) and len(data[key]) > 0:
                                    room_list = data[key]
                                    break

                    for item in room_list:
                        room_info = self._extract_room_from_api(item)
                        if room_info:
                            rooms.append(room_info)

                    if rooms:
                        api_intercepted.set()
                        logger.info(f"从 API 拦截到 {len(rooms)} 个直播间")
                except Exception as e:
                    logger.debug(f"解析房间列表 API 响应失败: {e}")

            # 也尝试捕获其他可能包含房间数据的接口
            elif ("webcast/room/web/" in url and "enter" not in url) or \
                 "webcast/web/partition/detail" in url or \
                 "/web/live/feed" in url or \
                 "/web/live/search" in url:
                try:
                    body = await response.json()
                    data = body.get("data", {})
                    if isinstance(data, dict):
                        for key in ("room_list", "rooms", "list", "data"):
                            candidate = data.get(key, [])
                            if isinstance(candidate, list) and len(candidate) > 0:
                                for item in candidate:
                                    room_info = self._extract_room_from_api(item)
                                    if room_info:
                                        rooms.append(room_info)
                                if rooms:
                                    api_intercepted.set()
                                    break
                except Exception:
                    pass

        # 注册响应拦截器
        self._page.on("response", _on_response)

        try:
            # 导航到带货/电商类直播分类页面（不使用首页兜底，避免抓到娱乐直播）
            commerce_urls = [
                "https://live.douyin.com/category/100102",   # 综合带货
                "https://live.douyin.com/category/100108",   # 美食带货
                "https://live.douyin.com/category/100106",   # 服饰穿搭
                "https://live.douyin.com/category/100101",   # 美妆护肤
                "https://live.douyin.com/category/100105",   # 母婴童装
                "https://live.douyin.com/category/100109",   # 数码家电
                "https://live.douyin.com/category/100103",   # 家居日用
                "https://live.douyin.com/category/100107",   # 鞋帽箱包
            ]
            for commerce_url in commerce_urls:
                logger.info(f"正在导航到 {commerce_url} ...（已累计 {len(rooms)} 个房间）")
                api_intercepted.clear()  # 每个分类页重新等待
                try:
                    await self._page.goto(
                        commerce_url,
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                except Exception as nav_err:
                    logger.warning(f"导航 {commerce_url} 失败: {nav_err}")
                    continue
                await self._random_delay(3, 6)

                # 如果 playwright-stealth 可用，应用之
                if self._stealth_available:
                    await self._stealth_async(self._page)

                # 等待 API 响应（最多 8 秒）
                try:
                    await asyncio.wait_for(api_intercepted.wait(), timeout=8)
                except asyncio.TimeoutError:
                    logger.info(f"API 拦截超时 ({commerce_url})，尝试滚动加载...")

                # 滚动加载更多房间（增加到8次以获取更多数据）
                for _ in range(8):
                    await self._variable_scroll(self._page, "down")
                    await self._random_delay(2, 4)

                # 当前页面 API 拦截失败 → 在此页面上尝试 DOM 抓取
                if not rooms:
                    logger.info(f"在 {commerce_url} 上尝试 DOM 降级方案...")
                    rooms = await self._scrape_rooms_from_dom()
                    if rooms:
                        logger.info(f"DOM 降级在 {commerce_url} 获取到 {len(rooms)} 个房间")

            # 如果所有页面都没获取到房间，做最后一次尝试
            if not rooms:
                # 回到第一个分类页再试一次
                logger.info("所有分类页均未获取到房间，最后尝试首页 DOM...")
                try:
                    await self._page.goto(
                        "https://live.douyin.com/category/100102",
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    await self._random_delay(3, 5)
                    for _ in range(5):
                        await self._variable_scroll(self._page, "down")
                        await self._random_delay(2, 3)
                    rooms = await self._scrape_rooms_from_dom()
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"发现直播间时出错: {e}")
            # 降级尝试 DOM 抓取
            if not rooms:
                try:
                    rooms = await self._scrape_rooms_from_dom()
                except Exception:
                    pass
        finally:
            # 移除响应拦截器
            self._page.remove_listener("response", _on_response)

        # 保存 Cookie
        await self._save_cookies()

        # 过滤非带货直播间
        before = len(rooms)
        rooms = [r for r in rooms if _is_commerce(r)]
        if before > len(rooms):
            logger.info(f"过滤掉 {before - len(rooms)} 个非带货直播间")

        # 限制返回数量
        result = rooms[:limit]
        logger.info(f"共发现 {len(result)} 个带货直播间")
        return result

    def _extract_room_from_api(self, item: dict) -> Optional[dict]:
        """从 API 返回的房间数据中提取房间信息"""
        try:
            room = item.get("room", item)
            room_id = str(room.get("id_str", room.get("id", "")))
            if not room_id:
                return None
            # web_rid is the URL-friendly ID used in Douyin URLs
            web_rid = str(room.get("web_rid", ""))

            owner = room.get("owner", {})
            stats = room.get("stats", {})

            # 尝试多种方式获取观众数
            viewer_count = 0
            # 1. text fields
            for text_key in ["total_user_desp", "total_user_desc"]:
                text_val = stats.get(text_key, "")
                if text_val:
                    viewer_count = _parse_count(str(text_val))
                    if viewer_count > 0:
                        break
            # 2. integer fields in stats
            if viewer_count == 0:
                for int_key in ["total_user", "user_count", "total_user_count"]:
                    val = stats.get(int_key, 0)
                    if val and int(val) > 0:
                        viewer_count = int(val)
                        break
            # 3. room-level fields
            if viewer_count == 0:
                for key in ["user_count_str", "user_count"]:
                    val = room.get(key, 0)
                    if val:
                        try:
                            viewer_count = int(val)
                        except (ValueError, TypeError):
                            viewer_count = _parse_count(str(val))
                    if viewer_count > 0:
                        break
            # 4. item-level fields
            if viewer_count == 0:
                for key in ["user_count", "viewer_count", "watch_count"]:
                    val = item.get(key, 0)
                    if val and int(val) > 0:
                        viewer_count = int(val)
                        break

            return {
                "room_id": room_id,
                "room_name": room.get("title", ""),
                "anchor_name": owner.get("nickname", ""),
                "viewer_count": viewer_count,
                "viewer_count_text": stats.get("total_user_desp", ""),
                "category": item.get("partition", {}).get("title", ""),
                "cover_url": (
                    room.get("cover", {}).get("url_list", [""])[0]
                    if isinstance(room.get("cover"), dict)
                    else ""
                ),
                "live_url": f"https://live.douyin.com/{web_rid or room_id}",
                "source": "api",
            }
        except Exception as e:
            logger.warning(f"提取房间数据失败: {e}")
            return None

    async def _scrape_rooms_from_dom(self) -> list[dict]:
        """
        降级方案：从 DOM 元素中抓取房间卡片信息。
        当 API 拦截失败时使用。多种策略逐级尝试。
        """
        rooms = []
        try:
            # === 策略1: 从页面所有链接中提取直播间 URL ===
            # 抖音直播间链接格式: https://live.douyin.com/{纯数字room_id}
            all_links = await self._page.evaluate("""
                () => {
                    const links = document.querySelectorAll('a[href]');
                    const results = [];
                    for (const a of links) {
                        const href = a.href || '';
                        const match = href.match(/live\\.douyin\\.com\\/(\\d+)/);
                        if (match) {
                            const card = a.closest('div') || a;
                            const text = card.innerText || '';
                            const img = card.querySelector('img');
                            results.push({
                                roomId: match[1],
                                text: text,
                                imgUrl: img ? img.src : ''
                            });
                        }
                    }
                    // 去重
                    const seen = new Set();
                    return results.filter(r => {
                        if (seen.has(r.roomId)) return false;
                        seen.add(r.roomId);
                        return true;
                    });
                }
            """)

            if all_links:
                logger.info(f"DOM 降级策略1：从链接中提取到 {len(all_links)} 个直播间")
                for item in all_links:
                    room_id = item.get("roomId", "")
                    if not room_id:
                        continue
                    text_lines = [l.strip() for l in (item.get("text", "") or "").split("\n") if l.strip()]
                    anchor_name = text_lines[0] if text_lines else ""
                    room_name = text_lines[1] if len(text_lines) > 1 else anchor_name
                    rooms.append({
                        "room_id": room_id,
                        "room_name": room_name,
                        "anchor_name": anchor_name,
                        "viewer_count": 0,
                        "viewer_count_text": "",
                        "category": "",
                        "cover_url": item.get("imgUrl", ""),
                        "live_url": f"https://live.douyin.com/{room_id}",
                        "source": "dom",
                    })
                return rooms

            # === 策略2: 从 script 标签中提取嵌入的 JSON 数据 ===
            script_data = await self._page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script');
                    for (const s of scripts) {
                        const text = s.textContent || '';
                        // 查找包含房间数据的 JSON
                        if (text.includes('room_list') || text.includes('roomList') || text.includes('room_id')) {
                            try {
                                // 尝试提取 JSON 对象
                                const match = text.match(/\\{[^]*"room[^]*\\}/);
                                if (match) return match[0].substring(0, 5000);
                            } catch(e) {}
                        }
                        // __NEXT_DATA__ (Next.js)
                        if (s.id === '__NEXT_DATA__') {
                            return text.substring(0, 10000);
                        }
                    }
                    return '';
                }
            """)

            if script_data:
                import json as _json
                try:
                    data = _json.loads(script_data) if script_data.startswith('{') else {}
                    # 递归查找房间列表
                    def find_rooms(obj, depth=0):
                        if depth > 5 or not isinstance(obj, dict):
                            return []
                        for key in ("room_list", "roomList", "rooms", "list"):
                            val = obj.get(key)
                            if isinstance(val, list) and len(val) > 0:
                                return val
                        for val in obj.values():
                            if isinstance(val, dict):
                                r = find_rooms(val, depth+1)
                                if r: return r
                            elif isinstance(val, list):
                                for item in val:
                                    if isinstance(item, dict):
                                        r = find_rooms(item, depth+1)
                                        if r: return r
                        return []
                    room_list = find_rooms(data)
                    for item in room_list:
                        room_info = self._extract_room_from_api(item)
                        if room_info:
                            rooms.append(room_info)
                    if rooms:
                        logger.info(f"DOM 降级策略2：从嵌入 JSON 中提取到 {len(rooms)} 个直播间")
                        return rooms
                except Exception:
                    pass

            # === 策略3: 通用 DOM 选择器 ===
            selectors = [
                "a[href*='live.douyin.com/']",
                '[class*="RoomCard"]',
                '[class*="room-card"]',
                '[class*="live-card"]',
                '[class*="webcast-room"]',
                '[data-e2e="live_room_card"]',
                ".RE4m4Rj9",  # 常见的抖音 class (会变，仅做尝试)
            ]

            for selector in selectors:
                elements = await self._page.query_selector_all(selector)
                if elements and len(elements) >= 1:
                    logger.info(
                        f"DOM 降级策略3：使用选择器 '{selector}' 找到 {len(elements)} 个元素"
                    )
                    for el in elements:
                        try:
                            href = await el.get_attribute("href") or ""
                            room_id_match = re.search(r"/(\d+)", href)
                            room_id = room_id_match.group(1) if room_id_match else ""

                            text_content = await el.inner_text()
                            lines = [l.strip() for l in text_content.split("\n") if l.strip()]
                            anchor_name = lines[0] if lines else ""
                            room_name = lines[1] if len(lines) > 1 else anchor_name

                            if room_id:
                                rooms.append({
                                    "room_id": room_id,
                                    "room_name": room_name,
                                    "anchor_name": anchor_name,
                                    "viewer_count": 0,
                                    "viewer_count_text": "",
                                    "category": "",
                                    "cover_url": "",
                                    "live_url": f"https://live.douyin.com/{room_id}",
                                    "source": "dom",
                                })
                        except Exception:
                            continue
                    if rooms:
                        break

        except Exception as e:
            logger.warning(f"DOM 降级抓取失败: {e}")

        return rooms

    # ------------------------------------------------------------------
    #  房间详情
    # ------------------------------------------------------------------
    async def fetch_room_detail(self, room_id: str) -> dict:
        """
        获取单个直播间的详细信息。

        通过导航到房间页面并拦截 enter API 获取详情数据。

        Args:
            room_id: 抖音直播间 ID。

        Returns:
            包含房间详细信息的字典。
        """
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        detail: dict = {}
        detail_event = asyncio.Event()

        async def _on_response(response):
            """拦截房间详情 API"""
            url = response.url
            if "webcast/room/web/enter" in url:
                try:
                    body = await response.json()
                    data = body.get("data", {})
                    room = (
                        data.get("data", [{}])[0]
                        if isinstance(data.get("data"), list)
                        else data
                    )
                    if not room:
                        room = data.get("room", {})

                    owner = room.get("owner", {})
                    stats = room.get("stats", {})

                    detail.update({
                        "room_id": room.get("id_str", str(room_id)),
                        "room_name": room.get("title", ""),
                        "anchor_name": owner.get("nickname", ""),
                        "anchor_id": owner.get("id_str", ""),
                        "anchor_avatar": (
                            owner.get("avatar_thumb", {}).get("url_list", [""])[0]
                            if isinstance(owner.get("avatar_thumb"), dict)
                            else ""
                        ),
                        "viewer_count": _parse_count(
                            stats.get("total_user_desp", "")
                        ),
                        "viewer_count_text": stats.get("total_user_desp", ""),
                        "start_time": room.get("create_time", 0),
                        "cover_url": (
                            room.get("cover", {}).get("url_list", [""])[0]
                            if isinstance(room.get("cover"), dict)
                            else ""
                        ),
                        "live_url": f"https://live.douyin.com/{room_id}",
                        "status": room.get("status", 0),
                        "category": "",
                        "source": "api",
                    })
                    detail_event.set()
                    logger.info(f"已获取房间详情: {detail.get('room_name', '')}")
                except Exception as e:
                    logger.warning(f"解析房间详情 API 失败: {e}")

        self._page.on("response", _on_response)

        try:
            logger.info(f"正在获取房间详情: {room_id}")
            try:
                await self._page.goto(
                    f"https://live.douyin.com/{room_id}",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
            except Exception as nav_err:
                err_msg = str(nav_err)
                if "ERR_ABORTED" in err_msg or "net::" in err_msg:
                    logger.warning(f"房间详情页面导航被中断: {room_id}，继续等待API...")
                else:
                    raise
            await self._random_delay(2, 4)

            if self._stealth_available:
                await self._stealth_async(self._page)

            # 等待详情 API 响应
            try:
                await asyncio.wait_for(detail_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                logger.warning(f"房间详情 API 超时: {room_id}")

            # 如果 API 拦截失败，尝试从页面提取基本信息
            if not detail:
                detail = await self._scrape_room_detail_from_dom(room_id)

        except Exception as e:
            logger.error(f"获取房间详情失败: {e}")
            if not detail:
                detail = {
                    "room_id": room_id,
                    "live_url": f"https://live.douyin.com/{room_id}",
                }

        finally:
            self._page.remove_listener("response", _on_response)

        await self._save_cookies()
        return detail

    async def _scrape_room_detail_from_dom(self, room_id: str) -> dict:
        """降级：从 DOM 中提取房间详情"""
        detail = {
            "room_id": room_id,
            "live_url": f"https://live.douyin.com/{room_id}",
            "source": "dom",
        }
        try:
            title_el = await self._page.query_selector(
                '[class*="room-title"], [class*="RoomTitle"], h1'
            )
            if title_el:
                detail["room_name"] = await title_el.inner_text()

            name_el = await self._page.query_selector(
                '[class*="anchor-name"], [class*="AnchorName"], [class*="nickname"]'
            )
            if name_el:
                detail["anchor_name"] = await name_el.inner_text()

        except Exception as e:
            logger.warning(f"DOM 降级获取房间详情失败: {e}")

        return detail

    # ------------------------------------------------------------------
    #  直连 WebSocket 弹幕流（绕过浏览器页面加载）
    # ------------------------------------------------------------------
    def _get_ttwid_from_http(self, room_id: str) -> str:
        """通过 HTTP 请求获取 ttwid Cookie，优先使用已保存的 Cookie。"""
        # 先尝试从已保存的 Cookie 文件获取
        if COOKIES_FILE.exists():
            try:
                cookies = json.loads(COOKIES_FILE.read_text(encoding='utf-8'))
                for c in cookies:
                    if c.get('name') == 'ttwid':
                        logger.info(f"从 Cookie 文件获取 ttwid")
                        return c['value']
            except Exception:
                pass

        # 通过 HTTP 请求获取
        try:
            req = urllib.request.Request(
                f"https://live.douyin.com/{room_id}",
                headers={
                    'User-Agent': REALISTIC_USER_AGENT,
                    'Accept': 'text/html,application/xhtml+xml',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                },
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                cookie_headers = resp.headers.get_all('Set-Cookie') or []
                for ch in cookie_headers:
                    if 'ttwid=' in ch:
                        ttwid = ch.split('ttwid=')[1].split(';')[0]
                        if ttwid:
                            logger.info(f"从 HTTP 响应获取 ttwid")
                            return ttwid
        except Exception as e:
            logger.warning(f"HTTP 获取 ttwid 失败: {e}")

        return ''

    def _get_cookies_from_browser(self) -> str:
        """从浏览器上下文提取 Cookie 字符串（同步方法，需在 async 外调用或用 run_in_executor）。"""
        return ''  # 异步版本在 _async_get_browser_cookies 中实现

    async def _async_get_browser_cookies(self) -> str:
        """从浏览器上下文异步提取 Cookie 字符串。"""
        if not self._context:
            return ''
        try:
            cookies = await self._context.cookies()
            parts = []
            for c in cookies:
                parts.append(f"{c['name']}={c['value']}")
            return '; '.join(parts)
        except Exception as e:
            logger.warning(f"提取浏览器 Cookie 失败: {e}")
            return ''

    @staticmethod
    def _build_heartbeat_frame() -> bytes:
        """构建 WebSocket 心跳帧（精简 PushFrame）。"""
        from data_pipeline.proto.douyin_decoder import _encode_varint
        parts = []
        # field 6 (payloadEncoding), wire_type=2, tag = 50
        hb_enc = b'hb'
        parts.append(bytes([50]))
        parts.append(_encode_varint(len(hb_enc)))
        parts.append(hb_enc)
        # field 8 (payload), wire_type=2, tag = 66
        payload = b'\x00'
        parts.append(bytes([66]))
        parts.append(_encode_varint(len(payload)))
        parts.append(payload)
        return b''.join(parts)

    async def _sign_ws_url_with_playwright(self, web_rid: str, signing_page=None) -> str:
        """Use Playwright to call byted_acrawler.frontierSign() for X-Bogus URL signing.

        Args:
            web_rid: The web_rid (used as room_id in the URL).
            signing_page: A Playwright page already loaded on douyin.com (directory page).

        Returns:
            Signed WebSocket URL string, or unsigned URL if signing fails.
        """
        if not signing_page:
            logger.warning(f"[房间 {web_rid}] No signing page, using unsigned URL")
            return ''

        ts = str(int(time.time() * 1000))
        cursor = f"t-{ts}_r-1_d-1_u-1_h-1"
        internal_ext = (
            f"internal_src:dim|wss_push_room_id:{web_rid}"
            f"|wss_push_did:0|dim_log_id:{ts}"
            f"|fetch_time:{ts}|seq:1|wss_info:0-{ts}-0-0"
        )

        unsigned_params = urllib.parse.urlencode({
            'app_name': 'douyin_web',
            'version_code': '180800',
            'webcast_sdk_version': '1.0.14-beta.0',
            'update_version_code': '1.0.14-beta.0',
            'compress': 'gzip',
            'device_platform': 'web',
            'cookie_enabled': 'true',
            'screen_width': '1920',
            'screen_height': '1080',
            'browser_language': 'zh-CN',
            'browser_platform': 'Win32',
            'browser_name': 'Mozilla',
            'browser_version': '5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'browser_online': 'true',
            'tz_name': 'Asia/Shanghai',
            'cursor': cursor,
            'internal_ext': internal_ext,
            'host': 'https://live.douyin.com',
            'aid': '6383',
            'live_id': '1',
            'did_rule': '3',
            'endpoint': 'live_pc',
            'support_wrds': '1',
            'user_unique_id': '',
            'im_path': '/webcast/im/fetch/',
            'identity': 'audience',
            'room_id': web_rid,
            'heartbeatDuration': '0',
        })

        unsigned_url = f"wss://webcast5-ws-web-lf.douyin.com/webcast/im/push/v2/?{unsigned_params}"

        try:
            # Verify signing page is still alive (lightweight check, no navigation)
            _page_ok = False
            try:
                await signing_page.evaluate("1")
                _page_ok = True
            except Exception:
                logger.warning(f"[房间 {web_rid}] Signing page context dead, using unsigned URL")

            if not _page_ok:
                logger.warning(f"[房间 {web_rid}] Signing page still not ready after refresh")
                return unsigned_url

            result = await signing_page.evaluate("""(url) => {
                try {
                    if (typeof window.byted_acrawler === 'undefined' ||
                        typeof window.byted_acrawler.frontierSign !== 'function') {
                        return { error: 'byted_acrawler.frontierSign not available',
                                 has_ac: typeof window.byted_acrawler !== 'undefined' };
                    }
                    return window.byted_acrawler.frontierSign(url);
                } catch(e) {
                    return { error: e.message };
                }
            }""", unsigned_url)

            if isinstance(result, str):
                logger.info(f"[房间 {web_rid}] frontierSign returned signed URL string")
                return result
            elif isinstance(result, dict):
                if 'error' in result:
                    logger.warning(f"[房间 {web_rid}] frontierSign error: {result['error']}")
                    return unsigned_url
                x_bogus = result.get('X-Bogus', result.get('x-bogus', ''))
                if x_bogus:
                    signed_url = unsigned_url + '&X-Bogus=' + x_bogus
                    logger.info(f"[房间 {web_rid}] URL signed: X-Bogus={x_bogus[:24]}...")
                    return signed_url
                # Check if result contains a full signed URL
                _sign = result.get('signature', result.get('sign', ''))
                if _sign:
                    signed_url = unsigned_url + '&signature=' + _sign
                    logger.info(f"[房间 {web_rid}] URL signed via signature param")
                    return signed_url
                logger.warning(f"[房间 {web_rid}] frontierSign returned dict but no X-Bogus/signature: {list(result.keys())}")
                return unsigned_url
            else:
                logger.warning(f"[房间 {web_rid}] frontierSign unexpected result type: {type(result)}")
                return unsigned_url
        except Exception as e:
            logger.warning(f"[房间 {web_rid}] frontierSign exception: {e}")
            return unsigned_url

    async def _resolve_internal_room_id(self, web_rid: str) -> str:
        """Resolve the internal room_id from a web_rid using Douyin's web enter API.

        The WebSocket danmaku endpoint requires the internal room_id, not the web_rid.
        This method calls the /webcast/room/web/enter/ API to get the mapping.

        Args:
            web_rid: The web_rid from the room URL (e.g., '285574252923').

        Returns:
            The internal room_id string, or the original web_rid if resolution fails.
        """
        try:
            # Get cookies for the API call
            ttwid = ''
            browser_cookies = await self._async_get_browser_cookies()
            if browser_cookies:
                for part in browser_cookies.split(';'):
                    part = part.strip()
                    if part.startswith('ttwid='):
                        ttwid = part.split('=', 1)[1]
                        break
            if not ttwid:
                ttwid = self._get_ttwid_from_http(web_rid)

            cookie_header = f'ttwid={ttwid}' if ttwid else ''
            if browser_cookies:
                cookie_header = browser_cookies

            url = (
                f"https://live.douyin.com/webcast/room/web/enter/?"
                f"aid=6383&app_name=douyin_web&live_id=1&device_platform=web"
                f"&enter_from=web_live&web_rid={web_rid}"
            )

            req = urllib.request.Request(url, headers={
                'User-Agent': REALISTIC_USER_AGENT,
                'Cookie': cookie_header,
                'Referer': 'https://live.douyin.com/',
                'Accept': 'application/json, text/plain, */*',
            })

            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            if data.get('status_code') == 0 or data.get('code') == 0:
                room_data = data.get('data', {})
                # data.data can be a list or a dict
                if isinstance(room_data, list) and room_data:
                    room_data = room_data[0]
                if isinstance(room_data, dict):
                    internal_id = str(room_data.get('id_str', '') or room_data.get('id', ''))
                    room_name = room_data.get('title', '')
                    owner = room_data.get('owner', {}) or {}
                    anchor = owner.get('nickname', '')
                    if internal_id and internal_id != web_rid:
                        logger.info(f"[房间 {web_rid}] Resolved internal room_id: {internal_id}"
                                    f" (title={room_name[:20]}, anchor={anchor})")
                        return internal_id
                    elif internal_id:
                        logger.info(f"[房间 {web_rid}] internal_id same as web_rid: {internal_id}")
                        return internal_id
                    else:
                        # Check nested structure
                        room_info = room_data.get('room', {}) or room_data.get('room_info', {})
                        internal_id = str(room_info.get('id_str', '') or room_info.get('id', ''))
                        if internal_id:
                            logger.info(f"[房间 {web_rid}] Resolved nested internal room_id: {internal_id}")
                            return internal_id

            logger.warning(f"[房间 {web_rid}] web enter API returned no room_id: status={data.get('status_code', '?')}")
            return web_rid

        except Exception as e:
            logger.warning(f"[房间 {web_rid}] Failed to resolve internal room_id: {e}")
            return web_rid

    async def start_danmaku_bridge(
        self,
        room_id: str,
        callback: Callable[[dict, str, str], None],
        duration: int = 300,
        signing_page=None,
    ):
        """
        通过浏览器 JS 内的 WebSocket 连接接收弹幕，再桥接到 Python 回调。

        在签名页（目录页）的 JavaScript 上下文中打开 WebSocket 连接，
        利用浏览器自带的 Cookie / frontierSign 签名绕过所有反爬检测。
        收到的二进制帧经 base64 编码传回 Python，由 protobuf 解码器解析。

        Args:
            room_id:      直播间 web_rid 或内部 room_id。
            callback:     消息回调 callback(message_dict, room_id, platform)。
            duration:     持续时间（秒）。
            signing_page: 已加载抖音目录页的 Playwright Page。
        """
        if not signing_page:
            raise RuntimeError("start_danmaku_bridge requires a signing_page")
        if not self._decode_websocket_frame or not self._build_ack_frame:
            raise RuntimeError("Protobuf decoder not available")

        import base64 as _b64

        logger.info(f"[房间 {room_id}] 启动浏览器桥接弹幕流...")

        # ── Phase 1: 在浏览器 JS 中打开 WebSocket ──
        ts = str(int(time.time() * 1000))
        cursor = f"t-{ts}_r-1_d-1_u-1_h-1"
        internal_ext = (
            f"internal_src:dim|wss_push_room_id:{room_id}"
            f"|wss_push_did:0|dim_log_id:{ts}"
            f"|fetch_time:{ts}|seq:1|wss_info:0-{ts}-0-0"
        )

        setup_result = await signing_page.evaluate("""async ([roomId, cursor, iext]) => {
            try {
                if (!window.__dmBridges) window.__dmBridges = {};

                const params = new URLSearchParams({
                    app_name: 'douyin_web',
                    version_code: '180800',
                    webcast_sdk_version: '1.0.14-beta.0',
                    update_version_code: '1.0.14-beta.0',
                    compress: 'gzip',
                    device_platform: 'web',
                    cookie_enabled: 'true',
                    screen_width: '1920',
                    screen_height: '1080',
                    browser_language: 'zh-CN',
                    browser_platform: 'Win32',
                    browser_name: 'Mozilla',
                    browser_version: '5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    browser_online: 'true',
                    tz_name: 'Asia/Shanghai',
                    cursor: cursor,
                    internal_ext: iext,
                    host: 'https://live.douyin.com',
                    aid: '6383',
                    live_id: '1',
                    did_rule: '3',
                    endpoint: 'live_pc',
                    support_wrds: '1',
                    user_unique_id: '',
                    im_path: '/webcast/im/fetch/',
                    identity: 'audience',
                    room_id: roomId,
                    heartbeatDuration: '0',
                });

                const unsignedUrl = 'wss://webcast5-ws-web-lf.douyin.com/webcast/im/push/v2/?' + params.toString();

                // Sign with frontierSign (AWAIT the Promise!)
                let signedUrl = unsignedUrl;
                let sigMethod = 'none';
                try {
                    if (typeof window.byted_acrawler !== 'undefined' &&
                        typeof window.byted_acrawler.frontierSign === 'function') {
                        sigMethod = 'frontierSign';
                        const signResult = await window.byted_acrawler.frontierSign(unsignedUrl);
                        if (typeof signResult === 'string') {
                            signedUrl = signResult;
                            sigMethod = 'frontierSign:string';
                        } else if (signResult && signResult['X-Bogus']) {
                            signedUrl = unsignedUrl + '&X-Bogus=' + signResult['X-Bogus'];
                            sigMethod = 'frontierSign:X-Bogus';
                        } else if (signResult && signResult.signature) {
                            signedUrl = unsignedUrl + '&signature=' + signResult.signature;
                            sigMethod = 'frontierSign:signature';
                        } else {
                            sigMethod = 'frontierSign:unknown_result(' + JSON.stringify(signResult).substring(0,80) + ')';
                        }
                    }
                } catch(e) { sigMethod = 'frontierSign_error:' + e.message; }

                // Create bridge state
                const bridge = {
                    messages: [],
                    connected: false,
                    error: null,
                    closed: false,
                    count: 0,
                    ws: null,
                    sigMethod: sigMethod,
                };

                bridge.ws = new WebSocket(signedUrl);
                bridge.ws.binaryType = 'arraybuffer';

                bridge.ws.onopen = () => { bridge.connected = true; };
                bridge.ws.onmessage = (e) => {
                    if (e.data instanceof ArrayBuffer) {
                        const bytes = new Uint8Array(e.data);
                        let binary = '';
                        for (let i = 0; i < bytes.byteLength; i++) {
                            binary += String.fromCharCode(bytes[i]);
                        }
                        bridge.messages.push(btoa(binary));
                        bridge.count++;
                    }
                };
                bridge.ws.onerror = (e) => {
                    bridge.error = 'ws_error';
                    bridge.readyState = bridge.ws.readyState;
                    bridge.urlLen = signedUrl.length;
                    try { bridge.errorDetail = JSON.stringify({type: e.type, rs: bridge.ws.readyState}); } catch(ex) {}
                };
                bridge.ws.onclose = (e) => {
                    bridge.closed = true;
                    bridge.closeCode = e.code;
                    bridge.closeReason = e.reason || '';
                    bridge.wasClean = e.wasClean;
                };

                // Heartbeat every 10s
                bridge.hbInterval = setInterval(() => {
                    if (bridge.ws.readyState === WebSocket.OPEN) {
                        const hb = new Uint8Array([50, 2, 104, 98, 66, 1, 0]);
                        bridge.ws.send(hb.buffer);
                    }
                }, 10000);

                window.__dmBridges[roomId] = bridge;
                return { ok: true, sigMethod: sigMethod, url: signedUrl.substring(0, 120) };
            } catch(e) {
                return { ok: false, error: e.message };
            }
        }""", [room_id, cursor, internal_ext])

        if not setup_result.get('ok'):
            raise RuntimeError(f"JS WebSocket setup failed: {setup_result.get('error')}")

        _sig = setup_result.get('sigMethod', '?')
        logger.info(f"[房间 {room_id}] JS WebSocket opening... sig={_sig}")

        # ── Phase 2: Poll messages from JS and decode in Python ──
        message_count = 0
        start_time = time.time()
        _empty_polls = 0
        _last_ack_iext = ''

        try:
            while time.time() - start_time < duration:
                await asyncio.sleep(2)

                # Check if signing page is still alive
                try:
                    _alive = await signing_page.evaluate(f"""() => {{
                        const b = window.__dmBridges && window.__dmBridges['{room_id}'];
                        if (!b) return {{ alive: false, reason: 'no_bridge' }};
                        return {{
                            alive: true,
                            connected: b.connected,
                            closed: b.closed,
                            error: b.error,
                            count: b.count,
                            pending: b.messages.length,
                            closeCode: b.closeCode || 0,
                            readyState: b.ws ? b.ws.readyState : -1,
                        }};
                    }}""")
                except Exception as poll_err:
                    logger.warning(f"[房间 {room_id}] Bridge poll error: {poll_err}")
                    break

                if not _alive.get('alive'):
                    logger.warning(f"[房间 {room_id}] Bridge not alive: {_alive}")
                    break

                if _alive.get('error'):
                    # Wait briefly for onclose to fire after onerror
                    await asyncio.sleep(1)
                    _close_info = await signing_page.evaluate(f"""() => {{
                        const b = window.__dmBridges && window.__dmBridges['{room_id}'];
                        if (!b) return {{}};
                        return {{
                            closed: b.closed,
                            closeCode: b.closeCode || 0,
                            closeReason: b.closeReason || '',
                            readyState: b.ws ? b.ws.readyState : -1,
                            urlLen: b.urlLen || 0,
                        }};
                    }}""")
                    _cc = _close_info.get('closeCode', 0)
                    _rs = _close_info.get('readyState', -1)
                    logger.warning(f"[房间 {room_id}] Bridge WS error: {_alive['error']}, "
                                   f"closeCode={_cc}, readyState={_rs}, urlLen={_close_info.get('urlLen', 0)}")
                    break

                if _alive.get('closed'):
                    _code = _alive.get('closeCode', 0)
                    logger.info(f"[房间 {room_id}] JS WebSocket closed (code={_code})")
                    break

                _pending = _alive.get('pending', 0)
                if _pending == 0:
                    _empty_polls += 1
                    # If no messages for 60s and never connected, the room might be ended
                    if _empty_polls > 30 and not _alive.get('connected'):
                        logger.info(f"[房间 {room_id}] Never connected after {_empty_polls*2}s, giving up")
                        break
                    continue

                _empty_polls = 0

                # Fetch and process messages
                raw_messages = await signing_page.evaluate(f"""() => {{
                    const b = window.__dmBridges && window.__dmBridges['{room_id}'];
                    if (!b || b.messages.length === 0) return [];
                    const batch = b.messages.splice(0, 50);
                    return batch;
                }}""")

                for b64_data in (raw_messages or []):
                    try:
                        raw_bytes = _b64.b64decode(b64_data)
                        _, messages, _, need_ack, iext = self._decode_websocket_frame(raw_bytes)
                        for msg in messages:
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(msg, room_id, "douyin")
                                else:
                                    callback(msg, room_id, "douyin")
                                message_count += 1
                            except Exception as cb_err:
                                logger.debug(f"[房间 {room_id}] Callback error: {cb_err}")

                        # Send ACK via JS WebSocket if needed
                        if need_ack and iext and iext != _last_ack_iext:
                            _last_ack_iext = iext
                            try:
                                await signing_page.evaluate(f"""(ackStr) => {{
                                    const b = window.__dmBridges && window.__dmBridges['{room_id}'];
                                    if (b && b.ws && b.ws.readyState === 1) {{
                                        // Decode base64 ack frame to ArrayBuffer
                                        const raw = atob(ackStr);
                                        const bytes = new Uint8Array(raw.length);
                                        for (let i = 0; i < raw.length; i++) bytes[i] = raw.charCodeAt(i);
                                        b.ws.send(bytes.buffer);
                                    }}
                                }}""", _b64.b64encode(self._build_ack_frame(iext)).decode('ascii'))
                            except Exception as ack_err:
                                logger.debug(f"[房间 {room_id}] ACK send error: {ack_err}")
                    except Exception as dec_err:
                        logger.debug(f"[房间 {room_id}] Frame decode error: {dec_err}")

        except Exception as e:
            logger.error(f"[房间 {room_id}] Bridge error: {e}")
        finally:
            # ── Phase 3: Cleanup ──
            try:
                await signing_page.evaluate(f"""() => {{
                    const b = window.__dmBridges && window.__dmBridges['{room_id}'];
                    if (b) {{
                        clearInterval(b.hbInterval);
                        if (b.ws) try {{ b.ws.close(); }} catch(e) {{}}
                        delete window.__dmBridges['{room_id}'];
                    }}
                }}""")
            except Exception:
                pass
            elapsed = time.time() - start_time
            logger.info(
                f"[房间 {room_id}] 桥接弹幕流结束：{elapsed:.1f} 秒，共 {message_count} 条消息"
            )
            if message_count == 0:
                raise RuntimeError(f"Bridge received 0 messages in {elapsed:.1f}s (ws_error)")

    async def start_danmaku_http_poll(
        self,
        room_id: str,
        callback: Callable[[dict, str, str], None],
        duration: int = 300,
        signing_page=None,
    ):
        """
        通过浏览器 JS fetch 轮询 HTTP 弹幕接口接收弹幕。

        作为 WebSocket 方案的备用方案。使用 frontierSign 签名 HTTP URL，
        通过 /webcast/im/fetch/ 端点获取 protobuf 弹幕数据，在 Python 端解码。

        Args:
            room_id:      直播间 ID。
            callback:     消息回调 callback(message_dict, room_id, platform)。
            duration:     持续时间（秒）。
            signing_page: 已加载抖音目录页的 Playwright Page。
        """
        if not signing_page:
            raise RuntimeError("start_danmaku_http_poll requires a signing_page")
        if not self._decode_websocket_frame or not self._build_ack_frame:
            raise RuntimeError("Protobuf decoder not available")

        import base64 as _b64

        logger.info(f"[房间 {room_id}] 启动 HTTP fetch 轮询弹幕流...")

        message_count = 0
        start_time = time.time()
        cursor = f"t-{int(time.time()*1000)}_r-1_d-1_u-1_h-1"
        internal_ext = (
            f"internal_src:dim|wss_push_room_id:{room_id}"
            f"|wss_push_did:0|dim_log_id:{int(time.time()*1000)}"
            f"|fetch_time:{int(time.time()*1000)}|seq:1|wss_info:0-{int(time.time()*1000)}-0-0"
        )
        _empty_count = 0
        _poll_interval = 3  # seconds between polls

        try:
            while time.time() - start_time < duration:
                # Lightweight page health check (no navigation - just detect dead context)
                try:
                    await signing_page.evaluate("1")
                except Exception as page_err:
                    err_msg = str(page_err)
                    if 'destroyed' in err_msg or 'closed' in err_msg:
                        logger.warning(f"[房间 {room_id}] Signing page context dead, stopping HTTP poll")
                        break
                    # Other errors (e.g., timeout) - just continue and try the fetch

                # Make signed HTTP fetch from browser JS (with timeout to prevent hanging)
                try:
                    fetch_result = await asyncio.wait_for(signing_page.evaluate("""async ([roomId, cursor, iext]) => {
                    try {
                        const params = new URLSearchParams({
                            app_name: 'douyin_web',
                            version_code: '180800',
                            webcast_sdk_version: '1.0.14-beta.0',
                            update_version_code: '1.0.14-beta.0',
                            compress: 'gzip',
                            device_platform: 'web',
                            cookie_enabled: 'true',
                            screen_width: '1920',
                            screen_height: '1080',
                            browser_language: 'zh-CN',
                            browser_platform: 'Win32',
                            browser_name: 'Mozilla',
                            browser_version: '5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                            browser_online: 'true',
                            tz_name: 'Asia/Shanghai',
                            cursor: cursor,
                            internal_ext: iext,
                            host: 'https://live.douyin.com',
                            aid: '6383',
                            live_id: '1',
                            did_rule: '3',
                            endpoint: 'live_pc',
                            support_wrds: '1',
                            user_unique_id: '',
                            im_path: '/webcast/im/fetch/',
                            identity: 'audience',
                            room_id: roomId,
                            heartbeatDuration: '0',
                        });

                        let url = 'https://live.douyin.com/webcast/im/fetch/?' + params.toString();

                        // Sign with frontierSign
                        let sigMethod = 'none';
                        try {
                            if (typeof window.byted_acrawler !== 'undefined' &&
                                typeof window.byted_acrawler.frontierSign === 'function') {
                                const signResult = await window.byted_acrawler.frontierSign(url);
                                if (typeof signResult === 'string') {
                                    url = signResult;
                                    sigMethod = 'string';
                                } else if (signResult && signResult['X-Bogus']) {
                                    url = url + '&X-Bogus=' + signResult['X-Bogus'];
                                    sigMethod = 'X-Bogus';
                                }
                            }
                        } catch(e) { sigMethod = 'error:' + e.message; }

                        const controller = new AbortController();
                        const fetchTimeout = setTimeout(() => controller.abort(), 10000);
                        let resp;
                        try {
                            resp = await fetch(url, { credentials: 'include', signal: controller.signal });
                        } finally {
                            clearTimeout(fetchTimeout);
                        }
                        const status = resp.status;
                        const contentType = resp.headers.get('content-type') || '';
                        const contentLen = resp.headers.get('content-length') || '';

                        if (!resp.ok) {
                            return { error: 'HTTP ' + status, sigMethod, contentType };
                        }

                        // Read response as ArrayBuffer -> base64
                        const buf = await resp.arrayBuffer();
                        const bytes = new Uint8Array(buf);
                        let binary = '';
                        for (let i = 0; i < bytes.byteLength; i++) {
                            binary += String.fromCharCode(bytes[i]);
                        }
                        return {
                            ok: true,
                            data: btoa(binary),
                            size: buf.byteLength,
                            sigMethod,
                            contentType,
                            contentLen,
                            firstBytes: Array.from(bytes.slice(0, 8)),
                        };
                    } catch(e) {
                        return { error: e.message };
                    }
                }""", [room_id, cursor, internal_ext]), timeout=15)
                except asyncio.TimeoutError:
                    logger.warning(f"[房间 {room_id}] HTTP fetch evaluate timed out after 15s")
                    fetch_result = {'error': 'evaluate_timeout'}
                except Exception as eval_err:
                    logger.warning(f"[房间 {room_id}] HTTP fetch evaluate error: {eval_err}")
                    fetch_result = {'error': str(eval_err)[:80]}

                if not fetch_result:
                    logger.warning(f"[房间 {room_id}] HTTP fetch returned null")
                    await asyncio.sleep(_poll_interval)
                    continue

                if fetch_result.get('error'):
                    _err = fetch_result['error']
                    logger.warning(f"[房间 {room_id}] HTTP fetch error: {_err} (sig={fetch_result.get('sigMethod', '?')})")
                    _empty_count += 1
                    if _empty_count > 10:
                        logger.info(f"[房间 {room_id}] Too many fetch errors, stopping")
                        break
                    await asyncio.sleep(_poll_interval)
                    continue

                if not fetch_result.get('ok'):
                    await asyncio.sleep(_poll_interval)
                    continue

                _size = fetch_result.get('size', 0)
                _sig = fetch_result.get('sigMethod', '?')
                _ct = fetch_result.get('contentType', '')

                if _size == 0:
                    _empty_count += 1
                    if _empty_count > 20:
                        logger.info(f"[房间 {room_id}] 20 consecutive empty responses, stopping")
                        break
                    await asyncio.sleep(_poll_interval)
                    continue

                _empty_count = 0

                # Decode protobuf response
                try:
                    raw_bytes = _b64.b64decode(fetch_result['data'])

                    # Try decoding as WebSocket frame format (same protobuf structure)
                    _, messages, new_cursor, need_ack, new_iext = self._decode_websocket_frame(raw_bytes)

                    if new_cursor:
                        cursor = new_cursor
                    if new_iext:
                        internal_ext = new_iext

                    for msg in messages:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(msg, room_id, "douyin")
                            else:
                                callback(msg, room_id, "douyin")
                            message_count += 1
                        except Exception as cb_err:
                            logger.debug(f"[房间 {room_id}] Callback error: {cb_err}")

                    if message_count > 0 and message_count % 20 == 0:
                        logger.info(f"[房间 {room_id}] HTTP poll: {message_count} messages so far")

                except Exception as dec_err:
                    logger.debug(f"[房间 {room_id}] Protobuf decode error: {dec_err} (size={_size}, sig={_sig}, ct={_ct})")

                await asyncio.sleep(_poll_interval)

        except Exception as e:
            logger.error(f"[房间 {room_id}] HTTP poll error: {e}")
        finally:
            elapsed = time.time() - start_time
            logger.info(
                f"[房间 {room_id}] HTTP 轮询弹幕流结束：{elapsed:.1f} 秒，"
                f"共 {message_count} 条消息"
            )
            if message_count == 0:
                raise RuntimeError(f"HTTP poll received 0 messages in {elapsed:.1f}s")

    async def start_danmaku_direct(
        self,
        room_id: str,
        callback: Callable[[dict, str, str], None],
        duration: int = 300,
        signing_page=None,
    ):
        """
        直连抖音 WebSocket 弹幕流（不依赖浏览器页面加载）。

        通过 Python websockets 库直接连接抖音弹幕 WebSocket 端点，
        绕过 Playwright 页面导航的 ERR_ABORTED 问题。

        Args:
            room_id:      直播间 ID。
            callback:     消息回调 callback(message_dict, room_id, platform)。
            duration:     抓取持续时间（秒），默认 300。
            signing_page: Playwright page loaded on douyin.com for frontierSign URL signing.
        """
        if not _HAS_WEBSOCKETS:
            logger.error("websockets 库未安装，无法使用直连弹幕模式")
            return
        if not self._decode_websocket_frame or not self._build_ack_frame:
            logger.error("弹幕解码器不可用，无法启动直连弹幕流")
            return

        logger.info(f"[房间 {room_id}] 启动直连 WebSocket 弹幕流...")

        # 1. 获取 ttwid（先从浏览器上下文，再从 HTTP）
        browser_cookies = await self._async_get_browser_cookies()
        ttwid = ''
        if browser_cookies:
            # 从浏览器 cookie 字符串中提取 ttwid
            for part in browser_cookies.split(';'):
                part = part.strip()
                if part.startswith('ttwid='):
                    ttwid = part.split('=', 1)[1]
                    break
        if not ttwid:
            ttwid = self._get_ttwid_from_http(room_id)

        if not ttwid:
            logger.warning(f"[房间 {room_id}] 无法获取 ttwid Cookie，直连可能失败")

        # 1.5 Resolve internal room_id from web_rid
        # WebSocket endpoint needs the internal room_id, not the URL web_rid
        internal_room_id = await self._resolve_internal_room_id(room_id)
        if internal_room_id != room_id:
            logger.info(f"[房间 {room_id}] Using internal room_id: {internal_room_id}")

        # 2. 构建 WebSocket URL (with frontierSign signing via Playwright)
        ws_url = ''
        if signing_page:
            ws_url = await self._sign_ws_url_with_playwright(internal_room_id, signing_page)
            if ws_url:
                logger.info(f"[房间 {internal_room_id}] Got signed WebSocket URL")
            else:
                logger.warning(f"[房间 {internal_room_id}] Signing returned empty, building unsigned URL")

        if not ws_url:
            # Fallback: unsigned URL (no signature — will likely be rejected by Douyin)
            ts = str(int(time.time() * 1000))
            cursor = f"t-{ts}_r-1_d-1_u-1_h-1"
            internal_ext = (
                f"internal_src:dim|wss_push_room_id:{internal_room_id}"
                f"|wss_push_did:0|dim_log_id:{ts}"
                f"|fetch_time:{ts}|seq:1|wss_info:0-{ts}-0-0"
            )
            params = urllib.parse.urlencode({
                'app_name': 'douyin_web',
                'version_code': '180800',
                'webcast_sdk_version': '1.0.14-beta.0',
                'update_version_code': '1.0.14-beta.0',
                'compress': 'gzip',
                'device_platform': 'web',
                'cookie_enabled': 'true',
                'screen_width': '1920',
                'screen_height': '1080',
                'browser_language': 'zh-CN',
                'browser_platform': 'Win32',
                'browser_name': 'Mozilla',
                'browser_version': '5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'browser_online': 'true',
                'tz_name': 'Asia/Shanghai',
                'cursor': cursor,
                'internal_ext': internal_ext,
                'host': 'https://live.douyin.com',
                'aid': '6383',
                'live_id': '1',
                'did_rule': '3',
                'endpoint': 'live_pc',
                'support_wrds': '1',
                'user_unique_id': '',
                'im_path': '/webcast/im/fetch/',
                'identity': 'audience',
                'room_id': internal_room_id,
                'heartbeatDuration': '0',
            })
            ws_url = f"wss://webcast5-ws-web-lf.douyin.com/webcast/im/push/v2/?{params}"
            logger.warning(f"[房间 {internal_room_id}] Using UNSIGNED URL (no frontierSign)")

        cookie_header = f'ttwid={ttwid}' if ttwid else ''
        # 附加其他浏览器 cookie（如 sessionid 等）
        if browser_cookies:
            cookie_header = browser_cookies

        headers = {
            'Cookie': cookie_header,
            'User-Agent': REALISTIC_USER_AGENT,
            'Origin': 'https://live.douyin.com',
            'Referer': 'https://live.douyin.com/',
        }

        message_count = 0
        heartbeat_task = None
        start_time = time.time()

        try:
            async with websockets.connect(
                ws_url,
                additional_headers=headers,
                ping_interval=None,
                close_timeout=5,
                open_timeout=15,
                max_size=2 ** 22,
            ) as ws:
                logger.info(
                    f"[房间 {room_id}] 直连 WebSocket 已建立"
                )

                async def _heartbeat_loop():
                    """定时发送心跳保活。"""
                    try:
                        while True:
                            await asyncio.sleep(10)
                            hb = self._build_heartbeat_frame()
                            await ws.send(hb)
                    except (websockets.ConnectionClosed, Exception):
                        pass

                heartbeat_task = asyncio.create_task(_heartbeat_loop())

                while time.time() - start_time < duration:
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=30
                        )
                    except asyncio.TimeoutError:
                        logger.debug(
                            f"[房间 {room_id}] WS 接收超时，继续等待..."
                        )
                        continue
                    except websockets.ConnectionClosed as ce:
                        logger.info(
                            f"[房间 {room_id}] WS 连接关闭: {ce}"
                        )
                        break

                    if isinstance(raw, bytes) and raw:
                        try:
                            _, messages, _, need_ack, iext = (
                                self._decode_websocket_frame(raw)
                            )
                            for msg in messages:
                                try:
                                    if asyncio.iscoroutinefunction(callback):
                                        await callback(msg, room_id, "douyin")
                                    else:
                                        callback(msg, room_id, "douyin")
                                    message_count += 1
                                except Exception as cb_err:
                                    logger.warning(
                                        f"[直连] 弹幕回调出错: {cb_err}"
                                    )

                            if need_ack and iext:
                                ack = self._build_ack_frame(iext)
                                await ws.send(ack)
                        except Exception as dec_err:
                            logger.debug(
                                f"[直连] 帧解码失败: {dec_err}"
                            )

        except websockets.InvalidStatusCode as ise:
            _extra = ''
            try:
                if hasattr(ise, 'response') and ise.response:
                    _extra = f" headers={dict(ise.response.headers)}"[:200]
            except Exception:
                pass
            logger.error(
                f"[房间 {room_id}→{internal_room_id}] WS rejected (HTTP {ise.status_code}){_extra}"
            )
        except Exception as e:
            err_str = str(e)
            logger.error(
                f"[房间 {room_id}→{internal_room_id}] 直连 WebSocket 异常: {err_str[:120]}"
            )
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
            elapsed = time.time() - start_time
            logger.info(
                f"[房间 {room_id}] 直连弹幕流结束：{elapsed:.1f} 秒，"
                f"共 {message_count} 条消息"
            )

    # ------------------------------------------------------------------
    #  弹幕流抓取（核心功能）
    #  策略：CDP (Chrome DevTools Protocol) 在网络层被动拦截 WebSocket 帧，
    #        让抖音页面自身的 JS 按原生方式建立 WebSocket（完美的 TLS 指纹、
    #        正确的签名、完整 Cookie、心跳等），完全绕开注入 WS 被
    #        DEVICE_BLOCKED 的问题。
    # ------------------------------------------------------------------
    async def start_danmaku_stream(
        self,
        room_id: str,
        callback: Callable[[dict, str, str], None],
        duration: int = 300,
        shared_context=None,
    ):
        """
        启动弹幕流抓取。

        采用 CDP 网络层被动拦截：页面自己的 JS 建立 WebSocket，我们在
        Chrome 网络栈层捕获所有帧，对页面完全透明，不会触发反爬。

        Args:
            room_id:  直播间 ID。
            callback: 消息回调函数 callback(message_dict, room_id, platform)。
            duration: 抓取持续时间（秒），默认 300 秒（5 分钟）。
        """
        if not self._decode_websocket_frame:
            logger.error(
                "弹幕解码器不可用！请确保 data_pipeline.proto.douyin_decoder "
                "模块已正确安装。"
            )
            return

        if not self._context:
            logger.error("浏览器上下文不可用，无法抓取弹幕")
            return

        # 使用非无头模式 + 持久化上下文（登录Cookie）
        # 经诊断验证：只有非无头模式 + 持久化上下文的组合才能让
        # 抖音页面原生 JS 建立弹幕 WebSocket 连接。
        monitor_page = None
        cdp_session = None
        danmaku_browser = None
        danmaku_context = None
        _owns_context = False
        try:
            if shared_context is not None:
                # 使用外部传入的共享上下文（多房间共享一个浏览器）
                danmaku_context = shared_context
                monitor_page = await danmaku_context.new_page()
                logger.info(f"[房间 {room_id}] 使用共享浏览器上下文")
            else:
                import os as _os
                _profile_dir = _os.path.join(
                    _os.path.expanduser('~'), '.qoderworkcn', 'douyin_browser_profile'
                )
                _os.makedirs(_profile_dir, exist_ok=True)
                danmaku_context = await self._playwright.chromium.launch_persistent_context(
                    _profile_dir,
                    headless=False,
                    channel='chrome',
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disk-cache-size=1',
                        '--disable-background-networking',
                    ],
                    viewport={'width': 1920, 'height': 1080},
                    locale='zh-CN',
                    timezone_id='Asia/Shanghai',
                )
                _owns_context = True
                monitor_page = await danmaku_context.new_page()
                logger.info(f"[房间 {room_id}] 已启动非无头持久化上下文")
        except Exception as e:
            logger.error(f"[房间 {room_id}] 启动独立浏览器失败: {e}")
            # 回退到共享上下文
            try:
                monitor_page = await self._context.new_page()
            except Exception as e2:
                logger.error(f"[房间 {room_id}] 回退也失败: {e2}")
                return

        # ── 状态统计 ──
        message_count = [0]
        frame_count = [0]
        ws_ids = set()                 # 被监控的 webSocketId 集合
        start_time = time.time()

        # ══════════════════════════════════════════════════════════════
        #  CDP 网络层被动监听（对页面 JS 完全透明）
        # ══════════════════════════════════════════════════════════════
        def _make_cdp_handler(room_id):
            """构造 CDP 事件处理器（闭包捕获统计变量）"""

            def on_ws_created(params):
                # CDP webSocketCreated: URL is nested in params.request.url
                req = params.get("request", {}) or {}
                url = req.get("url", "") or params.get("url", "")
                ws_id = params.get("requestId", "")
                # Accept webcast/im/push URLs, or any WS on live.douyin.com pages
                if url and ("webcast" not in url and "im/push" not in url
                            and "douyin" not in url):
                    return
                ws_ids.add(ws_id)
                logger.info(
                    f"[房间 {room_id}] [CDP] 检测到原生 WebSocket: "
                    f"id={ws_id} url={url[:90]}..."
                )

            def on_ws_frame(params):
                ws_id = params.get("requestId", "")
                if ws_id not in ws_ids:
                    return
                resp = params.get("response", {}) or {}
                payload_data = resp.get("payloadData", "")
                if not payload_data:
                    return
                frame_count[0] += 1
                try:
                    raw = base64.b64decode(payload_data)
                    _, parsed_msgs, _, need_ack, iext = (
                        self._decode_websocket_frame(raw)
                    )
                    if message_count[0] == 0 and parsed_msgs:
                        logger.info(
                            f"[房间 {room_id}] 首帧解码成功！"
                            f"含 {len(parsed_msgs)} 条消息"
                        )
                    for msg in parsed_msgs:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                asyncio.ensure_future(
                                    callback(msg, room_id, "douyin")
                                )
                            else:
                                callback(msg, room_id, "douyin")
                            message_count[0] += 1
                        except Exception as cb_err:
                            logger.warning(f"弹幕回调出错: {cb_err}")
                    if (
                        message_count[0] > 0
                        and message_count[0] % 100 == 0
                    ):
                        logger.info(
                            f"[房间 {room_id}] 已接收 {message_count[0]} 条弹幕"
                        )
                    # ACK 帧：通过 CDP Network.sendWebSocketFrame 发送
                    # (若需要 ACK，由 JS 层心跳已覆盖；此处保留日志)
                    if need_ack and iext and frame_count[0] % 20 == 0:
                        logger.debug(
                            f"[房间 {room_id}] 帧 #{frame_count[0]} 需要 ACK"
                        )
                except Exception as dec_err:
                    logger.debug(
                        f"[房间 {room_id}] CDP 帧解码失败: {dec_err}"
                    )

            def on_ws_closed(params):
                ws_id = params.get("requestId", "")
                if ws_id in ws_ids:
                    logger.info(
                        f"[房间 {room_id}] [CDP] WebSocket 关闭: {ws_id}"
                    )
                    ws_ids.discard(ws_id)

            return on_ws_created, on_ws_frame, on_ws_closed

        try:
            cdp_session = await (danmaku_context or self._context).new_cdp_session(monitor_page)
            await cdp_session.send("Network.enable")
            on_created, on_frame, on_closed = _make_cdp_handler(room_id)
            cdp_session.on("Network.webSocketCreated", on_created)
            cdp_session.on("Network.webSocketFrameReceived", on_frame)
            cdp_session.on("Network.webSocketFrameError", on_frame)  # 复用签名
            cdp_session.on("Network.webSocketClosed", on_closed)
            logger.info(f"[房间 {room_id}] CDP 网络层监听已启用")
        except Exception as cdp_err:
            logger.warning(
                f"[房间 {room_id}] CDP 会话创建失败，回退到 Playwright WS 监听: "
                f"{cdp_err}"
            )
            cdp_session = None
            # 回退：Playwright WebSocket 监听（兜底）
            async def _pw_ws_handler(websocket):
                ws_url = websocket.url
                if "webcast" not in ws_url:
                    return
                logger.info(
                    f"[房间 {room_id}] [PW-WS] 捕获 WebSocket: {ws_url[:80]}..."
                )
                ws_ids.add(id(websocket))

                async def _on_frame(payload):
                    if isinstance(payload, str) or not payload:
                        return
                    frame_count[0] += 1
                    try:
                        _, parsed_msgs, _, _, _ = (
                            self._decode_websocket_frame(payload)
                        )
                        for msg in parsed_msgs:
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(msg, room_id, "douyin")
                                else:
                                    callback(msg, room_id, "douyin")
                                message_count[0] += 1
                            except Exception:
                                pass
                    except Exception:
                        pass

                websocket.on(
                    "framereceived",
                    lambda ws, p: asyncio.ensure_future(_on_frame(p)),
                )

            monitor_page.on("websocket", _pw_ws_handler)

        # 注入反检测脚本（在导航前）
        if self._stealth_available:
            try:
                await monitor_page.add_init_script(STEALTH_JS)
            except Exception:
                pass

        # ── 移除所有资源阻断规则 ──
        # 弹幕SDK需要完整加载所有资源（包括图片/CSS）才能正确计算WS签名。
        # 之前尝试轻量阻断导致WS连接1006被关闭。
        _ctx_for_route = danmaku_context or self._context
        if _ctx_for_route:
            try:
                await _ctx_for_route.unroute('**/*')
                logger.info(f"[房间 {room_id}] 已移除所有资源阻断规则（允许完整加载）")
            except Exception:
                pass

        # ── 导航到直播间 ──
        try:
            logger.info(
                f"[房间 {room_id}] 正在导航到直播间（独立页面）..."
            )
            try:
                await monitor_page.goto(
                    f"https://live.douyin.com/{room_id}",
                    wait_until="domcontentloaded",
                    timeout=45000,
                )
                logger.info(
                    f"[房间 {room_id}] 页面导航成功（domcontentloaded）"
                )
                # 捕获页面JS错误和关键console消息，帮助诊断WS连接问题
                _js_errors = []
                monitor_page.on("pageerror", lambda err: _js_errors.append(str(err)[:200]))
                async def _on_console(msg):
                    text = msg.text
                    if any(kw in text.lower() for kw in ['websocket', 'ws error', 'signature', 'failed', 'blocked', 'refused', '403', '1006']):
                        logger.info(f"[房间 {room_id}] [Console] {text[:150]}")
                monitor_page.on("console", _on_console)
            except Exception as nav_err:
                err_msg = str(nav_err)
                if "ERR_ABORTED" in err_msg or "net::" in err_msg:
                    logger.warning(
                        f"[房间 {room_id}] 导航被中断（抖音反爬），继续等待原生 WS..."
                    )
                else:
                    logger.warning(f"[房间 {room_id}] 导航失败: {nav_err}")

            # 给页面较长时间让原生 JS 建立 WebSocket
            # （抖音的反爬检测需要一定时间完成）
            await self._random_delay(8, 15)

            try:
                await monitor_page.wait_for_load_state(
                    "domcontentloaded", timeout=15000
                )
            except Exception:
                pass

            # ── 阶段 A：等待原生 WS（最多 60 秒） ──
            # 非无头模式下抖音页面通常在 10-40 秒内建立 WebSocket
            logger.info(
                f"[房间 {room_id}] 等待页面原生 WebSocket 建立（最多 60 秒）..."
            )
            native_established = False
            for tick in range(12):  # 12 * 5s = 60s
                await asyncio.sleep(5)
                if ws_ids:
                    native_established = True
                    logger.info(
                        f"[房间 {room_id}] 原生 WebSocket 已建立！"
                        f"（{tick * 5}s 时检测到 {len(ws_ids)} 个连接）"
                    )
                    break
                if (tick + 1) % 3 == 0:
                    logger.info(
                        f"[房间 {room_id}] 等待原生 WS... {(tick+1)*5}s"
                    )

            # ── 阶段 B：已禁用 ──
            # 不再注入 JS WebSocket。实测发现：
            # 1) 页面原生 WS 通常在 30-90 秒后建立（VM 较慢）
            # 2) 注入的 WS 与原生 WS 冲突，导致原生 WS 被关闭
            # 3) DEVICE_BLOCKED 使注入的 WS 始终失败
            # CDP 被动截帧会在主循环中自动捕获原生 WS（无论何时建立）
            if not native_established:
                logger.info(
                    f"[房间 {room_id}] 原生 WS 尚未建立，跳过注入，"
                    f"CDP 将在主循环中持续监听..."
                )

            # ── HTTP fetch 兜底变量初始化 ──
            _fetch_fallback_active = False
            _fetch_cursor = ""
            _fetch_seen_msg_ids = set()

            async def _do_one_fetch():
                return {"error": "fetch disabled"}

            # ── 阶段 C：主采集循环 ──
            logger.info(
                f"[房间 {room_id}] 进入弹幕采集主循环，时长 {duration}s"
            )
            last_report = time.time()
            _fetch_tick = 0
            _ws_dead_since = None        # 记录 WS 断开时间
            _ws_reconnect_cooldown = 0   # 重连冷却时间戳
            _ws_ever_connected = len(ws_ids) > 0  # WS 是否曾经连接过
            _inactivity_last_count = message_count[0]  # 不活动检测：上次消息计数
            _inactivity_since = time.time()             # 不活动检测：上次有新消息的时间
            while time.time() - start_time < duration:
                await asyncio.sleep(2)
                _fetch_tick += 1

                # 定期输出统计
                if time.time() - last_report > 30:
                    last_report = time.time()
                    logger.info(
                        f"[房间 {room_id}] 状态: WS={len(ws_ids)} "
                        f"帧={frame_count[0]} 消息={message_count[0]} "
                        f"fetch={_fetch_fallback_active}"
                    )

                # ── 检测 WebSocket 是否仍然活跃 ──
                _ws_alive = len(ws_ids) > 0

                if _ws_alive:
                    _ws_dead_since = None  # 重置断开计时
                    if not _ws_ever_connected:
                        _ws_ever_connected = True
                        logger.info(
                            f"[房间 {room_id}] CDP 捕获到原生 WebSocket！"
                            f"（{len(ws_ids)} 个连接）"
                        )
                    # ── 不活动检测：WS活着但长时间无新消息 → 退出以便切换到更活跃的房间 ──
                    if message_count[0] > _inactivity_last_count:
                        _inactivity_last_count = message_count[0]
                        _inactivity_since = time.time()
                    else:
                        _idle = time.time() - _inactivity_since
                        if _idle > 300 and message_count[0] > 0:
                            logger.info(
                                f"[房间 {room_id}] 无活动 {_idle:.0f}s (消息={message_count[0]}), "
                                f"退出以刷新房间列表"
                            )
                            break
                    # CDP 正在接收帧，原生 WS 工作正常
                    continue

                # ── WebSocket 已断开 ──
                if _ws_dead_since is None:
                    _ws_dead_since = time.time()
                    if _ws_ever_connected:
                        logger.warning(
                            f"[房间 {room_id}] WebSocket 已断开，准备重连..."
                        )
                    else:
                        logger.info(
                            f"[房间 {room_id}] 等待原生 WebSocket 建立中..."
                        )

                _dead_duration = time.time() - _ws_dead_since

                # 策略1：断开 120~180 秒内且曾经连接过 → 尝试刷新页面重连
                if _ws_ever_connected and 120 < _dead_duration < 180 and time.time() > _ws_reconnect_cooldown:
                    try:
                        logger.info(
                            f"[房间 {room_id}] 尝试刷新页面重连 WebSocket..."
                        )
                        ws_ids.clear()
                        try:
                            await monitor_page.reload(
                                wait_until="domcontentloaded", timeout=30000
                            )
                        except Exception:
                            pass
                        # 等待新 WS 建立（最多 20 秒）
                        for _tick in range(4):
                            await asyncio.sleep(5)
                            if ws_ids:
                                logger.info(
                                    f"[房间 {room_id}] 重连成功！"
                                    f"新 WS 连接数: {len(ws_ids)}"
                                )
                                _ws_dead_since = None
                                break
                        else:
                            logger.info(
                                f"[房间 {room_id}] 页面刷新后 WS 未重建，"
                                f"启用 HTTP fetch 兜底"
                            )
                            _fetch_fallback_active = True
                            _ws_reconnect_cooldown = time.time() + 120
                    except Exception as reload_err:
                        logger.warning(
                            f"[房间 {room_id}] 页面重连失败: {reload_err}"
                        )
                        _fetch_fallback_active = True

                # 策略2：断开超过 180 秒且曾连接过 → 日志提醒（HTTP fetch 已禁用）
                if _ws_ever_connected and _dead_duration >= 180:
                    logger.info(
                        f"[房间 {room_id}] WS 断开超 180s"
                    )

                # 策略3：断开超过 300 秒 → 提前退出，让外层 while True 重启整个流
                if _dead_duration >= 300:
                    logger.info(
                        f"[房间 {room_id}] WS 断开超 5 分钟，提前退出采集循环"
                    )
                    break

                # HTTP fetch 轮询（每 4 秒）
                if _fetch_fallback_active and _fetch_tick % 2 == 0:
                    try:
                        result = await _do_one_fetch()
                        if result and result.get("new_msgs", 0) > 0:
                            logger.debug(
                                f"[房间 {room_id}] fetch 获取 "
                                f"{result['new_msgs']} 条"
                            )
                    except Exception:
                        pass

                # 轮询 JS 注入 WS 的消息队列（兜底方案）
                if not native_established and not _fetch_fallback_active:
                    try:
                        messages_b64 = await monitor_page.evaluate("""
                            () => {
                                const msgs = window.__wsMessages || [];
                                window.__wsMessages = [];
                                return msgs;
                            }
                        """)
                        for b64 in messages_b64:
                            try:
                                raw = base64.b64decode(b64)
                                _, parsed_msgs, _, need_ack, iext = (
                                    self._decode_websocket_frame(raw)
                                )
                                for msg in parsed_msgs:
                                    try:
                                        if asyncio.iscoroutinefunction(callback):
                                            await callback(msg, room_id, "douyin")
                                        else:
                                            callback(msg, room_id, "douyin")
                                        message_count[0] += 1
                                    except Exception:
                                        pass
                                # 通过 JS 发送 ACK
                                if need_ack and iext:
                                    ack_bytes = self._build_ack_frame(iext)
                                    ack_b64 = base64.b64encode(ack_bytes).decode()
                                    await monitor_page.evaluate(f"""
                                        (b64) => {{
                                            try {{
                                                const bin = atob(b64);
                                                const bytes = new Uint8Array(bin.length);
                                                for (let i = 0; i < bin.length; i++)
                                                    bytes[i] = bin.charCodeAt(i);
                                                if (window.__ws &&
                                                    window.__ws.readyState === 1)
                                                    window.__ws.send(bytes.buffer);
                                            }} catch(e) {{}}
                                        }}
                                    """, ack_b64)
                            except Exception:
                                pass
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"[房间 {room_id}] 弹幕流异常: {e}")
        finally:
            # 清理 CDP 会话
            if cdp_session:
                try:
                    await cdp_session.detach()
                except Exception:
                    pass
            if monitor_page:
                try:
                    await monitor_page.close()
                except Exception:
                    pass
            # 仅清理自己创建的上下文（共享上下文不能关闭）
            if danmaku_context and _owns_context:
                try:
                    await danmaku_context.close()
                except Exception:
                    pass
            if danmaku_browser:
                try:
                    await danmaku_browser.close()
                except Exception:
                    pass
            elapsed = time.time() - start_time
            logger.info(
                f"[房间 {room_id}] 弹幕流结束：{elapsed:.1f} 秒，"
                f"共 {message_count[0]} 条消息，帧 {frame_count[0]}"
            )

    # ------------------------------------------------------------------
    #  商品列表抓取
    # ------------------------------------------------------------------
    async def fetch_products(self, room_id: str) -> list[dict]:
        """
        获取直播间的商品列表（购物车商品）。

        通过点击购物车图标并拦截商品列表 API 获取数据。

        Args:
            room_id: 直播间 ID。

        Returns:
            商品信息字典列表。
        """
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        products: list[dict] = []
        product_event = asyncio.Event()

        async def _on_response(response):
            """拦截商品列表 API"""
            url = response.url
            # 匹配商品列表接口（多种可能的 URL 模式）
            if any(
                kw in url
                for kw in [
                    "webcast/room/commerce",
                    "webcast/product",
                    "buyin.jinritemai.com",
                    "ec.snssdk.com",
                ]
            ):
                try:
                    body = await response.json()
                    data = body.get("data", body)

                    # 尝试多种数据路径
                    product_list = (
                        data.get("product_list", [])
                        or data.get("products", [])
                        or data.get("list", [])
                    )

                    for item in product_list:
                        product = self._extract_product(item)
                        if product:
                            products.append(product)

                    if products:
                        product_event.set()
                        logger.info(
                            f"[房间 {room_id}] 拦截到 {len(products)} 个商品"
                        )
                except Exception as e:
                    logger.warning(f"解析商品列表 API 失败: {e}")

        self._page.on("response", _on_response)

        try:
            # 确保在房间页面
            current_url = self._page.url
            if room_id not in current_url:
                try:
                    await self._page.goto(
                        f"https://live.douyin.com/{room_id}",
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                except Exception as nav_err:
                    err_msg = str(nav_err)
                    if "ERR_ABORTED" in err_msg or "net::" in err_msg:
                        logger.warning(f"[房间 {room_id}] 商品页面导航被中断，继续尝试...")
                    else:
                        raise
                await self._random_delay(3, 5)

            # 尝试点击购物车图标
            cart_selectors = [
                '[class*="shopping-cart"]',
                '[class*="ShoppingCart"]',
                '[class*="product-cart"]',
                '[class*="cart-icon"]',
                '[data-e2e="shopping_cart"]',
                'div[class*="cart"]',
            ]

            clicked = False
            for selector in cart_selectors:
                try:
                    el = await self._page.query_selector(selector)
                    if el:
                        await self._mouse_jitter(self._page)
                        await el.click()
                        clicked = True
                        logger.info(
                            f"已点击购物车图标（选择器: {selector}）"
                        )
                        break
                except Exception:
                    continue

            if not clicked:
                logger.warning(
                    f"[房间 {room_id}] 未找到购物车图标，"
                    "可能该直播间没有商品"
                )

            # 等待商品列表 API 响应
            if clicked:
                try:
                    await asyncio.wait_for(product_event.wait(), timeout=15)
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[房间 {room_id}] 等待商品列表 API 超时"
                    )

        except Exception as e:
            logger.error(f"[房间 {room_id}] 获取商品列表失败: {e}")
        finally:
            self._page.remove_listener("response", _on_response)

        await self._save_cookies()
        return products

    def _extract_product(self, item: dict) -> Optional[dict]:
        """从商品 API 数据中提取单个商品信息"""
        try:
            return {
                "product_name": item.get("name", item.get("title", "")),
                "price": (
                    float(item.get("price", 0)) / 100
                    if isinstance(item.get("price"), int)
                    and item.get("price", 0) > 100
                    else float(item.get("price", 0))
                ),
                "original_price": (
                    float(item.get("market_price", 0)) / 100
                    if isinstance(item.get("market_price"), int)
                    and item.get("market_price", 0) > 100
                    else float(item.get("market_price", 0))
                ),
                "sales": item.get("sales", item.get("sold_count", "")),
                "image_url": (
                    item.get("image", item.get("cover", ""))
                    .get("url_list", [""])[0]
                    if isinstance(
                        item.get("image", item.get("cover", "")), dict
                    )
                    else item.get("image_url", item.get("cover_url", ""))
                ),
                "product_id": item.get("product_id", item.get("id", "")),
            }
        except Exception as e:
            logger.warning(f"提取商品数据失败: {e}")
            return None

    # ------------------------------------------------------------------
    #  高级运行方法
    # ------------------------------------------------------------------
    async def run_discovery_loop(self, interval: int = 300):
        """
        周期性发现直播间。

        每隔 interval 秒执行一次房间发现，并将结果通过 Kafka 或日志输出。

        Args:
            interval: 发现间隔（秒），默认 300 秒（5 分钟）。
        """
        logger.info(f"启动房间发现循环，间隔 {interval} 秒")

        while True:
            try:
                rooms = await self.discover_live_rooms(limit=20)
                logger.info(f"[发现循环] 本轮发现 {len(rooms)} 个直播间")

                # 如果配置了 Kafka 生产者，将结果推送到 Kafka
                if self.kafka_producer:
                    for room in rooms:
                        try:
                            self.kafka_producer.send(
                                "douyin_rooms",
                                value=room,
                                key=room.get("room_id", ""),
                            )
                        except Exception as kafka_err:
                            logger.warning(f"Kafka 推送失败: {kafka_err}")

                # 输出发现的房间
                for room in rooms:
                    logger.info(
                        f"  [{room.get('room_id')}] "
                        f"{room.get('anchor_name', '未知')} - "
                        f"{room.get('room_name', '未知')} "
                        f"(观众: {room.get('viewer_count_text', 'N/A')})"
                    )

            except Exception as e:
                logger.error(f"[发现循环] 出错: {e}")

            # 等待下一轮（加上随机抖动避免规律性）
            jitter = random.uniform(0, 30)
            await asyncio.sleep(interval + jitter)

    async def run_room_monitor(
        self,
        room_id: str,
        callback: Optional[Callable] = None,
        monitor_products: bool = True,
        shared_context=None,
        signing_page=None,
    ):
        """
        持续监控一个直播间：抓取弹幕流 + 定时获取商品列表。

        Args:
            room_id:          直播间 ID。
            callback:         弹幕消息回调函数。
            monitor_products: 是否同时监控商品列表变化。
            signing_page:     Playwright page for frontierSign URL signing.
        """
        logger.info(f"启动房间监控: {room_id}")

        # 默认回调：打印消息
        if callback is None:

            def callback(msg, rid, platform):
                msg_type = msg.get("type", "unknown")
                if msg_type == "chat":
                    logger.info(
                        f"[弹幕] "
                        f"{msg.get('user', {}).get('nickname', '?')}: "
                        f"{msg.get('content', '')}"
                    )
                elif msg_type == "gift":
                    logger.info(
                        f"[礼物] "
                        f"{msg.get('user', {}).get('nickname', '?')} "
                        f"送出 {msg.get('gift_name', '?')} "
                        f"x{msg.get('count', 1)}"
                    )
                elif msg_type == "like":
                    pass  # 点赞消息太多，默认不打印
                else:
                    logger.info(f"[{msg_type}] {msg}")

        # 房间详情和商品列表需要页面加载，直连模式下跳过
        # （抖音反爬会导致页面导航 ERR_ABORTED）
        logger.info(f"房间 {room_id} 监控启动（直连弹幕模式）")

        # 启动弹幕流（长时间运行）
        # 每次弹幕流断开后自动重连
        _context_dead_count = 0
        while True:
            try:
                # ── 上下文健康检查 ──
                ctx = shared_context or self._context
                if ctx:
                    _hp = None
                    try:
                        _hp = await ctx.new_page()
                    except Exception as health_err:
                        err_msg = str(health_err)
                        if 'closed' in err_msg.lower() or 'target' in err_msg.lower():
                            _context_dead_count += 1
                            logger.error(
                                f"[房间 {room_id}] 浏览器上下文已失效 ({_context_dead_count}次), "
                                f"退出监控等待重启: {err_msg[:80]}"
                            )
                            if _context_dead_count >= 2:
                                # 上下文不可恢复，跳出循环让外层重启
                                break
                            await asyncio.sleep(3)
                            continue
                    finally:
                        if _hp:
                            try: await _hp.close()
                            except: pass
                # ── 主方案：CDP 被动截帧（非无头模式下页面原生 WS）──
                # 诊断确认：抖音仅在非无头模式建立 WebSocket，headless 回退到 HTTP 轮询
                # CDP 在网络层被动捕获页面自己的 WS 连接，对页面完全透明
                try:
                    await self.start_danmaku_stream(
                        room_id=room_id,
                        callback=callback,
                        duration=1200,  # 每次运行 20 分钟，更频繁轮换
                        shared_context=shared_context,
                    )
                    # CDP completed normally (duration expired or room ended)
                    continue  # restart loop to reconnect
                except Exception as cdp_err:
                    logger.info(f"[房间 {room_id}] CDP截帧失败({str(cdp_err)[:50]}), 尝试桥接WS")

                # 备用方案1：浏览器桥接模式（JS内WebSocket）
                if signing_page:
                    try:
                        await self.start_danmaku_bridge(
                            room_id=room_id,
                            callback=callback,
                            duration=1200,
                            signing_page=signing_page,
                        )
                        continue
                    except Exception as bridge_err:
                        logger.info(f"[房间 {room_id}] 桥接WS失败({str(bridge_err)[:50]}), 尝试直连")

                # 备用方案2：直连WebSocket模式
                try:
                    await self.start_danmaku_direct(
                        room_id=room_id,
                        callback=callback,
                        duration=1200,
                        signing_page=signing_page,
                    )
                except Exception as direct_err:
                    logger.info(f"[房间 {room_id}] 所有弹幕方案均失败，等待重试")
                _context_dead_count = 0  # 成功后重置计数
            except Exception as e:
                err_str = str(e)
                if 'closed' in err_str.lower() or 'target' in err_str.lower():
                    _context_dead_count += 1
                    logger.error(f"[房间 {room_id}] 弹幕流上下文异常 ({_context_dead_count}次): {err_str[:80]}")
                    if _context_dead_count >= 3:
                        logger.error(f"[房间 {room_id}] 上下文持续失效，退出监控等待重启")
                        break
                else:
                    logger.error(f"弹幕流异常: {e}")

            # 短暂等待后重新开始
            logger.info(
                f"[房间 {room_id}] 弹幕流结束，5 秒后重新开始..."
            )
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    #  清理与关闭
    # ------------------------------------------------------------------
    async def close(self):
        """
        关闭浏览器并保存 Cookie。
        应在程序退出前调用。
        """
        logger.info("正在关闭浏览器...")
        try:
            await self._save_cookies()
        except Exception:
            pass

        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass

        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass

        await self.browser_pool.close_all()
        logger.info("浏览器已关闭")


# ============================================================================
#  CLI 入口
# ============================================================================
async def _cli_main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(
        description="抖音直播间爬虫 - 星播大数据分析平台",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 发现当前正在直播的房间
  python -m data_pipeline.douyin_crawler --mode discover

  # 监控指定房间的弹幕
  python -m data_pipeline.douyin_crawler --mode monitor --room-id 123456

  # 获取指定房间的详情
  python -m data_pipeline.douyin_crawler --mode detail --room-id 123456

  # 获取指定房间的商品列表
  python -m data_pipeline.douyin_crawler --mode products --room-id 123456
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["discover", "monitor", "detail", "products"],
        required=True,
        help="运行模式: discover(发现房间), monitor(监控弹幕), "
        "detail(房间详情), products(商品列表)",
    )
    parser.add_argument(
        "--room-id",
        type=str,
        default="",
        help="直播间 ID（monitor/detail/products 模式必填）",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="无头模式运行（不推荐，容易被检测）",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="弹幕抓取持续时间（秒），默认 300",
    )

    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    crawler = DouyinLiveCrawler(headless=args.headless)

    try:
        await crawler.init_browser()

        if args.mode == "discover":
            rooms = await crawler.discover_live_rooms(limit=20)
            print(f"\n发现 {len(rooms)} 个直播间:")
            print("-" * 80)
            for i, room in enumerate(rooms, 1):
                print(
                    f"{i:3d}. [{room.get('room_id', '')}] "
                    f"{room.get('anchor_name', '未知主播')} - "
                    f"{room.get('room_name', '未知房间')} "
                    f"(观众: {room.get('viewer_count_text', 'N/A')})"
                )
                print(f"     链接: {room.get('live_url', '')}")

        elif args.mode == "monitor":
            if not args.room_id:
                parser.error("monitor 模式需要指定 --room-id")

            def _print_callback(msg, rid, platform):
                msg_type = msg.get("type", "unknown")
                ts = time.strftime("%H:%M:%S")
                if msg_type == "chat":
                    user = msg.get("user", {}).get("nickname", "匿名")
                    content = msg.get("content", "")
                    print(f"[{ts}] {user}: {content}")
                elif msg_type == "gift":
                    user = msg.get("user", {}).get("nickname", "匿名")
                    gift = msg.get("gift_name", "未知礼物")
                    count = msg.get("count", 1)
                    print(f"[{ts}] [礼物] {user} 送出 {gift} x{count}")
                elif msg_type == "member":
                    user = msg.get("user", {}).get("nickname", "匿名")
                    print(f"[{ts}] {user} 进入直播间")
                elif msg_type == "like":
                    user = msg.get("user", {}).get("nickname", "匿名")
                    count = msg.get("count", 1)
                    print(f"[{ts}] [点赞] {user} x{count}")
                else:
                    print(f"[{ts}] [{msg_type}] {msg}")

            await crawler.run_room_monitor(
                room_id=args.room_id,
                callback=_print_callback,
            )

        elif args.mode == "detail":
            if not args.room_id:
                parser.error("detail 模式需要指定 --room-id")
            detail = await crawler.fetch_room_detail(args.room_id)
            print("\n房间详情:")
            print("-" * 40)
            for k, v in detail.items():
                print(f"  {k}: {v}")

        elif args.mode == "products":
            if not args.room_id:
                parser.error("products 模式需要指定 --room-id")
            products = await crawler.fetch_products(args.room_id)
            print(f"\n商品列表（共 {len(products)} 个）:")
            print("-" * 60)
            for i, p in enumerate(products, 1):
                print(f"{i:3d}. {p.get('product_name', '未知商品')}")
                print(
                    f"     价格: "
                    f"¥{p.get('price', 0):.2f}  "
                    f"原价: ¥{p.get('original_price', 0):.2f}"
                )
                print(f"     销量: {p.get('sales', 'N/A')}")

    finally:
        await crawler.close()


def main():
    """同步入口包装器"""
    asyncio.run(_cli_main())


if __name__ == "__main__":
    main()
