"""
淘宝直播间爬虫模块
基于 Playwright 实现淘宝直播间数据采集，包括：
- 直播间发现（从淘宝直播门户拦截 MTOP API 房间列表）
- 直播间详情抓取（从进入房间 API 拦截详情数据）
- 弹幕流实时抓取（通过长轮询拦截消息 API，淘宝弹幕不走 WebSocket）
- 商品列表抓取（购物车商品接口）
- Cookie 持久化与反检测机制（含 _m_h5_tk / 滑动验证码处理）

注意事项：
  1. 淘宝反爬比抖音更严格，首次运行需要手动登录淘宝账号
  2. MTOP 接口依赖 _m_h5_tk Cookie 进行签名验证
  3. 频繁请求会触发滑动验证码，需控制请求频率
  4. 最大并发房间数为 2（抖音为 3）
  5. 房间导航间隔 60 秒（抖音无此硬性限制）
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
from urllib.parse import parse_qs, urlparse

# ---------------------------------------------------------------------------
# 基础路径配置
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
BROWSER_PROFILES_DIR = BASE_DIR / "browser_profiles" / "taobao"
COOKIES_DIR = BASE_DIR / "cookies"
COOKIES_FILE = COOKIES_DIR / "taobao_cookies.json"

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
// 隐藏 webdriver 标志 - 淘宝对自动化检测非常敏感
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

// 隐藏 Playwright 注入的 $cdc 等属性
delete window.__playwright;
delete window.__pw_manual;
"""


# ---------------------------------------------------------------------------
# MTOP API 相关常量
# ---------------------------------------------------------------------------
# 淘宝直播推荐列表 API
MTOP_ROOM_LIST_PATTERNS = [
    "mtop.taobao.dreamroom.recommend.live.list",
    "mtop.taobao.dreamroom.live.list",
    "mtop.taobao.dreamroom.recommend",
    "mtop.mediaplatform.live.list",
]

# 淘宝直播房间详情 API
MTOP_ROOM_DETAIL_PATTERNS = [
    "mtop.taobao.livex.v2.room.detail",
    "mtop.taobao.livex.room.detail",
    "mtop.mediaplatform.live.detail",
]

# 淘宝直播弹幕 / 消息 API
MTOP_DANMAKU_PATTERNS = [
    "mtop.taobao.livex.message",
    "mtop.taobao.livex.chatroom",
    "mtop.taobao.social.chatroom",
    "mtop.taobao.dreamroom.message",
]

# 淘宝直播商品列表 API
MTOP_PRODUCT_PATTERNS = [
    "mtop.taobao.livex.product",
    "mtop.taobao.dreamroom.product",
    "mtop.mediaplatform.live.product",
    "mtop.taobao.livex.v2.product",
]

# MTOP API 通用域名
MTOP_DOMAINS = [
    "api.m.taobao.com",
    "h5api.m.taobao.com",
    "acs.m.taobao.com",
]


def _parse_count(text: str) -> int:
    """
    将淘宝显示的人数文本转为整数。
    示例:
        "3.2万" -> 32000
        "12万"  -> 120000
        "5234"  -> 5234
        ""      -> 0
    """
    if not text:
        return 0
    text = str(text).strip()
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


def _is_mtop_api(url: str, patterns: list[str]) -> bool:
    """
    判断 URL 是否匹配指定的 MTOP API 模式。
    淘宝 MTOP 请求的 URL 通常包含 api 名称作为路径或查询参数。
    """
    url_lower = url.lower()
    for pattern in patterns:
        if pattern.lower() in url_lower:
            return True
    return False


def _is_mtop_domain(url: str) -> bool:
    """判断 URL 是否来自 MTOP API 域名"""
    try:
        parsed = urlparse(url)
        return any(domain in parsed.hostname for domain in MTOP_DOMAINS if parsed.hostname)
    except Exception:
        return False


# ============================================================================
#  TaobaoLiveCrawler - 淘宝直播间爬虫主类
# ============================================================================
class TaobaoLiveCrawler:
    """
    淘宝直播间爬虫，基于 Playwright 异步实现。
    支持：房间发现、房间详情、弹幕流抓取（长轮询）、商品列表获取。

    淘宝反爬特点：
    - MTOP API 需要 _m_h5_tk Cookie 进行请求签名
    - 频繁请求触发滑动验证码（punish / baxia 风控）
    - 首次访问必须手动登录淘宝账号
    - 弹幕采用长轮询而非 WebSocket
    """

    def __init__(self, kafka_producer=None, headless: bool = False):
        """
        初始化爬虫实例。

        Args:
            kafka_producer: Kafka 生产者实例（可选），用于消息推送。
            headless:       是否无头模式（默认 False，淘宝首次必须手动登录）。
        """
        self.kafka_producer = kafka_producer
        self.headless = headless
        # 淘宝反爬更严格，最大并发房间数为 2
        self.max_concurrent_rooms = 2
        # 房间导航之间的冷却时间（秒），淘宝更严格
        self._navigation_cooldown = 60
        # 上次导航时间戳
        self._last_navigation_time = 0.0

        # 并发控制信号量
        self._room_semaphore = asyncio.Semaphore(self.max_concurrent_rooms)

        # Playwright 实例引用
        self._playwright = None
        self._context = None
        self._page = None

        # 会话有效性标记
        self._session_valid = False

        # 滑动验证码检测标记
        self._captcha_detected = False

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
        """从文件加载已保存的 Cookie（包含 _m_h5_tk 等关键 Cookie）"""
        if COOKIES_FILE.exists():
            try:
                with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                    cookies = json.load(f)
                logger.info(f"已加载 {len(cookies)} 条淘宝 Cookie")
                # 检查是否包含关键的 _m_h5_tk Cookie
                h5_tk_cookies = [c for c in cookies if c.get("name") in ("_m_h5_tk", "_m_h5_tk_enc")]
                if h5_tk_cookies:
                    logger.info(f"发现 _m_h5_tk 相关 Cookie: {len(h5_tk_cookies)} 条")
                else:
                    logger.warning("未找到 _m_h5_tk Cookie，可能需要重新登录")
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
                logger.info(f"已保存 {len(cookies)} 条淘宝 Cookie")
            except Exception as e:
                logger.warning(f"保存 Cookie 失败: {e}")

    # ------------------------------------------------------------------
    #  会话验证
    # ------------------------------------------------------------------
    async def _check_session_valid(self) -> bool:
        """
        验证淘宝登录会话是否有效。
        通过访问淘宝直播门户页面并检查响应状态来判断。

        Returns:
            True 表示会话有效（已登录），False 表示需要重新登录。
        """
        if not self._page:
            return False

        try:
            logger.info("正在验证淘宝会话有效性...")

            # 访问淘宝直播门户
            response = await self._page.goto(
                "https://live.taobao.com",
                wait_until="domcontentloaded",
                timeout=30000,
            )

            await self._random_delay(2, 4)

            # 检查响应状态
            if response and response.status >= 400:
                logger.warning(f"淘宝直播门户返回异常状态码: {response.status}")
                self._session_valid = False
                return False

            # 检查页面内容判断是否已登录
            # 淘宝登录后页面通常包含用户昵称或头像元素
            page_content = await self._page.content()

            # 检查是否被重定向到登录页面
            current_url = self._page.url
            if "login.taobao.com" in current_url or "login.tmall.com" in current_url:
                logger.warning("检测到被重定向到登录页面，会话已失效")
                self._session_valid = False
                return False

            # 检查页面中是否有登录后的标志元素
            logged_in_selectors = [
                ".user-nick",
                ".user-name",
                ".J_UserNick",
                '[class*="avatar"]',
                '[class*="user-info"]',
                '[class*="nick-name"]',
            ]

            for selector in logged_in_selectors:
                try:
                    el = await self._page.query_selector(selector)
                    if el:
                        logger.info(f"会话验证通过（检测到元素: {selector}）")
                        self._session_valid = True
                        return True
                except Exception:
                    continue

            # 检查 _m_h5_tk Cookie 是否存在（MTOP API 必需）
            cookies = await self._context.cookies()
            h5_tk = [c for c in cookies if c.get("name") == "_m_h5_tk"]
            if h5_tk:
                logger.info("会话验证通过（检测到 _m_h5_tk Cookie）")
                self._session_valid = True
                return True

            # 无法确认登录状态，视为无效
            logger.warning("无法确认登录状态，会话可能已失效")
            self._session_valid = False
            return False

        except Exception as e:
            logger.warning(f"会话验证过程出错: {e}")
            self._session_valid = False
            return False

    async def _wait_for_manual_login(self):
        """
        Wait for user to manually log in to Taobao.
        Opens taobao.com and asks user to click the login link on the page.
        Polls for _m_h5_tk cookie to detect successful login.
        """
        print()
        print("  " + "=" * 56)
        print("  *** TAOBAO LOGIN REQUIRED ***")
        print("  " + "=" * 56)
        print()
        print("  A browser window will open to www.taobao.com.")
        print("  Please follow these steps:")
        print()
        print("    1. Switch to the browser window (Chromium)")
        print("    2. On the taobao.com page, find the login link")
        print("       (top area: 'Hi, Welcome to Taobao' or similar)")
        print("    3. Click it to go to the login page")
        print("    4. Scan QR code with Taobao/Alipay app, or")
        print("       enter your username and password")
        print()
        print("  The script will auto-detect when login is complete.")
        print("  Waiting up to 5 minutes...")
        print("  " + "=" * 56)
        print()

        logger.warning("Please log in to Taobao in the browser window")

        # Navigate the CURRENT page to taobao.com (don't create new tab -
        # user would still see the old tab in the foreground)
        # First close any extra tabs so only one is visible
        try:
            pages = self._context.pages
            for p in pages:
                if p != self._page:
                    try:
                        await p.close()
                    except Exception:
                        pass
        except Exception:
            pass

        # Navigate the visible page to taobao.com
        for url in ["https://www.taobao.com", "https://login.taobao.com"]:
            try:
                print(f"  Opening {url} ...")
                await self._page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(3)
                current = self._page.url
                print(f"  Page loaded: {current[:80]}")
                if "taobao.com" in current:
                    break
            except Exception as e:
                print(f"  Failed: {e}")
                continue

        # Poll for login completion (up to 5 minutes)
        # Minimum 30 seconds before checking - user needs time to find login and scan QR
        min_wait = 30
        max_wait = 300
        start_time = time.time()

        while time.time() - start_time < max_wait:
            await asyncio.sleep(5)

            elapsed = int(time.time() - start_time)
            remaining = max_wait - elapsed

            # Don't check cookies until minimum wait is over
            if elapsed < min_wait:
                if elapsed % 10 == 0:
                    print(f"  ({remaining}s remaining - please log in on the browser page)")
                continue

            # Check for login-specific cookies:
            # _nk_ = user nick (only set after login)
            # cookie2 = session cookie (only set after login)
            try:
                cookies = await self._context.cookies()
                cookie_names = [c.get("name", "") for c in cookies]

                # Primary check: _nk_ cookie (contains username, only exists after login)
                if "_nk_" in cookie_names:
                    nk_val = [c.get("value", "") for c in cookies if c.get("name") == "_nk_"]
                    if nk_val and nk_val[0]:
                        print(f"  Login successful! (user: {nk_val[0]}, detected after {elapsed}s)")
                        logger.info(f"Taobao login successful (_nk_ cookie detected)")
                        self._session_valid = True
                        await self._save_cookies()
                        return True

                # Secondary check: cookie2 (session cookie, only after login)
                if "cookie2" in cookie_names:
                    c2_val = [c.get("value", "") for c in cookies if c.get("name") == "cookie2"]
                    if c2_val and c2_val[0] and len(c2_val[0]) > 10:
                        print(f"  Login successful! (detected after {elapsed}s)")
                        logger.info("Taobao login successful (cookie2 detected)")
                        self._session_valid = True
                        await self._save_cookies()
                        return True
            except Exception:
                pass

            # Status update every 30 seconds
            if elapsed % 30 == 0:
                print(f"  Waiting for login... ({remaining}s remaining)")
                print("  (Click the login link on taobao.com, then scan QR or enter password)")

        print("  Login timeout (5 minutes). Please re-run the script.")
        logger.error("Taobao login timed out after 5 minutes")
        return False

    # ------------------------------------------------------------------
    #  滑动验证码检测与处理
    # ------------------------------------------------------------------
    async def _detect_captcha(self) -> bool:
        """
        检测页面是否出现了淘宝滑动验证码。
        淘宝风控（baxia / punish）会弹出滑动验证码。

        Returns:
            True 表示检测到验证码，False 表示未检测到。
        """
        if not self._page:
            return False

        captcha_selectors = [
            "#baxia-dialog",
            '[class*="slide-verify"]',
            '[class*="baxia"]',
            '[class*="punish"]',
            '[class*="captcha"]',
            "#nc_1_wrapper",
            '[class*="nc-container"]',
            '[class*="slide-btn"]',
            "iframe[src*='baxia']",
        ]

        for selector in captcha_selectors:
            try:
                el = await self._page.query_selector(selector)
                if el:
                    logger.warning(f"*** 检测到滑动验证码！(选择器: {selector}) ***")
                    self._captcha_detected = True
                    return True
            except Exception:
                continue

        # 也检查页面 URL 是否包含风控关键词
        try:
            current_url = self._page.url
            if any(kw in current_url for kw in ["punish", "baxia", "captcha", "verify"]):
                logger.warning(f"*** 检测到 URL 中包含风控关键词: {current_url} ***")
                self._captcha_detected = True
                return True
        except Exception:
            pass

        self._captcha_detected = False
        return False

    async def _handle_captcha(self):
        """
        处理滑动验证码：暂停并等待用户手动完成验证。
        """
        if not self._captcha_detected:
            return

        logger.warning("=" * 60)
        logger.warning("  *** 淘宝弹出滑动验证码，请在浏览器中手动完成验证 ***")
        logger.warning("  *** 验证完成后程序将自动继续                     ***")
        logger.warning("=" * 60)

        # 等待用户手动完成验证（最长等待 2 分钟）
        max_wait = 120
        start_time = time.time()
        while time.time() - start_time < max_wait:
            await asyncio.sleep(3)
            # 验证码消失则视为通过
            still_has_captcha = await self._detect_captcha()
            if not still_has_captcha:
                logger.info("滑动验证码已通过！")
                self._captcha_detected = False
                await self._save_cookies()
                return True

        logger.error("等待滑动验证码超时，请重新运行程序")
        return False

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

    async def _enforce_cooldown(self):
        """
        强制执行导航冷却时间。
        淘宝对频繁导航非常敏感，两次页面导航之间至少间隔 60 秒。
        """
        elapsed = time.time() - self._last_navigation_time
        if elapsed < self._navigation_cooldown and self._last_navigation_time > 0:
            wait_time = self._navigation_cooldown - elapsed + random.uniform(0, 10)
            logger.info(
                f"导航冷却中，还需等待 {wait_time:.1f} 秒..."
                f"（淘宝反爬要求间隔 {self._navigation_cooldown} 秒）"
            )
            await asyncio.sleep(wait_time)
        self._last_navigation_time = time.time()

    async def _safe_navigate(self, url: str, wait_until: str = "domcontentloaded",
                              timeout: int = 30000):
        """
        安全的页面导航方法，内置冷却控制和验证码检测。

        Args:
            url:        目标 URL。
            wait_until: 等待条件。
            timeout:    超时时间（毫秒）。

        Returns:
            Response 对象或 None。
        """
        # 执行导航冷却
        await self._enforce_cooldown()

        try:
            response = await self._page.goto(url, wait_until=wait_until, timeout=timeout)
            await self._random_delay(2, 4)

            # 导航后检查验证码
            if await self._detect_captcha():
                await self._handle_captcha()

            return response
        except Exception as e:
            logger.error(f"导航到 {url} 失败: {e}")
            return None

    # ------------------------------------------------------------------
    #  浏览器初始化
    # ------------------------------------------------------------------
    async def init_browser(self):
        """
        初始化 Playwright 浏览器实例。
        - 使用持久化上下文保存登录状态
        - 注入反检测脚本
        - 加载已保存的 Cookie
        - 验证会话有效性，如失效则提示手动登录
        """
        from playwright.async_api import async_playwright

        logger.info("正在启动 Playwright 浏览器（淘宝模式）...")

        self._playwright = await async_playwright().start()

        # 使用持久化上下文，保存浏览器数据到 user_data_dir
        # 淘宝必须使用非无头模式以支持手动登录
        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_PROFILES_DIR),
            headless=self.headless,
            viewport={"width": 1920, "height": 1080},
            user_agent=REALISTIC_USER_AGENT,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            # 禁用自动化检测相关的命令行参数
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-infobars",
            ],
            # 忽略 HTTPS 错误
            ignore_https_errors=True,
        )

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
                logger.info("已恢复保存的淘宝 Cookie")
            except Exception as e:
                logger.warning(f"恢复 Cookie 失败（可能已过期）: {e}")

        # Check session by cookie only (don't navigate to live.taobao.com -
        # it shows a creator download page, not a login page)
        session_ok = False
        try:
            cookies = await self._context.cookies()
            cookie_names = [c.get("name", "") for c in cookies]
            # _nk_ cookie contains username and is only set after login
            if "_nk_" in cookie_names:
                nk_val = [c.get("value", "") for c in cookies if c.get("name") == "_nk_"]
                if nk_val and nk_val[0]:
                    logger.info(f"Session OK (logged in as: {nk_val[0]})")
                    self._session_valid = True
                    session_ok = True
            if not session_ok:
                logger.warning("Session expired (no login cookie found)")
        except Exception as e:
            logger.warning(f"Cookie check failed: {e}")

        if not session_ok:
            if self.headless:
                logger.error(
                    "Headless mode cannot do manual login. Use headless=False."
                )
                raise RuntimeError(
                    "Taobao session expired and headless mode cannot login."
                )
            # Go directly to login page (don't navigate to live.taobao.com first)
            login_ok = await self._wait_for_manual_login()
            if not login_ok:
                raise RuntimeError("Taobao login failed.")

        logger.info("浏览器初始化完成（淘宝模式）")

    # ------------------------------------------------------------------
    #  直播间发现
    # ------------------------------------------------------------------
    async def discover_live_rooms(self, limit: int = 20) -> list[dict]:
        """
        从淘宝直播门户发现正在直播的房间。

        通过拦截 MTOP API 响应获取房间列表数据，
        如果 API 拦截失败则降级到 DOM 元素抓取。

        核心拦截的 API:
        - mtop.taobao.dreamroom.recommend.live.list
        - h5api.m.taobao.com 下的推荐接口

        Args:
            limit: 最多返回的房间数量。

        Returns:
            房间信息字典列表，每个字典包含:
            room_id, title, anchorNick, viewCount, praiseCount,
            coverUrl, live_url
        """
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        # 操作前检查会话
        if not self._session_valid:
            session_ok = await self._check_session_valid()
            if not session_ok:
                logger.error("会话无效，请先登录淘宝")
                return []

        rooms: list[dict] = []
        api_intercepted = asyncio.Event()

        async def _on_response(response):
            """拦截直播间列表 MTOP API 响应"""
            url = response.url

            # 检查是否为 MTOP 域名或匹配已知 API 模式
            is_mtop = _is_mtop_domain(url) or _is_mtop_api(url, MTOP_ROOM_LIST_PATTERNS)

            # Also catch any MTOP API that might contain live stream data
            url_lower = url.lower()
            is_live_api = is_mtop or (
                _is_mtop_domain(url) and any(
                    kw in url_lower for kw in [
                        "live", "dreamroom", "mediaplatform",
                        "recommend.list", "room.list",
                    ]
                )
            )

            if not is_live_api:
                return

            try:
                # 尝试解析 JSON 响应
                body = await response.json()
                data = body.get("data", body)

                # 淘宝 MTOP API 的数据结构可能有多种嵌套方式
                room_list = None

                # 尝试路径 1: data.resultList
                if isinstance(data, dict):
                    room_list = (
                        data.get("resultList", [])
                        or data.get("liveList", [])
                        or data.get("roomList", [])
                        or data.get("list", [])
                    )

                # 尝试路径 2: data.data.resultList（双层嵌套）
                if not room_list and isinstance(data, dict):
                    inner = data.get("data", {})
                    if isinstance(inner, dict):
                        room_list = (
                            inner.get("resultList", [])
                            or inner.get("liveList", [])
                            or inner.get("roomList", [])
                        )

                # 尝试路径 3: data 本身就是列表
                if not room_list and isinstance(data, list):
                    room_list = data

                if not room_list:
                    return

                for item in room_list:
                    room_info = self._extract_room_from_api(item)
                    if room_info:
                        rooms.append(room_info)

                if rooms:
                    api_intercepted.set()
                    logger.info(f"从 MTOP API 拦截到 {len(rooms)} 个淘宝直播间")

            except Exception as e:
                logger.debug(f"解析 MTOP 响应失败（可能不是房间列表接口）: {e}")

        # 注册响应拦截器
        self._page.on("response", _on_response)

        try:
            # Navigate to taobao.com main site (live.taobao.com is creator page, no streams)
            nav_urls = [
                "https://www.taobao.com",
                "https://s.taobao.com/search?q=%E7%9B%B4%E6%92%AD&type=live",
            ]
            for nav_url in nav_urls:
                logger.info(f"正在导航到 {nav_url} ...")
                await self._safe_navigate(nav_url)
                await asyncio.sleep(3)

                if self._stealth_available:
                    await self._stealth_async(self._page)

                # 等待 API 响应（最多 15 秒）
                try:
                    await asyncio.wait_for(api_intercepted.wait(), timeout=15)
                except asyncio.TimeoutError:
                    logger.info(f"MTOP API 拦截超时 ({nav_url})")

                if rooms:
                    break

                # 滚动加载更多
                for i in range(3):
                    await self._variable_scroll(self._page, "down")
                    await self._random_delay(2, 4)

            # If still no rooms from API, try DOM
            if not rooms:
                logger.info("MTOP API 拦截未获取到数据，尝试 DOM 降级方案...")
                rooms = await self._scrape_rooms_from_dom()

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

        # 去重（按 room_id）
        seen_ids = set()
        unique_rooms = []
        for room in rooms:
            rid = room.get("room_id", "")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                unique_rooms.append(room)

        # 限制返回数量
        result = unique_rooms[:limit]
        logger.info(f"共发现 {len(result)} 个淘宝直播间")
        return result

    def _extract_room_from_api(self, item: dict) -> Optional[dict]:
        """
        从 MTOP API 返回的房间数据中提取房间信息。
        淘宝的数据结构字段名与抖音不同，需要做适配。
        """
        try:
            # 淘宝房间 ID 可能在多个字段中
            room_id = str(
                item.get("liveId", "")
                or item.get("roomId", "")
                or item.get("id", "")
                or item.get("uuid", "")
            )
            if not room_id or room_id == "0":
                return None

            # 主播昵称
            anchor_nick = (
                item.get("anchorNick", "")
                or item.get("nick", "")
                or item.get("accountName", "")
                or item.get("userNick", "")
            )

            # 观看人数（淘宝可能用 viewCount / watchNum / pv 等字段）
            view_count = _parse_count(
                str(
                    item.get("viewCount", "")
                    or item.get("watchNum", "")
                    or item.get("pv", "")
                    or item.get("viewerCount", "")
                    or "0"
                )
            )

            # 点赞数
            praise_count = _parse_count(
                str(
                    item.get("praiseCount", "")
                    or item.get("likeCount", "")
                    or item.get("favorCount", "")
                    or "0"
                )
            )

            # 封面图
            cover_url = (
                item.get("coverUrl", "")
                or item.get("coverImg", "")
                or item.get("cover", "")
            )
            # 如果封面是列表格式
            if isinstance(cover_url, list):
                cover_url = cover_url[0] if cover_url else ""

            # 标题
            title = (
                item.get("title", "")
                or item.get("roomName", "")
                or item.get("liveTitle", "")
                or f"{anchor_nick}的直播间"
            )

            # 构建直播间 URL
            live_url = f"https://live.taobao.com/room/{room_id}"

            return {
                "room_id": room_id,
                "title": title,
                "room_name": title,
                "anchorNick": anchor_nick,
                "anchor_name": anchor_nick,
                "viewCount": view_count,
                "viewer_count": view_count,
                "viewer_count_text": str(item.get("viewCount", "")),
                "praiseCount": praise_count,
                "coverUrl": cover_url,
                "cover_url": cover_url,
                "live_url": live_url,
                "category": item.get("category", item.get("tag", "")),
                "source": "api",
                "platform": "taobao",
            }
        except Exception as e:
            logger.warning(f"提取淘宝房间数据失败: {e}")
            return None

    async def _scrape_rooms_from_dom(self) -> list[dict]:
        """
        降级方案：从 DOM 元素中抓取淘宝直播房间卡片信息。
        当 MTOP API 拦截失败时使用。
        """
        rooms = []
        try:
            # 淘宝直播页面中房间卡片的可能选择器
            selectors = [
                '[class*="live-card"]',
                '[class*="LiveCard"]',
                '[class*="room-item"]',
                '[class*="RoomItem"]',
                '[class*="feed-item"]',
                '[class*="dreamroom-card"]',
                '[class*="live-item"]',
                'a[href*="live.taobao.com/room"]',
            ]

            elements = []
            for selector in selectors:
                elements = await self._page.query_selector_all(selector)
                if elements:
                    logger.info(
                        f"DOM 降级：使用选择器 '{selector}' 找到 {len(elements)} 个元素"
                    )
                    break

            for el in elements:
                try:
                    # 尝试从链接中提取 room_id
                    href = await el.get_attribute("href") or ""
                    room_id_match = re.search(r"/room/(\w+)", href)
                    if not room_id_match:
                        room_id_match = re.search(r"liveId=(\w+)", href)
                    room_id = room_id_match.group(1) if room_id_match else ""

                    # 提取文本内容
                    text_content = await el.inner_text()
                    lines = [l.strip() for l in text_content.split("\n") if l.strip()]

                    # 启发式提取信息
                    anchor_name = lines[0] if lines else ""
                    title = lines[1] if len(lines) > 1 else anchor_name

                    # 尝试提取观看人数
                    view_count = 0
                    for line in lines:
                        count_match = re.search(r"([\d.]+万?)\s*(?:人|观看|在看)", line)
                        if count_match:
                            view_count = _parse_count(count_match.group(1))
                            break

                    if room_id:
                        rooms.append({
                            "room_id": room_id,
                            "title": title,
                            "room_name": title,
                            "anchorNick": anchor_name,
                            "anchor_name": anchor_name,
                            "viewCount": view_count,
                            "viewer_count": view_count,
                            "viewer_count_text": "",
                            "praiseCount": 0,
                            "coverUrl": "",
                            "cover_url": "",
                            "live_url": f"https://live.taobao.com/room/{room_id}",
                            "category": "",
                            "source": "dom",
                            "platform": "taobao",
                        })
                except Exception:
                    continue

        except Exception as e:
            logger.warning(f"DOM 降级抓取失败: {e}")

        return rooms

    # ------------------------------------------------------------------
    #  房间详情
    # ------------------------------------------------------------------
    async def fetch_room_detail(self, room_id: str) -> dict:
        """
        获取单个淘宝直播间的详细信息。

        通过导航到房间页面并拦截 MTOP 详情 API 获取数据。

        核心拦截的 API:
        - mtop.taobao.livex.v2.room.detail

        Args:
            room_id: 淘宝直播间 ID。

        Returns:
            包含房间详细信息的字典。
        """
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        # 操作前检查会话
        if not self._session_valid:
            session_ok = await self._check_session_valid()
            if not session_ok:
                logger.error("会话无效，请先登录淘宝")
                return {"room_id": room_id, "live_url": f"https://live.taobao.com/room/{room_id}"}

        detail: dict = {}
        detail_event = asyncio.Event()

        async def _on_response(response):
            """拦截房间详情 MTOP API"""
            url = response.url

            if not (_is_mtop_api(url, MTOP_ROOM_DETAIL_PATTERNS) or
                    (_is_mtop_domain(url) and "room" in url.lower() and "detail" in url.lower())):
                return

            try:
                body = await response.json()
                data = body.get("data", body)

                # 淘宝详情 API 数据结构适配
                room = data if isinstance(data, dict) else {}
                if "data" in room and isinstance(room["data"], dict):
                    room = room["data"]

                # 提取详细信息
                anchor_info = room.get("anchorInfo", {})
                anchor_nick = (
                    anchor_info.get("nick", "")
                    or anchor_info.get("anchorNick", "")
                    or room.get("anchorNick", "")
                )

                detail.update({
                    "room_id": str(room.get("liveId", room.get("roomId", room_id))),
                    "room_name": room.get("title", room.get("roomName", "")),
                    "anchor_name": anchor_nick,
                    "anchor_id": str(anchor_info.get("userId", room.get("anchorId", ""))),
                    "anchor_avatar": anchor_info.get("avatarUrl", ""),
                    "viewer_count": _parse_count(
                        str(room.get("viewCount", room.get("watchNum", "0")))
                    ),
                    "viewer_count_text": str(room.get("viewCount", "")),
                    "praise_count": _parse_count(
                        str(room.get("praiseCount", room.get("likeCount", "0")))
                    ),
                    "start_time": room.get("startTime", room.get("gmtCreate", "")),
                    "cover_url": room.get("coverUrl", room.get("coverImg", "")),
                    "live_url": f"https://live.taobao.com/room/{room_id}",
                    "status": room.get("roomStatus", room.get("status", "")),
                    "category": room.get("category", room.get("tag", "")),
                    "products_preview": room.get("itemList", []),
                    "source": "api",
                    "platform": "taobao",
                })
                detail_event.set()
                logger.info(f"已获取淘宝房间详情: {detail.get('room_name', '')}")

            except Exception as e:
                logger.warning(f"解析淘宝房间详情 API 失败: {e}")

        self._page.on("response", _on_response)

        try:
            logger.info(f"正在获取淘宝房间详情: {room_id}")
            await self._safe_navigate(f"https://live.taobao.com/room/{room_id}")

            if self._stealth_available:
                await self._stealth_async(self._page)

            # 等待详情 API 响应
            try:
                await asyncio.wait_for(detail_event.wait(), timeout=20)
            except asyncio.TimeoutError:
                logger.warning(f"淘宝房间详情 API 超时: {room_id}")

            # 如果 API 拦截失败，尝试从页面提取基本信息
            if not detail:
                detail = await self._scrape_room_detail_from_dom(room_id)

        except Exception as e:
            logger.error(f"获取淘宝房间详情失败: {e}")
            if not detail:
                detail = {
                    "room_id": room_id,
                    "live_url": f"https://live.taobao.com/room/{room_id}",
                    "platform": "taobao",
                }
        finally:
            self._page.remove_listener("response", _on_response)

        await self._save_cookies()
        return detail

    async def _scrape_room_detail_from_dom(self, room_id: str) -> dict:
        """降级：从 DOM 中提取淘宝房间详情"""
        detail = {
            "room_id": room_id,
            "live_url": f"https://live.taobao.com/room/{room_id}",
            "source": "dom",
            "platform": "taobao",
        }
        try:
            # 尝试多种可能的选择器
            title_selectors = [
                '[class*="room-title"]',
                '[class*="RoomTitle"]',
                '[class*="live-title"]',
                "h1",
                ".title",
            ]
            for sel in title_selectors:
                try:
                    el = await self._page.query_selector(sel)
                    if el:
                        detail["room_name"] = await el.inner_text()
                        break
                except Exception:
                    continue

            name_selectors = [
                '[class*="anchor-name"]',
                '[class*="AnchorName"]',
                '[class*="nick"]',
                '[class*="host-name"]',
            ]
            for sel in name_selectors:
                try:
                    el = await self._page.query_selector(sel)
                    if el:
                        detail["anchor_name"] = await el.inner_text()
                        break
                except Exception:
                    continue

        except Exception as e:
            logger.warning(f"DOM 降级获取淘宝房间详情失败: {e}")

        return detail

    # ------------------------------------------------------------------
    #  弹幕流抓取（核心功能 - 淘宝长轮询模式）
    # ------------------------------------------------------------------
    async def start_danmaku_stream(
        self,
        room_id: str,
        callback: Callable[[dict, str, str], None],
        duration: int = 300,
    ):
        """
        启动弹幕流抓取。通过拦截淘宝长轮询消息 API 获取实时弹幕。

        与抖音不同，淘宝弹幕不走 WebSocket，而是通过长轮询机制：
        前端定期向 MTOP 消息接口发送请求，服务端返回新消息列表。

        工作流程:
        1. 导航到直播间页面
        2. 拦截重复的消息 API 请求（长轮询模式）
        3. 解析每条消息的类型（文本评论、系统消息、礼物消息）
        4. 对每条消息调用 callback(message_dict, room_id, "taobao")
        5. 如果检测到滑动验证码，暂停并等待处理

        Args:
            room_id:  淘宝直播间 ID。
            callback: 消息回调函数 callback(message_dict, room_id, platform)。
            duration: 抓取持续时间（秒），默认 300 秒（5 分钟）。
        """
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        # 操作前检查会话
        if not self._session_valid:
            session_ok = await self._check_session_valid()
            if not session_ok:
                logger.error("会话无效，无法启动弹幕流")
                return

        stop_event = asyncio.Event()
        message_count = [0]
        poll_interval = random.uniform(2.0, 3.0)  # 轮询间隔 2-3 秒

        async def _on_response(response):
            """拦截弹幕消息 MTOP API 响应"""
            url = response.url

            # 检查是否为消息 API
            is_message_api = _is_mtop_api(url, MTOP_DANMAKU_PATTERNS)
            if not is_message_api:
                # 也尝试匹配通用的消息接口特征
                if not (_is_mtop_domain(url) and any(
                    kw in url.lower() for kw in ["message", "chat", "barrage", "danmaku", "im"]
                )):
                    return

            try:
                body = await response.json()
                data = body.get("data", body)

                # 解析消息列表
                messages = (
                    data.get("messageList", [])
                    or data.get("msgList", [])
                    or data.get("messages", [])
                    or data.get("list", [])
                )

                if not messages and isinstance(data, dict):
                    # 尝试从嵌套结构中获取
                    inner = data.get("data", {})
                    if isinstance(inner, dict):
                        messages = (
                            inner.get("messageList", [])
                            or inner.get("msgList", [])
                            or inner.get("messages", [])
                        )

                if not messages:
                    return

                for msg in messages:
                    try:
                        parsed_msg = self._parse_danmaku_message(msg)
                        if parsed_msg:
                            # 调用用户回调
                            if asyncio.iscoroutinefunction(callback):
                                await callback(parsed_msg, room_id, "taobao")
                            else:
                                callback(parsed_msg, room_id, "taobao")
                            message_count[0] += 1
                    except Exception as cb_err:
                        logger.warning(f"弹幕回调处理出错: {cb_err}")

            except Exception as e:
                logger.debug(f"解析弹幕消息 API 失败: {e}")

        # 注册响应拦截器
        self._page.on("response", _on_response)

        start_time = time.time()
        try:
            logger.info(
                f"[淘宝房间 {room_id}] 正在启动弹幕流（长轮询模式，"
                f"时长 {duration} 秒，轮询间隔 {poll_interval:.1f} 秒）..."
            )

            # 导航到直播间
            await self._safe_navigate(f"https://live.taobao.com/room/{room_id}")

            if self._stealth_available:
                await self._stealth_async(self._page)

            await self._random_delay(3, 5)

            # 主循环：持续监听直到超时
            while time.time() - start_time < duration:
                await asyncio.sleep(poll_interval)

                # 定期检查验证码
                if await self._detect_captcha():
                    logger.warning("[弹幕流] 检测到滑动验证码，暂停弹幕抓取")
                    captcha_ok = await self._handle_captcha()
                    if not captcha_ok:
                        logger.error("[弹幕流] 验证码处理失败，终止弹幕流")
                        break
                    # 验证码处理完成后刷新页面
                    try:
                        await self._page.reload(wait_until="domcontentloaded", timeout=30000)
                        await self._random_delay(3, 5)
                    except Exception as reload_err:
                        logger.warning(f"验证码后刷新页面失败: {reload_err}")
                        break

                # 每 30 秒做一次鼠标微动，保持活跃
                elapsed = time.time() - start_time
                if int(elapsed) % 30 < int(poll_interval):
                    await self._mouse_jitter(self._page)

                # 每 60 秒做一次轻微滚动，模拟观看行为
                if int(elapsed) % 60 < int(poll_interval):
                    await self._variable_scroll(self._page, "down")
                    await asyncio.sleep(0.5)
                    await self._variable_scroll(self._page, "up")

        except Exception as e:
            logger.error(f"[淘宝房间 {room_id}] 弹幕流异常终止: {e}")
        finally:
            self._page.remove_listener("response", _on_response)
            elapsed = time.time() - start_time
            logger.info(
                f"[淘宝房间 {room_id}] 弹幕流结束：运行 {elapsed:.1f} 秒，"
                f"共接收 {message_count[0]} 条消息"
            )

    def _parse_danmaku_message(self, msg: dict) -> Optional[dict]:
        """
        解析淘宝弹幕消息。

        淘宝消息类型：
        - text / comment: 文字评论
        - enter / system: 进入直播间（系统消息）
        - gift / reward:  礼物消息
        - follow:         关注消息
        - like:           点赞消息

        Args:
            msg: 原始消息字典。

        Returns:
            解析后的标准化消息字典，或 None（如果无法解析）。
        """
        try:
            # 消息类型判断
            msg_type_raw = (
                msg.get("type", "")
                or msg.get("msgType", "")
                or msg.get("messageType", "")
            ).lower()

            # 用户信息
            user_info = msg.get("userInfo", msg.get("user", {}))
            if isinstance(user_info, str):
                user_info = {"nick": user_info}

            user = {
                "user_id": str(
                    user_info.get("userId", "")
                    or user_info.get("uid", "")
                    or msg.get("userId", "")
                ),
                "nickname": (
                    user_info.get("nick", "")
                    or user_info.get("nickname", "")
                    or user_info.get("displayNick", "")
                    or msg.get("userNick", "")
                ),
                "avatar": user_info.get("avatarUrl", user_info.get("avatar", "")),
            }

            # 根据消息类型构建标准化消息
            if msg_type_raw in ("text", "comment", "chat", "barrage", "danmu"):
                # 文字评论
                return {
                    "type": "chat",
                    "user": user,
                    "content": msg.get("content", msg.get("text", msg.get("message", ""))),
                    "timestamp": msg.get("timestamp", msg.get("sendTime", time.time())),
                }
            elif msg_type_raw in ("enter", "system", "join", "comein"):
                # 进入直播间
                return {
                    "type": "member",
                    "user": user,
                    "content": f"{user['nickname']} 进入直播间",
                    "timestamp": msg.get("timestamp", msg.get("sendTime", time.time())),
                }
            elif msg_type_raw in ("gift", "reward", "item", "present"):
                # 礼物消息
                return {
                    "type": "gift",
                    "user": user,
                    "gift_name": msg.get("giftName", msg.get("itemName", msg.get("name", "未知礼物"))),
                    "count": int(msg.get("count", msg.get("num", 1))),
                    "value": float(msg.get("value", msg.get("price", 0))),
                    "timestamp": msg.get("timestamp", msg.get("sendTime", time.time())),
                }
            elif msg_type_raw in ("follow", "subscribe"):
                # 关注消息
                return {
                    "type": "follow",
                    "user": user,
                    "content": f"{user['nickname']} 关注了主播",
                    "timestamp": msg.get("timestamp", msg.get("sendTime", time.time())),
                }
            elif msg_type_raw in ("like", "favor", "praise"):
                # 点赞消息
                return {
                    "type": "like",
                    "user": user,
                    "count": int(msg.get("count", msg.get("num", 1))),
                    "timestamp": msg.get("timestamp", msg.get("sendTime", time.time())),
                }
            else:
                # 未知类型，仍然返回（便于调试）
                content = msg.get("content", msg.get("text", ""))
                if content:
                    return {
                        "type": msg_type_raw or "unknown",
                        "user": user,
                        "content": content,
                        "timestamp": msg.get("timestamp", msg.get("sendTime", time.time())),
                        "raw": msg,
                    }
                return None

        except Exception as e:
            logger.debug(f"解析弹幕消息失败: {e}")
            return None

    # ------------------------------------------------------------------
    #  商品列表抓取
    # ------------------------------------------------------------------
    async def fetch_products(self, room_id: str) -> list[dict]:
        """
        获取淘宝直播间的商品列表（购物车/宝贝口袋中的商品）。

        通过拦截商品列表 MTOP API 获取数据。

        核心拦截的 API:
        - mtop.taobao.livex.product
        - mtop.taobao.dreamroom.product

        Args:
            room_id: 淘宝直播间 ID。

        Returns:
            商品信息字典列表。
        """
        if not self._page:
            raise RuntimeError("请先调用 init_browser() 初始化浏览器")

        # 操作前检查会话
        if not self._session_valid:
            session_ok = await self._check_session_valid()
            if not session_ok:
                logger.error("会话无效，无法获取商品列表")
                return []

        products: list[dict] = []
        product_event = asyncio.Event()

        async def _on_response(response):
            """拦截商品列表 MTOP API"""
            url = response.url

            is_product_api = _is_mtop_api(url, MTOP_PRODUCT_PATTERNS)
            if not is_product_api:
                # 也检查通用的商品接口特征
                if not (_is_mtop_domain(url) and any(
                    kw in url.lower() for kw in ["product", "item", "goods", "commodity"]
                )):
                    return

            try:
                body = await response.json()
                data = body.get("data", body)

                # 尝试多种数据路径
                product_list = None
                if isinstance(data, dict):
                    product_list = (
                        data.get("itemList", [])
                        or data.get("productList", [])
                        or data.get("products", [])
                        or data.get("list", [])
                        or data.get("resultList", [])
                    )

                # 双层嵌套
                if not product_list and isinstance(data, dict):
                    inner = data.get("data", {})
                    if isinstance(inner, dict):
                        product_list = (
                            inner.get("itemList", [])
                            or inner.get("productList", [])
                            or inner.get("list", [])
                        )

                if not product_list:
                    return

                for item in product_list:
                    product = self._extract_product(item)
                    if product:
                        products.append(product)

                if products:
                    product_event.set()
                    logger.info(
                        f"[淘宝房间 {room_id}] 拦截到 {len(products)} 个商品"
                    )

            except Exception as e:
                logger.debug(f"解析商品列表 API 失败: {e}")

        self._page.on("response", _on_response)

        try:
            # 确保在房间页面
            current_url = self._page.url
            if room_id not in current_url:
                await self._safe_navigate(f"https://live.taobao.com/room/{room_id}")

            if self._stealth_available:
                await self._stealth_async(self._page)

            await self._random_delay(3, 5)

            # 尝试点击购物车 / 宝贝口袋图标
            cart_selectors = [
                '[class*="shopping-cart"]',
                '[class*="ShoppingCart"]',
                '[class*="product-bag"]',
                '[class*="item-bag"]',
                '[class*="goods-list"]',
                '[class*="cart-icon"]',
                '[class*="item-icon"]',
                '[class*="pocket"]',
                'button[class*="product"]',
            ]

            clicked = False
            for selector in cart_selectors:
                try:
                    el = await self._page.query_selector(selector)
                    if el:
                        await self._mouse_jitter(self._page)
                        await el.click()
                        clicked = True
                        logger.info(f"已点击商品列表按钮（选择器: {selector}）")
                        break
                except Exception:
                    continue

            if not clicked:
                logger.warning(
                    f"[淘宝房间 {room_id}] 未找到商品列表按钮，"
                    "可能该直播间没有商品或按钮样式已更新"
                )

            # 等待商品列表 API 响应
            try:
                await asyncio.wait_for(product_event.wait(), timeout=20)
            except asyncio.TimeoutError:
                logger.warning(
                    f"[淘宝房间 {room_id}] 等待商品列表 API 超时"
                )

            # 检查是否出现验证码
            if await self._detect_captcha():
                await self._handle_captcha()

        except Exception as e:
            logger.error(f"[淘宝房间 {room_id}] 获取商品列表失败: {e}")
        finally:
            self._page.remove_listener("response", _on_response)

        await self._save_cookies()
        return products

    def _extract_product(self, item: dict) -> Optional[dict]:
        """
        从商品 API 数据中提取单个商品信息。
        适配淘宝商品数据结构。
        """
        try:
            # 商品名称
            product_name = (
                item.get("title", "")
                or item.get("itemTitle", "")
                or item.get("name", "")
                or item.get("productTitle", "")
            )
            if not product_name:
                return None

            # 价格（淘宝价格通常以元为单位，也可能以分为单位）
            price_raw = item.get("price", item.get("promotionPrice", 0))
            try:
                price = float(price_raw)
                # 如果价格大于 10000，可能是以分为单位
                if price > 10000 and isinstance(price_raw, int):
                    price = price / 100.0
            except (ValueError, TypeError):
                price = 0.0

            # 原价
            original_price_raw = item.get("originalPrice", item.get("marketPrice", item.get("tagPrice", 0)))
            try:
                original_price = float(original_price_raw)
                if original_price > 10000 and isinstance(original_price_raw, int):
                    original_price = original_price / 100.0
            except (ValueError, TypeError):
                original_price = 0.0

            # 销量
            sales = (
                item.get("sales", "")
                or item.get("soldCount", "")
                or item.get("buyCount", "")
                or item.get("monthSellCount", "")
            )

            # 图片 URL
            image_url = (
                item.get("picUrl", "")
                or item.get("imageUrl", "")
                or item.get("imgUrl", "")
                or item.get("coverUrl", "")
                or item.get("image", "")
            )
            # 确保图片 URL 有协议前缀
            if image_url and image_url.startswith("//"):
                image_url = "https:" + image_url

            return {
                "product_name": product_name,
                "price": price,
                "original_price": original_price,
                "sales": sales,
                "image_url": image_url,
                "product_id": str(
                    item.get("itemId", "")
                    or item.get("id", "")
                    or item.get("productId", "")
                    or item.get("nid", "")
                ),
                "platform": "taobao",
            }
        except Exception as e:
            logger.warning(f"提取淘宝商品数据失败: {e}")
            return None

    # ------------------------------------------------------------------
    #  清理与关闭
    # ------------------------------------------------------------------
    async def close(self):
        """
        关闭浏览器并保存 Cookie。
        应在程序退出前调用。
        """
        logger.info("正在关闭淘宝爬虫浏览器...")
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

        logger.info("淘宝爬虫浏览器已关闭")


# ============================================================================
#  CLI 入口
# ============================================================================
async def _cli_main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(
        description="淘宝直播间爬虫 - 星播大数据分析平台",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 发现当前正在直播的淘宝房间
  python -m data_pipeline.taobao_crawler --mode discover

  # 监控指定房间的弹幕
  python -m data_pipeline.taobao_crawler --mode monitor --room-id 123456

  # 获取指定房间的详情
  python -m data_pipeline.taobao_crawler --mode detail --room-id 123456

  # 获取指定房间的商品列表
  python -m data_pipeline.taobao_crawler --mode products --room-id 123456

注意:
  - 首次运行需要在弹出的浏览器窗口中手动登录淘宝账号
  - 淘宝反爬较严格，请控制爬取频率
  - 遇到滑动验证码时请在浏览器中手动完成验证
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
        help="淘宝直播间 ID（monitor/detail/products 模式必填）",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="无头模式运行（不推荐！首次必须手动登录淘宝）",
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

    crawler = TaobaoLiveCrawler(headless=args.headless)

    try:
        await crawler.init_browser()

        if args.mode == "discover":
            rooms = await crawler.discover_live_rooms(limit=20)
            print(f"\n发现 {len(rooms)} 个淘宝直播间:")
            print("-" * 80)
            for i, room in enumerate(rooms, 1):
                print(
                    f"{i:3d}. [{room.get('room_id', '')}] "
                    f"{room.get('anchor_name', '未知主播')} - "
                    f"{room.get('title', room.get('room_name', '未知房间'))} "
                    f"(观看: {room.get('viewer_count_text', 'N/A')})"
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
                elif msg_type == "follow":
                    user = msg.get("user", {}).get("nickname", "匿名")
                    print(f"[{ts}] {user} 关注了主播")
                elif msg_type == "like":
                    pass  # 点赞消息太多，默认不打印
                else:
                    print(f"[{ts}] [{msg_type}] {msg}")

            await crawler.start_danmaku_stream(
                room_id=args.room_id,
                callback=_print_callback,
                duration=args.duration,
            )

        elif args.mode == "detail":
            if not args.room_id:
                parser.error("detail 模式需要指定 --room-id")
            detail = await crawler.fetch_room_detail(args.room_id)
            print("\n淘宝房间详情:")
            print("-" * 40)
            for k, v in detail.items():
                print(f"  {k}: {v}")

        elif args.mode == "products":
            if not args.room_id:
                parser.error("products 模式需要指定 --room-id")
            products = await crawler.fetch_products(args.room_id)
            print(f"\n淘宝商品列表（共 {len(products)} 个）:")
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
