"""
抖音登录工具 - 手动登录并保存 Cookie，供弹幕采集使用。
运行后会打开浏览器，请用手机抖音扫码登录。
"""
import asyncio, json, os, sys, time, logging, pathlib
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

BASE_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data_pipeline'
COOKIES_DIR = DATA_DIR / 'cookies'
COOKIES_FILE = COOKIES_DIR / 'douyin_cookies.json'
# Chrome crashes with Chinese chars in user-data-dir, use home path
BROWSER_PROFILES_DIR = pathlib.Path.home() / '.qoderworkcn' / 'douyin_browser_profile'

async def login():
    from playwright.async_api import async_playwright
    COOKIES_DIR.mkdir(parents=True, exist_ok=True)
    BROWSER_PROFILES_DIR.mkdir(parents=True, exist_ok=True)

    print('=' * 55)
    print('  Douyin Login Tool')
    print('=' * 55)
    print('  Opening browser... please scan QR with Douyin app')

    pw = await async_playwright().start()
    ctx = await pw.chromium.launch_persistent_context(
        user_data_dir=str(BROWSER_PROFILES_DIR),
        channel='chrome', headless=False,
        viewport={'width': 1280, 'height': 800},
        locale='zh-CN', timezone_id='Asia/Shanghai',
        args=['--disable-blink-features=AutomationControlled','--no-first-run','--no-default-browser-check'],
        ignore_https_errors=True,
    )
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()

    await page.goto('https://www.douyin.com', wait_until='domcontentloaded', timeout=30000)
    await asyncio.sleep(3)

    try:
        btn = await page.query_selector('button:has-text("登录"), [class*="login"], [data-e2e="user-login"]')
        if btn:
            await btn.click()
            print('  Clicked login button')
    except: pass

    print('  >>> Please log in via the browser window <<<')
    login_keys = {'sessionid','sessionid_ss','sid_guard','passport_auth_status'}
    start = time.time()
    logged_in = False

    while time.time() - start < 300:
        await asyncio.sleep(5)
        cookies = await ctx.cookies()
        names = {c['name'] for c in cookies}
        if 'sessionid' in names or 'sessionid_ss' in names or 'passport_auth_status' in names:
            logged_in = True
            print(f'  LOGIN SUCCESS! Detected: {login_keys & names}')
            break
        elapsed = int(time.time() - start)
        if elapsed % 15 == 0 and elapsed > 0:
            print(f'  Waiting... ({elapsed}s) cookies={len(cookies)}')

    if logged_in:
        cookies = await ctx.cookies()
        with open(COOKIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)
        print(f'  Saved {len(cookies)} cookies to {COOKIES_FILE}')
        print('  Now restart run_cluster.py to enable danmaku collection!')
    else:
        print('  Timeout. Please retry.')

    input('\n  Press Enter to close...')
    await ctx.close()
    await pw.stop()

if __name__ == '__main__':
    asyncio.run(login())
