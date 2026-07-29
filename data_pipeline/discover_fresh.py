# -*- coding: utf-8 -*-
"""Fresh room discovery: crawl Douyin directory + enter API commerce check."""
import asyncio, logging
logger = logging.getLogger(__name__)

DIRECTORY_URLS = [
    'https://live.douyin.com/category/100102',  # 购物
    'https://live.douyin.com/category/100101',  # 娱乐
    'https://live.douyin.com/category/100106',  # 数码
]

JS_EXTRACT_RIDS = """() => {
    const rids = new Set();
    document.querySelectorAll('a[href]').forEach(a => {
        const m = a.href.match(/live\.douyin\.com\/(\d{3,15})/);
        if (m && m[1].length >= 6) rids.add(m[1]);
    });
    try {
        const chunks = window.__pace_f || [];
        for (const c of chunks) {
            const s = JSON.stringify(c);
            const re = /"web_rid"\s*:\s*"(\d{6,15})"/g;
            let m;
            while ((m = re.exec(s)) !== null) rids.add(m[1]);
        }
    } catch(e) {}
    return [...rids];
}"""

JS_CHECK_ROOM = """async (webRid) => {
    try {
        const url = 'https://live.douyin.com/webcast/room/web/enter/?aid=6383&app_name=douyin_web&live_id=1&device_platform=web&enter_from=web_live&web_rid=' + webRid;
        let signUrl = url;
        if (typeof window.byted_acrawler !== 'undefined' &&
            typeof window.byted_acrawler.frontierSign === 'function') {
            const signed = await window.byted_acrawler.frontierSign(url);
            if (signed && signed['X-Bogus']) signUrl = url + '&X-Bogus=' + signed['X-Bogus'];
        }
        const resp = await fetch(signUrl, {credentials: 'include'});
        const json = await resp.json();
        const data = json.data?.data;
        if (!data || !data.length) return {error: 'no data', code: json.status_code};
        const room = data[0];
        return {
            web_rid: webRid,
            id_str: room.id_str || '',
            status: room.status,
            title: room.title || '',
            has_commerce_goods: !!room.has_commerce_goods,
            user_count_str: room.user_count_str || '0',
            nickname: (room.owner && room.owner.nickname) || '',
        };
    } catch(e) {
        return {error: e.message, web_rid: webRid};
    }
}"""

async def discover_live_commerce_rooms(crawler, max_rooms=200, max_commerce=100):
    all_rids = set()
    page = await crawler._context.new_page()
    if crawler._stealth_available:
        try: await crawler._stealth_async(page)
        except: pass
    try:
        for _url_idx, url in enumerate(DIRECTORY_URLS):
            if len(all_rids) >= max_rooms: break
            if _url_idx > 0:
                await asyncio.sleep(5)  # 页面间延迟，防Chrome崩溃
            try:
                await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(8000)
                for _ in range(3):  # 减少滚动次数节省内存
                    await page.evaluate('window.scrollBy(0, 1500)')
                    await page.wait_for_timeout(1200)
                rids = await page.evaluate(JS_EXTRACT_RIDS)
                for r in rids: all_rids.add(r)
                logger.info(f"[Discover] {url}: {len(rids)} rids (total: {len(all_rids)})")
            except Exception as e:
                logger.warning(f"[Discover] {url} failed: {e}")
        logger.info(f"[Discover] Phase 1: {len(all_rids)} unique web_rids")
        if not all_rids: return []
        sign_page = await crawler._context.new_page()
        if crawler._stealth_available:
            try: await crawler._stealth_async(sign_page)
            except: pass
        try:
            await sign_page.goto('https://live.douyin.com', wait_until='domcontentloaded', timeout=20000)
            await sign_page.wait_for_timeout(5000)
            has_fs = await sign_page.evaluate("typeof window.byted_acrawler !== 'undefined' && typeof window.byted_acrawler.frontierSign === 'function'")
            if not has_fs:
                logger.error("[Discover] frontierSign not available")
                return []
            commerce_rooms = []
            rid_list = list(all_rids)
            for i in range(0, len(rid_list), 8):
                batch = rid_list[i:i+8]
                tasks = [sign_page.evaluate(JS_CHECK_ROOM, rid) for rid in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception) or not isinstance(result, dict): continue
                    if result.get('error'): continue
                    if result.get('status') == 2 and result.get('has_commerce_goods'):
                        commerce_rooms.append(result)
                if i % 40 == 0 and i > 0:
                    logger.info(f"[Discover] Checked {i}/{len(rid_list)}, commerce: {len(commerce_rooms)}")
                await asyncio.sleep(0.1)
                if len(commerce_rooms) >= max_commerce: break
            logger.info(f"[Discover] Found {len(commerce_rooms)} live commerce rooms")
            return commerce_rooms
        finally:
            await sign_page.close()
    finally:
        await page.close()
