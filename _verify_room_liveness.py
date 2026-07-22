# -*- coding: utf-8 -*-
"""Verify if DB "live" rooms are actually live on Douyin AND have shopping cart."""
import sys, json, asyncio, shutil, tempfile
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import pymysql

VMS = {'mysql': '192.168.104.100:3306'}
USER, PWD, DB = 'root', '123456', 'livecommerce_db'
COOKIE_FILE = r'C:\Users\MECHREVO\Desktop\星播大数据分析平台\data_pipeline\cookies\douyin_cookies.json'
BATCH_SIZE = 8
MAX_ROOMS = 100
SCRIPT_VERSION = 3  # v3: increased limits, marks ended as finished

def get_candidates():
    conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306,
        user=USER, password=PWD, database=DB, charset='utf8mb4', connect_timeout=10)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT room_id_external, room_name, anchor_name FROM live_room "
                "WHERE status='live' AND deleted=0 AND data_source='real' "
                "AND room_id_external IS NOT NULL AND room_id_external != '' "
                "ORDER BY viewer_count DESC LIMIT %s", (MAX_ROOMS,))
    rooms = cur.fetchall()
    cur.close(); conn.close()
    return rooms

async def verify_rooms(candidates):
    from playwright.async_api import async_playwright
    results = {'live': [], 'ended': [], 'no_cart': []}
    async with async_playwright() as p:
        tmp = tempfile.mkdtemp(prefix='verify_')
        try:
            browser = await p.chromium.launch(headless=True, channel='chrome',
                args=['--disable-blink-features=AutomationControlled', '--headless=new',
                       '--disk-cache-size=1', '--no-sandbox'])
            ctx = await browser.new_context(ignore_https_errors=True)
            try:
                import json as _j
                with open(COOKIE_FILE, 'r') as f:
                    cookies = _j.load(f)
                await ctx.add_cookies(cookies)
            except Exception:
                pass
            for i in range(0, len(candidates), BATCH_SIZE):
                batch = candidates[i:i+BATCH_SIZE]
                async def check_one(room, ctx=ctx):
                    page = await ctx.new_page()
                    try:
                        rid = str(room.get('room_id_external', ''))
                        await page.goto(f'https://live.douyin.com/{rid}',
                            wait_until='domcontentloaded', timeout=30000)
                        await page.wait_for_timeout(6000)
                        body = await page.evaluate('document.body?.innerText || ""')
                        if '已结束' in body or '直播已结束' in body:
                            return rid, 'ended'
                        has_video = await page.evaluate(
                            '(() => { const v = document.querySelector("video"); '
                            'if (!v) return false; '
                            'if (v.readyState >= 2 && !v.paused) return true; '
                            'if (v.src && v.readyState > 0) return true; '
                            'return false; })()')
                        if not has_video and '直播中' not in body:
                            return rid, 'ended'
                        has_cart = await page.evaluate("""(() => {
                            var bt = document.body ? document.body.innerText : '';
                            if (/购物车|去购物|去购买|正在卖|商品|下单|小黄车|讲解中/.test(bt)) return true;
                            if (/福利|秒杀|抢购|限时|链接|点击购/.test(bt)) return true;
                            var els = document.querySelectorAll('div, span, button, a, i, img, svg');
                            for (var i = 0; i < els.length; i++) {
                                var cn = els[i].className || '';
                                if (typeof cn === 'string' &&
                                    /shopping|cart|goods|product|commodity|commerce|ec-|buy|shop/i.test(cn)) return true;
                                var t = els[i].textContent || '';
                                if (t.length < 20 && /购物车|去购物|去购买|正在卖|商品|下单|小黄车|讲解中/.test(t)) return true;
                            }
                            try {
                                var ch = window.__pace_f || [];
                                for (var c = 0; c < ch.length; c++) {
                                    var s = JSON.stringify(ch[c]);
                                    if (s.includes('ShoppingCart') || s.includes('shopping_cart')
                                        || s.includes('productList') || s.includes('commerce')
                                        || s.includes('commodity') || s.includes('buyin')) return true;
                                }
                            } catch(e) {}
                            return false;
                        })()""")
                        if has_cart:
                            return rid, 'live'
                        else:
                            return rid, 'no_cart'
                    except Exception:
                        return str(room.get('room_id_external', '')), 'ended'
                    finally:
                        await page.close()
                tasks = [check_one(r) for r in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in batch_results:
                    if isinstance(r, tuple):
                        rid, status = r
                        results[status].append(rid)
            await ctx.close()
            await browser.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    return results

def update_db(results):
    """Mark ended/no_cart as finished, confirm live rooms as live."""
    conn = pymysql.connect(host=VMS['mysql'].split(':')[0], port=3306,
        user=USER, password=PWD, database=DB, charset='utf8mb4', connect_timeout=10)
    cur = conn.cursor()
    ended = 0
    live = 0
    # Mark ended rooms as finished
    if results['ended']:
        ph = ','.join(['%s'] * len(results['ended']))
        cur.execute(f"UPDATE live_room SET status='finished' "
                    f"WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results['ended'])
        ended = cur.rowcount
        cur.execute(f"UPDATE rt_room_stats SET status='finished' "
                    f"WHERE room_id IN ({ph})",
                    results['ended'])
    # Mark no_cart rooms as finished
    if results.get('no_cart'):
        ph = ','.join(['%s'] * len(results['no_cart']))
        cur.execute(f"UPDATE live_room SET status='finished' "
                    f"WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results['no_cart'])
        ended += cur.rowcount
    # Confirm live rooms
    if results['live']:
        ph = ','.join(['%s'] * len(results['live']))
        cur.execute(f"UPDATE live_room SET status='live', has_shopping_cart=1 "
                    f"WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results['live'])
        live = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    return live, ended

if __name__ == '__main__':
    candidates = get_candidates()
    if not candidates:
        print(json.dumps({'ok': True, 'live': 0, 'ended': 0, 'no_cart': 0, 'checked': 0}))
        sys.exit(0)
    results = asyncio.run(verify_rooms(candidates))
    live_n, ended_n = update_db(results)
    print(json.dumps({
        'ok': True, 'live': live_n, 'ended': ended_n,
        'no_cart': len(results.get('no_cart', [])),
        'checked': len(candidates),
        'live_ids': results['live'][:5],
        'ended_ids': results['ended'][:5],
        'no_cart_ids': results.get('no_cart', [])[:5],
    }))
