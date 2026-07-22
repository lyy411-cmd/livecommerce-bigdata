# -*- coding: utf-8 -*-
"""Active refresh: Playwright verify all live rooms, mark ended as finished."""
import sys, os, json, asyncio, tempfile, shutil, time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

MYSQL_HOST = "192.168.104.100"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PWD = "123456"
MYSQL_DB = "livecommerce_db"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
SIGNAL_FILE = os.path.join(PROJECT_DIR, ".refresh_restart.flag")
COOKIE_FILE = os.path.join(PROJECT_DIR, "data_pipeline", "cookies", "douyin_cookies.json")
BATCH_SIZE = 8
WAIT_MS = 5000


def get_live_rooms():
    import pymysql
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PWD, database=MYSQL_DB, charset="utf8mb4", connect_timeout=10)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT room_id_external, room_name, anchor_name FROM live_room "
                "WHERE status='live' AND deleted=0 AND data_source='real' "
                "AND room_id_external IS NOT NULL AND room_id_external != '' "
                "ORDER BY viewer_count DESC")
    rooms = cur.fetchall()
    cur.close(); conn.close()
    return rooms


async def verify_rooms(candidates):
    """Visit each room on Douyin with Playwright, classify as live/ended/no_cart."""
    from playwright.async_api import async_playwright
    results = {"live": [], "ended": [], "no_cart": []}
    async with async_playwright() as p:
        tmp = tempfile.mkdtemp(prefix="refresh_verify_")
        try:
            browser = await p.chromium.launch(
                headless=True, channel="chrome",
                args=["--disable-blink-features=AutomationControlled",
                       "--headless=new", "--disk-cache-size=1", "--no-sandbox"])
            ctx = await browser.new_context(ignore_https_errors=True)
            try:
                with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                    cookies = json.load(f)
                await ctx.add_cookies(cookies)
            except Exception:
                pass
            total = len(candidates)
            for i in range(0, total, BATCH_SIZE):
                batch = candidates[i:i + BATCH_SIZE]
                bn = i // BATCH_SIZE + 1
                tb = (total + BATCH_SIZE - 1) // BATCH_SIZE
                print(f"  Verifying batch {bn}/{tb} ({len(batch)} rooms)...", flush=True)

                async def check_one(room, ctx=ctx):
                    page = await ctx.new_page()
                    try:
                        rid = str(room.get("room_id_external", ""))
                        await page.goto(f"https://live.douyin.com/{rid}",
                            wait_until="domcontentloaded", timeout=25000)
                        await page.wait_for_timeout(WAIT_MS)
                        body = await page.evaluate("document.body?.innerText || ''")
                        if "\u5df2\u7ed3\u675f" in body or "\u76f4\u64ad\u5df2\u7ed3\u675f" in body:
                            return rid, "ended"
                        has_video = await page.evaluate(
                            "(() => { const v = document.querySelector('video'); "
                            "if (!v) return false; "
                            "if (v.readyState >= 2 && !v.paused) return true; "
                            "if (v.src && v.readyState > 0) return true; "
                            "return false; })()")
                        if not has_video and "\u76f4\u64ad\u4e2d" not in body:
                            return rid, "ended"
                        has_cart = await page.evaluate(r"""(() => {
                            var bt = document.body ? document.body.innerText : '';
                            if (/\u8d2d\u7269\u8f66|\u53bb\u8d2d\u7269|\u6b63\u5728\u5356|\u5546\u54c1|\u4e0b\u5355|\u5c0f\u9ec4\u8f66|\u8bb2\u89e3\u4e2d/.test(bt)) return true;
                            if (/\u798f\u5229|\u79d2\u6740|\u62a2\u8d2d|\u9650\u65f6|\u70b9\u51fb\u8d2d/.test(bt)) return true;
                            try {
                                var ch = window.__pace_f || [];
                                for (var c = 0; c < ch.length; c++) {
                                    var s = JSON.stringify(ch[c]);
                                    if (s.includes('ShoppingCart') || s.includes('shopping_cart')
                                        || s.includes('productList') || s.includes('buyin')) return true;
                                }
                            } catch(e) {}
                            return false;
                        })()""")
                        if has_cart:
                            return rid, "live"
                        else:
                            return rid, "no_cart"
                    except Exception:
                        return str(room.get("room_id_external", "")), "ended"
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
    """Mark ended rooms as finished, confirm live rooms."""
    import pymysql
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PWD, database=MYSQL_DB, charset="utf8mb4", connect_timeout=10)
    cur = conn.cursor()
    ended_count = 0
    live_count = 0
    if results["ended"]:
        ph = ",".join(["%s"] * len(results["ended"]))
        cur.execute(f"UPDATE live_room SET status='finished' WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results["ended"])
        ended_count = cur.rowcount
        cur.execute(f"UPDATE rt_room_stats SET status='finished' WHERE room_id IN ({ph})",
                    results["ended"])
    if results["no_cart"]:
        ph = ",".join(["%s"] * len(results["no_cart"]))
        cur.execute(f"UPDATE live_room SET status='finished' WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results["no_cart"])
        ended_count += cur.rowcount
    if results["live"]:
        ph = ",".join(["%s"] * len(results["live"]))
        cur.execute(f"UPDATE live_room SET status='live', has_shopping_cart=1 WHERE room_id_external IN ({ph}) AND data_source='real'",
                    results["live"])
        live_count = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    return live_count, ended_count


def main():
    from datetime import datetime
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Refreshing live rooms (active Playwright verify)...', flush=True)
    candidates = get_live_rooms()
    live_count = len(candidates)
    print(f"  Found {live_count} rooms marked as live in DB", flush=True)
    if not candidates:
        print("  No live rooms to verify.", flush=True)
        with open(SIGNAL_FILE, "w") as f:
            f.write(str(time.time()))
        return
    print(f"  Launching Playwright to verify {live_count} rooms (batches of {BATCH_SIZE})...", flush=True)
    start_time = time.time()
    results = asyncio.run(verify_rooms(candidates))
    elapsed = time.time() - start_time
    print(f"  Verification done in {elapsed:.1f}s:", flush=True)
    print(f"    Still live (with cart): {len(results['live'])}", flush=True)
    print(f"    Ended: {len(results['ended'])}", flush=True)
    print(f"    No cart: {len(results['no_cart'])}", flush=True)
    confirmed_live, marked_finished = update_db(results)
    print(f"  DB updated: {confirmed_live} confirmed live, {marked_finished} marked finished", flush=True)
    with open(SIGNAL_FILE, "w") as f:
        f.write(str(time.time()))
    print(f"  Restart signal sent - cluster will re-verify within ~30s", flush=True)
    import pymysql
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PWD, database=MYSQL_DB, charset="utf8mb4", connect_timeout=10)
    cur = conn.cursor()
    cur.execute("UPDATE live_room SET status='checking' WHERE status='finished' AND deleted=0 "
                "AND room_id_external IS NOT NULL AND room_id_external != '' "
                "ORDER BY viewer_count DESC LIMIT 500")
    re_count = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    print(f"  Reset {re_count} finished rooms for re-check", flush=True)
    print(f"\n[Done] {confirmed_live} rooms still live, {marked_finished} removed.", flush=True)


if __name__ == "__main__":
    main()
