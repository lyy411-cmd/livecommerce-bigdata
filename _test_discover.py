# -*- coding: utf-8 -*-
"""
Douyin Live Directory Discovery Test Script
Tests multiple methods to find currently-live rooms from Douyin's category pages.
"""

import sys
import os
import json
import re
import asyncio
import time

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- Config ---
BROWSER_PROFILE = r"C:\Users\MECHREVO\.qoderworkcn\douyin_browser_profile"
TIMEOUT = 60
SPA_LOAD_WAIT = 10

# Collected data
intercepted_responses = []
link_rooms = set()
ssr_rooms = set()
api_rooms = set()
all_rooms = []


async def on_response(response):
    """Intercept network responses looking for live room data."""
    url = response.url
    if any(kw in url for kw in ['webcast', 'feed', 'partition', 'room_list', 'live_room']):
        try:
            ct = response.headers.get('content-type', '')
            if 'json' in ct or 'text' in ct:
                body = await response.text()
                intercepted_responses.append({
                    'url': url[:200],
                    'status': response.status,
                    'body_len': len(body),
                    'body_preview': body[:500]
                })
                # Try to parse JSON and extract rooms
                try:
                    data = json.loads(body)
                    extract_rooms_from_json(data, 'intercepted')
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            intercepted_responses.append({
                'url': url[:200],
                'status': response.status,
                'error': str(e)
            })


def extract_rooms_from_json(data, source='unknown', depth=0):
    """Recursively search JSON for room-like objects."""
    if depth > 10:
        return
    if isinstance(data, dict):
        has_room_keys = any(k in data for k in ['web_rid', 'room_id', 'title', 'alive_info'])
        if has_room_keys and ('web_rid' in data or 'room_id' in data or 'id_str' in data):
            info = {
                'source': source,
                'web_rid': str(data.get('web_rid', data.get('webRid', ''))),
                'room_id': str(data.get('room_id', data.get('roomId', data.get('id_str', '')))),
                'title': data.get('title', data.get('room_name', '')),
                'nickname': '',
                'user_count': data.get('user_count', data.get('userCount', '')),
            }
            owner = data.get('owner', {})
            if isinstance(owner, dict):
                info['nickname'] = owner.get('nickname', owner.get('nick_name', ''))
            existing_ids = {str(r.get('web_rid', '') or r.get('room_id', '')) for r in all_rooms}
            rid = str(info.get('web_rid', '') or info.get('room_id', ''))
            if rid and rid not in existing_ids:
                all_rooms.append(info)
        for v in data.values():
            if isinstance(v, (dict, list)):
                extract_rooms_from_json(v, source, depth + 1)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                extract_rooms_from_json(item, source, depth + 1)


async def method_intercepted_responses(page):
    """Method A: Print intercepted network responses."""
    sep = '=' * 60
    print(f"\n{sep}\nMETHOD A: Intercepted Network Responses\n{sep}")
    print(f"Total intercepted relevant responses: {len(intercepted_responses)}")
    for i, resp in enumerate(intercepted_responses):
        print(f"\n--- Response #{i+1} ---")
        print(f"  URL: {resp.get('url', 'N/A')}")
        print(f"  Status: {resp.get('status', 'N/A')}")
        if 'error' in resp:
            print(f"  Error: {resp['error']}")
        else:
            print(f"  Body length: {resp.get('body_len', 0)}")
            print(f"  Body preview: {resp.get('body_preview', '')[:300]}")


async def method_js_links(page):
    """Method B: Find <a> elements with href matching live room URLs."""
    global link_rooms
    sep = '=' * 60
    print(f"\n{sep}\nMETHOD B: JS - Find <a> links matching live.douyin.com/digits\n{sep}")
    try:
        result = await page.evaluate("""
            () => {
                const links = Array.from(document.querySelectorAll('a[href]'));
                const roomLinks = links.filter(a => /live\\.douyin\\.com\\/\\d+/.test(a.href));
                return roomLinks.map(a => ({
                    href: a.href,
                    text: (a.textContent || '').trim().substring(0, 100),
                    title: a.title || ''
                }));
            }
        """)
        print(f"Found {len(result)} matching links")
        for link in result:
            print(f"  href={link['href']}  text={link['text'][:60]}  title={link['title'][:40]}")
            m = re.search(r'live\.douyin\.com/(\d+)', link['href'])
            if m:
                link_rooms.add(m.group(1))
        print(f"Unique web_rids from links: {link_rooms}")
    except Exception as e:
        print(f"  Error: {e}")


async def method_ssr_data(page):
    """Method C: Extract from window.__pace_f (SSR data)."""
    global ssr_rooms
    sep = '=' * 60
    print(f"\n{sep}\nMETHOD C: JS - Extract from window.__pace_f (SSR)\n{sep}")
    try:
        has_pace = await page.evaluate("typeof window.__pace_f !== 'undefined'")
        print(f"window.__pace_f exists: {has_pace}")

        if has_pace:
            pace_data = await page.evaluate("""
                () => {
                    try {
                        const f = window.__pace_f;
                        if (Array.isArray(f)) {
                            return f.slice(0, 10).map(item => {
                                if (Array.isArray(item) && item.length >= 2) {
                                    return { idx: item[0], len: String(item[1]).length, preview: String(item[1]).substring(0, 200) };
                                }
                                return { type: typeof item, preview: String(item).substring(0, 200) };
                            });
                        }
                        return { type: typeof f, keys: Object.keys(f || {}).slice(0, 20) };
                    } catch(e) {
                        return { error: e.message };
                    }
                }
            """)
            print(f"__pace_f structure: {json.dumps(pace_data, ensure_ascii=False)[:1000]}")

            # Try to extract room data from __pace_f
            rooms_from_ssr = await page.evaluate("""
                () => {
                    const rooms = [];
                    try {
                        const f = window.__pace_f;
                        if (!Array.isArray(f)) return rooms;
                        for (const item of f) {
                            if (!Array.isArray(item) || item.length < 2) continue;
                            const str = String(item[1]);
                            const webRidMatches = str.match(/"web_rid"\\s*:\\s*"(\\d+)"/g);
                            if (webRidMatches) {
                                for (const m of webRidMatches) {
                                    const rid = m.match(/(\\d+)/);
                                    if (rid) rooms.push({web_rid: rid[1]});
                                }
                            }
                            const roomIdMatches = str.match(/"room_id"\\s*:\\s*"?(\\d+)"?/g);
                            if (roomIdMatches) {
                                for (const m of roomIdMatches) {
                                    const rid = m.match(/(\\d+)/);
                                    if (rid) rooms.push({room_id: rid[1]});
                                }
                            }
                        }
                    } catch(e) {}
                    return rooms;
                }
            """)
            print(f"Rooms found in __pace_f: {len(rooms_from_ssr)}")
            for r in rooms_from_ssr:
                print(f"  {r}")
                rid = r.get('web_rid') or r.get('room_id', '')
                if rid:
                    ssr_rooms.add(rid)
            print(f"Unique room IDs from SSR: {ssr_rooms}")
    except Exception as e:
        print(f"  Error: {e}")

    # Also check other common SSR data locations
    print("\n  Checking other SSR data locations...")
    for var_name in ['__NEXT_DATA__', '__NUXT__', '__INITIAL_STATE__', '__SSR_DATA__']:
        try:
            exists = await page.evaluate(f"typeof {var_name} !== 'undefined'")
            print(f"  {var_name} exists: {exists}")
        except:
            pass


async def method_direct_api(page):
    """Method D: Call Douyin's API directly via page.evaluate (fetch)."""
    sep = '=' * 60
    print(f"\n{sep}\nMETHOD D: Direct API call via fetch()\n{sep}")

    api_url = "https://live.douyin.com/webcast/web/partition/detail/room/?aid=6383&app_name=douyin_web&live_id=1&device_platform=web&partition=72c5a010024e1001&partition_type=1&req_from=enter_from_merge&count=20&offset=0"

    try:
        result = await page.evaluate("""
            async (url) => {
                try {
                    const resp = await fetch(url, {
                        credentials: 'include',
                        headers: {
                            'Accept': 'application/json',
                            'Referer': 'https://live.douyin.com/'
                        }
                    });
                    const text = await resp.text();
                    return { status: resp.status, body: text.substring(0, 3000), body_len: text.length };
                } catch(e) {
                    return { error: e.message };
                }
            }
        """, api_url)

        print(f"API Response status: {result.get('status', 'N/A')}")
        print(f"API Response body_len: {result.get('body_len', 0)}")
        print(f"API Response body: {result.get('body', '')[:1500]}")

        if result.get('error'):
            print(f"API Error: {result['error']}")

        # Try to parse and extract rooms
        try:
            data = json.loads(result.get('body', '{}'))
            print(f"\nParsed JSON keys: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}")

            if isinstance(data, dict):
                status_code = data.get('status_code', data.get('status', ''))
                print(f"status_code: {status_code}")

                room_data = data.get('data', {})
                if isinstance(room_data, dict):
                    print(f"data keys: {list(room_data.keys())}")
                    rooms = room_data.get('data', room_data.get('room_list', room_data.get('rooms', [])))
                    if isinstance(rooms, list):
                        print(f"Found {len(rooms)} rooms in API response")
                        for room in rooms:
                            if isinstance(room, dict):
                                info = {
                                    'source': 'api',
                                    'web_rid': str(room.get('web_rid', '')),
                                    'room_id': str(room.get('id_str', room.get('room_id', ''))),
                                    'title': room.get('title', ''),
                                    'nickname': '',
                                    'user_count': '',
                                }
                                owner = room.get('owner', {})
                                if isinstance(owner, dict):
                                    info['nickname'] = owner.get('nickname', '')
                                stats = room.get('stats', {})
                                if isinstance(stats, dict):
                                    info['user_count'] = stats.get('total_user_desp', '')
                                all_rooms.append(info)
                                api_rooms.add(str(info['web_rid'] or info['room_id']))
                                print(f"  [{info['web_rid']}] {info['title'][:40]} by {info['nickname']}")
                elif isinstance(room_data, list):
                    print(f"data is a list with {len(room_data)} items")
        except (json.JSONDecodeError, TypeError) as e:
            print(f"JSON parse error: {e}")

    except Exception as e:
        print(f"  Error: {e}")


async def method_page_analysis(page):
    """Extra: Analyze page DOM for any room-like elements."""
    sep = '=' * 60
    print(f"\n{sep}\nEXTRA: DOM Analysis for room elements\n{sep}")

    try:
        analysis = await page.evaluate("""
            () => {
                const result = {};
                result.allLinks = document.querySelectorAll('a').length;
                result.imgCount = document.querySelectorAll('img').length;
                const dataEls = document.querySelectorAll('[data-room-id], [data-web-rid], [data-roomid]');
                result.dataAttrElements = dataEls.length;
                result.dataAttrDetails = Array.from(dataEls).slice(0, 10).map(el => ({
                    tag: el.tagName,
                    roomId: el.dataset.roomId || el.dataset.roomid || '',
                    webRid: el.dataset.webRid || el.dataset.webrid || '',
                    text: (el.textContent || '').trim().substring(0, 50)
                }));
                const allText = document.body.innerText || '';
                result.pageTextLength = allText.length;
                result.pageTextPreview = allText.substring(0, 800);
                result.hasReactRoot = !!document.getElementById('root') || !!document.querySelector('[data-reactroot]');
                result.hasVueApp = !!document.querySelector('[data-v-app]') || !!document.querySelector('#app');
                const scripts = document.querySelectorAll('script');
                let jsonScripts = 0;
                for (const s of scripts) {
                    if (s.textContent && s.textContent.includes('web_rid')) jsonScripts++;
                }
                result.scriptsWithRoomData = jsonScripts;
                result.title = document.title;
                result.url = window.location.href;
                return result;
            }
        """)
        print(f"Page URL: {analysis.get('url', 'N/A')}")
        print(f"Page title: {analysis.get('title', 'N/A')}")
        print(f"Total links: {analysis.get('allLinks', 0)}")
        print(f"Total images: {analysis.get('imgCount', 0)}")
        print(f"Data-attr elements: {analysis.get('dataAttrElements', 0)}")
        for d in analysis.get('dataAttrDetails', []):
            print(f"  {d}")
        print(f"Has React root: {analysis.get('hasReactRoot', False)}")
        print(f"Has Vue app: {analysis.get('hasVueApp', False)}")
        print(f"Scripts with room data: {analysis.get('scriptsWithRoomData', 0)}")
        print(f"Page text length: {analysis.get('pageTextLength', 0)}")
        print(f"Page text preview:\n{analysis.get('pageTextPreview', '')}")
    except Exception as e:
        print(f"  Error: {e}")


async def method_cookies(page):
    """Print current cookies for debugging."""
    sep = '=' * 60
    print(f"\n{sep}\nCOOKIES (for debugging)\n{sep}")
    try:
        cookies = await page.context.cookies()
        print(f"Total cookies: {len(cookies)}")
        for c in cookies:
            val = c['value'][:30]
            print(f"  {c['name']}={val}... (domain={c['domain']})")
    except Exception as e:
        print(f"  Error: {e}")


async def run_all_methods(page, label):
    """Run all discovery methods on the current page."""
    sep = '#' * 70
    print(f"\n{sep}\n# RUNNING ALL METHODS FOR: {label}\n{sep}")
    await method_js_links(page)
    await method_ssr_data(page)
    await method_direct_api(page)
    await method_intercepted_responses(page)
    await method_page_analysis(page)


async def main():
    start_time = time.time()

    print("=" * 70)
    print("DOUYIN LIVE DIRECTORY DISCOVERY TEST")
    print("=" * 70)
    print(f"Browser profile: {BROWSER_PROFILE}")

    async with async_playwright() as p:
        # Launch with persistent context
        context = await p.chromium.launch_persistent_context(
            user_data_dir=BROWSER_PROFILE,
            headless=False,
            args=[
                '--window-position=9999,9999',
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ],
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            locale='zh-CN',
        )

        # Apply stealth
        await Stealth().apply_stealth_async(context)

        page = context.pages[0] if context.pages else await context.new_page()

        # Set up response interception
        page.on('response', on_response)

        # --- Step 1: Navigate to live.douyin.com to establish session ---
        print("\n[Step 1] Navigating to https://live.douyin.com ...")
        try:
            await page.goto('https://live.douyin.com', wait_until='domcontentloaded', timeout=30000)
            print(f"  Page loaded. Title: {await page.title()}")
            print(f"  URL: {page.url}")
        except Exception as e:
            print(f"  Navigation error (may be OK for SPA): {e}")

        print(f"\n[Step 1b] Waiting {SPA_LOAD_WAIT}s for SPA to load...")
        await asyncio.sleep(SPA_LOAD_WAIT)

        # Print cookies after initial load
        await method_cookies(page)

        # --- Step 2: Navigate to category page ---
        print(f"\n[Step 2] Navigating to https://live.douyin.com/category/100102 ...")
        intercepted_responses.clear()
        all_rooms.clear()

        try:
            await page.goto('https://live.douyin.com/category/100102', wait_until='domcontentloaded', timeout=30000)
            print(f"  Page loaded. Title: {await page.title()}")
            print(f"  URL: {page.url}")
        except Exception as e:
            print(f"  Navigation error (may be OK for SPA): {e}")

        print(f"\n[Step 2b] Waiting {SPA_LOAD_WAIT}s for category page SPA to load...")
        await asyncio.sleep(SPA_LOAD_WAIT)

        # Scroll down to trigger lazy loading
        print("\n[Step 2c] Scrolling page to trigger lazy loading...")
        try:
            for i in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await asyncio.sleep(1)
                print(f"  Scroll #{i+1} done")
        except Exception as e:
            print(f"  Scroll error: {e}")

        # --- Run all discovery methods ---
        await run_all_methods(page, "Category 100102")

        # --- Summary ---
        elapsed = time.time() - start_time
        sep = '=' * 70
        print(f"\n{sep}\nSUMMARY\n{sep}")
        print(f"Elapsed time: {elapsed:.1f}s")
        print(f"Total unique rooms found (all methods): {len(all_rooms)}")
        print(f"  From JS links (web_rids): {len(link_rooms)} -> {link_rooms}")
        print(f"  From SSR data: {len(ssr_rooms)} -> {ssr_rooms}")
        print(f"  From direct API: {len(api_rooms)} -> {api_rooms}")

        print(f"\nAll rooms detail:")
        for i, room in enumerate(all_rooms):
            print(f"  #{i+1} web_rid={room.get('web_rid','')} room_id={room.get('room_id','')} "
                  f"title={room.get('title','')[:50]} nick={room.get('nickname','')} "
                  f"users={room.get('user_count','')} source={room.get('source','')}")

        # --- Close ---
        print(f"\nClosing browser...")
        await context.close()
        print("Done.")


if __name__ == '__main__':
    asyncio.run(main())
