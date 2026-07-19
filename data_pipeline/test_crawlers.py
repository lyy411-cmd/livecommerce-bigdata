#!/usr/bin/env python3
"""
Crawler Diagnostic Tool
========================
Tests each platform crawler independently.
Shows detailed error info when a crawler fails.

Usage:
  python -m data_pipeline.test_crawlers              # Test all
  python -m data_pipeline.test_crawlers douyin        # Douyin only
  python -m data_pipeline.test_crawlers kuaishou      # Kuaishou only
"""

import asyncio
import sys
import time
import traceback

# Force UTF-8 output
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")


async def test_single_platform(platform):
    """Test one platform and report results."""
    print()
    print("=" * 60)
    print(f"  Testing: {platform.upper()}")
    print("=" * 60)

    start = time.time()
    crawler = None
    rooms = []

    try:
        # Step 1: Import crawler
        print(f"  [1/4] Importing {platform} crawler module...")
        if platform == "douyin":
            from data_pipeline.douyin_crawler import DouyinLiveCrawler
            crawler = DouyinLiveCrawler()
        elif platform == "kuaishou":
            from data_pipeline.kuaishou_crawler import KuaishouLiveCrawler
            crawler = KuaishouLiveCrawler()
        else:
            print(f"  [FAIL] Unknown platform: {platform}")
            return False
        print("  [OK] Module imported successfully")

        # Step 2: Init browser
        print("  [2/4] Launching Playwright browser...")
        await crawler.init_browser()
        print("  [OK] Browser launched")

        # Step 3: Discover rooms
        print("  [3/4] Discovering live rooms (limit=20)...")
        rooms = await crawler.discover_live_rooms(limit=20)
        elapsed = time.time() - start

        print(f"  [OK] Found {len(rooms)} rooms in {elapsed:.1f}s")

        # Step 4: Show results
        print("  [4/4] Results:")
        if not rooms:
            print("  No rooms found. Possible reasons:")
            print("    - Off-peak hours (best: 19:00-23:00)")
            print("    - Anti-bot detection blocked the crawler")
            if platform == "kuaishou":
                print("    - Kuaishou search URLs return 404 (platform limitation)")
                print("    - Homepage mostly shows game streams")
        else:
            for i, r in enumerate(rooms[:5], 1):
                name = r.get("anchor_name", r.get("anchorNick", "?"))
                room = r.get("room_name", r.get("title", "?"))
                url = r.get("live_url", "")
                src = r.get("source", "?")
                print(f"    {i}. {name[:20]} | {room[:30]} | src={src}")
                print(f"       URL: {url[:80]}")
            if len(rooms) > 5:
                print(f"    ... and {len(rooms)-5} more")

        await crawler.close()
        status = "PASS" if rooms else "NO ROOMS"
        print(f"  RESULT: {platform.upper()} - {status} ({len(rooms)} rooms)")
        return True

    except Exception as e:
        elapsed = time.time() - start
        print(f"  [FAIL] {platform.upper()} crashed after {elapsed:.1f}s")
        print(f"  Error type: {type(e).__name__}")
        print(f"  Error message: {e}")
        print("  Full traceback:")
        traceback.print_exc()

        err_str = str(e).lower()
        if "chrome" in err_str or "channel" in err_str:
            print("  HINT: Chrome not installed. Run: python -m playwright install chromium")
        elif "playwright" in err_str and "install" in err_str:
            print("  HINT: Playwright not installed. Run: pip install playwright")
        elif "timeout" in err_str:
            print("  HINT: Network timeout. Check internet and try again.")
        elif "login" in err_str or "session" in err_str:
            print("  HINT: Login required. Watch browser window and complete login.")
        elif "connect" in err_str or "refused" in err_str:
            print("  HINT: Connection refused. Check if website is accessible.")

        if crawler:
            try:
                await crawler.close()
            except Exception:
                pass
        return False


async def main():
    platforms = sys.argv[1:] if len(sys.argv) > 1 else ["douyin", "kuaishou"]

    print()
    print("  =====================================================")
    print("  Crawler Diagnostic Tool")
    print("  =====================================================")
    plat_str = ", ".join(p.upper() for p in platforms)
    print(f"  Platforms to test: {plat_str}")
    print("  Playwright check: python -m playwright install chromium")
    print()

    try:
        from playwright.async_api import async_playwright
        print("  [OK] playwright package is installed")
    except ImportError:
        print("  [FAIL] playwright is NOT installed!")
        print("  Fix: pip install playwright")
        print("  Then: python -m playwright install chromium")
        return

    results = {}
    for p in platforms:
        ok = await test_single_platform(p.lower())
        results[p] = ok

    print()
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for p, ok in results.items():
        status = "PASS" if ok else "FAIL"
        print(f"  {p.upper():12s} : {status}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
