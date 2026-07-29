# -*- coding: utf-8 -*-
"""
commerce_discovery.py - 带货直播间发现与验证服务

通过CDP连接Chrome浏览器，爬取抖音分类页面发现直播间，
调用抖音enter API验证每个房间的真实状态，
筛选出正在直播且有购物车的带货直播间写入MySQL。

用法:
    python commerce_discovery.py

依赖:
    - Chrome 125 运行在 CDP 9225 端口（已登录抖音）
    - MySQL 192.168.104.100:3306
"""
import json
import time
import threading
import logging
import sys
import traceback
from urllib import request as urllib_request
from urllib.error import URLError
from datetime import datetime

import websocket  # pip install websocket-client
import pymysql

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ============================================================================
# 配置
# ============================================================================
CDP_HOST = "127.0.0.1"
CDP_PORT = 9225
MYSQL_HOST = "192.168.104.100"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "123456"
MYSQL_DB = "livecommerce_db"

DISCOVERY_INTERVAL = 300        # 5分钟一轮
PAGE_WAIT_SECONDS = 6           # 等待页面加载
SCROLL_ROUNDS = 8               # 每个分类页滚动次数
SCROLL_DELAY = 0.8              # 滚动间隔（秒）
VERIFY_BATCH_SIZE = 8           # API验证批次
VERIFY_DELAY = 0.15             # 批次间延迟
MAX_VERIFY_PER_ROUND = 500      # 每轮最多验证数
MAX_COMMERCE_TARGET = 150       # 目标找到数量
STALE_MINUTES = 20              # 超过此时间未验证则标记结束

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger("CommerceDiscovery")

# ============================================================================
# 分类页URL列表（20+个页面，覆盖所有带货热门分类）
# ============================================================================
CATEGORY_URLS = [
    # 带货/电商专区
    "https://live.douyin.com/category/100109",
    "https://live.douyin.com/category/100110",
    # 热门带货品类
    "https://live.douyin.com/category/100102",   # 美食
    "https://live.douyin.com/category/100101",   # 娱乐
    "https://live.douyin.com/category/100106",   # 美妆
    "https://live.douyin.com/category/100103",   # 服饰
    "https://live.douyin.com/category/100105",   # 珠宝
    "https://live.douyin.com/category/100107",   # 数码
    "https://live.douyin.com/category/100108",   # 家居
    "https://live.douyin.com/category/100104",   # 体育
    "https://live.douyin.com/category/100111",   # 教育
    "https://live.douyin.com/category/100112",   # 音乐
    "https://live.douyin.com/category/100113",   # 生活
    "https://live.douyin.com/category/100114",   # 旅游
    "https://live.douyin.com/category/100115",   # 汽车
    "https://live.douyin.com/category/100116",   # 二次元
    "https://live.douyin.com/category/100117",   # 亲子
    "https://live.douyin.com/category/100118",   # 健康
    "https://live.douyin.com/category/100119",   # 财经
    "https://live.douyin.com/category/100120",   # 科技
    # 首页推荐
    "https://live.douyin.com",
]

# ============================================================================
# CDP 操作封装
# ============================================================================
_cdp_msg_id = 0
_cdp_lock = threading.Lock()


def _next_id():
    global _cdp_msg_id
    with _cdp_lock:
        _cdp_msg_id += 1
        return _cdp_msg_id


def cdp_get_targets():
    """获取Chrome所有tab目标"""
    try:
        resp = urllib_request.urlopen(f"http://{CDP_HOST}:{CDP_PORT}/json", timeout=3)
        return json.loads(resp.read().decode())
    except Exception as e:
        log.error(f"CDP连接失败: {e}")
        return []


def cdp_create_target(url="about:blank"):
    """创建新tab"""
    try:
        resp = urllib_request.urlopen(
            f"http://{CDP_HOST}:{CDP_PORT}/json/new?{url}", timeout=5
        )
        return json.loads(resp.read().decode())
    except Exception as e:
        log.error(f"创建tab失败: {e}")
        return None


def cdp_close_target(target_id):
    """关闭tab"""
    try:
        urllib_request.urlopen(
            f"http://{CDP_HOST}:{CDP_PORT}/json/close/{target_id}", timeout=3
        )
    except:
        pass


class CDPSession:
    """单个CDP WebSocket会话"""

    def __init__(self, ws_url, timeout=15):
        self.ws = websocket.create_connection(ws_url, timeout=timeout)
        self._responses = {}
        self._events = []
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        # 启用必要域
        self.send("Page.enable")
        self.send("Runtime.enable")
        time.sleep(0.3)

    def _read_loop(self):
        while True:
            try:
                data = self.ws.recv()
                msg = json.loads(data)
                msg_id = msg.get("id")
                if msg_id is not None:
                    with self._lock:
                        self._responses[msg_id] = msg
                elif "method" in msg:
                    with self._lock:
                        self._events.append(msg)
            except:
                break

    def send(self, method, params=None, timeout=15):
        mid = _next_id()
        msg = {"id": mid, "method": method}
        if params:
            msg["params"] = params
        self.ws.send(json.dumps(msg))
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if mid in self._responses:
                    resp = self._responses.pop(mid)
                    return resp.get("result", {})
            time.sleep(0.05)
        return {}

    def navigate(self, url, wait=PAGE_WAIT_SECONDS):
        """导航到URL并等待页面加载"""
        self.send("Page.navigate", {"url": url})
        time.sleep(wait)

    def evaluate(self, expression, timeout=15):
        """执行JavaScript并返回结果"""
        result = self.send("Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
            "timeout": timeout * 1000,
        }, timeout=timeout + 2)
        rv = result.get("result", {})
        if rv.get("type") == "object" and "value" in rv:
            return rv["value"]
        return rv.get("value", None)

    def close(self):
        try:
            self.ws.close()
        except:
            pass


# ============================================================================
# JavaScript 提取/验证脚本
# ============================================================================

# 从分类页面提取所有 web_rid
JS_EXTRACT_RIDS = r"""(function() {
    var rids = {};
    document.querySelectorAll('a[href]').forEach(function(a) {
        var m = a.href.match(/live\.douyin\.com\/(\d{3,15})/);
        if (m && m[1].length >= 6) rids[m[1]] = 1;
    });
    try {
        var chunks = window.__pace_f || [];
        for (var i = 0; i < chunks.length; i++) {
            var s = JSON.stringify(chunks[i]);
            var re = /"web_rid"\s*:\s*"(\d{6,15})"/g;
            var m2;
            while ((m2 = re.exec(s)) !== null) rids[m2[1]] = 1;
        }
    } catch(e) {}
    document.querySelectorAll('script').forEach(function(s) {
        var text = s.textContent || '';
        var re2 = /"web_rid"\s*:\s*"(\d{6,15})"/g;
        var m3;
        while ((m3 = re2.exec(text)) !== null) rids[m3[1]] = 1;
    });
    return Object.keys(rids);
})()"""

# 批量验证（一次验证多个，减少CDP往返）
JS_VERIFY_BATCH = r"""(async function(rids) {
    var results = [];
    for (var i = 0; i < rids.length; i++) {
        try {
            var webRid = rids[i];
            var url = 'https://live.douyin.com/webcast/room/web/enter/?aid=6383'
                + '&app_name=douyin_web&live_id=1&device_platform=web'
                + '&enter_from=web_live&web_rid=' + webRid;
            var signUrl = url;
            if (typeof window.byted_acrawler !== 'undefined'
                && typeof window.byted_acrawler.frontierSign === 'function') {
                var signed = await window.byted_acrawler.frontierSign(url);
                if (signed && signed['X-Bogus']) {
                    signUrl = url + '&X-Bogus=' + signed['X-Bogus'];
                }
            }
            var resp = await fetch(signUrl, {credentials: 'include'});
            var json = await resp.json();
            var data = json.data && json.data.data;
            if (!data || !data.length) { results.push({error: 'no_data', web_rid: webRid}); continue; }
            var room = data[0];
            results.push({
                web_rid: webRid,
                id_str: room.id_str || '',
                status: room.status,
                title: room.title || '',
                has_commerce_goods: !!(room.has_commerce_goods),
                user_count_str: room.user_count_str || '0',
                nickname: (room.owner && room.owner.nickname) || '',
                cover_url: (room.cover && room.cover.url_list && room.cover.url_list[0]) || '',
            });
        } catch(e) {
            results.push({error: e.message, web_rid: rids[i]});
        }
        await new Promise(function(r) { setTimeout(r, 200); });
    }
    return results;
})(__RIDS_PLACEHOLDER__)"""

# 检查 frontierSign 是否可用
JS_CHECK_FS = """(function() {
    return typeof window.byted_acrawler !== 'undefined'
        && typeof window.byted_acrawler.frontierSign === 'function';
})()"""


# ============================================================================
# MySQL 操作
# ============================================================================
def get_db():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASS, database=MYSQL_DB, charset="utf8mb4",
        connect_timeout=5, autocommit=True,
    )


def write_live_rooms(rooms):
    """将验证通过的带货直播间写入MySQL（UPSERT）"""
    if not rooms:
        return 0
    conn = get_db()
    cur = conn.cursor()
    sql = """
        INSERT INTO live_room
            (room_no, room_name, anchor_name, platform, category, status,
             viewer_count, start_time, create_time, live_url, room_id_external,
             data_source, has_shopping_cart, cover_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            room_name = VALUES(room_name),
            anchor_name = VALUES(anchor_name),
            status = 'live',
            viewer_count = VALUES(viewer_count),
            data_source = 'real',
            has_shopping_cart = 1,
            cover_url = VALUES(cover_url),
            start_time = IF(status != 'live', NOW(), start_time)
    """
    count = 0
    for r in rooms:
        web_rid = r.get("web_rid", "")
        if not web_rid:
            continue
        room_no = f"CRAWL_DOUYIN_{web_rid}"
        viewer = parse_viewer_count(r.get("user_count_str", "0"))
        cur.execute(sql, (
            room_no,
            r.get("title", "")[:100],
            r.get("nickname", "")[:50],
            "douyin",
            "电商带货",
            "live",
            viewer,
            f"https://live.douyin.com/{web_rid}",
            web_rid,
            "real",
            1,
            r.get("cover_url", "")[:500],
        ))
        count += 1
    conn.close()
    return count


def mark_stale_as_finished(minutes=STALE_MINUTES):
    """将已验证过但超时的'live'房间标记为'finished'。
    重要：只动 last_verified 不为空的房间（即已被本轮验证过但超时的）。
    last_verified IS NULL 的房间是未经验证的存量数据，绝不触碰。
    也绝不碰 data_source != 'real' 的房间。
    """
    conn = get_db()
    cur = conn.cursor()
    affected = cur.execute(
        "UPDATE live_room SET status='finished' "
        "WHERE status='live' AND data_source='real' "
        "AND last_verified IS NOT NULL "
        "AND last_verified < NOW() - INTERVAL %s MINUTE",
        (minutes,)
    )
    conn.close()
    return affected


def update_last_verified(web_rids):
    """更新最后验证时间"""
    if not web_rids:
        return
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE live_room ADD COLUMN last_verified DATETIME")
    except:
        pass
    for rid in web_rids:
        cur.execute(
            "UPDATE live_room SET last_verified=NOW() WHERE room_id_external=%s",
            (rid,)
        )
    conn.close()


def get_existing_finished_count():
    """获取已结束房间数量（用于验证不影响）"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM live_room WHERE status='finished'")
    count = cur.fetchone()[0]
    conn.close()
    return count


def get_live_rooms_count():
    """获取当前live房间数"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM live_room WHERE status='live'")
    count = cur.fetchone()[0]
    conn.close()
    return count


def parse_viewer_count(s):
    """解析 '1.2万' -> 12000"""
    if not s:
        return 0
    s = str(s).strip()
    try:
        if "万" in s:
            return int(float(s.replace("万", "")) * 10000)
        return int(float(s))
    except:
        return 0


# ============================================================================
# 主发现流程
# ============================================================================
class CommerceDiscoveryService:
    def __init__(self):
        self.all_web_rids = set()
        self.live_commerce_rooms = []
        self.sign_target_id = None
        self.sign_session = None
        self._round = 0

    def connect_cdp(self):
        """连接到Chrome CDP"""
        targets = cdp_get_targets()
        if not targets:
            log.error("无法连接Chrome CDP，请确认Chrome在9225端口运行")
            return False
        log.info(f"CDP连接成功，{len(targets)} 个tab")
        return True

    def ensure_sign_session(self):
        """确保签名页面可用（frontierSign已加载）"""
        if self.sign_session and self.sign_target_id:
            try:
                has_fs = self.sign_session.evaluate(JS_CHECK_FS)
                if has_fs:
                    return True
            except:
                pass
            self._cleanup_sign_session()

        target = cdp_create_target("https://live.douyin.com")
        if not target:
            return False
        self.sign_target_id = target.get("id")
        ws_url = target.get("webSocketDebuggerUrl")
        if not ws_url:
            return False
        self.sign_session = CDPSession(ws_url, timeout=20)
        # 等待 byted_acrawler 加载
        for attempt in range(4):
            time.sleep(3)
            has_fs = self.sign_session.evaluate(JS_CHECK_FS)
            if has_fs:
                log.info("签名页面就绪 (frontierSign OK)")
                return True
            log.info(f"等待 frontierSign 加载... ({attempt+1}/4)")
        log.error("frontierSign不可用")
        return False

    def _cleanup_sign_session(self):
        if self.sign_target_id:
            cdp_close_target(self.sign_target_id)
        if self.sign_session:
            self.sign_session.close()
        self.sign_target_id = None
        self.sign_session = None

    def discover(self):
        """第一阶段：爬取分类页发现web_rid"""
        self.all_web_rids.clear()

        # 找一个现有的douyin tab或创建新的
        targets = cdp_get_targets()
        crawl_target = None
        for t in targets:
            url = t.get("url", "")
            if url.startswith("https://live.douyin.com") and t.get("id") != self.sign_target_id:
                crawl_target = t
                break
        if not crawl_target:
            crawl_target = cdp_create_target("https://live.douyin.com")
        if not crawl_target:
            log.error("无法创建爬取tab")
            return

        ws_url = crawl_target.get("webSocketDebuggerUrl")
        if not ws_url:
            log.error("无WebSocket URL")
            return

        session = CDPSession(ws_url, timeout=20)
        crawl_target_id = crawl_target.get("id")
        try:
            for url in CATEGORY_URLS:
                if len(self.all_web_rids) >= MAX_VERIFY_PER_ROUND:
                    break
                try:
                    session.navigate(url, wait=PAGE_WAIT_SECONDS)
                    # 滚动加载更多房间
                    for _ in range(SCROLL_ROUNDS):
                        session.evaluate("window.scrollBy(0, 1200)")
                        time.sleep(SCROLL_DELAY)
                    rids = session.evaluate(JS_EXTRACT_RIDS) or []
                    new_count = len(set(rids) - self.all_web_rids)
                    self.all_web_rids.update(rids)
                    label = url.split("/")[-1] or "home"
                    log.info(f"[发现] {label}: +{new_count}新, 总计: {len(self.all_web_rids)}")
                except Exception as e:
                    log.warning(f"[发现] {url} 异常: {e}")
        finally:
            session.close()
            # 不关闭tab，留给下次复用

        log.info(f"[发现] 完成: {len(self.all_web_rids)} 个web_rid")

    def verify(self):
        """第二阶段：通过enter API批量验证"""
        if not self.all_web_rids:
            log.warning("无web_rid可验证")
            return

        if not self.ensure_sign_session():
            log.error("签名页面不可用")
            return

        rid_list = list(self.all_web_rids)
        verified_commerce = []
        verified_live = []

        log.info(f"[验证] 开始验证 {len(rid_list)} 个房间...")

        for i in range(0, len(rid_list), VERIFY_BATCH_SIZE):
            batch = rid_list[i:i + VERIFY_BATCH_SIZE]
            try:
                js = JS_VERIFY_BATCH.replace("__RIDS_PLACEHOLDER__", json.dumps(batch))
                results = self.sign_session.evaluate(js, timeout=30)
                if not isinstance(results, list):
                    results = []
                for r in results:
                    if not isinstance(r, dict) or r.get("error"):
                        continue
                    if r.get("status") == 2:
                        verified_live.append(r)
                        if r.get("has_commerce_goods"):
                            verified_commerce.append(r)
            except Exception as e:
                log.warning(f"[验证] 批次 {i//VERIFY_BATCH_SIZE+1} 失败: {e}")

            time.sleep(VERIFY_DELAY)

            done = min(i + VERIFY_BATCH_SIZE, len(rid_list))
            if done % 80 == 0 or done == len(rid_list):
                log.info(
                    f"[验证] {done}/{len(rid_list)} "
                    f"在播:{len(verified_live)} 带货:{len(verified_commerce)}"
                )

            if len(verified_commerce) >= MAX_COMMERCE_TARGET:
                log.info(f"[验证] 已达目标 {MAX_COMMERCE_TARGET}，提前结束")
                break

        self.live_commerce_rooms = verified_commerce
        log.info(
            f"[验证] 完成: 在播 {len(verified_live)} 带货 {len(verified_commerce)}"
        )

    def save(self):
        """第三阶段：写入MySQL"""
        if not self.live_commerce_rooms:
            log.warning("无带货直播间可写入")
            return

        count = write_live_rooms(self.live_commerce_rooms)
        log.info(f"[保存] 写入 {count} 个带货直播间")

        rids = [r["web_rid"] for r in self.live_commerce_rooms]
        update_last_verified(rids)

    def cleanup_stale(self):
        """清理过期房间（第一轮跳过，避免误操作）"""
        if self._round <= 1:
            log.info("[清理] 第一轮跳过cleanup，仅标记本轮验证后超时的房间")
            return
        affected = mark_stale_as_finished()
        if affected > 0:
            log.info(f"[清理] {affected} 个过期房间 -> 已结束")

    def run_one_round(self):
        """执行一轮完整的发现-验证-保存"""
        finished_before = get_existing_finished_count()
        live_before = get_live_rooms_count()
        self._round += 1

        log.info(f"{'='*55}")
        log.info(f"  第 {self._round} 轮发现  |  当前: {live_before}直播中, {finished_before}已结束")
        log.info(f"{'='*55}")

        start = time.time()
        self.discover()
        self.verify()
        self.save()
        self.cleanup_stale()

        elapsed = time.time() - start
        finished_after = get_existing_finished_count()
        live_after = get_live_rooms_count()

        log.info(f"{'='*55}")
        log.info(f"  轮次完成 ({elapsed:.0f}s)")
        log.info(f"  发现: {len(self.all_web_rids)} web_rid")
        log.info(f"  带货: {len(self.live_commerce_rooms)} 个")
        log.info(f"  直播中: {live_before} -> {live_after}")
        log.info(f"  已结束: {finished_before} -> {finished_after}"
                 f" {'(不变 OK)' if finished_before == finished_after else ''}")
        log.info(f"{'='*55}")

    def run_periodic(self):
        """定期运行"""
        while True:
            try:
                if self.connect_cdp():
                    self.run_one_round()
                else:
                    log.warning("CDP不可用，60s后重试")
                    time.sleep(60)
                    continue
            except Exception as e:
                log.error(f"轮次异常: {e}")
                traceback.print_exc()
            finally:
                self._cleanup_sign_session()

            log.info(f"等待 {DISCOVERY_INTERVAL}s 后下一轮...")
            time.sleep(DISCOVERY_INTERVAL)

    def cleanup(self):
        self._cleanup_sign_session()


# ============================================================================
# 入口
# ============================================================================
def main():
    print("=" * 60)
    print("  星播 - 带货直播间发现服务")
    print(f"  CDP: {CDP_HOST}:{CDP_PORT}  MySQL: {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"  间隔: {DISCOVERY_INTERVAL}s  目标: {MAX_COMMERCE_TARGET}个带货房间")
    print("=" * 60)

    svc = CommerceDiscoveryService()
    try:
        svc.run_periodic()
    except KeyboardInterrupt:
        log.info("手动停止")
    finally:
        svc.cleanup()


if __name__ == "__main__":
    main()
