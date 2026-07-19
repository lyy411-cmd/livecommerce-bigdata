#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据充实脚本 —— 清理非带货直播间 + 填充真实指标 + 生成主播/订单数据
用法: python -m data_pipeline.enrich_data
"""
import pymysql
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal

# ── VM MySQL 连接 ──────────────────────────────────────────
HOST = '192.168.104.100'
PORT = 3306
USER = 'root'
PWD  = '123456'
DB   = 'livecommerce_db'

# ── 非带货关键词（匹配直播间名/主播名 → 删除）────────────
NON_COMMERCE_KEYWORDS = [
    # 游戏
    '游戏', 'CF', '穿越火线', '梦幻西游', '王者荣耀', '和平精英',
    '蛋仔派对', 'LOL', '英雄联盟', '吃鸡', '原神', '电竞', 'CSGO',
    'CS2', 'DOTA', 'dota', 'APEX', 'apex', '瓦罗兰特', '永劫无间',
    '使命召唤', 'PUBG', 'pubg', '暗黑', '传奇', 'DNF', 'dnf',
    '火影', '海贼王', '棋牌', '象棋', '围棋', '斗地主', '麻将',
    '游戏解说', '打游戏', '开黑', '上分', '排位',
    # 娱乐
    '唱歌', '跳舞', '才艺', 'DJ', 'dj', '打碟', '喊麦',
    '脱口秀', '相声', '小品', '综艺', 'K歌', 'k歌',
    # 新闻/体育
    '新闻', '资讯', '央视', '新华社', '人民日报',
    '体育', '足球', '篮球', 'NBA', 'nba', 'CBA', '世界杯',
    # 教育/其他
    '网课', '教学', '考研', '考公', '公务员', '英语听力',
]

# ── 带货分类 ──────────────────────────────────────────────
CATEGORIES = ['美妆', '服饰', '食品', '数码', '家居', '母婴', '珠宝', '运动']

# ── 分类 → 商品模板（用于生成订单）───────────────────────
CATEGORY_PRODUCTS = {
    '美妆': [
        ('花西子蜜粉饼', 129), ('完美日记唇釉套装', 89), ('珀莱雅双抗精华', 219),
        ('薇诺娜防晒霜', 138), ('自然堂水乳套装', 199), ('欧莱雅复颜面霜', 169),
        ('雅诗兰黛小棕瓶', 499), ('兰蔻粉水', 350), ('MAC子弹头口红', 170),
        ('3CE眼影盘', 159), ('悦木之源菌菇水', 280), ('SK-II神仙水', 890),
        ('谷雨光感水乳', 168), ('HBN视黄醇晚霜', 199), ('夸迪玻尿酸次抛', 258),
    ],
    '服饰': [
        ('夏季碎花连衣裙', 159), ('高腰阔腿牛仔裤', 129), ('纯棉短袖T恤', 69),
        ('真丝衬衫女', 289), ('运动休闲套装', 199), ('防晒衣UPF50+', 139),
        ('冰丝阔腿裤', 89), ('蕾丝拼接上衣', 119), ('棉麻半身裙', 109),
        ('商务POLO衫男', 159), ('夏季薄款西裤', 179), ('运动短裤速干', 79),
        ('小香风外套', 259), ('针织开衫', 149), ('牛仔外套女', 199),
    ],
    '食品': [
        ('良品铺子坚果礼盒', 99), ('三只松鼠每日坚果', 69), ('百草味猪肉脯', 39),
        ('蒙牛纯牛奶整箱', 59), ('农夫山泉矿泉水', 29), ('三只松鼠夏威夷果', 49),
        ('旺旺大礼包', 69), ('洽洽瓜子组合', 29), ('李子柒螺蛳粉', 35),
        ('自嗨锅牛肉火锅', 49), ('空刻意面套装', 59), ('认养一头牛酸奶', 68),
        ('百草味芒果干', 25), ('良品铺子蛋黄酥', 39), ('卫龙辣条大礼包', 29),
    ],
    '数码': [
        ('充电宝20000mAh', 89), ('蓝牙耳机降噪', 199), ('手机壳防摔', 29),
        ('Type-C数据线', 19), ('平板保护套', 59), ('无线充电器', 79),
        ('手机支架桌面', 25), ('移动硬盘1TB', 299), ('机械键盘', 199),
        ('鼠标无线静音', 79), ('USB扩展坞', 89), ('屏幕挂灯', 99),
        ('手机散热器', 59), ('音箱小钢炮', 129), ('电竞耳机', 159),
    ],
    '家居': [
        ('全棉四件套床品', 299), ('乳胶枕头', 159), ('收纳盒三件套', 39),
        ('保温杯500ml', 69), ('落地风扇静音', 199), ('遮光窗帘', 129),
        ('记忆棉坐垫', 49), ('LED台灯护眼', 89), ('垃圾桶智能感应', 79),
        ('拖鞋居家防滑', 25), ('置物架厨房', 59), ('晾衣架折叠', 49),
        ('地毯客厅简约', 159), ('衣架实木30支', 39), ('真空收纳袋', 29),
    ],
    '母婴': [
        ('婴儿纯棉连体衣', 59), ('宝宝辅食米粉', 49), ('儿童保温水杯', 69),
        ('婴儿湿巾大包装', 35), ('纸尿裤L码整箱', 159), ('儿童益智玩具', 89),
        ('宝宝学步鞋', 79), ('儿童防晒衣', 89), ('婴儿洗护套装', 99),
        ('儿童书包护脊', 199), ('宝宝餐椅', 299), ('儿童牙膏套装', 39),
    ],
    '珠宝': [
        ('925银项链', 199), ('珍珠耳环', 159), ('翡翠玉镯', 899),
        ('黄金转运珠手链', 599), ('天然水晶手串', 129), ('银手镯女', 259),
        ('钻石戒指简约款', 1299), ('和田玉吊坠', 499), ('碧玺项链', 359),
    ],
    '运动': [
        ('瑜伽垫加厚防滑', 79), ('跑步鞋轻便透气', 259), ('运动水壶1L', 49),
        ('筋膜枪按摩', 199), ('哑铃可调节', 159), ('跳绳计数', 29),
        ('运动护膝', 49), ('弹力带套装', 39), ('泡沫轴放松', 49),
        ('运动背包', 129), ('健身手套', 39), ('瑜伽球', 59),
    ],
}

# ── 平台列表 ──────────────────────────────────────────────
PLATFORMS = ['douyin', 'taobao', 'kuaishou']

# ── 用户名池 ──────────────────────────────────────────────
USER_NAMES = [
    '快乐购物狂', '小蜜蜂爱买', '省钱达人01', '精致生活家',
    '追风少女心', '品质生活派', '购物小能手', '理性消费者',
    '种草小达人', '直播间常客', '薅羊毛专家', '居家好帮手',
    '美丽不打折', '时尚买手王', '精明妈妈团', '数码控小哥',
    '零食收藏家', '运动爱好者', '护肤日记本', '穿搭研究员',
    '家居小清新', '宝宝好物官', '珠宝鉴赏师', '健身装备党',
    '厨房小当家', '办公好帮手', '旅行必备品', '宠物好伙伴',
]


def is_commerce_room(room_name, anchor_name, category):
    """判断是否为带货直播间"""
    combined = f"{room_name} {anchor_name}".lower()
    for kw in NON_COMMERCE_KEYWORDS:
        if kw.lower() in combined:
            return False
    return True


def classify_room(room_name, anchor_name):
    """根据直播间名称智能分类"""
    combined = f"{room_name} {anchor_name}"
    keyword_map = {
        '美妆': ['美妆', '化妆', '护肤', '口红', '粉底', '眼影', '精华', '面膜', '防晒', '粉底液', '遮瑕'],
        '服饰': ['服装', '穿搭', '女装', '男装', '童装', '连衣裙', 'T恤', '牛仔裤', '外套', '衬衫', '内衣'],
        '食品': ['零食', '美食', '食品', '水果', '牛奶', '坚果', '饼干', '蛋糕', '茶', '咖啡', '酒', '调味品'],
        '数码': ['数码', '手机', '电脑', '耳机', '充电', '键盘', '平板', '相机', '音箱', '显示器'],
        '家居': ['家居', '家纺', '厨房', '收纳', '清洁', '家具', '窗帘', '床品', '灯具', '卫浴'],
        '母婴': ['母婴', '宝宝', '婴儿', '儿童', '孕妇', '奶粉', '纸尿裤', '童装'],
        '珠宝': ['珠宝', '首饰', '黄金', '翡翠', '钻石', '玉石', '银饰', '珍珠'],
        '运动': ['运动', '健身', '瑜伽', '跑步', '户外', '骑行', '球拍'],
    }
    for cat, keywords in keyword_map.items():
        for kw in keywords:
            if kw in combined:
                return cat
    return random.choice(['美妆', '服饰', '食品'])  # 默认偏大众品类


def connect():
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=PWD,
                           database=DB, charset='utf8mb4', connect_timeout=10)


def main():
    print("=" * 60)
    print("  星播平台 - 数据充实脚本 (带货直播间 + 指标 + 订单)")
    print("=" * 60)

    conn = connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    # ─── 第1步: 清理非带货直播间 ──────────────────────────
    print("\n[1/6] 扫描直播间，移除非带货内容...")
    cur.execute("SELECT id, room_no, room_name, anchor_name, category, platform FROM live_room WHERE deleted=0")
    all_rooms = cur.fetchall()
    print(f"  当前直播间总数: {len(all_rooms)}")

    removed = 0
    kept_rooms = []
    for room in all_rooms:
        if is_commerce_room(room['room_name'] or '', room['anchor_name'] or '', room['category'] or ''):
            kept_rooms.append(room)
        else:
            cur.execute("UPDATE live_room SET deleted=1 WHERE id=%s", (room['id'],))
            removed += 1
            print(f"  ✗ 移除: [{room['platform']}] {room['room_name']} ({room['anchor_name']})")

    conn.commit()
    print(f"  移除 {removed} 个非带货直播间，保留 {len(kept_rooms)} 个")

    # ─── 第2步: 为保留的直播间分配分类 + 填充指标 ────────────
    print("\n[2/6] 为直播间分配带货分类并填充指标...")
    cur.execute("SELECT * FROM live_room WHERE deleted=0")
    rooms = cur.fetchall()
    print(f"  待充实直播间: {len(rooms)}")

    total_room_gmv = 0
    for room in rooms:
        rid = room['id']
        room_name = room['room_name'] or ''
        anchor_name = room['anchor_name'] or ''

        # 智能分类
        cat = classify_room(room_name, anchor_name)

        # 随机生成指标（带货直播间范围）
        viewers = random.randint(800, 65000)
        conv_rate = round(random.uniform(2.0, 8.5), 2)
        orders = max(1, int(viewers * conv_rate / 100))
        avg_price = random.randint(35, 350)
        gmv = round(orders * avg_price * random.uniform(0.85, 1.15), 2)
        total_room_gmv += gmv

        status = random.choice(['live', 'live', 'live', 'paused', 'finished'])

        cur.execute(
            "UPDATE live_room SET category=%s, viewer_count=%s, order_count=%s, "
            "gmv=%s, conversion_rate=%s, status=%s WHERE id=%s",
            (cat, viewers, orders, gmv, conv_rate, status, rid)
        )
        print(f"  ✓ {room_name}: {cat} | {viewers}人 | {orders}单 | ¥{gmv:,.0f}")

    conn.commit()

    # ─── 第3步: 更新 rt_room_stats ──────────────────────────
    print("\n[3/6] 同步 rt_room_stats 实时表...")
    cur.execute("SELECT * FROM live_room WHERE deleted=0 AND status='live'")
    live_rooms = cur.fetchall()
    updated_rt = 0
    for room in live_rooms:
        room_id = room.get('room_id_external') or room.get('room_no', '')
        if not room_id:
            continue
        viewers = int(room['viewer_count'] or 0)
        peak = int(viewers * random.uniform(1.1, 1.5))
        cur.execute(
            "INSERT INTO rt_room_stats "
            "(room_id, room_name, anchor_name, platform, category, status, "
            "current_viewers, peak_viewers, total_danmaku, total_orders, total_gmv, "
            "live_url, cover_url, start_time) VALUES "
            "(%s,%s,%s,%s,%s,'live',%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE "
            "room_name=VALUES(room_name), anchor_name=VALUES(anchor_name), "
            "platform=VALUES(platform), category=VALUES(category), "
            "current_viewers=VALUES(current_viewers), peak_viewers=VALUES(peak_viewers), "
            "total_danmaku=VALUES(total_danmaku), total_orders=VALUES(total_orders), "
            "total_gmv=VALUES(total_gmv)",
            (room_id, room['room_name'], room['anchor_name'], room['platform'],
             room['category'], viewers, peak,
             random.randint(50, 2000), int(room['order_count'] or 0),
             float(room['gmv'] or 0),
             room.get('live_url', '') or '', room.get('cover_url', '') or '',
             datetime.now() - timedelta(hours=random.randint(1, 8)))
        )
        updated_rt += 1
    conn.commit()
    print(f"  同步 {updated_rt} 个实时直播间")

    # ─── 第4步: 生成主播(anchor)数据 ────────────────────────
    print("\n[4/6] 生成主播数据...")
    # 清理旧主播数据
    cur.execute("UPDATE anchor SET deleted=1 WHERE deleted=0")

    # 按主播名聚合房间数据
    anchor_map = {}
    for room in rooms:
        aname = room['anchor_name'] or '未知主播'
        if aname not in anchor_map:
            anchor_map[aname] = {
                'platform': room['platform'],
                'category': room['category'],
                'total_gmv': 0, 'total_orders': 0, 'fans': 0
            }
        anchor_map[aname]['total_gmv'] += float(room['gmv'] or 0)
        anchor_map[aname]['total_orders'] += int(room['order_count'] or 0)

    anchor_levels = ['S', 'A', 'B', 'C']
    created_anchors = 0
    for aname, info in anchor_map.items():
        gmv = info['total_gmv']
        orders = info['total_orders']
        # 根据GMV分级
        if gmv > 500000:
            level = 'S'
        elif gmv > 100000:
            level = 'A'
        elif gmv > 30000:
            level = 'B'
        else:
            level = 'C'

        fans = max(orders * random.randint(3, 15), random.randint(500, 50000))
        live_hours = random.randint(20, 500)
        conv = round(random.uniform(2.5, 7.5), 2)

        cur.execute(
            "INSERT INTO anchor (name, nickname, platform, level, category, "
            "fans_count, live_hours, total_gmv, total_orders, avg_conversion, intro) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (aname, aname, info['platform'], level, info['category'],
             fans, live_hours, round(gmv, 2), orders, conv,
             f"{'资深' if level in 'SA' else '新锐'}{info['category']}带货主播，擅长选品和互动")
        )
        created_anchors += 1

    conn.commit()
    print(f"  创建 {created_anchors} 个主播记录")

    # ─── 第5步: 生成订单数据 ────────────────────────────────
    print("\n[5/6] 生成订单数据...")
    # 清理旧订单
    cur.execute("DELETE FROM order_info WHERE 1=1")

    order_count = 0
    now = datetime.now()
    order_statuses = ['pending', 'paid', 'shipped', 'delivered', 'delivered',
                      'delivered', 'delivered', 'cancelled']

    for room in rooms:
        room_cat = room['category'] or '食品'
        products = CATEGORY_PRODUCTS.get(room_cat, CATEGORY_PRODUCTS['食品'])
        n_orders = int(room['order_count'] or 0)
        room_gmv_target = float(room['gmv'] or 0)

        if n_orders == 0:
            continue

        # 按比例分配GMV到各订单
        remaining_gmv = room_gmv_target
        for i in range(n_orders):
            is_last = (i == n_orders - 1)
            if is_last:
                amount = max(remaining_gmv, 9.9)
            else:
                avg = remaining_gmv / (n_orders - i)
                amount = round(random.uniform(avg * 0.4, avg * 1.6), 2)
                amount = max(amount, 9.9)
                remaining_gmv -= amount

            product = random.choice(products)
            qty = random.choices([1, 2, 3, 5], weights=[60, 25, 10, 5])[0]
            # 价格调整使金额合理
            unit_price = round(amount / qty, 2)

            days_ago = random.randint(0, 30)
            hours_ago = random.randint(0, 23)
            mins_ago = random.randint(0, 59)
            order_time = now - timedelta(days=days_ago, hours=hours_ago, minutes=mins_ago)

            order_no = f"ORD{order_time.strftime('%Y%m%d%H%M%S')}{random.randint(1000,9999)}"
            status = random.choice(order_statuses)

            cur.execute(
                "INSERT INTO order_info "
                "(order_no, product_name, room_name, username, quantity, "
                "total_amount, platform, status, create_time) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (order_no, product[0], room['room_name'],
                 random.choice(USER_NAMES), qty,
                 round(amount, 2), room['platform'], status, order_time)
            )
            order_count += 1

            # 每200单提交一次防止内存堆积
            if order_count % 200 == 0:
                conn.commit()

    conn.commit()
    print(f"  创建 {order_count} 条订单记录")

    # ─── 第6步: 验证结果 ────────────────────────────────────
    print("\n[6/6] 验证数据完整性...")
    stats = {}
    cur.execute("SELECT COUNT(*) as cnt, COALESCE(SUM(viewer_count),0) as v, COALESCE(SUM(gmv),0) as g, COALESCE(SUM(order_count),0) as o FROM live_room WHERE deleted=0")
    r = cur.fetchone()
    stats['rooms'] = r['cnt']
    stats['viewers'] = int(r['v'])
    stats['gmv'] = float(r['g'])
    stats['orders_room'] = int(r['o'])

    cur.execute("SELECT COUNT(*) as cnt FROM anchor WHERE deleted=0")
    stats['anchors'] = cur.fetchone()['cnt']

    cur.execute("SELECT COUNT(*) as cnt, COALESCE(SUM(total_amount),0) as amt FROM order_info WHERE deleted=0")
    o = cur.fetchone()
    stats['orders'] = o['cnt']
    stats['order_amount'] = float(o['amt'])

    cur.execute("SELECT COUNT(*) as cnt FROM rt_room_stats WHERE status='live'")
    stats['rt_live'] = cur.fetchone()['cnt']

    cur.execute("SELECT category, COUNT(*) as cnt FROM live_room WHERE deleted=0 GROUP BY category ORDER BY cnt DESC")
    cat_dist = cur.fetchall()

    cur.execute("SELECT platform, COUNT(*) as cnt FROM live_room WHERE deleted=0 GROUP BY platform")
    plat_dist = cur.fetchall()

    print(f"\n  ┌─────────────────────────────────────┐")
    print(f"  │  直播间: {stats['rooms']:>5} 个              │")
    print(f"  │  总观众: {stats['viewers']:>8,} 人           │")
    print(f"  │  总GMV:  ¥{stats['gmv']:>12,.2f}        │")
    print(f"  │  订单数: {stats['orders']:>8,} 条           │")
    print(f"  │  订单额: ¥{stats['order_amount']:>12,.2f}        │")
    print(f"  │  主播数: {stats['anchors']:>5} 人              │")
    print(f"  │  实时在线: {stats['rt_live']:>4} 间            │")
    print(f"  ├─────────────────────────────────────┤")
    print(f"  │  分类分布:                            │")
    for c in cat_dist:
        print(f"  │    {c['category']:4s}: {c['cnt']:>3} 间              │")
    print(f"  ├─────────────────────────────────────┤")
    print(f"  │  平台分布:                            │")
    for p in plat_dist:
        pname = {'douyin': '抖音', 'taobao': '淘宝', 'kuaishou': '快手'}.get(p['platform'], p['platform'])
        print(f"  │    {pname}: {p['cnt']:>3} 间              │")
    print(f"  └─────────────────────────────────────┘")

    cur.close()
    conn.close()
    print("\n✓ 数据充实完成！刷新页面即可看到效果。")


if __name__ == '__main__':
    main()
