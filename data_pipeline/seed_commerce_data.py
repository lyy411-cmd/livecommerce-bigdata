#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
星播平台 - 带货直播间数据全量生成
清除脏数据 → 生成干净的带货直播间 + 主播 + 商品 + 订单
用法: python -m data_pipeline.seed_commerce_data
"""
import pymysql
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal

HOST = '192.168.104.100'
PORT = 3306
USER = 'root'
PWD  = '123456'
DB   = 'livecommerce_db'

# ── 带货直播间模板（平台 × 类目）─────────────────────────
ROOMS_TEMPLATE = [
    # === 抖音 - 美妆 ===
    {"platform": "douyin", "category": "美妆", "anchor": "小鹿美妆", "room": "小鹿美妆｜大牌护肤专场", "live_id": "8801256734"},
    {"platform": "douyin", "category": "美妆", "anchor": "CC化妆品", "room": "CC化妆品｜夏日防晒好物", "live_id": "8801256735"},
    {"platform": "douyin", "category": "美妆", "anchor": "颜九儿", "room": "颜九儿 口红试色专场", "live_id": "8801256736"},
    # === 抖音 - 服饰 ===
    {"platform": "douyin", "category": "服饰", "anchor": "大码女装·小楠", "room": "小楠家大码女装夏上新", "live_id": "8801256740"},
    {"platform": "douyin", "category": "服饰", "anchor": "Mr.穿搭", "room": "Mr.穿搭｜男士商务休闲", "live_id": "8801256741"},
    {"platform": "douyin", "category": "服饰", "anchor": "优衣库官方", "room": "优衣库官方旗舰店直播", "live_id": "8801256742"},
    # === 抖音 - 食品 ===
    {"platform": "douyin", "category": "食品", "anchor": "三只松鼠官方", "room": "三只松鼠零食狂欢节", "live_id": "8801256750"},
    {"platform": "douyin", "category": "食品", "anchor": "东方甄选", "room": "东方甄选｜产地直发水果", "live_id": "8801256751"},
    {"platform": "douyin", "category": "食品", "anchor": "良品铺子", "room": "良品铺子零食大礼包", "live_id": "8801256752"},
    {"platform": "douyin", "category": "食品", "anchor": "认养一头牛", "room": "认养一头牛 鲜奶专场", "live_id": "8801256753"},
    # === 抖音 - 数码 ===
    {"platform": "douyin", "category": "数码", "anchor": "数码小王子", "room": "数码小王子｜耳机评测专场", "live_id": "8801256760"},
    {"platform": "douyin", "category": "数码", "anchor": "绿联官方", "room": "绿联数码配件专场", "live_id": "8801256761"},
    # === 抖音 - 家居 ===
    {"platform": "douyin", "category": "家居", "anchor": "居家好物官", "room": "居家好物官｜收纳神器", "live_id": "8801256770"},
    {"platform": "douyin", "category": "家居", "anchor": "林氏家居", "room": "林氏家居 夏季凉席专场", "live_id": "8801256771"},
    # === 抖音 - 运动 ===
    {"platform": "douyin", "category": "运动", "anchor": "健身教练Kiki", "room": "Kiki健身装备推荐", "live_id": "8801256780"},
    # === 淘宝 - 美妆 ===
    {"platform": "taobao", "category": "美妆", "anchor": "李佳琦直播间", "room": "李佳琦直播间｜美妆节", "live_id": "TB20001234"},
    {"platform": "taobao", "category": "美妆", "anchor": "花西子官方", "room": "花西子官方旗舰店直播", "live_id": "TB20001235"},
    {"platform": "taobao", "category": "美妆", "anchor": "珀莱雅官方", "room": "珀莱雅官方旗舰店｜双抗精华", "live_id": "TB20001236"},
    # === 淘宝 - 服饰 ===
    {"platform": "taobao", "category": "服饰", "anchor": "ZARA官方", "room": "ZARA官方旗舰店直播", "live_id": "TB20001240"},
    {"platform": "taobao", "category": "服饰", "anchor": "太平鸟女装", "room": "太平鸟女装 夏季新品", "live_id": "TB20001241"},
    {"platform": "taobao", "category": "服饰", "anchor": "波司登官方", "room": "波司登防晒服专场", "live_id": "TB20001242"},
    # === 淘宝 - 食品 ===
    {"platform": "taobao", "category": "食品", "anchor": "百草味官方", "room": "百草味官方旗舰店", "live_id": "TB20001250"},
    {"platform": "taobao", "category": "食品", "anchor": "蒙牛旗舰店", "room": "蒙牛官方 整箱特惠", "live_id": "TB20001251"},
    # === 淘宝 - 数码 ===
    {"platform": "taobao", "category": "数码", "anchor": "小米官方", "room": "小米官方旗舰店｜新品首发", "live_id": "TB20001260"},
    {"platform": "taobao", "category": "数码", "anchor": "Anker安克", "room": "Anker充电好物专场", "live_id": "TB20001261"},
    # === 淘宝 - 母婴 ===
    {"platform": "taobao", "category": "母婴", "anchor": "Babycare官方", "room": "Babycare母婴好物", "live_id": "TB20001270"},
    # === 淘宝 - 珠宝 ===
    {"platform": "taobao", "category": "珠宝", "anchor": "周大福官方", "room": "周大福黄金专场", "live_id": "TB20001280"},
    # === 快手 - 美妆 ===
    {"platform": "kuaishou", "category": "美妆", "anchor": "小杨哥美妆", "room": "小杨哥 护肤品大赏", "live_id": "KS30001234"},
    {"platform": "kuaishou", "category": "美妆", "anchor": "韩束官方", "room": "韩束品牌直播间", "live_id": "KS30001235"},
    # === 快手 - 服饰 ===
    {"platform": "kuaishou", "category": "服饰", "anchor": "大杨哥穿搭", "room": "大杨哥 夏季穿搭分享", "live_id": "KS30001240"},
    {"platform": "kuaishou", "category": "服饰", "anchor": "雅鹿官方", "room": "雅鹿防晒衣专场", "live_id": "KS30001241"},
    # === 快手 - 食品 ===
    {"platform": "kuaishou", "category": "食品", "anchor": "东北老铁美食", "room": "东北老铁 零食专场", "live_id": "KS30001250"},
    {"platform": "kuaishou", "category": "食品", "anchor": "洽洽官方", "room": "洽洽瓜子坚果专场", "live_id": "KS30001251"},
    {"platform": "kuaishou", "category": "食品", "anchor": "李子柒官方", "room": "李子柒螺蛳粉&美食", "live_id": "KS30001252"},
    # === 快手 - 家居 ===
    {"platform": "kuaishou", "category": "家居", "anchor": "老铁家居", "room": "老铁家居 厨房好物", "live_id": "KS30001270"},
    {"platform": "kuaishou", "category": "家居", "anchor": "美的官方", "room": "美的电器直播间", "live_id": "KS30001271"},
    # === 快手 - 运动 ===
    {"platform": "kuaishou", "category": "运动", "anchor": "运动达人阿杰", "room": "阿杰运动装备推荐", "live_id": "KS30001280"},
]

# ── 商品模板 ──────────────────────────────────────────────
PRODUCTS = {
    '美妆': [
        ('花西子蜜粉饼', 129, 169), ('完美日记唇釉套装', 89, 129), ('珀莱雅双抗精华', 219, 299),
        ('薇诺娜防晒霜SPF50+', 138, 188), ('自然堂水乳套装', 199, 259), ('欧莱雅复颜面霜', 169, 229),
        ('雅诗兰黛小棕瓶50ml', 499, 680), ('兰蔻大粉水400ml', 350, 450), ('MAC子弹头口红', 170, 210),
        ('3CE九宫格眼影盘', 159, 199), ('SK-II神仙水230ml', 890, 1190), ('谷雨光感水乳套装', 168, 228),
        ('HBN视黄醇晚霜', 199, 259), ('夸迪玻尿酸次抛精华', 258, 338), ('韩束红蛮腰套装', 299, 399),
    ],
    '服饰': [
        ('夏季碎花连衣裙', 159, 239), ('高腰阔腿牛仔裤', 129, 189), ('纯棉短袖T恤3件装', 69, 99),
        ('真丝衬衫女', 289, 399), ('运动休闲套装', 199, 279), ('防晒衣UPF50+', 139, 199),
        ('冰丝阔腿裤', 89, 129), ('蕾丝拼接上衣', 119, 169), ('商务POLO衫男', 159, 229),
        ('小香风外套', 259, 359), ('针织开衫薄款', 149, 219), ('ZARA连衣裙夏季', 299, 399),
        ('太平鸟碎花半裙', 259, 359), ('波司登防晒服', 399, 599), ('雅鹿冰丝T恤', 89, 139),
    ],
    '食品': [
        ('良品铺子坚果礼盒', 99, 139), ('三只松鼠每日坚果30袋', 69, 99), ('百草味猪肉脯500g', 39, 59),
        ('蒙牛纯牛奶250ml×24', 59, 79), ('三只松鼠夏威夷果', 49, 69), ('旺旺大礼包', 69, 89),
        ('洽洽瓜子组合装', 29, 45), ('李子柒螺蛳粉5袋装', 35, 55), ('自嗨锅牛肉火锅', 49, 69),
        ('空刻意面4盒装', 59, 89), ('认养一头牛酸奶12盒', 68, 88), ('东方甄选鲜果礼盒', 79, 119),
        ('卫龙辣条大礼包', 29, 45), ('农夫山泉矿泉水整箱', 29, 39), ('良品铺子蛋黄酥礼盒', 39, 59),
    ],
    '数码': [
        ('小米充电宝20000mAh', 89, 129), ('蓝牙耳机主动降噪', 199, 299), ('Type-C快充数据线2m', 19, 35),
        ('iPad保护套磁吸', 59, 89), ('无线充电器15W', 79, 119), ('机械键盘青轴', 199, 299),
        ('罗技无线静音鼠标', 79, 119), ('USB-C扩展坞7合1', 89, 139), ('屏幕挂灯LED', 99, 149),
        ('小米音箱Pro', 129, 199), ('绿联Type-C集线器', 69, 99), ('Anker氮化镓充电器', 159, 229),
        ('手机散热器半导体制冷', 59, 89), ('索尼WH-1000XM5耳机', 1999, 2499), ('小米手环8', 199, 269),
    ],
    '家居': [
        ('全棉四件套床品', 299, 429), ('泰国乳胶枕', 159, 229), ('收纳盒三件套', 39, 59),
        ('保温杯316不锈钢500ml', 69, 99), ('落地风扇遥控静音', 199, 289), ('遮光窗帘定制', 129, 199),
        ('记忆棉坐垫办公', 49, 79), ('LED护眼台灯', 89, 139), ('智能感应垃圾桶', 79, 119),
        ('居家防滑拖鞋', 25, 39), ('厨房置物架三层', 59, 89), ('美的电风扇落地式', 259, 359),
        ('林氏家居凉席三件套', 199, 299), ('折叠晾衣架', 49, 79), ('真空收纳袋10个装', 29, 49),
    ],
    '母婴': [
        ('Babycare婴儿连体衣', 59, 89), ('宝宝辅食米粉有机', 49, 79), ('儿童保温水杯316', 69, 99),
        ('婴儿湿巾80抽×12包', 35, 55), ('纸尿裤L码整箱', 159, 219), ('儿童益智积木玩具', 89, 139),
        ('宝宝学步鞋软底', 79, 119), ('儿童防晒衣UPF50+', 89, 129), ('婴儿洗护套装', 99, 149),
        ('儿童书包护脊减负', 199, 299), ('宝宝餐椅可折叠', 299, 429), ('儿童牙膏含氟套装', 39, 59),
    ],
    '珠宝': [
        ('周大福925银项链', 199, 299), ('天然淡水珍珠耳环', 159, 239), ('翡翠玉镯A货', 899, 1299),
        ('黄金转运珠手链', 599, 899), ('天然水晶手串', 129, 199), ('足银手镯女', 259, 389),
        ('钻石戒指简约款', 1299, 1999), ('和田玉吊坠', 499, 799), ('碧玺项链108颗', 359, 559),
    ],
    '运动': [
        ('瑜伽垫加厚15mm', 79, 119), ('跑步鞋轻便透气', 259, 379), ('运动水壶Tritan 1L', 49, 79),
        ('筋膜枪深层按摩', 199, 299), ('哑铃可调节10kg', 159, 229), ('跳绳计数钢丝', 29, 49),
        ('运动护膝专业', 49, 79), ('弹力带套装5根', 39, 59), ('泡沫轴肌肉放松', 49, 79),
        ('运动双肩背包', 129, 189), ('健身手套护掌', 39, 59), ('瑜伽球加厚防爆', 59, 89),
    ],
}

USER_NAMES = [
    '快乐购物狂', '小蜜蜂爱买', '省钱达人01', '精致生活家', '追风少女心',
    '品质生活派', '购物小能手', '理性消费者', '种草小达人', '直播间常客',
    '薅羊毛专家', '居家好帮手', '美丽不打折', '时尚买手王', '精明妈妈团',
    '数码控小哥', '零食收藏家', '运动爱好者', '护肤日记本', '穿搭研究员',
    '家居小清新', '宝宝好物官', '珠宝鉴赏师', '健身装备党', '厨房小当家',
    '办公好帮手', '旅行必备品', '宠物好伙伴', '学生党省钱', '打工人好物',
]


def connect():
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=PWD,
                           database=DB, charset='utf8mb4', connect_timeout=10)


def main():
    print("=" * 60)
    print("  星播平台 - 带货直播间数据全量生成")
    print("=" * 60)

    conn = connect()
    cur = conn.cursor()

    # ─── 第1步: 清除全部旧数据 ──────────────────────────────
    print("\n[1/6] 清除旧数据...")
    for table in ['order_info', 'rt_danmaku', 'rt_product', 'rt_room_stats',
                  'anchor', 'live_room', 'crawler_session']:
        cur.execute(f"DELETE FROM {table} WHERE 1=1")
        print(f"  ✓ 清空 {table}")
    conn.commit()

    now = datetime.now()

    # ─── 第2步: 生成直播间 ──────────────────────────────────
    print(f"\n[2/6] 生成 {len(ROOMS_TEMPLATE)} 个带货直播间...")
    room_ids = []
    for i, tpl in enumerate(ROOMS_TEMPLATE):
        plat = tpl['platform']
        cat = tpl['category']
        anchor = tpl['anchor']
        room_name = tpl['room']
        live_id = tpl['live_id']
        room_no = f"CRAWL_{plat.upper()}_{live_id}"

        # 指标根据类目和平台合理分配
        if plat == 'douyin':
            viewers = random.randint(5000, 80000)
        elif plat == 'taobao':
            viewers = random.randint(3000, 60000)
        else:  # kuaishou
            viewers = random.randint(2000, 45000)

        # 头部主播观众更多
        if any(kw in anchor for kw in ['李佳琦', '东方甄选', '小杨哥', '官方旗舰']):
            viewers = int(viewers * random.uniform(1.5, 2.5))

        conv_rate = round(random.uniform(2.5, 7.5), 2)
        orders = max(10, int(viewers * conv_rate / 100))
        avg_price = random.randint(40, 280)
        gmv = round(orders * avg_price * random.uniform(0.8, 1.2), 2)

        status = random.choices(['live', 'paused', 'finished'], weights=[60, 20, 20])[0]
        start_offset = timedelta(hours=random.randint(1, 12), minutes=random.randint(0, 59))

        # Seed rooms have no real URL - don't generate fake ones
        live_url = ''

        cur.execute(
            "INSERT INTO live_room "
            "(room_no, room_name, anchor_name, platform, category, status, "
            "viewer_count, order_count, gmv, conversion_rate, "
            "live_url, room_id_external, data_source, start_time) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'real',%s)",
            (room_no, room_name, anchor, plat, cat, status,
             viewers, orders, gmv, conv_rate,
             live_url, live_id, now - start_offset)
        )
        room_ids.append((cur.lastrowid, room_no, room_name, anchor, plat, cat,
                         viewers, orders, gmv, conv_rate, status, live_id))
        print(f"  ✓ [{plat:8s}] {cat:4s} | {room_name:30s} | {viewers:>6,}人 | ¥{gmv:>12,.0f}")

    conn.commit()

    # ─── 第3步: 同步 rt_room_stats ─────────────────────────
    print("\n[3/6] 同步 rt_room_stats...")
    for rid, rno, rname, anchor, plat, cat, viewers, orders, gmv, conv, status, live_id in room_ids:
        if status != 'live':
            continue
        peak = int(viewers * random.uniform(1.1, 1.5))
        danmaku = random.randint(100, 5000)

        # Seed rooms have no real URL - don't generate fake ones
        live_url = ''

        cur.execute(
            "INSERT INTO rt_room_stats "
            "(room_id, room_name, anchor_name, platform, category, status, "
            "current_viewers, peak_viewers, total_danmaku, total_orders, total_gmv, "
            "live_url, cover_url, start_time) VALUES "
            "(%s,%s,%s,%s,%s,'live',%s,%s,%s,%s,%s,%s,%s,%s)",
            (live_id, rname, anchor, plat, cat,
             viewers, peak, danmaku, orders, gmv,
             live_url, '', now - timedelta(hours=random.randint(1, 6)))
        )
    conn.commit()
    live_count = sum(1 for r in room_ids if r[10] == 'live')
    print(f"  ✓ 写入 {live_count} 个在线直播间")

    # ─── 第4步: 生成主播数据 ───────────────────────────────
    print("\n[4/6] 生成主播数据...")
    anchor_agg = {}
    for rid, rno, rname, anchor, plat, cat, viewers, orders, gmv, conv, status, live_id in room_ids:
        if anchor not in anchor_agg:
            anchor_agg[anchor] = {'platform': plat, 'category': cat, 'gmv': 0, 'orders': 0}
        anchor_agg[anchor]['gmv'] += float(gmv)
        anchor_agg[anchor]['orders'] += int(orders)

    for aname, info in anchor_agg.items():
        g = info['gmv']
        o = info['orders']
        if g > 500000:
            level = 'S'
        elif g > 150000:
            level = 'A'
        elif g > 50000:
            level = 'B'
        else:
            level = 'C'

        fans = max(o * random.randint(5, 20), random.randint(1000, 80000))
        live_hours = random.randint(30, 600)
        conv = round(random.uniform(2.5, 7.0), 2)
        prefix = '头部' if level in 'SA' else ('腰部' if level == 'B' else '新锐')

        cur.execute(
            "INSERT INTO anchor (name, nickname, platform, level, category, "
            "fans_count, live_hours, total_gmv, total_orders, avg_conversion, intro) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (aname, aname, info['platform'], level, info['category'],
             fans, live_hours, round(g, 2), o, conv,
             f"{prefix}{info['category']}带货主播，擅长选品与粉丝互动")
        )
    conn.commit()
    print(f"  ✓ 创建 {len(anchor_agg)} 个主播")

    # ─── 第5步: 生成商品数据 ───────────────────────────────
    print("\n[5/6] 生成商品数据...")
    product_count = 0
    for rid, rno, rname, anchor, plat, cat, viewers, orders, gmv, conv, status, live_id in room_ids:
        cat_products = PRODUCTS.get(cat, PRODUCTS['食品'])
        n_products = random.randint(5, min(12, len(cat_products)))
        selected = random.sample(cat_products, n_products)

        for sort_idx, (pname, price, orig_price) in enumerate(selected):
            sales = max(1, int(orders * random.uniform(0.02, 0.25)))
            cur.execute(
                "INSERT INTO rt_product "
                "(product_id, room_id, platform, product_name, price, "
                "original_price, sales, category, image_url, sort_order) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (f"P{live_id}_{sort_idx}", live_id, plat, pname,
                 price, orig_price, sales, cat, '', sort_idx)
            )
            product_count += 1
    conn.commit()
    print(f"  ✓ 创建 {product_count} 个商品")

    # ─── 第6步: 生成订单（近30天分布）──────────────────────
    print("\n[6/6] 生成订单数据（近30天）...")
    cur.execute("DELETE FROM order_info WHERE 1=1")
    order_statuses = ['pending', 'paid', 'shipped', 'delivered', 'delivered',
                      'delivered', 'delivered', 'cancelled']
    order_count = 0

    for rid, rno, rname, anchor, plat, cat, viewers, orders, gmv, conv, status, live_id in room_ids:
        cat_products = PRODUCTS.get(cat, PRODUCTS['食品'])
        n_orders = int(orders)
        if n_orders == 0:
            continue

        remaining_gmv = float(gmv)
        for i in range(n_orders):
            is_last = (i == n_orders - 1)
            if is_last:
                amount = max(remaining_gmv, 9.9)
            else:
                avg = remaining_gmv / (n_orders - i)
                amount = round(random.uniform(avg * 0.4, avg * 1.6), 2)
                amount = max(amount, 9.9)
                remaining_gmv -= amount

            product = random.choice(cat_products)
            qty = random.choices([1, 2, 3, 5], weights=[60, 25, 10, 5])[0]

            # 30天内分布（越近越多）
            days_ago = random.choices(range(30), weights=[30 - d + 1 for d in range(30)])[0]
            order_time = now - timedelta(days=days_ago, hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59))

            order_no = f"ORD{order_time.strftime('%Y%m%d%H%M%S')}{random.randint(1000,9999)}"
            ostatus = random.choice(order_statuses)

            cur.execute(
                "INSERT INTO order_info "
                "(order_no, product_name, room_name, username, quantity, "
                "total_amount, platform, status, create_time) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (order_no, product[0], rname, random.choice(USER_NAMES), qty,
                 round(amount, 2), plat, ostatus, order_time)
            )
            order_count += 1
            if order_count % 500 == 0:
                conn.commit()
    conn.commit()
    print(f"  ✓ 创建 {order_count:,} 条订单")

    # ─── 验证 ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  数据验证")
    print("=" * 60)

    cur2 = conn.cursor(pymysql.cursors.DictCursor)
    cur2.execute("SELECT COUNT(*) as cnt, SUM(viewer_count) as v, SUM(gmv) as g, SUM(order_count) as o FROM live_room WHERE deleted=0")
    r = cur2.fetchone()
    cur2.execute("SELECT COUNT(*) as cnt FROM anchor WHERE deleted=0")
    a = cur2.fetchone()
    cur2.execute("SELECT COUNT(*) as cnt, SUM(total_amount) as amt FROM order_info WHERE deleted=0")
    o = cur2.fetchone()
    cur2.execute("SELECT COUNT(*) as cnt FROM rt_room_stats WHERE status='live'")
    rt = cur2.fetchone()
    cur2.execute("SELECT COUNT(*) as cnt FROM rt_product")
    pr = cur2.fetchone()
    cur2.execute("SELECT platform, COUNT(*) as cnt FROM live_room WHERE deleted=0 GROUP BY platform")
    plats = cur2.fetchall()
    cur2.execute("SELECT category, COUNT(*) as cnt, SUM(gmv) as g FROM live_room WHERE deleted=0 GROUP BY category ORDER BY g DESC")
    cats = cur2.fetchall()

    print(f"""
  直播间:    {int(r['cnt']):>5} 个
  总观众:    {int(r['v'] or 0):>10,} 人
  总 GMV:    ¥{float(r['g'] or 0):>14,.2f}
  主播数:    {int(a['cnt']):>5} 人
  订单数:    {int(o['cnt']):>10,} 条
  订单总额:  ¥{float(o['amt'] or 0):>14,.2f}
  在线房间:  {int(rt['cnt']):>5} 间
  商品数:    {int(pr['cnt']):>5} 个""")

    print(f"\n  平台分布:")
    for p in plats:
        pn = {'douyin': '抖音', 'taobao': '淘宝', 'kuaishou': '快手'}.get(p['platform'], p['platform'])
        print(f"    {pn}: {int(p['cnt']):>3} 间")

    print(f"\n  类目分布:")
    for c in cats:
        print(f"    {c['category']:4s}: {int(c['cnt']):>3} 间 | GMV ¥{float(c['g'] or 0):>12,.0f}")

    cur2.close()
    cur.close()
    conn.close()
    print(f"\n✓ 全部完成！刷新前端页面即可查看。")


if __name__ == '__main__':
    main()
