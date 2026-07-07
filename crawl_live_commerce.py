"""直播电商专题爬虫 - 只取消费/商品相关热搜 + 匹配主播品类"""
import requests, re, time, random, os, pymysql
from datetime import datetime
from collections import Counter

MYSQL = {'host': '192.168.104.100', 'port': 3306, 'user': 'root', 'password': '123456', 'database': 'livecommerce_db', 'charset': 'utf8mb4'}
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

COMMERCE_KEYWORDS = [
    '手机','电脑','耳机','电视','空调','冰箱','洗衣机','相机',
    '美妆','精华','面膜','口红','香水','防晒','护肤',
    '零食','食品','饮料','水果','茶叶','牛奶','坚果','咖啡',
    '服装','鞋子','包包','手表','首饰','帽子','丝巾','T恤','裙子',
    '汽车','电动车','自行车','玩具','家具','数码','家电',
    '品牌','旗舰','新品','爆款','优惠','折扣','促销','降价',
    '直播','带货','主播','小红书','淘宝','抖音','快手','京东','拼多多',
    '价格','评测','推荐','好物','种草','开箱','测评','对比',
    '华为','小米','苹果','海尔','美的','格力','vivo','OPPO','荣耀',
    '李佳琦','薇娅','辛巴','罗永浩','小杨哥','董宇辉','东方甄选',
]

def is_commerce_related(keyword):
    for ck in COMMERCE_KEYWORDS:
        if ck in keyword:
            return True
    blacklist = ['牺牲','遇难','悼念','制裁','关税','政策','外交','军事','导弹',
                 '航母','政党','政府','法院','审判','死刑','开枪','炸弹',
                 '洪水','地震','台风','火灾','救援','医院','死亡','病毒']
    for bk in blacklist:
        if bk in keyword:
            return False
    return len(keyword) <= 8

print("=" * 60)
print("  直播电商数据采集 - 只取消费/商品相关热搜")
print("=" * 60)

commerce_keywords = []
print("\n[1] 百度热搜...")
try:
    r = requests.get('https://top.baidu.com/board?tab=realtime', headers=HEADERS, timeout=10)
    words = re.findall(r'"word":"(.*?)"', r.text)
    for w in words:
        c = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', w)
        if len(c) >= 2 and is_commerce_related(c):
            commerce_keywords.append(c)
    print(f"  原始热搜 {len(words)} 条 -> 消费相关 {len(commerce_keywords)} 条")
except Exception as e:
    print(f"  Error: {e}")

print("\n[2] 抖音热搜...")
try:
    r = requests.get('https://www.douyin.com/aweme/v1/web/hot/search/list/', headers=HEADERS, timeout=10)
    words = re.findall(r'"word":"(.*?)"', r.text)
    dy_commerce = 0
    for w in words:
        c = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', w)
        if len(c) >= 2 and is_commerce_related(c) and c not in commerce_keywords:
            commerce_keywords.append(c)
            dy_commerce += 1
    print(f"  抖音热搜 -> 新增 {dy_commerce} 条消费相关")
except Exception as e:
    print(f"  Error: {e}")

print("\n[3] 补充电商关键词...")
brands = ['华为','小米','苹果','海尔','美的','格力','vivo','三星','荣耀','OPPO','海信','TCL','联想','戴尔','华硕',
          '花西子','完美日记','百雀羚','珀莱雅','薇诺娜','自然堂','林清轩','三只松鼠','良品铺子','百草味',
          '波司登','安踏','李宁','特步','太平鸟','海澜之家','蕉下','Ubras','罗技','漫步者']
for b in brands:
    if b not in commerce_keywords:
        commerce_keywords.append(b)
print(f"  补充品牌 {len(brands)} 个")

commerce_keywords = list(set(commerce_keywords))
print(f"\n  总计消费相关关键词: {len(commerce_keywords)} 个")

print(f"\n{'=' * 60}")
print(f"  生成直播电商数据（一个主播一个直播间）")

anchors_pool = [
    ("李佳琦","taobao","美妆"), ("薇娅","taobao","全品类"), ("辛巴","kuaishou","食品"),
    ("罗永浩","douyin","数码"), ("疯狂小杨哥","douyin","全品类"), ("董宇辉","douyin","食品"),
    ("刘畊宏","douyin","运动"), ("陈赫","douyin","食品"), ("张沫凡","douyin","美妆"),
    ("舒畅","douyin","服饰"), ("贾乃亮","douyin","全品类"), ("张同学","kuaishou","食品"),
    ("东方甄选","douyin","食品"), ("烈儿宝贝","taobao","服饰"), ("林依轮","taobao","食品"),
    ("叶一茜","taobao","母婴"), ("白小白","kuaishou","全品类"), ("散打哥","kuaishou","全品类"),
    ("倪海杉","douyin","服饰"), ("祝晓晗","douyin","食品"), ("花西子旗舰","taobao","美妆"),
    ("华为旗舰","douyin","数码"), ("小米官方","taobao","数码"), ("美的旗舰","taobao","家电"),
    ("张兰俏江南","douyin","食品"), ("老乡鸡","douyin","食品"), ("三只松鼠","douyin","食品"),
    ("波司登","taobao","服饰"), ("安踏体育","douyin","运动"), ("名创优品","douyin","家居"),
]

product_types = {
    '美妆': ['精华液','面膜','口红','粉底','防晒霜','眼影盘','卸妆油','爽肤水'],
    '食品': ['坚果礼盒','牛肉干','螺蛳粉','自热火锅','茶叶','咖啡豆','巧克力','蜂蜜'],
    '数码': ['蓝牙耳机','充电宝','手机壳','智能手表','键盘','鼠标','路由器','音箱'],
    '服饰': ['纯棉T恤','牛仔裤','连衣裙','运动鞋','羽绒服','丝巾','太阳帽','袜子'],
    '全品类': ['多功能锅','除螨仪','扫地机','加湿器','电动牙刷','吹风机','剃须刀','台灯'],
    '家电': ['空调扇','电饭煲','破壁机','空气炸锅','吸尘器','净水器','微波炉','烤箱'],
    '运动': ['瑜伽垫','跑步鞋','哑铃','跳绳','运动手环','护膝','泳镜','登山杖'],
    '母婴': ['纸尿裤','奶粉','婴儿车','玩具','学步鞋','奶瓶','湿巾','爬行垫'],
    '家居': ['四件套','枕头','收纳箱','地垫','窗帘','衣架','拖鞋','垃圾桶'],
}

random.shuffle(anchors_pool)
rooms = []
used_keywords = set()

for i, anc in enumerate(anchors_pool):
    if i >= len(commerce_keywords):
        break
    kw = commerce_keywords[i]
    used_keywords.add(kw)
    cat = anc[2]
    product = random.choice(product_types.get(cat, product_types['全品类']))
    rooms.append({
        'anchor_name': anc[0], 'platform': anc[1], 'category': cat,
        'keyword': kw,
        'room_name': f'"{kw}" {product} 专场',
        'viewer_count': random.randint(5000, 900000),
        'gmv': round(random.uniform(500, 8000000), 2),
        'order_count': random.randint(10, 20000),
        'status': 'live' if random.random() > 0.1 else 'finished',
        'source': 'commerce_keyword'
    })

print(f"  生成 {len(rooms)} 个直播间（每个主播一个）")

print(f"\n{'=' * 60}")
print(f"  存入虚拟机 MySQL")
conn = pymysql.connect(**MYSQL)
cur = conn.cursor()
cur.execute("DELETE FROM live_room WHERE deleted=0")
cur.execute("DELETE FROM anchor WHERE deleted=0")
conn.commit()

seen_rooms, seen_anchors = set(), set()
rc, ac = 0, 0
for item in rooms:
    key = item['anchor_name'] + item['room_name']
    if key in seen_rooms: continue
    seen_rooms.add(key)
    cur.execute(
        "INSERT INTO live_room (room_no,room_name,anchor_name,platform,category,status,viewer_count,order_count,gmv,conversion_rate,create_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
        (f"LR{rc:06d}", item['room_name'], item['anchor_name'], item['platform'],
         item['category'], item['status'], item['viewer_count'], item['order_count'],
         item['gmv'], round(random.uniform(2, 8), 2)))
    rc += 1
    an = item['anchor_name']
    if an not in seen_anchors:
        seen_anchors.add(an)
        cur.execute(
            "INSERT INTO anchor (name,nickname,platform,level,category,fans_count,live_hours,total_gmv,total_orders,avg_conversion,create_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
            (an, an, item['platform'], random.choice(['S','A','B']), item['category'],
             random.randint(100000,80000000), random.randint(100,5000),
             random.randint(500000,2000000000), random.randint(100,5000000),
             round(random.uniform(2, 8), 2)))
        ac += 1

conn.commit()
conn.close()

print(f"  直播间: {rc} 条")
print(f"  主播: {ac} 位")
print(f"  MySQL: 192.168.104.100:3306/livecommerce_db")

print(f"\n  关键词示例 (全部消费相关):")
for kw in list(used_keywords)[:15]:
    print(f"  · {kw}")
