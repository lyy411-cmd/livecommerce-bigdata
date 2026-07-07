#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""完善主播等级分布：确保 S/A/B/C 四级都存在且合理分布"""
import pymysql

MYSQL = {'host': '192.168.104.100', 'port': 3306, 'user': 'root',
         'password': '123456', 'database': 'livecommerce_db', 'charset': 'utf8mb4'}

def level_by_metrics(fans, gmv):
    """根据粉丝数和GMV划分 S/A/B/C 四级"""
    if gmv >= 1_000_000_000 or fans >= 50_000_000:
        return 'S'
    if gmv >= 100_000_000 or fans >= 10_000_000:
        return 'A'
    if gmv >= 10_000_000 or fans >= 1_000_000:
        return 'B'
    return 'C'

print("=" * 60)
print("  完善主播四级等级分布")
print("=" * 60)

conn = pymysql.connect(**MYSQL)
cur = conn.cursor()

# 1. 查看当前分布
cur.execute("SELECT level, COUNT(*) FROM anchor WHERE deleted=0 GROUP BY level")
print("\n当前等级分布:")
for lv, cnt in cur.fetchall():
    print(f"  {lv}级: {cnt}人")

# 2. 按粉丝+GMV重新计算等级
cur.execute("SELECT id, name, fans_count, total_gmv FROM anchor WHERE deleted=0")
rows = cur.fetchall()
updates = []
for aid, name, fans, gmv in rows:
    new_level = level_by_metrics(fans or 0, gmv or 0)
    updates.append((new_level, aid))

cur.executemany("UPDATE anchor SET level=%s WHERE id=%s", updates)
conn.commit()
print(f"\n已更新 {len(updates)} 位主播等级")

# 3. 如果缺少 B/C 级，补充一些底层主播
b_c_names = [
    ('小美', '小美呀', 'douyin', '美妆'),
    ('阿杰', '杰哥直播', 'douyin', '数码'),
    ('静静', '静静好物', 'taobao', '家居'),
    ('大鹏', '鹏哥严选', 'kuaishou', '食品'),
    ('丽丽', '丽丽穿搭', 'douyin', '服饰'),
    ('老王', '老王杂货铺', 'taobao', '全品类'),
    ('糖糖', '糖糖爱吃', 'douyin', '食品'),
    ('小北', '北北数码', 'kuaishou', '数码'),
]
cur.execute("SELECT name FROM anchor WHERE deleted=0")
existing = {r[0] for r in cur.fetchall()}
added = 0
for name, nickname, plat, cat in b_c_names:
    if name in existing:
        continue
    fans = __import__('random').randint(100_000, 900_000)
    gmv = __import__('random').randint(500_000, 8_000_000)
    level = level_by_metrics(fans, gmv)
    cur.execute(
        "INSERT INTO anchor (name,nickname,platform,level,category,fans_count,live_hours,total_gmv,total_orders,avg_conversion) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (name, nickname, plat, level, cat, fans,
         __import__('random').randint(100, 1500), gmv,
         __import__('random').randint(50, 5_000),
         round(__import__('random').uniform(1.5, 4.5), 2)))
    added += 1

conn.commit()
if added:
    print(f"补充 {added} 位 B/C 级主播")

# 4. 最终分布
cur.execute("SELECT level, COUNT(*) FROM anchor WHERE deleted=0 GROUP BY level ORDER BY level")
print("\n最终等级分布:")
for lv, cnt in cur.fetchall():
    print(f"  {lv}级: {cnt}人")

cur.execute("SELECT name, level, fans_count, total_gmv FROM anchor WHERE deleted=0 ORDER BY total_gmv DESC LIMIT 5")
print("\nTOP 5 主播:")
for name, lv, fans, gmv in cur.fetchall():
    print(f"  {name} [{lv}级] 粉丝 {fans:,} GMV {gmv:,.0f}")

conn.close()
print("\n  完成！刷新前端主播列表即可看到 S/A/B/C 四级。")
