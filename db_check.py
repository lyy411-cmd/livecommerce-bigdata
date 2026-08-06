import pymysql
conn = pymysql.connect(host='192.168.104.100', user='root', password='123456', database='livecommerce_db', charset='utf8mb4', connect_timeout=5)
cur = conn.cursor()
cur.execute('SHOW COLUMNS FROM live_room')
print('表字段:', [r[0] for r in cur.fetchall()])
cur.execute("SELECT COUNT(*) FROM live_room WHERE status='live' AND has_shopping_cart=1 AND data_source='real'")
print('真实直播带货:', cur.fetchone()[0])
cur.execute("SELECT id,title,status,has_shopping_cart,data_source,create_time,update_time FROM live_room WHERE status='live' AND has_shopping_cart=1 AND data_source='real' ORDER BY update_time DESC LIMIT 3")
print('最近3条:')
for r in cur.fetchall():
    print(r)
conn.close()
