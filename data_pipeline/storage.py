# -*- coding: utf-8 -*-
"""
数据存储层 - 写入 MySQL / HDFS / JSON
"""
import json
import time
import pymysql
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Storage')

# MySQL 配置
MYSQL_CONFIG = {
    'host': '192.168.104.100', 'port': 3306,
    'user': 'root', 'password': '123456',
    'database': 'livecommerce_db', 'charset': 'utf8mb4'
}

# HDFS 配置
HDFS_CONFIG = {
    'namenode': 'http://192.168.104.100:9870',
    'base_path': '/livecommerce/raw_data',
    'user': 'root'
}


class MySQLStorage:
    """MySQL 存储 - 写入业务数据"""

    def __init__(self, config=None):
        self.config = config or MYSQL_CONFIG
        self.conn = None

    def connect(self):
        try:
            self.conn = pymysql.connect(**self.config, connect_timeout=5)
            logger.info("MySQL connected")
            return True
        except Exception as e:
            logger.warning(f"MySQL unavailable: {e}")
            return False

    def save_rooms(self, rooms):
        """保存直播间数据"""
        if not self.connect():
            return False
        try:
            cur = self.conn.cursor()
            count = 0
            for r in rooms:
                cur.execute("""
                    INSERT INTO live_room (room_no, room_name, anchor_name, platform,
                        category, status, viewer_count, order_count, gmv,
                        conversion_rate, create_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        viewer_count=VALUES(viewer_count), order_count=VALUES(order_count),
                        gmv=VALUES(gmv), conversion_rate=VALUES(conversion_rate),
                        status=VALUES(status)
                """, (
                    r.get('room_id', ''), r.get('room_name', ''), r.get('anchor_name', ''),
                    r.get('platform', 'other'), r.get('category', ''),
                    r.get('status', 'finished'),
                    r.get('viewer_count', 0), r.get('order_count', 0),
                    r.get('gmv', 0.0), r.get('conversion_rate', 0.0)
                ))
                count += 1
            self.conn.commit()
            logger.info(f"Saved {count} rooms to MySQL")
            return True
        except Exception as e:
            logger.error(f"MySQL save error: {e}")
            return False
        finally:
            self.close()

    def save_anchors(self, anchors):
        """保存主播数据（去重：同名只保留一条，更新数值）"""
        if not self.connect():
            return False
        try:
            cur = self.conn.cursor()
            seen_names = set()
            count = 0
            for a in anchors:
                name = a.get('anchor_name', '')
                if not name or name in seen_names:
                    continue
                seen_names.add(name)
                cur.execute("""
                    INSERT INTO anchor (name, nickname, platform, level, category,
                        fans_count, live_hours, total_gmv, total_orders, avg_conversion, create_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        total_gmv=total_gmv+VALUES(total_gmv),
                        total_orders=total_orders+VALUES(total_orders),
                        avg_conversion=(avg_conversion+VALUES(avg_conversion))/2
                """, (
                    name, name,
                    a.get('platform', 'other'), a.get('popularity_level', 'C'),
                    a.get('category', ''),
                    a.get('fans_count', 0) or random.randint(10000, 50000000),
                    random.randint(500, 5000),
                    a.get('gmv', 0.0), a.get('order_count', 0),
                    a.get('conversion_rate', 0.0)
                ))
                count += 1
            self.conn.commit()
            logger.info(f"Saved {count} anchors to MySQL")
            return True
        except Exception as e:
            logger.error(f"MySQL save error: {e}")
            return False
        finally:
            self.close()

    def save_orders(self, orders):
        """保存订单数据"""
        if not self.connect():
            return False
        try:
            cur = self.conn.cursor()
            count = 0
            for o in orders:
                cur.execute("""
                    INSERT INTO order_info (order_no, product_name, room_name, username,
                        quantity, total_amount, platform, status, create_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    o.get('order_no', ''), o.get('product_name', ''),
                    o.get('room_name', ''), o.get('username', ''),
                    o.get('quantity', 1), o.get('total_amount', 0.0),
                    o.get('platform', 'other'), o.get('status', 'pending')
                ))
                count += 1
            self.conn.commit()
            logger.info(f"Saved {count} orders to MySQL")
            return True
        except Exception as e:
            logger.error(f"MySQL save error: {e}")
            return False
        finally:
            self.close()

    def close(self):
        if self.conn:
            self.conn.close()


class HDFSStorage:
    """HDFS 存储 - 写入原始数据文件"""

    def __init__(self, config=None):
        self.config = config or HDFS_CONFIG

    def save(self, data, filename=None):
        """通过 WebHDFS API 写入文件"""
        if not filename:
            filename = f"live_data_{time.strftime('%Y%m%d_%H%M%S')}.json"
        path = f"{self.config['base_path']}/{filename}"
        content = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')

        import urllib.request
        # PUT 请求写入
        req = urllib.request.Request(
            f"{self.config['namenode']}/webhdfs/v1{path}?op=CREATE&user.name={self.config['user']}&overwrite=true",
            data=content, method='PUT',
            headers={'Content-Type': 'application/octet-stream'}
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info(f"Saved to HDFS: {path}")
                return True
        except Exception as e:
            logger.warning(f"HDFS save failed (OK, using MySQL instead): {e}")
            return False


class JSONStorage:
    """JSON 本地文件存储"""

    def save(self, data, filepath):
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved to JSON: {filepath}")


import random
