# -*- coding: utf-8 -*-
"""
Kafka Consumer - 从 Kafka Topic 读取数据写入 MySQL
消费 topics: danmaku_events, live_room_events, product_events, order_events
写入 MySQL 表: rt_danmaku, rt_room_stats, rt_product
"""

import json
import threading
import time
import logging

logger = logging.getLogger('KafkaConsumer')

class LiveCommerceKafkaConsumer:
    """Kafka 消费者 - 消费消息写入 MySQL + 推送给外部回调"""
    
    def __init__(self, mysql_config=None, bootstrap_servers=None):
        self.mysql_config = mysql_config or {
            'host': '192.168.104.100', 'port': 3306,
            'user': 'root', 'password': '123456',
            'database': 'livecommerce_db', 'charset': 'utf8mb4'
        }
        self.bootstrap_servers = bootstrap_servers or ['192.168.104.100:9092']
        self.consumer = None
        self.running = False
        self.event_callbacks = []  # for SSE/WebSocket push
        self.stats = {
            'danmaku_count': 0,
            'room_events': 0,
            'product_events': 0,
            'errors': 0,
            'started_at': None
        }
    
    def _init_consumer(self):
        try:
            from kafka import KafkaConsumer
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.value.decode('utf-8')),
                group_id='backend-consumer-group',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                max_poll_records=100,
                consumer_timeout_ms=1000
            )
            logger.info(f"Kafka Consumer connected: {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.warning(f"Kafka Consumer init failed: {e}")
            self.consumer = None
            return False
    
    @property
    def available(self):
        return self.consumer is not None
    
    def register_callback(self, callback):
        """Register callback(topic, data) for external event push"""
        self.event_callbacks.append(callback)
    
    def start(self):
        if not self._init_consumer():
            return False
        self.consumer.subscribe([
            'danmaku_events', 'live_room_events', 'product_events', 'order_events'
        ])
        self.running = True
        self.stats['started_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        threading.Thread(target=self._consume_loop, daemon=True).start()
        logger.info("Kafka Consumer started")
        return True
    
    def _consume_loop(self):
        while self.running:
            try:
                records = self.consumer.poll(timeout_ms=1000)
                for tp, messages in records.items():
                    for msg in messages:
                        try:
                            self._process_message(msg.topic, msg.value)
                        except Exception as e:
                            self.stats['errors'] += 1
                            logger.debug(f"Process msg error: {e}")
            except Exception as e:
                if self.running:
                    logger.error(f"Consumer loop error: {e}")
                    time.sleep(2)
    
    def _process_message(self, topic, data):
        if topic == 'danmaku_events':
            self._save_danmaku(data)
            self.stats['danmaku_count'] += 1
        elif topic == 'live_room_events':
            self._update_room_stats(data)
            self.stats['room_events'] += 1
        elif topic == 'product_events':
            self._save_product(data)
            self.stats['product_events'] += 1
        elif topic == 'order_events':
            pass  # Orders handled by existing order_simulator
        
        for cb in self.event_callbacks:
            try:
                cb(topic, data)
            except:
                pass
    
    def _get_conn(self):
        import pymysql
        return pymysql.connect(**self.mysql_config, connect_timeout=5)
    
    def _save_danmaku(self, data):
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            ts = data.get('timestamp')
            event_time = f"FROM_UNIXTIME({ts}/1000)" if ts else "NOW(3)"
            cur.execute(
                "INSERT INTO rt_danmaku (event_id, room_id, platform, user_id, user_name, "
                "content, danmaku_type, event_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (data.get('event_id',''), data.get('room_id',''), data.get('platform',''),
                 data.get('user_id',''), data.get('user_name',''), data.get('content',''),
                 data.get('danmaku_type','comment'),
                 None)  # event_time set by MySQL DEFAULT
            )
            # Also update danmaku count in rt_room_stats
            cur.execute(
                "UPDATE rt_room_stats SET total_danmaku = total_danmaku + 1 WHERE room_id = %s",
                (data.get('room_id',''),)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Save danmaku error: {e}")
    
    def _update_room_stats(self, data):
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO rt_room_stats (room_id, room_name, anchor_name, platform, "
                "category, status, current_viewers, peak_viewers, total_orders, total_gmv, live_url) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE "
                "room_name=VALUES(room_name), anchor_name=VALUES(anchor_name), "
                "current_viewers=VALUES(current_viewers), "
                "peak_viewers=GREATEST(peak_viewers, VALUES(peak_viewers)), "
                "total_orders=VALUES(total_orders), total_gmv=VALUES(total_gmv), "
                "status=VALUES(status), live_url=COALESCE(VALUES(live_url), live_url)",
                (data.get('room_id',''), data.get('room_name',''),
                 data.get('anchor_name',''), data.get('platform',''),
                 data.get('category',''), data.get('status','live'),
                 int(data.get('viewer_count',0)), 
                 int(data.get('peak_viewers', data.get('viewer_count',0))),
                 int(data.get('order_count',0)),
                 float(data.get('gmv',0)),
                 data.get('live_url',''))
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Update room stats error: {e}")
    
    def _save_product(self, data):
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO rt_product (product_id, room_id, platform, product_name, "
                "price, original_price, sales, category, image_url) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE "
                "price=VALUES(price), sales=VALUES(sales), "
                "product_name=VALUES(product_name), image_url=VALUES(image_url)",
                (data.get('product_id',''), data.get('room_id',''),
                 data.get('platform',''), data.get('product_name',''),
                 float(data.get('price',0)),
                 float(data.get('original_price', data.get('price',0))),
                 int(data.get('sales',0)),
                 data.get('category',''), data.get('image_url',''))
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Save product error: {e}")
    
    def stop(self):
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
            except:
                pass
            self.consumer = None
            logger.info("Kafka Consumer stopped")
