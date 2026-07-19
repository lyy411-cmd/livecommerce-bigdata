# -*- coding: utf-8 -*-
"""
Kafka 消息生产者
将直播数据实时发送到 Kafka 集群，供下游 Flink / Spark 消费。

支持的 Topic:
    - live_room_events   直播间状态事件（开播、更新、下播等）
    - danmaku_events     弹幕 / 评论事件
    - product_events     商品上架 / 更新事件
    - order_events       订单事件（创建、支付、取消等）

依赖:
    kafka-python  (pip install kafka-python)
    若 kafka-python 不可用，所有发送操作会静默降级为 no-op。
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Topic 常量
# ---------------------------------------------------------------------------
TOPIC_ROOM_EVENTS    = 'live_room_events'
TOPIC_DANMAKU_EVENTS = 'danmaku_events'
TOPIC_PRODUCT_EVENTS = 'product_events'
TOPIC_ORDER_EVENTS   = 'order_events'

DEFAULT_BOOTSTRAP_SERVERS = ['192.168.104.100:9092']


class LiveCommerceKafkaProducer:
    """
    直播电商 Kafka 生产者。

    将直播间事件、弹幕消息、商品变更、订单流水序列化为 JSON
    并发送到对应的 Kafka Topic。

    使用方式::

        producer = LiveCommerceKafkaProducer()
        if producer.available:
            producer.send_danmaku(danmaku_data, room_id='12345')
        producer.close()
    """

    def __init__(
        self,
        bootstrap_servers: Optional[List[str]] = None,
    ) -> None:
        """
        :param bootstrap_servers: Kafka broker 地址列表，
                                  默认 ['192.168.104.100:9092']
        """
        self._servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
        self._producer = None
        self._available = False

        try:
            import warnings
            from kafka import KafkaProducer  # kafka-python
            # Suppress deprecation warnings about lambda serializers
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                self._producer = KafkaProducer(
                    bootstrap_servers=self._servers,
                    value_serializer=lambda v: json.dumps(
                        v, ensure_ascii=False
                    ).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    api_version=(0, 10, 0),
                )
            self._available = True
            logger.info(
                "Kafka 生产者已初始化，服务器: %s", self._servers
            )
        except ImportError:
            logger.warning(
                "kafka-python 未安装，Kafka 发送功能不可用。"
                "请执行: pip install kafka-python"
            )
        except Exception as exc:
            logger.warning(
                "Kafka 生产者初始化失败: %s。发送操作将静默跳过。", exc
            )

    # ------------------------------------------------------------------
    #  属性
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Kafka 生产者是否可用（依赖已安装且连接正常）。"""
        return self._available

    # ------------------------------------------------------------------
    #  内部发送
    # ------------------------------------------------------------------

    def _send(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any],
    ) -> None:
        """
        内部发送方法，带异常保护。

        :param topic: 目标 Topic
        :param key:   消息 Key（用于分区路由）
        :param value: 消息体（将被 JSON 序列化）
        """
        if not self._available or self._producer is None:
            return
        try:
            future = self._producer.send(topic, key=key, value=value)
            # 非阻塞：不等待 broker 确认，由 KafkaProducer 内部重试机制保障
            future.add_callback(
                lambda meta: logger.debug(
                    "已发送 -> topic=%s partition=%d offset=%d",
                    meta.topic, meta.partition, meta.offset,
                )
            )
            future.add_errback(
                lambda exc: logger.error(
                    "发送失败 -> topic=%s key=%s error=%s",
                    topic, key, exc,
                )
            )
        except Exception as exc:
            logger.error("Kafka 发送异常: topic=%s error=%s", topic, exc)

    # ------------------------------------------------------------------
    #  直播间事件
    # ------------------------------------------------------------------

    def send_room_event(
        self,
        room_data: Union[Dict[str, Any], Any],
        event_type: str = 'room_update',
    ) -> None:
        """
        发送直播间状态事件到 'live_room_events' Topic。

        :param room_data: 直播间数据，可以是 dict 或 LiveRoomData 对象
                          （对象需有对应属性）
        :param event_type: 事件类型，如 'room_update', 'live_start',
                           'live_end', 'stats_update'
        """
        # 兼容对象和字典两种形式
        if isinstance(room_data, dict):
            d = room_data
        else:
            d = vars(room_data) if hasattr(room_data, '__dict__') else {}

        message: Dict[str, Any] = {
            'event_id':    str(uuid.uuid4()),
            'event_type':  event_type,
            'timestamp':   int(time.time() * 1000),
            'platform':    d.get('platform', 'douyin'),
            'room_id':     str(d.get('room_id', '')),
            'room_name':   d.get('room_name', ''),
            'anchor_name': d.get('anchor_name', ''),
            'category':    d.get('category', ''),
            'status':      d.get('status', ''),
            'viewer_count':    d.get('viewer_count', 0),
            'order_count':     d.get('order_count', 0),
            'gmv':             d.get('gmv', 0.0),
            'peak_viewers':    d.get('peak_viewers', 0),
            'live_url':        d.get('live_url', ''),
        }

        key = str(d.get('room_id', ''))
        self._send(TOPIC_ROOM_EVENTS, key, message)

    # ------------------------------------------------------------------
    #  弹幕事件
    # ------------------------------------------------------------------

    def send_danmaku(
        self,
        danmaku_data: Dict[str, Any],
        room_id: str,
        platform: str = 'douyin',
        room_name: str = '',
    ) -> None:
        """
        发送单条弹幕事件到 'danmaku_events' Topic。

        :param danmaku_data: 弹幕数据字典，需包含:
                             - user_id   (str/int)  用户 ID
                             - user_name (str)      用户昵称
                             - content   (str)      弹幕文本
                             - danmaku_type (str)   弹幕类型
                               ('comment' / 'gift' / 'like' / 'enter' / 'follow')
        :param room_id:   直播间 ID
        :param platform:  平台标识，默认 'douyin'
        :param room_name: 直播间名称（可选）
        """
        message: Dict[str, Any] = {
            'event_id':     str(uuid.uuid4()),
            'event_type':   'danmaku',
            'timestamp':    int(time.time() * 1000),
            'platform':     platform,
            'room_id':      str(room_id),
            'room_name':    room_name,
            'user_id':      str(danmaku_data.get('user_id', '')),
            'user_name':    danmaku_data.get('user_name', ''),
            'content':      danmaku_data.get('content', ''),
            'danmaku_type': danmaku_data.get('danmaku_type', 'comment'),
        }

        key = str(room_id)
        self._send(TOPIC_DANMAKU_EVENTS, key, message)

    def send_danmaku_batch(
        self,
        danmaku_list: List[Dict[str, Any]],
        room_id: str,
        platform: str = 'douyin',
        room_name: str = '',
    ) -> None:
        """
        批量发送弹幕事件。

        :param danmaku_list: 弹幕数据字典列表
        :param room_id:      直播间 ID
        :param platform:     平台标识
        :param room_name:    直播间名称
        """
        for item in danmaku_list:
            self.send_danmaku(item, room_id, platform, room_name)

    # ------------------------------------------------------------------
    #  商品事件
    # ------------------------------------------------------------------

    def send_product_event(
        self,
        product_data: Dict[str, Any],
        room_id: str,
        platform: str = 'douyin',
    ) -> None:
        """
        发送商品事件到 'product_events' Topic。

        :param product_data: 商品数据字典，可包含:
                             - product_id    商品 ID
                             - product_name  商品名称
                             - price         价格
                             - original_price 原价
                             - sales_count   销量
                             - image_url     图片链接
                             - category      分类
                             - event_type    事件类型
                               ('product_add' / 'product_update' / 'product_remove')
        :param room_id:  直播间 ID
        :param platform: 平台标识
        """
        message: Dict[str, Any] = {
            'event_id':   str(uuid.uuid4()),
            'event_type': product_data.get('event_type', 'product_update'),
            'timestamp':  int(time.time() * 1000),
            'platform':   platform,
            'room_id':    str(room_id),
            'product_id': str(product_data.get('product_id', '')),
            'product_name':   product_data.get('product_name', ''),
            'price':          product_data.get('price', 0.0),
            'original_price': product_data.get('original_price', 0.0),
            'sales_count':    product_data.get('sales_count', 0),
            'image_url':      product_data.get('image_url', ''),
            'category':       product_data.get('category', ''),
        }

        key = str(product_data.get('product_id', room_id))
        self._send(TOPIC_PRODUCT_EVENTS, key, message)

    # ------------------------------------------------------------------
    #  订单事件
    # ------------------------------------------------------------------

    def send_order_event(
        self,
        order_data: Dict[str, Any],
        event_type: str = 'order_created',
    ) -> None:
        """
        发送订单事件到 'order_events' Topic。

        :param order_data: 订单数据字典，可包含:
                           - order_id      订单 ID
                           - room_id       直播间 ID
                           - platform      平台
                           - user_id       下单用户 ID
                           - product_id    商品 ID
                           - product_name  商品名称
                           - quantity      数量
                           - unit_price    单价
                           - total_amount  总金额
                           - status        订单状态
        :param event_type: 事件类型，如 'order_created', 'order_paid',
                           'order_cancelled', 'order_refunded'
        """
        message: Dict[str, Any] = {
            'event_id':     str(uuid.uuid4()),
            'event_type':   event_type,
            'timestamp':    int(time.time() * 1000),
            'platform':     order_data.get('platform', 'douyin'),
            'room_id':      str(order_data.get('room_id', '')),
            'order_id':     str(order_data.get('order_id', '')),
            'user_id':      str(order_data.get('user_id', '')),
            'product_id':   str(order_data.get('product_id', '')),
            'product_name': order_data.get('product_name', ''),
            'quantity':     order_data.get('quantity', 1),
            'unit_price':   order_data.get('unit_price', 0.0),
            'total_amount': order_data.get('total_amount', 0.0),
            'status':       order_data.get('status', ''),
        }

        key = str(order_data.get('order_id', order_data.get('room_id', '')))
        self._send(TOPIC_ORDER_EVENTS, key, message)

    # ------------------------------------------------------------------
    #  生命周期
    # ------------------------------------------------------------------

    def flush(self, timeout: float = 10.0) -> None:
        """
        刷新发送缓冲区，阻塞直到所有待发数据发送完毕或超时。

        :param timeout: 超时时间（秒）
        """
        if self._available and self._producer is not None:
            try:
                self._producer.flush(timeout=timeout)
            except Exception as exc:
                logger.error("Kafka flush 失败: %s", exc)

    def close(self, timeout: float = 10.0) -> None:
        """
        关闭 Kafka 生产者，释放资源。

        :param timeout: 关闭超时时间（秒）
        """
        if self._available and self._producer is not None:
            try:
                self._producer.flush(timeout=timeout)
                self._producer.close(timeout=timeout)
                logger.info("Kafka 生产者已关闭")
            except Exception as exc:
                logger.error("Kafka 生产者关闭异常: %s", exc)
            finally:
                self._available = False
