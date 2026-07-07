# -*- coding: utf-8 -*-
"""
数据预处理管道 - 清洗/去重/标准化/质量评分

使用方法:
    from data_pipeline.preprocessing import DataPipeline
    pipe = DataPipeline()
    clean_data = pipe.process(raw_data)
"""

import re
import json
import hashlib
import logging
from datetime import datetime
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Preprocessing')


class DataPipeline:
    """数据处理管道 - 6步处理流程"""

    def __init__(self):
        self.stats = {
            "raw_count": 0, "null_removed": 0, "duplicates_removed": 0,
            "format_fixed": 0, "anomaly_flagged": 0, "enriched_count": 0,
            "final_count": 0, "quality_scores": []
        }
        self.seen_ids = set()

    #====================================================================
    # Step 1: 空值处理
    #====================================================================
    def step_remove_null(self, data_list):
        """移除关键字段为空的数据"""
        clean = []
        for item in data_list:
            if not isinstance(item, dict):
                continue
            # 检查必须字段
            room_id = item.get('room_id') or item.get('roomId')
            platform = item.get('platform')
            if not room_id or not platform:
                self.stats['null_removed'] += 1
                logger.debug(f"NULL removed: missing id/platform")
                continue
            clean.append(item)
        self.stats['raw_count'] = len(data_list)
        logger.info(f"STEP1 NULL removal: {len(data_list)} -> {len(clean)} ({self.stats['null_removed']} removed)")
        return clean

    #====================================================================
    # Step 2: 去重
    #====================================================================
    def step_deduplicate(self, data_list):
        """去除重复数据（基于 room_id + platform 组合）"""
        seen = set()
        dedup = []
        for item in data_list:
            key = f"{item.get('platform')}_{item.get('room_id') or item.get('roomId')}"
            if key in seen:
                self.stats['duplicates_removed'] += 1
                continue
            seen.add(key)
            dedup.append(item)
        logger.info(f"STEP2 Dedup: {len(data_list)} -> {len(dedup)} ({self.stats['duplicates_removed']} duplicates)")
        return dedup

    #====================================================================
    # Step 3: 字段标准化
    #====================================================================
    def step_normalize(self, data_list):
        """统一字段格式、命名规范"""
        normalized = []
        for item in data_list:
            # 字段名统一映射（处理不同平台的不同字段名）
            mapping = {
                'roomId': 'room_id', 'roomName': 'room_name',
                'anchorName': 'anchor_name', 'anchorId': 'anchor_id',
                'viewerCount': 'viewer_count', 'likeCount': 'like_count',
                'commentCount': 'comment_count', 'orderCount': 'order_count',
                'productCount': 'product_count', 'dataSource': 'data_source',
                'crawlTime': 'crawl_time', 'totalAmount': 'total_amount',
                'totalOrders': 'total_orders', 'avgConversion': 'avg_conversion'
            }
            clean = {}
            for k, v in item.items():
                new_key = mapping.get(k, k)
                # 标准化值
                clean[new_key] = self._normalize_value(new_key, v)
            self.stats['format_fixed'] += 1
            normalized.append(clean)
        logger.info(f"STEP3 Normalization: {len(normalized)} records standardized")
        return normalized

    def _normalize_value(self, key, value):
        """标准化单个字段值"""
        if value is None:
            return self._default_value(key)

        # 平台名标准化
        if key == 'platform':
            return self._normalize_platform(value)

        # 状态标准化
        if key == 'status':
            return self._normalize_status(value)

        # 数字类型标准化
        if key in ['viewer_count', 'like_count', 'comment_count', 'order_count', 'fans_count']:
            if isinstance(value, str):
                value = value.replace('万', '0000').replace('+', '').replace(',', '')
                try:
                    return int(float(value))
                except:
                    return 0
            return max(0, int(value)) if value is not None else 0

        # GMV/金额标准化
        if key in ['gmv', 'total_gmv', 'amount', 'total_amount', 'price']:
            if isinstance(value, str):
                value = value.replace('￥', '').replace('$', '').replace('¥', '').replace(',', '')
                try:
                    return round(float(value), 2)
                except:
                    return 0.0
            return round(float(value), 2) if value is not None else 0.0

        return value

    def _normalize_platform(self, val):
        p = str(val).lower().strip()
        return {
            'douyin': 'douyin', '抖音': 'douyin', 'dy': 'douyin',
            'taobao': 'taobao', '淘宝': 'taobao', 'tb': 'taobao',
            'kuaishou': 'kuaishou', '快手': 'kuaishou', 'ks': 'kuaishou'
        }.get(p, 'other')

    def _normalize_status(self, val):
        s = str(val).lower().strip()
        return {
            'live': 'live', '正在直播': 'live', '1': 'live',
            'finished': 'finished', '已结束': 'finished', '0': 'finished',
            'paused': 'paused', '暂停': 'paused', '2': 'paused'
        }.get(s, 'finished')

    def _default_value(self, key):
        defaults = {
            'viewer_count': 0, 'like_count': 0, 'comment_count': 0,
            'gmv': 0.0, 'order_count': 0, 'status': 'finished',
            'data_source': 'unknown', 'platform': 'other', 'category': '其他'
        }
        return defaults.get(key, '')

    #====================================================================
    # Step 4: 异常检测
    #====================================================================
    def step_detect_anomaly(self, data_list):
        """检测并标记异常数据"""
        for item in data_list:
            anomalies = []

            if item.get('viewer_count', 0) > 10000000:
                anomalies.append('extreme_viewers')
            if item.get('gmv', 0) > 100000000:
                anomalies.append('extreme_gmv')
            if item.get('viewer_count', 0) > 0 and item.get('order_count', 0) == 0 \
                    and item.get('status') == 'live':
                anomalies.append('high_viewers_no_orders')
            if item.get('viewer_count', 0) < 10 and item.get('gmv', 0) > 100000:
                anomalies.append('suspicious_ratio')

            item['anomalies'] = anomalies
            if anomalies:
                self.stats['anomaly_flagged'] += 1

        logger.info(f"STEP4 Anomaly detection: {self.stats['anomaly_flagged']} flagged")
        return data_list

    #====================================================================
    # Step 5: 数据丰富
    #====================================================================
    def step_enrich(self, data_list):
        """补充计算字段"""
        for item in data_list:
            viewers = item.get('viewer_count', 1) or 1
            orders = item.get('order_count', 0)
            gmv = item.get('gmv', 0)

            # 转化率
            if viewers > 0:
                item['conversion_rate'] = round(orders / viewers * 100, 2)
            else:
                item['conversion_rate'] = 0.0

            # 客单价
            if orders > 0 and gmv > 0:
                item['avg_order_value'] = round(gmv / orders, 2)
            else:
                item['avg_order_value'] = 0.0

            # 人气等级
            if viewers > 500000:
                item['popularity_level'] = 'S'
            elif viewers > 100000:
                item['popularity_level'] = 'A'
            elif viewers > 10000:
                item['popularity_level'] = 'B'
            else:
                item['popularity_level'] = 'C'

            # 添加处理时间戳
            item['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.stats['enriched_count'] += 1

        logger.info(f"STEP5 Enrichment: {self.stats['enriched_count']} records enriched")
        return data_list

    #====================================================================
    # Step 6: 质量评分
    #====================================================================
    def step_quality_score(self, data_list):
        """计算数据质量评分"""
        for item in data_list:
            score = 100
            # 数据来源
            if item.get('data_source') == 'real':
                score += 5
            elif item.get('data_source') == 'unknown':
                score -= 10

            # 字段完整性（检查关键字段非空）
            key_fields = ['room_id', 'room_name', 'anchor_name', 'platform', 'viewer_count']
            filled = sum(1 for f in key_fields if item.get(f))
            score += (filled / len(key_fields) * 20) - 10

            # 异常惩罚
            anomalies = item.get('anomalies', [])
            score -= len(anomalies) * 15

            # 数据新鲜度（1小时内 = 满分）
            crawl_time = item.get('crawl_time', '')
            try:
                ct = datetime.strptime(crawl_time, "%Y-%m-%d %H:%M:%S")
                hours_ago = (datetime.now() - ct).total_seconds() / 3600
                if hours_ago > 24:
                    score -= 20
                elif hours_ago > 1:
                    score -= int(hours_ago)
            except:
                score -= 10

            item['quality_score'] = max(0, min(100, score))
            self.stats['quality_scores'].append(item['quality_score'])

        avg = sum(self.stats['quality_scores']) / max(len(self.stats['quality_scores']), 1)
        logger.info(f"STEP6 Quality scoring: avg={avg:.1f}/100")
        return data_list

    #====================================================================
    # 完整处理流程
    #====================================================================
    def process(self, data_list):
        """执行完整的6步处理流程"""
        print(f"\n{'='*60}")
        print(f"  Data Pipeline Started - {len(data_list)} raw records")
        print(f"{'='*60}")

        data = self.step_remove_null(data_list)
        data = self.step_deduplicate(data)
        data = self.step_normalize(data)
        data = self.step_detect_anomaly(data)
        data = self.step_enrich(data)
        data = self.step_quality_score(data)

        self.stats['final_count'] = len(data)

        print(f"\n{'='*60}")
        print(f"  Pipeline Summary:")
        print(f"    Raw:      {self.stats['raw_count']}")
        print(f"    Null:     {self.stats['null_removed']} removed")
        print(f"    Dup:      {self.stats['duplicates_removed']} removed")
        print(f"    Anomaly:  {self.stats['anomaly_flagged']} flagged")
        print(f"    Final:    {self.stats['final_count']}")
        print(f"    Quality:  {sum(self.stats['quality_scores'])/max(len(self.stats['quality_scores']),1):.1f}/100")
        print(f"{'='*60}\n")

        return data

    def process_file(self, input_path, output_path):
        """处理 JSON 文件"""
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        clean = self.process(data)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(clean, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(clean)} clean records to {output_path}")


# ============================================================
# 文本清洗工具（弹幕/评论预处理）
# ============================================================
class TextCleaner:
    """文本数据清洗 - 弹幕、评论等内容预处理"""

    @staticmethod
    def clean_danmaku(text):
        """清洗弹幕文本"""
        if not text:
            return ""
        # 去除重复字符（呵呵呵呵 -> 呵呵）
        text = re.sub(r'(.)\1{3,}', r'\1\1', text)
        # 去除纯表情字符
        text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9!！?？.。,，\s]', '', text)
        # 去除多余空格
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def extract_keywords(text):
        """提取关键词"""
        stopwords = {'的', '了', '吗', '呢', '啊', '吧', '我', '你', '是', '在', '有', '和', '就'}
        words = re.findall(r'[\u4e00-\u9fa5]+', text)
        return [w for w in words if len(w) >= 2 and w not in stopwords]

    @staticmethod
    def sentiment_score(text):
        """简单的文本情感分析（正/负/中）"""
        positive = {'好', '棒', '喜欢', '爱', '赞', '666', '种草', '冲', '买', '绝', '好看', '实惠'}
        negative = {'差', '垃圾', '坑', '退货', '骗', '不', '假', '贵'}
        pos_count = sum(1 for w in positive if w in text)
        neg_count = sum(1 for w in negative if w in text)
        if pos_count > neg_count:
            return 1  # 正面
        elif neg_count > pos_count:
            return -1  # 负面
        return 0  # 中性


if __name__ == '__main__':
    # 测试
    sample_data = [
        {"roomId": "D-123", "platform": "douyin", "viewerCount": "5.2万", "gmv": 123.45, "status": "live"},
        {"roomId": "D-123", "platform": "douyin", "viewerCount": "5.2万", "gmv": 123.45, "status": "live"},
        {"roomId": "D-456", "platform": "kuaishou", "viewerCount": 800, "gmv": 999999999, "status": "正在直播"},
        {"roomId": None, "platform": None, "viewerCount": 0},
    ]
    pipe = DataPipeline()
    clean = pipe.process(sample_data)
    for r in clean:
        print(r)
