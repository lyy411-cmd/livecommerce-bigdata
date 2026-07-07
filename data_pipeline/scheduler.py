# -*- coding: utf-8 -*-
"""
数据采集调度器 - 定时采集 -> 预处理 -> 入库
用法: python data_pipeline/scheduler.py
"""
import sys
import os
import time
import threading
import json
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline.spider_engine import SpiderScheduler, LiveRoomData, ProductData, DanmakuData
from data_pipeline.preprocessing import DataPipeline, TextCleaner
from data_pipeline.storage import MySQLStorage, HDFSStorage, JSONStorage

logging.basicConfig(level=logging.INFO, format='[%(name)s] %(message)s')
logger = logging.getLogger('Scheduler')


class DataCollectionPipeline:
    """完整的数据采集与存储管道"""

    def __init__(self, platforms=None, collect_interval=300):
        self.platforms = platforms or ["douyin", "taobao", "kuaishou"]
        self.collect_interval = collect_interval  # 采集间隔（秒）
        self.running = False
        self.thread = None

        # 组件初始化
        self.spider_scheduler = SpiderScheduler(self.platforms)
        self.preprocessor = DataPipeline()
        self.text_cleaner = TextCleaner()
        self.mysql = MySQLStorage()
        self.hdfs = HDFSStorage()
        self.json_store = JSONStorage()

        # 统计
        self.stats = {
            'total_collects': 0, 'total_rooms': 0, 'total_anchors': 0,
            'mysql_writes': 0, 'hdfs_writes': 0, 'start_time': '',
            'last_collect': '', 'errors': 0
        }

    def start(self):
        """启动定时采集"""
        self.spider_scheduler.init_spiders()
        self.running = True
        self.stats['start_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Pipeline started: platforms={self.platforms}, interval={self.collect_interval}s")

        # 立即执行一次
        self.run_once()

        # 定时循环
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        while self.running:
            time.sleep(self.collect_interval)
            self.run_once()

    def stop(self):
        self.running = False
        self.spider_scheduler.close()
        logger.info("Pipeline stopped")

    def run_once(self):
        """执行一次完整采集流程"""
        logger.info("=" * 50)
        logger.info(f"Collection #{self.stats['total_collects'] + 1} starting...")

        try:
            # 1. 爬取数据
            raw_data = self.spider_scheduler.crawl_all(limit_per_platform=15)
            raw_dicts = [r.to_dict() for r in raw_data] if raw_data else []

            if not raw_dicts:
                logger.warning("No data collected")
                return

            # 2. 数据预处理
            clean_data = self.preprocessor.process(raw_dicts)

            # 3. 弹幕文本清洗
            for item in clean_data:
                if 'danmaku' in item:
                    item['danmaku_count'] = len(item['danmaku'])
                if item.get('room_name'):
                    item['room_name'] = self.text_cleaner.clean_danmaku(item['room_name'])

            # 4. 存储到 MySQL（实时业务数据）
            try:
                self.mysql.save_rooms(clean_data)
                self.stats['mysql_writes'] += 1

                # 提取主播数据
                anchors = []
                seen = set()
                for r in clean_data:
                    key = r.get('anchor_name', '')
                    if key and key not in seen:
                        seen.add(key)
                        anchors.append(r)
                self.mysql.save_anchors(anchors)
                self.stats['total_anchors'] += len(anchors)
            except Exception as e:
                logger.error(f"MySQL write error: {e}")
                self.stats['errors'] += 1

            # 5. 存储到 HDFS（原始数据归档）
            try:
                self.hdfs.save(raw_dicts, f"live_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                self.stats['hdfs_writes'] += 1
            except:
                pass  # HDFS 不可用时跳过

            # 6. 存储到本地 JSON（备份）
            backup_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_backup')
            os.makedirs(backup_dir, exist_ok=True)
            self.json_store.save(clean_data, os.path.join(backup_dir, f"clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"))

            # 7. 更新统计
            self.stats['total_collects'] += 1
            self.stats['total_rooms'] += len(clean_data)
            self.stats['last_collect'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            logger.info(f"Collection complete: {len(clean_data)} rooms saved")
            logger.info(f"Total: {self.stats}")

        except Exception as e:
            logger.error(f"Collection error: {e}", exc_info=True)
            self.stats['errors'] += 1

    def get_stats(self):
        return self.stats


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Live Commerce Data Pipeline')
    parser.add_argument('--platforms', nargs='+', default=['douyin', 'taobao', 'kuaishou'])
    parser.add_argument('--interval', type=int, default=300, help='Collection interval in seconds')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    args = parser.parse_args()

    pipeline = DataCollectionPipeline(platforms=args.platforms, collect_interval=args.interval)

    if args.once:
        pipeline.spider_scheduler.init_spiders()
        pipeline.run_once()
        pipeline.stop()
    else:
        pipeline.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pipeline.stop()


if __name__ == '__main__':
    main()
