#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hive 数据加载器
MySQL -> CSV -> HDFS (ODS) -> Hive ETL -> ADS -> MySQL (结果回写)

每日凌晨执行:
    python -m data_pipeline.hive_loader --date 2024-01-01
"""

import os, sys, json, time, logging, subprocess, tempfile, csv
import pymysql
import urllib.request

logger = logging.getLogger('HiveLoader')

HIVE_HOST = '192.168.104.100'
HIVE_PORT = 10000
HDFS_WEB = 'http://192.168.104.100:9870'
MYSQL_CONFIG = {
    'host': '192.168.104.100', 'port': 3306,
    'user': 'root', 'password': '123456',
    'database': 'livecommerce_db', 'charset': 'utf8mb4'
}
SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sql')


class HiveDataLoader:
    """MySQL -> HDFS -> Hive ETL 管道"""

    def __init__(self):
        self.hdfs_base = '/livecommerce'
        self.temp_dir = tempfile.mkdtemp(prefix='hive_loader_')

    def _get_mysql_conn(self):
        return pymysql.connect(**MYSQL_CONFIG, connect_timeout=10)

    # ============ Step 1: MySQL -> CSV -> HDFS ============

    def export_table_to_csv(self, table_name, dt, where_clause=''):
        """导出 MySQL 表到 CSV 文件"""
        csv_path = os.path.join(self.temp_dir, f'{table_name}_{dt}.csv')
        try:
            conn = self._get_mysql_conn()
            cursor = conn.cursor()

            # Build query
            if table_name == 'live_room':
                sql = f"SELECT room_no, room_name, anchor_name, platform, category, status, viewer_count, order_count, gmv, total_viewers, conversion_rate FROM live_room WHERE deleted=0 {where_clause}"
            elif table_name == 'order_info':
                sql = f"SELECT order_no, username, total_amount, status, platform, quantity, product_name, room_name, create_time FROM order_info WHERE deleted=0 {where_clause}"
            elif table_name == 'rt_danmaku':
                sql = f"SELECT event_id, room_id, platform, user_name, content, danmaku_type, event_time FROM rt_danmaku WHERE DATE(event_time) = '{dt}'"
            else:
                sql = f"SELECT * FROM {table_name} WHERE 1=1 {where_clause}"

            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            conn.close()

            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow([str(v) if v is not None else '' for v in row])

            logger.info(f"Exported {table_name}: {len(rows)} rows -> {csv_path}")
            return csv_path, columns, len(rows)
        except Exception as e:
            logger.error(f"Export {table_name} failed: {e}")
            return None, [], 0

    def upload_to_hdfs(self, local_path, hdfs_path):
        """上传文件到 HDFS via WebHDFS REST API"""
        try:
            # Create directory
            mkdir_url = f"{HDFS_WEB}/webhdfs/v1{hdfs_path}?op=MKDIRS"
            urllib.request.urlopen(urllib.request.Request(mkdir_url, method='PUT'), timeout=10)

            # Upload file
            with open(local_path, 'rb') as f:
                data = f.read()

            upload_url = f"{HDFS_WEB}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true"
            req = urllib.request.Request(upload_url, data, method='PUT')
            req.add_header('Content-Type', 'application/octet-stream')

            # WebHDFS redirect
            try:
                urllib.request.urlopen(req, timeout=30)
            except urllib.error.HTTPError as e:
                if e.code == 307:
                    redirect_url = e.headers.get('Location')
                    if redirect_url:
                        urllib.request.urlopen(urllib.request.Request(redirect_url, data, method='PUT'), timeout=30)

            logger.info(f"Uploaded to HDFS: {hdfs_path}")
            return True
        except Exception as e:
            logger.error(f"HDFS upload failed: {e}")
            return False

    # ============ Step 2: Execute Hive ETL ============

    def run_hive_etl(self, dt):
        """执行 Hive ETL SQL"""
        etl_file = os.path.join(SQL_DIR, 'hive_etl.sql')
        if not os.path.exists(etl_file):
            logger.error(f"ETL file not found: {etl_file}")
            return False

        try:
            from pyhive import hive
            conn = hive.connect(HIVE_HOST, HIVE_PORT, database='default')
            cursor = conn.cursor()

            with open(etl_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # Replace ${dt} with actual date
            sql_content = sql_content.replace('${dt}', dt)

            # Execute each statement
            for stmt in sql_content.split(';'):
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--'):
                    try:
                        cursor.execute(stmt)
                        logger.info(f"ETL executed: {stmt[:60]}...")
                    except Exception as e:
                        logger.warning(f"ETL statement failed: {e}")

            cursor.close()
            conn.close()
            return True
        except ImportError:
            logger.warning("pyhive not installed, skipping Hive ETL")
            return False
        except Exception as e:
            logger.error(f"Hive ETL failed: {e}")
            return False

    # ============ Step 3: ADS -> MySQL ============

    def sync_ads_to_mysql(self, dt):
        """将 Hive ADS 层结果回写到 MySQL"""
        try:
            from pyhive import hive
            hive_conn = hive.connect(HIVE_HOST, HIVE_PORT, database='ads')
            hive_cursor = hive_conn.cursor()

            mysql_conn = self._get_mysql_conn()
            mysql_cur = mysql_conn.cursor()

            # Sync dashboard KPI
            hive_cursor.execute(f"SELECT metric_name, metric_value, metric_unit, change_rate FROM ads_dashboard_kpi WHERE dt='{dt}'")
            for row in hive_cursor.fetchall():
                mysql_cur.execute(
                    "INSERT INTO ads_dashboard_kpi_cache (metric_name, metric_value, metric_unit, change_rate, dt) "
                    "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE metric_value=VALUES(metric_value)",
                    row + (dt,))

            # Sync anchor ranking
            hive_cursor.execute(f"SELECT rank_no, anchor_name, platform, gmv, orders FROM ads_anchor_gmv_rank WHERE dt='{dt}'")
            for row in hive_cursor.fetchall():
                mysql_cur.execute(
                    "INSERT INTO ads_anchor_rank_cache (rank_no, anchor_name, platform, gmv, orders, dt) "
                    "VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE gmv=VALUES(gmv)",
                    row + (dt,))

            mysql_conn.commit()
            mysql_conn.close()
            hive_cursor.close()
            hive_conn.close()

            logger.info(f"ADS results synced to MySQL for {dt}")
            return True
        except ImportError:
            logger.warning("pyhive not installed, skipping ADS sync")
            return False
        except Exception as e:
            logger.error(f"ADS sync failed: {e}")
            return False

    # ============ Full Pipeline ============

    def run_daily_etl(self, dt=None):
        """执行完整的每日 ETL 流程"""
        if not dt:
            from datetime import datetime, timedelta
            dt = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        logger.info(f"Starting daily ETL for {dt}")

        # Step 1: Export MySQL to HDFS
        tables = ['live_room', 'order_info', 'rt_danmaku']
        for table in tables:
            csv_path, columns, count = self.export_table_to_csv(table, dt)
            if csv_path and count > 0:
                hdfs_path = f"{self.hdfs_base}/ods/ods_{table}/dt={dt}/{table}.csv"
                self.upload_to_hdfs(csv_path, hdfs_path)

        # Step 2: Run Hive ETL
        self.run_hive_etl(dt)

        # Step 3: Sync ADS results
        self.sync_ads_to_mysql(dt)

        # Cleanup temp files
        import shutil
        try:
            shutil.rmtree(self.temp_dir)
        except:
            pass

        logger.info(f"Daily ETL completed for {dt}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[HiveLoader] %(message)s')
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=None, help='Date to process (yyyy-MM-dd)')
    args = parser.parse_args()

    loader = HiveDataLoader()
    loader.run_daily_etl(args.date)
