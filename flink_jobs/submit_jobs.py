#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink SQL 任务提交脚本
通过 Flink SQL Gateway REST API 提交实时分析任务

用法:
    python flink_jobs/submit_jobs.py                    # 提交所有任务
    python flink_jobs/submit_jobs.py --list             # 列出当前任务
    python flink_jobs/submit_jobs.py --stop <job-id>    # 停止任务
"""

import os, sys, json, time, argparse, logging
import urllib.request, urllib.error

FLINK_REST = 'http://192.168.104.100:8081'
SQL_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.INFO, format='[FlinkSubmit] %(message)s')
logger = logging.getLogger()


def http_get(url, timeout=10):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return None


def http_post(url, data, timeout=30):
    try:
        body = json.dumps(data).encode('utf-8')
        req = urllib.request.Request(url, body, {'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return {'error': str(e)}


def check_flink_available():
    """检查 Flink 是否可用"""
    overview = http_get(f'{FLINK_REST}/overview')
    if overview:
        logger.info(f"Flink available: {overview.get('slots-total', 0)} slots, "
                    f"{overview.get('jobs-running', 0)} running jobs")
        return True
    logger.error(f"Flink not available at {FLINK_REST}")
    return False


def list_jobs():
    """列出所有 Flink 任务"""
    data = http_get(f'{FLINK_REST}/jobs')
    if not data:
        return []
    jobs = []
    for j in data.get('jobs', []):
        detail = http_get(f'{FLINK_REST}/jobs/{j["id"]}')
        jobs.append({
            'id': j['id'],
            'name': detail.get('name', 'unknown') if detail else 'unknown',
            'status': j.get('status', 'UNKNOWN'),
            'start_time': detail.get('start-time', 0) if detail else 0
        })
    return jobs


def stop_job(job_id):
    """停止指定任务"""
    result = http_post(f'{FLINK_REST}/jobs/{job_id}?mode=cancel', {})
    if result and 'error' not in result:
        logger.info(f"Job {job_id} cancelled")
        return True
    logger.error(f"Failed to cancel job {job_id}: {result}")
    return False


def submit_sql_job(sql_statements):
    """
    提交 SQL 任务到 Flink
    注意: Flink 1.17+ 需要 SQL Gateway 或使用 Table API
    这里使用 REST API 的 /jobs 端点 (需要 flink-sql-client jar)
    """
    # Method 1: Try SQL Gateway (Flink 1.16+)
    gateway_url = f'{FLINK_REST}/v1/sql'
    result = http_post(gateway_url, {'statements': sql_statements})
    if result and 'error' not in result:
        logger.info(f"SQL job submitted via Gateway")
        return result

    # Method 2: Try /jobs/upload (JAR-based)
    logger.warning("SQL Gateway not available. Please submit SQL jobs manually:")
    logger.warning("1. Open Flink Web UI: " + FLINK_REST)
    logger.warning("2. Use SQL Client or submit JAR with SQL statements")
    logger.warning("3. Required JARs: flink-sql-connector-kafka, flink-connector-jdbc, mysql-connector-java")

    # Print SQL for manual submission
    print("\n" + "=" * 60)
    print("SQL Statements to submit:")
    print("=" * 60)
    print(sql_statements)
    print("=" * 60)
    return None


def main():
    parser = argparse.ArgumentParser(description='Flink SQL Job Manager')
    parser.add_argument('--list', action='store_true', help='List running jobs')
    parser.add_argument('--stop', type=str, help='Stop a job by ID')
    parser.add_argument('--stop-all', action='store_true', help='Stop all jobs')
    parser.add_argument('--sql-file', default='realtime_analytics.sql', help='SQL file to submit')
    args = parser.parse_args()

    if not check_flink_available():
        sys.exit(1)

    if args.list:
        jobs = list_jobs()
        if jobs:
            print(f"\nRunning Flink Jobs ({len(jobs)}):")
            for j in jobs:
                start = time.strftime('%Y-%m-%d %H:%M', time.localtime(j['start_time']/1000)) if j['start_time'] else 'N/A'
                print(f"  [{j['status']}] {j['name']} (id: {j['id'][:12]}..., started: {start})")
        else:
            print("No running jobs")
        return

    if args.stop:
        stop_job(args.stop)
        return

    if args.stop_all:
        for j in list_jobs():
            stop_job(j['id'])
        return

    # Submit SQL job
    sql_file = os.path.join(SQL_DIR, args.sql_file)
    if not os.path.exists(sql_file):
        logger.error(f"SQL file not found: {sql_file}")
        sys.exit(1)

    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    logger.info(f"Submitting SQL from {args.sql_file}...")
    submit_sql_job(sql_content)


if __name__ == '__main__':
    main()
