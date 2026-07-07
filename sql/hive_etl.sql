-- ============================================
-- 智慧直播电商 - Hive ETL 每日任务
-- 调度方式：crontab / airflow 每日凌晨执行
-- ============================================

-- 1. ODS -> DWD: 清洗直播明细
INSERT OVERWRITE TABLE dwd.dwd_live_room_detail PARTITION (dt='${dt}')
SELECT
    room_no,
    room_name,
    anchor_name,
    platform,
    category,
    status,
    viewer_count,
    order_count,
    gmv,
    total_viewers,
    conversion_rate,
    CAST(FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT), 'HH') AS INT) AS start_hour,
    FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT), 'yyyy-MM-dd') AS event_date
FROM ods.ods_live_room_event
WHERE dt = '${dt}'
  AND room_no IS NOT NULL;

-- 2. DWD -> DWS: 主播维度统计
INSERT OVERWRITE TABLE dws.dws_anchor_stats PARTITION (dt='${dt}')
SELECT
    a.name AS anchor_name,
    a.platform,
    a.level,
    COUNT(DISTINCT d.room_no) AS total_rooms,
    SUM(d.total_viewers) AS total_viewers,
    SUM(d.order_count) AS total_orders,
    SUM(d.gmv) AS total_gmv,
    AVG(d.conversion_rate) AS avg_conversion,
    FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM anchor a
LEFT JOIN dwd.dwd_live_room_detail d
    ON a.name = d.anchor_name AND a.platform = d.platform
WHERE d.dt = '${dt}'
GROUP BY a.name, a.platform, a.level;

-- 3. DWD -> DWS: 类目维度统计
INSERT OVERWRITE TABLE dws.dws_category_stats PARTITION (dt='${dt}')
SELECT
    category,
    platform,
    COUNT(DISTINCT room_no) AS total_rooms,
    SUM(order_count) AS total_orders,
    SUM(gmv) AS total_gmv,
    AVG(viewer_count) AS avg_viewer_count
FROM dwd.dwd_live_room_detail
WHERE dt = '${dt}'
GROUP BY category, platform;

-- 4. DWS -> ADS: 仪表盘 KPI
INSERT OVERWRITE TABLE ads.ads_dashboard_kpi PARTITION (dt='${dt}')
SELECT 'total_gmv', SUM(total_gmv), '元',
    LAG(SUM(total_gmv)) OVER (ORDER BY '${dt}'),
    ROUND((SUM(total_gmv) - LAG(SUM(total_gmv)) OVER (ORDER BY '${dt}')) / LAG(SUM(total_gmv)) OVER (ORDER BY '${dt}') * 100, 2)
FROM dws.dws_anchor_stats WHERE dt='${dt}'
UNION ALL
SELECT 'total_orders', SUM(total_orders), '单',
    LAG(SUM(total_orders)) OVER (ORDER BY '${dt}'),
    ROUND((SUM(total_orders) - LAG(SUM(total_orders)) OVER (ORDER BY '${dt}')) / LAG(SUM(total_orders)) OVER (ORDER BY '${dt}') * 100, 2)
FROM dws.dws_anchor_stats WHERE dt='${dt}'
UNION ALL
SELECT 'total_viewers', SUM(total_viewers), '人',
    LAG(SUM(total_viewers)) OVER (ORDER BY '${dt}'),
    0
FROM dws.dws_anchor_stats WHERE dt='${dt}';

-- 5. 主播带货排行
INSERT OVERWRITE TABLE ads.ads_anchor_gmv_rank PARTITION (dt='${dt}')
SELECT
    ROW_NUMBER() OVER (ORDER BY total_gmv DESC) AS rank_no,
    anchor_name,
    platform,
    total_gmv AS gmv,
    total_orders AS orders
FROM dws.dws_anchor_stats
WHERE dt = '${dt}'
ORDER BY gmv DESC
LIMIT 10;
