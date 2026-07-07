-- ============================================
-- 智慧直播电商 - Hive 数据仓库分层
-- ODS (源数据层) -> DWD (明细层) -> DWS (汇总层) -> ADS (应用层)
-- ============================================

-- ODS 层 - 直播原始事件
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_live_room_event (
    event_id STRING COMMENT '事件ID',
    event_time BIGINT COMMENT '事件时间',
    room_no STRING COMMENT '直播间编号',
    room_name STRING COMMENT '直播间名称',
    anchor_name STRING COMMENT '主播名',
    platform STRING COMMENT '平台',
    category STRING COMMENT '类目',
    status STRING COMMENT '状态',
    viewer_count INT COMMENT '在线人数',
    order_count INT COMMENT '订单数',
    gmv DOUBLE COMMENT 'GMV',
    total_viewers INT COMMENT '累计观看',
    conversion_rate DOUBLE COMMENT '转化率'
)
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
STORED AS PARQUET
LOCATION '/livecommerce/ods/ods_live_room_event';

-- ODS 层 - 订单事件
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_order_event (
    event_id STRING,
    event_time BIGINT,
    order_no STRING,
    phone STRING,
    email STRING,
    amount DOUBLE,
    status STRING,
    user_id BIGINT,
    room_id BIGINT,
    product_id BIGINT,
    quantity INT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/livecommerce/ods/ods_order_event';

-- DWD 层 - 直播明细
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_live_room_detail (
    room_no STRING,
    room_name STRING,
    anchor_name STRING,
    platform STRING,
    category STRING,
    status STRING,
    viewer_count INT,
    order_count INT,
    gmv DOUBLE,
    total_viewers INT,
    conversion_rate DOUBLE,
    start_hour INT,
    event_date STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/livecommerce/dwd/dwd_live_room_detail';

-- DWS 层 - 主播维度统计
CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_anchor_stats (
    anchor_name STRING,
    platform STRING,
    level STRING,
    total_rooms BIGINT,
    total_viewers BIGINT,
    total_orders BIGINT,
    total_gmv DOUBLE,
    avg_conversion DOUBLE,
    update_time STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/livecommerce/dws/dws_anchor_stats';

-- DWS 层 - 类目维度统计
CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_category_stats (
    category STRING,
    platform STRING,
    total_rooms BIGINT,
    total_orders BIGINT,
    total_gmv DOUBLE,
    avg_viewer_count DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/livecommerce/dws/dws_category_stats';

-- ADS 层 - 仪表盘数据
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_dashboard_kpi (
    metric_name STRING,
    metric_value DOUBLE,
    metric_unit STRING,
    compare_value DOUBLE,
    change_rate DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/livecommerce/ads/ads_dashboard_kpi';

-- ADS 层 - 主播带货排行
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_anchor_gmv_rank (
    rank_no INT,
    anchor_name STRING,
    platform STRING,
    gmv DOUBLE,
    orders BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/livecommerce/ads/ads_anchor_gmv_rank';
