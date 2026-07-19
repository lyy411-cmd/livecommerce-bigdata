-- ============================================
-- Flink SQL 实时分析任务
-- 从 Kafka 消费 -> 实时聚合 -> 写入 MySQL
-- ============================================

-- ============ Kafka 数据源 ============

-- 弹幕事件源
CREATE TABLE IF NOT EXISTS kafka_danmaku (
    event_id STRING,
    event_type STRING,
    `timestamp` BIGINT,
    platform STRING,
    room_id STRING,
    room_name STRING,
    user_id STRING,
    user_name STRING,
    content STRING,
    danmaku_type STRING,
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'danmaku_events',
    'properties.bootstrap.servers' = '192.168.104.100:9092',
    'properties.group.id' = 'flink-danmaku-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.ignore-parse-errors' = 'true'
);

-- 直播间事件源
CREATE TABLE IF NOT EXISTS kafka_room_events (
    event_id STRING,
    event_type STRING,
    `timestamp` BIGINT,
    platform STRING,
    room_id STRING,
    room_name STRING,
    anchor_name STRING,
    category STRING,
    status STRING,
    viewer_count INT,
    order_count INT,
    gmv DOUBLE,
    peak_viewers INT,
    live_url STRING,
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'live_room_events',
    'properties.bootstrap.servers' = '192.168.104.100:9092',
    'properties.group.id' = 'flink-room-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.ignore-parse-errors' = 'true'
);

-- ============ MySQL Sink 表 ============

-- 实时房间统计 Sink
CREATE TABLE IF NOT EXISTS mysql_rt_room_stats (
    room_id STRING,
    room_name STRING,
    anchor_name STRING,
    platform STRING,
    current_viewers INT,
    peak_viewers INT,
    total_danmaku BIGINT,
    window_end TIMESTAMP(3),
    PRIMARY KEY (room_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.104.100:3306/livecommerce_db',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'rt_room_stats',
    'sink.buffer-flush.max-rows' = '50',
    'sink.buffer-flush.interval' = '5s',
    'sink.upsert-mode' = 'upsert'
);

-- 弹幕热词 Sink
CREATE TABLE IF NOT EXISTS mysql_danmaku_hotwords (
    word STRING,
    freq BIGINT,
    platform STRING,
    room_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PRIMARY KEY (word, room_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.104.100:3306/livecommerce_db',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'rt_danmaku_hotwords',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '10s',
    'sink.upsert-mode' = 'upsert'
);

-- ============ 实时分析查询 ============

-- Job 1: 实时房间观众统计 (1分钟滚动窗口)
INSERT INTO mysql_rt_room_stats
SELECT
    room_id,
    LAST_VALUE(room_name) AS room_name,
    LAST_VALUE(anchor_name) AS anchor_name,
    LAST_VALUE(platform) AS platform,
    LAST_VALUE(viewer_count) AS current_viewers,
    MAX(peak_viewers) AS peak_viewers,
    COUNT(*) AS total_danmaku,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end
FROM kafka_room_events
GROUP BY
    room_id,
    TUMBLE(event_time, INTERVAL '1' MINUTE);

-- Job 2: 弹幕热词统计 (5分钟滚动窗口)
-- 注意: 这里简化处理，将完整弹幕内容作为热词
-- 生产环境应先做分词
INSERT INTO mysql_danmaku_hotwords
SELECT
    content AS word,
    COUNT(*) AS freq,
    LAST_VALUE(platform) AS platform,
    LAST_VALUE(room_id) AS room_id,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end
FROM kafka_danmaku
WHERE danmaku_type = 'comment'
    AND content IS NOT NULL
    AND LENGTH(content) > 1
    AND LENGTH(content) < 20
GROUP BY
    content,
    TUMBLE(event_time, INTERVAL '5' MINUTE);
