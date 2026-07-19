-- ============================================
-- 实时数据表 - 爬虫真实数据 + Kafka/Flink 写入
-- ============================================

-- 实时直播间统计 (Kafka Consumer / Flink Sink)
CREATE TABLE IF NOT EXISTS rt_room_stats (
    room_id VARCHAR(50) PRIMARY KEY,
    room_name VARCHAR(200),
    anchor_name VARCHAR(50),
    platform VARCHAR(30),
    category VARCHAR(30),
    status VARCHAR(20) DEFAULT 'live',
    current_viewers INT DEFAULT 0,
    peak_viewers INT DEFAULT 0,
    total_danmaku BIGINT DEFAULT 0,
    total_orders BIGINT DEFAULT 0,
    total_gmv DECIMAL(18,2) DEFAULT 0,
    live_url VARCHAR(500),
    cover_url VARCHAR(500),
    start_time DATETIME,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_platform (platform),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 弹幕实时缓冲
CREATE TABLE IF NOT EXISTS rt_danmaku (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id VARCHAR(64),
    room_id VARCHAR(50),
    platform VARCHAR(30),
    user_id VARCHAR(50),
    user_name VARCHAR(100),
    content TEXT,
    danmaku_type VARCHAR(20) DEFAULT 'comment',
    event_time DATETIME(3),
    INDEX idx_room_time (room_id, event_time),
    INDEX idx_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 商品货架
CREATE TABLE IF NOT EXISTS rt_product (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_id VARCHAR(50),
    room_id VARCHAR(50),
    platform VARCHAR(30),
    product_name VARCHAR(200),
    price DECIMAL(12,2),
    original_price DECIMAL(12,2),
    sales INT DEFAULT 0,
    category VARCHAR(30),
    image_url VARCHAR(500),
    product_url VARCHAR(500),
    sort_order INT DEFAULT 0,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_room (room_id),
    INDEX idx_platform (platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 爬虫会话追踪
CREATE TABLE IF NOT EXISTS crawler_session (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    platform VARCHAR(30),
    session_type VARCHAR(20) DEFAULT 'discovery',
    room_id VARCHAR(50),
    room_name VARCHAR(200),
    status VARCHAR(20) DEFAULT 'running',
    rooms_discovered INT DEFAULT 0,
    danmaku_captured BIGINT DEFAULT 0,
    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat DATETIME,
    error_msg TEXT,
    INDEX idx_platform (platform),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 弹幕热词统计
CREATE TABLE IF NOT EXISTS rt_danmaku_hotwords (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    word VARCHAR(50),
    freq BIGINT DEFAULT 0,
    platform VARCHAR(30),
    room_id VARCHAR(50),
    window_start DATETIME,
    window_end DATETIME,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_platform_time (platform, update_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 给 live_room 表添加真实数据字段
ALTER TABLE live_room
    ADD COLUMN IF NOT EXISTS live_url VARCHAR(500) COMMENT '直播间实际URL',
    ADD COLUMN IF NOT EXISTS room_id_external VARCHAR(50) COMMENT '平台原始房间ID',
    ADD COLUMN IF NOT EXISTS cover_url VARCHAR(500) COMMENT '封面图URL',
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(20) DEFAULT 'simulated' COMMENT '数据来源';
