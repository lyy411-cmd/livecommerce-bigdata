-- ============================================
-- 智慧直播电商大数据分析平台 - 数据库初始化
-- CREATE DATABASE livecommerce_db DEFAULT CHARACTER SET utf8mb4;
-- ============================================

-- 系统用户
CREATE TABLE IF NOT EXISTS sys_user (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100),
    phone VARCHAR(20),
    password VARCHAR(128) NOT NULL,
    role VARCHAR(20) DEFAULT 'customer',
    user_type VARCHAR(20) DEFAULT 'customer',
    department VARCHAR(50),
    status TINYINT DEFAULT 1,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted TINYINT DEFAULT 0,
    INDEX idx_username (username),
    INDEX idx_user_type (user_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 直播间
CREATE TABLE IF NOT EXISTS live_room (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    room_no VARCHAR(50) UNIQUE,
    room_name VARCHAR(100),
    anchor_id BIGINT,
    anchor_name VARCHAR(50),
    platform VARCHAR(30),
    category VARCHAR(30),
    status VARCHAR(20),
    viewer_count INT DEFAULT 0,
    peak_viewers INT DEFAULT 0,
    total_viewers INT DEFAULT 0,
    like_count INT DEFAULT 0,
    comment_count INT DEFAULT 0,
    share_count INT DEFAULT 0,
    order_count INT DEFAULT 0,
    gmv DECIMAL(18, 2) DEFAULT 0,
    conversion_rate DECIMAL(8, 4) DEFAULT 0,
    start_time DATETIME,
    end_time DATETIME,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted TINYINT DEFAULT 0,
    INDEX idx_status (status),
    INDEX idx_platform (platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 主播
CREATE TABLE IF NOT EXISTS anchor (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50),
    nickname VARCHAR(50),
    avatar VARCHAR(255),
    phone VARCHAR(20),
    platform VARCHAR(30),
    level VARCHAR(10),
    category VARCHAR(30),
    fans_count INT DEFAULT 0,
    live_hours INT DEFAULT 0,
    total_gmv DECIMAL(18, 2) DEFAULT 0,
    total_orders INT DEFAULT 0,
    avg_conversion DECIMAL(8, 4) DEFAULT 0,
    intro TEXT,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted TINYINT DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 订单
CREATE TABLE IF NOT EXISTS order_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_no VARCHAR(50) UNIQUE,
    user_id BIGINT,
    username VARCHAR(50),
    room_id BIGINT,
    room_name VARCHAR(100),
    product_id BIGINT,
    product_name VARCHAR(200),
    product_image VARCHAR(500),
    quantity INT DEFAULT 1,
    price DECIMAL(12, 2),
    total_amount DECIMAL(18, 2),
    status VARCHAR(20),
    receiver_name VARCHAR(50),
    receiver_phone VARCHAR(20),
    receiver_address VARCHAR(500),
    platform VARCHAR(30),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    pay_time DATETIME,
    deliver_time DATETIME,
    deleted TINYINT DEFAULT 0,
    INDEX idx_status (status),
    INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 初始化数据：admin/123456 的 SHA-256 = 8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92
INSERT IGNORE INTO sys_user (username, email, password, role, user_type, department, status)
VALUES
('admin', 'admin@livecommerce.com', '8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92', 'admin', 'staff', '技术部', 1),
('operator', 'operator@livecommerce.com', '8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92', 'operator', 'staff', '运营部', 1),
('analyst', 'analyst@livecommerce.com', '8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92', 'analyst', 'staff', '数据分析部', 1);

-- 初始化主播
INSERT IGNORE INTO anchor (name, nickname, platform, level, category, fans_count, live_hours, total_gmv, total_orders, avg_conversion, intro)
VALUES
('李佳琦', '口红一哥', 'taobao', 'S', '美妆', 65800000, 5200, 1850000000, 3200000, 5.8, '美妆带货达人'),
('薇娅', '哆啦薇娅', 'taobao', 'S', '全品类', 78000000, 4800, 2200000000, 4100000, 6.2, '全品类主播'),
('罗永浩', '老罗', 'douyin', 'S', '数码', 19500000, 2800, 850000000, 1800000, 4.8, '数码科技'),
('辛巴', '辛有志', 'kuaishou', 'S', '食品', 98000000, 6200, 1650000000, 3500000, 5.5, '食品日用'),
('董宇辉', '东方甄选', 'douyin', 'A', '食品', 18500000, 1500, 580000000, 1200000, 4.2, '知识带货');

-- 初始化直播间
INSERT IGNORE INTO live_room (room_no, room_name, anchor_name, platform, category, status, viewer_count, total_viewers, order_count, gmv, conversion_rate, start_time)
VALUES
('ROOM1001', '618李佳琦专场', '李佳琦', 'taobao', '美妆', 'live', 285000, 1850000, 12800, 5860000, 6.92, NOW()),
('ROOM1002', '薇娅日不落', '薇娅', 'taobao', '全品类', 'live', 312000, 2050000, 15600, 7230000, 7.61, NOW()),
('ROOM1003', '老罗数码专场', '罗永浩', 'douyin', '数码', 'live', 158000, 980000, 5200, 2380000, 5.31, NOW()),
('ROOM1004', '辛巴严选', '辛巴', 'kuaishou', '食品', 'live', 425000, 2680000, 18200, 8950000, 6.79, NOW()),
('ROOM1005', '东方甄选直播间', '董宇辉', 'douyin', '食品', 'finished', 0, 1250000, 6800, 3520000, 5.44, NOW()),
('ROOM1006', '小杨哥搞笑带货', '疯狂小杨哥', 'douyin', '全品类', 'live', 380000, 3200000, 21500, 9850000, 6.72, NOW()),
('ROOM1007', '刘畊宏健身专场', '刘畊宏', 'douyin', '运动', 'finished', 0, 850000, 2100, 580000, 2.47, NOW());
