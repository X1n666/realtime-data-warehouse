-- =============================================================
-- MySQL result tables for ADS indicators.
-- Run on project csu01:
--   mysql -uroot -p < ~/ecommerce-offline-data-warehouse/mysql/ads_result_ddl.sql
--
-- gmall is the source business database. gmall_report is the ADS result
-- database used by BI tools such as Superset.
-- =============================================================

CREATE DATABASE IF NOT EXISTS gmall_report
DEFAULT CHARACTER SET utf8mb4
DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE gmall_report_rt;

CREATE TABLE IF NOT EXISTS ads_gmv_day (
    dt DATE NOT NULL COMMENT 'Business date',
    gmv_amount DECIMAL(16,2) NOT NULL COMMENT 'Successful payment amount',
    payment_order_count BIGINT NOT NULL COMMENT 'Paid order count',
    payment_user_count BIGINT NOT NULL COMMENT 'Paying user count',
    avg_payment_amount DECIMAL(16,2) NOT NULL COMMENT 'GMV divided by paid order count',
    PRIMARY KEY (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS daily GMV and payment summary';

CREATE TABLE IF NOT EXISTS ads_trade_refund (
    dt DATE NOT NULL COMMENT 'Business date',
    gmv_amount DECIMAL(16,2) NOT NULL COMMENT 'Successful payment amount',
    refund_amount DECIMAL(16,2) NOT NULL COMMENT 'Successful refund amount',
    refund_rate DECIMAL(16,4) NOT NULL COMMENT 'Refund amount divided by GMV',
    PRIMARY KEY (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS daily trade refund summary';

CREATE TABLE IF NOT EXISTS ads_sku_topn (
    dt DATE NOT NULL COMMENT 'Business date',
    sku_rank BIGINT NOT NULL COMMENT 'Rank by order amount',
    sku_id BIGINT NOT NULL COMMENT 'SKU id',
    sku_name VARCHAR(200) NOT NULL COMMENT 'SKU name',
    category_id BIGINT COMMENT 'Category id',
    category_name VARCHAR(100) COMMENT 'Category name',
    order_count BIGINT NOT NULL COMMENT 'Order count',
    order_sku_num BIGINT NOT NULL COMMENT 'Ordered SKU quantity',
    order_amount DECIMAL(16,2) NOT NULL COMMENT 'Order amount',
    refund_amount DECIMAL(16,2) NOT NULL COMMENT 'Refund amount',
    PRIMARY KEY (dt, sku_rank),
    KEY idx_ads_sku_topn_sku_id (sku_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS daily SKU sales top N';

CREATE TABLE IF NOT EXISTS ads_area_stats (
    dt DATE NOT NULL COMMENT 'Business date',
    province_id BIGINT NOT NULL COMMENT 'Province id',
    province_name VARCHAR(100) NOT NULL COMMENT 'Province name',
    payment_user_count BIGINT NOT NULL COMMENT 'Paying user count',
    payment_amount DECIMAL(16,2) NOT NULL COMMENT 'Successful payment amount',
    refund_amount DECIMAL(16,2) NOT NULL COMMENT 'Successful refund amount',
    refund_rate DECIMAL(16,4) NOT NULL COMMENT 'Refund amount divided by payment amount',
    PRIMARY KEY (dt, province_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS daily area trade summary';

CREATE TABLE IF NOT EXISTS ads_user_stats (
    dt DATE NOT NULL COMMENT 'Business date',
    payment_user_count BIGINT NOT NULL COMMENT 'Paying user count',
    repeat_payment_user_count BIGINT NOT NULL COMMENT 'Users with at least two paid orders',
    repeat_payment_rate DECIMAL(16,4) NOT NULL COMMENT 'Repeat paying users divided by paying users',
    payment_amount DECIMAL(16,2) NOT NULL COMMENT 'Successful payment amount',
    refund_amount DECIMAL(16,2) NOT NULL COMMENT 'Successful refund amount',
    PRIMARY KEY (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS daily user trade summary';

-- ---------- 流量指标（行为日志域，2026-08-23 新增） ----------

CREATE TABLE IF NOT EXISTS ads_flow_funnel_day (
    dt DATE NOT NULL COMMENT 'Business date',
    launch_users BIGINT NOT NULL COMMENT 'App launch distinct users',
    browse_users BIGINT NOT NULL COMMENT 'Browse distinct users (UV)',
    click_users BIGINT NOT NULL COMMENT 'Click distinct users',
    add_cart_users BIGINT NOT NULL COMMENT 'Add-to-cart distinct users',
    order_users BIGINT NOT NULL COMMENT 'Order distinct users',
    pay_users BIGINT NOT NULL COMMENT 'Pay distinct users',
    click_rate DECIMAL(16,4) NOT NULL COMMENT 'click / browse',
    add_cart_rate DECIMAL(16,4) NOT NULL COMMENT 'add_cart / click',
    order_rate DECIMAL(16,4) NOT NULL COMMENT 'order / add_cart',
    pay_rate DECIMAL(16,4) NOT NULL COMMENT 'pay / order',
    PRIMARY KEY (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS daily conversion funnel';

CREATE TABLE IF NOT EXISTS ads_user_retention_day (
    start_dt DATE NOT NULL COMMENT 'Base date (day 0)',
    start_users BIGINT NOT NULL COMMENT 'Active users on base date',
    retention_dt DATE NOT NULL COMMENT 'Observation date (base + N days)',
    retention_n BIGINT NOT NULL COMMENT 'N days after base',
    retained_users BIGINT NOT NULL COMMENT 'Active on both base and observation date',
    retention_rate DECIMAL(16,4) NOT NULL COMMENT 'retained / start',
    PRIMARY KEY (start_dt, retention_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS active-user N-day retention';
