-- =============================================================
-- ADS 实时结果库 DDL（V2 设计文档 4.2）
-- gmall_report_rt：首期 6 指标，通用(metric_date, window_start,
-- metric_name, dimension_key) 主键 + 覆盖式 upsert（幂等）
-- 写入方：Job2/Job3（flink-connector-jdbc, INSERT ... ON DUPLICATE KEY UPDATE）
-- =============================================================

CREATE DATABASE IF NOT EXISTS gmall_report_rt
DEFAULT CHARACTER SET utf8mb4
DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE gmall_report_rt;

-- 流量分钟指标（PV/分钟UV 等）
CREATE TABLE IF NOT EXISTS ads_traffic_1m (
    metric_date    DATE         NOT NULL COMMENT '业务日期(Asia/Shanghai)',
    window_start   DATETIME     NOT NULL COMMENT '窗口起始(Asia/Shanghai)',
    metric_name    VARCHAR(32)  NOT NULL COMMENT '指标名: browse_pv/browse_uv',
    dimension_key  VARCHAR(64)  NOT NULL DEFAULT 'ALL' COMMENT '维度键(默认 ALL)',
    metric_value   DECIMAL(18,2) NOT NULL COMMENT '指标值',
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP COMMENT '写入时间',
    PRIMARY KEY (metric_date, window_start, metric_name, dimension_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS 流量分钟指标';

-- 交易分钟指标（GMV/支付订单数等）
CREATE TABLE IF NOT EXISTS ads_trade_1m (
    metric_date    DATE         NOT NULL,
    window_start   DATETIME     NOT NULL,
    metric_name    VARCHAR(32)  NOT NULL,
    dimension_key  VARCHAR(64)  NOT NULL DEFAULT 'ALL',
    metric_value   DECIMAL(18,2) NOT NULL,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, window_start, metric_name, dimension_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS 交易分钟指标';

-- 流量日指标（日 UV：自然日独立去重，禁止 SUM 分钟）
CREATE TABLE IF NOT EXISTS ads_traffic_day (
    metric_date    DATE         NOT NULL COMMENT '业务日期',
    metric_name    VARCHAR(32)  NOT NULL COMMENT '指标名: browse_pv/day_uv',
    dimension_key  VARCHAR(64)  NOT NULL DEFAULT 'ALL',
    metric_value   DECIMAL(18,2) NOT NULL,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, metric_name, dimension_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS 流量日指标';

-- 交易日指标（日支付用户数等）
CREATE TABLE IF NOT EXISTS ads_trade_day (
    metric_date    DATE         NOT NULL,
    metric_name    VARCHAR(32)  NOT NULL COMMENT '指标名: gmv/payment_order_count/payment_user_count/refund_amount',
    dimension_key  VARCHAR(64)  NOT NULL DEFAULT 'ALL',
    metric_value   DECIMAL(18,2) NOT NULL,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, metric_name, dimension_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS 交易日指标';
