-- =============================================================
-- Job2 交易域：DWD(payment) -> DWS/ADS 交易日指标
-- 指标（口径锚点 4/5/6）:
--   gmv                  = SUM(payment_amount) WHERE status='SUCCESS'，不退 refund
--   payment_order_count  = COUNT(DISTINCT order_id)（支付订单数）
--   payment_user_count   = COUNT(DISTINCT user_id)（日支付用户数）
-- 时间归属: create_time（支付时间，MySQL datetime 本地语义，无时区转换）
-- 聚合方式: GROUP BY CAST(create_time AS DATE) —— 非窗口持续累计
--   （vs 窗口聚合: 无 watermark/迟到问题，任何时刻到达的支付归入其支付日）
-- 输出: dws_trade_day(upsert-kafka) + ads_trade_day(MySQL JDBC upsert)
-- 注意: dws/ads 双 INSERT = 两个独立作业，source 必须各自独立 group.id
--   （否则共享 consumer group 消息被瓜分，见开发日志"修复"节）
-- =============================================================
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.target' = 'remote';
SET 'pipeline.name' = 'job2_trade_dws';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'state.checkpoints.num-retained' = '3';
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- DWS 分支 source
CREATE TABLE dwd_payment_detail (
  id             BIGINT,
  order_id       BIGINT,
  user_id        BIGINT,
  payment_type   STRING,
  trade_no       STRING,
  payment_amount DECIMAL(16, 2),
  payment_status STRING,
  create_time    TIMESTAMP(3),
  callback_time  TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_payment_detail',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_trade_dws_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- ADS 分支独立 source（独立 group.id，见文件头注释）
CREATE TABLE dwd_payment_detail_ads (
  id             BIGINT,
  order_id       BIGINT,
  user_id        BIGINT,
  payment_type   STRING,
  trade_no       STRING,
  payment_amount DECIMAL(16, 2),
  payment_status STRING,
  create_time    TIMESTAMP(3),
  callback_time  TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_payment_detail',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_trade_ads_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- 退款 source（dws 分支）
CREATE TABLE dwd_refund_detail (
  id            BIGINT,
  user_id       BIGINT,
  order_id      BIGINT,
  sku_id        BIGINT,
  refund_amount DECIMAL(16, 2),
  refund_status STRING,
  create_time   TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_refund_detail',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_trade_refund_dws_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- 退款 source（ads 分支，独立 group.id）
CREATE TABLE dwd_refund_detail_ads (
  id            BIGINT,
  user_id       BIGINT,
  order_id      BIGINT,
  sku_id        BIGINT,
  refund_amount DECIMAL(16, 2),
  refund_status STRING,
  create_time   TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_refund_detail',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_trade_refund_ads_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE dws_trade_day (
  metric_date   DATE,
  metric_name   STRING,
  dimension_key STRING,
  metric_value  DECIMAL(18, 2),
  PRIMARY KEY (metric_date, metric_name, dimension_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dws_trade_day',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE ads_trade_day (
  metric_date   DATE,
  metric_name   STRING,
  dimension_key STRING,
  metric_value  DECIMAL(18, 2),
  PRIMARY KEY (metric_date, metric_name, dimension_key) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/gmall_report_rt?useSSL=false&serverTimezone=Asia/Shanghai',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'ads_trade_day',
  'sink.buffer-flush.max-rows' = '1'
);

-- DWS：3 指标按支付日持续累计（upsert 更新当日值）
INSERT INTO dws_trade_day
SELECT metric_date, metric_name, dimension_key, metric_value
FROM (
  SELECT
    CAST(create_time AS DATE)                  AS metric_date,
    'gmv'                                      AS metric_name,
    'ALL'                                      AS dimension_key,
    CAST(SUM(payment_amount) AS DECIMAL(18,2)) AS metric_value
  FROM dwd_payment_detail
  WHERE payment_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'payment_order_count',
    'ALL',
    CAST(COUNT(DISTINCT order_id) AS DECIMAL(18,2))
  FROM dwd_payment_detail
  WHERE payment_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'payment_user_count',
    'ALL',
    CAST(COUNT(DISTINCT user_id) AS DECIMAL(18,2))
  FROM dwd_payment_detail
  WHERE payment_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'refund_amount',
    'ALL',
    CAST(SUM(refund_amount) AS DECIMAL(18,2))
  FROM dwd_refund_detail
  WHERE refund_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
) t;

-- ADS（独立作业分支）
INSERT INTO ads_trade_day
SELECT metric_date, metric_name, dimension_key, metric_value
FROM (
  SELECT
    CAST(create_time AS DATE)                  AS metric_date,
    'gmv'                                      AS metric_name,
    'ALL'                                      AS dimension_key,
    CAST(SUM(payment_amount) AS DECIMAL(18,2)) AS metric_value
  FROM dwd_payment_detail_ads
  WHERE payment_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'payment_order_count',
    'ALL',
    CAST(COUNT(DISTINCT order_id) AS DECIMAL(18,2))
  FROM dwd_payment_detail_ads
  WHERE payment_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'payment_user_count',
    'ALL',
    CAST(COUNT(DISTINCT user_id) AS DECIMAL(18,2))
  FROM dwd_payment_detail_ads
  WHERE payment_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'refund_amount',
    'ALL',
    CAST(SUM(refund_amount) AS DECIMAL(18,2))
  FROM dwd_refund_detail_ads
  WHERE refund_status = 'SUCCESS'
  GROUP BY CAST(create_time AS DATE)
) t;
