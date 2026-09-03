-- =============================================================
-- Job2 交易域：DWD(payment/refund) -> DWS(dws_trade_day)，只写 Kafka
-- ADS 落库由 Job3 无状态转发（节点6 拆三作业，见 job3_ads_sink.sql）
-- 指标（口径锚点 4/5/6）:
--   gmv                  = SUM(payment_amount) WHERE status='SUCCESS'，不退 refund
--   payment_order_count  = COUNT(DISTINCT order_id)（支付订单数）
--   payment_user_count   = COUNT(DISTINCT user_id)（日支付用户数）
--   refund_amount        = SUM(refund_amount) WHERE refund_status='SUCCESS'（按退款日归日）
-- 时间归属: create_time（支付/退款时间，MySQL datetime 本地语义，无时区转换）
-- 聚合方式: GROUP BY CAST(create_time AS DATE) —— 非窗口持续累计
--   （vs 窗口聚合: 无 watermark/迟到问题，任何时刻到达的支付归入其支付日）
-- 输出: dws_trade_day（upsert-kafka，PK metric_date/metric_name/dimension_key）
-- 多 source（payment/refund 两表）在 DWS 层合流成 4 指标 → 同一 INSERT
-- =============================================================
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.target' = 'remote';
SET 'pipeline.name' = 'job2_trade_dws';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'state.checkpoints.num-retained' = '3';
SET 'table.local-time-zone' = 'Asia/Shanghai';

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

-- 4 指标 UNION ALL 合并成 1 个 INSERT（每段独立聚合状态）；
-- UNION 输出列名继承第一段 → 第一段必须显式 AS 别名（节点4 排障教训）
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
