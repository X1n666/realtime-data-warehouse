-- =============================================================
-- Job2: DWD -> DWS（流量域计算层，只写 Kafka，不碰 MySQL）
-- 输入: Kafka dwd_traffic_event
-- 输出: Kafka dws_traffic_1m（分钟窗口）/ dws_traffic_day（日指标）
-- ADS 落库由 Job3 无状态转发（节点6 拆三作业：指标只在 Job2 算一次，
--   Job3 读 DWS 当前绝对值 + 同主键覆盖写 MySQL，见 job3_ads_sink.sql）
-- 有状态: 分钟=窗口聚合（watermark+5s 乱序容忍）；日=COUNT DISTINCT 状态
-- =============================================================
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.target' = 'remote';
SET 'pipeline.name' = 'job2_dwd_to_dws';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'state.checkpoints.num-retained' = '3';
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE dwd_traffic_event (
  uid BIGINT,
  platform STRING,
  province_id BIGINT,
  page_id STRING,
  action_id STRING,
  item BIGINT,
  ts STRING,
  event_id STRING,
  event_time_ms BIGINT,
  biz_date STRING,
  source_timezone STRING,
  ts_ltz AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_traffic_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_dwd_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- 日聚合 source（独立 group.id；非窗口聚合不需要 watermark/时间计算列，只取最小列）
CREATE TABLE dwd_traffic_event_day (
  uid BIGINT,
  action_id STRING,
  biz_date STRING,
  event_id STRING,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_traffic_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_traffic_day_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE dws_traffic_1m (
  metric_date   DATE,
  window_start  TIMESTAMP(3),
  metric_name   STRING,
  dimension_key STRING,
  metric_value  DECIMAL(18, 2),
  PRIMARY KEY (metric_date, window_start, metric_name, dimension_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dws_traffic_1m',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE dws_traffic_day (
  metric_date   DATE,
  metric_name   STRING,
  dimension_key STRING,
  metric_value  DECIMAL(18, 2),
  PRIMARY KEY (metric_date, metric_name, dimension_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dws_traffic_day',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- 分钟窗口：browse PV / 分钟 UV（同窗口两指标 UNION ALL，窗口结束才触发）
-- 分钟 UV 是中间指标（锚点 2）：窗口内去重，禁止 SUM 当日 UV（红线见日段注释）
INSERT INTO dws_traffic_1m
SELECT metric_date, window_start, metric_name, dimension_key, metric_value
FROM (
  SELECT
    CAST(biz_date AS DATE)                AS metric_date,
    TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE) AS window_start,
    'browse_pv'                           AS metric_name,
    'ALL'                                 AS dimension_key,
    CAST(COUNT(*) AS DECIMAL(18, 2))      AS metric_value
  FROM dwd_traffic_event
  WHERE action_id = 'browse'
  GROUP BY biz_date, TUMBLE(ts_ltz, INTERVAL '1' MINUTE)
  UNION ALL
  SELECT
    CAST(biz_date AS DATE),
    TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE),
    'browse_uv',
    'ALL',
    CAST(COUNT(DISTINCT uid) AS DECIMAL(18, 2))
  FROM dwd_traffic_event
  WHERE action_id = 'browse'
  GROUP BY biz_date, TUMBLE(ts_ltz, INTERVAL '1' MINUTE)
) t;

-- =============================================================
-- 日指标（dws_traffic_day）：从 DWD 明细独立重算，非窗口持续累计
--   browse_pv = 当日 browse 事件数（明细 COUNT，非 SUM 分钟）
--   day_uv    = 当日 browse 用户独立重新去重（红线：禁止 SUM 分钟 UV）
-- 必须独立重算的原因（面试点，见开发日志 节点4）：
--   1. 红线：分钟 UV 不可加（同一用户可跨多个分钟窗口活跃）
--   2. 分钟窗口尾部未触发丢数：SUM 分钟 PV = 636 ≠ 明细 browse 641
--      → 日指标从明细兜底，窗口延迟不丢数
-- =============================================================
INSERT INTO dws_traffic_day
SELECT metric_date, metric_name, dimension_key, metric_value
FROM (
  SELECT
    CAST(biz_date AS DATE)                  AS metric_date,
    'browse_pv'                             AS metric_name,
    'ALL'                                   AS dimension_key,
    CAST(COUNT(*) AS DECIMAL(18, 2))        AS metric_value
  FROM dwd_traffic_event_day
  WHERE action_id = 'browse'
  GROUP BY CAST(biz_date AS DATE)
  UNION ALL
  SELECT
    CAST(biz_date AS DATE),
    'day_uv',
    'ALL',
    CAST(COUNT(DISTINCT uid) AS DECIMAL(18, 2))
  FROM dwd_traffic_event_day
  WHERE action_id = 'browse'
  GROUP BY CAST(biz_date AS DATE)
) t;
