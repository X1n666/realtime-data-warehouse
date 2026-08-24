-- =============================================================
-- Job2: DWD -> DWS（流量分钟窗口聚合）+ ADS MySQL（MVP 同作业分支）
-- 输入: Kafka dwd_traffic_event
-- 输出: Kafka dws_traffic_1m（Upsert）+ MySQL gmall_report_rt.ads_traffic_1m
-- 有状态: 窗口聚合（watermark + 5s 乱序容忍）
-- 口径: PV = browse 事件数（锚点 1）；窗口按 UTC 整分钟切，
--       展示值由 table.local-time-zone=Asia/Shanghai 转为 +8 本地时间
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

CREATE TABLE ads_traffic_1m (
  metric_date   DATE,
  window_start  TIMESTAMP(0),
  metric_name   STRING,
  dimension_key STRING,
  metric_value  DECIMAL(18, 2),
  PRIMARY KEY (metric_date, window_start, metric_name, dimension_key) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/gmall_report_rt?useSSL=false&serverTimezone=Asia/Shanghai',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'ads_traffic_1m',
  'sink.buffer-flush.max-rows' = '1'
);

-- DWS Upsert Kafka：browse PV 分钟窗口（窗口结束才触发，Upsert 输出当前值）
INSERT INTO dws_traffic_1m
SELECT
  CAST(biz_date AS DATE)                AS metric_date,
  TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE) AS window_start,
  'browse_pv'                           AS metric_name,
  'ALL'                                 AS dimension_key,
  CAST(COUNT(*) AS DECIMAL(18, 2))      AS metric_value
FROM dwd_traffic_event
WHERE action_id = 'browse'
GROUP BY biz_date, TUMBLE(ts_ltz, INTERVAL '1' MINUTE);

-- ADS MySQL（同作业分支，覆盖式 upsert 幂等）
INSERT INTO ads_traffic_1m
SELECT
  CAST(biz_date AS DATE)                AS metric_date,
  TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE) AS window_start,
  'browse_pv'                           AS metric_name,
  'ALL'                                 AS dimension_key,
  CAST(COUNT(*) AS DECIMAL(18, 2))      AS metric_value
FROM dwd_traffic_event
WHERE action_id = 'browse'
GROUP BY biz_date, TUMBLE(ts_ltz, INTERVAL '1' MINUTE);
