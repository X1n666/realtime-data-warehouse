-- =============================================================
-- Job2: DWD -> DWS（流量分钟窗口聚合 + 日指标独立重算）+ ADS MySQL
-- 输入: Kafka dwd_traffic_event
-- 输出: Kafka dws_traffic_1m/dws_traffic_day + MySQL gmall_report_rt.ads_traffic_1m/ads_traffic_day
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

-- ADS 分支独立 source（同一 topic，不同 group.id）：
-- 两个 INSERT 提交为两个独立作业，若共享 group 则 partition 只被一个
-- 作业消费，另一个空转（Day4 实测：dws_traffic_1m 空、ads 有数据）
CREATE TABLE dwd_traffic_event_ads (
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
  'properties.group.id' = 'job2_ads_group',
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

-- ADS MySQL（独立作业分支，覆盖式 upsert 幂等；source 用独立的 dwd_traffic_event_ads）
INSERT INTO ads_traffic_1m
SELECT
  CAST(biz_date AS DATE)                AS metric_date,
  TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE) AS window_start,
  'browse_pv'                           AS metric_name,
  'ALL'                                 AS dimension_key,
  CAST(COUNT(*) AS DECIMAL(18, 2))      AS metric_value
FROM dwd_traffic_event_ads
WHERE action_id = 'browse'
GROUP BY biz_date, TUMBLE(ts_ltz, INTERVAL '1' MINUTE);

-- =============================================================
-- 日指标（dws_traffic_day / ads_traffic_day）：从 DWD 明细独立重算
--   browse_pv = 当日 browse 事件数（明细 COUNT，非 SUM 分钟）
--   day_uv    = 当日 browse 用户独立重新去重（锚点红线：禁止 SUM 分钟 UV）
-- 为什么必须独立重算而不是从分钟表上卷（面试点，见开发日志 节点4）：
--   1. 红线：分钟 UV 不可加（同一用户可跨多个分钟窗口活跃）→ 日 UV 需重新去重
--   2. 分钟窗口尾部未触发丢数：2000 条日志尾部 5 条 browse 落在未触发窗口，
--      SUM 分钟 PV = 636 ≠ 明细 browse 641 → 日指标从明细兜底（窗口延迟不丢数）
-- 聚合方式: GROUP BY CAST(biz_date AS DATE) 非窗口持续累计（同交易日模式，
--   无 watermark/窗口触发问题；biz_date 由生成器按事件本地日打标，与分钟表同锚点）
-- 注意: 本段 2 INSERT 又是 2 个独立作业 → source 用独立 group.id（同文件既有模式）
-- =============================================================
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

-- ADS 分支独立 source（同 topic，独立 group.id）
CREATE TABLE dwd_traffic_event_day_ads (
  uid BIGINT,
  action_id STRING,
  biz_date STRING,
  event_id STRING,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_traffic_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_traffic_day_ads_group',
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

CREATE TABLE ads_traffic_day (
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
  'table-name' = 'ads_traffic_day',
  'sink.buffer-flush.max-rows' = '1'
);

-- DWS：browse_pv / day_uv 按业务日持续累计（非窗口，upsert 更新当日值）
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

-- ADS（独立作业分支）
INSERT INTO ads_traffic_day
SELECT metric_date, metric_name, dimension_key, metric_value
FROM (
  SELECT
    CAST(biz_date AS DATE)                  AS metric_date,
    'browse_pv'                             AS metric_name,
    'ALL'                                   AS dimension_key,
    CAST(COUNT(*) AS DECIMAL(18, 2))        AS metric_value
  FROM dwd_traffic_event_day_ads
  WHERE action_id = 'browse'
  GROUP BY CAST(biz_date AS DATE)
  UNION ALL
  SELECT
    CAST(biz_date AS DATE),
    'day_uv',
    'ALL',
    CAST(COUNT(DISTINCT uid) AS DECIMAL(18, 2))
  FROM dwd_traffic_event_day_ads
  WHERE action_id = 'browse'
  GROUP BY CAST(biz_date AS DATE)
) t;
