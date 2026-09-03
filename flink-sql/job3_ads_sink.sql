-- =============================================================
-- Job3: DWS -> ADS 无状态 sink（物理四层落库层，不碰计算）
-- 输入: Kafka dws_traffic_1m / dws_traffic_day / dws_trade_day（各自独立 group.id）
-- 输出: MySQL gmall_report_rt（JDBC upsert = INSERT ... ON DUPLICATE KEY UPDATE）
-- 无状态论证（面试点，README/设计 V2 决策 6）：
--   1. 读的是 DWS 聚合后的"当前绝对值"（upsert-kafka 同 key 已合并），
--      无窗口/去重/累计任何算子 → 无状态作业 → 重启快、天然幂等
--   2. 写是"同主键覆盖写 MySQL"：重放多少次最终值都收敛到同一行
--   3. 指标只在 Job2 算一次（节点6 前 MVP 里 ads 分支从 DWD 重算 = 双份状态，
--      与 dws 分支各自维护一份窗口/去重状态；拆后计算与落库解耦）
-- 类型: dws 层 window_start TIMESTAMP(3)（Kafka json）→ MySQL DATETIME
--       需 CAST TIMESTAMP(0)（Flink 精度不隐式转换，同 DWD 教训）
-- =============================================================
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.target' = 'remote';
SET 'pipeline.name' = 'job3_ads_sink';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'state.checkpoints.num-retained' = '3';
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- 分支1: 流量分钟
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
  'properties.group.id' = 'job3_ads_1m_group',
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

-- 分支2: 流量日
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
  'properties.group.id' = 'job3_ads_traffic_day_group',
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

-- 分支3: 交易日
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
  'properties.group.id' = 'job3_ads_trade_day_group',
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

-- 纯转发（无聚合无过滤）：DWS 值 → 同主键覆盖写 MySQL
INSERT INTO ads_traffic_1m
SELECT
  metric_date,
  CAST(window_start AS TIMESTAMP(0)),
  metric_name,
  dimension_key,
  metric_value
FROM dws_traffic_1m;

INSERT INTO ads_traffic_day
SELECT metric_date, metric_name, dimension_key, metric_value
FROM dws_traffic_day;

INSERT INTO ads_trade_day
SELECT metric_date, metric_name, dimension_key, metric_value
FROM dws_trade_day;
