CREATE TABLE dwd_paymnet-detail (
  id BIGINT,
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
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_traffic_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);
