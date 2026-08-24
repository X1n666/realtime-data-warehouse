-- =============================================================
-- Job1: ODS -> DWD（行为日志域）
-- 输入: Kafka ods_traffic_log（V2 契约 JSON）
-- 输出: Kafka dwd_traffic_event（扁平化清洗后）
-- 有状态: event_id 去重（ROW_NUMBER，同 event_id 只留最早一条）
-- 提交: 独立 sql-client 容器 + remote target（见 scripts/submit_sql.sh）
-- =============================================================
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.target' = 'remote';
SET 'pipeline.name' = 'job1_ods_to_dwd';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'state.checkpoints.num-retained' = '3';

CREATE TABLE ods_traffic_log (
  common ROW<uid BIGINT, platform STRING, province_id BIGINT, ts STRING>,
  page ROW<page_id STRING>,
  action ROW<action_id STRING, item BIGINT>,
  ts STRING,
  event_id STRING,
  event_time_ms BIGINT,
  biz_date STRING,
  source_timezone STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_traffic_log',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job1_ods_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- 主键=event_id（设计文档 4.1）：ROW_NUMBER 去重产生 changelog，
-- 必须用 upsert-kafka（append sink 不接受 update/delete）
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
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_traffic_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- event_id 去重（有状态）：同 event_id 重复只保留 event_time_ms 最早一条
INSERT INTO dwd_traffic_event
SELECT uid, platform, province_id, page_id, action_id, item, ts,
       event_id, event_time_ms, biz_date, source_timezone
FROM (
  SELECT
    common.uid        AS uid,
    common.platform   AS platform,
    common.province_id AS province_id,
    page.page_id      AS page_id,
    action.action_id  AS action_id,
    action.item       AS item,
    ts, event_id, event_time_ms, biz_date, source_timezone,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time_ms) AS rn
  FROM ods_traffic_log
)
WHERE rn = 1;
