-- =============================================================
-- Job2 交易域：DWD(payment/refund/order_detail) -> DWS(dws_trade_day)，只写 Kafka
-- ADS 落库由 Job3 无状态转发（节点6 拆三作业，见 job3_ads_sink.sql）
-- 指标（口径锚点 4/5/6 + 节点10 下单侧锚点 7）:
--   gmv                  = SUM(payment_amount) WHERE status='SUCCESS'，不退 refund
--   payment_order_count  = COUNT(DISTINCT order_id)（支付订单数）
--   payment_user_count   = COUNT(DISTINCT user_id)（日支付用户数）
--   refund_amount        = SUM(refund_amount) WHERE refund_status='SUCCESS'（按退款日归日）
--   order_gmv            = SUM(order_price*sku_num)（下单金额；与 gmv 对照=下单→支付转化）
--   order_count          = COUNT(DISTINCT order_id)（下单订单数）
--   order_detail_count   = COUNT(*)（下单明细行数）
-- 时间归属: create_time（支付/退款时间，MySQL datetime 本地语义，无时区转换）
-- 日聚合方式: GROUP BY CAST(create_time AS DATE) —— 非窗口持续累计
--   （vs 窗口聚合: 无 watermark/迟到问题，任何时刻到达的支付归入其支付日）
-- 分钟聚合: TUMBLE 窗口（watermark = create_time - 5s）——测试日尾部 1 个
--   窗口（23:32 单笔）因无后续事件推进水位不触发，属预期差异（同流量尾部）
-- 输出: dws_trade_day（upsert-kafka）+ dws_trade_1m（upsert-kafka，topic 已预留）
-- 事件化重排（节点8 排障）: 测试数据 payment 的 id 流水号与支付时间错位，
--   CDC snapshot 按主键扫描 → topic 内事件时间非单调 → 窗口水位跳变丢数。
--   消费 topic = dwd_payment_detail_sorted（scripts/replay_dwd_payment_sorted.sh
--   一次性按 create_time 排序重放，模拟真实 binlog 提交序；幂等可重跑）
-- 多 source（payment/refund 两表）在 DWS 层合流成指标 → 同一/多个 INSERT
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
  -- 事件时间 = 支付时间（本地墙上时间，TIMESTAMP 窗口直接切本地整分钟）
  WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_payment_detail_sorted',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_trade_day_sorted_group',
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
  'properties.group.id' = 'job2_trade_refund_day_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- 订单明细 source（节点10 加分：下单侧指标）。独立 group（与支付/退款 source
-- 各作业独立 group，防共享 group 消费中断 —— 节点8 教训），读取 job1 CDC 产出的
-- dwd_order_detail（job1 一次性快照 + binlog，非 sorted 重放：订单不参与窗口
-- 聚合，日聚合 GROUP BY DATE 天然容忍乱序，无需事件时间单调）
CREATE TABLE dwd_order_detail (
  id          BIGINT,
  order_id    BIGINT,
  sku_id      BIGINT,
  sku_name    STRING,
  order_price DECIMAL(16, 2),
  sku_num     BIGINT,
  create_time TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_order_detail',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_trade_order_day_group',
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

-- 分钟窗口独立 source（日/分钟两个 INSERT = 两个独立作业，必须各自 group.id，
-- 共享 group 会 rebalance 瓜分 1 partition —— 节点8 实测分钟作业消费中断在 12:04）
CREATE TABLE dwd_payment_detail_1m (
  id             BIGINT,
  order_id       BIGINT,
  user_id        BIGINT,
  payment_type   STRING,
  trade_no       STRING,
  payment_amount DECIMAL(16, 2),
  payment_status STRING,
  create_time    TIMESTAMP(3),
  callback_time  TIMESTAMP(3),
  WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_payment_detail_sorted',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job2_trade_1m_sorted_group',
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
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'order_gmv',
    'ALL',
    CAST(SUM(order_price * sku_num) AS DECIMAL(18,2))
  FROM dwd_order_detail
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'order_count',
    'ALL',
    CAST(COUNT(DISTINCT order_id) AS DECIMAL(18,2))
  FROM dwd_order_detail
  GROUP BY CAST(create_time AS DATE)
  UNION ALL
  SELECT
    CAST(create_time AS DATE),
    'order_detail_count',
    'ALL',
    CAST(COUNT(*) AS DECIMAL(18,2))
  FROM dwd_order_detail
  GROUP BY CAST(create_time AS DATE)
) t;

-- =============================================================
-- 交易分钟窗口（dws_trade_1m）：分钟 GMV / 支付订单数
-- 窗口按支付时间本地整分钟切（create_time 是本地语义，无需 UTC 转换）
-- 与流量分钟同机制：watermark 推进触发；尾部窗口差异同流量（见文件头注释）
-- =============================================================
CREATE TABLE dws_trade_1m (
  metric_date   DATE,
  window_start  TIMESTAMP(3),
  metric_name   STRING,
  dimension_key STRING,
  metric_value  DECIMAL(18, 2),
  PRIMARY KEY (metric_date, window_start, metric_name, dimension_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dws_trade_1m',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO dws_trade_1m
SELECT metric_date, window_start, metric_name, dimension_key, metric_value
FROM (
  SELECT
    CAST(TUMBLE_START(create_time, INTERVAL '1' MINUTE) AS DATE) AS metric_date,
    TUMBLE_START(create_time, INTERVAL '1' MINUTE) AS window_start,
    'gmv'                                    AS metric_name,
    'ALL'                                    AS dimension_key,
    CAST(SUM(payment_amount) AS DECIMAL(18,2)) AS metric_value
  FROM dwd_payment_detail_1m
  WHERE payment_status = 'SUCCESS'
  GROUP BY TUMBLE(create_time, INTERVAL '1' MINUTE)
  UNION ALL
  SELECT
    CAST(TUMBLE_START(create_time, INTERVAL '1' MINUTE) AS DATE),
    TUMBLE_START(create_time, INTERVAL '1' MINUTE),
    'payment_order_count',
    'ALL',
    CAST(COUNT(DISTINCT order_id) AS DECIMAL(18,2))
  FROM dwd_payment_detail_1m
  WHERE payment_status = 'SUCCESS'
  GROUP BY TUMBLE(create_time, INTERVAL '1' MINUTE)
) t;
