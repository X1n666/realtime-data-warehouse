-- =============================================================
-- Job4 维度建模：Lookup Join 产维度指标（节点11，补齐「只有事实表没有维度」的缺口）
--
-- 产出两条指标，写入 dws_trade_day（与 job2 同一张 DWS 表、同一套主键）：
--   gmv_province           = 分省支付 GMV      dimension_key = 省份名
--   order_amount_category  = 分品类下单金额    dimension_key = 品类名
--
-- 对账锚点（天然完整性校验，见下）：
--   各维度求和必须回到已验证的日口径锚点——
--     SUM(gmv_province)          → 支付 GMV 1,136,485.35 / 182 单 ✓
--     SUM(order_amount_category) → 下单金额 1,334,596.00 / 398 明细行 ✓
--   这条「求和回锚点」同时是 **join 基数校验**：
--     LEFT 丢行（维表未命中）→ 和偏小；维表一对多导致行膨胀 → 和偏大。
--     两侧都严丝合缝，说明维表关联既没丢也没重。
--
-- 为什么写 dws_trade_day 就够了、job3 不用改：
--   job3 的 ADS 落库是**按 (metric_date, metric_name, dimension_key) 主键的纯转发**，
--   不感知 metric_name 取值 → 本作业新增的指标会被 job3 自动转发进 MySQL
--   ads_trade_day，Grafana 直接可查。这是长表（metric_name/dimension_key）设计
--   的红利：加维度指标不动下游。
--
-- ============ 三个已实测的语法/机制约束（EXPLAIN 实证，别踩） ============
-- 1) 【必须给左表时间属性】`FOR SYSTEM_TIME AS OF` 只能引用左表的**时间属性**。
--    直接写事件时间列会报：ValidationException: Temporal table join currently only
--    supports 'FOR SYSTEM_TIME AS OF' left table's time attribute field。
-- 2) 【JDBC 维表不能用事件时间】改成 `WATERMARK FOR create_time` 后再报：
--    Event-Time Temporal Table Join requires both primary key and row time attribute
--    in versioned table, but no row time attribute can be found。
--    → JDBC LookupTableSource **不是版本表**，事件时间 temporal join 用不了，
--      只能配处理时间。事件时间版本表要求维表本身是带 rowtime 的 changelog 流
--      （如 CDC 维表 / Hive 分区维表）——那是另一种建模（维表也要 CDC 入湖）。
-- 3) 【PROCTIME 不需要水位】本作业刻意用 `proc_time AS PROCTIME()` 而非水位：
--    dwd_order_detail 是 CDC 快照按主键扫描产出、**topic 内事件时间非单调**
--    （节点8 那个坑的根源）。用 PROCTIME 就完全不需要水位，天然绕开乱序问题——
--    而且 JDBC lookup 本来就是「处理时刻点查」，不存在按事件时间取历史版本的能力，
--    用事件时间语义是假的精确。**这是本作业能安全消费未排序 topic 的原因。**
--
-- 说明：两条 INSERT = 两个独立作业（Flink SQL 特性），各自 source 独立 group.id。
-- =============================================================
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.target' = 'remote';
SET 'pipeline.name' = 'job4_dim_lookup';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'state.checkpoints.num-retained' = '3';
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- ---------- 事实流 1：支付 DWD（分省 GMV 的度量来源） ----------
-- 独立 group.id：与 job2 的 job2_trade_day_sorted_group 分开，否则 1 个 partition
-- 会被一个作业独吞、另一个空转（节点6/8 踩过两次的教训）
CREATE TABLE dwd_payment_dim (
  id             BIGINT,
  order_id       BIGINT,
  user_id        BIGINT,
  payment_amount DECIMAL(16, 2),
  payment_status STRING,
  create_time    TIMESTAMP(3),
  -- 处理时间属性：维表关联的时刻（不需要 WATERMARK，见文件头约束 3）
  proc_time      AS PROCTIME(),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_payment_detail_sorted',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job4_dim_province_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- ---------- 维表 1：订单（取 province_id；MySQL 业务表直接当维表用） ----------
-- 说明：DWD 层没有 order_info 流（job1 只接了 payment/refund/order_detail 三条 CDC），
--   province_id 只存在于 MySQL 的 order_info 里 → 用 Lookup Join 补这一跳。
--   这也是链式 lookup 的典型场景：事实流 → 订单表 → 省份表。
CREATE TABLE dim_order_info (
  id          BIGINT,
  user_id     BIGINT,
  province_id BIGINT,
  total_amount DECIMAL(16, 2),
  create_time TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/gmall_rt?useSSL=false&serverTimezone=Asia/Shanghai',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'order_info',
  -- 维表缓存：max-rows 封顶 + ttl 到期失效。一致性与打库压力的权衡——
  --   缓存越大越省打库、但维表变更的可见延迟越长；ttl 是"最长不一致窗口"。
  --   省份几乎不变 → 可以放大 cache、放长 ttl；若维表是频繁变更的（如商品价格），
  --   要么缩短 ttl、要么走 CDC 维表（Flink 1.16+ lookup join 支持 changelog 维表）。
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '30min'
);

-- ---------- 维表 2：省份（链式 lookup 的第二跳） ----------
CREATE TABLE dim_province (
  id        BIGINT,
  name      STRING,
  region_id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/gmall_rt?useSSL=false&serverTimezone=Asia/Shanghai',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'base_province',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '30min'
);

-- ---------- 事实流 2：订单明细 DWD（分品类下单金额的度量来源） ----------
CREATE TABLE dwd_order_detail_dim (
  id          BIGINT,
  order_id    BIGINT,
  sku_id      BIGINT,
  order_price DECIMAL(16, 2),
  sku_num     BIGINT,
  create_time TIMESTAMP(3),
  proc_time   AS PROCTIME(),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_order_detail',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'job4_dim_category_group',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- ---------- 维表 3 / 4：商品 → 品类 ----------
CREATE TABLE dim_sku (
  id          BIGINT,
  spu_id      BIGINT,
  sku_name    STRING,
  price       DECIMAL(16, 2),
  category_id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/gmall_rt?useSSL=false&serverTimezone=Asia/Shanghai',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'sku_info',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '30min'
);

CREATE TABLE dim_category (
  id             BIGINT,
  name           STRING,
  category_level INT,
  parent_id      BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/gmall_rt?useSSL=false&serverTimezone=Asia/Shanghai',
  'username' = 'root',
  'password' = '123456',
  'table-name' = 'base_category',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '30min'
);

-- ---------- DWS sink（与 job2 同表同主键；job3 会自动转发到 MySQL） ----------
CREATE TABLE dws_trade_day_dim (
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

-- ---------- 分支 1：分省支付 GMV（链式 Lookup Join：payment → order → province） ----------
-- 时间归属与 job2 的 gmv 一致：按 create_time 的日期归日（非窗口持续累计）。
-- 聚合产生 retract 流（EXPLAIN 里是 SUM_RETRACT）→ 必须 upsert-kafka 承载（节点2 教训）。
INSERT INTO dws_trade_day_dim
SELECT
  CAST(p.create_time AS DATE)      AS metric_date,
  'gmv_province'                   AS metric_name,
  pr.name                          AS dimension_key,
  CAST(SUM(p.payment_amount) AS DECIMAL(18, 2)) AS metric_value
FROM dwd_payment_dim AS p
JOIN dim_order_info FOR SYSTEM_TIME AS OF p.proc_time AS o
  ON p.order_id = o.id
JOIN dim_province   FOR SYSTEM_TIME AS OF p.proc_time AS pr
  ON o.province_id = pr.id
WHERE p.payment_status = 'SUCCESS'
GROUP BY CAST(p.create_time AS DATE), pr.name;

-- ---------- 分支 2：分品类下单金额（链式：order_detail → sku → category） ----------
INSERT INTO dws_trade_day_dim
SELECT
  CAST(d.create_time AS DATE)      AS metric_date,
  'order_amount_category'          AS metric_name,
  c.name                           AS dimension_key,
  CAST(SUM(d.order_price * d.sku_num) AS DECIMAL(18, 2)) AS metric_value
FROM dwd_order_detail_dim AS d
JOIN dim_sku      FOR SYSTEM_TIME AS OF d.proc_time AS s
  ON d.sku_id = s.id
JOIN dim_category FOR SYSTEM_TIME AS OF d.proc_time AS c
  ON s.category_id = c.id
GROUP BY CAST(d.create_time AS DATE), c.name;
