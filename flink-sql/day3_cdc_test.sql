-- Day3 CDC source 连通性测试：mysql-cdc 读 payment_info 全量快照
-- 运行方式（避免占用 jobmanager 容器 cgroup 内存）:
--   docker run --rm --network ecommerce-realtime-data-warehouse_default \
--     -v $PWD/flink-lib:/opt/flink/lib -v $PWD/flink-sql:/tmp/flink-sql \
--     flink:1.19.1 /opt/flink/bin/sql-client.sh -f /tmp/flink-sql/day3_cdc_test.sql
-- 注意: MySQL TZ=Asia/Shanghai(+8), 必须显式 server-time-zone 否则校验失败
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.target' = 'local';

CREATE TABLE payment_cdc (
  id            BIGINT,
  out_trade_no  STRING,
  order_id      BIGINT,
  user_id       BIGINT,
  payment_type  STRING,
  trade_no      STRING,
  payment_amount DECIMAL(16, 2),
  subject       STRING,
  payment_status STRING,
  create_time   TIMESTAMP(0),
  callback_time TIMESTAMP(0),
  update_time   TIMESTAMP(0),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector'         = 'mysql-cdc',
  'hostname'          = 'mysql',
  'port'              = '3306',
  'username'          = 'root',
  'password'          = '123456',
  'database-name'     = 'gmall_rt',
  'table-name'        = 'payment_info',
  'scan.startup.mode' = 'initial',
  'server-id'         = '5400-5404',
  'server-time-zone'  = 'Asia/Shanghai'
);

SELECT COUNT(*) AS payment_count FROM payment_cdc;
