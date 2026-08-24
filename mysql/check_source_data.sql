-- =============================================================
-- gmall 源业务库数据验收脚本（只读）
-- 用途:
--   1. 导入业务数据后快速确认行数和关键业务关系
--   2. 为后续 DataX 同步、ODS 行数校验、ADS 指标抽样提供基准
-- =============================================================

USE gmall;

-- 1. 基础行数：确认每张源表是否有数据
SELECT 'base_province' AS table_name, COUNT(*) AS row_count FROM base_province
UNION ALL
SELECT 'base_category', COUNT(*) FROM base_category
UNION ALL
SELECT 'spu_info', COUNT(*) FROM spu_info
UNION ALL
SELECT 'sku_info', COUNT(*) FROM sku_info
UNION ALL
SELECT 'user_info', COUNT(*) FROM user_info
UNION ALL
SELECT 'order_info', COUNT(*) FROM order_info
UNION ALL
SELECT 'order_detail', COUNT(*) FROM order_detail
UNION ALL
SELECT 'payment_info', COUNT(*) FROM payment_info
UNION ALL
SELECT 'order_refund_info', COUNT(*) FROM order_refund_info;

-- 2. 关联完整性：这些结果都应该是 0
SELECT 'order_without_user' AS check_name, COUNT(*) AS bad_count
FROM order_info o
LEFT JOIN user_info u ON o.user_id = u.id
WHERE u.id IS NULL
UNION ALL
SELECT 'order_without_province', COUNT(*)
FROM order_info o
LEFT JOIN base_province p ON o.province_id = p.id
WHERE p.id IS NULL
UNION ALL
SELECT 'detail_without_order', COUNT(*)
FROM order_detail d
LEFT JOIN order_info o ON d.order_id = o.id
WHERE o.id IS NULL
UNION ALL
SELECT 'detail_without_sku', COUNT(*)
FROM order_detail d
LEFT JOIN sku_info s ON d.sku_id = s.id
WHERE s.id IS NULL
UNION ALL
SELECT 'payment_without_order', COUNT(*)
FROM payment_info p
LEFT JOIN order_info o ON p.order_id = o.id
WHERE o.id IS NULL
UNION ALL
SELECT 'refund_without_order', COUNT(*)
FROM order_refund_info r
LEFT JOIN order_info o ON r.order_id = o.id
WHERE o.id IS NULL;

-- 3. 业务状态分布：订单数通常大于支付数，因为存在未支付订单
SELECT order_status, COUNT(*) AS order_count, ROUND(SUM(total_amount), 2) AS total_amount
FROM order_info
GROUP BY order_status
ORDER BY order_status;

-- 4. 核心指标基准：后续 ADS 手工抽样时可用这张结果做对照
SELECT
    DATE(create_time) AS pay_date,
    COUNT(*) AS paid_order_count,
    ROUND(SUM(payment_amount), 2) AS gmv
FROM payment_info
WHERE payment_status = 'SUCCESS'
GROUP BY DATE(create_time)
ORDER BY pay_date;

-- 5. 退款基准：后续可核对退款率 = refund_amount / gmv
SELECT
    DATE(create_time) AS refund_date,
    COUNT(*) AS refund_count,
    ROUND(SUM(refund_amount), 2) AS refund_amount
FROM order_refund_info
WHERE refund_status = 'SUCCESS'
GROUP BY DATE(create_time)
ORDER BY refund_date;

-- 6. 增量水位范围：后续 DataX where 条件会依赖 update_time
SELECT 'user_info' AS table_name, MIN(update_time) AS min_update_time, MAX(update_time) AS max_update_time FROM user_info
UNION ALL
SELECT 'order_info', MIN(update_time), MAX(update_time) FROM order_info
UNION ALL
SELECT 'order_detail', MIN(update_time), MAX(update_time) FROM order_detail
UNION ALL
SELECT 'payment_info', MIN(update_time), MAX(update_time) FROM payment_info
UNION ALL
SELECT 'order_refund_info', MIN(update_time), MAX(update_time) FROM order_refund_info;
