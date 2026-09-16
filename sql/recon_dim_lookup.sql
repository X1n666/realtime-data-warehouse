-- =============================================================
-- 维度指标批侧对账（对应 flink-sql/job4_dim_lookup.sql 的两条流指标）
--
-- 用法：
--   docker exec -i gmall_mysql mysql -uroot -p123456 -B < sql/recon_dim_lookup.sql
--
-- 对账逻辑同项目其余指标：**批侧独立重算一遍**，再与流侧 ads_trade_day 逐值比对。
-- 本文件只负责给出批侧"标准答案"，流侧值用：
--   SELECT * FROM gmall_report_rt.ads_trade_day
--   WHERE metric_name IN ('gmv_province','order_amount_category') ORDER BY metric_name, metric_value DESC;
--
-- 测试日 2026-09-01（生成器 seed 20260901，确定性可重放）
-- =============================================================

-- ---------- 对账 0：join 基数预检（先证明维表关联不丢行不膨胀） ----------
-- 若这里不是 200/200/398/182，说明维表数据有问题，后面所有维度值都不可信
SELECT 'join 命中数（期望 200 / 398 / 11 / 182）' AS check_item,
       (SELECT COUNT(*) FROM gmall_rt.order_info   o JOIN gmall_rt.base_province p ON o.province_id = p.id) AS order_hit_province,
       (SELECT COUNT(*) FROM gmall_rt.order_detail d JOIN gmall_rt.sku_info      s ON d.sku_id      = s.id) AS detail_hit_sku,
       (SELECT COUNT(*) FROM gmall_rt.sku_info     s JOIN gmall_rt.base_category c ON s.category_id = c.id) AS sku_hit_category,
       (SELECT COUNT(*) FROM gmall_rt.payment_info pi JOIN gmall_rt.order_info   o ON pi.order_id   = o.id) AS payment_hit_order;

-- ---------- 对账 1：gmv_province（分省支付 GMV） ----------
-- 期望合计 = 1,136,485.35 / 182 单（= 日口径锚点 gmv，见 README §4.3）
-- 12 行，维度 key = 省份名
SELECT p.name AS dimension_key,
       ROUND(SUM(pi.payment_amount), 2) AS metric_value,
       COUNT(DISTINCT pi.order_id)      AS pay_orders
FROM gmall_rt.payment_info pi
JOIN gmall_rt.order_info   o ON pi.order_id = o.id
JOIN gmall_rt.base_province p ON o.province_id = p.id
WHERE pi.payment_status = 'SUCCESS'
GROUP BY p.name
ORDER BY metric_value DESC;

-- ---------- 对账 2：order_amount_category（分品类下单金额） ----------
-- 期望合计 = 1,334,596.00 / 398 明细行（= 日口径锚点 order_gmv）
-- 9 行，维度 key = 品类名
SELECT c.name AS dimension_key,
       ROUND(SUM(d.order_price * d.sku_num), 2) AS metric_value,
       COUNT(*)                                  AS detail_rows
FROM gmall_rt.order_detail d
JOIN gmall_rt.sku_info      s ON d.sku_id = s.id
JOIN gmall_rt.base_category c ON s.category_id = c.id
GROUP BY c.name
ORDER BY metric_value DESC;

-- ---------- 对账 3：两条指标的合计必须回到日锚点（一行看完） ----------
-- 这是维度建模的**完整性校验**：LEFT 丢行 → 和偏小；维表一对多 → 和偏大
SELECT 'sum(gmv_province) 应 = 1136485.35'          AS assertion,
       ROUND(SUM(pi.payment_amount), 2)             AS batch_value
FROM gmall_rt.payment_info pi
JOIN gmall_rt.order_info   o ON pi.order_id = o.id
JOIN gmall_rt.base_province p ON o.province_id = p.id
WHERE pi.payment_status = 'SUCCESS'
UNION ALL
SELECT 'sum(order_amount_category) 应 = 1334596.00',
       ROUND(SUM(d.order_price * d.sku_num), 2)
FROM gmall_rt.order_detail d
JOIN gmall_rt.sku_info      s ON d.sku_id = s.id
JOIN gmall_rt.base_category c ON s.category_id = c.id;
