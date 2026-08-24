# SQL 错题复习手册

> 根据当前对话中的提问整理。题目描述以截图和对话内容为依据做了精简还原，重点保留考点、错误原因、解题路径和可直接复习的标准 SQL。

## 使用说明

这份手册按照你在当前对话中的**提问频率**排序，而不是按照题号或传统教材目录排序。

- **最高频**：你反复出现相似错误，应该优先复习。
- **高频**：基本思路已经掌握，但容易在粒度、边界或语法上出错。
- **中频**：题型相对独立，掌握模板即可。
- 每个知识点最多保留 3 道题，优先选择覆盖面最广、最典型的题。
- 后续出现新的 SQL 问题时，优先补充已有章节；只有出现新的考点时才新增章节。
- 如果同一知识点超过 3 道题，删除重复度最高的题，保留最能暴露薄弱点的题。

## 建议复习顺序

1. 聚合粒度与窗口函数
2. 多表连接与数据膨胀
3. 日期窗口与滚动统计
4. 连续区间问题
5. 排名、Top N 与百分位
6. 条件聚合、比例与 NULL
7. 每组第一条、最后一条记录
8. 事件流与同时在线人数

---

# 一、聚合粒度与窗口函数（最高频）

## 核心判断

写 SQL 前先回答一句话：**最终结果中的一行代表什么？**

如果一行代表“每月每份试卷”，就必须先得到：

```text
exam_id + month 唯一一行
```

然后才能在这些“月汇总行”上计算累计值。不要在明细行上计算窗口，再用 `GROUP BY` 强行压缩。

## 典型题 1：SQL143 每份试卷每月作答数和截至当月作答总数

### 原题摘要

输出每份试卷每个月的作答次数 `month_cnt`，以及截至当月的累计作答次数 `cum_exam_cnt`。

### 你容易出错的地方

- 在答题明细上直接执行窗口 `COUNT()`。
- 窗口中错误地按月份分区，导致每个月重新累计。
- 窗口计算后再 `GROUP BY`，出现 `ONLY_FULL_GROUP_BY` 报错。
- 使用 `COUNT(score)`，遗漏了未完成但已经开始作答的记录。

### 思路

1. 先按“试卷、月份”聚合，每个月得到一行。
2. 再按试卷分区、月份排序，对月作答数累计求和。

### 标准答案

```sql
WITH monthly AS (
    SELECT
        exam_id,
        DATE_FORMAT(start_time, '%Y%m') AS start_month,
        COUNT(*) AS month_cnt
    FROM exam_record
    GROUP BY exam_id, DATE_FORMAT(start_time, '%Y%m')
)
SELECT
    exam_id,
    start_month,
    month_cnt,
    SUM(month_cnt) OVER (
        PARTITION BY exam_id
        ORDER BY start_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_exam_cnt
FROM monthly
ORDER BY exam_id, start_month;
```

### 一句话模板

> 先聚合到结果需要的最小粒度，再在聚合结果上使用窗口函数。

---

## 典型题 2：SQL142 对试卷得分做 Min-Max 归一化

### 原题摘要

对每份高难度试卷的成绩进行 Min-Max 归一化：

```text
(score - min_score) / (max_score - min_score) * 100
```

然后输出每个用户、每份试卷的归一化平均分。若该试卷只有一个有效成绩，则保留原成绩。

### 你容易出错的地方

- `GROUP BY uid, exam_id` 后直接选明细列 `score`。
- 按用户分组求最值，而题目要求的是“每份试卷”的全体成绩最值。
- 先求用户平均分再归一化，计算顺序反了。

### 思路

1. 筛选高难度试卷和有效成绩。
2. 使用窗口函数给每条成绩附上该试卷的最小值、最大值、记录数。
3. 对每条成绩归一化。
4. 最后按用户和试卷求平均。

### 标准答案

```sql
WITH hard_exam AS (
    SELECT r.*
    FROM exam_record r
    JOIN examination_info i USING (exam_id)
    WHERE i.difficulty = 'hard'
      AND r.score IS NOT NULL
),
scored AS (
    SELECT
        uid,
        exam_id,
        score,
        MIN(score) OVER (PARTITION BY exam_id) AS min_score,
        MAX(score) OVER (PARTITION BY exam_id) AS max_score,
        COUNT(*)   OVER (PARTITION BY exam_id) AS score_cnt
    FROM hard_exam
)
SELECT
    uid,
    exam_id,
    ROUND(AVG(
        CASE
            WHEN score_cnt = 1 THEN score
            ELSE (score - min_score) * 100.0 / (max_score - min_score)
        END
    ), 0) AS avg_new_score
FROM scored
GROUP BY uid, exam_id
ORDER BY exam_id, avg_new_score DESC;
```

---

## 典型题 3：LeetCode 1661 每台机器的进程平均运行时间

### 原题摘要

每个进程有一条 `start` 和一条 `end` 记录，时间是浮点数秒。计算每台机器完成进程的平均耗时。

### 你容易出错的地方

- 对浮点数字段使用 `TIMESTAMPDIFF()`。该函数用于日期时间，不适用于浮点秒数。
- 自连接时只连接机器，没有同时连接进程。
- 先 `ROUND()` 每个进程耗时，再求平均，过早损失精度。

### 标准答案

```sql
SELECT
    s.machine_id,
    ROUND(AVG(e.timestamp - s.timestamp), 3) AS processing_time
FROM Activity s
JOIN Activity e
  ON s.machine_id = e.machine_id
 AND s.process_id = e.process_id
WHERE s.activity_type = 'start'
  AND e.activity_type = 'end'
GROUP BY s.machine_id;
```

---

# 二、多表连接与数据膨胀（最高频）

## 核心判断

连接之前，分别判断两张表在连接键上是：

```text
一对一、一对多，还是多对多
```

如果左右两边同一个连接键都不唯一，就会发生数据膨胀。解决方法通常是：**先分别聚合，再连接**。

## 典型题 1：SQL126 最畅销的 SKU

### 原题摘要

以最新库存快照日为基准，统计每家门店最近 7 天各 SKU 销量、日均销量、库存覆盖天数，并选出门店内销量 Top 3。无销量 SKU 也参与排名，销量记为 0。

### 你容易出错的地方

- 将全部历史库存快照与每日销量直接连接，形成多对多膨胀。
- 使用内连接，导致无销量 SKU 消失。
- `ROW_NUMBER()` 按 `store_id, sku_id` 分区，使每个 SKU 都从第 1 名开始。
- 在明细行上使用窗口 `SUM()`，然后再 `GROUP BY`。

### 思路

1. 找最新快照日期。
2. 只保留该日库存，每个门店 SKU 一行。
3. 单独汇总最近 7 天销量，每个门店 SKU 一行。
4. 左连接，补齐无销量 SKU。
5. 按门店排名。

### 标准答案

```sql
WITH latest_date AS (
    SELECT MAX(snapshot_date) AS dt
    FROM store_stock_
),
latest_stock AS (
    SELECT s.store_id, s.sku_id, s.stock_qty
    FROM store_stock_ s
    CROSS JOIN latest_date d
    WHERE s.snapshot_date = d.dt
),
sales_7d AS (
    SELECT
        x.store_id,
        x.sku_id,
        SUM(x.qty) AS last7d_qty
    FROM sales_daily_ x
    CROSS JOIN latest_date d
    WHERE x.sale_date BETWEEN DATE_SUB(d.dt, INTERVAL 6 DAY) AND d.dt
    GROUP BY x.store_id, x.sku_id
),
sku_stats AS (
    SELECT
        i.store_id,
        i.store_name,
        i.city,
        s.sku_id,
        COALESCE(d.last7d_qty, 0) AS last7d_qty,
        ROUND(COALESCE(d.last7d_qty, 0) / 7, 2) AS avg_daily_qty,
        s.stock_qty
    FROM latest_stock s
    JOIN store_info_ i ON i.store_id = s.store_id
    LEFT JOIN sales_7d d
      ON d.store_id = s.store_id
     AND d.sku_id = s.sku_id
),
ranked AS (
    SELECT
        *,
        CASE
            WHEN avg_daily_qty > 0
            THEN ROUND(stock_qty / avg_daily_qty, 1)
            ELSE NULL
        END AS coverage_days,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY last7d_qty DESC, sku_id ASC
        ) AS rank_in_store
    FROM sku_stats
)
SELECT *
FROM ranked
WHERE rank_in_store <= 3
ORDER BY store_id, rank_in_store;
```

---

## 典型题 2：SQL188 牛客直播各科目出勤率

### 原题摘要

出勤定义为同一用户在同一课程累计在线至少 10 分钟。计算每门课程的出勤人数占报名人数的比例。

### 你容易出错的地方

- 课程表、报名表、在线记录表只按课程连接，造成用户之间交叉匹配。
- 虽然补上 `b.user_id = a.user_id`，但一个用户可能有多段在线记录，连接后报名行仍会重复。
- 在 `WHERE` 中过滤在线时长，使 `LEFT JOIN` 退化成内连接。

### 思路

先把在线记录聚合成“每个用户、每门课程一行”，判断是否出勤，再与报名数据计算比例。

### 标准答案

```sql
WITH attended_user AS (
    SELECT
        user_id,
        course_id
    FROM attend_tb
    GROUP BY user_id, course_id
    HAVING SUM(TIMESTAMPDIFF(SECOND, in_datetime, out_datetime)) >= 600
),
course_stat AS (
    SELECT
        b.course_id,
        SUM(b.if_sign) AS signed_cnt,
        COUNT(DISTINCT CASE
            WHEN b.if_sign = 1 AND a.user_id IS NOT NULL THEN b.user_id
        END) AS attended_cnt
    FROM behavior_tb b
    LEFT JOIN attended_user a
      ON a.course_id = b.course_id
     AND a.user_id = b.user_id
    GROUP BY b.course_id
)
SELECT
    c.course_id,
    c.course_name,
    ROUND(s.attended_cnt * 100.0 / s.signed_cnt, 2) AS attend_rate
FROM course_tb c
JOIN course_stat s ON s.course_id = c.course_id
ORDER BY c.course_id;
```

---

## 典型题 3：SQL125 医院门诊复诊率与抗生素用药占比

### 原题摘要

按科室统计 2024 年 2 月就诊人次、去重患者数、30 天内同科室复诊率和抗生素处方占比。

### 你容易出错的地方

- 将就诊表和处方表直接连接后统计就诊人次，一次就诊有多条处方时会重复计数。
- 先筛选 2 月再使用 `LAG()`，导致 2 月第一次就诊看不到 1 月的上次就诊。
- 复诊率误用“复诊患者数 / 去重患者数”，题目实际要求“复诊人次 / 就诊人次”。

### 标准答案

```sql
WITH visit_with_prev AS (
    SELECT
        visit_id,
        patient_id,
        dept,
        visit_date,
        LAG(visit_date, 1) OVER (
            PARTITION BY patient_id, dept
            ORDER BY visit_date, visit_id
        ) AS prev_visit_date
    FROM visits
),
visit_stat AS (
    SELECT
        dept,
        COUNT(*) AS feb_2024_visits,
        COUNT(DISTINCT patient_id) AS feb_2024_unique_patients,
        ROUND(
            SUM(
                CASE
                    WHEN DATEDIFF(visit_date, prev_visit_date) <= 30 THEN 1
                    ELSE 0
                END
            ) * 100.0 / COUNT(*),
            2
        ) AS feb_2024_revisit_rate
    FROM visit_with_prev
    WHERE visit_date >= '2024-02-01'
      AND visit_date <  '2024-03-01'
    GROUP BY dept
),
prescription_stat AS (
    SELECT
        v.dept,
        ROUND(
            COALESCE(SUM(p.is_antibiotic) * 100.0 / COUNT(p.prescription_id), 0),
            2
        ) AS feb_2024_antibiotic_rate
    FROM visits v
    LEFT JOIN prescriptions p ON p.visit_id = v.visit_id
    WHERE v.visit_date >= '2024-02-01'
      AND v.visit_date <  '2024-03-01'
    GROUP BY v.dept
)
SELECT v.*, p.feb_2024_antibiotic_rate
FROM visit_stat v
JOIN prescription_stat p USING (dept)
ORDER BY dept;
```

---

# 三、日期窗口与滚动统计（高频）

## 核心判断

先区分两个概念：

- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`：最近 7 **行**。
- 日期范围：最近 7 **天**。

只有在“每天恰好一行且日期连续”时，最近 7 行才等价于最近 7 天。

## 典型题 1：SQL177 国庆期间近 7 日日均取消订单量

### 原题摘要

统计 10 月 1 日至 3 日每天最近 7 天的日均完成订单量和日均取消订单量。

### 思路

1. 先按天统计完成量、取消量。
2. 再在每日结果上计算最近 7 行的总量。
3. 最后只输出 10 月 1 日至 3 日。

### 标准答案

```sql
WITH daily AS (
    SELECT
        DATE(order_time) AS dt,
        SUM(start_time IS NOT NULL) AS finish_num,
        SUM(start_time IS NULL AND finish_time IS NOT NULL) AS cancel_num
    FROM tb_get_car_order
    WHERE DATE(order_time) BETWEEN '2021-09-25' AND '2021-10-03'
    GROUP BY DATE(order_time)
),
rolling AS (
    SELECT
        dt,
        ROUND(SUM(finish_num) OVER (
            ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) / 7, 2) AS finish_num_7d,
        ROUND(SUM(cancel_num) OVER (
            ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) / 7, 2) AS cancel_num_7d
    FROM daily
)
SELECT *
FROM rolling
WHERE dt BETWEEN '2021-10-01' AND '2021-10-03'
ORDER BY dt;
```

### 关键提醒

不能先筛选 10 月 1 日至 3 日再做窗口，否则前 6 天已经被删除。

---

## 典型题 2：LeetCode 1321 餐馆营业额变化增长

### 原题摘要

计算每一天以及此前 6 天的 7 日营业额和日均营业额。题目保证每天至少有一位顾客。

### 标准答案

```sql
WITH daily AS (
    SELECT
        visited_on,
        SUM(amount) AS amount
    FROM Customer
    GROUP BY visited_on
),
rolling AS (
    SELECT
        visited_on,
        SUM(amount) OVER (
            ORDER BY visited_on ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS amount,
        ROUND(
            SUM(amount) OVER (
                ORDER BY visited_on ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) / 7,
            2
        ) AS average_amount,
        ROW_NUMBER() OVER (ORDER BY visited_on) AS rn
    FROM daily
)
SELECT visited_on, amount, average_amount
FROM rolling
WHERE rn >= 7
ORDER BY visited_on;
```

---

## 典型题 3：SQL139 近三个月未完成试卷数为 0 的用户

### 原题摘要

对每个用户取其最近 3 个有作答记录的月份，筛选这 3 个月内没有未完成试卷的用户，并统计完成数。

### 你容易出错的地方

- 使用 `MONTH(start_time)`，跨年时月份会混淆。
- 用“最大月份减 2”代替最近 3 个有记录月份，遇到断月就不等价。
- 提前执行 `WHERE submit_time IS NOT NULL`，未完成记录被删掉，无法判断用户是否全部完成。

### 标准答案

```sql
WITH user_month AS (
    SELECT DISTINCT
        uid,
        DATE_FORMAT(start_time, '%Y%m') AS ym
    FROM exam_record
),
ranked_month AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY uid ORDER BY ym DESC
        ) AS rn
    FROM user_month
),
recent_exam AS (
    SELECT r.*
    FROM exam_record r
    JOIN ranked_month m
      ON m.uid = r.uid
     AND m.ym = DATE_FORMAT(r.start_time, '%Y%m')
    WHERE m.rn <= 3
)
SELECT
    uid,
    COUNT(submit_time) AS exam_complete_cnt
FROM recent_exam
GROUP BY uid
HAVING COUNT(*) = COUNT(submit_time)
ORDER BY exam_complete_cnt DESC, uid DESC;
```

---

# 四、连续区间问题（高频）

## 通用模板

连续日期或连续编号最常用“错位相减法”：

```text
连续值 - 组内行号 = 相同分组标记
```

连续日期写成：

```sql
DATE_SUB(dt, INTERVAL ROW_NUMBER() OVER (...) DAY)
```

分组字段通常至少包括：

```text
对象字段 + 连续段标记
```

如果连续的是“相同值”，还要加上该值本身。

## 典型题 1：LeetCode 180 连续出现的数字

### 原题摘要

找出至少连续出现 3 次的数字。

### 标准答案

```sql
WITH tagged AS (
    SELECT
        id,
        num,
        id - ROW_NUMBER() OVER (
            PARTITION BY num ORDER BY id
        ) AS grp
    FROM Logs
)
SELECT DISTINCT num AS ConsecutiveNums
FROM tagged
GROUP BY num, grp
HAVING COUNT(*) >= 3;
```

### 为什么需要 `num, grp` 两个分组字段

`grp` 只能标识一段连续编号，不保证不同数字不会得到相同 `grp`。`num` 表示“连续的是什么”，`grp` 表示“这是它的哪一段连续区间”。

---

## 典型题 2：SQL184 连续 2 天及以上购物的用户

### 原题摘要

统计连续购物至少 2 天的用户及其最长连续购物天数。同一用户同一天购买多件商品只算一天。

### 标准答案

```sql
WITH user_day AS (
    SELECT DISTINCT user_id, sales_date
    FROM sales_tb
),
tagged AS (
    SELECT
        user_id,
        sales_date,
        DATE_SUB(
            sales_date,
            INTERVAL ROW_NUMBER() OVER (
                PARTITION BY user_id ORDER BY sales_date
            ) DAY
        ) AS grp
    FROM user_day
),
streak AS (
    SELECT
        user_id,
        grp,
        COUNT(*) AS days_count
    FROM tagged
    GROUP BY user_id, grp
    HAVING COUNT(*) >= 2
)
SELECT
    user_id,
    MAX(days_count) AS days_count
FROM streak
GROUP BY user_id
ORDER BY user_id;
```

---

## 典型题 3：SQL50 查询连续登录不少于 3 天的新注册用户

### 原题摘要

注册当天视为第一次登录，将注册日期和登录日期合并，找出连续登录不少于 3 天的新注册用户。

### 你容易出错的地方

- `UNION` 两边字段顺序不一致。
- 登录表中可能存在不属于注册表目标用户的数据，产生额外用户。
- 同一天多次登录没有先去重。

### 标准答案

```sql
WITH login_info AS (
    SELECT user_id, DATE(reg_time) AS dt
    FROM register_tb

    UNION

    SELECT l.user_id, DATE(l.log_time) AS dt
    FROM login_tb l
    JOIN register_tb r ON r.user_id = l.user_id
),
tagged AS (
    SELECT
        user_id,
        dt,
        DATE_SUB(
            dt,
            INTERVAL ROW_NUMBER() OVER (
                PARTITION BY user_id ORDER BY dt
            ) DAY
        ) AS grp
    FROM login_info
)
SELECT DISTINCT user_id
FROM tagged
GROUP BY user_id, grp
HAVING COUNT(*) >= 3
ORDER BY user_id;
```

---

# 五、排名、Top N 与百分位（高频）

## 函数选择

| 函数 | 并列处理 | 适用场景 |
|---|---|---|
| `ROW_NUMBER()` | 不保留并列，每行唯一编号 | 明确要求只取 N 条，或有第二排序规则 |
| `RANK()` | 并列后跳号，如 1、1、3 | 比赛名次 |
| `DENSE_RANK()` | 并列后不跳号，如 1、1、2 | 前 N 个等级 |
| `PERCENT_RANK()` | 返回 0 到 1 的相对排名 | 前 50%、后 20% 等比例筛选 |

## 典型题 1：SQL140 未完成率 Top 50% 用户近三个月答卷情况

### 原题摘要

先根据所有用户的 SQL 试卷未完成率做百分位排名，取未完成率较高的 50%，然后在其中筛选 6、7 级用户，统计其最近 3 个有答题记录月份的全部试卷情况。

### 你容易出错的地方

- 把“排名小于等于 50%”理解为整数名次。
- 用 `RANK()`，实际上题目示例使用的是 0 到 1 的百分位位置。
- 将等级条件提前到排名前，使排名总体发生变化。
- 最近三个月只统计 SQL 试卷，而题目要求用户全部试卷。

### 标准思路骨架

```sql
WITH sql_exam AS (
    SELECT r.*
    FROM exam_record r
    JOIN examination_info i USING (exam_id)
    WHERE i.tag = 'SQL'
),
user_rate AS (
    SELECT
        u.uid,
        u.level,
        SUM(s.submit_time IS NULL) / COUNT(*) AS incomplete_rate
    FROM user_info u
    JOIN sql_exam s USING (uid)
    GROUP BY u.uid, u.level
),
ranked_user AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            ORDER BY incomplete_rate DESC
        ) AS rate_position
    FROM user_rate
),
user_month AS (
    SELECT DISTINCT
        uid,
        DATE_FORMAT(start_time, '%Y%m') AS ym
    FROM exam_record
),
recent_month AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY uid ORDER BY ym DESC
        ) AS rn
    FROM user_month
)
SELECT
    e.uid,
    m.ym AS start_month,
    COUNT(*) AS total_cnt,
    COUNT(e.submit_time) AS complete_cnt
FROM ranked_user r
JOIN recent_month m ON m.uid = r.uid AND m.rn <= 3
JOIN exam_record e
  ON e.uid = m.uid
 AND DATE_FORMAT(e.start_time, '%Y%m') = m.ym
WHERE r.rate_position <= 0.5
  AND r.level IN (6, 7)
GROUP BY e.uid, m.ym
ORDER BY e.uid, m.ym;
```

---

## 典型题 2：SQL126 门店内销量 Top 3

这道题同时属于“多表连接”和“Top N”。排名前必须确保每个门店 SKU 只有一行：

```sql
ROW_NUMBER() OVER (
    PARTITION BY store_id
    ORDER BY last7d_qty DESC, sku_id ASC
) AS rank_in_store
```

题目要求相同销量时按 `sku_id` 升序，意味着排序结果唯一，应使用 `ROW_NUMBER()`。

---

## 典型题 3：LeetCode 1341 电影评分

### 原题摘要

输出评分电影数量最多的用户名；再输出 2020 年 2 月平均评分最高的电影名。并列时均按名称字典序最小值。

### 标准答案

```sql
(
    SELECT u.name AS results
    FROM MovieRating r
    JOIN Users u USING (user_id)
    GROUP BY r.user_id, u.name
    ORDER BY COUNT(*) DESC, u.name ASC
    LIMIT 1
)
UNION ALL
(
    SELECT m.title AS results
    FROM MovieRating r
    JOIN Movies m USING (movie_id)
    WHERE r.created_at >= '2020-02-01'
      AND r.created_at <  '2020-03-01'
    GROUP BY r.movie_id, m.title
    ORDER BY AVG(r.rating) DESC, m.title ASC
    LIMIT 1
);
```

---

# 六、条件聚合、比例与 NULL（高频）

## 高频陷阱

下面这条表达式几乎总是错的：

```sql
COUNT(IF(condition, 1, 0))
```

因为 `COUNT()` 只忽略 `NULL`，而 `0` 也会被计数。应该使用：

```sql
SUM(condition)
```

或者：

```sql
COUNT(IF(condition, 1, NULL))
```

## 典型题 1：LeetCode 1934 用户确认率

### 原题摘要

确认率为 `confirmed` 消息数除以确认消息总数，没有任何确认请求的用户确认率为 0。

### 标准答案

```sql
SELECT
    s.user_id,
    ROUND(COALESCE(AVG(c.action = 'confirmed'), 0), 2) AS confirmation_rate
FROM Signups s
LEFT JOIN Confirmations c USING (user_id)
GROUP BY s.user_id;
```

### 为什么 `AVG(条件)` 可行

在 MySQL 中，条件成立为 `1`，不成立为 `0`，所以 `AVG(条件)` 就是成立比例。

---

## 典型题 2：LeetCode 1211 查询结果的质量和占比

### 原题摘要

- 查询质量：`rating / position` 的平均值。
- 差查询占比：`rating < 3` 的记录占比。

### 标准答案

```sql
SELECT
    query_name,
    ROUND(AVG(rating * 1.0 / position), 2) AS quality,
    ROUND(AVG(rating < 3) * 100, 2) AS poor_query_percentage
FROM Queries
GROUP BY query_name;
```

### 你当时的问题

子查询先按 `query_name` 分组，却仍选择了明细列 `rating`；外层再根据这一条随机保留下来的 `rating` 统计，粒度已经错误。

---

## 典型题 3：SQL178 工作日各时段叫车量、等待和调度时间

### 原题摘要

按照开始打车时间划分早高峰、工作时间、晚高峰和休息时间，统计叫车量、平均等待接单时间和完成订单的平均调度时间，单位为分钟。

### 你容易出错的地方

- 使用 `TIMESTAMPDIFF(MINUTE, ...)`，不足一分钟直接得到 0。
- `20:00 <= time < 07:00` 永远不成立，应使用 `OR`。
- 将未完成订单按调度时间 0 参与平均，题目要求调度时间只计算已完成订单。

### 标准写法

```sql
SELECT
    CASE
        WHEN TIME(r.event_time) >= '07:00:00'
         AND TIME(r.event_time) <  '09:00:00' THEN '早高峰'
        WHEN TIME(r.event_time) >= '09:00:00'
         AND TIME(r.event_time) <  '17:00:00' THEN '工作时间'
        WHEN TIME(r.event_time) >= '17:00:00'
         AND TIME(r.event_time) <  '20:00:00' THEN '晚高峰'
        ELSE '休息时间'
    END AS period,
    COUNT(o.order_id) AS get_car_num,
    ROUND(AVG(TIMESTAMPDIFF(SECOND, r.event_time, o.order_time)) / 60, 1)
        AS avg_wait_time,
    ROUND(AVG(
        CASE
            WHEN o.start_time IS NOT NULL
             AND o.finish_time IS NOT NULL
            THEN TIMESTAMPDIFF(SECOND, o.order_time, o.start_time)
        END
    ) / 60, 1) AS avg_dispatch_time
FROM tb_get_car_record r
LEFT JOIN tb_get_car_order o ON o.order_id = r.order_id
WHERE DAYOFWEEK(r.event_time) BETWEEN 2 AND 6
GROUP BY period
ORDER BY get_car_num;
```

---

# 七、每组第一条、最后一条记录（高频）

## 核心区别

```sql
SELECT customer_id, MIN(order_date)
GROUP BY customer_id
```

只能可靠地得到最早日期，不能同时可靠地取出“该日期所在行的其他字段”。其他字段必须通过连接、相关子查询或窗口排名取回。

## 典型题 1：LeetCode 1174 即时食物配送 II

### 原题摘要

每个顾客只有一个首次订单，统计首次订单中即时订单的比例。即时订单是下单日期等于期望配送日期。

### 你容易出错的地方

按顾客分组后同时选择 `MIN(order_date)` 和普通列 `customer_pref_delivery_date`。这个期望日期不一定来自最早订单所在行。

### 不使用窗口函数的标准答案

```sql
WITH first_date AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM Delivery
    GROUP BY customer_id
)
SELECT
    ROUND(
        AVG(d.order_date = d.customer_pref_delivery_date) * 100,
        2
    ) AS immediate_percentage
FROM Delivery d
JOIN first_date f
  ON f.customer_id = d.customer_id
 AND f.first_order_date = d.order_date;
```

---

## 典型题 2：LeetCode 1164 指定日期的产品价格

### 原题摘要

产品初始价格为 10，输出 2019-08-16 当天每个产品的价格，即该日期之前最近一次变价后的价格。

### 你容易出错的地方

- 在 `WHERE` 中直接使用 `MAX(change_date)`。
- 先将日期和 `LAG()` 比较，逻辑复杂且一个产品会保留多行。
- 只从符合日期的变价记录出发，遗漏从未在该日前变价的产品。

### 标准答案

```sql
WITH ranked AS (
    SELECT
        product_id,
        new_price,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY change_date DESC
        ) AS rn
    FROM Products
    WHERE change_date <= '2019-08-16'
),
all_product AS (
    SELECT DISTINCT product_id
    FROM Products
)
SELECT
    p.product_id,
    COALESCE(r.new_price, 10) AS price
FROM all_product p
LEFT JOIN ranked r
  ON r.product_id = p.product_id
 AND r.rn = 1;
```

---

## 典型题 3：用户活跃基础表中的首次登录日期

### 原题摘要

结果既要保留每位用户的每个活跃日期，又要在每一行附带该用户的首次登录日期。

### 为什么使用窗口函数

`GROUP BY uid` 会把每个用户压缩成一行，无法继续保留多个活跃日期。窗口函数只计算、不减少行数：

```sql
SELECT DISTINCT
    uid,
    DATE(in_time) AS dt,
    MIN(DATE(in_time)) OVER (
        PARTITION BY uid
    ) AS first_dt
FROM tb_user_log;
```

判断规则：

- 结果只要“每个用户一行”：使用 `GROUP BY uid`。
- 结果还要保留“每个用户的多条明细”：使用窗口函数。

---

# 八、事件流与同时在线人数（中高频）

## 通用方法

区间 `[start_time, end_time]` 不能直接逐时刻展开。应转换成事件：

```text
开始事件：+1
结束事件：-1
```

按照时间排序后累计求和，就是每个事件发生后的实时人数。

## 典型题 1：SQL189 牛客直播各科目同时在线人数

### 原题摘要

将进入直播间记为 `+1`，离开直播间记为 `-1`，求每门课程最大同时在线人数。

### 标准答案

```sql
WITH events AS (
    SELECT course_id, in_datetime AS dt, 1 AS cnt
    FROM attend_tb

    UNION ALL

    SELECT course_id, out_datetime AS dt, -1 AS cnt
    FROM attend_tb
),
online AS (
    SELECT
        course_id,
        dt,
        SUM(cnt) OVER (
            PARTITION BY course_id
            ORDER BY dt, cnt DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS online_num
    FROM events
)
SELECT
    c.course_id,
    c.course_name,
    MAX(o.online_num) AS max_num
FROM course_tb c
LEFT JOIN online o ON o.course_id = c.course_id
GROUP BY c.course_id, c.course_name
ORDER BY c.course_id;
```

### `ROWS BETWEEN ...` 为什么能得到当前人数

它本身不理解“在线”。真正的含义来自事件值：进入 `+1`、离开 `-1`。窗口只是把当前行之前的所有变化量累计起来：

```text
当前在线人数 = 历史所有进入次数 - 历史所有离开次数
```

`ORDER BY dt, cnt DESC` 表示同一时刻先处理 `+1`，再处理 `-1`。

---

## 典型题 2：SQL179 各城市最大同时等车人数

### 原题摘要

用户开始打车时进入等待状态；司机接到乘客、用户取消或者等待超时时退出等待状态。同一时刻先增加后减少。

### 结束等待的三种情况

```text
没有司机接单：r.order_id IS NULL，结束于 r.end_time
司机接单并上车：o.start_time IS NOT NULL，结束于 o.start_time
接单后上车前取消：o.start_time IS NULL，结束于 o.finish_time
```

### 解题骨架

```sql
WITH events AS (
    SELECT city, event_time AS dt, 1 AS cnt
    FROM tb_get_car_record

    UNION ALL

    SELECT
        r.city,
        CASE
            WHEN r.order_id IS NULL THEN r.end_time
            WHEN o.start_time IS NOT NULL THEN o.start_time
            ELSE o.finish_time
        END AS dt,
        -1 AS cnt
    FROM tb_get_car_record r
    LEFT JOIN tb_get_car_order o ON o.order_id = r.order_id
),
running AS (
    SELECT
        city,
        SUM(cnt) OVER (
            PARTITION BY city
            ORDER BY dt, cnt DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS wait_uv
    FROM events
)
SELECT city, MAX(wait_uv) AS max_wait_uv
FROM running
GROUP BY city
ORDER BY max_wait_uv, city;
```

---

# 九、反连接：查找“不存在”的记录（中频补充）

## 典型题 1：LeetCode 1581 进店却未进行交易的顾客

```sql
SELECT
    v.customer_id,
    COUNT(*) AS count_no_trans
FROM Visits v
LEFT JOIN Transactions t USING (visit_id)
WHERE t.transaction_id IS NULL
GROUP BY v.customer_id;
```

你当时缺少 `GROUP BY customer_id`。`DISTINCT customer_id` 不能代替分组，因为你还需要分别统计每位顾客的次数。

---

## 典型题 2：LeetCode 1084 销售分析 III

### 原题摘要

找出只在 2019 年第一季度销售过的产品。产品必须在第一季度有销售，而且第一季度之外没有销售。

### 标准答案

```sql
SELECT
    p.product_id,
    p.product_name
FROM Product p
JOIN Sales s USING (product_id)
GROUP BY p.product_id, p.product_name
HAVING MIN(s.sale_date) >= '2019-01-01'
   AND MAX(s.sale_date) <= '2019-03-31';
```

你当时只排除了“在区间外销售过的商品”，但没有保证商品在区间内确实有销售。使用内连接和 `MIN/MAX` 可以同时表达两个条件。

---

# 十、考场自检清单

提交 SQL 前，按照下面顺序检查。

## 1. 粒度

- 最终一行代表什么？
- 当前每个 CTE 的一行又代表什么？
- 排名之前是否已经做到“一件排名对象一行”？

## 2. 连接

- 连接键是否完整？例如是否同时需要 `user_id + course_id`？
- 左右两边连接键是否唯一？
- 是否可能出现多对多膨胀？
- 需要保留零记录对象时，是否使用了 `LEFT JOIN`？

## 3. 聚合

- `SELECT` 中的普通字段是否都在 `GROUP BY` 中？
- `COUNT(*)`、`COUNT(column)` 和 `COUNT(DISTINCT column)` 是否选对？
- 是否误写了 `COUNT(IF(condition, 1, 0))`？

## 4. 时间

- 日期边界是否包含首尾？
- 跨年时是否错误地只使用 `MONTH()`？
- 滚动窗口需要的数据是否在窗口计算前被提前过滤？
- 最近 7 行是否真的等于最近 7 天？

## 5. 窗口函数

- `PARTITION BY` 是“为谁分别计算”？
- `ORDER BY` 是“按照什么顺序计算”？
- 当前窗口是在明细行上，还是在已经聚合的数据上？
- `LAG(column)` 与 `LAG(column, 1)` 等价，默认偏移量就是 1。

## 6. 比例与 NULL

- 分子和分母的业务口径是否一致？
- 分母是否可能为 0？
- 未匹配数据应该删除、保留为 `NULL`，还是补成 0？
- 是否使用浮点除法，避免整数截断？

## 7. 排名

- 并列时到底要不要占用多个名额？
- 是否有第二排序字段用于打破并列？
- Top N 是按全表还是按组？
- “前 50%”是否应该使用 `PERCENT_RANK()`？

---

# 十一、你的高频错误画像

根据当前对话，最值得优先改进的不是某个函数的记忆，而是下面三个习惯：

1. **先明确粒度再写 SQL。** 你的很多错误都来自明细、用户、月份、SKU 等粒度混在同一层。
2. **连接前先聚合。** 当两张表都是一对多记录时，直接连接很容易把统计值放大。
3. **把复杂题拆成可验证的 CTE。** 每个 CTE 只完成一件事，并检查它是否已经达到预期的一行粒度。

建议复习时不要只背最终 SQL。对每道题都先在纸上写出：

```text
原始表粒度
→ 第一个中间表粒度
→ 第二个中间表粒度
→ 最终结果粒度
```

只要这条链路是清楚的，大多数聚合、连接和窗口函数题都会明显稳定下来。
