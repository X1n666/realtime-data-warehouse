# 电商实时数仓项目（学生版，物理分层）

> 一句话定位：用 **Flink + Kafka + Flink CDC** 在单机 WSL2 环境里搭建一个"物理四层"的电商实时数仓，覆盖**行为日志**与**交易数据**两大域，产出 PV/UV/GMV/支付/退款等指标，支持批流对账与可视化。
>
> 设计总纲：《实时数仓项目设计方案V2.md》（项目根目录），本项目 README 是其运行态快照。学习资料（Flink 复习笔记、交接提示词、SQL 错题手册、面试手册）见 [docs/](docs/)。

---

## 1. 项目是干什么的

模拟一个电商平台的实时数据链路：用户在前端产生**行为日志**（浏览/点击/下单/支付），同时订单系统产生**交易流水**（下单→支付→退款三阶段生命周期）。两条数据流经实时数仓分层处理，最终在 MySQL 结果库和 Grafana 看板上呈现分钟级/天级指标。

**为什么做这个项目**（秋招导向）：

- 覆盖大数据开发岗位的核心面试点：分层建模、事件时间与 watermark、窗口、状态与去重、checkpoint、CDC/changelog、Upsert 幂等、批流对账、故障恢复。
- 简历亮点：物理四层实时数仓、Flink CDC 交易链路、三阶段交易生命周期可复现对账、严格指标口径。

---

## 2. 总体架构

```
行为日志生成器 V2（确定性回放）
  → Kafka ods_traffic_log ──┐
交易生命周期生成器 V2（分阶段写）   │
  → MySQL gmall_rt（9 表）→ binlog │
                           │
        ┌──────────────────┴───────────────────┐
        │ Job1（并行度 1）                       │
        │  ODS Sources → DWD Kafka              │
        │  · Kafka source: ods_traffic_log      │
        │  · CDC source: 支付/退款/订单明细      │
        │  · 解析/清洗/event_id 去重            │
        │  → dwd_traffic_event                  │
        │  → dwd_payment_detail 等              │
        └───────────────────────────────────────┘
                          │
        ┌─────────────────┴─────────────────────┐
        │ Job2（并行度 1）                       │
        │  DWD Kafka → DWS Upsert Kafka         │
        │  同时输出 ADS MySQL（同作业分支）       │
        │  · 窗口聚合（watermark + 迟到容忍）     │
        │  · 日 UV / 日支付用户独立去重          │
        │  → dws_traffic_1m / dws_trade_1m      │
        │  → dws_traffic_day / dws_trade_day    │
        │  → MySQL gmall_report_rt（幂等 upsert）│
        └───────────────────────────────────────┘
                          │
                     Grafana 看板
```

**分层语义**：ODS（原样接入，不落地）→ DWD（明细清洗、去重、规范化）→ DWS（按指标维度聚合的中间结果）→ ADS（面向展示的结果表）。

**MVP 阶段为两作业**（逻辑四层、物理 3 段）；**第 3 周拆成三作业**（Job1 接入、Job2 DWS、Job3 无状态 sink 写 MySQL）后才可称"物理四层"——这是简历红线，拆作业前禁止这样写简历。

---

## 3. 技术栈与选型理由

| 组件           | 版本                                   | 为什么选它                                                                                                                                                        |
| -------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Flink          | 1.19.1                                 | 真流引擎 + Flink SQL 降低实现成本；checkpoint/exactly-once 语义最完整；生态统一（Kafka/JDBC/CDC 都有官方 connector）                                              |
| Flink CDC      | 3.1.1（flink-sql-connector-mysql-cdc） | 内置 binlog 订阅，**快照 + 增量一体化**（initial 模式先读全量再续增量，自动记录 binlog 位点），避免 Canal/MaxWell 需要额外部署一套服务 + 自维护位点的复杂度 |
| MySQL          | 8.0                                    | 业务源库（binlog ROW + FULL 行镜像，CDC 前置条件）；同时作为 ADS 结果库                                                                                           |
| Kafka          | 3.7.1（KRaft 单节点）                  | ODS/DWD/DWS 之间解耦的消息通道；KRaft 省掉 Zookeeper，单节点也能跑；官方镜像 3.7 起发布                                                                           |
| Upsert Kafka   | —                                     | 承接 changelog 流（+I/-D/-U/+U）：DWD 去重、DWS 聚合结果都含 update/delete 语义，append-only sink 会直接报错（Day4 实测踩坑）                                     |
| JDBC sink      | flink-connector-jdbc 3.2.0-1.19        | 幂等写 MySQL：`INSERT ... ON DUPLICATE KEY UPDATE` 覆盖式 upsert，可重放不累加                                                                                  |
| Docker Compose | WSL2 单机                              | 一键起全环境；限制每容器内存模拟资源受限场景；挂载 flink-lib 集成 connector jar                                                                                   |
| Grafana        | 11.1.0                                 | 指标可视化看板                                                                                                                                                    |

**明确不引入**（防止范围膨胀）：Doris / ClickHouse / Paimon（OLAP 或湖格式，资源与学习阶段不必要）、Canal/MaxWell（额外服务）、HBase/Redis（维表缓存暂用 Flink 状态）。

---

## 4. 核心设计

### 4.1 行为日志数据契约（V2）

```json
{"common": {"uid": 100515, "platform": "app", "province_id": 7, "ts": "2026-09-01 08:00:05"},
 "page": {"page_id": "home"},
 "action": {"action_id": "browse", "item": null},
 "ts": "2026-09-01 08:00:05",
 "event_id": "a3f2...",
 "event_time_ms": 1788220805000,
 "biz_date": "2026-09-01",
 "source_timezone": "Asia/Shanghai"}
```

- `event_id = md5(biz_date|seq|uid|ts|action_id|page_id|item)`：**生成器确定性**产出，禁止 Flink 运行时自增（多并行不唯一）；`seq` 为文件累积行号，追加模式用 `--offset` 续号。
- `event_time_ms`：UTC epoch 毫秒；字面回填按 Asia/Shanghai 解释，实时模式标 UTC。
- 日志用户池（uid 100001-101000，每日活跃子集）与业务用户池（user_info 表）**刻意分离**，日志域只做流量指标，不做跨域 join。

### 4.2 交易生命周期三阶段（对账基石）

单次生成器运行产出**三段独立事务**（同 seed 可完全复现）：

```
段1: 全部订单 INSERT（order_status=UNPAID）+ 订单明细
段2: 已支付订单 UPDATE→PAID + INSERT payment_info（SUCCESS）
段3: 退款订单 UPDATE→REFUNDED + INSERT order_refund_info（SUCCESS）
```

binlog 实测事件形态（ROW 格式下每行一个事件）：

| 事务 | 事件                                                             |
| ---- | ---------------------------------------------------------------- |
| 段1  | 3 表 Write_rows（user 100 / order 200 / detail 398）             |
| 段2  | 170 × Update_rows(order_info) + Write_rows(payment_info 182)    |
| 段3  | 12 × Update_rows(order_info) + Write_rows(order_refund_info 12) |

### 4.3 指标口径锚点（固定 6 项，禁止漂移）

| # | 指标         | 口径                                             | 对账方式                    |
| - | ------------ | ------------------------------------------------ | --------------------------- |
| 1 | PV           | browse 事件数                                    | 批 SQL：COUNT(*) browse     |
| 2 | 分钟 UV      | 窗口内 browse 用户去重（中间指标）               | 不参与日对账                |
| 3 | 日 UV        | 自然日内 browse 用户**独立重新去重**       | 批 SQL：COUNT(DISTINCT uid) |
| 4 | GMV          | SUCCESS 支付金额，按支付时间，**不减退款** | 批 SQL 第 4 节              |
| 5 | 支付订单数   | DISTINCT order_id（支付流水）                    | 同上                        |
| 6 | 日支付用户数 | 自然日内 DISTINCT user_id（支付流水）            | 同上                        |

**红线**：禁止 `SUM(分钟UV)` 当日 UV、`SUM(分钟支付用户数)` 当日支付用户数、browse+click 混合当 PV。分钟指标与日指标天然不可加（同一用户可跨多个分钟窗口活跃），差异要在对账中写清楚，而不是消除。

### 4.4 数据流与幂等性

- 生成器确定性 → 同 seed 重放产出相同事件 → 对账可复现。
- 全链路 upsert 幂等：DWD/DWS 用 upsert-kafka（主键覆盖），ADS 用 MySQL `ON DUPLICATE KEY UPDATE`。作业重放/重启不会产生重复指标（retract 语义下聚合正确回退）。

---

## 5. 环境与数据清单

### 5.1 Docker Compose（内存预算 ≈ 5.75G）

| 容器              | 镜像                   | 资源                  | 端口          |
| ----------------- | ---------------------- | --------------------- | ------------- |
| gmall_mysql       | mysql:8.0              | 1G                    | 3306          |
| gmall_kafka       | apache/kafka:3.7.1     | 1.5G                  | 29092（主机） |
| gmall_jobmanager  | flink:1.19.1           | 768M                  | 8081          |
| gmall_taskmanager | flink:1.19.1           | 2G，**4 slots** | —            |
| gmall_grafana     | grafana/grafana:11.1.0 | 512M                  | 3000          |

flink-lib/ 挂载 17 个 jar（flink-dist + mysql-cdc / kafka / jdbc / mysql-connector-j）。

### 5.2 Kafka Topic

| Topic              | 语义         | 格式             | 主键                                               |
| ------------------ | ------------ | ---------------- | -------------------------------------------------- |
| ods_traffic_log    | ODS 行为日志 | JSON             | —                                                 |
| dwd_traffic_event  | DWD 流量明细 | JSON（清洗后）   | event_id                                           |
| dwd_payment_detail | DWD 支付明细 | changelog        | 待设计（见第 7 节任务）                            |
| dwd_refund_detail  | DWD 退款明细 | changelog        | 同上                                               |
| dwd_order_detail   | DWD 订单明细 | changelog        | 同上                                               |
| dws_traffic_1m     | DWS 流量分钟 | **Upsert** | metric_date+window_start+metric_name+dimension_key |
| dws_trade_1m       | DWS 交易分钟 | **Upsert** | 同上                                               |

（dws_*_day 两个日级 topic 第 2 周建）

### 5.3 MySQL

- **gmall_rt**（CDC 源库，9 表）：user_info / base_province / base_category / spu_info / sku_info / order_info / order_detail / payment_info / order_refund_info
- **gmall_report_rt**（结果库，4 表）：ads_traffic_1m / ads_trade_1m / ads_traffic_day / ads_trade_day，通用主键 `(metric_date, window_start, metric_name, dimension_key)` + `metric_value` + `updated_at`

### 5.4 测试日数据（2026-09-01）

- 行为日志：2000 条（--seed 20260901 --count 2000 --active-users 500 --ts "2026-09-01 08:00:00"）
- 业务数据：200 单 / 398 明细 / 182 支付 / 12 退款 / 100 用户（--seed 20260901 --orders 200 --users 100），manifest 记录于 data-generator/data/generated/manifest_20260901.txt

---

## 6. 已完成的进度与验证结果

| 阶段  | 内容                                                                                           | 验证结果                                                                                              |
| ----- | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Day 1 | 环境：MySQL binlog(ROW/FULL/30天)、Kafka 产消、Flink 1 TM、A1-A5 从离线仓库复制改造            | MySQL healthy + binlog 参数实测；Kafka 产消通过                                                       |
| Day 2 | 生成器 V2（event_id/--offset/event_time_ms/biz_date/source_timezone）+ ODS 2000 条             | Flink SQL 消费 COUNT=2000 且 DISTINCT event_id=2000 一致                                              |
| Day 3 | gmall_rt 9 表 + 静态维度；业务生成器三阶段交易改造；manifest；binlog 三段验证；CDC source 测试 | 行数验收 200/398/182/12；binlog TXN7/8/9 三段独立事务；CDC 全量快照 payment 182 行                    |
| Day 4 | Job1（ods→dwd 去重清洗）+ Job2（1m 窗口 browse PV → dws + ads）                              | 3 作业 RUNNING；ads_traffic_1m 33 个完整窗口；**PV 对账 641 = 636(已触发) + 5(尾部未触发窗口)** |

**过程中修掉的关键问题**（面试可讲）：

1. 生成器字段索引错位导致 `tracking_no=NULL`（灌库前 SQL 审查发现）
2. 段1 误含 payment/refund INSERT 破坏三段事务语义（binlog 验证发现，TRUNCATE 重灌）
3. jobmanager OOM：`docker exec` 进 JM 容器跑 sql-client，客户端 JVM 与 JM 共享 768m cgroup → 改独立容器 + `-D` 集群地址提交
4. MySQL 时区校验失败（+8 vs Etc/UTC）→ CDC 显式 `server-time-zone=Asia/Shanghai`
5. ROW_NUMBER 去重输出 changelog，append sink 编译报错 → dwd 改 upsert-kafka（主键=event_id）
6. TM slots 2→4：Job1 + Job2 双分支 = 3 作业超过 2 slots，NoResourceAvailable 无限重启

---

## 7. 下一步（第 2 周）

1. **交易域 DWD 接入**（Job1 加 CDC source 分支，产出 dwd_payment_detail / dwd_refund_detail / dwd_order_detail）——进行中，schema 主键设计待定
2. 交易 DWS 聚合：GMV / 支付订单数 / 支付用户数 / 退款金额
3. 分钟 UV / 日 UV（独立去重，论证可加性）
4. 批流对账（测试日 2026-09-01 + manifest）
5. 拆三作业（Job3 无状态 sink）→ 可称物理四层
6. Grafana 看板

---

## 8. 目录结构

```
ecommerce-realtime-data-warehouse/
├── docker-compose.yml          # 全环境编排（MySQL/Kafka/Flink/Grafana）
├── flink-lib/                  # Flink connector jars（挂载到 /opt/flink/lib）
├── flink-sql/                  # Job1/Job2/测试 SQL
├── scripts/submit_sql.sh       # 作业提交脚本（独立容器 + remote target）
├── data-generator/             # 行为日志生成器 V2 / 业务生成器 V2 + manifest
├── mysql/                      # business DDL、init、ads 结果库 DDL
├── docs/                       # 学习文档（Flink 复习笔记、交接提示词、SQL 错题手册、面试手册）
├── 实时数仓项目设计方案V2.md   # 设计总纲
└── README.md
```
