# 电商实时数仓项目（学生版，物理分层）

> 一句话定位：用 **Flink + Kafka + Flink CDC** 在单机 WSL2 环境里搭建"物理四层"的电商实时数仓，覆盖**行为日志**与**交易数据**两大域，产出 PV/UV/GMV/支付/退款等指标，支持批流对账与 Grafana 可视化。
>
> 设计总纲：《实时数仓项目设计方案V2.md》（项目根目录），本项目 README 是其运行态快照。学习资料（Flink 复习笔记、SQL 错题手册、面试手册）见 [docs/](docs/)。**每一步的做了什么/为什么/怎么验证/出错如何重跑**，见 [docs/开发日志.md](docs/开发日志.md)（节点式日志）。

---

## 1. 项目是干什么的

模拟一个电商平台的实时数据链路：用户在前端产生**行为日志**（浏览/点击/下单/支付），同时订单系统产生**交易流水**（下单→支付→退款三阶段生命周期）。两条数据流经实时数仓分层处理，最终在 MySQL 结果库和 Grafana 看板上呈现分钟级/天级指标。

**为什么做这个项目**（秋招导向）：

- 覆盖大数据开发岗位的核心面试点：分层建模、事件时间与 watermark、窗口、状态与去重、checkpoint、CDC/changelog、Upsert 幂等、批流对账、故障恢复。
- 简历亮点（当前状态即可这样写）：**物理四层实时数仓**（Job1 接入 / Job2 DWS / Job3 无状态 sink 已拆）、Flink CDC 交易链路、三阶段交易生命周期可复现对账、**6 指标批流全对**。

---

## 2. 总体架构

```
行为日志生成器 V2（确定性回放）
  → Kafka ods_traffic_log ──┐
交易生命周期生成器 V2（分阶段写）   │
  → MySQL gmall_rt（9 表）→ binlog │
                           │
        ┌──────────────────┴───────────────────┐
        │ Job1 ×3 作业（并行度 1）ODS → DWD    │
        │  · 流量：Kafka → 解析清洗/去重       │
        │  · 支付：CDC(payment_info) → 透传    │
        │  · 退款：CDC(order_refund_info) → 透传│
        │  → dwd_traffic_event                │
        │  → dwd_payment_detail（主键=id）     │
        │  → dwd_refund_detail（主键=id）      │
        └──────────────────────────────────────┘
                          │
        ┌─────────────────┴─────────────────────┐
        │ Job2 ×3 作业（并行度 1）DWD → DWS     │
        │  · 分钟窗口：browse PV（watermark）   │
        │  · 日 PV/UV：明细独立重算（不 SUM 分钟）│
        │  · 交易 4 指标：非窗口按日持续累计    │
        │  → dws_traffic_1m / dws_traffic_day  │
        │  → dws_trade_day（upsert-kafka）      │
        └───────────────────────────────────────┘
                          │
        ┌─────────────────┴─────────────────────┐
        │ Job3 ×3 作业（并行度 1，无状态 sink） │
        │  DWS 当前值 → MySQL 同主键覆盖写      │
        │  → ads_traffic_1m / ads_traffic_day   │
        │  → ads_trade_day（gmall_report_rt）   │
        └───────────────────────────────────────┘
                          │
                     Grafana 看板（provisioning）
```

**分层语义**：ODS（原样接入）→ DWD（明细清洗、去重、规范化）→ DWS（按指标聚合的中间结果）→ ADS（面向展示的结果表）。

**物理四层已于 2026-09-03 达成**（开发日志 节点 6）：9 作业三层拓扑（Job1×3 / Job2×3 / Job3×3）全部 RUNNING。拆作业的关键收益：**指标只在 Job2 算一次**（拆前 ads 分支从 DWD 明细重复计算 = 每指标双份窗口/去重状态），Job3 读 DWS 聚合后的"当前绝对值" + 同主键覆盖写 MySQL → **无状态作业**（无窗口/去重/累计算子，重启秒级、重放天然幂等）。

---

## 3. 技术栈与选型理由

| 组件           | 版本                                   | 为什么选它                                                                                                                                                        |
| -------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Flink          | 1.19.1                                 | 真流引擎 + Flink SQL 降低实现成本；checkpoint/exactly-once 语义完整；生态统一（Kafka/JDBC/CDC 官方 connector）                                                    |
| Flink CDC      | 3.1.1（flink-sql-connector-mysql-cdc） | 内置 binlog 订阅，**快照 + 增量一体化**（initial 模式先读全量再续增量，自动记录 binlog 位点），避免 Canal/MaxWell 需要额外部署一套服务 + 自维护位点的复杂度 |
| MySQL          | 8.0                                    | 业务源库（binlog ROW + FULL 行镜像，CDC 前置条件）；同时作为 ADS 结果库                                                                                           |
| Kafka          | 3.7.1（KRaft 单节点）                  | ODS/DWD/DWS 之间解耦的消息通道；KRaft 省掉 Zookeeper；官方镜像 3.7 起发布                                                                                         |
| Upsert Kafka   | —                                      | 承接 changelog 流（+I/-D/-U/+U）：DWD 去重、DWS 聚合结果都含 update/delete 语义，append-only sink 会直接报错（Day4 实测踩坑）                                     |
| JDBC sink      | flink-connector-jdbc 3.2.0-1.19        | 幂等写 MySQL：`INSERT ... ON DUPLICATE KEY UPDATE` 覆盖式 upsert，可重放不累加                                                                                  |
| Docker Compose | WSL2 单机                              | 一键起全环境；`name:` 字段固定项目名（目录为中文名时防网络名漂移）；named volume 持久化 mysql/kafka 数据                                               |
| Grafana        | 11.1.0                                 | 指标可视化；provisioning 配置化管理（数据源/看板随 compose 注册，可复现）                                                                                          |

**明确不引入**（防止范围膨胀）：Doris / ClickHouse / Paimon（OLAP 或湖格式，单机资源与学习阶段不必要）、Canal/MaxWell（额外服务）、HBase/Redis（维表缓存暂用 Flink 状态）。

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

**退款时间分布**：退款 create_time 滞后支付 1-2 天（9/1 支付，退款落在 9/1-9/3）——生成器模拟真实业务，退款按"退款时间"归日聚合（开发日志 节点 3）。

### 4.3 指标口径锚点（固定 6 项，禁止漂移）

| # | 指标         | 口径                                             | 对账方式                    |
| - | ------------ | ------------------------------------------------ | --------------------------- |
| 1 | PV           | browse 事件数                                    | 批 SQL：COUNT(*) browse     |
| 2 | 分钟 UV      | 窗口内 browse 用户去重（中间指标）               | 不参与日对账                |
| 3 | 日 UV        | 自然日内 browse 用户**独立重新去重**       | 批 SQL：COUNT(DISTINCT uid) |
| 4 | GMV          | SUCCESS 支付金额，按支付时间，**不减退款** | 批 SQL：COUNT/SUM           |
| 5 | 支付订单数   | DISTINCT order_id（支付流水）                    | 同上                        |
| 6 | 日支付用户数 | 自然日内 DISTINCT user_id（支付流水）            | 同上                        |

**红线**：禁止 `SUM(分钟UV)` 当日 UV、`SUM(分钟支付用户数)` 当日支付用户数、browse+click 混合当 PV。分钟指标与日指标天然不可加（同一用户可跨多个分钟窗口活跃），差异要在对账中写清楚，而不是消除。

### 4.4 幂等性与可重放

- 生成器确定性 → 同 seed 重放产出相同事件 → 批基准可复现（交易：MySQL 批 SQL + manifest；流量：生成器重放统计）。
- 全链路 upsert 幂等：DWD/DWS 用 upsert-kafka（主键覆盖），ADS 用 MySQL `ON DUPLICATE KEY UPDATE`。作业 cancel 后重提交 = 无状态全量重算 + 覆盖写 = 不产生重复指标。

---

## 5. 环境与数据清单

### 5.1 Docker Compose（容器内存上限合计 ≈ 6.3G，WSL2 限 memory=8GB）

| 容器              | 镜像                   | 资源                     | 端口          |
| ----------------- | ---------------------- | ------------------------ | ------------- |
| gmall_mysql       | mysql:8.0              | 1G                       | 3306          |
| gmall_kafka       | apache/kafka:3.7.1     | 1.5G                     | 29092（主机） |
| gmall_jobmanager  | flink:1.19.1           | 768M                     | 8081          |
| gmall_taskmanager | flink:1.19.1           | 2.5G，**12 slots** | —            |
| gmall_grafana     | grafana/grafana:11.1.0 | 512M                     | 3000          |

- 全部容器 `restart: unless-stopped`；mysql/kafka 数据挂 **named volume**（Docker 意外退出不丢数据，2026-08-24 两次丢失教训）。
- `name: ecommerce-realtime-data-warehouse` 固定 compose 项目名（目录为中文名，防默认项目名漂移导致网络名变化）。
- flink-lib/ 挂载 connector jars（mysql-cdc / kafka / jdbc / mysql-connector-j 等）。

### 5.2 Kafka Topic（实际已建）

| Topic              | 语义         | 写入作业 | 格式     | 主键                                               |
| ------------------ | ------------ | -------- | -------- | -------------------------------------------------- |
| ods_traffic_log    | ODS 行为日志 | 生成器   | JSON     | —                                                  |
| dwd_traffic_event  | DWD 流量明细 | Job1     | changelog | event_id                                           |
| dwd_payment_detail | DWD 支付明细 | Job1     | changelog | id（支付流水，非 order_id——同订单可多流水）        |
| dwd_refund_detail  | DWD 退款明细 | Job1     | changelog | id（退款流水）                                     |
| dws_traffic_1m     | DWS 流量分钟 | Job2     | Upsert   | metric_date+window_start+metric_name+dimension_key |
| dws_traffic_day    | DWS 流量日   | Job2     | Upsert   | metric_date+metric_name+dimension_key              |
| dws_trade_day      | DWS 交易日   | Job2     | Upsert   | 同上（无 window_start）                            |
| dws_trade_1m       | 预留（交易分钟无作业写，扩展用）| —        | Upsert   | —                                                  |

（设计初稿中的 dwd_order_detail 未实现：订单明细 398 行留在 MySQL 源库，DWD 只做支付/退款两交易链路。）

### 5.3 MySQL

- **gmall_rt**（CDC 源库，9 表）：user_info / base_province / base_category / spu_info / sku_info / order_info / order_detail / payment_info / order_refund_info。binlog ROW + FULL + expire 30 天。
- **gmall_report_rt**（结果库，DDL 4 表，实际写入 3 表）：ads_traffic_1m（33 窗口）/ ads_traffic_day / ads_trade_day 由 Job3 覆盖写；ads_trade_1m 预留无作业。

### 5.4 测试日数据（2026-09-01）

- 行为日志：2000 条（--seed 20260901 --count 2000 --active-users 500 --ts "2026-09-01 08:00:00"）
- 业务数据：200 单 / 398 明细 / 182 支付 / 12 退款 / 100 用户（--seed 20260901 --orders 200 --users 100），manifest 记录于 data-generator/data/generated/manifest_20260901.txt

---

## 6. 进度与验证结果

**节点式日志见 [docs/开发日志.md](docs/开发日志.md)**（每节点含：做了什么 / 为什么 / 验证 / 出错重跑方法，出错可从该节点重跑）。

| 阶段  | 内容                                                                                                     | 验证结果                                                                                    |
| ----- | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Day 1 | 环境：MySQL binlog(ROW/FULL/30天)、Kafka 产消、Flink、A1-A5 从离线仓库复制改造                          | MySQL healthy + binlog 参数实测；Kafka 产消通过                                             |
| Day 2 | 生成器 V2（event_id/--offset/event_time_ms/biz_date/source_timezone）+ ODS 2000 条                       | Flink SQL 消费 COUNT=2000 且 DISTINCT event_id=2000 一致                                    |
| Day 3 | gmall_rt 9 表 + 业务生成器三阶段交易；manifest；binlog 三段验证；CDC source 测试                          | 行数验收 200/398/182/12；binlog TXN 三段独立事务；CDC 快照 payment 182 行                   |
| Day 4 | Job1（流量 ods→dwd 去重）+ Job2（1m 窗口 browse PV → dws+ads）                                          | 3 作业 RUNNING；ads_traffic_1m 33 窗口；PV 对账 641 = 636(已触发) + 5(尾部未触发窗口)      |
| 节点1 | **支付 CDC 接入**：Job1 加 mysql-cdc 分支 → dwd_payment_detail（9 列，PK id，透传无去重）                | topic 182 条 = payment_info 182 行 ✓                                                        |
| 节点2 | **交易 DWS 日聚合**：GMV / 支付订单数 / 支付用户数 → dws_trade_day + ads_trade_day（非窗口按日持续累计） | 3 指标流=批 ✓（gmv 1,136,485.35 / 订单 182 / 用户 83）                                      |
| 节点3 | **退款 CDC**：Job1 加 refund 分支（server-id 5410-5414）→ dwd_refund_detail；refund_amount 指标           | topic 12 条 ✓；退款按日流批一致（9/1: 3,680.40 + 9/2: 27,535.37 + 9/3: 17,848.42）        |
| 节点4 | **日 PV/UV 独立重算**：dws_traffic_day / ads_traffic_day（红线：不 SUM 分钟）；TM slots 8→12             | browse_pv 641/641、day_uv 365/365 ✓；分钟层仍 636                                           |
| 节点5 | **批流对账 6 指标总表**                                                                                   | **流 = 批 全 ✓**（含差异说明：分钟 636 vs 日 641 为尾部未触发窗口，预期差异非 bug）        |
| 节点6 | **拆三作业**：Job2 精简为纯 DWS，Job3 无状态 sink（DWS→MySQL）                                            | 9 作业三层拓扑 RUNNING；MySQL 与拆前逐值一致（物理四层成立）                                |
| 节点7 | **Grafana 看板**：provisioning 注册数据源 + 8 面板                                                       | 数据源/看板注册 ✓；代理查询取数 ✓（浏览器 localhost:3000 admin/admin）                     |

**过程中修掉的关键问题**（面试可讲，均记录在开发日志）：

1. 生成器字段索引错位导致 `tracking_no=NULL`（灌库前 SQL 审查发现）
2. 段1 误含 payment/refund INSERT 破坏三段事务语义（binlog 验证发现，TRUNCATE 重灌）
3. jobmanager OOM：`docker exec` 进 JM 容器跑 sql-client 共享 768m cgroup → 改独立容器 + `-D` 集群地址提交
4. MySQL 时区校验失败（+8 vs Etc/UTC）→ CDC 显式 `server-time-zone=Asia/Shanghai`
5. ROW_NUMBER 去重输出 changelog，append sink 编译报错 → DWD 改 upsert-kafka
6. **共享 consumer group bug**：同一 SQL 文件双 INSERT = 两独立作业但共享 source group.id → partition 被一个作业独吞、另一个空转（dws 空 / ads 有）→ 每分支独立 group.id；教训：验证必须覆盖每条输出路径
7. **UNION ALL 列名继承第一段**：首段无 `AS` 别名时列名退化为 EXPR$N → 外层引用报 not found；修复=首段显式别名
8. 多分支 SQL 文件改动单个分支时不能整文件重跑（重复提交 → 同 group 瓜分）→ 抽单段单独提交

---

## 7. 后续方向（当前里程碑已完成，均为可选项）

1. ~~第 2 周计划~~ 已全部完成（交易 DWD、交易 DWS、日 PV/UV、批流对账、拆三作业、Grafana）
2. 分钟 UV（中间指标，不参与日对账）、dws_trade_1m 分钟交易指标
3. dwd_order_detail 订单明细 CDC（扩大交易域覆盖）
4. 环境一键启动脚本（compose up + 灌数 + 提交作业）
5. 学习向：README 锚点 ↔ 面试手册/SQL 错题册对照复习

---

## 8. 目录结构

```
实时数据分析平台/            # 项目根目录（中文名；compose 已用 name: 字段固定项目名）
├── docker-compose.yml       # 全环境编排（MySQL/Kafka/Flink/Grafana + named volume + restart）
├── flink-lib/               # Flink connector jars（挂载到 /opt/flink/lib）
├── flink-sql/               # job1（ODS→DWD 三分支）/ job2（DWD→DWS）×2 / job3（DWS→ADS 无状态 sink）
├── scripts/submit_sql.sh    # 作业提交脚本（独立容器 + remote target）
├── data-generator/          # 行为日志生成器 V2 / 业务生成器 V2 + manifest
├── mysql/                   # business DDL、init、ads 结果库 DDL
├── grafana/                 # provisioning（数据源/看板声明式注册）
├── docs/                    # 开发日志（节点式可重跑）+ 学习文档
├── 实时数仓项目设计方案V2.md # 设计总纲
└── README.md
```
