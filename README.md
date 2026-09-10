# 电商实时数仓项目（学生版，物理分层）

> 一句话定位：用 **Flink + Kafka + Flink CDC** 在单机 WSL2 环境里搭建"物理四层"的电商实时数仓，覆盖**行为日志**与**交易数据**两大域，产出 PV/UV/GMV/支付/退款等指标，支持批流对账与 Grafana 可视化。
>
> 设计总纲：《实时数仓项目设计方案V2.md》（项目根目录），本项目 README 是其运行态快照。学习资料（Flink 复习笔记、SQL 错题手册、面试手册）见 [docs/](docs/)。**每一步的做了什么/为什么/怎么验证/出错如何重跑**，见 [docs/开发日志.md](docs/开发日志.md)（节点式日志）。

---

## 1. 项目是干什么的

模拟一个电商平台的实时数据链路：用户在前端产生**行为日志**（浏览/点击/下单/支付），同时订单系统产生**交易流水**（下单→支付→退款三阶段生命周期）。两条数据流经实时数仓分层处理，最终在 MySQL 结果库和 Grafana 看板上呈现分钟级/天级指标。

**为什么做这个项目**（秋招导向）：

- 覆盖大数据开发岗位的核心面试点：分层建模、事件时间与 watermark、窗口、状态与去重、checkpoint、CDC/changelog、Upsert 幂等、批流对账、故障恢复。
- 简历亮点（当前状态即可这样写）：**物理四层实时数仓**（Job1 接入 ×4 / Job2 DWS ×4 / Job3 无状态 sink ×4，**12 作业**全 RUNNING）、Flink CDC 交易链路（下单/支付/退款三阶段生命周期）、**日口径 9 项指标流批逐值对账分毫不差 + 分钟级 UV/GMV 逐值对账**、环境一键启动复现。

---

## 2. 总体架构

```
行为日志生成器 V2（确定性回放）
  → Kafka ods_traffic_log ──┐
交易生命周期生成器 V2（分阶段写）   │
  → MySQL gmall_rt（9 表）→ binlog │
                           │
        ┌──────────────────┴───────────────────┐
        │ Job1 ×4 作业（并行度 1）ODS → DWD    │
        │  · 流量：Kafka → 解析清洗/去重       │
        │  · 支付：CDC(payment_info) → 透传    │
        │  · 退款：CDC(order_refund_info) → 透传│
        │  · 订单明细：CDC(order_detail) → 透传 │
        │  → dwd_traffic_event                │
        │  → dwd_payment_detail（主键=id）     │
        │  → dwd_refund_detail（主键=id）      │
        │  → dwd_order_detail（主键=id）       │
        └──────────────────────────────────────┘
                          │（交易 DWD 经一次性按时间序重放，
                          │  见 dwd_payment_detail_sorted，节点8）
        ┌─────────────────┴─────────────────────┐
        │ Job2 ×4 作业（并行度 1）DWD → DWS     │
        │  · 分钟窗口：browse PV / 分钟 UV      │
        │  · 日 PV/UV：明细独立重算（不 SUM 分钟）│
        │  · 交易日 7 指标：非窗口按日持续累计  │
        │  · 交易分钟：GMV / 支付订单数（窗口） │
        │  → dws_traffic_1m / dws_traffic_day  │
        │  → dws_trade_day / dws_trade_1m      │
        └───────────────────────────────────────┘
                          │
        ┌─────────────────┴─────────────────────┐
        │ Job3 ×4 作业（并行度 1，无状态 sink） │
        │  DWS 当前值 → MySQL 同主键覆盖写      │
        │  → ads_traffic_1m / ads_traffic_day   │
        │  → ads_trade_day / ads_trade_1m       │
        └───────────────────────────────────────┘
                          │
                     Grafana 看板（provisioning）
```

**分层语义**：ODS（原样接入）→ DWD（明细清洗、去重、规范化）→ DWS（按指标聚合的中间结果）→ ADS（面向展示的结果表）。

**物理四层已于 2026-09-03 达成**（开发日志 节点 6+8+10）：**12 作业三层拓扑**（Job1×4 / Job2×4 / Job3×4）全部 RUNNING。拆作业的关键收益：**指标只在 Job2 算一次**（拆前 ads 分支从 DWD 明细重复计算 = 每指标双份窗口/去重状态），Job3 读 DWS 聚合后的"当前绝对值" + 同主键覆盖写 MySQL → **无状态作业**（无窗口/去重/累计算子，重启秒级、重放天然幂等）。节点 8 后分钟指标补齐（分钟 UV / 交易分钟 GMV、订单数），分钟层共 4 指标 × 分钟窗口驱动趋势曲线；节点 10 交易域扩到下单侧（订单明细 CDC → 下单金额/单数/明细行数，与支付 GMV 对照 = 下单→支付转化视角），日指标 6 → 9。

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

### 4.3 指标口径锚点（支付/流量 6 项 + 下单 3 项 = 9 项，禁止漂移）

| # | 指标         | 口径                                             | 对账方式                    |
| - | ------------ | ------------------------------------------------ | --------------------------- |
| 1 | PV           | browse 事件数                                    | 批 SQL：COUNT(*) browse     |
| 2 | 分钟 UV      | 窗口内 browse 用户去重（中间指标）               | 不参与日对账                |
| 3 | 日 UV        | 自然日内 browse 用户**独立重新去重**       | 批 SQL：COUNT(DISTINCT uid) |
| 4 | GMV          | SUCCESS 支付金额，按支付时间，**不减退款** | 批 SQL：COUNT/SUM           |
| 5 | 支付订单数   | DISTINCT order_id（支付流水）                    | 同上                        |
| 6 | 日支付用户数 | 自然日内 DISTINCT user_id（支付流水）            | 同上                        |
| 7 | 下单金额     | SUM(order_price×sku_num)，按下单时间（节点10 新增，来源 dwd_order_detail）| 批 SQL：SUM               |
| 8 | 下单订单数   | DISTINCT order_id（订单明细表）                  | 批 SQL：COUNT(DISTINCT)     |
| 9 | 下单明细行数 | COUNT(*)（订单明细行）                           | 批 SQL：COUNT(*)            |

**红线**：禁止 `SUM(分钟UV)` 当日 UV、`SUM(分钟支付用户数)` 当日支付用户数、browse+click 混合当 PV。分钟指标与日指标天然不可加（同一用户可跨多个分钟窗口活跃），差异要在对账中写清楚，而不是消除。

**分钟层补充指标（节点 8，锚点外"中间指标"，不参与日对账）**：分钟 UV（窗口内 browse 去重）、交易分钟 GMV / 支付订单数（TUMBLE 窗口，事件时间 = create_time 本地墙上时间）。分钟值用于趋势图；日锚点仍由日表独立兜底（红线同前）。

**下单侧对照（节点 10，锚点 7-9）**：测试日 2026-09-01 下单金额 **1,334,596.00（200 单）** vs 支付 GMV **1,136,485.35（182 单）**，差值 = 下单未支付部分——天然的下单→支付漏斗第一层，与支付/退款三链路合起来覆盖交易完整生命周期（下单→支付→退款）。订单明细**不参与窗口聚合**（日聚合按 DATE 分组天然容忍乱序），故消费未经 sorted 重放的 dwd_order_detail（对比支付必须 sorted：窗口水位推进要求事件时间单调，见节点 8）。

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
- flink-lib/ 挂载 connector jars（17 个 = 13 个 Flink 发行版自带 + 4 个外挂：mysql-cdc / kafka / jdbc / mysql-connector-j）。**jar 为二进制依赖不入库**（合计 ~230MB，其中 flink-dist 单文件 121MB 超 GitHub 100MB 硬限制）——clone 后由 [scripts/fetch_flink_lib.sh](scripts/fetch_flink_lib.sh) 幂等重建：13 个从 `flink:1.19.1` 镜像 `docker cp` 提取（版本与镜像锁定），4 个从 Maven Central 下载；`start_env.sh` 检测到缺失会自动调用。

### 5.2 Kafka Topic（实际已建）

| Topic              | 语义         | 写入作业 | 格式     | 主键                                               |
| ------------------ | ------------ | -------- | -------- | -------------------------------------------------- |
| ods_traffic_log    | ODS 行为日志 | 生成器   | JSON     | —                                                  |
| dwd_traffic_event  | DWD 流量明细 | Job1     | changelog | event_id                                           |
| dwd_payment_detail | DWD 支付明细（CDC 原样）| Job1     | changelog | id（支付流水，非 order_id——同订单可多流水）        |
| dwd_payment_detail_sorted | 支付明细按时间序重放版（节点8：重灌伪影→真实事件流）| replay 脚本 | changelog | 同上                               |
| dwd_refund_detail  | DWD 退款明细 | Job1     | changelog | id（退款流水）                                     |
| dwd_order_detail   | DWD 订单明细 | Job1     | changelog | id（明细行，节点10 新增）                          |
| dws_traffic_1m     | DWS 流量分钟 | Job2     | Upsert   | metric_date+window_start+metric_name+dimension_key |
| dws_traffic_day    | DWS 流量日   | Job2     | Upsert   | metric_date+metric_name+dimension_key              |
| dws_trade_day      | DWS 交易日   | Job2     | Upsert   | 同上（无 window_start）                            |
| dws_trade_1m       | DWS 交易分钟 | Job2     | Upsert   | metric_date+window_start+metric_name+dimension_key |

Job2 交易域支付/分钟窗口消费 `dwd_payment_detail_sorted`——原因见 开发日志 节点 8：测试数据 id 流水号与支付时间错位 + CDC snapshot 主键扫描 → topic 内事件时间乱序 → 水位跳变丢窗口；重放工具 [scripts/replay_dwd_payment_sorted.sh](scripts/replay_dwd_payment_sorted.sh) 幂等可重跑。下单侧（dwd_order_detail，节点 10）消费**未经重放的原始 topic**：日聚合按 DATE 分组不依赖水位，乱序天然无害——两套链路（窗口 vs 非窗口）消费策略的对照，面试可讲。

### 5.3 MySQL

- **gmall_rt**（CDC 源库，9 表）：user_info / base_province / base_category / spu_info / sku_info / order_info / order_detail / payment_info / order_refund_info。binlog ROW + FULL + expire 30 天。
- **gmall_report_rt**（结果库，DDL 见 [mysql/ads_result_rt_ddl.sql](mysql/ads_result_rt_ddl.sql)，实际写入 4 表，均由 Job3 覆盖写）：ads_traffic_1m（33 窗口）/ ads_traffic_day / ads_trade_day（9 行 = 9/1 七指标 + 9/2、9/3 退款）/ ads_trade_1m（162 窗口）。**此库为独立 database**（非 gmall_rt 源库）——建表脚本在 MySQL 空卷初始化后执行过，容器重建/卷丢失后需重跑该 DDL 再启 Job3。

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
| 节点8 | **分钟指标补齐**：流量分钟 UV + 交易分钟表 + 一键启动脚本                                                | 分钟 UV 33 窗口 627≤PV 636 ✓；交易分钟 **162 窗口** GMV 1,119,295.10 = 日 1,136,485.35 − 尾部单笔 17,190.25 ✓；抽 3 分钟窗口 vs MySQL 明细逐值相等 ✓（11 作业 RUNNING） |
| 节点9 | **看板分钟层补齐**：9 面板（PV/UV 双系列 + 分钟 GMV 曲线）                                                | API 验证：9 面板注册 ✓；GMV 序列 162 行 / UV 33 行取数 ✓（浏览器 localhost:3000 admin/admin）                                          |
| 节点10 | **下单侧链路（加分）**：Job1 加订单明细 CDC 分支（server-id 5420-5424）→ dwd_order_detail；Job2 交易日聚合 +3 指标（下单金额/单数/明细行数）；start_env.sh 支持 Job1×4 | **7/7 流=批 ✓**（下单金额 1,334,596.00 / 200 单 / 398 明细行；旧 4 指标 1,136,485.35/182/83/退款按日不变；分钟层 162/33 窗口完好）12 作业 RUNNING（Job1×4/Job2×4/Job3×4） |

**过程中修掉的关键问题**（面试可讲，均记录在开发日志）：

1. 生成器字段索引错位导致 `tracking_no=NULL`（灌库前 SQL 审查发现）
2. 段1 误含 payment/refund INSERT 破坏三段事务语义（binlog 验证发现，TRUNCATE 重灌）
3. jobmanager OOM：`docker exec` 进 JM 容器跑 sql-client 共享 768m cgroup → 改独立容器 + `-D` 集群地址提交
4. MySQL 时区校验失败（+8 vs Etc/UTC）→ CDC 显式 `server-time-zone=Asia/Shanghai`
5. ROW_NUMBER 去重输出 changelog，append sink 编译报错 → DWD 改 upsert-kafka
6. **共享 consumer group bug**：同一 SQL 文件双 INSERT = 两独立作业但共享 source group.id → partition 被一个作业独吞、另一个空转（dws 空 / ads 有）→ 每分支独立 group.id；教训：验证必须覆盖每条输出路径
7. **UNION ALL 列名继承第一段**：首段无 `AS` 别名时列名退化为 EXPR$N → 外层引用报 not found；修复=首段显式别名
8. 多分支 SQL 文件改动单个分支时不能整文件重跑（重复提交 → 同 group 瓜分）→ 抽单段单独提交
9. **共享 consumer group 第二次踩**（节点8）：给既有文件的日 INSERT 旁加分钟 INSERT 时，直接复用了原 source DDL → 两个独立作业同 group → rebalance 中断分钟消费。教训：**同一文件每新增一个 INSERT 分支，其 source 必须再立一份 DDL + 独立 group.id**（job2_dwd 的日段 source 早独立，trade 加段时漏了）
10. **事件时间乱序 → 水位跳变丢窗口**（节点8，最值钱）：CDC snapshot 按主键扫描输出，而测试数据 id 流水号与支付时间错位 → topic 内时间非单调 → 窗口只 fire 43 个（中午前）就停。分层排查法：非窗口日聚合对账正确 → 临时无窗口小时统计全量 ✓ → dump topic 时间序列见乱序 → 机制 = upsert-kafka 水位按物理消费序推进，先读到晚事件则其后早事件全被判"迟到"丢弃。修复 = 一次性按时间序重放成"真实事件流"（replay 工具，与流量 generator replay 同哲学）
11. **误双提 + 异步 cancel 竞赛 → 作业风暴**（节点10）：提交命令笔误执行两次 → 同 server-id 的 CDC 作业互相踢下线 → 15 个 RUNNING/RESTARTING 循环，cancel 与 restart 赛跑清不掉 → 正确恢复 = **重启 jobmanager**（standalone 无 HA，作业全灭，数据在 Kafka/MySQL 不丢）→ 按依赖序一键重提 12 作业。教训：CDC 多作业必须分段 server-id，恢复用"全灭重建"而不是逐 job cancel；另修复 start_env.sh CRLF 坑（Windows docker.exe 管道输出 CRLF 化 → URL 混入 \r，curl exit 3）

---

## 7. 项目经验 ↔ 学习资料交叉引用

简历/面试用：下方三份笔记覆盖了本项目 90% 踩坑点的原理层；项目反过来是三份笔记的"工程实证"。复习顺序建议：面试手册（广）→ SQL 错题册（SQL 手感）→ Flink 笔记（实时原理纵深），每看一条回 §6 找对应 bug 复盘。

| # | 项目经验（§6 关键问题 / 开发日志节点） | Flink 学习与思考题复习笔记 | SQL 错题复习手册 | 数据开发实习面试复习手册 |
| - | ------------------------------------- | -------------------------- | ---------------- | ------------------------ |
| 1 | 物理四层 ODS/DWD/DWS/ADS + 12 作业、Job3 无状态化动机（节点6） | 第一讲 流处理全景（分层与流批对比） | — | 第一章 P0 1.1-1.2 数仓分层/表粒度 |
| 2 | 锚点红线：分钟 UV/用户数不可 SUM 当日（节点4/5） | 第三讲 窗口（窗口与聚合粒度） | 一、聚合粒度与窗口函数 | 4.2 为什么 UV 不能把每天结果相加 |
| 3 | order_gmv 1,334,596.00 vs gmv 1,136,485.35 = 下单→支付漏斗（节点10） | — | 一、聚合粒度（多表多粒度计数） | 5.3 漏斗转化率 / 5.1 PV 与 UV |
| 4 | 支付流水 PK id 非 order_id；明细 vs 订单粒度（节点1） | 第一讲（粒度/主键语义） | 二、多表连接与数据膨胀 | 3.1 事务事实表 / 1.2 粒度 |
| 5 | **水位跳变丢窗口**：乱序 → 早事件被判迟到（节点8 最值钱） | **第二讲 时间语义与 Watermark Q2/Q3** | — | 7.3 如何处理迟到数据 |
| 6 | 尾部单事件窗口不触发（23:32 单笔 17,190.25，节点8） | 第三讲 窗口（触发条件=水位） | 三、日期窗口与滚动统计 | — |
| 7 | 幂等：upsert-kafka + ON DUPLICATE、重跑不重复（节点6/10） | 第四讲 状态与 checkpoint | — | 7.1-7.2 幂等/盲目追加重复统计 |
| 8 | CDC changelog 不声明主键 → -U/+U 当两条翻倍（节点1） | 第一讲（changelog/有状态） | — | 7.1 幂等 |
| 9 | event_id ROW_NUMBER 取最早去重（DWD 流量，节点1 前） | 第一讲 思考题（去重） | 七、每组第一条/最后一条 | 7.3 迟到（同键后到覆盖） |
| 10 | 日 PV/UV 独立 COUNT(DISTINCT) 重算（节点4） | 第三讲（无界聚合 vs 窗口） | 一、聚合粒度 | 4.2 不可加指标 |
| 11 | 生成器字段索引错位、嵌套 ROW 解析清洗（Day2） | — | 六、条件聚合与 NULL | 6.1 DWD 清洗规则 |

（三份笔记头部另有"← 项目交叉索引"提示行，见各文件第 1 行注释下方——项目实践笔记由此双向闭环。）

---

## 8. 后续方向（当前里程碑已完成，均为可选项）

1. ~~第 2 周计划~~ 已全部完成（交易 DWD、交易 DWS、日 PV/UV、批流对账、拆三作业、Grafana）
2. ~~分钟指标补齐（节点8）~~：分钟 UV（33 窗口）、dws_trade_1m 交易分钟（162 窗口）+ 一键启动脚本 scripts/start_env.sh
3. ~~dwd_order_detail 订单明细 CDC（节点10 加分）~~：下单金额/单数/明细行数 3 指标流批对账 ✓（面试点：下单→支付→退款完整生命周期 + 漏斗对照）
4. ~~学习向交叉引用（加分）~~：见上方 §7——三份笔记 ↔ 项目锚点/bug 双向对照表，含每讲思考题与项目机制的映射

---

## 9. 快速开始（clone 后如何跑起来）

```bash
# 1) 重建 Flink 依赖 jar（首次必跑；start_env.sh 检测缺失也会自动调用）
bash scripts/fetch_flink_lib.sh     # 13 个从 flink:1.19.1 镜像提取 + 4 个 Maven 下载

# 2) 一键启动：compose up → 健康等待 → topic 检查 → 作业补齐提交（按依赖序）
bash scripts/start_env.sh           # 幂等可重跑；环境重建后加 --full 触发数据重放

# 3) 打开面板
#    看板   http://localhost:3000   （admin / admin，本地测试凭据）
#     Flink  http://localhost:8081   （12 作业应全 RUNNING）
```

前提：Docker Desktop + WSL2（内存建议 ≥8GB）、bash（Git Bash 可用）、curl。首次启动需拉取镜像（MySQL/Kafka/Flink/Grafana）并执行 MySQL 初始化脚本，请留出几分钟。结果表 DDL 在 [mysql/ads_result_rt_ddl.sql](mysql/ads_result_rt_ddl.sql)，MySQL 卷丢失后需重跑（见 §5.3）。

---

## 10. 目录结构

```
实时数据分析平台/            # 项目根目录（中文名；compose 已用 name: 字段固定项目名）
├── docker-compose.yml       # 全环境编排（MySQL/Kafka/Flink/Grafana + named volume + restart）
├── flink-lib/               # Flink connector jars（挂载到 /opt/flink/lib；jar 不入库，见 §9 重建）
├── flink-sql/               # job1（ODS→DWD 四分支）/ job2（DWD→DWS）×2 / job3（DWS→ADS 无状态 sink）
├── scripts/fetch_flink_lib.sh  # 重建 flink-lib（镜像提取 13 + Maven 下载 4，幂等）
├── scripts/submit_sql.sh    # 作业提交脚本（独立容器 + remote target）
├── scripts/start_env.sh     # 环境一键启动（compose up + 健康/topic/作业检查，幂等）
├── scripts/replay_dwd_payment_sorted.sh  # 交易支付按时间序重放（节点8，幂等）
├── data-generator/          # 行为日志生成器 V2 / 业务生成器 V2 + manifest
├── mysql/                   # business DDL、init、ads 结果库 DDL
├── grafana/                 # provisioning（数据源/看板声明式注册）
├── docs/                    # 开发日志（节点式可重跑）+ 学习文档
├── 实时数仓项目设计方案V2.md # 设计总纲
└── README.md
```
