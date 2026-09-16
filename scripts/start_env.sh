#!/usr/bin/env bash
# =============================================================
# 环境一键启动（幂等可重跑）：compose 拉起 + 健康等待 + 数据源体检
#   + 作业补齐提交（按依赖顺序 job1 → job2 → job4 → job3）
# 用法:
#   scripts/start_env.sh            # 日常启动/重跑（不动数据，只补作业）
#   scripts/start_env.sh --full     # 冷启动/环境重建：重灌 MySQL 源库 + 重放流量
#                                   #   + 重建交易 topic + 再启作业（完整恢复链）
#
# 为什么按文件管理作业: 一个 .sql 文件 = 多个分支 = 多个独立作业，
#   文件内分支共享命名约定(pipeline.name)，按计数检查缺失，缺则整个
#   文件重提（先 cancel 同名残留，避免同 group 多作业消费冲突，见开发日志 节点2）
#
# --full 的顺序是**有依赖的，别调换**（依据见各步注释）：
#   ⓪ 先 cancel 全部作业 → ① MySQL 源库 → ② 流量源 topic → ③ 只提 job1
#   → ④ 等源链灌出 → ⑤ 重排交易 topic → （第 5 步）再按依赖序重提全部
# 关键点：⑤（删除并重建 topic）必须在**下游作业提交之前**做，
#   否则在线消费者的 metadata 请求会触发 Kafka auto-create 把 topic 立刻重建为空，
#   删除永远完不成（实测：有消费者在线时是 60s 一轮的删除/重建循环，
#   无消费者时删除 1 秒生效）→ 见 scripts/replay_dwd_payment_sorted.sh 文件头。
# ⓪ 就是为这条服务的：② 和 ⑤ 各要删除一个"有在线消费者"的 topic
#   （② 删 ods_traffic_log ← job1 订阅；⑤ 删 dwd_payment_detail_sorted ← job2/job4 订阅），
#   所以 --full 先整体 cancel 一次，让"没有消费者在线"从**隐含前提**变成**自己保证的条件**——
#   不这么做的话，运行中执行 --full 会撞上删除/重建竞态（⑤ 硬失败退出 1）。
#   这也是开发日志 节点10 那条已验收的恢复套路（先全灭、再按依赖序重提），
#   因此 --full 在冷启动和运行中**都能跑**，不需要用户先手动停作业。
# =============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-}"

# ---------- 0. 配置 ----------
JM=http://localhost:8081          # jobmanager REST（host 映射）
# 各文件预期分支数（RUNNING 计数达标即视为健康）
declare -A JOBS=(
  [job1_ods_to_dwd]=4
  [job2_dwd_to_dws]=2
  [job2_trade_dws]=2
  [job4_dim_lookup]=2
  [job3_ads_sink]=4
)
SQL_ORDER=(job1_ods_to_dwd.sql job2_dwd_to_dws.sql job2_trade_dws.sql job4_dim_lookup.sql job3_ads_sink.sql)

# 上游源 topic（**恒非空**，空 = 静默故障）。依赖图（消费者视角）:
#   ods_traffic_log     ← 生成器重放          → job1 → dwd_traffic_event → job2_dwd_to_dws
#   (MySQL binlog)      ← load_mysql_source   → job1 → dwd_payment_detail ┐
#                                                       dwd_order_detail ├→ job2_trade_dws / job4
#                                                       dwd_refund_detail┘
#   dwd_payment_detail_sorted ← replay_dwd_payment_sorted.sh（按事件时间重排）
TOPICS_UPSTREAM=(ods_traffic_log dwd_traffic_event dwd_payment_detail
                 dwd_payment_detail_sorted dwd_order_detail dwd_refund_detail)
# 作业产出 topic（job2/job4 写、job3 读）。冷启动或刚重提时为空的**正常**状态
#   —— 1 分钟窗口要等窗口闭合才出数，所以**不能**拿它当故障信号（只会误报）。
TOPICS_DWS=(dws_traffic_1m dws_traffic_day dws_trade_day dws_trade_1m)

say()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$1"; }
warn() { printf '\033[1;33m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$1"; }
err()  { printf '\033[1;31m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$1"; exit 1; }

# ---------- 0.5 flink-lib 依赖检查（jar 不入库，clone 后需重建） ----------
# flink-dist 单文件 121MB 超 GitHub 100MB 限制，二进制依赖整体不入库；
# 缺失时自动重建（fetch 脚本幂等，已存在的 jar 会跳过）
if ! ls flink-lib/flink-dist-*.jar >/dev/null 2>&1; then
  say "0/7 flink-lib 缺失 -> 自动重建（scripts/fetch_flink_lib.sh）..."
  bash scripts/fetch_flink_lib.sh
fi

# ---------- 通用 helper ----------
kafka_list() { MSYS_NO_PATHCONV=1 docker exec gmall_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null; }
offset_of() { # $1=topic $2=-2(earliest)|-1(latest) → 打印 offset 数字
  MSYS_NO_PATHCONV=1 docker exec gmall_kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$1" --time "$2" 2>/dev/null | tail -1 | awk -F: '{print $NF}'
}
wait_topic() { # $1=topic $2=最小 latest 条数 $3=超时秒 → 等"≥min 且连续 10s 无新增"
  local t=$1 min=$2 timeout=$3 last="" stable=0 now i
  for i in $(seq 1 "$timeout"); do
    now=$(offset_of "$t" -1)
    if [ -n "$now" ] && [ "$now" -ge "$min" ] 2>/dev/null; then
      if [ "$now" = "$last" ]; then
        stable=$((stable+1))
        if [ "$stable" -ge 10 ]; then say "  $t: latest=$now ✓（连续 10s 无新增）"; return 0; fi
      else
        stable=0; last="$now"
      fi
    fi
    sleep 1
  done
  warn "  $t 等待超时（latest=${last:-无}，期望 ≥$min）—— 查 job1 是否 RUNNING / jobmanager 日志"
  return 1
}
run_count() { # 返回某 pipeline.name 的 RUNNING 作业数
  docker exec gmall_jobmanager curl -s "$JM/jobs/overview" 2>/dev/null \
    | python -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for j in d.get('jobs',[]) if j.get('state')=='RUNNING' and j.get('name')=='$1'))"
}
cancel_name() { # cancel 所有同名 RUNNING（防同 group 多作业冲突）
  # 注意: docker exec 输出经 Windows docker.exe 管道会被 CRLF 化（空输出->\r\n），
  #   read 剥 \n 后残留裸 \r 会混进 URL（curl exit 3 malformed）—— 用 tr -d '\r' 清洗
  docker exec gmall_jobmanager curl -s "$JM/jobs/overview" 2>/dev/null \
    | python -c "import json,sys; d=json.load(sys.stdin); print(' '.join(j['jid'] for j in d.get('jobs',[]) if j.get('state')=='RUNNING' and j.get('name')=='$1'))" \
    | tr -d '\r' \
    | while read -r jids; do for jid in $jids; do
        docker exec gmall_jobmanager curl -s -X PATCH "$JM/jobs/$jid" > /dev/null; done; done
}
submit_file() { # $1 = flink-sql 下的文件名 → 补齐该文件全部分支
  local sql=$1 name="${1%.sql}" expect now
  expect="${JOBS[$name]}"
  now=$(run_count "$name")
  if [ "$now" -ge "$expect" ]; then
    say "  $name: $now/$expect RUNNING 已就绪 ✓"
    return 0
  fi
  warn "  $name: $now/$expect 不达标 -> 重提（cancel 同名残留后提交）"
  cancel_name "$name"
  sleep 5
  bash scripts/submit_sql.sh "flink-sql/$sql" "$name"
  # 等待作业进入 RUNNING
  for i in $(seq 1 40); do
    sleep 3
    [ "$(run_count "$name")" -ge "$expect" ] && break
  done
  now=$(run_count "$name")
  [ "$now" -ge "$expect" ] || warn "  $name 提交后 RUNNING=$now（预期 $expect）—— 查 jobmanager 日志"
}

# ---------- 1. compose 拉起 ----------
say "1/7 docker compose up -d ..."
docker compose up -d

# ---------- 2. 健康等待 ----------
say "2/7 等待服务健康 (mysql/kafka healthcheck + jobmanager REST)..."
for i in $(seq 1 60); do
  MS=$(docker inspect --format '{{.State.Health.Status}}' gmall_mysql   2>/dev/null || echo none)
  KS=$(docker inspect --format '{{.State.Health.Status}}' gmall_kafka   2>/dev/null || echo none)
  JR=$(curl -s -o /dev/null -w '%{http_code}' "$JM/overview" 2>/dev/null || echo 000)
  if [ "$MS" = healthy ] && [ "$KS" = healthy ] && [ "$JR" = 200 ]; then
    say "  全部就绪 (mysql=$MS kafka=$KS jobmanager=http $JR)"
    break
  fi
  [ "$i" = 60 ] && err "等待超时: mysql=$MS kafka=$KS jm=$JR —— docker compose logs 看原因"
  sleep 3
done

# ---------- 3. 数据源体检（只诊断，不改数据） ----------
say "3/7 数据源体检 ..."
existing=$(kafka_list)
missing=()
for t in "${TOPICS_UPSTREAM[@]}" "${TOPICS_DWS[@]}"; do
  echo "$existing" | grep -qx "$t" || missing+=("$t")
done
if [ ${#missing[@]} -gt 0 ]; then
  warn "  缺失 topic: ${missing[*]}"
  warn "  → 需重建数据源: scripts/start_env.sh --full"
else
  say "  全部 ${#TOPICS_UPSTREAM[@]} 个上游 + ${#TOPICS_DWS[@]} 个产出 topic 就位 ✓"
fi
# MySQL 源库是交易域**唯一**数据源：空库 → CDC 无表可读 → 指标恒 0 且不报错
PAY=$(MSYS_NO_PATHCONV=1 docker exec gmall_mysql mysql -uroot -p123456 -N -s \
      -e "SELECT COUNT(*) FROM gmall_rt.payment_info" 2>/dev/null || echo "")
if [ -z "$PAY" ]; then
  warn "  MySQL 源表 gmall_rt.payment_info 不存在（空库）→ 交易域无法重建"
  warn "    修复: scripts/load_mysql_source.sh   （--full 会自动调用）"
elif [ "$PAY" = 0 ]; then
  warn "  MySQL 源表 payment_info 为 0 行 → 交易域无法重建"
  warn "    修复: scripts/load_mysql_source.sh   （--full 会自动调用）"
else
  say "  MySQL 源表就绪 (payment_info=$PAY 行) ✓"
fi
# 保留期收敛（2026-09-16 实测故障，见 docs/开发日志.md）：
#   Kafka 默认保留 168 小时，且按**消息时间戳**算 → 本套"固定历史测试日"数据写入满 7 天
#   就被 retention cleaner 静默删掉（实测 ods_traffic_log 已被删空，log start offset
#   从 0 跳到 2000；同期其余 topic 距失效只剩 79 分钟）。删完 topic 还在、作业全 RUNNING、
#   零异常，只是源数据没了 → 静默故障。所以每次启动都把保留期**收敛**为无限（幂等）。
#   为什么不用改 compose 的 broker 默认：改 env 要重建 Kafka 容器，而当前 kafka-data 卷
#   挂错路径（见 compose 注释）→ 重建会清空全部 topic，必须与卷修复一起做。
MSYS_NO_PATHCONV=1 docker exec gmall_kafka sh -c '
  for t in ods_traffic_log dwd_traffic_event dwd_payment_detail dwd_payment_detail_sorted \
           dwd_order_detail dwd_refund_detail dws_traffic_1m dws_traffic_day dws_trade_day dws_trade_1m; do
    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "$t"; then
      /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
        --entity-type topics --entity-name "$t" --add-config retention.ms=-1 >/dev/null 2>&1 \
        && echo "  保留期收敛 $t: retention.ms=-1 ✓" \
        || echo "  保留期收敛 $t: 失败（需人工确认）"
    else
      echo "  保留期收敛 $t: 跳过（topic 尚不存在，将由重放脚本以 -1 创建）"
    fi
  done' 2>&1 | sed 's/^/  /'

# ---------- 4. [--full] 冷启动重建链 ----------
if [ "$MODE" = "--full" ]; then
  say "4/7 [--full] 冷启动重建链（顺序有依赖，见文件头）..."
  # ⓪ 先 cancel 全部：② 和 ⑤ 要删除的两个 topic 都有在线消费者（job1 / job2 / job4），
  #    消费者在线时删除永远完不成（auto-create 竞态）。先整体停掉，第 5 步会按依赖序重提。
  say "  ⓪ cancel 全部作业（让下面两次 topic 重建不必和在线消费者赛跑）..."
  for sql in "${SQL_ORDER[@]}"; do cancel_name "${sql%.sql}"; done
  sleep 5
  say "  ① MySQL 源库（交易域唯一数据源）..."
  bash scripts/load_mysql_source.sh
  say "  ② 流量源 topic 重放（流量域只在 Kafka，无批表可捞）..."
  bash scripts/replay_traffic.sh
  say "  ③ 只先提 job1 —— 它是两条源链的共同上游(流量加工 + 交易 CDC 快照)..."
  submit_file job1_ods_to_dwd.sql
  say "  ④ 等 job1 把两条源链灌出来..."
  # 2000 = 生成器 2000 行 → 2000 个唯一 event_id（upsert 主键，1 行 1 条）
  #   注意别拿 12000 当期望：job1 的 source 写死 earliest-offset，**每提交一次就重放一遍**
  #   全量 → topic 里会有 N 份历史副本（当前实测 12000 = 6 次提交 × 2000）。
  #   副本无害（下游 upsert 读法是"每 key 取最新"），但期望值必须是单次重放量。
  wait_topic dwd_traffic_event  2000 300
  # 182 = 测试日 payment_info 全量行数（manifest 锚点）
  wait_topic dwd_payment_detail 182  300
  say "  ⑤ 重排交易 topic（此刻下游未提交 = 无消费者在线，删除 1s 生效）..."
  bash scripts/replay_dwd_payment_sorted.sh
else
  say "4/7 日常模式：跳过冷启动重建链（需要时: scripts/start_env.sh --full）"
fi

# ---------- 5. 作业补齐（依赖顺序） ----------
say "5/7 作业补齐提交 ..."
for sql in "${SQL_ORDER[@]}"; do
  submit_file "$sql"
done

# ---------- 6. 空 topic 哨兵（最难发现的一类故障，2026-09-16 实测踩到） ----------
# topic **存在但为空**（earliest == latest ≠ 0）不报任何错：
#   下游以 auto.offset.reset=earliest 起读 → 落在 log-start = 末尾 → 读 0 条、
#   作业保持 RUNNING、指标为 0、消费组因为没有提交位点而"不存在"。
#   整条链路看起来完全健康，只是所有指标静默变空（本次排查花了一小时）。
# 成因是三件事叠加（缺一不可）：
#   ① Kafka 的 auto.create.topics.enable=true → 删除后立刻被在线消费者的 metadata
#      请求自动重建为空 topic（日志特征 "Sent auto-creation request for Set(<topic>)"）；
#   ② topic 删除是**异步**的，实测本机耗时 60s → 重建/生产与删除互相赛跑；
#   ③ 消费端 committed offset 落在新 topic 的合法区间内（正好 = 末尾）→ 不回退。
# 只查 TOPICS_UPSTREAM：dws_* 是作业产出，刚提交时窗口还没闭合，为空是正常的（查它会误报）。
if [ ${#TOPICS_UPSTREAM[@]} -gt 0 ]; then
  # 两类"空"要分开报，因为**成因完全不同**（实测各踩过一次）：
  #   earliest==latest==0    → 从来没写进去过，或删除/重建竞态把它重置成了空 topic
  #                            （auto.create + 异步删除，见文件头 / 开发日志）
  #   earliest==latest!=0    → **曾经有数据、被删掉了**：log start offset 前移，
  #                            最常见就是保留期过期（本机实测：ods_traffic_log 从 0 跳到 2000）
  empty_topics=(); expired_topics=()
  for t in "${TOPICS_UPSTREAM[@]}"; do
    e=$(offset_of "$t" -2); l=$(offset_of "$t" -1)
    # 只统计"存在但为空"；不存在的 topic 由第 3 步负责（冷启动属正常）
    [ -n "$e" ] && [ -n "$l" ] && [ "$e" != "$l" ] && continue
    if [ "$e" = 0 ]; then empty_topics+=("$t"); else expired_topics+=("$t(=$e)"); fi
  done
  if [ ${#empty_topics[@]} -gt 0 ]; then
    warn "6/7 空 topic（earliest=latest=0，从未写入/被重建为空）: ${empty_topics[*]}"
    warn "  → 这不是'数据还没到'，是静默故障：作业会 RUNNING 但指标恒为空"
    warn "  → dwd_payment_detail_sorted: 跑 scripts/replay_dwd_payment_sorted.sh"
    warn "    注意：必须先把订阅它的作业 cancel（job2_trade_dws / job4），否则删除会被"
    warn "    在线消费者立刻自动重建，删不干净（实测复现为 60s 一轮的删除/重建循环）"
    warn "  → ods_traffic_log: 跑 scripts/replay_traffic.sh（同样先 cancel job1）"
  fi
  if [ ${#expired_topics[@]} -gt 0 ]; then
    warn "6/7 数据被删空的 topic（earliest=latest≠0，log start offset 已前移）: ${expired_topics[*]}"
    warn "  → 这类**不是**写入失败，是数据本来在、后来被删除：最常见是保留期过期"
    warn "    （Kafka 默认 168 小时、按消息时间戳算；本机实测踩到过，见开发日志）"
    warn "  → 本脚本第 3 步已把保留期收敛为 -1；但**已被删的数据找不回来**，必须重放："
    warn "    ods_traffic_log → scripts/replay_traffic.sh"
    warn "    dwd_payment_detail_sorted → scripts/replay_dwd_payment_sorted.sh"
  fi
  if [ ${#empty_topics[@]} = 0 ] && [ ${#expired_topics[@]} = 0 ]; then
    say "6/7 上游 ${#TOPICS_UPSTREAM[@]} 个 topic 全部非空 ✓"
  fi
fi

# ---------- 7. 汇总 ----------
say "7/7 最终作业清单:"
docker exec gmall_jobmanager curl -s "$JM/jobs/overview" 2>/dev/null | python -c "
import json,sys,collections
d=json.load(sys.stdin)
c=collections.Counter(j.get('name','?') for j in d.get('jobs',[]) if j.get('state')=='RUNNING')
print('  RUNNING jobs total:', sum(c.values()))
for n,k in sorted(c.items()): print(f'    {n}: {k}')
"
say "完成。面板: Grafana http://localhost:3000 | Flink http://localhost:8081"
