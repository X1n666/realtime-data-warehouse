#!/usr/bin/env bash
# =============================================================
# 环境一键启动（幂等可重跑）：compose 拉起 + 健康等待 + topic 检查
#   + 作业补齐提交（按依赖顺序 job1 → job2 → job3）
# 用法:
#   scripts/start_env.sh            # 日常启动/重跑
#   scripts/start_env.sh --full     # 环境重建后: 重放流量+交易数据再启作业
# 为什么按文件管理作业: 一个 .sql 文件 = 多个分支 = 多个独立作业，
#   文件内分支共享命名约定(pipeline.name)，按计数检查缺失，缺则整个
#   文件重提（先 cancel 同名残留，避免同 group 多作业消费冲突，见开发日志 节点2）
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
  [job3_ads_sink]=4
)
SQL_ORDER=(job1_ods_to_dwd.sql job2_dwd_to_dws.sql job2_trade_dws.sql job3_ads_sink.sql)
# 运行时依赖 topic（缺 = 数据丢了，需要先重放）
TOPICS=(dwd_traffic_event dwd_payment_detail_sorted dwd_refund_detail
        dws_traffic_1m dws_traffic_day dws_trade_day dws_trade_1m)

say()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$1"; }
warn() { printf '\033[1;33m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$1"; }
err()  { printf '\033[1;31m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$1"; exit 1; }

# ---------- 1. compose 拉起 ----------
say "1/6 docker compose up -d ..."
docker compose up -d

# ---------- 2. 健康等待 ----------
say "2/6 等待服务健康 (mysql/kafka healthcheck + jobmanager REST)..."
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

# ---------- 3. 数据层检查 ----------
say "3/6 topic 数据层检查 ..."
kafka_list() { MSYS_NO_PATHCONV=1 docker exec gmall_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null; }
existing=$(kafka_list)
missing=()
for t in "${TOPICS[@]}"; do
  echo "$existing" | grep -qx "$t" || missing+=("$t")
done
if [ ${#missing[@]} -gt 0 ]; then
  warn "  以下运行时 topic 缺失（Kafka named volume 数据已丢？）: ${missing[*]}"
  if [ "$MODE" = "--full" ]; then
    say "  --full 模式: 触发数据重放 ..."
    # 1) 流量: generator 重放（见 docs/开发日志.md 节点1；确定性 seed 20260901）
    #    scripts/generator_replay.sh 不存在则提示手工执行（保幂等）
    if [ -f scripts/replay_traffic.sh ]; then
      bash scripts/replay_traffic.sh
    else
      warn "  未找到 scripts/replay_traffic.sh —— 按 docs/开发日志.md 节点1 手工重放流量"
    fi
    # 2) 交易: 需先有 job1 CDC 灌出 dwd_payment_detail，再重排 —— 该链依赖 MySQL 数据，
    #    见 docs/开发日志.md 节点8；此处只提示不自动做（MySQL 数据在 named volume）
    warn "  交易数据链: 若 gmall_rt.payment_info 有数据，启动 job1 后运行 scripts/replay_dwd_payment_sorted.sh"
  else
    warn "  日常模式跳过重放；如需完整重建: scripts/start_env.sh --full"
  fi
else
  say "  全部运行时 topic 就位 ✓"
fi

# ---------- 4. 作业补齐（依赖顺序） ----------
say "4/6 作业补齐提交 ..."
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
for sql in "${SQL_ORDER[@]}"; do
  name="${sql%.sql}"
  expect="${JOBS[$name]}"
  now=$(run_count "$name")
  if [ "$now" -ge "$expect" ]; then
    say "  $name: $now/$expect RUNNING 已就绪 ✓"
  else
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
  fi
done

# ---------- 5. 汇总 ----------
say "5/6 最终作业清单:"
docker exec gmall_jobmanager curl -s "$JM/jobs/overview" 2>/dev/null | python -c "
import json,sys,collections
d=json.load(sys.stdin)
c=collections.Counter(j.get('name','?') for j in d.get('jobs',[]) if j.get('state')=='RUNNING')
print('  RUNNING jobs total:', sum(c.values()))
for n,k in sorted(c.items()): print(f'    {n}: {k}')
"
say "6/6 完成。面板: Grafana http://localhost:3000 | Flink http://localhost:8081"
