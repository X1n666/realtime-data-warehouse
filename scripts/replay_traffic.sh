#!/usr/bin/env bash
# =============================================================
# 流量源重放（一次性重放工具，可重跑 = 幂等）
#
# 为什么需要它:
#   流量域的数据源只在 Kafka（ods_traffic_log），没有 MySQL 批表可查 →
#   环境重建后无法"从库里捞回来"，只能由生成器**确定性重放**。
#   seed/ts/count 三者固定 → 每次重放产出**逐字节相同**的 2000 行，
#   所以下游 641/365 这些锚点是可复现的（不是"每次跑数值都不一样"的 mock）。
#
# 为什么不能只 produce 不清空:
#   重放必须让 ods_traffic_log 回到 earliest=0 的干净状态。若在旧数据后面
#   **追加**，job1 会把同一批 2000 条事件再消费一遍 → dwd_traffic_event 翻倍
#   → 流量指标全部翻倍（且作业不报错，只是数字变大）。
#   所以：删 topic → 等删除真正生效 → 重建 → 灌入 → 断言 earliest=0/latest=2000。
#
# 与 replay_dwd_payment_sorted.sh 同源的坑（那里踩过，详见其文件头）:
#   Kafka 的 topic 删除是**异步**的（先标记 → controller 清日志目录），实测本机
#   耗时可达 60s。删除没落地就 create 会撞 "already exists" 并继续往一个"随后才被
#   删掉"的 topic 生产 → **数据静默丢失**（topic 变成 earliest=latest 的空 topic，
#   下游以 earliest 起读 → 落在末尾 → 读 0 条、作业仍 RUNNING、指标恒空）。
#   → 这里删完轮询 --list 直到 topic 真的消失（最多 120s），没落地就直接失败退出。
#
# 另一个叠加坑（实测复现）: 若此时仍有消费者在线，Kafka 的
#   auto.create.topics.enable=true 会把刚删掉的 topic 用 metadata 请求**自动重建为空**，
#   删除永远完不成（实测表现为 60s 一轮的删除/重建循环）。
#   → 重放前请先 cancel 订阅 ods_traffic_log 的作业（job1_ods_to_dwd）；
#     **冷启动场景下本来就没有消费者，天然满足**（start_env.sh --full 即此路径）。
#
# 用法:
#   scripts/replay_traffic.sh              # 重放 ods_traffic_log
#   TOPIC=_selftest scripts/replay_traffic.sh   # 换 topic（自测用，不动生产 topic）
#
# ============ 坑 1：保留期会静默删掉源数据（2026-09-16 实测，本脚本的存在理由之一）============
#   Kafka 默认保留 168 小时（7 天），且 message.timestamp.type=CreateTime 语义下
#   保留期按**消息自身时间戳**算 —— 而消息时间戳 = **重放那一刻**（不是 --ts 传的业务时间，
#   那个只进消息体）。所以本套"固定历史测试日"的数据集有一个天然的死期：写入满 7 天，
#   broker 的 retention cleaner（每 5 分钟一轮）就会删段文件。
#   实测（2026-09-16）：ods_traffic_log 的记录写入于 09-03，broker 在 09-16 02:24 重启后
#   33 秒就把整段删了 —— broker 日志原文:
#     "Deleting segment LogSegment(baseOffset=0, ...) due to log retention time 604800000ms
#      breach based on the largest record timestamp in the segment"
#     "Incremented log start offset to 2000 due to segment deletion"
#   删完 earliest 直接跳到 2000 = latest：topic 还在、作业全 RUNNING、**零异常**，
#   但源数据没了 → 任何一次 job1 重提都读不到流量（earliest-offset 落在末尾），
#   流量链从此静默。同期 09-09 写入的其余 topic 距同一失效点只剩 79 分钟。
#   → 对策：建 topic 时显式 retention.ms=-1（数据集固定、总量 MB 级，用无限保留），
#     并在 broker 层动态设 log.retention.ms=-1，避免 auto-create 出来的新 topic 又踩。
# =============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

BOOTSTRAP=localhost:9092
TOPIC="${TOPIC:-ods_traffic_log}"
COUNT=2000          # 测试日流量行数（新增的"当日活跃用户"口径锚点）
SEED=20260901       # 与 manifest_20260901.txt 同日同 seed
TS="2026-09-01 08:00:00"   # --ts 字面回填模式：逐秒 +1，事件时间确定可复现
ACTIVE_USERS=500

kafka() { MSYS_NO_PATHCONV=1 docker exec "$@" ; }
kctl()  { kafka gmall_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP "$@" ; }
offset_of() { kafka gmall_kafka /opt/kafka/bin/kafka-get-offsets.sh \
                --bootstrap-server $BOOTSTRAP --topic "$TOPIC" --time "$1" 2>/dev/null \
                | tail -1 | awk -F: '{print $NF}' ; }

# 步骤1: 删除旧 topic 并**等删除真正生效**（异步删除，见文件头坑）
echo "[1/4] 删除 $TOPIC 并等待删除生效..."
kctl --delete --topic $TOPIC 2>/dev/null || true
gone=0
for i in $(seq 1 120); do
  kctl --list 2>/dev/null | grep -qx "$TOPIC" || { echo "  删除已生效（轮询 ${i}s）"; gone=1; break; }
  sleep 1
done
[ "$gone" = 1 ] || {
  echo "  失败: 120s 内 topic 仍未删除干净，中止（避免旧数据残留 → 指标翻倍）"
  echo "  提示: 若有消费者在线，auto.create.topics.enable 会把 topic 立刻重建为空，"
  echo "        删除永远完不成 —— 先 cancel 订阅它的作业再重跑"
  exit 1
}
# 若 topic 本来就不存在（全新环境），跳过删除等待即可，无需报错
# retention.ms=-1 必须显式给：topic 级配置在**重建时全部丢失**并退回 broker 默认 168 小时，
#   而保留期是按"记录写入时间"算的 → 重放满 7 天后段文件会被静默删除。
#   本 topic 实测就是这么被删空的（2026-09-16，详见文件头坑 1）。
kctl --create --topic $TOPIC --partitions 1 --replication-factor 1 --config retention.ms=-1 2>&1 | tail -1

# 步骤2: 生成器确定性重放（--output - 直接管道进 producer，不落盘中转）
#   generate_action_log.py 的行格式与真实日志一致（common/page/action + 事件化字段），
#   stderr 会打 "Wrote N lines"；只取 stdout 进 producer。
echo "[2/4] 重放 $COUNT 行（seed=$SEED, ts=$TS, active-users=$ACTIVE_USERS）..."
python data-generator/generate_action_log.py \
  --seed "$SEED" --count "$COUNT" --active-users "$ACTIVE_USERS" --ts "$TS" --output - \
  | kafka -i gmall_kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server $BOOTSTRAP --topic $TOPIC

# 步骤3: 断言（earliest 必须是 0 —— 非 0 说明重建没干净）
#   这两条断言是本脚本的存在意义：**这类故障不报错**，只能靠显式校验发现。
echo "[3/4] 校验 offset..."
early=$(offset_of -2); late=$(offset_of -1)
echo "  earliest=$early latest=$late"
[ "$early" = 0 ] || { echo "  失败: earliest=$early（应为 0，topic 重建未生效 → 旧数据还在）"; exit 1; }
[ "$late" = "$COUNT" ] || { echo "  失败: latest=$late（应为 $COUNT，生成器或 producer 丢行）"; exit 1; }

# 步骤4: 保留期断言（坑 1）—— 建完必须确认 retention.ms=-1，否则 7 天后静默失效
ret=$(kafka gmall_kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server $BOOTSTRAP \
      --entity-type topics --entity-name $TOPIC --describe --all 2>/dev/null \
      | grep -oE '(^| )retention\.ms=[-0-9]+' | head -1 | tr -d ' ')
echo "[4/4] 保留期校验: ${ret:-retention.ms=<读不到>}"
case "$ret" in
  retention.ms=-1) echo "  ✓ 无限保留（源数据不会被 7 天保留期删掉）" ;;
  *) echo "  警告: $TOPIC 的有效 retention 不是 -1（${ret:-未知}）→ 写入满 7 天后会被静默删除！"
     echo "        修复: 见本文件头坑 1；broker 层可动态设 log.retention.ms=-1" ;;
esac
echo "  完成: $TOPIC 已重置为 earliest=0 / latest=$COUNT ✓"
echo "  下一步: 提交 job1_ods_to_dwd.sql 消费它（scripts/start_env.sh 会自动做）"
