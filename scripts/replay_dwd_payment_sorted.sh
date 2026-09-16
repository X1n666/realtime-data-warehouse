#!/usr/bin/env bash
# =============================================================
# 交易支付数据"事件化"重排（一次性重放工具，可重跑 = 幂等）
#
# 为什么需要它（开发日志 节点8 排障）:
#   测试数据 payment 的 id 是流水号，与支付时间 create_time 天然错位；
#   CDC snapshot 按主键扫描输出 → Kafka topic 内事件时间非单调（乱序）。
#   真实生产里 Kafka 事件序 ≈ 发生序（binlog 提交序）；本工具把重灌伪影
#   "重放成真实事件流"（与流量域 generator replay 同哲学），使窗口聚合的
#   watermark 语义成立。
#
# 流程: dwd_payment_detail(乱序) --按 create_time 排序--> dwd_payment_detail_sorted
# 排序稳定键: (create_time, id) —— 同秒记录按 id 保持确定性
#
# ============ 三个已实测的坑（2026-09-16 复盘修复，别改回去） ============
# 1) 【必须显式设 retention.ms=-1】本 topic 是每次重放**重建**的，而 topic 级配置
#    在重建时全部丢失 → 退回 broker 默认 168 小时。表面上无所谓，实际是静默炸弹：
#    Kafka 的 CreateTime 语义下，保留期是按**消息自身时间戳**算的，而本数据集全是
#    "重放那一刻"写入的历史测试日数据（事件时间是 2026-09-01，但记录时间戳是写入时间）
#    → 7 天后 broker 的 retention cleaner（每 5 分钟一轮）会把段文件删掉，
#    **不留任何作业侧异常**：earliest 追上 latest，下游 earliest 起读落在末尾，读 0 条、
#    作业仍 RUNNING、指标恒为空。实测（2026-09-16）：ods_traffic_log 就是这样被删空的
#    （broker 日志证据: "Deleting segment ... due to log retention time 604800000ms
#     breach based on the largest record timestamp in the segment"，删完 log start
#     offset 直接从 0 跳到 2000）；同批 09-09 写入的其余 topic 距失效只剩 79 分钟。
# 2) 【必须等删除真的生效】原实现是 `--delete` 后 sleep 3 再 `--create`。
#    Kafka 的 topic 删除是**异步**的（先标记 → controller 清理日志目录），
#    实测本机删除耗时 **>30s**，sleep 3 远远不够 → create 撞上 "already exists"，
#    然后 producer 往一个"随后才被真正删掉"的 topic 写 → **数据静默丢失**。
#    实测后果（2026-09-16 发现）：本 topic 变成 earliest=latest=182 的**空 topic**
#    （当时该 topic 刚被重放脚本创建仅几分钟，7 天保留期不可能已过期，故本例排除
#      retention —— 保留期致空是**另一种**情形，见坑 1，发生在写入满 7 天之后），
#    下游 job2/job4 以 earliest 起读 → 落到 182 = 末尾 → **一条不读、且不报错**
#    （作业保持 RUNNING、指标为 0，极难发现）。
#    → 现在改成：删完**轮询 --list 直到 topic 真的消失（最多 120s）**，再 create；
#      删除没落地就**直接失败退出**，绝不在脏状态下继续。
# 3) 【必须按 key 取最新版本】源 topic 是 upsert-kafka（key=id），job1 每次
#    提交都会做一次全量快照重发 → 同一个 id 在 topic 里有 N 份历史副本。
#    原实现 `--max-messages 182` 取的是**最老的 182 条**，等于按"最旧版本"
#    重组状态。upsert 语义的正确读法是**每个 key 只保留最后一条**。
#    → 现在改成：drain 全量 + 按 key 取最后一条 + 再排序，并断言唯一 key 数。
# =============================================================
set -euo pipefail

BOOTSTRAP=localhost:9092
SRC=dwd_payment_detail
DST=dwd_payment_detail_sorted
EXPECT_KEYS=182    # 测试日 payment 全量行数（batch 锚点）
DUMP=/tmp/payment_dump.tsv
SORTED=/tmp/payment_sorted.tsv

kafka() { MSYS_NO_PATHCONV=1 docker exec "$@" ; }
kctl()  { kafka gmall_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP "$@" ; }

# 步骤1: drain 源 topic 全量（key\tvalue 两列，key=json{id}）
#   --timeout-ms: 源 topic 不会关闭，靠"静默超时"退出；超时退出码非 0 → || true，
#   实际完整性由下一步的"唯一 key 数断言"保证（不是靠退出码）。
echo "[1/5] dump $SRC 全量（含历史副本）..."
kafka -i gmall_kafka sh -c \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP \
   --topic $SRC --from-beginning --timeout-ms 8000 --property print.key=true 2>/dev/null" \
  > "$DUMP" || true
echo "  dumped: $(wc -l < "$DUMP") 条原始记录"

# 步骤2: 按 key 取最新（upsert 语义）→ 按 (create_time, id) 稳定排序
echo "[2/5] 按 key 取最新版本 → 按 create_time 排序..."
python - "$DUMP" "$SORTED" "$EXPECT_KEYS" <<'PY'
import sys, json
src, dst, expect = sys.argv[1], sys.argv[2], int(sys.argv[3])
latest = {}                      # key -> (create_time, id, 行内容)
raw = 0
for line in open(src, encoding='utf-8'):
    line = line.rstrip('\n')
    if not line:
        continue
    raw += 1
    key, _, value = line.partition('\t')
    if not _:
        continue
    j = json.loads(value)
    k = json.loads(key)['id']
    latest[k] = (j['create_time'], k, key + '\t' + value + '\n')   # 同名 key 覆盖 = 取最后一条
rows = sorted(latest.values(), key=lambda r: (r[0], r[1]))         # (create_time, id) 稳定确定序
with open(dst, 'w', encoding='utf-8') as f:
    f.writelines(r[2] for r in rows)
ts = [r[0] for r in rows]
assert ts == sorted(ts), '排序结果非单调！'
print(f'  原始 {raw} 条 → 去重后 {len(rows)} 个 key（期望 {expect}），时间单调性校验通过: {ts[0]} .. {ts[-1]}')
assert len(rows) == expect, f'唯一 key 数 {len(rows)} != 期望 {expect}：源 topic 数据不完整或含有额外测试行'
PY

# 步骤3: 重建目标 topic —— 必须等删除真正生效（见文件头坑 1）
echo "[3/5] 重建 $DST topic（等删除生效）..."
kctl --delete --topic $DST 2>/dev/null || true
gone=0
for i in $(seq 1 120); do
  kctl --list 2>/dev/null | grep -qx "$DST" || { echo "  删除已生效（轮询 ${i}s）"; gone=1; break; }
  sleep 1
done
# 实测：本机 Kafka 的 topic 删除耗时**远超**原来的 sleep 3（>30s）。删除没落地就 create
# 会拿到"Topic already exists"并继续往一个"随后才被删掉"的 topic 生产 → 数据静默丢失。
# 所以这里宁可失败退出，也不在未删除干净的情况下继续。
[ "$gone" = 1 ] || { echo "  失败: 120s 内 topic 仍未删除干净，中止（避免继承旧 log-start-offset）"; exit 1; }
# retention.ms=-1 必须显式给：topic 级配置在**重建时会全部丢失**并退回 broker 默认
#   （168 小时），而本套数据集的记录时间戳是"重放那一刻" → 7 天后会被静默删除，
#   表现为 earliest 追上 latest、下游读 0 条且不报错。见文件头坑 3。
kctl --create --topic $DST --partitions 1 --replication-factor 1 --config retention.ms=-1 2>&1 | tail -1

# 步骤4: producer 灌入（key/value 均 json）
echo "[4/5] 灌入 $DST..."
kafka -i gmall_kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BOOTSTRAP --topic $DST \
  --property parse.key=true --property key.separator=$'\t' < "$SORTED"

# 步骤5: 产出校验（earliest 必须是 0 —— 非 0 说明重建没干净）
echo "[5/5] 校验:"
kafka gmall_kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server $BOOTSTRAP --topic $DST 2>/dev/null | grep -v '^$' | sed 's/^/  /'
early=$(kafka gmall_kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server $BOOTSTRAP --topic $DST --time -2 2>/dev/null | tail -1)
late=$(kafka gmall_kafka /opt/kafka/bin/kafka-get-offsets.sh \
       --bootstrap-server $BOOTSTRAP --topic $DST --time -1 2>/dev/null | tail -1)
[ "${early##*:}" = 0 ] || { echo "  失败: earliest=${early##*:}（应为 0，topic 重建未生效）"; exit 1; }
[ "${late##*:}" = "$EXPECT_KEYS" ] || { echo "  失败: latest=${late##*:}（应为 $EXPECT_KEYS）"; exit 1; }
echo "  earliest=0 / latest=$EXPECT_KEYS ✓ 重排完成"
