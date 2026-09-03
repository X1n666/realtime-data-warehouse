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
# =============================================================
set -euo pipefail

BOOTSTRAP=localhost:9092
SRC=dwd_payment_detail
DST=dwd_payment_detail_sorted
MAX_MSG=182        # 测试日 payment 全量行数（batch 锚点）
DUMP=/tmp/payment_dump.tsv
SORTED=/tmp/payment_sorted.tsv

# 步骤1: dump 源 topic 全量（key\tvalue 两列，key=json{id}）
echo "[1/4] dump $SRC ($MAX_MSG 条)..."
MSYS_NO_PATHCONV=1 docker exec gmall_kafka sh -c \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP \
   --topic $SRC --from-beginning --max-messages $MAX_MSG --property print.key=true 2>/dev/null" \
  > "$DUMP"
echo "  dumped: $(wc -l < "$DUMP") 行"

# 步骤2: python 按 (create_time, id) 稳定排序
echo "[2/4] 按 create_time 排序..."
python - "$DUMP" "$SORTED" <<'PY'
import sys, json
src, dst = sys.argv[1], sys.argv[2]
rows = []
for line in open(src, encoding='utf-8'):
    key, _, value = line.rstrip('\n').partition('\t')
    ct = json.loads(value)['create_time']
    iid = json.loads(key)['id']
    rows.append((ct, iid, key + '\t' + value + '\n'))
rows.sort(key=lambda r: (r[0], r[1]))          # (create_time, id) 稳定确定序
with open(dst, 'w', encoding='utf-8') as f:
    f.writelines(r[2] for r in rows)
# 验证: 时间序列应单调不减
ts = [r[0] for r in rows]
assert ts == sorted(ts), '排序结果非单调！'
print(f'  sorted: {len(rows)} 行, 时间单调性校验通过: {ts[0]} .. {ts[-1]}')
PY

# 步骤3: 建目标 topic（已存在则先删——幂等重跑）
echo "[3/4] 建 $DST topic..."
MSYS_NO_PATHCONV=1 docker exec gmall_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP --delete --topic $DST 2>/dev/null || true
sleep 3
MSYS_NO_PATHCONV=1 docker exec gmall_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP --create --topic $DST --partitions 1 --replication-factor 1 2>&1 | tail -1

# 步骤4: producer 灌入（key/value 均 json）
echo "[4/4] 灌入 $DST..."
MSYS_NO_PATHCONV=1 docker exec -i gmall_kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BOOTSTRAP --topic $DST \
  --property parse.key=true --property key.separator=$'\t' < "$SORTED"

echo "完成。验证:"
MSYS_NO_PATHCONV=1 docker exec gmall_kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server $BOOTSTRAP --topic $DST 2>/dev/null | grep -v '^$'
