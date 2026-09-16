#!/usr/bin/env bash
# =============================================================
# MySQL 源库初始化 / 重灌（业务表 + 维度种子 + 交易数据 + 实时结果表）
#
# 为什么需要它:
#   交易域整条链的**唯一数据源**是 MySQL（binlog → CDC → dwd_payment_detail → …）。
#   若 gmall_rt 是空库，后面所有环节都"正常运转但产出为空"：job1 的 CDC 无表可读、
#   指标恒为 0、作业全部 RUNNING —— 与"空 topic"同一类静默故障。
#   而 mysql 官方镜像只在**数据目录为空**时执行 /docker-entrypoint-initdb.d，
#   已有数据的卷永远不会重跑 → 必须有一个显式的重灌入口。
#
# 场景 A（全新环境）: docker-compose 已把这 4 个文件挂进 /docker-entrypoint-initdb.d，
#   空卷首次启动**自动**灌好，本脚本无需运行（跑也只是看到"已就绪"跳过）。
# 场景 B（卷被清空/半初始化/手工删表）: 数据目录非空 → initdb.d 不重跑 → 用本脚本。
#
# 幂等保护（重要）:
#   business_data_20260901.sql 里是**裸 INSERT**，执行两次会翻倍（200 单变 400 单），
#   且下游锚点会静默跟着翻倍。所以本脚本先体检：源表非空就**拒绝重灌**并说明原因，
#   必须显式 --force 才覆盖（--force 会先 TRUNCATE 交易表再灌，避免重复行）。
#
# 用法:
#   scripts/load_mysql_source.sh          # 源表为空才灌（安全默认）
#   scripts/load_mysql_source.sh --force  # 清空交易表后重灌（会丢失实时结果库数据？不会，只动源库）
# =============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

FORCE=0
[ "${1:-}" = "--force" ] && FORCE=1

CONTAINER="${CONTAINER:-gmall_mysql}"   # 可覆盖：自测时指向一次性容器，避免动生产库
MYSQL="docker exec -i $CONTAINER mysql -uroot -p123456 --default-character-set=utf8mb4"
q() { # $1=SQL（取单值）
  MSYS_NO_PATHCONV=1 docker exec "$CONTAINER" mysql -uroot -p123456 -N -s -e "$1" 2>/dev/null
}
load() { # $1=sql 文件
  echo "  ← $1"
  MSYS_NO_PATHCONV=1 $MYSQL < "$1"
}

# 依赖顺序（等价于 compose 里 01→04 的挂载顺序，改这里要同步改 compose 注释）:
#   business_ddl       建 9 张业务表（user_info/order_info/order_detail/payment_info/order_refund_info + 4 维度表）
#   init_data          维度种子（base_province 12 / base_category 21 / spu 9 / sku 11）
#   business_data      交易数据（seed=20260901 → 200 单 / 398 明细 / 182 支付 / 12 退款）
#   ads_result_rt_ddl  实时结果表 4 张（写入方 = job3_ads_sink）
# 刻意不含 mysql/ads_result_ddl.sql：离线项目遗留件，内部自相矛盾
#   （CREATE DATABASE gmall_report 后却 USE gmall_report_rt）→ 会让初始化中断。

echo "[1/3] 体检 gmall_rt 源表..."
PAY=$(q "SELECT COUNT(*) FROM gmall_rt.payment_info" 2>/dev/null || echo "")
if [ -z "$PAY" ]; then
  echo "  库/表不存在（全新或未初始化）"
  NEED=1
elif [ "$PAY" = 0 ]; then
  echo "  payment_info 存在但为 0 行 → 需要灌数"
  NEED=1
else
  echo "  payment_info 已有 $PAY 行"
  NEED=0
fi

if [ "$NEED" = 0 ] && [ "$FORCE" = 0 ]; then
  echo "  源表非空，跳过重灌（裸 INSERT 重跑会翻倍）✓"
  echo "  如确认要覆盖: scripts/load_mysql_source.sh --force"
  exit 0
fi

echo "[2/3] 灌入（DDL 用 IF NOT EXISTS，可安全重复；数据段按需）..."
load mysql/business_ddl.sql
load mysql/init_data.sql

if [ "$NEED" = 1 ] || [ "$FORCE" = 1 ]; then
  if [ "$FORCE" = 1 ] && [ "$NEED" = 0 ]; then
    echo "  --force: 先清空交易表（避免重复行）"
    q "SET FOREIGN_KEY_CHECKS=0;
       TRUNCATE gmall_rt.order_detail; TRUNCATE gmall_rt.payment_info;
       TRUNCATE gmall_rt.order_refund_info; TRUNCATE gmall_rt.order_info;
       TRUNCATE gmall_rt.user_info; SET FOREIGN_KEY_CHECKS=1;"
  fi
  load data-generator/data/generated/business_data_20260901.sql
fi

load mysql/ads_result_rt_ddl.sql

echo "[3/3] 校验（对照 manifest_20260901.txt）..."
q "SELECT 'user_info', COUNT(*) FROM gmall_rt.user_info
   UNION ALL SELECT 'order_info', COUNT(*) FROM gmall_rt.order_info
   UNION ALL SELECT 'order_detail', COUNT(*) FROM gmall_rt.order_detail
   UNION ALL SELECT 'payment_info', COUNT(*) FROM gmall_rt.payment_info
   UNION ALL SELECT 'order_refund_info', COUNT(*) FROM gmall_rt.order_refund_info
   UNION ALL SELECT 'base_province', COUNT(*) FROM gmall_rt.base_province
   UNION ALL SELECT 'base_category', COUNT(*) FROM gmall_rt.base_category
   UNION ALL SELECT 'spu_info', COUNT(*) FROM gmall_rt.spu_info
   UNION ALL SELECT 'sku_info', COUNT(*) FROM gmall_rt.sku_info;" \
  | awk -F'\t' 'BEGIN{e["user_info"]=100;e["order_info"]=200;e["order_detail"]=398;e["payment_info"]=182;e["order_refund_info"]=12;e["base_province"]=12;e["base_category"]=21;e["spu_info"]=9;e["sku_info"]=11}
     {s=($2==e[$1])?"✓":"✗ 期望 "e[$1]; printf "  %-18s %-5s %s\n",$1,$2,s; if($2!=e[$1]) bad=1}
     END{exit bad}'
echo "  9 张源表行数与 manifest 一致 ✓"
echo ""
echo "下一步: 提交 job1 做 CDC 全量快照（scripts/start_env.sh --full 会自动串起来）"
