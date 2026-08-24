#!/usr/bin/env bash
# 提交 Flink SQL 作业到 jobmanager（独立 sql-client 容器，remote target）
# 用法: scripts/submit_sql.sh <sql文件相对路径> [job名称]
# 注意: 不要用 docker exec 进 jobmanager 容器跑 sql-client
#       （客户端 JVM 与 JM 共享 cgroup 内存会导致 OOM kill）
set -euo pipefail
cd "$(dirname "$0")/.."

SQL_FILE="$1"
JOB_NAME="${2:-$(basename "$SQL_FILE" .sql)}"
IMAGE="flink:1.19.1"
NET="ecommerce-realtime-data-warehouse_default"

echo ">>> 提交作业 [$JOB_NAME] <- $SQL_FILE"
MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
  -v "$(pwd)/flink-lib:/opt/flink/lib" \
  -v "$(pwd)/flink-sql:/tmp/flink-sql" \
  "$IMAGE" /opt/flink/bin/sql-client.sh \
  -Djobmanager.rpc.address=jobmanager \
  -Djobmanager.rpc.port=6123 \
  -Drest.address=jobmanager \
  -Drest.port=8081 \
  -f "/tmp/flink-sql/$(basename "$SQL_FILE")"
