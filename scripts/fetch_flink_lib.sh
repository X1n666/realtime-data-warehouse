#!/usr/bin/env bash
# =============================================================
# 重建 flink-lib/（Flink 依赖 jar 不入版本库）
# 为什么: 二进制依赖共 ~230MB，其中 flink-dist 单文件 121MB，超
#   GitHub 单文件 100MB 硬限制（推送会被直接拒绝）；且编译产物/依赖
#   本就不该进版本库。本脚本幂等重建，clone 后跑一次即可。
# 组成:
#   1) flink:1.19.1 镜像自带 lib 13 个（docker cp 提取，与镜像版本锁定）
#   2) 外部 connector 4 个（Maven Central 下载，是唯一需要外挂的 jar）
# 用法: bash scripts/fetch_flink_lib.sh
# =============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE="flink:1.19.1"
MVN="https://repo1.maven.org/maven2"
declare -A EXT=(
  [flink-sql-connector-kafka-3.2.0-1.19.jar]="$MVN/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar"
  [flink-sql-connector-mysql-cdc-3.1.1.jar]="$MVN/org/apache/flink/flink-sql-connector-mysql-cdc/3.1.1/flink-sql-connector-mysql-cdc-3.1.1.jar"
  [flink-connector-jdbc-3.2.0-1.19.jar]="$MVN/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar"
  [mysql-connector-j-8.4.0.jar]="$MVN/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar"
)

mkdir -p flink-lib

# ---------- 1) Flink 发行版自带 lib（从镜像提取，版本与镜像一致） ----------
if ls flink-lib/flink-dist-*.jar >/dev/null 2>&1; then
  echo "  发行版 lib 已存在（flink-dist 在），跳过镜像提取"
else
  echo "  从 $IMAGE 提取自带 lib（首次会拉取镜像）..."
  docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull "$IMAGE"
  cid=$(docker create "$IMAGE")
  docker cp "$cid:/opt/flink/lib/." flink-lib/
  docker rm "$cid" > /dev/null
fi

# ---------- 2) 外部 connector（Maven Central） ----------
for name in "${!EXT[@]}"; do
  if [ -s "flink-lib/$name" ]; then
    echo "  $name 已存在，跳过"
  else
    echo "  下载 $name ..."
    curl -fL --retry 3 --connect-timeout 15 -o "flink-lib/$name" "${EXT[$name]}"
  fi
done

echo "  完成：flink-lib/ 共 $(ls flink-lib/*.jar 2>/dev/null | wc -l) 个 jar"
