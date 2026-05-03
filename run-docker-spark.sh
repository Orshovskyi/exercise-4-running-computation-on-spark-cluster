#!/usr/bin/env bash
# Запуск кластера і spark-submit з прогресом у поточному терміналі.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo "=== 1/3 docker compose up -d ==="
docker compose up -d

echo ""
echo "=== 2/3 Очікування воркерів (~15 с) ==="
sleep 15
docker compose ps

echo ""
echo "=== 3/3 spark-submit на кластер (нижче — стейджі Spark) ==="
echo "    (образ Bitnami: Ivy потребує user.home; Hadoop — валідний Unix user → -u root)"
echo ""

docker exec -u root \
  -e JAVA_TOOL_OPTIONS="-Duser.home=/root -Duser.name=root" \
  -e HADOOP_USER_NAME=root \
  spark-master bash -c \
  'mkdir -p /root/.ivy2 && exec /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/bitnami/spark/jobs/process.py'

echo ""
echo "Готово. Результати: $ROOT/out/<назва_завдання>/part-*.csv"
