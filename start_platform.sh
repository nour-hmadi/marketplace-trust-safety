#!/usr/bin/env bash
# Start the whole platform from docker-compose.yml, in the right order:
# Zookeeper gets a head start before Kafka (avoids the "NodeExists" crash after a restart).
set -e
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "Missing .env: create it first (see 'How to run' in README.md)."
  exit 1
fi

echo "1/3 HDFS, Zookeeper, Airflow database"
docker compose up -d zookeeper namenode datanode airflow-db
sleep 30

echo "2/3 Kafka"
docker compose up -d kafka
sleep 20

echo "3/3 Spark, Airflow, Kafka UI (builds the Airflow image the first time)"
docker compose up -d
docker compose ps --format "table {{.Name}}\t{{.Status}}"
echo
echo "First run only: python bootstrap.py"
echo "Then: terminal 2 -> python3 app.py | terminal 3 -> streamlit run streamlit_app.py"
