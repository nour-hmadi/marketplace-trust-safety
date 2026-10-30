#!/usr/bin/env bash
# Start the platform in the right order (Zookeeper before Kafka avoids the NodeExists crash).
cd "$(dirname "$0")"

echo "1/5 Docker"
sudo systemctl stop mysql 2>/dev/null
sudo systemctl start docker

echo "2/5 HDFS, Zookeeper, databases"
docker start namenode datanode zookeeper airflow-db hive-metastore-db >/dev/null
sleep 30

echo "3/5 Kafka"
docker start kafka >/dev/null
sleep 20

echo "4/5 Kafka UI, Airflow, Spark"
docker start kafka-ui airflow-webserver airflow-scheduler spark-master spark-worker >/dev/null

echo "5/5 Checks"
for c in airflow-webserver airflow-scheduler; do
  docker exec "$c" python -c "import kafka" 2>/dev/null \
    || docker exec -u root "$c" cp -r /root/.local/lib/python3.8/site-packages/kafka /usr/local/lib/python3.8/site-packages/
done
sleep 10
docker ps -a --format "table {{.Names}}\t{{.Status}}"
if [ "$(docker inspect -f '{{.State.Running}}' kafka)" != "true" ]; then
  echo "WARNING: Kafka stopped. Run: docker start kafka"
fi
echo
echo "Next: terminal 2 -> python3 app.py | terminal 3 -> streamlit run streamlit_app.py"
