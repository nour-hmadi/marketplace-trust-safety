# 🎬 DEMO CHEAT SHEET — 30 MINUTES

## STARTUP (before demo)
```bash
sudo systemctl stop mysql
sudo systemctl start docker
docker start namenode datanode airflow-db hive-metastore-db zookeeper kafka kafka-ui airflow-webserver airflow-scheduler spark-master spark-worker
cd ~/Desktop/Project\ 6-\ Final\ BDF/marketplace-trust-safety
```

---

## MIN 0-3: INTRODUCTION
> Talk only — show GitHub repo

---

## MIN 3-6: KAFKA (http://localhost:8080)
> Show current message count first, then run:
```bash
python3 kafka/kafka_demo.py
```
> Refresh Kafka UI — count increases!

---

## MIN 6-10: HDFS (http://localhost:9870)
```bash
docker exec -it namenode hdfs dfs -du -h /data/raw/
docker exec -it namenode hdfs dfs -du -h /data/refined/
docker exec -it namenode hdfs dfs -du -h /data/curated/
docker exec -it namenode hdfs dfs -ls /data/raw/orders/
```

---

## MIN 10-14: SPARK (http://localhost:8082)
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/normalize_orders.py 2>/dev/null
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/seller_aggregates.py 2>/dev/null
```

---

## MIN 14-18: RISK SCORING
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/risk_scoring.py 2>/dev/null
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/show_results.py 2>/dev/null | grep -A 30 "SELLER RISK"
```

---

## MIN 18-21: GOVERNANCE
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/governance.py 2>/dev/null | grep -A 60 "GOVERNANCE"
```

---

## MIN 21-24: AIRFLOW (http://localhost:8081)
> Login: admin / admin123
> Show 3 DAGs → click seller_risk_pipeline → Graph view → Trigger DAG

---

## MIN 24-28: LIVE DEMO
```bash
xdg-open docs/geo_risk_dashboard.html
./demo_live.sh
```

---

## MIN 28-30: HIVE + CLOSING
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/register_hive_tables.py 2>/dev/null | grep -A 20 "REGISTERING"
```

---

## KEY NUMBERS
- Total orders: 655+
- Total returns: 127+
- Total complaints: 146+
- Sellers monitored: 8
- Regions: 4
- HIGH risk sellers: 2
- Highest risk score: 748 (S58)
- Highest risk index: 78.2/100
- Spark jobs: 6
- Airflow DAGs: 3
- HDFS layers: 3

---

## KEY PHRASES
- "End-to-end governed data platform"
- "Real-time threat detection"
- "Layered storage architecture"
- "Automated orchestration with Airflow"
- "Customer PII protected with SHA-256 masking"
- "Role-based access control"
- "Risk index normalized to 0-100 for prioritization"
- "Fraud pattern detection with explainability"

---

## BROWSER TABS TO OPEN BEFORE DEMO
1. http://localhost:8080 (Kafka UI)
2. http://localhost:9870 (HDFS)
3. http://localhost:8082 (Spark)
4. http://localhost:8081 (Airflow)
5. docs/geo_risk_dashboard.html (Dashboard)

---

## EMERGENCY COMMANDS
```bash
# If Spark stops
docker start spark-master spark-worker

# If containers stop
docker start namenode datanode airflow-db hive-metastore-db zookeeper kafka kafka-ui airflow-webserver airflow-scheduler spark-master spark-worker

# Before turning off laptop
docker stop $(docker ps -q)
```
