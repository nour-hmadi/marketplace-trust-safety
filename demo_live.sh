#!/bin/bash
echo "================================================"
echo " MARKETPLACE TRUST & SAFETY - LIVE DEMO"
echo "================================================"
echo ""
echo ">>> STEP 1: Injecting suspicious activity for PremiumHub (S62)..."
python3 -c "
import json
from datetime import datetime, timedelta
import random
random.seed(999)
new_orders = [{'order_id':f'O99{100+i}','seller_id':'S62','customer_id':f'C{random.randint(100,199)}','amount':round(random.uniform(200,2000),2),'timestamp':(datetime.now()-timedelta(days=random.randint(1,10))).strftime('%Y-%m-%dT%H:%M:%S'),'status':'completed','category':'luxury'} for i in range(15)]
new_returns = [{'return_id':f'R99{100+i}','order_id':f'O99{100+i}','seller_id':'S62','timestamp':(datetime.now()-timedelta(days=random.randint(1,8))).strftime('%Y-%m-%dT%H:%M:%S'),'reason':'counterfeit','status':'approved'} for i in range(12)]
new_complaints = [{'complaint_id':f'CMP99{100+i}','seller_id':'S62','timestamp':(datetime.now()-timedelta(days=random.randint(1,7))).strftime('%Y-%m-%dT%H:%M:%S'),'severity':'high','category':'counterfeit_claim'} for i in range(10)]
with open('data/orders.json','a') as f:
    for o in new_orders: f.write(json.dumps(o)+'\n')
with open('data/returns.json','a') as f:
    for r in new_returns: f.write(json.dumps(r)+'\n')
with open('data/complaints.json','a') as f:
    for c in new_complaints: f.write(json.dumps(c)+'\n')
print('Injected 15 orders, 12 counterfeit returns, 10 high complaints for PremiumHub')
"
echo ""
echo ">>> STEP 2: Uploading new data to HDFS raw layer..."
docker cp data/orders.json namenode:/tmp/orders.json
docker cp data/returns.json namenode:/tmp/returns.json
docker cp data/complaints.json namenode:/tmp/complaints.json
docker exec namenode hdfs dfs -put -f /tmp/orders.json /data/raw/orders/orders.json
docker exec namenode hdfs dfs -put -f /tmp/returns.json /data/raw/returns/returns.json
docker exec namenode hdfs dfs -put -f /tmp/complaints.json /data/raw/complaints/complaints.json
echo "Data uploaded to HDFS!"

echo ""
echo ">>> STEP 3: Running the full Spark pipeline..."
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/normalize_orders.py 2>/dev/null && echo "normalize_orders ✅"
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/normalize_returns.py 2>/dev/null && echo "normalize_returns ✅"
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/normalize_complaints.py 2>/dev/null && echo "normalize_complaints ✅"
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/normalize_sellers.py 2>/dev/null && echo "normalize_sellers ✅"
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/seller_aggregates.py 2>/dev/null && echo "seller_aggregates ✅"
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/risk_scoring.py 2>/dev/null && echo "risk_scoring ✅"

echo ""
echo ">>> STEP 4: Showing updated risk results..."
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/show_results.py 2>/dev/null | grep -A 50 "SELLER RISK"

echo ""
echo ">>> STEP 5: Generating updated dashboard..."
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/generate_dashboard.py 2>/dev/null
docker cp spark-master:/tmp/dashboard.html docs/geo_risk_dashboard.html
xdg-open docs/geo_risk_dashboard.html

echo ""
echo "================================================"
echo " DEMO COMPLETE! Dashboard updated in browser."
echo "================================================"

echo ""
echo ">>> STEP 6: Regenerating dashboard with new data..."
docker cp spark_jobs/generate_dashboard.py spark-master:/opt/spark_jobs/
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/generate_dashboard.py 2>/dev/null
docker cp spark-master:/tmp/dashboard.html docs/geo_risk_dashboard.html
xdg-open docs/geo_risk_dashboard.html
echo "Dashboard updated!"
