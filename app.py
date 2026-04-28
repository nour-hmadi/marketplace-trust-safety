from flask import Flask, request, jsonify
from flask_cors import CORS
import json, random, subprocess
from datetime import datetime

app = Flask(__name__)
CORS(app)
events_log = []

def try_kafka(topic, message):
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=3000,
            api_version=(0, 10, 1)
        )
        producer.send(topic, value=message)
        producer.flush()
        producer.close()
        return True
    except Exception as e:
        print(f"Kafka not available: {e}")
        return False

def fallback_sellers():
    return [
        {"seller_id":"S58","seller_name":"BudgetFinds","region":"Middle East","verification_status":"unverified","total_orders":101,"total_returns":46,"return_rate":0.455,"total_complaints":60,"high_severity_complaints":47,"avg_order_amount":329.6,"risk_score":748.2,"risk_index":61.8,"risk_label":"HIGH"},
        {"seller_id":"S61","seller_name":"CheapGoods","region":"Asia","verification_status":"unverified","total_orders":66,"total_returns":29,"return_rate":0.439,"total_complaints":36,"high_severity_complaints":26,"avg_order_amount":403.6,"risk_score":442.6,"risk_index":58.1,"risk_label":"HIGH"},
        {"seller_id":"S56","seller_name":"QuickDeals","region":"Europe","verification_status":"unverified","total_orders":71,"total_returns":20,"return_rate":0.282,"total_complaints":24,"high_severity_complaints":4,"avg_order_amount":305.2,"risk_score":106.3,"risk_index":33.0,"risk_label":"MEDIUM"},
        {"seller_id":"S62","seller_name":"PremiumHub","region":"Europe","verification_status":"verified","total_orders":106,"total_returns":28,"return_rate":0.264,"total_complaints":24,"high_severity_complaints":22,"avg_order_amount":617.4,"risk_score":345.6,"risk_index":37.4,"risk_label":"MEDIUM"},
        {"seller_id":"S55","seller_name":"TechZone Store","region":"North America","verification_status":"verified","total_orders":54,"total_returns":12,"return_rate":0.222,"total_complaints":10,"high_severity_complaints":6,"avg_order_amount":327.5,"risk_score":113.9,"risk_index":27.0,"risk_label":"LOW"},
        {"seller_id":"S60","seller_name":"FastShip Co","region":"North America","verification_status":"verified","total_orders":82,"total_returns":9,"return_rate":0.110,"total_complaints":8,"high_severity_complaints":4,"avg_order_amount":339.6,"risk_score":79.4,"risk_index":18.7,"risk_label":"LOW"},
        {"seller_id":"S57","seller_name":"MegaSupplies","region":"Asia","verification_status":"verified","total_orders":97,"total_returns":9,"return_rate":0.093,"total_complaints":4,"high_severity_complaints":2,"avg_order_amount":324.5,"risk_score":48.7,"risk_index":16.3,"risk_label":"LOW"},
        {"seller_id":"S59","seller_name":"LuxuryGoods","region":"Europe","verification_status":"verified","total_orders":93,"total_returns":2,"return_rate":0.022,"total_complaints":2,"high_severity_complaints":1,"avg_order_amount":328.8,"risk_score":15.9,"risk_index":13.8,"risk_label":"LOW"}
    ]

def run_spark(script, marker, timeout=120):
    """Run a Spark script and extract marked output"""
    with open('/tmp/spark_script.py', 'w') as f:
        f.write(script)
    subprocess.run(['docker', 'cp', '/tmp/spark_script.py', 'spark-master:/tmp/spark_script.py'])
    result = subprocess.run([
        'docker', 'exec', 'spark-master',
        '/spark/bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '/tmp/spark_script.py'
    ], capture_output=True, text=True, timeout=timeout)
    for line in result.stdout.split('\n'):
        if line.startswith(marker):
            return json.loads(line.replace(marker, ''))
    return None

@app.route('/api/sellers', methods=['GET'])
def get_sellers():
    try:
        with open('/tmp/sellers_cache.json', 'r') as f:
            data = json.load(f)
            return jsonify({"success": True, "data": data, "source": "cache"})
    except:
        return jsonify({"success": True, "data": fallback_sellers(), "source": "fallback"})

@app.route('/api/refresh', methods=['POST'])
def refresh_sellers():
    script = """
from pyspark.sql import SparkSession
import json
spark = SparkSession.builder.appName("Refresh").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("hdfs://namenode:9000/data/curated/seller_risk/")
data = []
for row in df.collect():
    d = {}
    for k,v in row.asDict().items():
        d[k] = v.item() if hasattr(v,'item') else v
    data.append(d)
with open('/tmp/sellers_cache.json','w') as f:
    json.dump(data, f)
print("REFRESH_DONE:true")
spark.stop()
"""
    try:
        run_spark(script, "REFRESH_DONE:")
        subprocess.run(['docker','cp','spark-master:/tmp/sellers_cache.json','/tmp/sellers_cache.json'])
        return jsonify({"success": True, "message": "Refreshed from HDFS!"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/customer_orders/<seller_id>', methods=['GET'])
def get_customer_orders(seller_id):
    script = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, count, sum as fsum
import json
spark = SparkSession.builder.appName("CustOrders").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
orders = spark.read.parquet("hdfs://namenode:9000/data/refined/orders/")
returns = spark.read.parquet("hdfs://namenode:9000/data/refined/returns/")
seller_orders = orders.filter(col("seller_id") == "{seller_id}")
seller_orders = seller_orders.withColumn("cid", sha2(col("customer_id"),256).substr(1,8))
order_agg = seller_orders.groupBy("cid").agg(
    count("order_id").alias("total_orders"),
    fsum("amount").alias("total_amount")
)
ret_by_order = returns.filter(col("seller_id") == "{seller_id}").select("order_id")
seller_with_returns = seller_orders.join(ret_by_order, "order_id", "left_semi")
ret_agg = seller_with_returns.groupBy("cid").agg(count("order_id").alias("total_returns"))
result = order_agg.join(ret_agg, "cid", "left").fillna(0)
data = []
for row in result.collect():
    d = {{}}
    for k,v in row.asDict().items():
        d[k] = v.item() if hasattr(v,'item') else v
    data.append(d)
print("CUST_JSON:" + json.dumps(data))
spark.stop()
"""
    try:
        data = run_spark(script, "CUST_JSON:")
        if data is not None:
            return jsonify({"success": True, "data": data})
        return jsonify({"success": False, "data": []})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "data": []})

@app.route('/api/order', methods=['POST'])
def send_order():
    data = request.json
    order = {
        "order_id": f"O_LIVE_{random.randint(10000,99999)}",
        "seller_id": data.get('seller_id'),
        "customer_id": f"C_{random.randint(100,999)}",
        "amount": float(data.get('amount', 0)),
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "status": data.get('status', 'completed'),
        "category": data.get('category', 'electronics')
    }
    kafka_ok = try_kafka('orders', order)
    events_log.append({"type": "ORDER", "data": order, "kafka": kafka_ok})
    return jsonify({"success": True, "event": order, "kafka_sent": kafka_ok})

@app.route('/api/return', methods=['POST'])
def send_return():
    data = request.json
    ret = {
        "return_id": f"R_LIVE_{random.randint(10000,99999)}",
        "order_id": f"O_{random.randint(1000,9999)}",
        "seller_id": data.get('seller_id'),
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "reason": data.get('reason', 'damaged_item'),
        "status": "approved"
    }
    kafka_ok = try_kafka('returns', ret)
    events_log.append({"type": "RETURN", "data": ret, "kafka": kafka_ok})
    return jsonify({"success": True, "event": ret, "kafka_sent": kafka_ok})

@app.route('/api/complaint', methods=['POST'])
def send_complaint():
    data = request.json
    complaint = {
        "complaint_id": f"CMP_LIVE_{random.randint(10000,99999)}",
        "seller_id": data.get('seller_id'),
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "severity": data.get('severity', 'medium'),
        "category": data.get('category', 'poor_quality')
    }
    kafka_ok = try_kafka('complaints', complaint)
    events_log.append({"type": "COMPLAINT", "data": complaint, "kafka": kafka_ok})
    return jsonify({"success": True, "event": complaint, "kafka_sent": kafka_ok})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify({
        "total": len(events_log),
        "orders": len([e for e in events_log if e['type'] == 'ORDER']),
        "returns": len([e for e in events_log if e['type'] == 'RETURN']),
        "complaints": len([e for e in events_log if e['type'] == 'COMPLAINT']),
    })

if __name__ == '__main__':
    print("API running on port 5000")
    print("Endpoints: /api/sellers /api/refresh /api/customer_orders/<id> /api/order /api/return /api/complaint")
    app.run(host='0.0.0.0', port=5000, debug=False)

@app.route('/api/pipeline', methods=['POST'])
def run_pipeline():
    """Run the full Spark pipeline and refresh data"""
    try:
        import subprocess
        steps = [
            'normalize_orders',
            'normalize_returns', 
            'normalize_complaints',
            'normalize_sellers',
            'seller_aggregates',
            'risk_scoring'
        ]
        for step in steps:
            print(f"Running {step}...")
            subprocess.run([
                'docker', 'exec', 'spark-master',
                '/spark/bin/spark-submit',
                '--master', 'spark://spark-master:7077',
                f'/opt/spark_jobs/{step}.py'
            ], capture_output=True, text=True, timeout=120)

        # Refresh cache after pipeline
        refresh_script = """
from pyspark.sql import SparkSession
import json
spark = SparkSession.builder.appName("Refresh").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("hdfs://namenode:9000/data/curated/seller_risk/")
data = []
for row in df.collect():
    d = {}
    for k,v in row.asDict().items():
        d[k] = v.item() if hasattr(v,'item') else v
    data.append(d)
with open('/tmp/sellers_cache.json','w') as f:
    json.dump(data, f)
print("REFRESH_DONE:true")
spark.stop()
"""
        with open('/tmp/refresh_after.py', 'w') as f:
            f.write(refresh_script)
        subprocess.run(['docker','cp','/tmp/refresh_after.py','spark-master:/tmp/refresh_after.py'])
        subprocess.run([
            'docker','exec','spark-master',
            '/spark/bin/spark-submit',
            '--master','spark://spark-master:7077',
            '/tmp/refresh_after.py'
        ], capture_output=True, text=True, timeout=120)
        subprocess.run(['docker','cp','spark-master:/tmp/sellers_cache.json','/tmp/sellers_cache.json'])

        return jsonify({"success": True, "message": "Pipeline completed! Data refreshed."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
