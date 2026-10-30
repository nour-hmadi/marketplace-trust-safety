"""Backend API (Flask) for the Trust & Safety platform.

1. Event producer: receives orders, returns and complaints from Streamlit and publishes them to Kafka.
2. Spark job gateway: Airflow calls /api/spark/run/<job> to run one Spark job on the cluster.
   (The Airflow container has no Spark. In production, Apache Livy plays this role.)
"""
import json
import os
import random
import subprocess
import threading
import time
import uuid
from datetime import datetime

from flask import Flask, jsonify, request
from flask_cors import CORS
from kafka import KafkaProducer

app = Flask(__name__)
CORS(app)

KAFKA = "localhost:9092"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------- 1. EVENT PRODUCER → KAFKA ----------
_producer = None


def publish(topic, event):
    """Send one event to a Kafka topic and wait for Kafka's acknowledgement."""
    global _producer
    try:
        if _producer is None:
            _producer = KafkaProducer(bootstrap_servers=KAFKA, api_version=(0, 10, 1), request_timeout_ms=5000,
                                      value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        _producer.send(topic, value=event).get(timeout=10)
        return True
    except Exception as error:
        print(f"Kafka publish failed: {error}")
        _producer = None   # reconnect on the next event
        return False


def now():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def new_id(prefix):
    return f"{prefix}_LIVE_{uuid.uuid4().hex[:10]}"   # unique, so Spark never drops a real event as a duplicate


@app.route("/api/order", methods=["POST"])
def send_order():
    data = request.json
    event = {"order_id": new_id("O"), "seller_id": data.get("seller_id"),
             "customer_id": f"C{random.randint(100, 299)}",   # same range as the existing customers
             "amount": float(data.get("amount", 0)), "timestamp": now(),
             "status": data.get("status", "completed"), "category": data.get("category", "electronics")}
    return jsonify({"success": True, "event": event, "kafka_sent": publish("orders", event)})


@app.route("/api/return", methods=["POST"])
def send_return():
    data = request.json
    if not data.get("order_id"):
        return jsonify({"success": False, "error": "order_id is required: a return must belong to a real order"}), 400
    event = {"return_id": new_id("R"), "order_id": data["order_id"], "seller_id": data.get("seller_id"),
             "timestamp": now(), "reason": data.get("reason", "damaged_item"), "status": "approved"}
    return jsonify({"success": True, "event": event, "kafka_sent": publish("returns", event)})


@app.route("/api/complaint", methods=["POST"])
def send_complaint():
    data = request.json
    event = {"complaint_id": new_id("CMP"), "seller_id": data.get("seller_id"), "timestamp": now(),
             "severity": data.get("severity", "medium"), "category": data.get("category", "poor_quality")}
    return jsonify({"success": True, "event": event, "kafka_sent": publish("complaints", event)})


# ---------- 2. SPARK JOB GATEWAY (called by Airflow) ----------
SPARK_JOBS = ["normalize_orders", "normalize_returns", "normalize_complaints", "normalize_sellers",
              "seller_aggregates", "risk_scoring", "customer_activity", "governance", "register_hive_tables"]
SPARK_LOCK = threading.Lock()   # one Spark job at a time, to protect the laptop's RAM
SECRETS = ("MASKING_SALT",)     # passed to Spark jobs as environment variables, never stored in Git


def load_env_file(path=os.path.join(PROJECT_DIR, ".env")):
    """Read KEY=value lines from the local .env file (ignored by Git)."""
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def secret_env():
    load_env_file()
    return [flag for name in SECRETS if os.environ.get(name) for flag in ("-e", f"{name}={os.environ[name]}")]


@app.route("/api/spark/run/<job>", methods=["POST"])
def run_spark_job(job):
    if job not in SPARK_JOBS:   # allowlist: nobody can send an arbitrary command through this endpoint
        return jsonify({"success": False, "error": f"unknown job: {job}"}), 404
    with SPARK_LOCK:
        started = time.time()
        # deploy the repo version first: the repo is the source of truth
        subprocess.run(["docker", "cp", os.path.join(PROJECT_DIR, "spark_jobs", f"{job}.py"),
                        f"spark-master:/opt/spark_jobs/{job}.py"], check=True)
        result = subprocess.run(["docker", "exec", *secret_env(), "spark-master", "/spark/bin/spark-submit",
                                 "--master", "spark://spark-master:7077", f"/opt/spark_jobs/{job}.py"],
                                capture_output=True, text=True, timeout=900)
    lines = (result.stdout + result.stderr).splitlines()
    body = {"job": job, "success": result.returncode == 0, "exit_code": result.returncode,
            "seconds": round(time.time() - started), "log_tail": [l for l in lines if " INFO " not in l][-40:]}
    return jsonify(body), (200 if result.returncode == 0 else 500)


if __name__ == "__main__":
    print("Backend API on port 5000: /api/order /api/return /api/complaint /api/spark/run/<job>")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
