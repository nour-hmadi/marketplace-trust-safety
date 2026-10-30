"""Live status of each pipeline stage, read directly from each service's own API."""
from datetime import datetime

import requests
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

KAFKA = "localhost:9092"
AIRFLOW = "http://localhost:8081/api/v1"
AIRFLOW_AUTH = ("admin", "admin123")
WEBHDFS = "http://localhost:9870/webhdfs/v1"
SPARK_MASTER = "http://localhost:8082/json/"
TOPICS = ["orders", "returns", "complaints"]
INGEST_DAG = "ingest_orders"


def local_time(value):
    """Convert an ISO string (UTC) or epoch milliseconds to local time text."""
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        moment = datetime.fromtimestamp(value / 1000)
    else:
        moment = datetime.fromisoformat(value).astimezone()
    return moment.strftime("%Y-%m-%d %H:%M:%S")


# ---------- 1. KAFKA ----------
def kafka_status():
    """Messages kept in each topic, and how many Airflow has not ingested yet."""
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, request_timeout_ms=10000)
    admin = KafkaAdminClient(bootstrap_servers=KAFKA, request_timeout_ms=10000)
    rows = []
    try:
        for topic in TOPICS:
            partition = TopicPartition(topic, 0)
            start = consumer.beginning_offsets([partition])[partition]
            end = consumer.end_offsets([partition])[partition]
            committed = admin.list_consumer_group_offsets(f"airflow-{topic}-consumer-v1").get(partition)
            read_up_to = max(committed.offset if committed else start, start)
            rows.append({"topic": topic,
                         "messages in Kafka": end - start,
                         "waiting for Airflow": end - read_up_to})
    finally:
        consumer.close()
        admin.close()
    return rows


# ---------- 2. AIRFLOW ----------
def airflow_runs(limit=5):
    """Latest runs of the ingestion DAG."""
    response = requests.get(f"{AIRFLOW}/dags/{INGEST_DAG}/dagRuns", auth=AIRFLOW_AUTH,
                            params={"order_by": "-execution_date", "limit": limit}, timeout=10)
    response.raise_for_status()
    return [{"run": run["dag_run_id"], "state": run["state"],
             "started": local_time(run["start_date"]), "ended": local_time(run["end_date"])}
            for run in response.json()["dag_runs"]]


def airflow_next_run():
    """When Airflow will start the next scheduled ingestion."""
    response = requests.get(f"{AIRFLOW}/dags/{INGEST_DAG}", auth=AIRFLOW_AUTH, timeout=10)
    response.raise_for_status()
    return local_time(response.json().get("next_dagrun_create_after"))


def trigger_ingestion():
    """Start the ingestion DAG now, without waiting for the schedule."""
    response = requests.post(f"{AIRFLOW}/dags/{INGEST_DAG}/dagRuns", auth=AIRFLOW_AUTH, json={}, timeout=10)
    response.raise_for_status()
    return response.json()["dag_run_id"]


# ---------- 3 & 5. HDFS ----------
def hdfs_files(path):
    """Files in one HDFS folder, newest first. Empty list if the folder does not exist."""
    response = requests.get(f"{WEBHDFS}{path}", params={"op": "LISTSTATUS"}, timeout=10)
    if response.status_code == 404:
        return []
    response.raise_for_status()
    files = [f for f in response.json()["FileStatuses"]["FileStatus"] if f["type"] == "FILE"]
    files.sort(key=lambda f: f["modificationTime"], reverse=True)
    return [{"file": f["pathSuffix"], "size_kb": round(f["length"] / 1024, 1),
             "written": local_time(f["modificationTime"])} for f in files]


def hdfs_layer(layer):
    """One row per dataset folder in /data/<layer>: files, total size, last write."""
    response = requests.get(f"{WEBHDFS}/data/{layer}", params={"op": "LISTSTATUS"}, timeout=10)
    response.raise_for_status()
    folders = sorted(f["pathSuffix"] for f in response.json()["FileStatuses"]["FileStatus"]
                     if f["type"] == "DIRECTORY")
    rows = []
    for folder in folders:
        files = hdfs_files(f"/data/{layer}/{folder}")
        rows.append({"dataset": folder, "files": len(files),
                     "size_kb": round(sum(f["size_kb"] for f in files), 1),
                     "last write": files[0]["written"] if files else "-"})
    return rows


# ---------- 4. SPARK ----------
def spark_status():
    """Cluster health and the most recent Spark jobs."""
    data = requests.get(SPARK_MASTER, timeout=10).json()
    apps = sorted(data.get("completedapps", []), key=lambda a: a.get("starttime", 0), reverse=True)
    return {
        "status": data.get("status", "-"),
        "alive_workers": data.get("aliveworkers", len(data.get("workers", []))),
        "cores": data.get("cores", 0),
        "running_jobs": len(data.get("activeapps", [])),
        "recent_jobs": [{"job": a.get("name"), "state": a.get("state"),
                         "started": local_time(a.get("starttime")),
                         "duration_s": round(a.get("duration", 0) / 1000)} for a in apps[:8]],
    }


# ---------- PROCESSING PIPELINE (Airflow DAG seller_risk_pipeline) ----------
from urllib.parse import quote

PROCESSING_DAG = "seller_risk_pipeline"
PROCESSING_JOBS = ["normalize_orders", "normalize_returns", "normalize_complaints",
                   "normalize_sellers", "seller_aggregates", "risk_scoring", "customer_activity", "governance"]


def trigger_dag(dag_id):
    """Start a DAG now and return its run id."""
    response = requests.post(f"{AIRFLOW}/dags/{dag_id}/dagRuns", auth=AIRFLOW_AUTH, json={}, timeout=10)
    response.raise_for_status()
    return response.json()["dag_run_id"]


def latest_run_id(dag_id):
    response = requests.get(f"{AIRFLOW}/dags/{dag_id}/dagRuns", auth=AIRFLOW_AUTH,
                            params={"order_by": "-execution_date", "limit": 1}, timeout=10)
    response.raise_for_status()
    runs = response.json()["dag_runs"]
    return runs[0]["dag_run_id"] if runs else None


def dag_run(dag_id, run_id):
    response = requests.get(f"{AIRFLOW}/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}",
                            auth=AIRFLOW_AUTH, timeout=10)
    response.raise_for_status()
    run = response.json()
    return {"state": run["state"], "started": local_time(run["start_date"]),
            "ended": local_time(run["end_date"])}


def task_instances(dag_id, run_id):
    """State of every task in one run, keyed by task id."""
    response = requests.get(f"{AIRFLOW}/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances",
                            auth=AIRFLOW_AUTH, timeout=10)
    response.raise_for_status()
    return {t["task_id"]: t for t in response.json()["task_instances"]}


def task_log(dag_id, run_id, task_id, try_number):
    """Airflow log text of one task attempt."""
    base = f"{AIRFLOW}/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}/logs"
    for attempt in (try_number, try_number - 1):
        if attempt < 1:
            continue
        response = requests.get(f"{base}/{attempt}", auth=AIRFLOW_AUTH,
                                headers={"Accept": "text/plain"}, timeout=10)
        if response.ok:
            return response.text
    raise RuntimeError("log not available yet")
