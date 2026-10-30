"""Processing pipeline: HDFS raw -> refined -> curated, orchestrated by Airflow.

Spark is not installed in the Airflow container, so each task asks the Flask Spark gateway
to run one job on the Spark cluster (the role Apache Livy plays in production).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

GATEWAY = os.environ.get("SPARK_GATEWAY", "http://172.18.0.1:5000/api/spark/run")

# Run order. In production the four normalize jobs would run in parallel;
# here they run one after another to protect the laptop's memory.
JOBS = [
    "normalize_orders",
    "normalize_returns",
    "normalize_complaints",
    "normalize_sellers",
    "seller_aggregates",
    "risk_scoring",
    "customer_activity",
]

default_args = {
    'owner': 'trust-safety-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15),
    'email_on_failure': False,
}


def run_spark_job(job):
    import requests

    response = requests.post(f"{GATEWAY}/{job}", timeout=900)
    body = response.json()
    for line in body.get("log_tail", []):
        print(line)
    print(f"{job}: success={body.get('success')} in {body.get('seconds')} s")
    if not body.get("success"):
        raise RuntimeError(f"Spark job {job} failed: {body.get('error') or body.get('exit_code')}")


with DAG(
    dag_id='seller_risk_pipeline',
    default_args=default_args,
    description='Spark processing: raw -> refined (clean, deduplicate) -> curated (seller risk scores)',
    schedule='0 7 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['processing', 'spark'],
) as dag:
    previous = None
    for job in JOBS:
        task = PythonOperator(task_id=job, python_callable=run_spark_job, op_kwargs={'job': job})
        if previous:
            previous >> task
        previous = task
