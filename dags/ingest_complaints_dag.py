from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import subprocess

default_args = {
    'owner': 'trust-safety-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

def load_complaints_to_hdfs():
    subprocess.run([
        'hdfs', 'dfs', '-mkdir', '-p', '/data/raw/complaints/'
    ])
    subprocess.run([
        'hdfs', 'dfs', '-put', '-f',
        '/opt/airflow/data/complaints.json',
        '/data/raw/complaints/complaints.json'
    ])
    print("Complaints loaded to HDFS raw layer.")

def load_sellers_to_hdfs():
    subprocess.run([
        'hdfs', 'dfs', '-mkdir', '-p', '/data/raw/sellers/'
    ])
    subprocess.run([
        'hdfs', 'dfs', '-put', '-f',
        '/opt/airflow/data/sellers.json',
        '/data/raw/sellers/sellers.json'
    ])
    print("Sellers loaded to HDFS raw layer.")

with DAG(
    dag_id='ingest_complaints_and_sellers',
    default_args=default_args,
    description='Ingest batch complaints and seller profiles into HDFS raw layer',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'complaints', 'sellers']
) as dag:

    load_complaints = PythonOperator(
        task_id='load_complaints_to_hdfs',
        python_callable=load_complaints_to_hdfs
    )

    load_sellers = PythonOperator(
        task_id='load_sellers_to_hdfs',
        python_callable=load_sellers_to_hdfs
    )

    load_complaints >> load_sellers