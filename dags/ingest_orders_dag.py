from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'trust-safety-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

with DAG(
    dag_id='ingest_orders',
    default_args=default_args,
    description='Ingest orders and returns from Kafka into HDFS raw layer',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'orders']
) as dag:

    check_kafka = BashOperator(
        task_id='check_kafka_connection',
        bash_command='kafka-topics.sh --bootstrap-server kafka:9092 --list'
    )

    ingest_orders = BashOperator(
        task_id='ingest_orders_to_hdfs',
        bash_command="""
            kafka-console-consumer.sh \
            --bootstrap-server kafka:9092 \
            --topic orders \
            --timeout-ms 8000 \
            >> /tmp/orders_batch.json && \
            hdfs dfs -mkdir -p /data/raw/orders/ && \
            hdfs dfs -put -f /tmp/orders_batch.json \
            /data/raw/orders/orders_{{ ds_nodash }}_{{ ts_nodash }}.json
        """
    )

    ingest_returns = BashOperator(
        task_id='ingest_returns_to_hdfs',
        bash_command="""
            kafka-console-consumer.sh \
            --bootstrap-server kafka:9092 \
            --topic returns \
            --timeout-ms 8000 \
            >> /tmp/returns_batch.json && \
            hdfs dfs -mkdir -p /data/raw/returns/ && \
            hdfs dfs -put -f /tmp/returns_batch.json \
            /data/raw/returns/returns_{{ ds_nodash }}_{{ ts_nodash }}.json
        """
    )

    check_kafka >> ingest_orders >> ingest_returns