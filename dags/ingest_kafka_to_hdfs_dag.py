from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TOPICS = ['orders', 'returns', 'complaints']

default_args = {
    'owner': 'trust-safety-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


def ingest_topic_to_hdfs(topic):
    """Read new messages from one Kafka topic and write them to HDFS raw as one JSON-lines file."""
    import json
    import urllib.error
    import urllib.request
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='kafka:29092',          # internal Docker listener
        group_id=f'airflow-{topic}-consumer-v1',   # Kafka remembers what this group already read
        auto_offset_reset='earliest',
        enable_auto_commit=False,                  # commit manually, only after HDFS write succeeds
        consumer_timeout_ms=15000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
    )

    messages = []
    for message in consumer:
        if message.value is not None:
            messages.append(message.value)
        if len(messages) >= 5000:
            break

    if not messages:
        consumer.close()
        print(f"[{topic}] no new messages")
        return 0

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    hdfs_path = f"/data/raw/{topic}/{topic}_airflow_{timestamp}.json"
    body = ''.join(json.dumps(m) + '\n' for m in messages).encode('utf-8')

    # WebHDFS step 1: the namenode answers with a redirect to a datanode
    create_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
    try:
        urllib.request.urlopen(urllib.request.Request(create_url, method='PUT'))
        raise RuntimeError("WebHDFS did not redirect to a datanode")
    except urllib.error.HTTPError as error:
        if error.code != 307:
            raise
        datanode_url = error.headers['Location']

    # WebHDFS step 2: upload the data to the datanode
    upload = urllib.request.Request(datanode_url, data=body, method='PUT')
    upload.add_header('Content-Type', 'application/octet-stream')
    urllib.request.urlopen(upload)

    consumer.commit()   # mark messages as read only now that they are safe in HDFS
    consumer.close()
    print(f"[{topic}] wrote {len(messages)} messages to {hdfs_path}")
    return len(messages)


with DAG(
    dag_id='ingest_kafka_to_hdfs',
    default_args=default_args,
    description='Stream ingestion: Kafka topics (orders, returns, complaints) -> HDFS raw layer',
    schedule='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'kafka'],
) as dag:
    for topic in TOPICS:
        PythonOperator(
            task_id=f'ingest_{topic}_to_hdfs',
            python_callable=ingest_topic_to_hdfs,
            op_kwargs={'topic': topic},
        )
