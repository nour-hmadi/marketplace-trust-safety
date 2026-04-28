from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'trust-safety-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

def ingest_orders_from_kafka():
    import json
    import urllib.request
    from datetime import datetime
    from kafka import KafkaConsumer

    group_id = f'airflow-orders-{datetime.now().strftime("%Y%m%d%H%M%S")}'
    print(f"Using group_id: {group_id}")

    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers='kafka:29092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=group_id,
        consumer_timeout_ms=15000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
    )

    messages = []
    for msg in consumer:
        if msg.value is not None:
            messages.append(msg.value)
        if len(messages) >= 1000:
            break
    consumer.close()

    print(f"Read {len(messages)} messages from Kafka orders topic")

    if not messages:
        print("No messages found in Kafka")
        return

    batch_file = f"/tmp/orders_airflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(batch_file, 'w') as f:
        for msg in messages:
            f.write(json.dumps(msg) + '\n')

    hdfs_path = f"/data/raw/orders/orders_airflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
    req = urllib.request.Request(webhdfs_url, method='PUT')
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code == 307:
            redirect_url = e.headers['Location']
            with open(batch_file, 'rb') as f:
                data = f.read()
            put_req = urllib.request.Request(redirect_url, data=data, method='PUT')
            put_req.add_header('Content-Type', 'application/octet-stream')
            urllib.request.urlopen(put_req)
            print(f"Successfully written to HDFS: {hdfs_path}")
        else:
            raise

def ingest_returns_from_kafka():
    import json
    import urllib.request
    from datetime import datetime
    from kafka import KafkaConsumer

    group_id = f'airflow-returns-{datetime.now().strftime("%Y%m%d%H%M%S")}'
    print(f"Using group_id: {group_id}")

    consumer = KafkaConsumer(
        'returns',
        bootstrap_servers='kafka:29092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=group_id,
        consumer_timeout_ms=15000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
    )

    messages = []
    for msg in consumer:
        if msg.value is not None:
            messages.append(msg.value)
        if len(messages) >= 1000:
            break
    consumer.close()

    print(f"Read {len(messages)} messages from Kafka returns topic")

    if not messages:
        print("No messages found in Kafka returns topic")
        return

    batch_file = f"/tmp/returns_airflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(batch_file, 'w') as f:
        for msg in messages:
            f.write(json.dumps(msg) + '\n')

    hdfs_path = f"/data/raw/returns/returns_airflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
    req = urllib.request.Request(webhdfs_url, method='PUT')
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code == 307:
            redirect_url = e.headers['Location']
            with open(batch_file, 'rb') as f:
                data = f.read()
            put_req = urllib.request.Request(redirect_url, data=data, method='PUT')
            put_req.add_header('Content-Type', 'application/octet-stream')
            urllib.request.urlopen(put_req)
            print(f"Successfully written to HDFS: {hdfs_path}")
        else:
            raise

with DAG(
    dag_id='ingest_orders',
    default_args=default_args,
    description='Ingest orders and returns from Kafka into HDFS raw layer',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'orders']
) as dag:

    task_ingest_orders = PythonOperator(
        task_id='ingest_orders_to_hdfs',
        python_callable=ingest_orders_from_kafka,
    )

    task_ingest_returns = PythonOperator(
        task_id='ingest_returns_to_hdfs',
        python_callable=ingest_returns_from_kafka,
    )

    task_ingest_orders >> task_ingest_returns
