FROM apache/airflow:2.8.1
# The only extra library the DAGs need: the Kafka client used by ingest_kafka_to_hdfs.
# Installed from a local wheel, so building the image needs no internet access.
COPY wheels/ /tmp/wheels/
RUN pip install --no-cache-dir --no-index /tmp/wheels/kafka_python-*.whl
