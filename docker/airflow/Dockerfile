FROM apache/airflow:2.8.0

USER root

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

RUN pip install \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka \
    kafka-python \
    pyspark==3.3.0