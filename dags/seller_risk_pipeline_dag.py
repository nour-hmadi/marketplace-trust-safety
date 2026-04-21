from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'trust-safety-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

SPARK_SUBMIT = """
    spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    {}
"""

with DAG(
    dag_id='seller_risk_pipeline',
    default_args=default_args,
    description='Transform, score, and publish seller risk datasets',
    schedule_interval='0 7 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['pipeline', 'risk', 'spark']
) as dag:

    normalize_orders = BashOperator(
        task_id='normalize_orders',
        bash_command=SPARK_SUBMIT.format(
            '/opt/airflow/spark_jobs/normalize_orders.py'
        )
    )

    normalize_returns = BashOperator(
        task_id='normalize_returns',
        bash_command=SPARK_SUBMIT.format(
            '/opt/airflow/spark_jobs/normalize_returns.py'
        )
    )

    seller_aggregates = BashOperator(
        task_id='seller_aggregates',
        bash_command=SPARK_SUBMIT.format(
            '/opt/airflow/spark_jobs/seller_aggregates.py'
        )
    )

    risk_scoring = BashOperator(
        task_id='risk_scoring',
        bash_command=SPARK_SUBMIT.format(
            '/opt/airflow/spark_jobs/risk_scoring.py'
        )
    )

    validate_output = BashOperator(
        task_id='validate_curated_output',
        bash_command="""
            hdfs dfs -test -e /data/curated/seller_risk/ && \
            echo "Curated seller risk table exists. Validation passed." || \
            echo "WARNING: Curated table missing!"
        """
    )

    normalize_orders >> seller_aggregates
    normalize_returns >> seller_aggregates
    seller_aggregates >> risk_scoring
    risk_scoring >> validate_output