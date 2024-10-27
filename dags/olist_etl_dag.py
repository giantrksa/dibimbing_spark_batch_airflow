from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}

dag = DAG(
    dag_id="olist_etl_analysis",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Olist ETL and Analysis Pipeline",
    start_date=days_ago(1),
)

process_olist_data = SparkSubmitOperator(
    application="/spark-scripts/olist_etl_analysis.py",
    conn_id="spark_main",
    task_id="process_olist_data",
    jars="/spark-scripts/jars/postgresql-42.2.20.jar",
    dag=dag,
)

process_olist_data