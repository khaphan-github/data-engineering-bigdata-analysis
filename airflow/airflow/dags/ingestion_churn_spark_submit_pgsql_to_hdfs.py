from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_APP = "/opt/airflow/apps/ingestion_churn_pgsql_to_hdfs.py"
POSTGRES_JAR = "/opt/airflow/jars/postgresql-42.7.3.jar"

with DAG(
    dag_id="ingestion_churn_spark_submit_pgsql_to_hdfs",
    description="Read churn source tables from Postgres and write to HDFS using Spark",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["spark", "postgres", "hdfs", "churn"],
) as dag:
    submit_churn_pgsql_to_hdfs = BashOperator(
        task_id="ingestion_churn_pgsql_to_hdfs",
        bash_command=(
            "PYSPARK_PYTHON=python3 "
            "PYSPARK_DRIVER_PYTHON=python3 "
            "spark-submit "
            "--master ${SPARK_MASTER_URL:-spark://ingest-spark-master:7077} "
            "--conf spark.pyspark.python=python3 "
            "--conf spark.pyspark.driver.python=python3 "
            f"--jars {POSTGRES_JAR} "
            f"{SPARK_APP}"
        ),
    )

    submit_churn_pgsql_to_hdfs
