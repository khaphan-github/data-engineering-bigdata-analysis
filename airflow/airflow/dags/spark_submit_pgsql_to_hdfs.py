from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_APP = "/opt/airflow/apps/pgsql_to_hdfs.py"
POSTGRES_JAR = "/opt/airflow/jars/postgresql-42.7.3.jar"

with DAG(
    dag_id="spark_submit_pgsql_to_hdfs",
    description="Read clickstream from Postgres and write to HDFS using Spark",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "postgres", "hdfs"],
) as dag:
    submit_pgsql_to_hdfs = BashOperator(
        task_id="submit_pgsql_to_hdfs",
        bash_command=(
            "spark-submit "
            "--master ${SPARK_MASTER_URL:-spark://spark-master:7077} "
            f"--jars {POSTGRES_JAR} "
            f"{SPARK_APP}"
        ),
    )

    submit_pgsql_to_hdfs
