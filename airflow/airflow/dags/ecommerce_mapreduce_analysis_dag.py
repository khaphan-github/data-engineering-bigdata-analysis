from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_APP = "/opt/airflow/apps/ecommerce_mapreduce_analysis.py"

with DAG(
    dag_id="ecommerce_mapreduce_analysis",
    description="Run MapReduce-style analysis for ecommerce dataset on HDFS",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "mapreduce", "ecommerce"],
) as dag:
    run_mapreduce_analysis = BashOperator(
        task_id="run_mapreduce_analysis",
        bash_command=(
            "PYSPARK_PYTHON=python3 "
            "PYSPARK_DRIVER_PYTHON=python3 "
            "spark-submit "
            "--master ${SPARK_MASTER_URL:-spark://spark-master:7077} "
            "--conf spark.pyspark.python=python3 "
            "--conf spark.pyspark.driver.python=python3 "
            f"{SPARK_APP}"
        ),
    )

    run_mapreduce_analysis
