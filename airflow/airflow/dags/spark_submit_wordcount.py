from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_APP = "/opt/airflow/apps/wordcount.py"

with DAG(
    dag_id="spark_submit_wordcount",
    description="Submit a Spark job from Airflow to the standalone cluster",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark"],
) as dag:
    submit_wordcount = BashOperator(
        task_id="submit_wordcount",
        bash_command=(
            "spark-submit "
            "--master ${SPARK_MASTER_URL:-spark://spark-master:7077} "
            f"{SPARK_APP}"
        ),
    )

    submit_wordcount
