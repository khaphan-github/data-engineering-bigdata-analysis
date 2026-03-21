from __future__ import annotations

import os

from pyspark.sql import SparkSession


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def main() -> None:
    host = _env("POSTGRES_HOST", "postgres")
    port = _env("POSTGRES_PORT", "5432")
    db = _env("POSTGRES_DB", "postgres")
    user = _env("POSTGRES_USER", "admin")
    password = _env("POSTGRES_PASSWORD", "admin")
    table = _env("CLICKSTREAM_TABLE", "clickstream")

    output_path = _env(
        "HDFS_OUTPUT_PATH",
        "hdfs://namenode:8020/user/airflow/clickstream",
    )

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

    spark = (
        SparkSession.builder.appName("airflow-pgsql-to-hdfs")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
