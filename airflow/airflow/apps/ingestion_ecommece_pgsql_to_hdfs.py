from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

'''
Workflow
File ingestion_ecommece_pgsql_to_hdfs.py là job Spark ETL dùng để ingest dữ liệu ecommerce từ PostgreSQL sang HDFS dạng Parquet.
Script hỗ trợ chạy full load lần đầu, sau đó chạy incremental load theo cột khóa tăng dần (mặc định id) bằng cơ chế watermark lưu ở HDFS (last_ingested_id.txt).
Nếu không có dữ liệu mới thì bỏ qua ghi file và không cập nhật watermark.
'''

def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _read_hdfs_text(spark: SparkSession, file_path: str) -> str | None:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(file_path)
    fs = path.getFileSystem(hadoop_conf)
    if not fs.exists(path):
        return None

    stream = fs.open(path)
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream))
    try:
        line = reader.readLine()
        return line.strip() if line else None
    finally:
        reader.close()
        stream.close()


def _write_hdfs_text(spark: SparkSession, file_path: str, content: str) -> None:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(file_path)
    fs = path.getFileSystem(hadoop_conf)

    parent = path.getParent()
    if parent and not fs.exists(parent):
        fs.mkdirs(parent)

    out = fs.create(path, True)
    writer = jvm.java.io.OutputStreamWriter(out)
    try:
        writer.write(content)
        writer.flush()
    finally:
        writer.close()
        out.close()


def main() -> None:
    host = _env("POSTGRES_HOST", "postgres")
    port = _env("POSTGRES_PORT", "5432")
    db = _env("POSTGRES_DB", "postgres")
    user = _env("POSTGRES_USER", "admin")
    password = _env("POSTGRES_PASSWORD", "admin")
    table = _env("ECOMMERCE_TABLE", "ecommerce")
    key_column = _env("INCREMENTAL_KEY_COLUMN", "id")

    output_path = _env(
        "HDFS_OUTPUT_PATH",
        "hdfs://namenode:8020/user/airflow/ecommerce",
    )
    last_update_path = _env(
        "HDFS_LAST_UPDATE_PATH",
        f"{output_path}/_metadata/last_ingested_id.txt",
    )

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

    spark = (
        SparkSession.builder.appName("airflow-pgsql-to-hdfs")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )

    last_ingested_id = _read_hdfs_text(spark, last_update_path)
    last_id_value: int | None = None
    if last_ingested_id:
        try:
            last_id_value = int(last_ingested_id)
        except ValueError as exc:
            raise ValueError(
                f"Invalid watermark value '{last_ingested_id}' in {last_update_path}. "
                "Expected an integer id."
            ) from exc

    if last_id_value is not None:
        source_query = (
            f"(SELECT * FROM {table} "
            f"WHERE {key_column} > {last_id_value}) AS src"
        )
        write_mode = "append"
        print(f"Incremental load from watermark id: {last_id_value}")
    else:
        source_query = table
        write_mode = "overwrite"
        print("No watermark found, running full load.")

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", source_query)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    if key_column not in df.columns:
        raise ValueError(
            f"Column '{key_column}' not found in source data. "
            "Set INCREMENTAL_KEY_COLUMN correctly."
        )

    if df.rdd.isEmpty():
        print("No new rows found. Skipping write and watermark update.")
        spark.stop()
        return

    df.write.mode(write_mode).parquet(output_path)

    latest = df.agg(F.max(F.col(key_column)).alias("max_key")).collect()[0]["max_key"]
    if latest is not None:
        _write_hdfs_text(spark, last_update_path, str(latest))
        print(f"Updated watermark id to: {latest}")

    spark.stop()


if __name__ == "__main__":
    main()
