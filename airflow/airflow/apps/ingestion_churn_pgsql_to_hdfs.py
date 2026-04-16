from __future__ import annotations

import datetime as dt
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def _jdbc_read(spark: SparkSession, jdbc_url: str, dbtable: str, user: str, password: str):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def _ingest_customers(
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    customers_table: str,
    customers_base_path: str,
) -> None:
    run_dt = dt.date.today().isoformat()
    target_partition_path = f"{customers_base_path}/dt={run_dt}"

    customers_df = _jdbc_read(
        spark,
        jdbc_url=jdbc_url,
        dbtable=customers_table,
        user=user,
        password=password,
    )

    customers_df.write.mode("overwrite").parquet(target_partition_path)
    print(f"[DONE] customers snapshot -> {target_partition_path} | rows={customers_df.count()}")


def _ingest_orders_incremental(
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    orders_table: str,
    orders_base_path: str,
    orders_watermark_path: str,
) -> None:
    last_order_ts = _read_hdfs_text(spark, orders_watermark_path)

    if last_order_ts:
        dbtable = (
            f"(SELECT * FROM {orders_table} "
            f"WHERE order_ts > TIMESTAMPTZ '{last_order_ts}') AS src"
        )
        print(f"Incremental orders load from watermark order_ts={last_order_ts}")
    else:
        dbtable = orders_table
        print("No orders watermark found, running full load for orders.")

    orders_df = _jdbc_read(
        spark,
        jdbc_url=jdbc_url,
        dbtable=dbtable,
        user=user,
        password=password,
    )

    if orders_df.rdd.isEmpty():
        print("No new orders found. Skipping orders write and watermark update.")
        return

    orders_out = orders_df.withColumn("dt", F.date_format(F.col("order_ts"), "yyyy-MM-dd"))
    orders_out.write.mode("append").partitionBy("dt").parquet(orders_base_path)

    max_order_ts = orders_df.agg(F.max(F.col("order_ts")).alias("max_order_ts")).collect()[0]["max_order_ts"]
    if max_order_ts is not None:
        _write_hdfs_text(spark, orders_watermark_path, max_order_ts.isoformat())
        print(f"Updated orders watermark -> {max_order_ts.isoformat()}")

    print(f"[DONE] orders incremental append -> {orders_base_path} | rows={orders_df.count()}")


def main() -> None:
    host = _env("POSTGRES_HOST", "postgres")
    port = _env("POSTGRES_PORT", "5432")
    db = _env("SIMULATOR_POSTGRES_DB", "customer_churn")
    user = _env("POSTGRES_USER", "admin")
    password = _env("POSTGRES_PASSWORD", "admin")

    customers_table = _env("CHURN_CUSTOMERS_TABLE", "customers")
    orders_table = _env("CHURN_ORDERS_TABLE", "orders")

    customers_base_path = _env(
        "HDFS_CHURN_CUSTOMERS_PATH",
        "hdfs://namenode:8020/data/raw/churn/customers",
    )
    orders_base_path = _env(
        "HDFS_CHURN_ORDERS_PATH",
        "hdfs://namenode:8020/data/raw/churn/orders",
    )
    orders_watermark_path = _env(
        "HDFS_CHURN_ORDERS_WATERMARK_PATH",
        "hdfs://namenode:8020/data/raw/churn/_metadata/orders_last_order_ts.txt",
    )

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

    spark = (
        SparkSession.builder.appName("ingestion-churn-pgsql-to-hdfs")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )

    _ingest_customers(
        spark=spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        customers_table=customers_table,
        customers_base_path=customers_base_path,
    )

    _ingest_orders_incremental(
        spark=spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        orders_table=orders_table,
        orders_base_path=orders_base_path,
        orders_watermark_path=orders_watermark_path,
    )

    spark.stop()


if __name__ == "__main__":
    main()
