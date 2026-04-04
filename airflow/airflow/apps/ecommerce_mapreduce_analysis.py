from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _to_age_group(age: int) -> str:
    if age < 30:
        return "<30"
    if age < 45:
        return "[30,45)"
    if age < 60:
        return "[45,60)"
    return ">=60"


def main() -> None:
    input_path = _env("ECOMMERCE_INPUT_PATH", "hdfs://namenode:8020/user/airflow/ecommerce")
    output_base = _env(
        "ECOMMERCE_OUTPUT_BASE",
        "hdfs://namenode:8020/user/airflow/ecommerce_analysis",
    )
    output_a = f"{output_base}/a_businessman_300k_car_phone"
    output_b = f"{output_base}/b_age_groups"
    output_b_csv = f"{output_b}_csv"

    spark = SparkSession.builder.appName("ecommerce-mapreduce-analysis").getOrCreate()

    df = spark.read.parquet(input_path)
    required_columns = {"id", "name", "age", "occupation", "income", "owns_car", "phone_number"}
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns in input parquet: {missing}")

    # a) Trích xuất khách hàng businessman, income >= 300000, có ô tô, có số điện thoại.
    result_a = (
        df.filter(F.lower(F.col("occupation")) == F.lit("businessman"))
        .filter(F.col("income") >= F.lit(300000))
        .filter(F.lower(F.col("owns_car")).isin("yes", "y", "true", "1"))
        .filter(F.col("phone_number").isNotNull())
        .filter(F.length(F.trim(F.col("phone_number"))) > 0)
        .select("id", "name", "age", "occupation", "income", "owns_car", "phone_number")
    )

    result_a.write.mode("overwrite").parquet(output_a)

    # b) MapReduce-style: map(age->group,1) + reduceByKey(sum)
    age_group_counts_rdd = (
        df.select("age")
        .rdd.map(lambda row: (_to_age_group(int(row["age"])), 1))
        .reduceByKey(lambda left, right: left + right)
    )

    result_b = spark.createDataFrame(age_group_counts_rdd, schema=["age_group", "customer_count"])
    result_b = result_b.orderBy(
        F.when(F.col("age_group") == "<30", F.lit(1))
        .when(F.col("age_group") == "[30,45)", F.lit(2))
        .when(F.col("age_group") == "[45,60)", F.lit(3))
        .otherwise(F.lit(4))
    )

    result_b.write.mode("overwrite").parquet(output_b)
    result_b.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_b_csv)

    count_a = result_a.count()
    print(f"[DONE] Output a: {output_a} | matched rows = {count_a}")
    print(f"[DONE] Output b parquet: {output_b}")
    print(f"[DONE] Output b csv: {output_b_csv}")

    spark.stop()


if __name__ == "__main__":
    main()
