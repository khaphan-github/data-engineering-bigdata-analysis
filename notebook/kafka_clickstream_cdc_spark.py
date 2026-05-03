from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructField, StructType, StringType


# Debezium message schema (JSON with schema+payload envelope)
payload_after_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("device", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("product_id", StringType(), True),
    ]
)

payload_schema = StructType(
    [
        StructField("before", payload_after_schema, True),
        StructField("after", payload_after_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True),
    ]
)

debezium_envelope_schema = StructType([StructField("payload", payload_schema, True)])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("clickstream-cdc-consumer")
        .master("spark://ingest-spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .option("subscribe", "srcs_ecommerce.public.clickstream")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = raw_stream.select(
        F.from_json(F.col("value").cast("string"), debezium_envelope_schema).alias("msg")
    )

    clickstream_events = (
        parsed.select(
            F.col("msg.payload.op").alias("op"),
            F.col("msg.payload.after.id").alias("id"),
            F.col("msg.payload.after.timestamp").alias("event_ts"),
            F.col("msg.payload.after.user_id").alias("user_id"),
            F.col("msg.payload.after.event_type").alias("event_type"),
            F.col("msg.payload.after.page_url").alias("page_url"),
            F.col("msg.payload.after.referrer").alias("referrer"),
            F.col("msg.payload.after.device").alias("device"),
            F.col("msg.payload.after.browser").alias("browser"),
            F.col("msg.payload.after.ip").alias("ip"),
            F.col("msg.payload.after.product_id").alias("product_id"),
        )
        .filter(F.col("id").isNotNull())
        .filter(F.col("op").isin("c", "u", "r"))
    )

    query = (
        clickstream_events.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "20")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
