from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("airflow-wordcount").getOrCreate()

    data = [
        "hello world",
        "hello airflow",
        "spark submit via airflow",
    ]

    rdd = spark.sparkContext.parallelize(data)
    word_counts = (
        rdd.flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    df = word_counts.toDF(["word", "count"])
    df.write.mode("overwrite").json("/opt/airflow/output/airflow_wordcount")

    spark.stop()


if __name__ == "__main__":
    main()
