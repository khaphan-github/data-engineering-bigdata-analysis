FROM apache/airflow:2.10.2

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
COPY deps/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm -f /tmp/spark.tgz

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=/home/airflow/.local/bin:/opt/spark/bin:$PATH
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/python/lib/pyspark.zip

# PostgreSQL JDBC driver for Spark reads (provided locally)
USER root
RUN mkdir -p /opt/airflow/jars
COPY deps/postgresql-42.7.3.jar /opt/airflow/jars/postgresql-42.7.3.jar
USER airflow
