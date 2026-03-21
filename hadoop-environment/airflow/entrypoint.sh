#!/usr/bin/env bash
set -euo pipefail

# Ensure Airflow folders exist with permissive access for mounted volumes
mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/output /opt/airflow/jars
chmod -R 777 /opt/airflow/logs /opt/airflow/output || true


# Download Spark and PostgreSQL JDBC jar if not present
SPARK_VERSION="3.5.1"
HADOOP_VERSION="3"
POSTGRES_JAR="postgresql-42.7.3.jar"
POSTGRES_JAR_URL="https://jdbc.postgresql.org/download/${POSTGRES_JAR}"
SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
SPARK_TGZ_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

# Download Spark tgz if not present
if [ ! -f "/tmp/${SPARK_TGZ}" ]; then
  echo "Downloading Spark..."
  curl -fSL "$SPARK_TGZ_URL" -o "/tmp/${SPARK_TGZ}"
fi

# Download PostgreSQL JDBC jar if not present
if [ ! -f "/opt/airflow/jars/${POSTGRES_JAR}" ]; then
  echo "Downloading PostgreSQL JDBC driver..."
  curl -fSL "$POSTGRES_JAR_URL" -o "/opt/airflow/jars/${POSTGRES_JAR}"
fi

# Hand off to the official Airflow entrypoint
exec /entrypoint "$@"
