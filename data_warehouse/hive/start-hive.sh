#!/usr/bin/env bash
set -euo pipefail

export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
export HIVE_HOME=${HIVE_HOME:-/opt/hive}
export HIVE_CONF_DIR=${HIVE_CONF_DIR:-/opt/hive/conf}

mkdir -p /tmp/hive /warehouse

# Wait for PostgreSQL metastore DB.
for i in {1..30}; do
  if timeout 2 bash -lc "</dev/tcp/dtwarehouse-postgres/5432" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Initialize schema if needed (PostgreSQL).
if ! ${HIVE_HOME}/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  ${HIVE_HOME}/bin/schematool -dbType postgres -initSchema
fi

# Ensure HDFS directories required by HiveServer2 exist with proper permissions.
for i in {1..30}; do
  if ${HADOOP_HOME}/bin/hdfs dfs -ls / >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /tmp /tmp/hive /user/hive/warehouse
${HADOOP_HOME}/bin/hdfs dfs -chmod 1777 /tmp /tmp/hive
${HADOOP_HOME}/bin/hdfs dfs -chmod 777 /user/hive/warehouse

${HIVE_HOME}/bin/hive --service metastore &

for i in {1..30}; do
  if timeout 2 bash -lc "</dev/tcp/127.0.0.1/9083" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

${HIVE_HOME}/bin/hiveserver2
