#!/bin/bash

# === Tez integration (from previous fix) ===
export TEZ_HOME=/opt/tez
export TEZ_CONF_DIR=${TEZ_HOME}/conf
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${TEZ_HOME}/*:${TEZ_HOME}/lib/*

# === NEW: Fix Java 17 + Hadoop 3.3.6 protobuf IllegalAccessError ===
export HADOOP_OPTS="${HADOOP_OPTS} \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens java.base/java.net=ALL-UNNAMED"

# Optional: also for HiveServer2 specifically
export HIVE_SERVER2_OPTS="${HIVE_SERVER2_OPTS} ${HADOOP_OPTS}"