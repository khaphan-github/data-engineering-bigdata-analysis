#!/usr/bin/env bash
set -euo pipefail

export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
export HIVE_HOME=${HIVE_HOME:-/opt/hive}
export HIVE_CONF_DIR=${HIVE_CONF_DIR:-/opt/hive/conf}
export HIVE_SERVICE_ROLE=${HIVE_SERVICE_ROLE:-hiveserver2}

mkdir -p /tmp/hive /warehouse

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local retries="${3:-60}"

  for i in $(seq 1 "$retries"); do
    if timeout 2 bash -lc "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  echo "Timed out waiting for ${host}:${port}" >&2
  return 1
}

ensure_hdfs_dirs() {
  wait_for_tcp dists-hdfs-namenode 8020 60

  for i in {1..60}; do
    if ${HADOOP_HOME}/bin/hdfs dfs -ls / >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done

  ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /tmp /tmp/hive /user/hive/warehouse
  ${HADOOP_HOME}/bin/hdfs dfs -chmod 1777 /tmp /tmp/hive
  ${HADOOP_HOME}/bin/hdfs dfs -chmod 777 /user/hive/warehouse
}

case "${HIVE_SERVICE_ROLE}" in
  metastore)
    wait_for_tcp dists-hive-metastore-db 5432 60

    # Initialize schema once; retries remain safe because schematool -info guards it.
    if ! ${HIVE_HOME}/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
      ${HIVE_HOME}/bin/schematool -dbType postgres -initSchema
    fi

    exec ${HIVE_HOME}/bin/hive --service metastore
    ;;

  hiveserver2)
    wait_for_tcp dists-hive-metastore 9083 60
    ensure_hdfs_dirs

    exec ${HIVE_HOME}/bin/hiveserver2
    ;;

  *)
    echo "Unsupported HIVE_SERVICE_ROLE=${HIVE_SERVICE_ROLE}. Expected: metastore | hiveserver2" >&2
    exit 1
    ;;
esac
