#!/usr/bin/env bash
set -euo pipefail

# Ensure Airflow folders exist with permissive access for mounted volumes
mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/output /opt/airflow/jars
chmod -R 777 /opt/airflow/logs /opt/airflow/output || true

# If user-provided deps exist, copy jars into the runtime jars folder
if [ -d /opt/airflow/deps ]; then
  find /opt/airflow/deps -maxdepth 1 -type f -name "*.jar" -exec cp -n {} /opt/airflow/jars/ \; || true
fi

# Hand off to the official Airflow entrypoint
exec /entrypoint "$@"
