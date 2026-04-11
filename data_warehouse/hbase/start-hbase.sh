#!/usr/bin/env bash
set -euo pipefail

export HBASE_HOME=${HBASE_HOME:-/opt/hbase}
mkdir -p /opt/hbase/logs

export HBASE_MANAGES_ZK=false

# Single-container startup with external ZooKeeper service.
${HBASE_HOME}/bin/hbase-daemon.sh start master
${HBASE_HOME}/bin/hbase-daemon.sh start regionserver

# Keep container alive while HBase services run.
tail -F /opt/hbase/logs/*
