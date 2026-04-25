#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

echo "Downloading Hadoop, Hive, and PostgreSQL concurrently..."
echo ""

"${SCRIPT_DIR}/download-hive.sh" &
"${SCRIPT_DIR}/download-hadoop.sh" &
"${SCRIPT_DIR}/download-pgsql.sh" &

echo "[download-hive] started in background"
echo "[download-hadoop] started in background"
echo "[download-pgsql] started in background"
echo ""
echo "Waiting for all downloads to complete..."

wait

echo ""
echo "All downloads complete!"
echo "Local build artifacts are ready in: ${SCRIPT_DIR}/deps"
echo "Next step: docker compose build --no-cache dtwarehouse-metastore"
