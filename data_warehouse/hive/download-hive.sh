#!/usr/bin/env bash
set -euo pipefail

HIVE_VERSION="${HIVE_VERSION:-4.1.0}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DEPS_DIR="${DEPS_DIR:-${SCRIPT_DIR}/deps}"

mkdir -p "${DEPS_DIR}"

OUTPUT="${DEPS_DIR}/apache-hive-${HIVE_VERSION}-bin.tar.gz"

if [[ -s "${OUTPUT}" ]]; then
    echo "[skip] apache-hive-${HIVE_VERSION}-bin.tar.gz already exists"
    exit 0
fi

curl -fL --retry 5 --retry-delay 2 --retry-all-errors \
    "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
    -o "${OUTPUT}"
