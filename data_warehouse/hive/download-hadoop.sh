#!/usr/bin/env bash
set -euo pipefail

HADOOP_VERSION="${HADOOP_VERSION:-3.3.6}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DEPS_DIR="${DEPS_DIR:-${SCRIPT_DIR}/deps}"

mkdir -p "${DEPS_DIR}"

OUTPUT="${DEPS_DIR}/hadoop-${HADOOP_VERSION}.tar.gz"

if [[ -s "${OUTPUT}" ]]; then
    echo "[skip] hadoop-${HADOOP_VERSION}.tar.gz already exists"
    exit 0
fi

curl -fL --retry 5 --retry-delay 2 --retry-all-errors \
    "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    -o "${OUTPUT}"
