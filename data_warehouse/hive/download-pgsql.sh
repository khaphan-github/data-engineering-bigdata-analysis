#!/usr/bin/env bash
set -euo pipefail

POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION:-42.7.4}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DEPS_DIR="${DEPS_DIR:-${SCRIPT_DIR}/deps}"

mkdir -p "${DEPS_DIR}"

OUTPUT="${DEPS_DIR}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

if [[ -s "${OUTPUT}" ]]; then
    echo "[skip] postgresql-${POSTGRES_JDBC_VERSION}.jar already exists"
    exit 0
fi

curl -fL --retry 5 --retry-delay 2 --retry-all-errors \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o "${OUTPUT}"
