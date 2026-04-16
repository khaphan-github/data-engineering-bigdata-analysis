from __future__ import annotations

import logging
import string
import time

import psycopg2


def safe_table_name(name: str) -> str:
    if not name or any(ch not in string.ascii_letters + string.digits + "_" for ch in name):
        raise ValueError("Invalid table name. Use only letters, numbers, and underscore.")
    return name


def safe_db_name(name: str) -> str:
    if not name or any(ch not in string.ascii_letters + string.digits + "_" for ch in name):
        raise ValueError("Invalid database name. Use only letters, numbers, and underscore.")
    return name


def build_dsn(config, db_name: str) -> str:
    return (
        f"host={config.postgres_host} "
        f"port={config.postgres_port} "
        f"dbname={db_name} "
        f"user={config.postgres_user} "
        f"password={config.postgres_password}"
    )


def connect_with_retry(dsn: str, retry_seconds: int = 5, autocommit: bool = False):
    while True:
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = autocommit
            return conn
        except psycopg2.OperationalError as exc:
            logging.warning("Postgres not ready (%s). Retrying in %s seconds...", exc, retry_seconds)
            time.sleep(retry_seconds)