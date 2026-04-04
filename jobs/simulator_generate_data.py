# Generate data then write them to postgresql.
"""
1. Create database table if not exist.
2. Scheduler: every interval generate batch_size records and write to PostgreSQL.
3. Generates and inserts:
   - clickstream records.
   - ecommerce customer records.
"""

from __future__ import annotations

import logging
import string
import time

import psycopg2

from env import load_config
from simulator import clicksteam, ecommerce


def _safe_table_name(name: str) -> str:
    if not name or any(ch not in string.ascii_letters + string.digits + "_" for ch in name):
        raise ValueError("Invalid table name. Use only letters, numbers, and underscore.")
    return name


def _connect_with_retry(dsn: str, retry_seconds: int = 5):
    while True:
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as exc:
            logging.warning("Postgres not ready (%s). Retrying in %s seconds...", exc, retry_seconds)
            time.sleep(retry_seconds)


def main() -> None:
    config = load_config()
    clickstream_table_name = _safe_table_name(config.table_name)
    ecommerce_table_name = _safe_table_name(config.ecommerce_table_name)

    dsn = (
        f"host={config.postgres_host} "
        f"port={config.postgres_port} "
        f"dbname={config.postgres_db} "
        f"user={config.postgres_user} "
        f"password={config.postgres_password}"
    )

    logging.info("Connecting to Postgres at %s:%s/%s", config.postgres_host, config.postgres_port, config.postgres_db)
    conn = _connect_with_retry(dsn)

    with conn.cursor() as cur:
        clicksteam.create_table(cur, clickstream_table_name)
        ecommerce.create_table(cur, ecommerce_table_name)
        conn.commit()

    logging.info(
        "Starting generator: clickstream_table=%s ecommerce_table=%s batch_size=%s interval_seconds=%s",
        clickstream_table_name,
        ecommerce_table_name,
        config.batch_size,
        config.interval_seconds,
    )

    while True:
        clickstream_rows = clicksteam.generate_batch(config.batch_size)
        ecommerce_rows = ecommerce.generate_batch(config.batch_size)
        with conn.cursor() as cur:
            clicksteam.insert_batch(cur, clickstream_table_name, clickstream_rows)
            ecommerce.insert_batch(cur, ecommerce_table_name, ecommerce_rows)
        conn.commit()
        logging.info(
            "Inserted %s rows into %s and %s rows into %s",
            len(clickstream_rows),
            clickstream_table_name,
            len(ecommerce_rows),
            ecommerce_table_name,
        )
        time.sleep(config.interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    main()
