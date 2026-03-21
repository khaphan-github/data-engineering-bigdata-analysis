# Generate data then write them to postgresql.
"""
1. Create database table if not exist.
2. Scheduler: every interval generate batch_size records and write to PostgreSQL.
3. Data format click stream:
   {
      "timestamp": "2026-03-21T10:15:30Z",
      "user_id": "U123",
      "event_type": "click",
      "page_url": "/product/abc",
      "referrer": "/search?q=giay",
      "device": "mobile",
      "browser": "Chrome",
      "ip": "203.xxx.xxx.xxx",
      "product_id": "ABC123"
  }
"""

from __future__ import annotations

import datetime as dt
import logging
import random
import string
import time
from typing import Iterable, List, Tuple

import psycopg2
from psycopg2.extras import execute_values

from env import load_config


EVENT_TYPES = ["click", "view", "add_to_cart", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
REFERRERS = ["/", "/search?q=giay", "/category/shoes", "/promo/sale"]


def _safe_table_name(name: str) -> str:
    if not name or any(ch not in string.ascii_letters + string.digits + "_" for ch in name):
        raise ValueError("Invalid table name. Use only letters, numbers, and underscore.")
    return name


def _random_product_id() -> str:
    return "P" + "".join(random.choices(string.ascii_uppercase + string.digits, k=5))


def _random_user_id() -> str:
    return "U" + str(random.randint(100, 999))


def _random_ip() -> str:
    return f"203.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def _random_page_url(product_id: str) -> str:
    if random.random() < 0.7:
        return f"/product/{product_id.lower()}"
    return "/search?q=giay"


def _generate_record() -> Tuple[str, str, str, str, str, str, str, str, str]:
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    product_id = _random_product_id()
    event_type = random.choice(EVENT_TYPES)
    return (
        ts,
        _random_user_id(),
        event_type,
        _random_page_url(product_id),
        random.choice(REFERRERS),
        random.choice(DEVICES),
        random.choice(BROWSERS),
        _random_ip(),
        product_id,
    )


def _generate_batch(batch_size: int) -> List[Tuple[str, str, str, str, str, str, str, str, str]]:
    return [_generate_record() for _ in range(batch_size)]


def _create_table(cur, table_name: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            user_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            page_url TEXT NOT NULL,
            referrer TEXT NOT NULL,
            device TEXT NOT NULL,
            browser TEXT NOT NULL,
            ip TEXT NOT NULL,
            product_id TEXT NOT NULL
        );
        """
    )


def _insert_batch(cur, table_name: str, rows: Iterable[Tuple[str, str, str, str, str, str, str, str, str]]) -> None:
    sql = (
        f"INSERT INTO {table_name} "
        "(timestamp, user_id, event_type, page_url, referrer, device, browser, ip, product_id) "
        "VALUES %s"
    )
    execute_values(cur, sql, rows, page_size=500)


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
    table_name = _safe_table_name(config.table_name)

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
        _create_table(cur, table_name)
        conn.commit()

    logging.info(
        "Starting generator: table=%s batch_size=%s interval_seconds=%s",
        table_name,
        config.batch_size,
        config.interval_seconds,
    )

    while True:
        rows = _generate_batch(config.batch_size)
        with conn.cursor() as cur:
            _insert_batch(cur, table_name, rows)
        conn.commit()
        logging.info("Inserted %s rows into %s", len(rows), table_name)
        time.sleep(config.interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    main()
