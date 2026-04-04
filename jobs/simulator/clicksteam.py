from __future__ import annotations

import datetime as dt
import random
import string
from typing import Iterable, List, Tuple

from psycopg2.extras import execute_values


EVENT_TYPES = ["click", "view", "add_to_cart", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
REFERRERS = ["/", "/search?q=giay", "/category/shoes", "/promo/sale"]


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


def generate_batch(batch_size: int) -> List[Tuple[str, str, str, str, str, str, str, str, str]]:
    return [_generate_record() for _ in range(batch_size)]


def create_table(cur, table_name: str) -> None:
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


def insert_batch(cur, table_name: str, rows: Iterable[Tuple[str, str, str, str, str, str, str, str, str]]) -> None:
    sql = (
        f"INSERT INTO {table_name} "
        "(timestamp, user_id, event_type, page_url, referrer, device, browser, ip, product_id) "
        "VALUES %s"
    )
    execute_values(cur, sql, rows, page_size=500)

