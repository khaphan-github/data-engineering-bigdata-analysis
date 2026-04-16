from __future__ import annotations

import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
import random
from typing import Iterable, List, Optional, Sequence, Tuple

from psycopg2.extras import execute_values

from simulator.customer_churn.customers import CustomerProfile


ORDER_STATUSES = ["completed", "cancelled", "returned"]
PAYMENT_METHODS = ["cod", "bank_transfer", "card", "ewallet"]
PROMO_CODES = ["SAVE10", "LOYAL20", "FREESHIP", "VIP30"]

OrderRow = Tuple[
    str,
    str,
    dt.datetime,
    str,
    str,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    str,
    Optional[str],
]


def _quantize(amount: Decimal) -> Decimal:
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _pick_customer(customer_profiles: Sequence[CustomerProfile]) -> CustomerProfile:
    weights = []
    for profile in customer_profiles:
        segment_weight = {
            "new": 1.5,
            "regular": 2.5,
            "vip": 3.5,
        }.get(profile["segment"], 1.0)
        active_multiplier = 1.4 if profile["is_active"] else 0.35
        weights.append(segment_weight * active_multiplier)
    return random.choices(list(customer_profiles), weights=weights, k=1)[0]


def _random_order_ts(start_ts: dt.datetime, end_ts: dt.datetime) -> dt.datetime:
    total_seconds = max(int((end_ts - start_ts).total_seconds()), 1)
    return start_ts + dt.timedelta(seconds=random.randint(0, total_seconds))


def _generate_row(
    customer_profiles: Sequence[CustomerProfile],
    index: int,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
) -> OrderRow:
    customer = _pick_customer(customer_profiles)
    order_ts = _random_order_ts(start_ts, end_ts)
    order_status = random.choices(ORDER_STATUSES, weights=[0.92, 0.05, 0.03], k=1)[0]
    subtotal_amount = Decimal(random.randint(150_000, 1_200_000))
    discount_amount = Decimal(0)
    if random.random() < 0.4:
        discount_amount = _quantize(subtotal_amount * Decimal(random.choice([0.05, 0.10, 0.15, 0.20])))
    shipping_fee = Decimal(random.choice([0, 15000, 25000, 30000]))
    tax_amount = _quantize((subtotal_amount - discount_amount) * Decimal("0.08"))
    total_amount = _quantize(subtotal_amount - discount_amount + shipping_fee + tax_amount)
    promo_code = None if random.random() < 0.6 else random.choice(PROMO_CODES)

    return (
        f"ORD_{order_ts:%Y%m%d}_{index:08d}",
        customer["customer_id"],
        order_ts,
        order_status,
        "VND",
        _quantize(subtotal_amount),
        _quantize(discount_amount),
        _quantize(shipping_fee),
        tax_amount,
        total_amount,
        random.choice(PAYMENT_METHODS),
        promo_code,
    )


def generate_batch(
    customer_profiles: Sequence[CustomerProfile],
    start_index: int,
    batch_size: int,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
) -> List[OrderRow]:
    if not customer_profiles:
        return []
    return [
        _generate_row(customer_profiles, start_index + offset, start_ts, end_ts)
        for offset in range(batch_size)
    ]


def create_table(cur, table_name: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_ts TIMESTAMPTZ NOT NULL,
            order_status TEXT NOT NULL,
            currency TEXT NOT NULL,
            subtotal_amount NUMERIC(18,2) NOT NULL,
            discount_amount NUMERIC(18,2) NOT NULL,
            shipping_fee NUMERIC(18,2) NOT NULL,
            tax_amount NUMERIC(18,2) NOT NULL,
            total_amount NUMERIC(18,2) NOT NULL,
            payment_method TEXT NOT NULL,
            promo_code TEXT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_{table_name}_customer_ts ON {table_name} (customer_id, order_ts);
        """
    )


def insert_batch(cur, table_name: str, rows: Iterable[OrderRow]) -> None:
    sql = (
        f"INSERT INTO {table_name} "
        "(order_id, customer_id, order_ts, order_status, currency, subtotal_amount, discount_amount, shipping_fee, tax_amount, total_amount, payment_method, promo_code) "
        "VALUES %s"
    )
    execute_values(cur, sql, rows, page_size=500)