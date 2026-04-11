from __future__ import annotations

import datetime as dt
import random
from typing import Dict, Iterable, List, Optional, Tuple

from psycopg2.extras import execute_values

from simulator.customer_churn.customers import CustomerProfile


LabelMetrics = Dict[str, object]
LabelRow = Tuple[dt.date, str, int, Optional[str]]


def _days_since_last_order(last_order_date: Optional[dt.date], snapshot_date: dt.date) -> int:
    if last_order_date is None:
        return 9999
    return (snapshot_date - last_order_date).days


def _evaluate_label(
    snapshot_date: dt.date,
    profile: CustomerProfile,
    metrics: Optional[LabelMetrics],
    base_churn_rate: float,
) -> Tuple[int, Optional[str]]:
    last_order_date = None if metrics is None else metrics.get("last_order_date")
    recent_orders = 0 if metrics is None else int(metrics.get("recent_orders", 0))
    prior_orders = 0 if metrics is None else int(metrics.get("prior_orders", 0))
    recent_avg_amount = None if metrics is None else metrics.get("recent_avg_amount")
    prior_avg_amount = None if metrics is None else metrics.get("prior_avg_amount")

    reasons: List[str] = []
    score = 0
    days_since_last_order = _days_since_last_order(last_order_date, snapshot_date)

    if days_since_last_order >= 45:
        reasons.append("inactive")
        score += 2
    if not profile["is_active"]:
        reasons.append("inactive")
        score += 1
    if prior_orders >= 2 and recent_orders <= max(prior_orders // 2, 1):
        reasons.append("service_quality")
        score += 1
    if (
        prior_avg_amount is not None
        and recent_avg_amount is not None
        and float(recent_avg_amount) < float(prior_avg_amount) * 0.7
    ):
        reasons.append("price")
        score += 1
    if recent_orders == 0 and prior_orders > 0:
        reasons.append("inactive")
        score += 1

    if score < 2 and days_since_last_order >= 30 and random.random() < base_churn_rate * 0.35:
        reasons.append("inactive")
        score += 1
    if score < 2 and recent_orders == 0 and random.random() < base_churn_rate * 0.25:
        reasons.append("payment_issue")
        score += 1

    churn_30d = 1 if score >= 2 else 0
    churn_reason = reasons[0] if churn_30d else None
    return churn_30d, churn_reason


def generate_snapshot_rows(
    snapshot_date: dt.date,
    customer_profiles: Iterable[CustomerProfile],
    metrics_by_customer: Dict[str, LabelMetrics],
    base_churn_rate: float,
) -> List[LabelRow]:
    rows: List[LabelRow] = []
    for profile in customer_profiles:
        churn_30d, churn_reason = _evaluate_label(
            snapshot_date,
            profile,
            metrics_by_customer.get(profile["customer_id"]),
            base_churn_rate,
        )
        rows.append((snapshot_date, profile["customer_id"], churn_30d, churn_reason))
    return rows


def create_table(cur, table_name: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            snapshot_date DATE NOT NULL,
            customer_id TEXT NOT NULL,
            churn_30d INTEGER NOT NULL,
            churn_reason TEXT NULL,
            PRIMARY KEY (snapshot_date, customer_id)
        );
        CREATE INDEX IF NOT EXISTS idx_{table_name}_customer ON {table_name} (customer_id);
        """
    )


def upsert_batch(cur, table_name: str, rows: Iterable[LabelRow]) -> None:
    sql = (
        f"INSERT INTO {table_name} "
        "(snapshot_date, customer_id, churn_30d, churn_reason) VALUES %s "
        "ON CONFLICT (snapshot_date, customer_id) DO UPDATE SET "
        "churn_30d = EXCLUDED.churn_30d, churn_reason = EXCLUDED.churn_reason"
    )
    execute_values(cur, sql, rows, page_size=500)