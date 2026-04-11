from __future__ import annotations

import datetime as dt
import logging
import time
from typing import List

from psycopg2 import sql

from simulator.common import build_dsn, connect_with_retry, safe_db_name, safe_table_name
from simulator.customer_churn import customers, orders


def _ensure_database_exists(config) -> None:
    admin_db_name = safe_db_name(config.postgres_admin_db)
    target_db_name = safe_db_name(config.simulator_postgres_db)
    conn = connect_with_retry(build_dsn(config, admin_db_name), autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db_name,))
            if cur.fetchone() is None:
                logging.info("Creating simulator database %s", target_db_name)
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db_name)))
    finally:
        conn.close()


def _row_count(cur, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _load_customer_profiles(cur, customers_table: str) -> List[customers.CustomerProfile]:
    cur.execute(
        f"""
        SELECT customer_id, signup_date, segment, is_active
        FROM {customers_table}
        ORDER BY customer_id
        """
    )
    rows = cur.fetchall()
    return [
        {
            "customer_id": row[0],
            "signup_date": row[1],
            "segment": row[2],
            "is_active": row[3],
        }
        for row in rows
    ]


def _load_customer_metrics(
    cur,
    customers_table: str,
    orders_table: str,
    snapshot_date: dt.date,
) -> None:
    cur.execute(
        f"""
        SELECT
            c.customer_id,
            MAX(o.order_ts)::date AS last_order_date,
            COUNT(*) FILTER (
                WHERE o.order_ts >= %s::date - INTERVAL '30 day'
                  AND o.order_ts < %s::date + INTERVAL '1 day'
            ) AS recent_orders,
            COUNT(*) FILTER (
                WHERE o.order_ts >= %s::date - INTERVAL '60 day'
                  AND o.order_ts < %s::date - INTERVAL '30 day'
            ) AS prior_orders,
            AVG(o.total_amount) FILTER (
                WHERE o.order_ts >= %s::date - INTERVAL '30 day'
                  AND o.order_ts < %s::date + INTERVAL '1 day'
            ) AS recent_avg_amount,
            AVG(o.total_amount) FILTER (
                WHERE o.order_ts >= %s::date - INTERVAL '60 day'
                  AND o.order_ts < %s::date - INTERVAL '30 day'
            ) AS prior_avg_amount
        FROM {customers_table} c
        LEFT JOIN {orders_table} o ON o.customer_id = c.customer_id
        GROUP BY c.customer_id
        """,
        (
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
        ),
    )
    # Keep this query as a lightweight quality check that order data is queryable.
    # Label generation is intentionally handled by another system.
    _ = cur.fetchall()


def _next_customer_index(customer_profiles: List[customers.CustomerProfile]) -> int:
    return len(customer_profiles) + 1


def _next_order_index(cur, orders_table: str) -> int:
    return _row_count(cur, orders_table) + 1


def _bootstrap_data(
    conn,
    config,
    customers_table: str,
    orders_table: str,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    with conn.cursor() as cur:
        customer_count = _row_count(cur, customers_table)
        order_count = _row_count(cur, orders_table)

        if customer_count == 0:
            seed_customer_rows = customers.generate_batch(1, config.churn_seed_customers, now.date())
            customers.insert_batch(cur, customers_table, seed_customer_rows)
            logging.info("Bootstrapped %s customers into %s", len(seed_customer_rows), customers_table)

        customer_profiles = _load_customer_profiles(cur, customers_table)

        if order_count == 0:
            seed_order_rows = orders.generate_batch(
                customer_profiles,
                start_index=1,
                batch_size=max(config.churn_order_batch_size * 8, len(customer_profiles) * 4),
                start_ts=now - dt.timedelta(days=config.churn_seed_days),
                end_ts=now,
            )
            orders.insert_batch(cur, orders_table, seed_order_rows)
            logging.info("Bootstrapped %s orders into %s", len(seed_order_rows), orders_table)

        _load_customer_metrics(cur, customers_table, orders_table, now.date())

    conn.commit()


def run(config) -> None:
    customers_table = safe_table_name(config.churn_customers_table)
    orders_table = safe_table_name(config.churn_orders_table)

    _ensure_database_exists(config)
    target_db_name = safe_db_name(config.simulator_postgres_db)
    logging.info("Connecting to Postgres at %s:%s/%s", config.postgres_host, config.postgres_port, target_db_name)
    conn = connect_with_retry(build_dsn(config, target_db_name))

    with conn.cursor() as cur:
        customers.create_table(cur, customers_table)
        orders.create_table(cur, orders_table)
    conn.commit()

    _bootstrap_data(conn, config, customers_table, orders_table)

    logging.info(
        "Starting churn generator: db=%s customers_table=%s orders_table=%s batch_size=%s order_batch_size=%s interval_seconds=%s",
        target_db_name,
        customers_table,
        orders_table,
        config.batch_size,
        config.churn_order_batch_size,
        config.interval_seconds,
    )

    while True:
        now = dt.datetime.now(dt.timezone.utc)
        with conn.cursor() as cur:
            customer_profiles = _load_customer_profiles(cur, customers_table)
            new_customer_rows = customers.generate_batch(
                _next_customer_index(customer_profiles),
                config.batch_size,
                now.date(),
            )
            customers.insert_batch(cur, customers_table, new_customer_rows)

            customer_profiles.extend(customers.rows_to_profiles(new_customer_rows))

            order_rows = orders.generate_batch(
                customer_profiles,
                start_index=_next_order_index(cur, orders_table),
                batch_size=config.churn_order_batch_size,
                start_ts=now - dt.timedelta(seconds=max(config.interval_seconds, 3600)),
                end_ts=now,
            )
            if order_rows:
                orders.insert_batch(cur, orders_table, order_rows)

            _load_customer_metrics(cur, customers_table, orders_table, now.date())

        conn.commit()
        logging.info(
            "Inserted %s customers and %s orders",
            len(new_customer_rows),
            len(order_rows),
        )
        time.sleep(config.interval_seconds)