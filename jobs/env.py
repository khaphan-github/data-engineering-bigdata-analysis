from __future__ import annotations

from dataclasses import dataclass
import os


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Config:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    simulator_postgres_db: str
    postgres_admin_db: str
    postgres_user: str
    postgres_password: str
    table_name: str
    ecommerce_table_name: str
    churn_customers_table: str
    churn_orders_table: str
    batch_size: int
    interval_seconds: int
    churn_seed_customers: int
    churn_seed_days: int
    churn_order_batch_size: int
    base_churn_rate: float
    simulator_random_seed: int


def load_config() -> Config:
    return Config(
        postgres_host=_get_env("POSTGRES_HOST", "postgres"),
        postgres_port=_get_int("POSTGRES_PORT", 5432),
        postgres_db=_get_env("POSTGRES_DB", "postgres"),
        simulator_postgres_db=_get_env("SIMULATOR_POSTGRES_DB", _get_env("POSTGRES_DB", "postgres")),
        postgres_admin_db=_get_env("POSTGRES_ADMIN_DB", "postgres"),
        postgres_user=_get_env("POSTGRES_USER", "admin"),
        postgres_password=_get_env("POSTGRES_PASSWORD", "admin"),
        table_name=_get_env("CLICKSTREAM_TABLE", "clickstream"),
        ecommerce_table_name=_get_env("ECOMMERCE_TABLE", "ecommerce"),
        churn_customers_table=_get_env("CHURN_CUSTOMERS_TABLE", "customers"),
        churn_orders_table=_get_env("CHURN_ORDERS_TABLE", "orders"),
        batch_size=_get_int("BATCH_SIZE", 100),
        interval_seconds=_get_int("INTERVAL_SECONDS", 60),
        churn_seed_customers=_get_int("CHURN_SEED_CUSTOMERS", 1000),
        churn_seed_days=_get_int("CHURN_SEED_DAYS", 120),
        churn_order_batch_size=_get_int("CHURN_ORDER_BATCH_SIZE", 250),
        base_churn_rate=_get_float("BASE_CHURN_RATE", 0.15),
        simulator_random_seed=_get_int("SIMULATOR_RANDOM_SEED", 42),
    )
