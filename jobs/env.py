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


@dataclass(frozen=True)
class Config:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    table_name: str
    batch_size: int
    interval_seconds: int


def load_config() -> Config:
    return Config(
        postgres_host=_get_env("POSTGRES_HOST", "postgres"),
        postgres_port=_get_int("POSTGRES_PORT", 5432),
        postgres_db=_get_env("POSTGRES_DB", "postgres"),
        postgres_user=_get_env("POSTGRES_USER", "admin"),
        postgres_password=_get_env("POSTGRES_PASSWORD", "admin"),
        table_name=_get_env("CLICKSTREAM_TABLE", "clickstream"),
        batch_size=_get_int("BATCH_SIZE", 100),
        interval_seconds=_get_int("INTERVAL_SECONDS", 60),
    )
