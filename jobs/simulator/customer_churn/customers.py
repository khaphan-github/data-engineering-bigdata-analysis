from __future__ import annotations

import datetime as dt
import random
from typing import Iterable, List, Optional, Tuple, TypedDict

from psycopg2.extras import execute_values


GENDERS = ["male", "female", "other", "unknown"]
CITIES = [
    "Ho Chi Minh",
    "Ha Noi",
    "Da Nang",
    "Can Tho",
    "Hai Phong",
    "Nha Trang",
]
ACQUISITION_CHANNELS = ["ads", "organic", "referral", "social"]
SEGMENTS = ["new", "regular", "vip"]


class CustomerProfile(TypedDict):
    customer_id: str
    signup_date: dt.date
    segment: str
    is_active: bool


CustomerRow = Tuple[str, dt.date, Optional[int], str, str, str, str, bool]


def _random_birth_year() -> Optional[int]:
    if random.random() < 0.1:
        return None
    return random.randint(1970, 2005)


def _random_segment() -> str:
    return random.choices(SEGMENTS, weights=[0.25, 0.6, 0.15], k=1)[0]


def _random_is_active(segment: str) -> bool:
    active_probability = {
        "new": 0.92,
        "regular": 0.88,
        "vip": 0.96,
    }[segment]
    return random.random() < active_probability


def _generate_row(index: int, reference_date: dt.date) -> CustomerRow:
    segment = _random_segment()
    signup_days_ago = random.randint(1, 720)
    signup_date = reference_date - dt.timedelta(days=signup_days_ago)
    return (
        f"CUST_{index:06d}",
        signup_date,
        _random_birth_year(),
        random.choice(GENDERS),
        random.choice(CITIES),
        random.choice(ACQUISITION_CHANNELS),
        segment,
        _random_is_active(segment),
    )


def generate_batch(start_index: int, batch_size: int, reference_date: dt.date) -> List[CustomerRow]:
    return [_generate_row(start_index + offset, reference_date) for offset in range(batch_size)]


def rows_to_profiles(rows: Iterable[CustomerRow]) -> List[CustomerProfile]:
    profiles: List[CustomerProfile] = []
    for row in rows:
        profiles.append(
            {
                "customer_id": row[0],
                "signup_date": row[1],
                "segment": row[6],
                "is_active": row[7],
            }
        )
    return profiles


def create_table(cur, table_name: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            customer_id TEXT PRIMARY KEY,
            signup_date DATE NOT NULL,
            birth_year INTEGER NULL,
            gender TEXT NOT NULL,
            city TEXT NOT NULL,
            acquisition_channel TEXT NOT NULL,
            segment TEXT NOT NULL,
            is_active BOOLEAN NOT NULL
        );
        """
    )


def insert_batch(cur, table_name: str, rows: Iterable[CustomerRow]) -> None:
    sql = (
        f"INSERT INTO {table_name} "
        "(customer_id, signup_date, birth_year, gender, city, acquisition_channel, segment, is_active) "
        "VALUES %s"
    )
    execute_values(cur, sql, rows, page_size=500)