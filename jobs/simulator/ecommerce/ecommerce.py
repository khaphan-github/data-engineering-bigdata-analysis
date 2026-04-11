from __future__ import annotations

import random
from typing import Iterable, List, Optional, Tuple

from psycopg2.extras import execute_values


FIRST_NAMES = [
    "John",
    "Emma",
    "William",
    "Olivia",
    "James",
    "Ava",
    "Benjamin",
    "Sophia",
    "Lucas",
    "Isabella",
    "Henry",
    "Mia",
    "Alexander",
    "Amelia",
    "Michael",
    "Charlotte",
    "Daniel",
    "Emily",
    "Matthew",
    "Abigail",
]
LAST_NAMES = [
    "Smith",
    "Johnson",
    "Brown",
    "Davis",
    "Miller",
    "Wilson",
    "Moore",
    "Taylor",
    "Anderson",
    "Thomas",
    "Jackson",
    "White",
    "Harris",
    "Martin",
    "Thompson",
    "Garcia",
    "Martinez",
    "Clark",
    "Lewis",
    "Young",
]
OCCUPATIONS = ["teacher", "businessman", "engineer", "retired", "student", "doctor"]


def _random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def _random_owns_car(income: int) -> str:
    if income >= 150000:
        return "yes" if random.random() < 0.8 else "no"
    return "yes" if random.random() < 0.45 else "no"


def _random_phone() -> Optional[str]:
    if random.random() < 0.2:
        return None
    return "09" + "".join(str(random.randint(0, 9)) for _ in range(8))


def _generate_record() -> Tuple[str, int, str, int, str, Optional[str]]:
    age = random.randint(20, 75)
    occupation = random.choice(OCCUPATIONS)
    income = random.randint(40000, 400000)
    return (_random_name(), age, occupation, income, _random_owns_car(income), _random_phone())


def generate_batch(batch_size: int) -> List[Tuple[str, int, str, int, str, Optional[str]]]:
    return [_generate_record() for _ in range(batch_size)]


def create_table(cur, table_name: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            occupation TEXT NOT NULL,
            income INTEGER NOT NULL,
            owns_car TEXT NOT NULL,
            phone_number TEXT NULL
        );
        """
    )


def insert_batch(cur, table_name: str, rows: Iterable[Tuple[str, int, str, int, str, Optional[str]]]) -> None:
    sql = (
        f"INSERT INTO {table_name} "
        "(name, age, occupation, income, owns_car, phone_number) "
        "VALUES %s"
    )
    execute_values(cur, sql, rows, page_size=500)
