from __future__ import annotations

import logging
import time

from simulator.ecommerce import clickstream, ecommerce
from simulator.common import build_dsn, connect_with_retry, safe_table_name


def run(config) -> None:
    clickstream_table_name = safe_table_name(config.table_name)
    ecommerce_table_name = safe_table_name(config.ecommerce_table_name)

    logging.info("Connecting to Postgres at %s:%s/%s", config.postgres_host, config.postgres_port, config.postgres_db)
    conn = connect_with_retry(build_dsn(config, config.postgres_db))

    with conn.cursor() as cur:
        clickstream.create_table(cur, clickstream_table_name)
        ecommerce.create_table(cur, ecommerce_table_name)
        conn.commit()

    logging.info(
        "Starting legacy generator: clickstream_table=%s ecommerce_table=%s batch_size=%s interval_seconds=%s",
        clickstream_table_name,
        ecommerce_table_name,
        config.batch_size,
        config.interval_seconds,
    )

    while True:
        clickstream_rows = clickstream.generate_batch(config.batch_size)
        ecommerce_rows = ecommerce.generate_batch(config.batch_size)
        with conn.cursor() as cur:
            clickstream.insert_batch(cur, clickstream_table_name, clickstream_rows)
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
