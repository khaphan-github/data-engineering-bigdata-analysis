# Generate synthetic source data and write them to PostgreSQL.
"""
Runs all simulators in parallel:
1. ecommerce simulator: clickstream + generic ecommerce customer data.
2. churn simulator: customers + orders.
"""

from __future__ import annotations

import logging
import random
import threading

from env import load_config
from simulator.customer_churn import generator as churn_generator
from simulator.ecommerce import generator as ecommerce_generator


def _run_all_modes(config) -> None:
    legacy_thread = threading.Thread(target=ecommerce_generator.run, args=(config,), name="legacy-generator")
    churn_thread = threading.Thread(target=churn_generator.run, args=(config,), name="churn-generator")

    legacy_thread.start()
    churn_thread.start()

    # Keep container alive while both generators run continuously.
    legacy_thread.join()
    churn_thread.join()


def main() -> None:
    config = load_config()
    random.seed(config.simulator_random_seed)
    _run_all_modes(config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    main()
