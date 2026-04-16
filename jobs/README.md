# Jobs - Data Simulator

This module generates synthetic source-system data and writes them to PostgreSQL on a fixed schedule.

## Simulators

This service always runs both simulators in parallel:

1. Ecommerce simulator: generates clickstream and generic ecommerce customer data.
2. Customer churn simulator: generates churn source tables (`customers`, `orders`).

## What It Does

1. Creates the destination database and tables if they do not exist.
2. Every interval, generates a batch of records and inserts them into PostgreSQL.
3. Runs multiple simulator loops concurrently in one container.

## Environment Variables

The generator reads configuration from environment variables (with defaults):

- `POSTGRES_HOST` (default: `postgres`)
- `POSTGRES_PORT` (default: `5432`)
- `POSTGRES_DB` (default: `postgres`)
- `SIMULATOR_POSTGRES_DB` (default: fallback to `POSTGRES_DB`)
- `POSTGRES_ADMIN_DB` (default: `postgres`)
- `POSTGRES_USER` (default: `admin`)
- `POSTGRES_PASSWORD` (default: `admin`)
- `CLICKSTREAM_TABLE` (default: `clickstream`)
- `ECOMMERCE_TABLE` (default: `ecommerce`)
- `CHURN_CUSTOMERS_TABLE` (default: `customers`)
- `CHURN_ORDERS_TABLE` (default: `orders`)
- `BATCH_SIZE` (default: `100`)
- `INTERVAL_SECONDS` (default: `60`)
- `CHURN_SEED_CUSTOMERS` (default: `1000`)
- `CHURN_SEED_DAYS` (default: `120`)
- `CHURN_ORDER_BATCH_SIZE` (default: `250`)
- `BASE_CHURN_RATE` (default: `0.15`)
- `SIMULATOR_RANDOM_SEED` (default: `42`)

## Churn Behavior

The churn simulator:

1. Connects to `POSTGRES_ADMIN_DB` and creates `SIMULATOR_POSTGRES_DB` if needed.
2. Creates two churn tables: `customers` and `orders`.
3. Bootstraps historical customer and order data.

Churn labels are intentionally not generated here and should be computed by a separate system.

This keeps churn data isolated from the legacy clickstream/ecommerce demo tables.

Churn generator code lives under `simulator/customer_churn/`.

The service starts two continuous loops in parallel:

1. Ecommerce loop: writes clickstream + ecommerce tables into `POSTGRES_DB`.
2. Churn loop: writes customers + orders into `SIMULATOR_POSTGRES_DB`.

## Run With Docker Compose

If you added the `jobs` service to `docker-compose.yml`, you can start it with:

```bash
docker compose up -d jobs
```

To view logs:

```bash
docker compose logs -f jobs
```

## Local Run (Optional)

You can also run the generator locally:

```bash
pip install -r requirements.txt
python simulator_generate_data.py
```

To run locally, just configure environment variables and execute `python simulator_generate_data.py`.

## Data Schema

The clickstream table schema created in Postgres:

- `id` SERIAL PRIMARY KEY
- `timestamp` TIMESTAMPTZ
- `user_id` TEXT
- `event_type` TEXT
- `page_url` TEXT
- `referrer` TEXT
- `device` TEXT
- `browser` TEXT
- `ip` TEXT
- `product_id` TEXT

The ecommerce table schema created in Postgres:

- `id` INTEGER
- `name` TEXT
- `age` INTEGER
- `occupation` TEXT
- `income` INTEGER
- `owns_car` TEXT
- `phone_number` TEXT

## Churn Tables

The churn simulator creates these tables:

- `customers(customer_id, signup_date, birth_year, gender, city, acquisition_channel, segment, is_active)`
- `orders(order_id, customer_id, order_ts, order_status, currency, subtotal_amount, discount_amount, shipping_fee, tax_amount, total_amount, payment_method, promo_code)`

## Verify Inserts

Example checks after running churn mode:

```sql
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM orders;
```
