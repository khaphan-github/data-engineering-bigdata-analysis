# Jobs - Clickstream Generator

This module generates synthetic clickstream data and writes it to PostgreSQL on a fixed schedule.

## What It Does
1. Creates the clickstream table if it does not exist.
2. Every interval, generates a batch of records and inserts them into PostgreSQL.

## Environment Variables
The generator reads configuration from environment variables (with defaults):

- `POSTGRES_HOST` (default: `postgres`)
- `POSTGRES_PORT` (default: `5432`)
- `POSTGRES_DB` (default: `postgres`)
- `POSTGRES_USER` (default: `admin`)
- `POSTGRES_PASSWORD` (default: `admin`)
- `CLICKSTREAM_TABLE` (default: `clickstream`)
- `BATCH_SIZE` (default: `100`)
- `INTERVAL_SECONDS` (default: `60`)

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

## Data Schema
The table schema created in Postgres:

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
