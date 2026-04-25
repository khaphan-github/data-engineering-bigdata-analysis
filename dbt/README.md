# dbt (Data Build Tool)

## Problem

- Raw data in warehouse is messy and hard to use
- SQL queries are duplicated across reports
- No testing or data quality checks
- Hard to track data lineage and dependencies
- Transformation logic is hidden in BI tools or scripts

## Solution: When to Use dbt

Use dbt when you need to:
- Transform raw data into analytics-ready models
- Build reusable, version-controlled SQL models
- Add data quality tests
- Document your data transformation logic
- Collaborate with team on data pipelines

## Basic Commands

```bash
# Initialize dbt project
dbt init my_project

# Run all models
dbt run

# Run specific model
dbt run --model my_model

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Debug connection
dbt debug

# List all models
dbt ls
```

## Quick Example

```sql
-- models/staging_orders.sql
{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    total_amount,
    created_at
FROM raw.orders
```

Then run: `dbt run`

## Q&A

### Why not query the application database directly?

**Q: Why do we need to transform data from app DB to data warehouse? Why can't we just query the app database?**

**A:**
1. **Performance** - App DB is optimized for transactions, not complex analytical queries. Queries can take 10-30 seconds on app DB vs <1 second on warehouse.

2. **Isolation** - Analytical queries compete with production traffic and risk slowing down your application for users.

3. **Data Structure** - App DB uses normalized structure (efficient for writes). Analytics needs denormalized star schema (fast reads).

4. **Historical Data** - App DB shows current state only. Data warehouse stores historical snapshots.

5. **Multiple Sources** - Warehouse can combine data from multiple apps/services.

**When you CAN query app DB directly:**
- Small data, simple queries
- Development/test environments
- When you tolerate slower query performance

### Why is star schema more efficient for queries?

**Q: Why does star schema make queries faster?**

**A:**
1. **Fewer joins** - Fact table directly joins dimensions. No complex multi-table joins.

2. **Denormalized** - Dimensions store all attributes in one table (no repeated JOINs to fetch related data).

3. **Aggregation-ready** - Fact table has numeric measures already joined with keys, ready for GROUP BY.

4. **Index-friendly** - Star schema works well with columnar databases (Snowflake, BigQuery) for fast scans.

5. **Simple queries** - BI tools can generate SQL automatically without complex logic.

**Example:**
```sql
-- Star schema (fast)
SELECT d.month, SUM(f.amount)
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.month

-- Normalized (slower, complex)
SELECT ... FROM orders o
JOIN customers c ON ...
JOIN products p ON ...
JOIN categories c2 ON ...