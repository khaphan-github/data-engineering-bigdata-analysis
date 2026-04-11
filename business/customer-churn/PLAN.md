# Implementation Plan: Customer Churn Pipeline

## 0) Trạng thái hiện tại (đã làm)

- Jobs simulator đã chạy ổn định theo interval.
- Service jobs hiện chạy nhiều simulator song song (ecommerce + churn source).
- Churn source simulator hiện chỉ sinh 2 bảng trong DB `customer_churn`:
  - `customers`
  - `orders`
- `churn_labels` không còn được generate ở simulator; sẽ được tính ở Hive query layer.

## 1) Mục tiêu

- Tách simulator job sang database riêng để đảm bảo separation of concern.
- Orchestrate luồng batch bằng Airflow: Postgres source -> Spark -> HDFS raw zone.
- Dùng Hive query layer để tạo output phân tích churn cho BI/ML.

## 2) Phạm vi triển khai

- In scope (MVP):
  - Simulator cho 2 bảng nguồn: `customers`, `orders`.
  - DAG Airflow ingest batch từ Postgres sang HDFS (partition theo ngày).
  - Hive external tables + query output cơ bản, bao gồm tính `churn_labels` ở tầng query.
- Out of scope (Phase 2):
  - `order_items`, `payments`, stream ingestion Kafka.
  - Model training/scoring production.

## 3) Thiết kế tách database cho simulator

### 3.1 Nguyên tắc

- Giữ 1 Postgres service, tách theo database-level:
  - DB hiện tại (ecommerce/clickstream) giữ nguyên.
  - DB mới cho churn simulator, ví dụ: `customer_churn`.

### 3.2 Cấu hình env mới

- `SIMULATOR_POSTGRES_DB` (default fallback về `POSTGRES_DB` để backward-compatible).
- `POSTGRES_ADMIN_DB` (default: `postgres`) cho bước tạo DB đích.

### 3.3 Hành vi runtime

1. Job connect vào `POSTGRES_ADMIN_DB`.
2. Kiểm tra và tạo `SIMULATOR_POSTGRES_DB` nếu chưa tồn tại.
3. Reconnect vào DB đích.
4. Tạo bảng và insert batch như bình thường.

## 4) Kế hoạch code change chi tiết

### 4.1 `jobs/env.py`

- Bổ sung field config:
  - `simulator_postgres_db`
  - `postgres_admin_db`
- Giữ fallback để không phá code cũ.

### 4.2 `jobs/simulator_generate_data.py`

- Thêm hàm validate database name.
- Thêm hàm ensure database exists (idempotent).
- Đổi DSN write từ `POSTGRES_DB` sang `SIMULATOR_POSTGRES_DB`.
- Bỏ mode switch; jobs service chạy đồng thời nhiều simulator.

### 4.3 `docker-compose.yml`

- Trong service `jobs`, truyền thêm env:
  - `SIMULATOR_POSTGRES_DB`
  - `POSTGRES_ADMIN_DB`

### 4.4 `.env`

- Thêm cấu hình mặc định:
  - `SIMULATOR_POSTGRES_DB=customer_churn`
  - `POSTGRES_ADMIN_DB=postgres`

### 4.5 `jobs/README.md`

- Cập nhật phần env và giải thích lý do tách DB.
- Thêm hướng dẫn verify database/tables.

## 5) Airflow orchestration plan

### 5.1 DAG mới cho churn ingestion

- Tạo DAG: `churn_pgsql_to_hdfs_dag.py`.
- Tần suất đề xuất: mỗi 5 phút (hoặc theo SLA thực tế).

### 5.2 Spark app ingest

- Tạo app mới dùng JDBC đọc từ DB churn.
- Ghi Parquet vào HDFS:
  - `/data/raw/customers/dt=YYYY-MM-DD`
  - `/data/raw/orders/dt=YYYY-MM-DD`
- Incremental:
  - `orders`: watermark theo `id` hoặc `order_ts`.
  - `customers`: snapshot overwrite theo `dt`.

### 5.3 Data quality checks cơ bản

- Validate cột bắt buộc.
- Validate row count > 0.
- Log record count mỗi bảng.

## 6) Hive query layer plan

### 6.1 DDL

- `CREATE DATABASE IF NOT EXISTS raw_churn;`
- Tạo external table cho `customers`, `orders` trỏ vào HDFS raw path.
- Partition theo `dt`.

### 6.2 Churn label derivation tại Hive

- Tính `churn_labels` bằng Hive SQL từ dữ liệu `customers` + `orders` theo rule nghiệp vụ.
- Ví dụ rule MVP:
  - `churn_30d = 1` nếu khách không có đơn trong 30 ngày gần nhất tại thời điểm snapshot.
  - Có thể mở rộng bằng recency/frequency/amount để tăng chất lượng nhãn.
- Output đề xuất:
  - `curated_churn.churn_labels_daily(snapshot_date, customer_id, churn_30d, churn_reason)`

### 6.3 Output tables/view

- `churn_metrics_daily`:
  - churn rate theo ngày/segment/city/channel.
- `customer_risk_candidates_daily`:
  - danh sách khách có tín hiệu rủi ro churn cao.

### 6.4 Orchestration

- Airflow task cuối DAG chạy Hive SQL (beeline) sau ingest.

## 7) Thứ tự triển khai đề xuất

1. Tách DB cho simulator và verify insert.
2. Sinh dữ liệu churn MVP 2 bảng nguồn (`customers`, `orders`).
3. Tạo Spark ingest app + DAG Airflow.
4. Tạo Hive DDL + query output + derivation `churn_labels`.
5. Chạy E2E và chốt acceptance.

## 8) Kiểm thử và acceptance criteria

### 8.1 Kiểm thử

- Backward-compatible test (không set env mới vẫn chạy).
- Dedicated DB test (`SIMULATOR_POSTGRES_DB=customer_churn`).
- HDFS output test (có partition mới).
- Hive query test (trả kết quả không rỗng).

### 8.2 Acceptance

- Simulator ghi dữ liệu vào DB churn riêng.
- Airflow chạy thành công Spark ingestion.
- Hive tính được `churn_labels` từ source tables.
- Hive query được ít nhất 2 output phục vụ phân tích.

## 9) Rủi ro và giảm thiểu

- Thiếu quyền tạo DB:
  - Pre-create DB hoặc cấp quyền `CREATEDB`.
- Sai cấu hình env:
  - Validate và fail-fast với thông điệp rõ ràng.
- Nhiều file nhỏ trên HDFS:
  - Repartition/compact theo batch window.
