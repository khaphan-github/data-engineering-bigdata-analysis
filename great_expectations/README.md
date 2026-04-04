# Self-host Great Expectations (GX) in this project

This repo now includes a dedicated Docker service named `great-expectations`.

## 1) Start required services

From repo root:

```bash
docker compose up -d postgres great-expectations
```

## 2) Initialize a GX project

```bash
docker compose exec great-expectations bash -lc 'mkdir -p /opt/gx/project && cd /opt/gx/project && gx init'
```

This creates your GX project under:
- `/opt/gx/project` in container
- `./great_expectations/project` on host

## 3) Configure a datasource (PostgreSQL in this stack)

Run interactive setup:

```bash
docker compose exec great-expectations bash -lc 'cd /opt/gx/project && gx datasource new'
```

Use these values:
- host: `postgres`
- port: `5432`
- database: `${POSTGRES_DB}` (default in `.env`: `postgres`)
- username: `${POSTGRES_USER}` (default: `admin`)
- password: `${POSTGRES_PASSWORD}` (default: `admin`)

## 4) Create Expectation Suite + Checkpoint

Create an expectation suite and checkpoint from your selected asset/table:

```bash
docker compose exec great-expectations bash -lc 'cd /opt/gx/project && gx suite new'
docker compose exec great-expectations bash -lc 'cd /opt/gx/project && gx checkpoint new'
```

Run validation:

```bash
docker compose exec great-expectations bash -lc 'cd /opt/gx/project && gx checkpoint run <YOUR_CHECKPOINT_NAME>'
```

## 5) Self-host Data Docs UI

Build docs:

```bash
docker compose exec great-expectations bash -lc 'cd /opt/gx/project && gx docs build'
```

Start static docs server (inside same container):

```bash
docker compose exec -d great-expectations bash -lc 'serve_data_docs.sh 8093'
```

Open:
- `http://localhost:8093`

## Useful commands

Open shell in GX container:

```bash
docker compose exec great-expectations bash
```

Stop docs server:

```bash
docker compose restart great-expectations
```

Stop GX service:

```bash
docker compose stop great-expectations
```


### START CODE HERE ### (~ 3 lines of code)

# Use the "vendor_id" column as splitter column
trips.add_splitter_column_value("vendor_id")

# Build the batch request
batch_request = trips.build_batch_request()

# Get the batches
batches = trips.get_batch_list_from_batch_request(batch_request)

### END CODE HERE ###

for batch in batches:
    print(batch.batch_spec)




# Add an expectation suite name to the context
expectation_suite_name = f"{LAB_PREFIX}-expectation-suite-trips-taxi-db"

### START CODE HERE ### (~ 1 line of code)

context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

### END CODE HERE ###




### START CODE HERE ### (~ 4 lines of code)

validator = context.get_validator(
    batch_request_list=batch_request_list,
    expectation_suite_name=expectation_suite_name,
) 


### END CODE HERE ###




### START CODE HERE ### (~ 3 lines of code)

validator.expect_column_values_to_not_be_null(column="pickup_datetime")
validator.expect_column_values_to_not_be_null(column="passenger_count")
validator.expect_column_values_to_be_between(column="congestion_surcharge", min_value=0, max_value=1000)

### END CODE HERE ###



validator.save_expectation_suite(discard_failed_expectations=False)




# Build the batch request
batch_request = trips.build_batch_request() 

# Create your batches using the batch_request from the previous cell
batches = trips.get_batch_list_from_batch_request(batch_request)



### START CODE HERE ### (~ 4 lines of code)

validations = [ 
    
    {"batch_request": batch.batch_request, "expectation_suite_name": None}
    for batch in batches
] 


### END CODE HERE ###

validations



### START CODE HERE ### (~ 1 line of code)

context.add_or_update_checkpoint(checkpoint=checkpoint_name)

### END CODE HERE ###


checkpoint_result = checkpoint.run()