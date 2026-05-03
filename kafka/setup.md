# Setup pgsql enable click stream
```bash
docker exec -it srcs-ecommerce-postgres psql -U admin -d postgres -c "CREATE ROLE debezium WITH LOGIN PASSWORD 'debezium' REPLICATION;"
docker exec -it srcs-ecommerce-postgres psql -U admin -d postgres -c "GRANT CONNECT ON DATABASE postgres TO debezium;"
docker exec -it srcs-ecommerce-postgres psql -U admin -d postgres -c "GRANT USAGE ON SCHEMA public TO debezium;"
docker exec -it srcs-ecommerce-postgres psql -U admin -d postgres -c "GRANT SELECT ON TABLE public.clickstream TO debezium;"
docker exec -it srcs-ecommerce-postgres psql -U admin -d postgres -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;"

docker exec -it srcs-ecommerce-postgres psql -U admin -d postgres -c "DROP PUBLICATION IF EXISTS debezium_clickstream_pub;"
docker exec -it srcs-ecommerce-postgres psql -U admin -d postgres -c "CREATE PUBLICATION debezium_clickstream_pub FOR TABLE public.clickstream;"


```
# Run thí config to connect cdc connector to postgres
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg-clickstream-cdc",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "srcs-ecommerce-postgres",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "debezium",
      "database.dbname": "postgres",
      "database.server.name": "srcs_ecommerce",
      "topic.prefix": "srcs_ecommerce",
      "table.include.list": "public.clickstream",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_clickstream_slot",
      "publication.name": "debezium_clickstream_pub",
      "publication.autocreate.mode": "filtered",
      "snapshot.mode": "initial",
      "tombstones.on.delete": "false"
    }
  }'

```


## Verify:
```bash
curl http://localhost:8083/connectors/pg-clickstream-cdc/status
```

## confum events:

```bash
docker exec -it kafka-cluster-cdc-kafka1 /var/lib/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-cluster-cdc-kafka1:9092 \
  --topic srcs_ecommerce.public.clickstream \
  --from-beginning

```


## COnsum
docker exec -it kafka-cluster-cdc-kafka1 /opt/kafka_2.13-3.9.0/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:9092 \
  --topic srcs_ecommerce.public.clickstream \
  --from-beginning
