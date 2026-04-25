# Hive Production Split Plan

## Goal

Split Hive into dedicated services for production (`metastore` and `hiveserver2`), fix Java runtime compatibility, and ensure Hive can read/write/query data in HDFS from the existing Hadoop cluster.

## Current Issues

1. Hive image uses Java 8 while current Hive binaries require newer JVM bytecode support.
2. A single combined Hive service (`dtwarehouse-hive`) couples metastore and HiveServer2 startup.
3. Hive container does not explicitly consume Hadoop cluster client configs from the main cluster setup.

## Target Architecture

1. `dtwarehouse-postgres`: metastore DB (existing).
2. `dtwarehouse-metastore`: dedicated Hive Metastore service (port 9083).
3. `dtwarehouse-hiveserver2`: dedicated HiveServer2 service (ports 10000, 10002).
4. Shared custom Hive image with role-based startup script.

## Implementation Steps

1. Update Hive image base runtime from Java 8 to Java 17.
2. Make startup script role-based using `HIVE_SERVICE_ROLE`:
   - `metastore`: wait for Postgres, initialize schema if needed, start metastore.
   - `hiveserver2`: wait for metastore and HDFS, ensure required HDFS directories and permissions, start HiveServer2.
3. Update `hive-site.xml` metastore URI to `thrift://dtwarehouse-metastore:9083`.
4. Split `data_warehouse/docker-compose.yml` service definitions:
   - Replace `dtwarehouse-hive` with `dtwarehouse-metastore` and `dtwarehouse-hiveserver2`.
   - Mount Hadoop configs from `../conf/core-site.xml` and `../conf/hdfs-site.xml` into both services.
   - Add health checks and proper dependency ordering.
5. Validate compose file syntax with `docker compose config`.

## Verification Checklist

1. `java -version` inside Hive containers reports Java 17.
2. `schematool -dbType postgres -info` works in metastore container.
3. `hdfs dfs -ls /` works from HiveServer2 container.
4. Beeline can connect: `jdbc:hive2://localhost:10000/default`.
5. Create/read table in HDFS-backed warehouse path succeeds.
