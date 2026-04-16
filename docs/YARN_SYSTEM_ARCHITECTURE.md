# YARN System Architecture

## Overall architecture (from current docker-compose)

```mermaid
flowchart LR
  U["Data Engineer / Analyst"] --> AW["Airflow Webserver :8080"]
  AW --> AS["Airflow Scheduler"]
  AS --> AWorker["Airflow Worker (Celery)"]
  AWorker -->|submit ingestion/mapreduce jobs| RM

  subgraph META["Airflow Control Plane"]
    AS
    AWorker
    AW
    APG[("Postgres Airflow Metadata")]
    R[("Redis Broker")]
    AS --> APG
    AWorker --> APG
    AS --> R
    AWorker --> R
  end

  SRC[("Source Postgres :5432")]
  JOBS["jobs service (simulator)"] --> SRC
  SRC -->|extract| AWorker

  subgraph YARN["YARN Cluster"]
    RM["ResourceManager :8088"]
    NM1["NodeManager :8042"]
    HS["HistoryServer :8188"]

    RM -->|allocate containers| NM1
    NM1 -->|job timeline| HS
  end

  subgraph HDFS["HDFS Cluster"]
    NN["NameNode :9870 / :8020"]
    DN1["DataNode1 :9864"]
    DN2["DataNode2 :9864"]
    DN3["DataNode3 :9864"]

    NN <--> DN1
    NN <--> DN2
    NN <--> DN3
  end

  RM -->|read/write data| NN
  NM1 -->|task I/O| NN

  subgraph SPARK["Spark Standalone"]
    SM["Spark Master :7077 / :8080"]
    SW1["Spark Worker-1 :8081"]
    SW2["Spark Worker-2 :8082"]
    SM --> SW1
    SM --> SW2
  end

  AWorker -->|spark-submit| SM
  SM -->|read/write| NN

  subgraph DWH["Data Warehouse"]
    HIVE["HiveServer2 + Metastore :10000/:9083"]
    HPG[("Postgres Metastore :5433")]
    HIVE --> HPG
  end

  HIVE -->|warehouse data| NN
  AWorker -->|query/ETL| HIVE
  NB["Jupyter Notebook :8888"] --> NN
  NB --> HIVE
```

## YARN-focused responsibilities

- `ResourceManager`: nhận job request, lập lịch tài nguyên và phân phối container.
- `NodeManager`: chạy container/task thực thi trên node worker.
- `HistoryServer`: lưu và tra cứu lịch sử job (timeline/logical history).
- `NameNode/DataNode`: lớp lưu trữ dữ liệu HDFS phục vụ input/output cho YARN jobs.

## Typical execution flow (YARN path)

1. Airflow Scheduler kích hoạt DAG.
2. Airflow Worker submit job (MapReduce hoặc Spark client flow) vào cụm Hadoop.
3. ResourceManager cấp container cho NodeManager.
4. Task đọc dữ liệu từ HDFS (NameNode/DataNode), xử lý, ghi kết quả về HDFS.
5. Trạng thái và lịch sử job được theo dõi qua ResourceManager UI / HistoryServer.

