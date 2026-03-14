# HDFS Commands (from NameNode container)

## Enter NameNode container

```bash
docker exec -it namenode bash
```

## CRUD folders/files (run inside container)

```bash
# Create folder
hdfs dfs -mkdir -p /tiktok
hdfs dfs -ls /tiktok
hdfs dfs -get /tiktok/run-wordcount.sh .
# Upload file (create)
hdfs dfs -put /hadoop-data/input/yourfile /tiktok/

# Read file
hdfs dfs -cat /tiktok/yourfile

# Update/overwrite file
hdfs dfs -put -f /hadoop-data/input/yourfile /tiktok/yourfile

# Delete file
hdfs dfs -rm /tiktok/yourfile

# Delete folder (recursive)
hdfs dfs -rm -r -skipTrash /tiktok
```

## One-liners (run from host)

```bash
# Create folder
docker exec -it namenode hdfs dfs -mkdir -p /tiktok

# List folder
docker exec -it namenode hdfs dfs -ls /tiktok

# Upload file (create)
docker exec -it namenode hdfs dfs -put /hadoop-data/input/yourfile /tiktok/

# Read file
docker exec -it namenode hdfs dfs -cat /tiktok/yourfile

# Update/overwrite file
docker exec -it namenode hdfs dfs -put -f /hadoop-data/input/yourfile /tiktok/yourfile

# Delete file
docker exec -it namenode hdfs dfs -rm /tiktok/yourfile

# Delete folder (recursive)
docker exec -it namenode hdfs dfs -rm -r -skipTrash /tiktok
```
