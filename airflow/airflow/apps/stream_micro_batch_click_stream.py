'''
This file handle:
0. Airflow to handle orchestreation of microbatch processing.
1. Read micribatch from click stream topic in Kafka.
2. Verify data qualities using Great Expectation
3. Write the microbatch to HDFS.
'''

