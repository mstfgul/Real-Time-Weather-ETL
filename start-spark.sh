#!/bin/bash

echo "🔥 Starting Spark Streaming Job"
echo "==============================="
echo ""
echo "This will:"
echo "  1. Read data from Kafka"
echo "  2. Write to PostgreSQL"
echo "  3. Write to MinIO (Data Lake)"
echo ""
echo "Press Ctrl+C to stop"
echo ""

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.streaming.checkpointLocation=/opt/data/checkpoint \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/spark-jobs/spark_consumer.py
