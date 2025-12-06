#!/bin/bash

echo "Starting Spark Streaming Job..."
echo "This will process data from Kafka and write to PostgreSQL + MinIO"
echo ""

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///opt/spark/conf/log4j.properties" \
  --conf spark.sql.streaming.checkpointLocation=/opt/data/checkpoint \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  /opt/spark-jobs/spark_consumer.py

echo ""
echo "Spark job finished or stopped."
