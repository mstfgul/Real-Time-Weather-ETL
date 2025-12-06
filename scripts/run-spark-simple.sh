#!/bin/bash

echo "🔥 Starting Spark Streaming Job"
echo "================================"
echo "This will process data from Kafka → PostgreSQL"
echo ""

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.sql.streaming.checkpointLocation=/opt/data/checkpoint \
  /opt/spark-jobs/spark_consumer_simple.py

echo ""
echo "Spark job finished or stopped."
