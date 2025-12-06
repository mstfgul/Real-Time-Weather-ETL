import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

class WeatherSparkConsumer:
    def __init__(self):
        self.spark = None
        self.schema = StructType([
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", DoubleType(), True),
            StructField("timezone", StringType(), True)
        ])

    def create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("WeatherDataProcessor") \
            .config("spark.jars.packages",
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.postgresql:postgresql:42.6.0,"
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.streaming.checkpointLocation", "/opt/data/checkpoint") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark session created successfully")

    def read_from_kafka(self):
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9093") \
            .option("subscribe", "weather-data") \
            .option("startingOffsets", "latest") \
            .load()

        weather_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*")

        weather_df = weather_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        ).withColumn(
            "ingestion_time",
            current_timestamp()
        )

        return weather_df

    def write_to_postgres(self, batch_df, batch_id):
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/weather_db") \
                .option("dbtable", "weather_data") \
                .option("user", "weather_user") \
                .option("password", "weather_pass") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            print(f"Batch {batch_id}: Written {batch_df.count()} records to PostgreSQL")
        except Exception as e:
            print(f"Error writing batch {batch_id} to PostgreSQL: {e}")

    def write_to_minio(self, batch_df, batch_id):
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            path = f"s3a://weather-lake/raw/date={date_str}/batch_{batch_id}.parquet"

            batch_df.write \
                .mode("append") \
                .parquet(path)

            print(f"Batch {batch_id}: Written to MinIO at {path}")
        except Exception as e:
            print(f"Error writing batch {batch_id} to MinIO: {e}")

    def process_batch(self, batch_df, batch_id):
        if not batch_df.isEmpty():
            self.write_to_postgres(batch_df, batch_id)
            self.write_to_minio(batch_df, batch_id)

    def start_streaming(self):
        self.create_spark_session()
        weather_stream = self.read_from_kafka()

        query = weather_stream.writeStream \
            .foreachBatch(self.process_batch) \
            .outputMode("append") \
            .start()

        print("Streaming query started. Waiting for data...")
        query.awaitTermination()

if __name__ == "__main__":
    consumer = WeatherSparkConsumer()
    consumer.start_streaming()
