"""
Simple Kafka consumer for testing - reads and displays weather data from Kafka
"""
import json
from kafka import KafkaConsumer
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def consume_weather_data():
    consumer = KafkaConsumer(
        'weather-data',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    logger.info("Starting Kafka consumer for topic 'weather-data'")
    logger.info("Waiting for messages...")

    try:
        for message in consumer:
            weather_data = message.value
            logger.info(f"\n{'='*60}")
            logger.info(f"City: {weather_data['city']}")
            logger.info(f"Temperature: {weather_data['temperature']}°C")
            logger.info(f"Humidity: {weather_data['humidity']}%")
            logger.info(f"Wind Speed: {weather_data['wind_speed']} km/h")
            logger.info(f"Precipitation: {weather_data['precipitation']} mm")
            logger.info(f"Timestamp: {weather_data['timestamp']}")
            logger.info(f"{'='*60}\n")

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    consume_weather_data()
