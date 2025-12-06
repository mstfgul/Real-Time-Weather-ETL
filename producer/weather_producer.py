import json
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherProducer:
    def __init__(self, kafka_broker='localhost:9092', topic='weather-data'):
        self.topic = topic
        self.producer = None
        self.kafka_broker = kafka_broker
        self.cities = [
            {"name": "Istanbul", "lat": 41.0082, "lon": 28.9784},
            {"name": "Ankara", "lat": 39.9334, "lon": 32.8597},
            {"name": "Izmir", "lat": 38.4237, "lon": 27.1428},
            {"name": "London", "lat": 51.5074, "lon": -0.1278},
            {"name": "New York", "lat": 40.7128, "lon": -74.0060},
            {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
        ]

    def connect_kafka(self):
        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_broker,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info(f"Successfully connected to Kafka at {self.kafka_broker}")
                return True
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)

        logger.error("Could not connect to Kafka after multiple attempts")
        return False

    def fetch_weather_data(self, city):
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m",
                "timezone": "auto"
            }

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            weather_data = {
                "city": city["name"],
                "latitude": city["lat"],
                "longitude": city["lon"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": data["current"]["temperature_2m"],
                "humidity": data["current"]["relative_humidity_2m"],
                "precipitation": data["current"]["precipitation"],
                "wind_speed": data["current"]["wind_speed_10m"],
                "wind_direction": data["current"]["wind_direction_10m"],
                "timezone": data["timezone"]
            }

            return weather_data

        except Exception as e:
            logger.error(f"Error fetching weather data for {city['name']}: {e}")
            return None

    def send_to_kafka(self, data):
        try:
            future = self.producer.send(self.topic, value=data)
            record_metadata = future.get(timeout=10)
            logger.info(f"Sent data for {data['city']} to Kafka topic '{self.topic}' "
                       f"[partition: {record_metadata.partition}, offset: {record_metadata.offset}]")
            return True
        except Exception as e:
            logger.error(f"Error sending data to Kafka: {e}")
            return False

    def run(self, interval=60):
        if not self.connect_kafka():
            logger.error("Exiting due to Kafka connection failure")
            return

        logger.info(f"Starting weather data collection for {len(self.cities)} cities")
        logger.info(f"Data will be collected every {interval} seconds")

        try:
            while True:
                for city in self.cities:
                    weather_data = self.fetch_weather_data(city)
                    if weather_data:
                        self.send_to_kafka(weather_data)
                    time.sleep(2)

                logger.info(f"Waiting {interval} seconds before next collection cycle...")
                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("Kafka producer closed")

if __name__ == "__main__":
    producer = WeatherProducer(
        kafka_broker='localhost:9092',
        topic='weather-data'
    )
    producer.run(interval=60)
