---
noteId: "e324e360d24411f0b88af55be93658d8"
tags: []

---

# 🌦️ Real-Time Weather Data Pipeline

A production-ready, end-to-end real-time data engineering project that collects weather data from multiple cities worldwide, processes it through a streaming pipeline, stores it in a data lake and analytical database, and visualizes it through interactive dashboards.

![Architecture](https://img.shields.io/badge/Architecture-Microservices-blue)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success)
![Python](https://img.shields.io/badge/Python-3.13-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Data Pipeline](#data-pipeline)
- [Visualizations](#visualizations)
- [Monitoring](#monitoring)
- [Future Enhancements](#future-enhancements)

## 🎯 Overview

This project demonstrates a complete real-time data engineering solution that:
- Collects weather data from 6 major cities every 60 seconds
- Streams data through Apache Kafka for reliable message delivery
- Processes data in real-time using Apache Spark Structured Streaming
- Stores raw data in a Data Lake (MinIO/S3) and processed data in PostgreSQL
- Provides interactive dashboards for data visualization using Metabase

**Key Metrics:**
- 🌍 **6 Cities**: Istanbul, Ankara, Izmir, London, New York, Tokyo
- ⚡ **Real-time Processing**: ~60 second latency
- 📊 **Data Points**: Temperature, Humidity, Wind Speed, Precipitation
- 🔄 **24/7 Operation**: Containerized for continuous data collection

## 🏗️ Architecture

```
┌─────────────────┐
│  Open-Meteo API │ (Free Weather API)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Python Producer │ (Data Collection)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Kafka   │ (Stream Processing)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Spark   │ (Real-time ETL)
└────────┬────────┘
         │
         ├──────────────────┬──────────────────┐
         ▼                  ▼                  ▼
  ┌────────────┐    ┌─────────────┐   ┌──────────────┐
  │   MinIO    │    │ PostgreSQL  │   │  Metabase    │
  │ Data Lake  │    │  Database   │   │  Dashboard   │
  └────────────┘    └─────────────┘   └──────────────┘
```

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Collection** | Python 3.13 | Weather API integration |
| **API** | Open-Meteo API | Free weather data source |
| **Message Broker** | Apache Kafka 7.5.0 | Stream processing |
| **Processing Engine** | Apache Spark 3.5.0 | Real-time ETL |
| **Data Lake** | MinIO (S3-compatible) | Raw data storage (Parquet) |
| **Database** | PostgreSQL 15 | Analytical queries |
| **Orchestration** | Docker Compose | Service management |
| **Visualization** | Metabase | Interactive dashboards |

## ✨ Features

- ✅ **Real-time Data Collection**: Automated weather data fetching every 60 seconds
- ✅ **Fault-tolerant Streaming**: Kafka ensures no data loss
- ✅ **Scalable Processing**: Spark handles high-throughput data processing
- ✅ **Data Lake Architecture**: Raw data stored in Parquet format
- ✅ **ACID Compliance**: PostgreSQL for reliable data storage
- ✅ **Interactive Dashboards**: Real-time visualization with Metabase
- ✅ **Containerized Deployment**: Easy setup with Docker Compose
- ✅ **Production Ready**: Error handling, logging, and monitoring

## 📁 Project Structure

```
LiveWeather/
├── producer/                   # Weather data producer
│   ├── weather_producer.py    # Main producer script
│   └── requirements.txt       # Python dependencies
├── consumer/                   # Kafka consumer (testing)
│   └── kafka_consumer_test.py
├── spark-jobs/                # Spark streaming applications
│   ├── spark_consumer.py      # Full ETL with MinIO
│   ├── spark_consumer_simple.py  # Simplified PostgreSQL only
│   └── requirements.txt
├── config/                    # Configuration files
│   ├── init-postgres.sql     # Database schema & views
│   └── init-minio.sh         # MinIO bucket setup
├── scripts/                   # Utility scripts
│   ├── setup.sh              # One-command setup
│   ├── run-spark-job.sh      # Start Spark job
│   ├── run-spark-simple.sh   # Start simplified Spark job
│   └── stop.sh               # Stop all services
├── data/                      # Data directories
│   ├── raw/                  # Raw parquet files
│   ├── processed/            # Processed data
│   └── checkpoint/           # Spark checkpoints
├── docker-compose.yml        # Service orchestration
├── Makefile                  # Quick commands
└── README.md                 # This file
```

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- 4GB RAM minimum
- Internet connection

### Installation

1. **Clone the repository**
   ```bash
   git clone git@github.com:mstfgul/Real-Time-Weather-ETL.git
   cd Real-Time-Weather-ETL
   ```

2. **Start all services**
   ```bash
   chmod +x scripts/*.sh
   ./scripts/setup.sh
   ```

   Or manually:
   ```bash
   docker-compose up -d
   ```

3. **Install Python dependencies**
   ```bash
   cd producer
   pip install -r requirements.txt
   ```

4. **Start the data pipeline**

   Terminal 1 - Producer:
   ```bash
   cd producer
   python3 weather_producer.py
   ```

   Terminal 2 - Spark Consumer:
   ```bash
   ./scripts/run-spark-simple.sh
   ```

### Quick Commands

```bash
make setup        # Initial setup
make start        # Start services
make producer     # Run producer
make spark-job    # Run Spark job
make stop         # Stop services
make clean        # Clean everything
```

## 📊 Data Pipeline

### 1. Data Collection

The producer fetches weather data from Open-Meteo API:

```python
cities = [
    {"name": "Istanbul", "lat": 41.0082, "lon": 28.9784},
    {"name": "Ankara", "lat": 39.9334, "lon": 32.8597},
    {"name": "Izmir", "lat": 38.4237, "lon": 27.1428},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503}
]
```

**Data Points:**
- Temperature (°C)
- Humidity (%)
- Precipitation (mm)
- Wind Speed (km/h)
- Wind Direction (degrees)

### 2. Stream Processing

Kafka topic: `weather-data`
- Partitions: 1
- Replication factor: 1
- Retention: 7 days

### 3. ETL with Spark

- Reads from Kafka in micro-batches
- Transforms and validates data
- Writes to PostgreSQL (append mode)
- Archives to MinIO (Parquet format)

### 4. Database Schema

```sql
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp TIMESTAMP,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    timezone VARCHAR(50),
    ingestion_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes:**
- `idx_weather_city` on city
- `idx_weather_timestamp` on timestamp
- `idx_weather_city_timestamp` on (city, timestamp)

## 📈 Visualizations

### Access Metabase

1. Navigate to http://localhost:3000
2. Complete initial setup
3. Add PostgreSQL connection:
   - Host: `postgres`
   - Port: `5432`
   - Database: `weather_db`
   - Username: `weather_user`
   - Password: `weather_pass`

### Sample Queries

**Average Temperature by City:**
```sql
SELECT
    city,
    ROUND(AVG(temperature)::numeric, 1) as avg_temp
FROM weather_data
GROUP BY city
ORDER BY avg_temp DESC;
```

**24-Hour Temperature Trend:**
```sql
SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    city,
    AVG(temperature) as temp
FROM weather_data
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY hour, city
ORDER BY hour;
```

**Current Weather Snapshot:**
```sql
SELECT
    city,
    temperature,
    humidity,
    wind_speed,
    timestamp
FROM weather_data
WHERE id IN (
    SELECT MAX(id)
    FROM weather_data
    GROUP BY city
);
```

## 🔍 Monitoring

### Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Metabase | http://localhost:3000 | Setup on first access |
| PostgreSQL | localhost:5433 | weather_user / weather_pass |
| Kafka | localhost:9092 | - |

### Health Checks

```bash
# Check all services
docker-compose ps

# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9093

# Check PostgreSQL data
docker exec postgres psql -U weather_user -d weather_db -c "SELECT COUNT(*) FROM weather_data;"

# View Kafka messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic weather-data \
  --from-beginning \
  --max-messages 5
```

## 🔧 Configuration

### Environment Variables

Copy `.env.example` to `.env` and customize:

```bash
KAFKA_BROKER=localhost:9092
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
MINIO_ENDPOINT=http://localhost:9000
COLLECTION_INTERVAL=60
```

### Scaling

Adjust in `docker-compose.yml`:

```yaml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=4G
    - SPARK_WORKER_CORES=4
```

## 🎓 Key Learnings & Skills Demonstrated

- **Data Engineering**: End-to-end pipeline design and implementation
- **Stream Processing**: Real-time data processing with Kafka and Spark
- **Data Modeling**: Efficient schema design for analytical queries
- **DevOps**: Docker containerization and orchestration
- **Python Development**: Async operations, error handling, logging
- **SQL**: Complex queries, window functions, aggregations
- **Cloud Technologies**: S3-compatible storage (MinIO)
- **Monitoring & Observability**: Service health checks and logging

## 🚧 Future Enhancements

- [ ] Machine Learning: Weather prediction models
- [ ] Alerting: Email/Slack notifications for extreme weather
- [ ] Data Quality: Great Expectations integration
- [ ] CI/CD: GitHub Actions pipeline
- [ ] Testing: Unit tests, integration tests
- [ ] Kubernetes: K8s deployment manifests
- [ ] API: REST API for data access
- [ ] More Cities: Expand to 50+ cities
- [ ] Historical Analysis: Long-term trend analysis
- [ ] Performance: Query optimization and caching

## 📝 License

MIT License - feel free to use this project for learning and portfolio purposes.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 👨‍💻 Author

**Mustafa Gul**
- GitHub: [@mstfgul](https://github.com/mstfgul)
- LinkedIn: [Add your LinkedIn]
- Portfolio: [Add your portfolio site]

## 🙏 Acknowledgments

- Weather data provided by [Open-Meteo API](https://open-meteo.com/)
- Built with open-source technologies
- Inspired by real-world data engineering challenges

---

⭐ Star this repository if you find it helpful!
