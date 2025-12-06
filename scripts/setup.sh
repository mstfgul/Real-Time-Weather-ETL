#!/bin/bash

echo "====================================="
echo "Real-Time Weather Data Pipeline Setup"
echo "====================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Step 1: Checking Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo -e "${GREEN}Docker and Docker Compose are installed.${NC}"

echo -e "\n${YELLOW}Step 2: Creating required directories...${NC}"
mkdir -p data/raw data/processed data/checkpoint

echo -e "${GREEN}Directories created.${NC}"

echo -e "\n${YELLOW}Step 3: Starting Docker containers...${NC}"
docker-compose up -d

echo -e "\n${YELLOW}Step 4: Waiting for services to be ready...${NC}"
sleep 30

echo -e "\n${YELLOW}Step 5: Creating MinIO bucket...${NC}"
docker exec -it minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker exec -it minio mc mb myminio/weather-lake --ignore-existing 2>/dev/null || true

echo -e "\n${GREEN}Setup completed!${NC}"
echo -e "\n${YELLOW}Services are running at:${NC}"
echo "  - Kafka: localhost:9092"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  - PostgreSQL: localhost:5432"
echo "  - Metabase: http://localhost:3000"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Install producer dependencies: cd producer && pip install -r requirements.txt"
echo "  2. Start the weather producer: python producer/weather_producer.py"
echo "  3. Start Spark consumer: docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 /opt/spark-jobs/spark_consumer.py"
echo "  4. Access Metabase at http://localhost:3000 to create dashboards"
