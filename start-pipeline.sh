#!/bin/bash

echo "🚀 Starting Real-Time Weather Data Pipeline"
echo "=========================================="

# Check if services are running
echo ""
echo "📊 Checking Docker services..."
RUNNING=$(docker ps --format '{{.Names}}' | wc -l)
echo "   Running containers: $RUNNING/7"

if [ "$RUNNING" -lt 7 ]; then
    echo "   ⚠️  Not all services are running!"
    echo "   Starting services with: docker-compose up -d"
    docker-compose up -d
    echo "   Waiting 20 seconds for services to be ready..."
    sleep 20
fi

echo ""
echo "✅ All services are ready!"
echo ""
echo "Starting pipeline in 3 separate terminals:"
echo ""
echo "1️⃣  Terminal 1: Weather Data Producer"
echo "   Run: cd producer && python3 weather_producer.py"
echo ""
echo "2️⃣  Terminal 2: Spark Streaming Job"
echo "   Run: ./scripts/run-spark-job.sh"
echo ""
echo "3️⃣  Terminal 3: Test Consumer (Optional)"
echo "   Run: cd consumer && python3 kafka_consumer_test.py"
echo ""
echo "Or run the producer in background:"
echo "   cd producer && python3 weather_producer.py > ../logs/producer.log 2>&1 &"
echo ""
echo "📊 Monitor at:"
echo "   - Spark UI: http://localhost:8080"
echo "   - MinIO: http://localhost:9001"
echo "   - Metabase: http://localhost:3000"
