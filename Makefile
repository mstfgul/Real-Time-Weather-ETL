.PHONY: help setup start stop clean logs producer consumer spark-job

help:
	@echo "Real-Time Weather Data Pipeline - Available Commands:"
	@echo ""
	@echo "  make setup        - Initial setup (create dirs, start services)"
	@echo "  make start        - Start all Docker services"
	@echo "  make stop         - Stop all Docker services"
	@echo "  make clean        - Stop services and remove volumes"
	@echo "  make logs         - Show logs from all services"
	@echo "  make producer     - Start weather data producer"
	@echo "  make consumer     - Start test Kafka consumer"
	@echo "  make spark-job    - Submit Spark streaming job"
	@echo "  make status       - Check status of all services"
	@echo "  make restart      - Restart all services"
	@echo ""

setup:
	@echo "Setting up the project..."
	@chmod +x scripts/*.sh
	@./scripts/setup.sh

start:
	@echo "Starting all services..."
	@docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 30
	@echo "Services started! Access them at:"
	@echo "  - Spark UI: http://localhost:8080"
	@echo "  - MinIO Console: http://localhost:9001"
	@echo "  - Metabase: http://localhost:3000"

stop:
	@echo "Stopping all services..."
	@docker-compose down

clean:
	@echo "Cleaning up everything..."
	@docker-compose down -v
	@echo "All services stopped and volumes removed"

logs:
	@docker-compose logs -f

producer:
	@echo "Starting weather producer..."
	@cd producer && python3 weather_producer.py

consumer:
	@echo "Starting test Kafka consumer..."
	@cd consumer && python3 kafka_consumer_test.py

spark-job:
	@echo "Submitting Spark streaming job..."
	@./scripts/run-spark-job.sh

status:
	@echo "Checking service status..."
	@docker-compose ps

restart:
	@make stop
	@make start

install-deps:
	@echo "Installing Python dependencies..."
	@pip3 install -r producer/requirements.txt
	@echo "Dependencies installed!"
