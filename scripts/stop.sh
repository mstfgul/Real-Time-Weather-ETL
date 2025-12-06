#!/bin/bash

echo "Stopping all services..."
docker-compose down

echo "All services stopped."
echo "To remove all data, run: docker-compose down -v"
