#!/bin/bash

echo "Waiting for MinIO to be ready..."
sleep 10

# Install MinIO client
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc

# Configure MinIO client
./mc alias set myminio http://minio:9000 minioadmin minioadmin

# Create bucket
./mc mb myminio/weather-lake --ignore-existing

# Set public policy (for development only)
./mc anonymous set download myminio/weather-lake

echo "MinIO bucket 'weather-lake' created successfully"
