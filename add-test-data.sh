#!/bin/bash

echo "Adding more test data to PostgreSQL..."

docker exec -i postgres psql -U weather_user -d weather_db << 'EOF'
-- Son 24 saat için veri ekle
INSERT INTO weather_data (city, latitude, longitude, timestamp, temperature, humidity, precipitation, wind_speed, wind_direction, timezone)
SELECT
    city,
    latitude,
    longitude,
    NOW() - (random() * INTERVAL '24 hours'),
    10 + (random() * 15)::numeric,
    50 + (random() * 40)::numeric,
    (random() * 5)::numeric,
    5 + (random() * 15)::numeric,
    (random() * 360)::numeric,
    timezone
FROM (VALUES
    ('Istanbul', 41.0082, 28.9784, 'Europe/Istanbul'),
    ('Ankara', 39.9334, 32.8597, 'Europe/Istanbul'),
    ('Izmir', 38.4237, 27.1428, 'Europe/Istanbul'),
    ('London', 51.5074, -0.1278, 'Europe/London'),
    ('New York', 40.7128, -74.0060, 'America/New_York'),
    ('Tokyo', 35.6762, 139.6503, 'Asia/Tokyo')
) AS cities(city, latitude, longitude, timezone),
generate_series(1, 10);

SELECT COUNT(*) as total_records FROM weather_data;
EOF

echo "✅ Test data added!"
