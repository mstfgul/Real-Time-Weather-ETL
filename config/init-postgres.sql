-- Create main weather data table
CREATE TABLE IF NOT EXISTS weather_data (
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

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_data(city);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp ON weather_data(city, timestamp);

-- Create aggregated view for dashboard
CREATE OR REPLACE VIEW weather_summary AS
SELECT
    city,
    DATE(timestamp) as date,
    AVG(temperature) as avg_temperature,
    MAX(temperature) as max_temperature,
    MIN(temperature) as min_temperature,
    AVG(humidity) as avg_humidity,
    AVG(wind_speed) as avg_wind_speed,
    SUM(precipitation) as total_precipitation,
    COUNT(*) as record_count
FROM weather_data
GROUP BY city, DATE(timestamp)
ORDER BY date DESC, city;

-- Create metabase database if not exists
CREATE DATABASE metabase;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user;
GRANT ALL PRIVILEGES ON DATABASE metabase TO weather_user;
