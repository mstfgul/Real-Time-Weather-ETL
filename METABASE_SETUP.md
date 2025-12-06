# Metabase Dashboard Setup Guide

This guide will help you create beautiful dashboards in Metabase for your weather data.

## Initial Setup

1. Open http://localhost:3000
2. Create your admin account
3. Click "Add a database"

### Database Connection Settings

- **Database type**: PostgreSQL
- **Name**: Weather Database
- **Host**: postgres
- **Port**: 5432
- **Database name**: weather_db
- **Username**: weather_user
- **Password**: weather_pass

Click "Save" and wait for Metabase to sync your tables.

## Recommended Dashboards

### 1. Real-Time Weather Overview

**Cards to create:**

- **Current Temperature by City** (Number card)
  ```sql
  SELECT city, temperature, timestamp
  FROM weather_data
  WHERE id IN (
    SELECT MAX(id) FROM weather_data GROUP BY city
  )
  ```

- **Latest Weather Readings** (Table)
  ```sql
  SELECT
    city,
    ROUND(temperature::numeric, 1) as temp_c,
    ROUND(humidity::numeric, 1) as humidity_pct,
    ROUND(wind_speed::numeric, 1) as wind_kmh,
    timestamp
  FROM weather_data
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  ORDER BY timestamp DESC
  ```

### 2. Temperature Trends

- **24-Hour Temperature Trend** (Line chart)
  ```sql
  SELECT
    timestamp,
    city,
    temperature
  FROM weather_data
  WHERE timestamp > NOW() - INTERVAL '24 hours'
  ORDER BY timestamp
  ```
  - X-axis: timestamp
  - Y-axis: temperature
  - Series: city

- **Temperature Comparison** (Bar chart)
  ```sql
  SELECT
    city,
    AVG(temperature) as avg_temp,
    MAX(temperature) as max_temp,
    MIN(temperature) as min_temp
  FROM weather_data
  WHERE timestamp > NOW() - INTERVAL '24 hours'
  GROUP BY city
  ```

### 3. Weather Metrics Dashboard

- **Humidity Levels** (Gauge)
  ```sql
  SELECT AVG(humidity) as avg_humidity
  FROM weather_data
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  ```

- **Wind Speed Distribution** (Bar chart)
  ```sql
  SELECT
    city,
    AVG(wind_speed) as avg_wind_speed
  FROM weather_data
  WHERE timestamp > NOW() - INTERVAL '24 hours'
  GROUP BY city
  ORDER BY avg_wind_speed DESC
  ```

- **Precipitation Tracking** (Area chart)
  ```sql
  SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    city,
    SUM(precipitation) as total_precipitation
  FROM weather_data
  WHERE timestamp > NOW() - INTERVAL '24 hours'
  GROUP BY hour, city
  ORDER BY hour
  ```

### 4. Daily Summary Dashboard

Use the pre-created `weather_summary` view:

```sql
SELECT * FROM weather_summary
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date DESC, city
```

**Visualizations:**
- Table view of all metrics
- Line chart for avg_temperature over time
- Bar chart for total_precipitation by city

## Dashboard Tips

1. **Auto-refresh**: Set dashboards to auto-refresh every 1-5 minutes
   - Click dashboard settings → Auto-refresh → Select interval

2. **Filters**: Add filters for:
   - Date range
   - City selection
   - Temperature thresholds

3. **Color coding**:
   - Temperature: Blue (cold) → Red (hot)
   - Humidity: White (dry) → Blue (humid)
   - Wind: Light → Dark (calm to strong)

4. **Alerts**: Set up alerts for extreme weather
   - Temperature > 35°C or < 0°C
   - Wind speed > 50 km/h
   - Heavy precipitation > 10mm/hour

## Sample Dashboard Layout

```
+----------------------------------+----------------------------------+
|  Current Temp - Istanbul         |  Current Temp - London           |
|  25°C                           |  18°C                           |
+----------------------------------+----------------------------------+
|                                                                     |
|  24-Hour Temperature Trend (Line Chart)                             |
|  All Cities                                                         |
|                                                                     |
+---------------------------------------------------------------------+
|  Avg Humidity    |  Avg Wind Speed  |  Total Precipitation          |
|  65%            |  12 km/h        |  2.5 mm                      |
+-----------------+------------------+----------------------------------+
|                                                                     |
|  Latest Weather Readings (Table)                                    |
|  City | Temp | Humidity | Wind | Timestamp                         |
|                                                                     |
+---------------------------------------------------------------------+
```

## Advanced Queries

### Temperature Anomalies
```sql
WITH city_stats AS (
  SELECT
    city,
    AVG(temperature) as avg_temp,
    STDDEV(temperature) as stddev_temp
  FROM weather_data
  WHERE timestamp > NOW() - INTERVAL '7 days'
  GROUP BY city
)
SELECT
  w.city,
  w.temperature,
  w.timestamp,
  ABS(w.temperature - cs.avg_temp) / cs.stddev_temp as z_score
FROM weather_data w
JOIN city_stats cs ON w.city = cs.city
WHERE ABS(w.temperature - cs.avg_temp) / cs.stddev_temp > 2
ORDER BY z_score DESC;
```

### Weather Correlation
```sql
SELECT
  city,
  CORR(temperature, humidity) as temp_humidity_corr,
  CORR(wind_speed, precipitation) as wind_precip_corr
FROM weather_data
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY city;
```

## Sharing Dashboards

1. **Public links**: Enable public sharing for stakeholder access
2. **Email subscriptions**: Schedule daily/weekly reports
3. **Slack integration**: Send alerts to Slack channels
4. **Embedding**: Embed dashboards in websites

## Troubleshooting

### No data showing
- Check if producer is running
- Verify Spark job is processing data
- Check PostgreSQL has records: `SELECT COUNT(*) FROM weather_data;`

### Slow queries
- Ensure indexes are created (done automatically in init-postgres.sql)
- Limit time ranges to improve performance
- Use the weather_summary view for aggregated data

### Dashboard not updating
- Check auto-refresh is enabled
- Verify data is flowing: check producer and Spark logs
- Refresh browser cache

## Resources

- [Metabase Documentation](https://www.metabase.com/docs/latest/)
- [SQL Tutorial](https://www.metabase.com/learn/sql-questions/sql-intro)
- [Visualization Best Practices](https://www.metabase.com/learn/visualization/)
