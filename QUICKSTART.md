# Hızlı Başlangıç Rehberi

## Servisleri İlk Kez Başlatma

Docker image'ları indiriliyor. İndirme tamamlandığında:

```bash
# Tüm servisleri başlat
docker-compose up -d

# Servislerin çalıştığını kontrol et
docker ps

# Tüm 7 servisin "Up" durumunda olması gerekli:
# - zookeeper
# - kafka
# - postgres
# - minio
# - metabase
# - spark-master
# - spark-worker
```

## Servisler Başladıktan Sonra

### 1. Producer'ı Başlat

```bash
cd producer
python3 weather_producer.py
```

Producer her 60 saniyede bir şu şehirlerden veri toplayacak:
- İstanbul
- Ankara
- İzmir
- Londra
- New York
- Tokyo

Çıktıda şunu göreceksiniz:
```
INFO - Sent data for Istanbul to Kafka topic 'weather-data'
INFO - Sent data for Ankara to Kafka topic 'weather-data'
...
```

### 2. Spark Streaming Job'ı Başlat (Yeni Terminal)

```bash
./scripts/run-spark-job.sh
```

Bu job:
- Kafka'dan veri okur
- PostgreSQL'e kaydeder
- MinIO'ya (Data Lake) yazar

### 3. Test Consumer (Opsiyonel - Yeni Terminal)

Kafka'dan gelen verileri görmek için:

```bash
cd consumer
python3 kafka_consumer_test.py
```

## Sorun Giderme

### Port 5432 zaten kullanımda hatası
Postgres portu 5433'e değiştirildi. Harici bağlantı için:
```bash
psql -h localhost -p 5433 -U weather_user -d weather_db
```

### Docker servisleri başlamıyor
```bash
# Tüm logları gör
docker-compose logs -f

# Belirli bir servisin loglarını gör
docker logs kafka
docker logs spark-master
```

### Kafka bağlantı hatası
30 saniye bekle, Kafka başlatılması zaman alıyor:
```bash
# Kafka'nın hazır olup olmadığını kontrol et
docker logs kafka | grep "started"
```

## Web Arayüzleri

- **Spark Master UI**: http://localhost:8080
  - Worker'ların bağlandığını burada görebilirsiniz

- **MinIO Console**: http://localhost:9001
  - Login: minioadmin / minioadmin
  - weather-lake bucket'ını burada görebilirsiniz

- **Metabase**: http://localhost:3000
  - İlk açılışta setup yapmanız gerekecek
  - PostgreSQL bağlantı bilgileri:
    - Host: `postgres` (Docker network içinden)
    - Port: `5432` (internal)
    - Database: `weather_db`
    - User: `weather_user`
    - Password: `weather_pass`

## Veri Akışını Doğrulama

### 1. PostgreSQL'de veri olup olmadığını kontrol et

```bash
docker exec -it postgres psql -U weather_user -d weather_db
```

PostgreSQL içinde:
```sql
SELECT COUNT(*) FROM weather_data;
SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 5;
SELECT * FROM weather_summary;
```

### 2. MinIO'da dosyaları kontrol et

http://localhost:9001 → weather-lake → raw/

Tarih bazlı partition'lar ve parquet dosyaları göreceksiniz.

### 3. Spark Job'ın çalıştığını kontrol et

http://localhost:8080 → Running Applications

## Servisleri Durdurma

```bash
# Servisleri durdur
docker-compose down

# Servisleri durdur VE tüm verileri sil
docker-compose down -v
```

## Yardımcı Komutlar

```bash
# Tüm servislerin durumunu gör
make status

# Tüm logları takip et
make logs

# Sadece Kafka loglarını gör
docker logs -f kafka

# Sadece Spark loglarını gör
docker logs -f spark-master
docker logs -f spark-worker
```

## Beklenen Veri Akışı

1. Producer → Kafka (her 60 saniyede 6 şehir = 6 mesaj)
2. Spark → Kafka'dan okur
3. Spark → PostgreSQL'e yazar
4. Spark → MinIO'ya yazar
5. Metabase → PostgreSQL'den okur ve görselleştirir

## Sorun mu var?

1. Docker Desktop'ın çalıştığından emin olun
2. Tüm servislerin "Up" durumunda olduğunu kontrol edin: `docker ps`
3. Log'ları kontrol edin: `docker-compose logs`
4. Port çakışması varsa docker-compose.yml'deki portları değiştirin
