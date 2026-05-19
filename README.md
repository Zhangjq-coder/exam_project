# AeroSense IoT Data Engineering Platform

## 1. Overview

AeroSense is an end-to-end data engineering platform for IoT sensor data. It ingests real-time sensor readings (temperature, humidity, pressure) via Kafka, processes them using Spark Structured Streaming, stores results in a three-zone data lake (Raw/Curated/Consumption), and exposes analytics through a Flask REST API.

**Technologies Used:**
- Apache Kafka 7.5 (3-broker KRaft cluster)
- Apache Spark 3.5 (Structured Streaming)
- Python 3.9+ (Producer, Analytics, API)
- Flask 3.0 (REST API)
- Parquet (Columnar storage with Snappy compression)

## 2. Architecture

```
+-------------------------+
| Python Generator        |
| (producer.py)           |
+-----------+-------------+
            |
            v
+-------------------------+
| Kafka Cluster (3 br.)   |
| topic: sensor-events    |
+-----------+-------------+
            |
    +-------+-------+
    |               |
    v               v
+-----------------------+    +----------------------+
| Spark Structured Str. |    | REST API (Flask)     |
| - parse JSON          |    | GET /sensors         |
| - watermark + window  |    | GET /latest          |
| - anomaly detection   |    | GET /stats           |
| - Parquet sink        |    | POST /readings       |
+-----------+-----------+    +----------+-----------+
            |                           |
            v                           |
+-----------------------+               |
| Data Lake (local)     | <-------------+
| raw / curated /       | Spark SQL reads
| consumption           |
+-----------------------+
```

**Components:**
- **producer.py**: Generates realistic sensor events with configurable rate and count
- **spark_pipeline.py**: Consumes from Kafka, validates, detects anomalies, computes 5-min windows
- **analytics.py**: Batch Spark SQL queries with partition pruning demonstration
- **api/app.py**: Flask REST API with 6 endpoints, input validation, and error handling

## 3. Instructions

### Prerequisites
- Docker / Docker Compose v2.0+
- Python 3.9+
- Apache Spark 3.5 (for spark-submit)

### Installation
```bash
pip install -r requirements.txt
```

### Step-by-Step Execution

1. **Start Kafka cluster:**
```bash
docker compose up -d
```

2. **Create Kafka topic:**
```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic sensor-events --partitions 3 --replication-factor 3
```

3. **Run producer:**
```bash
python src/producer.py --count 200 --rate 10 --source site-A-rack-12
```

4. **Submit Spark pipeline:**
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/spark_pipeline.py
```

5. **Run analytics:**
```bash
spark-submit src/analytics.py
```

6. **Start REST API:**
```bash
python -m api.app
```

### Test Commands
See `tests/test_curl_commands.sh` for comprehensive API testing.

## 4. Technical Choices

### Partitioning Strategy (Curated Zone)
Partitioned by `sensor_type/year/month/day` based on event time. `sensor_type` has low cardinality (3 values), making it ideal for partitioning. Time-based partitioning enables efficient date-range queries. Alternative considered: partitioning by `source`, but this would create too many partitions.

### Spark Structured Streaming OutputMode
- **Raw Zone**: `append` mode - each batch writes new raw records
- **Curated Zone**: `append` mode - validated records are appended once
- **Consumption Zone**: `update` mode - windowed aggregates are updated as new data arrives within the watermark window

### Replication Factor and min.insync.replicas
- **Replication Factor = 3**: Ensures data survives up to 2 broker failures
- **min.insync.replicas = 2**: Guarantees at least 2 replicas acknowledge writes before confirming, balancing durability and availability

### Event Time vs Ingestion Time
- **Raw Zone**: Uses ingestion time for partitioning to track when data arrived
- **Curated/Consumption Zones**: Use event time for business queries (e.g., "average temperature on Jan 15")
- Gap between times indicates pipeline latency or clock synchronization issues

### Delivery Semantics
Chosen: **At-least-once** with checkpointing. Limitations: duplicates possible if pipeline restarts. Exactly-once would require idempotent writes or transactional sinks, adding complexity.

## 5. Results

### Analytical Query Results
- Top anomaly hours: See `outputs/analytics/top_anomaly_hours/`
- Sensor statistics: See `outputs/analytics/sensor_statistics/`
- Temperature daily evolution: See `outputs/analytics/temperature_daily/`
- Partition pruning speedup: See `outputs/analytics/partition_pruning.txt`

### Sample API Response
```json
{
  "status": "ok",
  "service": "AeroSense REST API",
  "timestamp": "2024-01-15T10:30:00",
  "version": "1.0.0"
}
```

## 6. Limitations and Improvements

### Limitations
- Spark pipeline runs locally; production would use YARN/Kubernetes
- API keys stored in memory; production needs database
- No schema registry for Kafka messages
- Limited error recovery in streaming pipeline

### Improvements (with 2 extra days)
1. Add Delta Lake for ACID transactions and schema evolution
2. Implement Kafka Connect for automated data lake ingestion
3. Add Prometheus/Grafana monitoring for pipeline health
4. Implement automated file compaction for small file problem
5. Add OAuth2/JWT authentication for API
6. Create CI/CD pipeline for automated testing and deployment
