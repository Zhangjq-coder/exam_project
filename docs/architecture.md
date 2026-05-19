# Architecture Documentation

## Pipeline Overview

The AeroSense platform follows a modern data engineering architecture:

```
Sensor Data → Kafka → Spark Streaming → Data Lake → REST API → Clients
```

### Component Details

#### 1. Kafka Cluster (3 brokers, KRaft mode)
- 3 brokers eliminating single point of failure (no ZooKeeper)
- Replication factor 3, min.insync.replicas 2
- Topic sensor-events: 3 partitions, key-based partitioning
- Kafka UI accessible at http://localhost:8080
- KRaft mode removes ZooKeeper dependency, simplifying deployment

#### 2. Spark Structured Streaming Pipeline
- Reads from Kafka topic in streaming mode with spark-sql-kafka
- Parses JSON payloads using `from_json` with explicit StructType schema
- Validates records and filters physical outliers
- Applies 2-minute watermark on event_time for late data handling
- Computes 5-minute tumbling window aggregations per sensor type
- Triple write to data lake with independent checkpoints

#### 3. Three-Zone Data Lake
- **Raw Zone** (`raw/source=kafka/topic=sensor-events/`): Original JSON, partitioned by ingestion year/month/day/hour
- **Curated Zone** (`curated/domain=iot/`): Validated Parquet (Snappy), partitioned by sensor_type/year/month/day on event time
- **Consumption Zone** (`consumption/use_case=sensor_averages/`): Windowed aggregates in Parquet, partitioned by sensor_type/year/month

#### 4. REST API (Flask)
- 6 endpoints following REST principles
- Consistent JSON response structure for success and error
- Strict input validation: sensor type whitelist, days 1-90, numeric values
- Correct HTTP status codes: 200, 201, 400, 404, 422, 500
- Global error handlers for 404, 405, 500
- Server-side error logging via app.logger
- Direct integration with Kafka producer for publishing readings
- Direct integration with Parquet data lake for historical queries

#### 5. Analytics Layer
- Spark SQL batch queries on curated zone
- 4 analytical queries with CSV output
- Partition pruning demonstration with quantified speedup