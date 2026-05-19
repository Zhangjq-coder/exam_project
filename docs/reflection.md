# Reflection Questions

## Question 1: Pipeline Crash Impact and Checkpoint Strategy

**Scenario:** Pipeline crashes after writing to raw zone but before writing to curated zone.

**Impact:** Raw zone contains the data, but curated zone is missing those records. The data is not lost but the curated zone is incomplete.

**Prevention Strategy:** Use separate checkpoints for each sink. When the pipeline restarts:
- Raw zone checkpoint ensures no duplicate writes to raw
- Curated zone checkpoint causes Spark to reprocess from last committed Kafka offset
- Since raw zone uses append mode with its own checkpoint, data already written is not duplicated
- Curated zone catches up from the last successful write offset

**Best Practice:** At-least-once semantics with checkpointing ensures no data loss, though duplicates are possible in curated zone after restart. Exactly-once would require transactional sinks.

---

## Question 2: Scaling to 50,000 Messages/Second

**First Bottlenecks:**
1. **Kafka Producer**: Single-threaded producer cannot sustain 50k msg/s. Fix: Increase batch_size and linger_ms, or use async producer with callback batching
2. **Spark Processing**: Trigger interval of 30 seconds limits throughput. Fix: Reduce trigger to 5 seconds, increase spark.sql.shuffle.partitions
3. **Small File Problem**: Many small Parquet files from frequent triggers. Fix: Implement file compaction job, or use Delta Lake's OPTIMIZE
4. **Network I/O**: Kafka broker bandwidth may saturate. Fix: Add more brokers to distribute load

**Architecture Fixes:**
- Deploy Spark on Kubernetes/YARN for resource elasticity
- Use Kafka Connect for automated topic-to-lake ingestion
- Implement Delta Lake for automatic small file compaction and schema evolution
- Add Prometheus/Grafana monitoring for bottleneck detection

---

## Question 3: Kafka vs Parquet Data Lake for Historical Data

**Kafka as Source of Truth:**
- ✅ Ordered by partition key, real-time access, replayable offsets
- ❌ Expensive long-term storage, limited query capability, not columnar
- **Best for:** Event streaming, real-time dashboards, short-term buffer (hours/days)

**Parquet Data Lake:**
- ✅ Columnar storage, efficient compression (Snappy), Spark SQL support, cost-effective at scale
- ❌ Batch-oriented, not real-time, requires ETL pipeline
- **Best for:** Historical analysis, ML training, regulatory compliance

**Recommendation:** Kafka retains data for 7 days as real-time buffer; Spark streams data to data lake for permanent storage. Use Kafka for latest readings API, data lake for statistics and analytics.

---

## Question 4: Sensor Emitting Aberrant Values

**Detection Strategy:**
- Spark pipeline computes `is_anomaly` column independently from producer flag
- Temperature > 35°C OR Humidity > 90% OR Pressure < 990 hPa OR > 1030 hPa
- All raw data preserved; anomalies marked but not deleted

**Isolation Strategy:**
1. All records written to raw zone (JSON) for audit trail
2. No records filtered out of curated zone - anomaly flag allows downstream consumers to decide
3. Consumption zone aggregates include `anomaly_count` for monitoring trends
4. API `/api/v1/anomalies` endpoint exposes recent anomalies for operational review

**Benefits:** Preserves complete data lineage. Operations team can investigate anomaly patterns without losing any raw data. If a sensor is confirmed faulty, filters can be added without data loss.

---

## Question 5: Adding New Sensor Type (co2)

**Files to Modify:**

1. **src/producer.py:**
   - Add 'co2' to SENSOR_CONFIG with range (300-5000 ppm)
   - Update anomaly thresholds in generate_reading()
   - No schema change needed (sensor field is StringType)

2. **src/spark_pipeline.py:**
   - Add anomaly detection rule: co2 value > 4000
   - No schema or partition changes needed

3. **api/app.py:**
   - Add 'co2' to ALLOWED_SENSORS list
   - validate_sensor_type() auto-handles new type

4. **src/analytics.py:**
   - No changes needed (queries use groupBy on sensor_type, which is generic)

5. **api/lake_utils.py:**
   - No changes needed (uses generic sensor_type filtering)

**Total Changes:** 3 files require minimal modifications (producer.py, spark_pipeline.py, app.py). The three-zone architecture and analytics queries are designed to handle new sensor types without structural changes.