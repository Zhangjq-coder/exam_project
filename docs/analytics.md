# Analytics Documentation

## Query Results Summary

### Query 1: Top 5 Hours with Most Anomalies
This query identifies peak anomaly periods across all sensor types. Results are written to `outputs/analytics/top_anomaly_hours/`.

**Business Value:** Helps operations team identify when sensor anomalies are most frequent for proactive maintenance scheduling.

**Query Logic:** Filters `is_anomaly = true`, groups by hour formatted as `yyyy-MM-dd HH`, counts anomalies per hour, and returns top 5.

### Query 2: Sensor Type Statistics
Computes global mean, min, max, standard deviation, and anomaly rate per sensor type. Results saved to `outputs/analytics/sensor_statistics/`.

**Key Metrics:**
- Mean value indicates typical operating conditions per sensor type
- Standard deviation shows natural variability of each measurement
- Anomaly rate percentage helps assess overall data quality and sensor health

**Expected Columns:** sensor_type, mean_value, min_value, max_value, stddev_value, total_count, anomaly_count, anomaly_rate

### Query 3: Temperature Daily Evolution
Tracks daily mean temperature and anomaly count over time. Results saved to `outputs/analytics/temperature_daily/`.

**Use Case:** Trend analysis for predictive maintenance. Rising daily means could indicate cooling system degradation; increasing anomaly counts may signal sensor malfunction.

**Expected Columns:** date, daily_mean, daily_anomaly_count

### Query 4: Partition Pruning Demonstration

**Test Query:** Count temperature records across the curated zone.

**Methodology:**
- **Full scan query:** `SELECT COUNT(*) FROM curated WHERE sensor_type = 'temperature'`
  Reads all year/month/day partitions, including those for other sensor types
- **Pruned scan query:** `SELECT COUNT(*) FROM curated WHERE sensor_type = 'temperature' AND year = 2024 AND month = 1`
  Restricts scans to only the `sensor_type=temperature` directory and within `year=2024/month=01`

**How to Interpret Results:**
- The speedup factor = (full scan time) / (pruned scan time)
- With proper Hive-style partitioning, Spark's query planner reads directory metadata and skips entire partitions that don't match filters
- The speedup is proportional to the number of partitions skipped: if there are 12 months, filtering to 1 month should reduce I/O by roughly 12x
- At larger scales (TB), this optimization can reduce query time from hours to seconds

**Output:** Timing results are saved to `outputs/analytics/partition_pruning.txt` and printed to stdout.

**Expected Output Format:**
```
Full scan count: <N>, time: <X.XXXX>s
Pruned scan count: <M>, time: <Y.YYYY>s
Speedup factor: <Z.ZZ>x
```

## Notes for Reviewer

All query results are:
- Printed to standard output during script execution
- Saved as CSV files in `outputs/analytics/` with headers
- Readable in the Parquet files directly under `/tmp/datalake/curated/domain=iot/`