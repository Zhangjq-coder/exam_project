"""
AeroSense Analytics Script
Executes analytical queries on the data lake using Spark SQL and demonstrates partition pruning.
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, stddev, sum, when, desc, hour, date_format, to_date

DATA_LAKE_PATH = 'file:///C:/tmp/datalake'
CURATED_PATH = DATA_LAKE_PATH + '/curated/domain=iot'
OUTPUT_DIR = 'outputs/analytics'


def create_spark_session():
    """Create Spark session for batch analytics."""
    return SparkSession.builder \
        .appName('AeroSenseAnalytics') \
        .config('spark.sql.shuffle.partitions', '4') \
        .getOrCreate()


def load_curated_data(spark):
    """Load curated zone data as a DataFrame."""
    return spark.read.parquet(CURATED_PATH)


def query_top_anomaly_hours(df):
    """Query 1: Top 5 hours with the highest number of anomalies."""
    print('\n=== Query 1: Top 5 Hours with Most Anomalies ===')

    result = df.filter(col('is_anomaly') == True) \
        .groupBy(date_format(col('event_time'), 'yyyy-MM-dd HH').alias('hour')) \
        .agg(count('*').alias('anomaly_count')) \
        .orderBy(desc('anomaly_count')) \
        .limit(5)

    result.show(truncate=False)
    result.coalesce(1).write.mode('overwrite').csv(
        os.path.join(OUTPUT_DIR, 'top_anomaly_hours'), header=True
    )
    return result


def query_sensor_statistics(df):
    """Query 2: Global statistics per sensor type."""
    print('\n=== Query 2: Sensor Type Statistics ===')

    result = df.groupBy('sensor_type') \
        .agg(
            avg('value').alias('mean_value'),
            min('value').alias('min_value'),
            max('value').alias('max_value'),
            stddev('value').alias('stddev_value'),
            count('*').alias('total_count'),
            sum(when(col('is_anomaly'), 1).otherwise(0)).alias('anomaly_count')
        ) \
        .withColumn(
            'anomaly_rate',
            (col('anomaly_count') / col('total_count')) * 100
        )

    result.show(truncate=False)
    result.coalesce(1).write.mode('overwrite').csv(
        os.path.join(OUTPUT_DIR, 'sensor_statistics'), header=True
    )
    return result


def query_temperature_daily(df):
    """Query 3: Daily evolution of temperature mean and anomaly count."""
    print('\n=== Query 3: Temperature Daily Evolution ===')

    result = df.filter(col('sensor_type') == 'temperature') \
        .groupBy(to_date(col('event_time')).alias('date')) \
        .agg(
            avg('value').alias('daily_mean'),
            sum(when(col('is_anomaly'), 1).otherwise(0)).alias('daily_anomaly_count')
        ) \
        .orderBy('date')

    result.show(truncate=False)
    result.coalesce(1).write.mode('overwrite').csv(
        os.path.join(OUTPUT_DIR, 'temperature_daily'), header=True
    )
    return result


def demonstrate_partition_pruning(spark):
    """Query 4: Demonstrate partition pruning performance impact."""
    print('\n=== Query 4: Partition Pruning Demonstration ===')

    count_query = "SELECT COUNT(*) FROM curated WHERE sensor_type = 'temperature'"
    pruned_query = "SELECT COUNT(*) FROM curated WHERE sensor_type = 'temperature' AND year = 2024 AND month = 1"

    df = spark.read.parquet(CURATED_PATH)
    df.createOrReplaceTempView('curated')

    spark.sql('ANALYZE TABLE curated COMPUTE STATISTICS')

    start_full = time.time()
    result_full = spark.sql(count_query).collect()[0][0]
    time_full = time.time() - start_full

    start_pruned = time.time()
    result_pruned = spark.sql(pruned_query).collect()[0][0]
    time_pruned = time.time() - start_pruned

    speedup = time_full / time_pruned if time_pruned > 0 else float('inf')

    print(f'Full scan count: {result_full}, time: {time_full:.4f}s')
    print(f'Pruned scan count: {result_pruned}, time: {time_pruned:.4f}s')
    print(f'Speedup factor: {speedup:.2f}x')

    with open(os.path.join(OUTPUT_DIR, 'partition_pruning.txt'), 'w') as f:
        f.write(f'Full scan: {time_full:.4f}s\n')
        f.write(f'Pruned scan: {time_pruned:.4f}s\n')
        f.write(f'Speedup: {speedup:.2f}x\n')


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    print('Loading curated data...')
    df = load_curated_data(spark)
    print(f'Loaded {df.count()} records from curated zone')

    query_top_anomaly_hours(df)
    query_sensor_statistics(df)
    query_temperature_daily(df)
    demonstrate_partition_pruning(spark)

    print('\nAll analytics queries completed. Results saved to outputs/analytics/')
    spark.stop()


if __name__ == '__main__':
    main()
