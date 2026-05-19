"""
AeroSense Spark Structured Streaming Pipeline
Consumes sensor-events from Kafka, processes events, and writes to three-zone data lake.
"""

import logging
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, min, max, sum,
    when, current_timestamp, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class ProgressListener:
    """Streaming query listener that logs progress events."""
    def __init__(self, zone_name):
        self.zone_name = zone_name
        self.batch_count = 0
        self.total_rows = 0

    def on_query_progress(self, event):
        self.batch_count += 1
        rows = event['numInputRows']
        self.total_rows += rows
        logger.info(
            '[%s] Batch#%d | input=%d rows | total=%d rows | '
            'duration=%.2fs | offset=%s',
            self.zone_name, self.batch_count, rows, self.total_rows,
            event.get('batchDuration', 0) / 1000,
            event.get('sources', [{}])[0].get('endOffset', 'N/A')
        )

DATA_LAKE_PATH = 'file:///C:/tmp/datalake'
CHECKPOINT_BASE = 'file:///C:/tmp/datalake/checkpoints'
BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPIC = 'sensor-events'
RAW_PATH = DATA_LAKE_PATH + '/raw/source=kafka/topic=sensor-events'
CURATED_PATH = DATA_LAKE_PATH + '/curated/domain=iot'
CONSUMPTION_PATH = DATA_LAKE_PATH + '/consumption/use_case=sensor_averages'


def create_spark_session():
    """Create Spark session with required packages."""
    return SparkSession.builder \
        .appName('AeroSenseStreamingPipeline') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
        .config('spark.sql.shuffle.partitions', '4') \
        .getOrCreate()


def get_sensor_schema():
    """Define explicit schema for sensor events."""
    return StructType([
        StructField('sensor', StringType(), True),
        StructField('value', DoubleType(), True),
        StructField('unit', StringType(), True),
        StructField('timestamp', LongType(), True),
        StructField('source', StringType(), True),
        StructField('anomaly', BooleanType(), True),
    ])


def read_from_kafka(spark):
    """Subscribe to Kafka topic in streaming mode."""
    return spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
        .option('subscribe', TOPIC) \
        .option('startingOffsets', 'earliest') \
        .load()


def parse_and_validate(df):
    """Parse JSON payload, add event_time, and validate records."""
    schema = get_sensor_schema()

    parsed = df.select(
        from_json(col('value').cast('string'), schema).alias('data'),
        col('timestamp').alias('kafka_timestamp')
    ).select(
        col('data.*'),
        col('kafka_timestamp')
    )

    valid = parsed.filter(
        col('sensor').isNotNull() &
        col('value').isNotNull() &
        col('value').between(-100, 2000)
    )

    event_time_df = valid.withColumn(
        'event_time',
        (col('timestamp') / 1000).cast('timestamp')
    )

    return event_time_df


def detect_anomalies(df):
    """Add is_anomaly column based on business rules."""
    return df.withColumn(
        'is_anomaly',
        (
            (col('sensor') == 'temperature') & (col('value') > 35)
        ) | (
            (col('sensor') == 'humidity') & (col('value') > 90)
        ) | (
            (col('sensor') == 'pressure') & ((col('value') < 990) | (col('value') > 1030))
        )
    )


def write_raw_zone(df):
    """Write raw JSON to raw zone partitioned by ingestion year/month/day/hour."""
    checkpoint = os.path.join(CHECKPOINT_BASE, 'raw')

    enriched = df.withColumn('ingestion_time', current_timestamp()) \
        .withColumn('year', year(col('ingestion_time'))) \
        .withColumn('month', month(col('ingestion_time'))) \
        .withColumn('day', dayofmonth(col('ingestion_time'))) \
        .withColumn('hour', hour(col('ingestion_time')))

    return enriched.writeStream \
        .format('json') \
        .option('path', RAW_PATH) \
        .option('checkpointLocation', checkpoint) \
        .partitionBy('year', 'month', 'day', 'hour') \
        .outputMode('append') \
        .trigger(processingTime='30 seconds') \
        .start()


def write_curated_zone(df):
    """Write curated Parquet partitioned by sensor_type/year/month/day."""
    checkpoint = os.path.join(CHECKPOINT_BASE, 'curated')

    partitioned = df.withColumn('sensor_type', col('sensor')) \
        .withColumn('year', year(col('event_time'))) \
        .withColumn('month', month(col('event_time'))) \
        .withColumn('day', dayofmonth(col('event_time')))

    return partitioned.writeStream \
        .format('parquet') \
        .option('path', CURATED_PATH) \
        .option('checkpointLocation', checkpoint) \
        .option('compression', 'snappy') \
        .partitionBy('sensor_type', 'year', 'month', 'day') \
        .outputMode('append') \
        .trigger(processingTime='30 seconds') \
        .start()


def write_consumption_zone(df):
    """Write windowed aggregates to consumption zone."""
    checkpoint = os.path.join(CHECKPOINT_BASE, 'consumption')

    windowed = df.withWatermark('event_time', '2 minutes') \
        .groupBy(
            window(col('event_time'), '5 minutes'),
            col('sensor')
        ) \
        .agg(
            avg('value').alias('avg_value'),
            min('value').alias('min_value'),
            max('value').alias('max_value'),
            count('value').alias('observation_count'),
            sum(when(col('is_anomaly'), 1).otherwise(0)).alias('anomaly_count')
        ) \
        .select(
            col('window.start').alias('window_start'),
            col('window.end').alias('window_end'),
            col('sensor').alias('sensor_type'),
            col('avg_value'),
            col('min_value'),
            col('max_value'),
            col('observation_count'),
            col('anomaly_count')
        ) \
        .withColumn('year', year(col('window_start'))) \
        .withColumn('month', month(col('window_start')))

    return windowed.writeStream \
        .format('parquet') \
        .option('path', CONSUMPTION_PATH) \
        .option('checkpointLocation', checkpoint) \
        .partitionBy('sensor_type', 'year', 'month') \
        .outputMode('append') \
        .trigger(processingTime='30 seconds') \
        .start()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    logger.info('===========================================')
    logger.info('AeroSense Streaming Pipeline Starting...')
    logger.info('===========================================')
    logger.info('Bootstrap Servers : %s', BOOTSTRAP_SERVERS)
    logger.info('Topic             : %s', TOPIC)
    logger.info('Data Lake Path    : %s', DATA_LAKE_PATH)
    logger.info('Raw Path          : %s', RAW_PATH)
    logger.info('Curated Path      : %s', CURATED_PATH)
    logger.info('Consumption Path  : %s', CONSUMPTION_PATH)
    logger.info('Checkpoint Base   : %s', CHECKPOINT_BASE)

    t0 = time.time()
    logger.info('Connecting to Kafka cluster...')

    kafka_df = read_from_kafka(spark)
    parsed_df = parse_and_validate(kafka_df)
    enriched_df = detect_anomalies(parsed_df)

    elapsed = time.time() - t0
    logger.info('Kafka stream source created (%.1fs)', elapsed)
    logger.info('Starting three zone writers (trigger=30s)...')

    raw_query = write_raw_zone(enriched_df)
    curated_query = write_curated_zone(enriched_df)
    consumption_query = write_consumption_zone(enriched_df)

    raw_query.addListener(ProgressListener('RAW'))
    curated_query.addListener(ProgressListener('CURATED'))
    consumption_query.addListener(ProgressListener('CONSUMPTION'))

    logger.info('Starter status:')
    logger.info('  RAW         - isActive=%s, id=%s', raw_query.isActive, str(raw_query.id)[:8])
    logger.info('  CURATED     - isActive=%s, id=%s', curated_query.isActive, str(curated_query.id)[:8])
    logger.info('  CONSUMPTION - isActive=%s, id=%s', consumption_query.isActive, str(consumption_query.id)[:8])
    logger.info('Waiting for micro-batches (trigger every 30s)...')

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
