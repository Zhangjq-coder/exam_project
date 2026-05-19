"""
AeroSense IoT Sensor Data Producer
Generates realistic sensor events and publishes them to Kafka topic 'sensor-events'.
Supports --count, --rate, and --source arguments.
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
TOPIC = 'sensor-events'

SENSOR_CONFIG = {
    'temperature': {'unit': 'C', 'min': 15, 'max': 45, 'anomaly_low': 35, 'anomaly_high': 60},
    'humidity': {'unit': '%', 'min': 30, 'max': 95, 'anomaly_low': 90, 'anomaly_high': 100},
    'pressure': {'unit': 'hPa', 'min': 980, 'max': 1040, 'anomaly_low': 970, 'anomaly_high': 989, 'anomaly_high_alt': 1031, 'anomaly_high_alt_max': 1050},
}

SOURCES = ['site-A-rack-12', 'site-B-rack-05', 'site-C-rack-08', 'site-D-rack-03']

running = True


def signal_handler(sig, frame):
    global running
    running = False
    logger.info('Shutdown signal received, flushing producer...')


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def create_producer():
    """Create and configure Kafka producer with reliability settings."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
        batch_size=16384,
    )


def generate_reading(sensor_type, source, force_anomaly=False):
    """Generate a single sensor reading with realistic or anomalous values."""
    config = SENSOR_CONFIG[sensor_type]
    is_anomaly = force_anomaly or random.random() < 0.10

    if is_anomaly:
        if sensor_type == 'temperature':
            value = round(random.uniform(config['anomaly_low'], config['anomaly_high']), 2)
        elif sensor_type == 'humidity':
            value = round(random.uniform(config['anomaly_low'], config['anomaly_high']), 2)
        elif sensor_type == 'pressure':
            if random.random() < 0.5:
                value = round(random.uniform(config['anomaly_low'], config['anomaly_high']), 2)
            else:
                value = round(random.uniform(config['anomaly_high_alt'], config['anomaly_high_alt_max']), 2)
    else:
        value = round(random.uniform(config['min'], config['max']), 2)

    return {
        'sensor': sensor_type,
        'value': value,
        'unit': config['unit'],
        'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
        'source': source,
        'anomaly': is_anomaly,
    }


def main():
    parser = argparse.ArgumentParser(description='AeroSense IoT Sensor Producer')
    parser.add_argument('--count', type=int, default=100, help='Number of events to produce')
    parser.add_argument('--rate', type=float, default=10.0, help='Events per second')
    parser.add_argument('--source', type=str, default=None, help='Site identifier (e.g., site-A-rack-12)')
    args = parser.parse_args()

    source = args.source or random.choice(SOURCES)
    sensor_types = list(SENSOR_CONFIG.keys())
    interval = 1.0 / args.rate if args.rate > 0 else 0

    logger.info(f'Starting producer: count={args.count}, rate={args.rate}/s, source={source}')

    producer = create_producer()
    sent = 0
    failed = 0

    try:
        for i in range(args.count):
            if not running:
                break

            sensor_type = random.choice(sensor_types)
            force_anomaly = (i % 10 == 0)
            reading = generate_reading(sensor_type, source, force_anomaly)

            future = producer.send(
                TOPIC,
                value=reading,
                key=sensor_type,
            )

            try:
                record_metadata = future.get(timeout=10)
                sent += 1
                if sent % 50 == 0:
                    logger.info(f'Sent {sent}/{args.count} messages')
            except Exception as e:
                failed += 1
                logger.error(f'Failed to send message: {e}')

            if interval > 0:
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info('Interrupted by user')
    finally:
        producer.flush()
        producer.close()
        logger.info(f'Producer finished: sent={sent}, failed={failed}')


if __name__ == '__main__':
    main()
