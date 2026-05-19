"""
Kafka utility functions for the REST API.
Handles producing and consuming sensor events.
"""

import json
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
TOPIC = 'sensor-events'

logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    """Wrapper for Kafka producer operations."""

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks='all',
            retries=3,
        )

    def publish_reading(self, payload):
        """Publish a sensor reading to Kafka."""
        try:
            reading = {
                'sensor': payload['sensor'],
                'value': float(payload['value']),
                'unit': payload.get('unit', 'C'),
                'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
                'source': payload['source'],
                'anomaly': False,
            }

            future = self.producer.send(
                TOPIC,
                value=reading,
                key=payload['sensor'],
            )
            future.get(timeout=10)
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f'Failed to publish reading: {e}')
            return False

    def get_latest_reading(self, sensor_type):
        """Get the latest reading for a sensor type."""
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                consumer_timeout_ms=5000,
            )

            # Consume a few messages to find the latest matching one
            latest = None
            for msg in consumer:
                if msg.value['sensor'] == sensor_type:
                    latest = msg.value

            consumer.close()
            return latest
        except Exception as e:
            logger.error(f'Failed to get latest reading: {e}')
            return None

    def close(self):
        """Close the producer."""
        if self.producer:
            self.producer.close()


class KafkaConsumerWrapper:
    """Wrapper for Kafka consumer operations."""

    def __init__(self, group_id='api-consumer'):
        self.consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=group_id,
            auto_offset_reset='earliest',
            consumer_timeout_ms=3000,
        )

    def consume_messages(self, max_messages=10):
        """Consume messages from Kafka."""
        messages = []
        for msg in self.consumer:
            messages.append(msg.value)
            if len(messages) >= max_messages:
                break
        return messages

    def close(self):
        """Close the consumer."""
        if self.consumer:
            self.consumer.close()
