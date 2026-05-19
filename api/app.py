"""
AeroSense REST API
Exposes sensor data and analytics through REST endpoints.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from flask import Flask, request, jsonify

from api.kafka_utils import KafkaProducerWrapper, KafkaConsumerWrapper
from api.lake_utils import DataLakeReader

app = Flask(__name__)

kafka_producer = KafkaProducerWrapper()
lake_reader = DataLakeReader()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALLOWED_SENSORS = ['temperature', 'humidity', 'pressure']


def validate_sensor_type(sensor_type):
    """Validate that sensor type is one of the allowed values."""
    return sensor_type in ALLOWED_SENSORS


def validate_days_param(days_str):
    """Validate days parameter is an integer between 1 and 90."""
    try:
        days = int(days_str)
        return 1 <= days <= 90
    except (ValueError, TypeError):
        return False


def validate_reading_payload(payload):
    """Validate sensor reading payload. Returns (is_valid, error_type, message)."""
    if not isinstance(payload, dict):
        return False, 'malformed', 'Request body must be a JSON object'

    required_fields = ['sensor', 'value', 'unit', 'source']
    for field in required_fields:
        if field not in payload:
            return False, 'malformed', f'Missing required field: {field}'

    if not isinstance(payload['sensor'], str) or not validate_sensor_type(payload['sensor']):
        return False, 'malformed', f'Invalid sensor type. Must be one of: {ALLOWED_SENSORS}'

    if not isinstance(payload['value'], (int, float)):
        return False, 'malformed', 'Value must be a number'

    if not isinstance(payload['unit'], str) or len(payload['unit']) == 0:
        return False, 'malformed', 'Unit must be a non-empty string'

    if not isinstance(payload['source'], str) or len(payload['source']) == 0:
        return False, 'malformed', 'Source must be a non-empty string'

    return True, None, 'Valid'


@app.route('/api/v1/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'service': 'AeroSense REST API',
        'timestamp': datetime.utcnow().isoformat()
    }), 200


@app.route('/api/v1/sensors', methods=['GET'])
def list_sensors():
    """List all available sensor types."""
    return jsonify({
        'sensor_types': ALLOWED_SENSORS,
        'count': len(ALLOWED_SENSORS)
    }), 200


@app.route('/api/v1/sensors/<sensor_type>/latest', methods=['GET'])
def get_latest_reading(sensor_type):
    """Get the latest reading for a specific sensor type from Kafka."""
    if not validate_sensor_type(sensor_type):
        return jsonify({
            'status': 'error',
            'message': f'Invalid sensor type: {sensor_type}',
            'valid_types': ALLOWED_SENSORS
        }), 400

    try:
        latest = kafka_producer.get_latest_reading(sensor_type)
        if latest:
            return jsonify({
                'status': 'ok',
                'sensor_type': sensor_type,
                'reading': latest
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': f'No readings found for sensor type: {sensor_type}'
            }), 404
    except Exception as e:
        app.logger.error(f'Error getting latest reading: {e}')
        return jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500


@app.route('/api/v1/sensors/<sensor_type>/stats', methods=['GET'])
def get_sensor_stats(sensor_type):
    """Get daily statistics for a sensor type from the data lake."""
    if not validate_sensor_type(sensor_type):
        return jsonify({
            'status': 'error',
            'message': f'Invalid sensor type: {sensor_type}',
            'valid_types': ALLOWED_SENSORS
        }), 400

    days_str = request.args.get('days', '7')
    if not validate_days_param(days_str):
        return jsonify({
            'status': 'error',
            'message': 'Days parameter must be an integer between 1 and 90'
        }), 400

    try:
        days = int(days_str)
        stats = lake_reader.get_sensor_stats(sensor_type, days)

        if stats:
            return jsonify({
                'status': 'ok',
                'sensor_type': sensor_type,
                'days_requested': days,
                'statistics': stats
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': f'No statistics found for sensor type: {sensor_type}'
            }), 404
    except Exception as e:
        app.logger.error(f'Error getting sensor stats: {e}')
        return jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500


@app.route('/api/v1/anomalies', methods=['GET'])
def get_anomalies():
    """Get list of recent anomalies."""
    sensor_type = request.args.get('sensor')
    limit_str = request.args.get('limit', '10')

    if sensor_type and not validate_sensor_type(sensor_type):
        return jsonify({
            'status': 'error',
            'message': f'Invalid sensor type: {sensor_type}',
            'valid_types': ALLOWED_SENSORS
        }), 400

    try:
        limit = int(limit_str)
        if limit <= 0 or limit > 100:
            return jsonify({
                'status': 'error',
                'message': 'Limit must be between 1 and 100'
            }), 400
    except ValueError:
        return jsonify({
            'status': 'error',
            'message': 'Limit must be a valid integer'
        }), 400

    try:
        anomalies = lake_reader.get_recent_anomalies(sensor_type, limit)
        return jsonify({
            'status': 'ok',
            'anomalies': anomalies,
            'count': len(anomalies),
            'sensor_filter': sensor_type
        }), 200
    except Exception as e:
        app.logger.error(f'Error getting anomalies: {e}')
        return jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500


@app.route('/api/v1/readings', methods=['POST'])
def publish_reading():
    """Publish a new sensor reading to Kafka."""
    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({
            'status': 'error',
            'message': 'Request body must be valid JSON'
        }), 400

    is_valid, error_type, msg = validate_reading_payload(payload)
    if not is_valid:
        return jsonify({
            'status': 'error',
            'message': msg
        }), 422

    try:
        success = kafka_producer.publish_reading(payload)
        if success:
            return jsonify({
                'status': 'ok',
                'message': 'Reading published successfully',
                'reading': payload
            }), 201
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to publish reading to Kafka'
            }), 500
    except Exception as e:
        app.logger.error(f'Error publishing reading: {e}')
        return jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'status': 'error',
        'message': 'Endpoint not found'
    }), 404


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'status': 'error',
        'message': 'Method not allowed'
    }), 405


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'status': 'error',
        'message': 'Internal server error'
    }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
