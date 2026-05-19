"""
Data Lake utility functions for the REST API.
Handles reading from curated and consumption zones.
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta

DATA_LAKE_PATH = '/tmp/datalake'
CURATED_PATH = os.path.join(DATA_LAKE_PATH, 'curated', 'domain=iot')
CONSUMPTION_PATH = os.path.join(DATA_LAKE_PATH, 'consumption', 'use_case=sensor_averages')

logger = logging.getLogger(__name__)


class DataLakeReader:
    """Reader for the data lake zones."""

    def __init__(self):
        self.curated_path = CURATED_PATH
        self.consumption_path = CONSUMPTION_PATH

    def get_sensor_stats(self, sensor_type, days=7):
        """Get statistics for a sensor type from the curated zone."""
        try:
            if not os.path.exists(self.curated_path):
                logger.warning('Curated zone path does not exist')
                return None

            df = pd.read_parquet(self.curated_path)
            df = df[df['sensor_type'] == sensor_type]

            cutoff_date = datetime.now() - timedelta(days=days)
            df['event_time'] = pd.to_datetime(df['event_time'])
            df = df[df['event_time'] >= cutoff_date]

            if df.empty:
                return None

            stats = {
                'mean': round(df['value'].mean(), 2),
                'min': round(df['value'].min(), 2),
                'max': round(df['value'].max(), 2),
                'std': round(df['value'].std(), 2),
                'count': int(len(df)),
                'anomaly_count': int(df['is_anomaly'].sum()),
                'anomaly_rate': round((df['is_anomaly'].sum() / len(df)) * 100, 2),
            }

            return stats
        except Exception as e:
            logger.error(f'Failed to get sensor stats: {e}')
            return None

    def get_recent_anomalies(self, sensor_type=None, limit=10):
        """Get recent anomalies from the curated zone."""
        try:
            if not os.path.exists(self.curated_path):
                logger.warning('Curated zone path does not exist')
                return []

            df = pd.read_parquet(self.curated_path)
            df = df[df['is_anomaly'] == True]

            if sensor_type:
                df = df[df['sensor_type'] == sensor_type]

            df['event_time'] = pd.to_datetime(df['event_time'])
            df = df.sort_values('event_time', ascending=False).head(limit)

            anomalies = []
            for _, row in df.iterrows():
                anomalies.append({
                    'sensor': row['sensor_type'],
                    'value': round(row['value'], 2),
                    'unit': row['unit'],
                    'source': row['source'],
                    'timestamp': row['event_time'].isoformat(),
                })

            return anomalies
        except Exception as e:
            logger.error(f'Failed to get anomalies: {e}')
            return []

    def get_consumption_data(self, sensor_type=None):
        """Get aggregated data from the consumption zone."""
        try:
            if not os.path.exists(self.consumption_path):
                logger.warning('Consumption zone path does not exist')
                return None

            df = pd.read_parquet(self.consumption_path)

            if sensor_type:
                df = df[df['sensor_type'] == sensor_type]

            if df.empty:
                return None

            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f'Failed to get consumption data: {e}')
            return None
