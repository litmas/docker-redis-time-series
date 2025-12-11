"""
Configuration settings for the Flask Time-Series API
"""

import os


class Config:
    # Redis Settings
    REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    REDIS_TTL_TIMESERIES = int(
        os.getenv('REDIS_TTL_TIMESERIES', 3600))  # 1 hour
    REDIS_TTL_PREDICTIONS = int(
        os.getenv('REDIS_TTL_PREDICTIONS', 86400))  # 24 hours

    # Model Settings
    MODEL_PATH = os.getenv('MODEL_PATH', '/app/models/timeseries_model.pkl')
    LOOKBACK_WINDOW = int(os.getenv('LOOKBACK_WINDOW', 20))
    DATA_PATH = os.getenv('DATA_PATH', '/app/data')
    PRICE_LOOKBACK = int(os.getenv('PRICE_LOOKBACK', 24))
    MIN_POINTS = int(os.getenv('MIN_POINTS', 6))
    DEFAULT_HORIZON_MINUTES = int(os.getenv('DEFAULT_HORIZON_MINUTES', 60))

    # Flask Settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() in ['true', '1', 'yes']
    HOST = os.getenv('FLASK_HOST', '0.0.0.0')
    PORT = int(os.getenv('FLASK_PORT', 5001))

    # API Settings
    MAX_DATA_POINTS_PER_REQUEST = int(
        os.getenv('MAX_DATA_POINTS_PER_REQUEST', 1000))
    MAX_SERIES_PER_INSTANCE = int(os.getenv('MAX_SERIES_PER_INSTANCE', 100))
    PRICE_THRESHOLD = float(os.getenv('PRICE_THRESHOLD', 80.0))
    LOW_PRICE_THRESHOLD = float(os.getenv('LOW_PRICE_THRESHOLD', 25.0))
    OFFPEAK_HOURS = os.getenv('OFFPEAK_HOURS', '0-6,22-23')
    SPIKE_DELTA_PCT = float(os.getenv('SPIKE_DELTA_PCT', 15.0))
    FORECAST_HORIZON_MINUTES = int(os.getenv('FORECAST_HORIZON_MINUTES', 240))
    FORECAST_POINTS = int(os.getenv('FORECAST_POINTS', 4))
    HISTORY_LOOKBACK_POINTS = int(os.getenv('HISTORY_LOOKBACK_POINTS', 48))
    PROCESS_EVERY_N = int(os.getenv('PROCESS_EVERY_N', 1))  # sample stream load (1=all messages)
    MAX_MESSAGES_PER_SECOND = int(os.getenv('MAX_MESSAGES_PER_SECOND', 200))  # backpressure guard

    # Streaming / Message Bus
    ENABLE_STREAM_CONSUMER = os.getenv('ENABLE_STREAM_CONSUMER', 'true').lower() in ['true', '1', 'yes']
    KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', '194.47.171.153:30092')
    ENERGY_TOPIC = os.getenv('ENERGY_TOPIC', 'meter-readings')
    PROCESSED_TOPIC = os.getenv('PROCESSED_TOPIC', 'energy-processed')
    CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'algorithm-processor')
    STREAM_HORIZON_MINUTES = int(os.getenv('STREAM_HORIZON_MINUTES', 60))
    MAX_STREAM_BUFFER = int(os.getenv('MAX_STREAM_BUFFER', 48))

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Kubernetes/Production Settings
    WORKERS = int(os.getenv('GUNICORN_WORKERS', 2))
    TIMEOUT = int(os.getenv('GUNICORN_TIMEOUT', 60))
    MAX_REQUESTS = int(os.getenv('GUNICORN_MAX_REQUESTS', 1000))
    MAX_REQUESTS_JITTER = int(os.getenv('GUNICORN_MAX_REQUESTS_JITTER', 100))


class DevelopmentConfig(Config):
    DEBUG = True


class ProductionConfig(Config):
    DEBUG = False


class TestingConfig(Config):
    TESTING = True
    REDIS_DB = 1  # Use different DB for testing


# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
