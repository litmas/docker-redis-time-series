from flask import Flask, request, jsonify
import redis
import numpy as np
import pandas as pd
import pickle
import json
import logging
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any
import traceback
from config import config
from spot_price_predictor import SpotPricePredictor
from processor_service import build_algorithm_processor, start_consumer_if_enabled

# Get configuration based on environment
config_name = os.getenv('FLASK_ENV', 'default')
app_config = config[config_name]

# Configure logging (JSON-friendly messages for Kubernetes)
logging.basicConfig(level=getattr(logging, app_config.LOG_LEVEL), format='%(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(app_config)

# Configuration from config class
REDIS_HOST = app_config.REDIS_HOST
REDIS_PORT = app_config.REDIS_PORT
REDIS_DB = app_config.REDIS_DB
DATA_PATH = app_config.DATA_PATH

# Model configuration
MODEL_PATH = app_config.MODEL_PATH
LOOKBACK_WINDOW = app_config.LOOKBACK_WINDOW

# Initialize Redis connection
try:
    redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client.ping()
    logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    redis_client = None


class TimeSeriesPredictor:
    def __init__(self):
        self.model = None
        self.scaler = None
        self.feature_columns = None
        self.load_model()

    def load_model(self):
        """Load the trained model from disk"""
        try:
            if os.path.exists(MODEL_PATH):
                with open(MODEL_PATH, 'rb') as f:
                    model_data = pickle.load(f)
                    self.model = model_data.get('model')
                    self.scaler = model_data.get('scaler')
                    self.lookback_window = model_data.get('lookback_window', 5)
                logger.info(f"Model loaded successfully from {MODEL_PATH}")
            else:
                logger.warning(
                    f"Model file not found at {MODEL_PATH}. Using dummy model.")
                self._create_dummy_model()
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            self._create_dummy_model()

    def _create_dummy_model(self):
        """Create a simple but intelligent trend-following model for demonstration purposes"""
        class SmartDummyModel:
            def predict(self, X):
                # Intelligent trend prediction for 2D scikit-learn format
                if len(X.shape) == 2 and X.shape[1] >= 2:
                    # Get the sequence values (features are the lookback window)
                    sequence = X[0]  # First (and typically only) sample
                    
                    # Calculate trends and patterns
                    if len(sequence) >= 2:
                        # Calculate differences between consecutive points
                        diffs = np.diff(sequence)
                        
                        if len(diffs) >= 3:
                            # Look for patterns in the differences
                            recent_diffs = diffs[-3:]  # Last 3 differences
                            
                            # Check if differences are consistent (arithmetic sequence)
                            if np.std(recent_diffs) < 0.1:  # Very consistent differences
                                # It's an arithmetic sequence, use the consistent difference
                                next_value = sequence[-1] + np.mean(recent_diffs)
                            else:
                                # Use weighted average of recent differences (more weight to recent)
                                weights = np.array([0.3, 0.5, 0.2]) if len(recent_diffs) == 3 else np.ones(len(recent_diffs))
                                weighted_diff = np.average(recent_diffs, weights=weights[:len(recent_diffs)])
                                next_value = sequence[-1] + weighted_diff
                        else:
                            # Not enough differences, use simple linear trend
                            diff = diffs[-1] if len(diffs) > 0 else 0
                            next_value = sequence[-1] + diff
                    else:
                        # Only one point, can't determine trend
                        next_value = sequence[-1]
                    
                    return np.array([float(next_value)])
                
                elif len(X.shape) == 2 and X.shape[1] == 1:
                    # Only one data point
                    return X[0]
                else:
                    return np.array([100.0])  # Fallback

        self.model = SmartDummyModel()
        self.scaler = None
        self.lookback_window = 5
        logger.info("Using smart trend-following dummy model for demonstration")

    def predict(self, historical_data: List[Dict]) -> float:
        """Make a prediction based on historical data"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(historical_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            # Extract values
            values = df['value'].values.astype(float)

            # Get lookback window (use instance variable or fallback)
            window = getattr(self, 'lookback_window', LOOKBACK_WINDOW)

            # Prepare features (last 'window' values)
            if len(values) >= window:
                features = values[-window:]
            else:
                # Pad with the mean if not enough data
                mean_val = np.mean(values) if len(values) > 0 else 100.0
                features = np.full(window, mean_val)
                features[-len(values):] = values

            # Reshape for scikit-learn (1 sample, window features)
            X = features.reshape(1, -1)

            # Apply scaling if available
            if self.scaler:
                X = self.scaler.transform(X)

            # Make prediction
            prediction = self.model.predict(X)

            return float(prediction[0])

        except Exception as e:
            logger.error(f"Prediction error: {e}")
            logger.error(traceback.format_exc())
            raise


# Initialize predictor
predictor = TimeSeriesPredictor()
spot_price_predictor = SpotPricePredictor(
    lookback=app_config.PRICE_LOOKBACK,
    min_points=app_config.MIN_POINTS
)
algorithm_processor = build_algorithm_processor(app_config, redis_client)
stream_processor = start_consumer_if_enabled(app_config, algorithm_processor)


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with API documentation"""
    endpoints = {
        'health': {
            'url': '/health',
            'method': 'GET',
            'description': 'Health check endpoint'
        },
        'predict': {
            'url': '/predict',
            'method': 'POST',
            'description': 'Make time-series predictions',
            'example': {
                'series_id': 'default',
                'historical_data': [
                    {'timestamp': '2025-01-01T00:00:00', 'value': 100.0},
                    {'timestamp': '2025-01-01T01:00:00', 'value': 101.0}
                ]
            }
        },
        'predict_spot_price': {
            'url': '/predict/spot-price',
            'method': 'POST',
            'description': 'Predict near-term electricity spot price with DSM action',
            'example': {
                'records': [
                    {'DateTime': '2020-01-06 16:00:00', 'Price': 34.05, 'AREA': 'Stensö'}
                ],
                'horizon_minutes': 60
            }
        },
        'process_message': {
            'url': '/process/message',
            'method': 'POST',
            'description': 'Run full algorithm processor decision flow on a single incoming message'
        },
        'add_data': {
            'url': '/add_data',
            'method': 'POST',
            'description': 'Add new data points to Redis',
            'example': {
                'series_id': 'default',
                'data_points': [
                    {'timestamp': '2025-01-01T02:00:00', 'value': 102.0}
                ]
            }
        },
        'status': {
            'url': '/status/<series_id>',
            'method': 'GET',
            'description': 'Get status information for a specific time series'
        },
        'reload_model': {
            'url': '/reload_model',
            'method': 'POST',
            'description': 'Reload the model from disk'
        },
        'model_metadata': {
            'url': '/model/metadata',
            'method': 'GET',
            'description': 'Get model metadata and training metrics'
        },
        'ingestion_status': {
            'url': '/data/ingestion/status',
            'method': 'GET',
            'description': 'Get data ingestion status and metrics'
        },
        'historical_summary': {
            'url': '/data/historical/summary',
            'method': 'GET',
            'description': 'Get summary of all data stored in Redis'
        },
        'clear_cache': {
            'url': '/admin/clear_cache',
            'method': 'POST',
            'description': 'Clear all time-series data from Redis (admin function)'
        }
    }
    
    return jsonify({
        'message': 'Time Series Prediction API',
        'version': '1.0.0',
        'status': 'running',
        'redis_connected': redis_client is not None if redis_client else False,
        'model_loaded': predictor.model is not None,
        'available_endpoints': endpoints,
        'documentation': 'Use POST endpoints with JSON data. All timestamps should be in ISO format (YYYY-MM-DDTHH:MM:SS)'
    })


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    status = {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'redis_connected': redis_client is not None,
        'model_loaded': predictor.model is not None,
        'algorithm_processor_ready': algorithm_processor is not None,
        'stream_consumer_running': stream_processor is not None and app_config.ENABLE_STREAM_CONSUMER
    }

    if redis_client:
        try:
            redis_client.ping()
            status['redis_status'] = 'connected'
        except:
            status['redis_status'] = 'disconnected'
            status['redis_connected'] = False

    return jsonify(status)


def validate_data_point(point, series_id):
    """
    Validate a single data point before storing in Redis.
    Returns (is_valid, error_message, cleaned_point)
    """
    try:
        # Check required fields
        if not isinstance(point, dict):
            return False, "Data point must be a dictionary", None
            
        timestamp = point.get('timestamp')
        value = point.get('value')
        
        if timestamp is None:
            return False, "Missing 'timestamp' field", None
            
        if value is None:
            return False, "Missing 'value' field", None
        
        # Validate timestamp format
        try:
            # Use pandas to_datetime which is more robust and matches what the predictor uses
            parsed_timestamp = pd.to_datetime(timestamp)
            # Convert back to standardized ISO format
            clean_timestamp = parsed_timestamp.strftime('%Y-%m-%dT%H:%M:%S')
        except (ValueError, TypeError, pd.errors.ParserError) as e:
            return False, f"Invalid timestamp format '{timestamp}': {str(e)}", None
        
        # Validate value is numeric
        try:
            clean_value = float(value)
            if not np.isfinite(clean_value):  # Check for NaN, inf, -inf
                return False, f"Value must be a finite number, got: {value}", None
        except (ValueError, TypeError):
            return False, f"Value must be numeric, got: {value} (type: {type(value)})", None
        
        # Validate series_id
        if not isinstance(series_id, str) or len(series_id.strip()) == 0:
            return False, "series_id must be a non-empty string", None
        
        # Return cleaned data point
        cleaned_point = {
            'timestamp': clean_timestamp,
            'value': clean_value,
            'series_id': series_id.strip()
        }
        
        return True, None, cleaned_point
        
    except Exception as e:
        return False, f"Validation error: {str(e)}", None


@app.route('/predict/spot-price', methods=['POST'])
def predict_spot_price():
    """Predict near-term electricity spot price and suggest DSM action."""
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({'error': 'No JSON data provided'}), 400

    records = payload.get('records') or payload.get('data') or payload.get('historical_data')
    if not records or not isinstance(records, list):
        return jsonify({'error': "Payload must include 'records' as a non-empty list"}), 400

    try:
        horizon = int(payload.get('horizon_minutes', app_config.DEFAULT_HORIZON_MINUTES))
    except (TypeError, ValueError):
        horizon = app_config.DEFAULT_HORIZON_MINUTES

    try:
        result = spot_price_predictor.predict(records, horizon_minutes=horizon)
    except ValueError as exc:
        logger.warning(f"Validation error on spot price prediction: {exc}")
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        logger.error(f"Spot price prediction failed: {exc}")
        return jsonify({'error': 'Prediction failed', 'details': str(exc)}), 500

    prediction_key = f"predicted_price_next_{horizon}min"
    response = {
        prediction_key: result.predicted_price,
        'confidence': result.confidence,
        'trend': result.trend,
        'change_pct': result.change_pct,
        'volatility': result.volatility,
        'explanation': result.explanation,
        'recommendation': result.recommendation,
        'metadata': {
            'horizon_minutes': result.horizon_minutes,
            'lookback_used': result.lookback_used,
            'supporting_points': result.supporting_points,
            'interval_minutes': result.interval_minutes,
        }
    }

    logger.info(json.dumps({
        'event': 'spot_price_prediction',
        'prediction': response[prediction_key],
        'confidence': response['confidence'],
        'trend': response['trend'],
        'horizon_minutes': horizon
    }))

    return jsonify(response), 200


@app.route('/process/message', methods=['POST'])
def process_message():
    """Run the full algorithm processor decision flow on a single message."""
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({'error': 'No JSON payload provided'}), 400

    try:
        decision = algorithm_processor.process(payload)
        response = {
            'actionType': decision.action_type,
            'currentPrice': decision.current_price,
            'threshold': decision.threshold,
            'predictedValues': decision.predicted_values,
            'predictedSpike': decision.predicted_spike,
            'confidenceScore': decision.confidence_score,
            'timestamp': decision.timestamp.isoformat(),
            'explanation': decision.explanation
        }
        return jsonify(response), 200
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        logger.error(f"Processing failed: {exc}")
        return jsonify({'error': 'Processing failed', 'details': str(exc)}), 500


@app.route('/predict', methods=['POST'])
def predict():
    """Make a time-series prediction"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400

        # Get series_id for this time series (default to 'default')
        series_id = data.get('series_id', 'default')

        # Store new historical data in Redis if provided
        if 'historical_data' in data:
            new_historical_data = data['historical_data']

            # Validate historical data format
            if not isinstance(new_historical_data, list) or len(new_historical_data) == 0:
                return jsonify({'error': 'historical_data must be a non-empty list'}), 400

            # Validate and clean all data points before storing ANYTHING in Redis
            validated_points = []
            validation_errors = []
            
            for i, point in enumerate(new_historical_data):
                is_valid, error_msg, cleaned_point = validate_data_point(point, series_id)
                if is_valid:
                    validated_points.append(cleaned_point)
                else:
                    validation_errors.append(f"Point {i+1}: {error_msg}")
            
            # If ANY validation failed, reject the entire request
            if validation_errors:
                return jsonify({
                    'error': 'Data validation failed',
                    'validation_errors': validation_errors,
                    'message': 'No data was stored due to validation errors'
                }), 400
            
            # Only store data if ALL points are valid
            if redis_client and validated_points:
                try:
                    stored_count = 0
                    for cleaned_point in validated_points:
                        timestamp = cleaned_point['timestamp']
                        value = cleaned_point['value']
                        
                        # Store individual point (using cleaned data)
                        point_key = f"timeseries:{series_id}:{timestamp}"
                        redis_data = {
                            'timestamp': timestamp,
                            'value': value,
                            'series_id': series_id
                        }
                        redis_client.set(point_key, json.dumps(redis_data))
                        
                        # Add to timeline (timestamp already validated)
                        try:
                            timestamp_score = datetime.fromisoformat(timestamp).timestamp()
                            redis_client.zadd(f"timeline:{series_id}", {timestamp: timestamp_score})
                            stored_count += 1
                        except Exception as timeline_error:
                            logger.error(f"Failed to add timestamp {timestamp} to timeline: {timeline_error}")
                            # Remove the problematic point from Redis to maintain consistency
                            redis_client.delete(point_key)
                    
                    if stored_count > 0:
                        # Add to active series only if we successfully stored some data
                        redis_client.sadd("series:active", series_id)
                        logger.info(f"Stored {stored_count} validated data points for series {series_id} in Redis")
                    else:
                        return jsonify({'error': 'Failed to store any data points due to internal errors'}), 500
                        
                except Exception as e:
                    logger.error(f"Failed to store validated data in Redis: {e}")
                    return jsonify({'error': f'Failed to store data in Redis: {str(e)}'}), 500

        # Now retrieve ALL available data for this series from Redis for prediction
        if redis_client:
            try:
                # Get all timestamps for this series from the timeline
                timeline_key = f"timeline:{series_id}"
                timestamps = redis_client.zrange(timeline_key, 0, -1)
                
                if not timestamps:
                    # If new data was just added but no existing timeline, use the new data
                    if 'historical_data' in data:
                        historical_data = data['historical_data']
                        logger.info(f"Using {len(historical_data)} newly provided data points for series {series_id}")
                    else:
                        return jsonify({'error': f'No data found for series_id: {series_id}. Please provide historical_data in the request or ingest data first.'}), 400
                else:
                    # Fetch all data points from Redis and combine with any new data
                    all_data_points = []
                    
                    for timestamp in timestamps:
                        point_key = f"timeseries:{series_id}:{timestamp}"
                        point_data = redis_client.get(point_key)
                        if point_data:
                            point = json.loads(point_data)
                            all_data_points.append({
                                'timestamp': point['timestamp'],
                                'value': point['value']
                            })
                    
                    # Sort by timestamp to ensure chronological order
                    all_data_points.sort(key=lambda x: x['timestamp'])
                    
                    # Use the most recent data points for prediction (up to LOOKBACK_WINDOW)
                    historical_data = all_data_points[-LOOKBACK_WINDOW:] if len(all_data_points) > LOOKBACK_WINDOW else all_data_points
                    
                    # Update the recent cache with the latest data
                    recent_key = f"recent:{series_id}"
                    redis_client.set(recent_key, json.dumps(historical_data))
                    
                    logger.info(f"Retrieved {len(historical_data)} data points from Redis timeline for series {series_id} (total available: {len(all_data_points)})")

            except Exception as e:
                logger.error(f"Error retrieving data from Redis: {e}")
                return jsonify({'error': 'Failed to retrieve data from Redis cache'}), 500

        else:
            # Mode 3: Stateless prediction - no Redis and no historical data
            # Generate dummy historical data for demonstration purposes
            logger.warning(f"Making stateless prediction for series {series_id} - no historical context available")
            
            # Create dummy/default historical data points
            # This will produce inconsistent results, demonstrating the need for stateful storage
            base_time = datetime.utcnow() - timedelta(days=5)
            base_value = data.get('value', 100.0)  # Use provided value or default to 100
            
            historical_data = []
            for i in range(LOOKBACK_WINDOW):
                # Generate some dummy historical points with slight variation
                dummy_value = base_value + np.random.normal(0, 1)  # Add small random variation
                historical_data.append({
                    'timestamp': (base_time + timedelta(days=i)).isoformat(),
                    'value': float(dummy_value)
                })
            
            stateless_mode = True

        # Make prediction
        try:
            prediction_value = predictor.predict(historical_data)

            # Prepare response
            response = {
                'prediction': prediction_value,
                'series_id': series_id,
                'timestamp': datetime.utcnow().isoformat(),
                'data_points_used': len(historical_data),
                'model_type': 'LSTM' if hasattr(predictor.model, 'layers') else 'smart_trend_following'
            }
            
            # Add warnings and context for stateless predictions
            if 'stateless_mode' in locals() and stateless_mode:
                response.update({
                    'warning': 'STATELESS PREDICTION: No historical context available',
                    'note': 'This prediction is based on randomly generated dummy data and will be inconsistent across requests',
                    'recommendation': 'Use Redis for stateful predictions or provide historical_data in the request',
                    'redis_connected': False,
                    'prediction_quality': 'LOW - No historical context'
                })
            else:
                response.update({
                    'redis_connected': redis_client is not None,
                    'prediction_quality': 'HIGH - Based on historical data'
                })

            # Store prediction in Redis for monitoring
            if redis_client:
                try:
                    prediction_key = f"predictions:{series_id}:{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                    redis_client.setex(prediction_key, 86400, json.dumps(
                        response))  # Keep for 24 hours
                except Exception as e:
                    logger.warning(f"Failed to store prediction in Redis: {e}")

            logger.info(
                f"Prediction made for series {series_id}: {prediction_value}")
            return jsonify(response)

        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return jsonify({'error': f'Prediction failed: {str(e)}'}), 500

    except Exception as e:
        logger.error(f"Request processing error: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': f'Request processing failed: {str(e)}'}), 500


@app.route('/add_data', methods=['POST'])
def add_data():
    """Add new data points to Redis (single source of truth)"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400

        series_id = data.get('series_id', 'default')
        new_data_points = data.get('data_points', [])

        if not isinstance(new_data_points, list) or len(new_data_points) == 0:
            return jsonify({'error': 'data_points must be a non-empty list'}), 400

        if not redis_client:
            return jsonify({'error': 'Redis not available for data storage'}), 500

        # Validate all data points before storing ANYTHING
        validated_points = []
        validation_errors = []
        
        for i, point in enumerate(new_data_points):
            is_valid, error_msg, cleaned_point = validate_data_point(point, series_id)
            if is_valid:
                validated_points.append(cleaned_point)
            else:
                validation_errors.append(f"Point {i+1}: {error_msg}")
        
        # If ANY validation failed, reject the entire request
        if validation_errors:
            return jsonify({
                'error': 'Data validation failed',
                'validation_errors': validation_errors,
                'message': 'No data was stored due to validation errors'
            }), 400

        try:
            points_added = 0
            
            for cleaned_point in validated_points:
                timestamp = cleaned_point['timestamp']
                value = cleaned_point['value']
                
                # Store individual point (persistent) using validated data
                point_key = f"timeseries:{series_id}:{timestamp}"
                redis_data = {
                    'timestamp': timestamp,
                    'value': value,
                    'series_id': series_id
                }
                redis_client.set(point_key, json.dumps(redis_data))
                
                # Add to timeline for ordering (timestamp already validated)
                try:
                    timestamp_score = datetime.fromisoformat(timestamp).timestamp()
                    redis_client.zadd(f"timeline:{series_id}", {timestamp: timestamp_score})
                    points_added += 1
                except Exception as timeline_error:
                    logger.error(f"Failed to add timestamp {timestamp} to timeline: {timeline_error}")
                    # Remove the problematic point from Redis to maintain consistency
                    redis_client.delete(point_key)
            
            # Update recent cache
            recent_key = f"recent:{series_id}"
            
            # Get recent timeline entries
            recent_timestamps = redis_client.zrevrange(f"timeline:{series_id}", 0, LOOKBACK_WINDOW-1)
            recent_data = []
            
            for ts in recent_timestamps:
                point_key = f"timeseries:{series_id}:{ts}"
                point_data = redis_client.get(point_key)
                if point_data:
                    point = json.loads(point_data)
                    recent_data.append({
                        'timestamp': point['timestamp'],
                        'value': point['value']
                    })
            
            # Reverse to get chronological order
            recent_data.reverse()
            redis_client.set(recent_key, json.dumps(recent_data))
            
            # Add to active series
            redis_client.sadd("series:active", series_id)

            response = {
                'status': 'success',
                'series_id': series_id,
                'points_added': points_added,
                'recent_points_cached': len(recent_data),
                'timestamp': datetime.utcnow().isoformat(),
                'storage_type': 'redis_persistent'
            }

            logger.info(f"Added {points_added} data points to series {series_id} in Redis")
            return jsonify(response)

        except Exception as e:
            logger.error(f"Failed to store data in Redis: {e}")
            return jsonify({'error': f'Failed to store data: {str(e)}'}), 500

    except Exception as e:
        logger.error(f"Add data error: {e}")
        return jsonify({'error': f'Failed to add data: {str(e)}'}), 500


@app.route('/status/<series_id>', methods=['GET'])
def get_series_status(series_id):
    """Get status information for a specific time series from Redis"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500

        # Check if series exists
        if not redis_client.sismember("series:active", series_id):
            return jsonify({
                'series_id': series_id,
                'status': 'no_data',
                'data_points': 0,
                'message': 'No data found for this series'
            })

        # Get timeline info
        timeline_key = f"timeline:{series_id}"
        total_points = redis_client.zcard(timeline_key)
        
        # Get data range
        earliest_ts = redis_client.zrange(timeline_key, 0, 0)
        latest_ts = redis_client.zrevrange(timeline_key, 0, 0)
        
        # Get recent predictions
        prediction_pattern = f"predictions:{series_id}:*"
        prediction_keys = redis_client.keys(prediction_pattern)
        recent_predictions = len(prediction_keys)

        response = {
            'series_id': series_id,
            'status': 'active',
            'total_data_points': total_points,
            'recent_predictions': recent_predictions,
            'data_range': {
                'earliest': earliest_ts[0] if earliest_ts else None,
                'latest': latest_ts[0] if latest_ts else None
            },
            'storage_type': 'redis_persistent',
            'timestamp': datetime.utcnow().isoformat()
        }

        return jsonify(response)

    except Exception as e:
        logger.error(f"Status check error: {e}")
        return jsonify({'error': f'Status check failed: {str(e)}'}), 500


@app.route('/reload_model', methods=['POST'])
def reload_model():
    """Reload the model from disk (useful after retraining)"""
    try:
        global predictor
        predictor = TimeSeriesPredictor()

        return jsonify({
            'status': 'success',
            'message': 'Model reloaded successfully',
            'timestamp': datetime.utcnow().isoformat(),
            'model_loaded': predictor.model is not None
        })

    except Exception as e:
        logger.error(f"Model reload error: {e}")
        return jsonify({'error': f'Model reload failed: {str(e)}'}), 500


@app.route('/model/metadata', methods=['GET'])
def get_model_metadata():
    """Get model metadata and training metrics"""
    try:
        metadata = {}
        
        # Try to get metadata from Redis
        if redis_client:
            try:
                redis_metadata = redis_client.get("model:metadata")
                if redis_metadata:
                    metadata.update(json.loads(redis_metadata))
            except Exception as e:
                logger.warning(f"Could not retrieve metadata from Redis: {e}")
        
        # Add current model info
        metadata.update({
            'model_path': MODEL_PATH,
            'model_loaded': predictor.model is not None,
            'lookback_window': LOOKBACK_WINDOW,
            'current_timestamp': datetime.utcnow().isoformat()
        })
        
        return jsonify(metadata)
        
    except Exception as e:
        logger.error(f"Error getting model metadata: {e}")
        return jsonify({'error': f'Failed to get metadata: {str(e)}'}), 500


@app.route('/data/ingestion/status', methods=['GET'])
def get_ingestion_status():
    """Get data ingestion status and metrics"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        # Get ingestion metrics
        metrics_key = "ingestion:metrics"
        ingestion_data = redis_client.get(metrics_key)
        
        if ingestion_data:
            metrics = json.loads(ingestion_data)
        else:
            metrics = {'status': 'no_data', 'message': 'No ingestion data available'}
        
        # Get count of stored series
        series_keys = redis_client.keys("timeseries:*")
        metrics['active_series'] = len(series_keys)
        metrics['series_list'] = [key.split(':')[1] for key in series_keys]
        
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"Error getting ingestion status: {e}")
        return jsonify({'error': f'Failed to get ingestion status: {str(e)}'}), 500


@app.route('/data/historical/summary', methods=['GET'])
def get_historical_data_summary():
    """Get summary of all data stored in Redis"""
    try:
        summary = {
            'storage_type': 'redis_persistent',
            'active_series': [],
            'total_data_points': 0,
            'total_series': 0
        }
        
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
        
        try:
            # Get all active series
            active_series = redis_client.smembers("series:active")
            summary['total_series'] = len(active_series)
            
            for series_id in active_series:
                timeline_key = f"timeline:{series_id}"
                point_count = redis_client.zcard(timeline_key)
                
                # Get date range
                earliest_ts = redis_client.zrange(timeline_key, 0, 0)
                latest_ts = redis_client.zrevrange(timeline_key, 0, 0)
                
                series_info = {
                    'series_id': series_id,
                    'data_points': point_count,
                    'earliest_timestamp': earliest_ts[0] if earliest_ts else None,
                    'latest_timestamp': latest_ts[0] if latest_ts else None
                }
                
                summary['active_series'].append(series_info)
                summary['total_data_points'] += point_count
            
            # Get training summary if available
            training_summary = redis_client.get("training:data_summary")
            if training_summary:
                summary['last_training'] = json.loads(training_summary)
        
        except Exception as e:
            logger.error(f"Error getting Redis data summary: {e}")
            summary['error'] = str(e)
        
        return jsonify(summary)
        
    except Exception as e:
        logger.error(f"Error getting data summary: {e}")
        return jsonify({'error': f'Failed to get data summary: {str(e)}'}), 500


@app.route('/admin/clear_cache', methods=['POST'])
def clear_redis_cache():
    """Clear all time-series data from Redis (admin function)"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
        
        # Get all time-series related keys
        patterns = ["timeseries:*", "recent:*", "timeline:*", "predictions:*"]
        total_deleted = 0
        
        for pattern in patterns:
            keys = redis_client.keys(pattern)
            if keys:
                deleted = redis_client.delete(*keys)
                total_deleted += deleted
        
        # Clear active series set
        redis_client.delete("series:active")
        
        # Clear training summary
        redis_client.delete("training:data_summary")
        
        return jsonify({
            'status': 'success',
            'message': f'Cleared {total_deleted} data entries from Redis',
            'timestamp': datetime.utcnow().isoformat(),
            'storage_type': 'redis_persistent'
        })
        
    except Exception as e:
        logger.error(f"Error clearing Redis data: {e}")
        return jsonify({'error': f'Failed to clear data: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
