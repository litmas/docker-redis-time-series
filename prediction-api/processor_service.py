"""
Factory helpers to build the AlgorithmProcessor with app config, Redis, and Kafka.
"""

import logging
import json
from typing import Any

import redis
from kafka import KafkaProducer

from algorithm_processor import AlgorithmProcessor, DecisionEngine, ShortTermForecaster, StoragePublisher
from bus_consumer import BusConsumer

logger = logging.getLogger(__name__)


def parse_offpeak_ranges(raw_ranges: str):
    ranges = []
    for block in raw_ranges.split(","):
        block = block.strip()
        if not block:
            continue
        if "-" in block:
            start_s, end_s = block.split("-", 1)
            try:
                ranges.append((int(start_s), int(end_s)))
            except ValueError:
                continue
    return ranges


def build_redis_client(host: str, port: int, db: int) -> Any:
    try:
        client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        client.ping()
        return client
    except Exception as exc:
        logger.warning("Redis unavailable for AlgorithmProcessor", extra={"error": str(exc)})
        return None


def build_algorithm_processor(app_config, redis_client=None) -> AlgorithmProcessor:
    offpeak_ranges = parse_offpeak_ranges(app_config.OFFPEAK_HOURS)
    forecaster = ShortTermForecaster(spike_delta_pct=app_config.SPIKE_DELTA_PCT)
    decision_engine = DecisionEngine(
        price_threshold=app_config.PRICE_THRESHOLD,
        low_price_threshold=app_config.LOW_PRICE_THRESHOLD,
        offpeak_hours=offpeak_ranges,
    )

    producer = None
    if app_config.KAFKA_BROKERS:
        try:
            producer = KafkaProducer(
                bootstrap_servers=app_config.KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as exc:
            logger.warning("Kafka producer unavailable", extra={"error": str(exc)})

    storage_publisher = StoragePublisher(redis_client=redis_client, producer=producer, target_topic=app_config.PROCESSED_TOPIC)

    return AlgorithmProcessor(
        forecaster=forecaster,
        decision_engine=decision_engine,
        storage_publisher=storage_publisher,
        redis_client=redis_client,
        history_lookback=app_config.HISTORY_LOOKBACK_POINTS,
    )


def start_consumer_if_enabled(app_config, algorithm_processor: AlgorithmProcessor):
    if not app_config.ENABLE_STREAM_CONSUMER:
        return None
    consumer = BusConsumer(
        algorithm_processor=algorithm_processor,
        brokers=app_config.KAFKA_BROKERS,
        topic=app_config.ENERGY_TOPIC,
        group_id=app_config.CONSUMER_GROUP,
        process_every_n=app_config.PROCESS_EVERY_N,
        max_messages_per_second=app_config.MAX_MESSAGES_PER_SECOND,
    )
    consumer.start()
    return consumer
