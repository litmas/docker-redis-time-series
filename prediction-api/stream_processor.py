import json
import logging
import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional

from spot_price_predictor import SpotPricePredictor

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:  # pragma: no cover - optional dependency for local runs
    KafkaConsumer = None
    KafkaProducer = None


logger = logging.getLogger(__name__)


class EnergyStreamProcessor:
    """
    Lightweight Kafka/NATS-style consumer that keeps a small sliding window
    per source (customer/area) and emits price predictions to a downstream topic.
    """

    def __init__(
        self,
        predictor: SpotPricePredictor,
        brokers: str,
        source_topic: str,
        target_topic: str,
        group_id: str,
        horizon_minutes: int = 60,
        buffer_size: int = 48,
    ) -> None:
        self.predictor = predictor
        self.brokers = brokers
        self.source_topic = source_topic
        self.target_topic = target_topic
        self.group_id = group_id
        self.horizon_minutes = horizon_minutes
        self.buffer_size = buffer_size

        self.consumer: Optional[Any] = None
        self.producer: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.buffers: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=self.buffer_size))

    def start_in_background(self) -> None:
        if not self.brokers or not KafkaConsumer:
            logger.warning("Stream consumer not started. Missing brokers or kafka-python.")
            return

        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(target=self._run_loop, name="energy-stream-consumer", daemon=True)
        self._thread.start()
        logger.info("Energy stream consumer started in background", extra={"topic": self.source_topic})

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        try:
            self.consumer = KafkaConsumer(
                self.source_topic,
                bootstrap_servers=self.brokers.split(","),
                group_id=self.group_id,
                value_deserializer=lambda v: v.decode("utf-8"),
                consumer_timeout_ms=5000,
                enable_auto_commit=True,
                auto_offset_reset="latest",
            )
            self.producer = KafkaProducer(
                bootstrap_servers=self.brokers.split(","), value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as exc:
            logger.error("Failed to start Kafka consumer/producer", extra={"error": str(exc)})
            return

        while not self._stop_event.is_set():
            try:
                for msg in self.consumer:
                    if self._stop_event.is_set():
                        break

                    record = self._parse_message(msg.value)
                    if not record:
                        continue

                    series_id = record["series_id"]
                    self.buffers[series_id].append(record)

                    try:
                        result = self.predictor.predict(list(self.buffers[series_id]), self.horizon_minutes)
                        payload = self._format_payload(record, result)
                        if self.producer:
                            self.producer.send(self.target_topic, payload)
                        else:
                            logger.info("Predicted price (no producer configured)", extra=payload)
                    except Exception as exc:
                        logger.warning("Prediction failed for stream record", extra={"error": str(exc)})

                # brief pause to avoid busy loops when consumer_timeout triggers
                time.sleep(0.5)
            except Exception as exc:
                logger.error("Stream consumer loop error", extra={"error": str(exc)})
                time.sleep(2)

        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()

    def _parse_message(self, raw_value: str) -> Optional[Dict[str, Any]]:
        try:
            record = json.loads(raw_value)
        except Exception:
            return None

        price = record.get("Price") or record.get("price") or record.get("spot_price")
        timestamp = (
            record.get("DateTime")
            or record.get("timestamp")
            or record.get("time")
            or record.get("date")
        )
        customer = record.get("CUSTOMER") or record.get("customer") or "default"
        area = record.get("AREA") or record.get("area") or ""

        if price is None or timestamp is None:
            return None

        try:
            price_val = float(price)
        except (TypeError, ValueError):
            return None

        series_id = f"{area}:{customer}"
        return {
            "timestamp": timestamp,
            "price": price_val,
            "series_id": series_id,
            "raw": record,
        }

    def _format_payload(self, record: Dict[str, Any], prediction_result: Any) -> Dict[str, Any]:
        return {
            "series_id": record.get("series_id"),
            "area": record.get("raw", {}).get("AREA") or record.get("raw", {}).get("area"),
            "customer": record.get("raw", {}).get("CUSTOMER") or record.get("raw", {}).get("customer"),
            "ingest_timestamp": record.get("timestamp"),
            "predicted_price_next_minutes": prediction_result.horizon_minutes,
            "predicted_price": prediction_result.predicted_price,
            "confidence": prediction_result.confidence,
            "trend": prediction_result.trend,
            "change_pct": prediction_result.change_pct,
            "recommendation": prediction_result.recommendation,
            "meta": {
                "supporting_points": prediction_result.supporting_points,
                "lookback_used": prediction_result.lookback_used,
                "interval_minutes": prediction_result.interval_minutes,
            },
        }
