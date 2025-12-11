"""
Message consumer that subscribes to a Kafka topic, validates incoming price
messages, and delegates to the AlgorithmProcessor.
"""

import json
import logging
import threading
import time
from typing import Any, Optional

from algorithm_processor import AlgorithmProcessor

try:
    from kafka import KafkaConsumer
except ImportError:  # pragma: no cover
    KafkaConsumer = None

logger = logging.getLogger(__name__)


class BusConsumer:
    def __init__(
        self,
        algorithm_processor: AlgorithmProcessor,
        brokers: str,
        topic: str,
        group_id: str,
        process_every_n: int = 1,
        max_messages_per_second: int = 200,
    ) -> None:
        self.processor = algorithm_processor
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.process_every_n = max(1, process_every_n)
        self.max_messages_per_second = max(1, max_messages_per_second)
        self.consumer: Optional[Any] = None
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._msg_counter = 0
        self._window_start = time.time()
        self._window_count = 0

    def start(self) -> None:
        if not KafkaConsumer:
            logger.warning("kafka-python not installed; BusConsumer not started.")
            return

        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(target=self._run_loop, name="bus-consumer", daemon=True)
        self._thread.start()
        logger.info("Bus consumer started", extra={"topic": self.topic})

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.brokers.split(","),
                group_id=self.group_id,
                value_deserializer=lambda v: v.decode("utf-8"),
                enable_auto_commit=True,
                auto_offset_reset="latest",
                consumer_timeout_ms=5000,
            )
        except Exception as exc:
            logger.error("Failed to start Kafka consumer", extra={"error": str(exc)})
            return

        while not self._stop_event.is_set():
            try:
                for msg in self.consumer:
                    if self._stop_event.is_set():
                        break
                    raw = self._parse(msg.value)
                    if not raw:
                        continue

                    # lightweight backpressure guard: skip if local consumption bursts too high
                    if not self._allow_now():
                        continue

                    # sample stream to avoid overloading prediction path
                    if (self._msg_counter % self.process_every_n) != 0:
                        self._msg_counter += 1
                        continue

                    self._msg_counter += 1
                    try:
                        self.processor.process(raw)
                    except Exception as exc:
                        logger.warning("Processing failed", extra={"error": str(exc), "payload": raw})
                time.sleep(0.3)
            except Exception as exc:
                logger.error("Bus consumer loop error", extra={"error": str(exc)})
                time.sleep(1.0)

        if self.consumer:
            self.consumer.close()

    def _parse(self, raw: str) -> Optional[dict]:
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _allow_now(self) -> bool:
        """Simple per-instance rate limiter to avoid CPU thrash under bursts."""
        now = time.time()
        if now - self._window_start >= 1.0:
            self._window_start = now
            self._window_count = 0
        self._window_count += 1
        return self._window_count <= self.max_messages_per_second
