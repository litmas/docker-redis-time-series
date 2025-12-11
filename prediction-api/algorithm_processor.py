"""
Algorithm Processor implementing end-to-end decision flow:
1) Consume price messages from message bus.
2) Enrich with historical data and forecast next 2–4 hours.
3) Apply multi-scenario decision logic (critical/spike/low/normal).
4) Store and publish the decision for downstream systems.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
from dateutil import parser

logger = logging.getLogger(__name__)


# --- Data models -----------------------------------------------------------------


@dataclass
class PriceMessage:
    timestamp: datetime
    price: float
    area: str
    customer: str
    raw: Dict[str, Any]


@dataclass
class ForecastResult:
    predicted_values: List[float]
    confidence: float
    predicted_spike: bool
    explanation: str


@dataclass
class DecisionResult:
    action_type: str
    explanation: str
    current_price: float
    threshold: float
    predicted_values: List[float]
    predicted_spike: bool
    confidence_score: float
    timestamp: datetime


# --- Prediction module -----------------------------------------------------------


class ShortTermForecaster:
    """Lightweight forecaster using linear trend extrapolation over recent history."""

    def __init__(self, spike_delta_pct: float = 15.0):
        self.spike_delta_pct = spike_delta_pct

    def predict(
        self,
        history: Sequence[Tuple[datetime, float]],
        horizon_points: int,
    ) -> ForecastResult:
        if len(history) < 3:
            # Not enough data; fall back to naive projection
            last_price = history[-1][1] if history else 0.0
            predicted = [last_price] * horizon_points
            return ForecastResult(predicted, confidence=0.2, predicted_spike=False, explanation="naive")

        times = np.arange(len(history))
        values = np.array([p for _, p in history], dtype=float)
        slope, intercept = np.polyfit(times, values, 1)

        # Compute fit quality as simple confidence proxy
        fitted = intercept + slope * times
        ss_res = float(np.sum((values - fitted) ** 2))
        ss_tot = float(np.sum((values - np.mean(values)) ** 2)) or 1e-6
        r2 = max(0.0, min(1.0, 1 - ss_res / ss_tot))

        future_indices = np.arange(len(history), len(history) + horizon_points)
        predicted = list(np.round(intercept + slope * future_indices, 2))

        last_price = values[-1]
        spike_level = last_price * (1 + self.spike_delta_pct / 100.0)
        predicted_spike = any(p >= spike_level for p in predicted)

        explanation = (
            f"slope={slope:.4f}, r2={r2:.3f}, last_price={last_price:.2f}, "
            f"spike_level={spike_level:.2f}"
        )
        confidence = round(0.2 + 0.6 * r2, 2)

        return ForecastResult(predicted, confidence=confidence, predicted_spike=predicted_spike, explanation=explanation)


# --- Decision engine -------------------------------------------------------------


class DecisionEngine:
    """Implements the decision tree from the flowchart."""

    def __init__(self, price_threshold: float, low_price_threshold: float, offpeak_hours: List[Tuple[int, int]]):
        self.price_threshold = price_threshold
        self.low_price_threshold = low_price_threshold
        self.offpeak_hours = offpeak_hours

    def is_offpeak(self, ts: datetime) -> bool:
        hour = ts.hour
        for start, end in self.offpeak_hours:
            if start <= hour <= end:
                return True
        return False

    def decide(self, message: PriceMessage, forecast: ForecastResult) -> DecisionResult:
        now = message.timestamp
        current_price = message.price
        predicted_spike = forecast.predicted_spike

        # Decision 1: Critical price now
        if current_price >= self.price_threshold:
            action_type = "CRITICAL_NOW"
            explanation = f"Current price {current_price} >= threshold {self.price_threshold}; reduce usage immediately."
        else:
            # Decision 2: Spike predicted soon
            if predicted_spike:
                action_type = "PREPARE_FOR_SPIKE"
                peak = max(forecast.predicted_values) if forecast.predicted_values else current_price
                explanation = (
                    f"Predicted spike with forecast peak {peak:.2f} (>{current_price}); "
                    "pre-charge or adjust before spike."
                )
            else:
                # Decision 3: Low and off-peak
                if current_price <= self.low_price_threshold and self.is_offpeak(now):
                    action_type = "OPPORTUNITY_CHARGE"
                    explanation = (
                        f"Price {current_price} is low and off-peak hour {now.hour}; charge/pre-heat now."
                    )
                else:
                    action_type = "NORMAL_OPERATION"
                    explanation = "No spike predicted and price not critically high; run baseline schedule."

        return DecisionResult(
            action_type=action_type,
            explanation=explanation,
            current_price=current_price,
            threshold=self.price_threshold,
            predicted_values=forecast.predicted_values,
            predicted_spike=predicted_spike,
            confidence_score=forecast.confidence,
            timestamp=now,
        )


# --- Storage / Publishing --------------------------------------------------------


class StoragePublisher:
    """Stores decision records and publishes downstream."""

    def __init__(self, redis_client: Any = None, producer: Any = None, target_topic: Optional[str] = None):
        self.redis = redis_client
        self.producer = producer
        self.target_topic = target_topic

    def store(self, series_id: str, decision: DecisionResult) -> None:
        if not self.redis:
            return

        key = f"decisions:{series_id}:{decision.timestamp.isoformat()}"
        payload = self._serialize(decision)
        try:
            self.redis.set(key, json.dumps(payload))
        except Exception as exc:
            logger.warning("Failed to store decision", extra={"error": str(exc)})

    def publish(self, decision: DecisionResult) -> None:
        if not self.producer or not self.target_topic:
            return

        payload = self._serialize(decision)
        try:
            self.producer.send(self.target_topic, payload)
        except Exception as exc:
            logger.warning("Failed to publish decision", extra={"error": str(exc)})

    def _serialize(self, decision: DecisionResult) -> Dict[str, Any]:
        return {
            "actionType": decision.action_type,
            "currentPrice": decision.current_price,
            "threshold": decision.threshold,
            "predictedValues": decision.predicted_values,
            "predictedSpike": decision.predicted_spike,
            "confidenceScore": decision.confidence_score,
            "timestamp": decision.timestamp.isoformat(),
            "explanation": decision.explanation,
        }


# --- Main processor --------------------------------------------------------------


class AlgorithmProcessor:
    """Coordinates parsing, enrichment, prediction, decision, and output."""

    def __init__(
        self,
        forecaster: ShortTermForecaster,
        decision_engine: DecisionEngine,
        storage_publisher: StoragePublisher,
        redis_client: Any = None,
        history_lookback: int = 48,
    ) -> None:
        self.forecaster = forecaster
        self.decision_engine = decision_engine
        self.storage_publisher = storage_publisher
        self.redis = redis_client
        self.history_lookback = history_lookback

    def parse_message(self, raw: Dict[str, Any]) -> PriceMessage:
        """Validate and normalize incoming price message."""
        ts_raw = raw.get("DateTime") or raw.get("timestamp") or raw.get("time")
        price_raw = raw.get("Price") or raw.get("price")
        area = raw.get("AREA") or raw.get("area") or "unknown"
        customer = raw.get("CUSTOMER") or raw.get("customer") or "unknown"

        if price_raw is None:
            raise ValueError("Missing price in message")
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid price value: {price_raw}")

        try:
            ts = parser.parse(ts_raw) if ts_raw else datetime.now(timezone.utc)
        except Exception:
            ts = datetime.now(timezone.utc)

        return PriceMessage(timestamp=ts, price=price, area=area, customer=customer, raw=raw)

    def fetch_history(self, series_id: str) -> List[Tuple[datetime, float]]:
        """Fetch recent history from Redis time-series structures."""
        if not self.redis:
            return []

        timeline_key = f"timeline:{series_id}"
        timestamps = self.redis.zrevrange(timeline_key, 0, self.history_lookback - 1)
        history: List[Tuple[datetime, float]] = []
        for ts in reversed(timestamps):
            point_key = f"timeseries:{series_id}:{ts}"
            data = self.redis.get(point_key)
            if not data:
                continue
            try:
                parsed = json.loads(data)
                price = float(parsed.get("value"))
                history.append((parser.parse(parsed["timestamp"]), price))
            except Exception:
                continue

        return history

    def process(self, raw_message: Dict[str, Any]) -> DecisionResult:
        """Process one raw message end-to-end."""
        message = self.parse_message(raw_message)
        series_id = f"{message.area}:{message.customer}"
        history = self.fetch_history(series_id)

        # Include current price in the history for richer context
        history.append((message.timestamp, message.price))

        forecast = self.forecaster.predict(
            history[-self.history_lookback :],
            horizon_points=4,
        )
        decision = self.decision_engine.decide(message, forecast)

        # Persist and publish
        self.storage_publisher.store(series_id, decision)
        self.storage_publisher.publish(decision)

        logger.info(
            "decision_made",
            extra={
                "series_id": series_id,
                "action": decision.action_type,
                "price": decision.current_price,
                "predicted_spike": decision.predicted_spike,
                "confidence": decision.confidence_score,
            },
        )
        return decision
