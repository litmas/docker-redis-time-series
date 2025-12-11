import logging
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

import numpy as np
from dateutil import parser


logger = logging.getLogger(__name__)


@dataclass
class PredictionResult:
    predicted_price: float
    confidence: float
    trend: str
    change_pct: float
    volatility: float
    horizon_minutes: int
    lookback_used: int
    explanation: str
    recommendation: Dict[str, Any]
    supporting_points: int
    interval_minutes: Optional[float]


class SpotPricePredictor:
    """
    Lightweight forecasting helper for electricity spot prices.
    Uses a small linear regression over the last N prices and applies
    simple stability heuristics to keep predictions fast for HPA scaling.
    """

    def __init__(self, lookback: int = 24, min_points: int = 6):
        self.lookback = lookback
        self.min_points = min_points

    def _parse_timestamp(self, raw_ts: Any) -> Optional[datetime]:
        """Parse timestamps coming from mixed sources."""
        if raw_ts is None:
            return None

        if isinstance(raw_ts, (int, float)):
            # Treat numeric as epoch seconds
            return datetime.utcfromtimestamp(float(raw_ts))

        if isinstance(raw_ts, datetime):
            return raw_ts

        try:
            return parser.parse(str(raw_ts))
        except (ValueError, TypeError, OverflowError):
            return None

    def _normalize_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize incoming records into a consistent structure."""
        normalized: List[Dict[str, Any]] = []

        for record in records:
            price_raw = record.get("Price") or record.get("price") or record.get("spot_price")
            ts_raw = (
                record.get("DateTime")
                or record.get("timestamp")
                or record.get("time")
                or record.get("date")
            )

            ts = self._parse_timestamp(ts_raw)
            if ts is None or price_raw is None:
                continue

            try:
                price = float(price_raw)
            except (TypeError, ValueError):
                continue

            normalized.append(
                {
                    "timestamp": ts,
                    "price": price,
                    "raw": record,
                }
            )

        normalized.sort(key=lambda x: x["timestamp"])
        return normalized

    def _median_interval_minutes(self, timestamps: List[datetime]) -> Optional[float]:
        """Calculate median interval in minutes between sorted timestamps."""
        if len(timestamps) < 2:
            return None

        deltas = []
        for earlier, later in zip(timestamps, timestamps[1:]):
            delta = (later - earlier).total_seconds() / 60.0
            if delta > 0:
                deltas.append(delta)

        if not deltas:
            return None

        deltas.sort()
        mid = len(deltas) // 2
        if len(deltas) % 2 == 0:
            return (deltas[mid - 1] + deltas[mid]) / 2
        return deltas[mid]

    def _derive_recommendation(self, change_pct: float, horizon_minutes: int) -> Dict[str, Any]:
        """Translate predicted change into DSM-friendly guidance."""
        if change_pct >= 7:
            action = "reduce_now"
            note = "Prices expected to spike; shift discretionary loads."
        elif change_pct >= 3:
            action = "preemptive_reduce"
            note = "Upward trend detected; ramp down flexible usage soon."
        elif change_pct <= -7:
            action = "increase_now"
            note = "Prices expected to drop; charge or pre-heat while cheap."
        elif change_pct <= -3:
            action = "opportunistic_use"
            note = "Mild decrease expected; consider advancing demand."
        else:
            action = "hold"
            note = "Flat outlook; run baseline schedule."

        return {
            "action": action,
            "window_minutes": horizon_minutes,
            "note": note,
            "thresholds": {
                "increase_pct": -3,
                "reduce_pct": 3,
            },
        }

    def predict(
        self, records: List[Dict[str, Any]], horizon_minutes: int = 60
    ) -> PredictionResult:
        cleaned = self._normalize_records(records)
        if len(cleaned) < self.min_points:
            raise ValueError(
                f"Not enough data points for prediction. Got {len(cleaned)}, need at least {self.min_points}."
            )

        window = cleaned[-self.lookback :]
        prices = np.array([item["price"] for item in window], dtype=float)
        timestamps = [item["timestamp"] for item in window]
        interval_minutes = self._median_interval_minutes(timestamps) or 60.0

        # Build simple linear regression with minimal overhead
        idx = np.arange(len(prices))
        slope, intercept = np.polyfit(idx, prices, 1)

        steps_ahead = max(1, math.ceil(horizon_minutes / interval_minutes))
        predicted = float(intercept + slope * (len(prices) - 1 + steps_ahead))

        fitted = intercept + slope * idx
        residuals = prices - fitted

        ss_res = float(np.sum(residuals ** 2))
        ss_tot = float(np.sum((prices - np.mean(prices)) ** 2)) or 1e-6
        r_squared = max(0.0, min(0.99, 1.0 - ss_res / ss_tot))

        volatility = float(np.std(residuals))
        stability = 1.0 / (1.0 + volatility)
        horizon_penalty = 1.0 / (1.0 + (steps_ahead - 1) * 0.4)
        confidence = max(0.05, min(0.98, r_squared * 0.6 + stability * 0.3 + horizon_penalty * 0.1))

        last_price = float(prices[-1])
        change_pct = ((predicted - last_price) / last_price) * 100 if last_price else 0.0
        trend = "up" if slope > 0.05 else "down" if slope < -0.05 else "flat"

        recommendation = self._derive_recommendation(change_pct, horizon_minutes)
        explanation = (
            f"Trend={trend}, slope={slope:.4f}, R2={r_squared:.3f}, "
            f"interval={interval_minutes:.1f}m, horizon_steps={steps_ahead}"
        )

        return PredictionResult(
            predicted_price=round(predicted, 2),
            confidence=round(confidence, 2),
            trend=trend,
            change_pct=round(change_pct, 2),
            volatility=round(volatility, 3),
            horizon_minutes=horizon_minutes,
            lookback_used=len(window),
            explanation=explanation,
            recommendation=recommendation,
            supporting_points=len(window),
            interval_minutes=interval_minutes,
        )
