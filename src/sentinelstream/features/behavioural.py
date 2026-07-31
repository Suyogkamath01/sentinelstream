"""Point-in-time behavioural feature definitions."""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.features.temporal import temporal_features

WINDOWS = {
    "10m": timedelta(minutes=10),
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
}
EARTH_RADIUS_KM = 6_371.0


def _distance_km(first: TransactionEvent, second: TransactionEvent) -> float:
    latitude_one = math.radians(first.latitude)
    latitude_two = math.radians(second.latitude)
    delta_latitude = math.radians(second.latitude - first.latitude)
    delta_longitude = math.radians(second.longitude - first.longitude)
    haversine = (
        math.sin(delta_latitude / 2) ** 2
        + math.cos(latitude_one) * math.cos(latitude_two) * math.sin(delta_longitude / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(haversine))


def _prior_events(
    event: TransactionEvent,
    history: Sequence[TransactionEvent],
) -> list[TransactionEvent]:
    return sorted(
        (candidate for candidate in history if candidate.timestamp < event.timestamp),
        key=lambda candidate: (candidate.timestamp, str(candidate.event_id)),
    )


def _window_events(
    event: TransactionEvent,
    history: Sequence[TransactionEvent],
    window: timedelta,
) -> list[TransactionEvent]:
    lower_bound = event.timestamp - window
    return [
        candidate for candidate in history if lower_bound <= candidate.timestamp < event.timestamp
    ]


def _ewm_amount(amounts: list[float], alpha: float = 0.2) -> float:
    if not amounts:
        return 0.0
    estimate = amounts[0]
    for amount in amounts[1:]:
        estimate = alpha * amount + (1 - alpha) * estimate
    return estimate


def calculate_behavioural_features(
    event: TransactionEvent,
    history: Sequence[TransactionEvent],
) -> dict[str, Any]:
    """Calculate features from the event and strictly earlier customer events."""

    prior = _prior_events(event, history)
    amounts = [candidate.transaction_amount for candidate in prior]
    prior_count = len(prior)
    prior_sum = sum(amounts)
    prior_mean = prior_sum / prior_count if prior_count else 0.0
    prior_max = max(amounts, default=0.0)
    prior_std = (
        math.sqrt(sum((amount - prior_mean) ** 2 for amount in amounts) / prior_count)
        if prior_count
        else 0.0
    )
    previous = prior[-1] if prior else None
    seconds_since_previous = (
        (event.timestamp - previous.timestamp).total_seconds() if previous else -1.0
    )
    denominator = prior_std if prior_std > 0 else None
    amount_z_score = (event.transaction_amount - prior_mean) / denominator if denominator else 0.0
    feature_values: dict[str, Any] = {
        **temporal_features(event.timestamp),
        "amount_log1p": math.log1p(event.transaction_amount),
        "is_refund": int(event.transaction_type.value == "refund"),
        "is_cash_withdrawal": int(event.transaction_type.value == "cash_withdrawal"),
        "is_card_present": int(event.card_present),
        "customer_prior_transaction_count": prior_count,
        "customer_prior_amount_sum": prior_sum,
        "customer_prior_amount_mean": prior_mean,
        "customer_prior_amount_max": prior_max,
        "customer_prior_amount_std": prior_std,
        "customer_seconds_since_previous": seconds_since_previous,
        "merchant_novelty": int(
            not any(candidate.merchant_id == event.merchant_id for candidate in prior)
        ),
        "device_novelty": int(
            not any(candidate.device_id == event.device_id for candidate in prior)
        ),
        "country_novelty": int(not any(candidate.country == event.country for candidate in prior)),
        "merchant_category_novelty": int(
            not any(candidate.merchant_category == event.merchant_category for candidate in prior)
        ),
        "amount_z_score": amount_z_score,
        "failed_transaction_rate_prior": (
            sum(candidate.transaction_status.value != "approved" for candidate in prior)
            / prior_count
            if prior_count
            else 0.0
        ),
        "decline_rate_prior": (
            sum(candidate.transaction_status.value == "declined" for candidate in prior)
            / prior_count
            if prior_count
            else 0.0
        ),
        "card_present_ratio_prior": (
            sum(candidate.card_present for candidate in prior) / prior_count if prior_count else 0.0
        ),
        "customer_ewm_amount": _ewm_amount(amounts),
    }
    for name, window in WINDOWS.items():
        recent = _window_events(event, prior, window)
        recent_amounts = [candidate.transaction_amount for candidate in recent]
        feature_values[f"recent_transaction_count_{name}"] = len(recent)
        feature_values[f"recent_amount_sum_{name}"] = sum(recent_amounts)
        feature_values[f"recent_amount_mean_{name}"] = (
            sum(recent_amounts) / len(recent_amounts) if recent_amounts else 0.0
        )
        feature_values[f"recent_amount_max_{name}"] = max(recent_amounts, default=0.0)
        feature_values[f"recent_amount_std_{name}"] = (
            math.sqrt(
                sum(
                    (amount - feature_values[f"recent_amount_mean_{name}"]) ** 2
                    for amount in recent_amounts
                )
                / len(recent_amounts)
            )
            if recent_amounts
            else 0.0
        )
    distance = _distance_km(previous, event) if previous else 0.0
    elapsed_hours = seconds_since_previous / 3_600 if seconds_since_previous > 0 else 0.0
    velocity = distance / elapsed_hours if elapsed_hours > 0 else 0.0
    feature_values["geographic_distance_km"] = distance
    feature_values["geographic_velocity_kmh"] = velocity
    feature_values["impossible_travel"] = int(velocity > 900.0)
    return feature_values
