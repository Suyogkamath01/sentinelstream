"""Checkpointed per-customer event-time feature state for Spark streaming."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame

from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.features.behavioural import calculate_behavioural_features
from sentinelstream.spark.schemas import FEATURE_STATE_SCHEMA, feature_schema

FEATURE_COLUMNS = (
    "event_hour",
    "event_day_of_week",
    "is_weekend",
    "is_night",
    "amount_log1p",
    "is_refund",
    "is_cash_withdrawal",
    "is_card_present",
    "customer_prior_transaction_count",
    "customer_prior_amount_sum",
    "customer_prior_amount_mean",
    "customer_prior_amount_max",
    "customer_prior_amount_std",
    "customer_seconds_since_previous",
    "merchant_novelty",
    "device_novelty",
    "country_novelty",
    "merchant_category_novelty",
    "amount_z_score",
    "failed_transaction_rate_prior",
    "decline_rate_prior",
    "card_present_ratio_prior",
    "customer_ewm_amount",
    "recent_transaction_count_10m",
    "recent_amount_sum_10m",
    "recent_amount_mean_10m",
    "recent_amount_max_10m",
    "recent_amount_std_10m",
    "recent_transaction_count_1h",
    "recent_amount_sum_1h",
    "recent_amount_mean_1h",
    "recent_amount_max_1h",
    "recent_amount_std_1h",
    "recent_transaction_count_24h",
    "recent_amount_sum_24h",
    "recent_amount_mean_24h",
    "recent_amount_max_24h",
    "recent_amount_std_24h",
    "geographic_distance_km",
    "geographic_velocity_kmh",
    "impossible_travel",
    "customer_transaction_velocity_10m",
    "customer_transaction_velocity_1h",
    "merchant_transaction_velocity_1h",
    "unique_merchants_24h",
    "unique_devices_24h",
    "unique_countries_24h",
    "time_since_previous_transaction",
    "customer_spending_deviation",
    "merchant_popularity",
    "rolling_transaction_count_1h",
    "rolling_transaction_amount_1h",
    "rolling_transaction_mean_1h",
    "rolling_transaction_stddev_1h",
    "rolling_transaction_max_1h",
    "rolling_transaction_min_1h",
)


@dataclass(frozen=True, slots=True)
class StatefulFeatureConfig:
    """Event-time and state-retention policy for customer feature state."""

    watermark_duration: str = "15 minutes"
    allowed_lateness: timedelta = timedelta(minutes=15)
    history_retention: timedelta = timedelta(hours=24)
    state_timeout: timedelta = timedelta(hours=24)


def duration_to_timedelta(value: str) -> timedelta:
    """Parse the simple duration syntax shared by Spark and application config."""

    match = re.fullmatch(
        r"\s*(?P<amount>\d+(?:\.\d+)?)\s*(?P<unit>seconds?|minutes?|hours?|days?)\s*",
        value,
        flags=re.IGNORECASE,
    )
    if match is None:
        raise ValueError(f"unsupported Spark duration: {value}")
    amount = float(match.group("amount"))
    unit = match.group("unit").lower()
    seconds_per_unit = {
        "second": 1.0,
        "seconds": 1.0,
        "minute": 60.0,
        "minutes": 60.0,
        "hour": 3_600.0,
        "hours": 3_600.0,
        "day": 86_400.0,
        "days": 86_400.0,
    }
    return timedelta(seconds=amount * seconds_per_unit[unit])


def _empty_features() -> dict[str, float]:
    return {name: 0.0 for name in FEATURE_COLUMNS}


def _feature_values(
    event: TransactionEvent,
    history: Sequence[TransactionEvent],
) -> dict[str, float]:
    base = calculate_behavioural_features(event, history)
    prior_24h = [
        candidate
        for candidate in history
        if event.timestamp - timedelta(hours=24) <= candidate.timestamp < event.timestamp
    ]
    prior_1h = [
        candidate
        for candidate in history
        if event.timestamp - timedelta(hours=1) <= candidate.timestamp < event.timestamp
    ]
    merchant_history = [
        candidate for candidate in prior_1h if candidate.merchant_id == event.merchant_id
    ]
    merchant_24h = [
        candidate for candidate in prior_24h if candidate.merchant_id == event.merchant_id
    ]
    seconds_since_previous = float(base["customer_seconds_since_previous"])
    elapsed_hours = max(seconds_since_previous / 3_600, 1 / 60)
    elapsed_merchant_hours = elapsed_hours
    if merchant_history:
        elapsed_merchant_hours = max(
            (event.timestamp - merchant_history[-1].timestamp).total_seconds() / 3_600,
            1 / 60,
        )
    amounts = [candidate.transaction_amount for candidate in prior_1h]
    values: dict[str, float] = {
        name: float(base.get(name, 0.0)) for name in FEATURE_COLUMNS if name in base
    }
    values.update(
        {
            "customer_transaction_velocity_10m": float(base["recent_transaction_count_10m"])
            / (10 / 60),
            "customer_transaction_velocity_1h": float(base["recent_transaction_count_1h"]) / 1.0,
            "merchant_transaction_velocity_1h": len(merchant_history) / elapsed_merchant_hours,
            "unique_merchants_24h": float(len({candidate.merchant_id for candidate in prior_24h})),
            "unique_devices_24h": float(len({candidate.device_id for candidate in prior_24h})),
            "unique_countries_24h": float(len({candidate.country for candidate in prior_24h})),
            "time_since_previous_transaction": seconds_since_previous,
            "customer_spending_deviation": float(base["amount_z_score"]),
            "merchant_popularity": float(len(merchant_24h)),
            "rolling_transaction_count_1h": float(len(prior_1h)),
            "rolling_transaction_amount_1h": float(sum(amounts)),
            "rolling_transaction_mean_1h": (float(sum(amounts) / len(amounts)) if amounts else 0.0),
            "rolling_transaction_stddev_1h": float(base["recent_amount_std_1h"]),
            "rolling_transaction_max_1h": float(max(amounts, default=0.0)),
            "rolling_transaction_min_1h": float(min(amounts, default=0.0)),
        }
    )
    return {name: values.get(name, 0.0) for name in FEATURE_COLUMNS}


def _result_row(
    event: TransactionEvent,
    features: dict[str, float],
    *,
    accepted: bool,
    reason: str | None,
) -> dict[str, Any]:
    return {
        "event_json": json.dumps(event.model_dump(mode="json"), sort_keys=True),
        "event_id": str(event.event_id),
        "transaction_id": str(event.transaction_id),
        "customer_id": event.customer_id,
        "merchant_id": event.merchant_id,
        "device_id": event.device_id,
        "country": event.country,
        "event_time": event.timestamp,
        "transaction_amount": float(event.transaction_amount),
        "accepted": accepted,
        "reason": reason,
        **features,
    }


def process_event_sequence(
    events: Sequence[TransactionEvent],
    config: StatefulFeatureConfig | None = None,
) -> list[dict[str, Any]]:
    """Process an arrival-ordered sequence using the same policy as Spark state."""

    config = config or StatefulFeatureConfig()
    history: list[TransactionEvent] = []
    seen_event_ids: set[str] = set()
    seen_transaction_ids: set[str] = set()
    max_event_time = None
    output: list[dict[str, Any]] = []
    for event in events:
        if max_event_time is None or event.timestamp > max_event_time:
            max_event_time = event.timestamp
        watermark = max_event_time - config.allowed_lateness
        event_id = str(event.event_id)
        transaction_id = str(event.transaction_id)
        if event_id in seen_event_ids or transaction_id in seen_transaction_ids:
            output.append(
                _result_row(event, _empty_features(), accepted=False, reason="duplicate_event")
            )
            continue
        if event.timestamp < watermark:
            output.append(
                _result_row(
                    event, _empty_features(), accepted=False, reason="event_beyond_watermark"
                )
            )
            continue
        features = _feature_values(event, history)
        output.append(_result_row(event, features, accepted=True, reason=None))
        history.append(event)
        seen_event_ids.add(event_id)
        seen_transaction_ids.add(transaction_id)
        cutoff = event.timestamp - config.history_retention
        retained = [candidate for candidate in history if candidate.timestamp >= cutoff]
        removed_event_ids = {str(candidate.event_id) for candidate in history}
        removed_event_ids.difference_update(str(candidate.event_id) for candidate in retained)
        removed_transaction_ids = {str(candidate.transaction_id) for candidate in history}
        removed_transaction_ids.difference_update(
            str(candidate.transaction_id) for candidate in retained
        )
        history = retained
        seen_event_ids.difference_update(removed_event_ids)
        seen_transaction_ids.difference_update(removed_transaction_ids)
    return output


def _state_history(state: Any) -> list[TransactionEvent]:
    if not state.exists:
        return []
    stored = state.get
    if callable(stored):
        stored = stored()
    events_json = stored[0] if stored else "[]"
    return [TransactionEvent.model_validate(item) for item in json.loads(events_json)]


def _stateful_customer_function(
    key: tuple[Any, ...],
    batches: Iterator[pd.DataFrame],
    state: Any,
    history_retention: timedelta = timedelta(hours=24),
    state_timeout: timedelta = timedelta(hours=24),
) -> Iterator[pd.DataFrame]:
    if state.hasTimedOut:
        state.remove()
        return iter(())
    history = _state_history(state)
    seen_event_ids = {str(event.event_id) for event in history}
    seen_transaction_ids = {str(event.transaction_id) for event in history}
    output: list[dict[str, Any]] = []
    watermark_ms = state.getCurrentWatermarkMs()
    latest_timestamp = max((event.timestamp for event in history), default=None)
    for batch in batches:
        if batch.empty:
            continue
        ordered = batch.sort_values(["event_time", "event_id"], kind="stable")
        for row in ordered.itertuples(index=False):
            event = TransactionEvent.model_validate_json(row.event_json)
            event_id = str(event.event_id)
            transaction_id = str(event.transaction_id)
            if event_id in seen_event_ids or transaction_id in seen_transaction_ids:
                output.append(
                    _result_row(event, _empty_features(), accepted=False, reason="duplicate_event")
                )
                continue
            event_ms = int(event.timestamp.timestamp() * 1_000)
            if watermark_ms >= 0 and event_ms < watermark_ms:
                output.append(
                    _result_row(
                        event,
                        _empty_features(),
                        accepted=False,
                        reason="event_beyond_watermark",
                    )
                )
                continue
            features = _feature_values(event, history)
            output.append(_result_row(event, features, accepted=True, reason=None))
            history.append(event)
            seen_event_ids.add(event_id)
            seen_transaction_ids.add(transaction_id)
            latest_timestamp = (
                max(latest_timestamp, event.timestamp) if latest_timestamp else event.timestamp
            )
            cutoff = event.timestamp - history_retention
            history = [candidate for candidate in history if candidate.timestamp >= cutoff]
            seen_event_ids = {str(candidate.event_id) for candidate in history}
            seen_transaction_ids = {str(candidate.transaction_id) for candidate in history}
    if history:
        state.update((json.dumps([event.model_dump(mode="json") for event in history]),))
        if latest_timestamp is None:
            raise RuntimeError("state history cannot be updated without a latest timestamp")
        state.setTimeoutTimestamp(int((latest_timestamp + state_timeout).timestamp() * 1_000))
    if output:
        return iter([pd.DataFrame(output)])
    return iter(())


def _batch_group(batch: pd.DataFrame) -> pd.DataFrame:
    events = [
        TransactionEvent.model_validate_json(value)
        for value in batch.sort_values(["event_time", "event_id"], kind="stable").event_json
    ]
    return pd.DataFrame(process_event_sequence(events))


def build_feature_batch(events: DataFrame) -> DataFrame:
    """Build deterministic features for a bounded Spark dataframe."""

    schema = feature_schema(FEATURE_COLUMNS)
    return events.groupBy("customer_id").applyInPandas(_batch_group, schema=schema)


def build_stateful_feature_stream(
    events: DataFrame,
    config: StatefulFeatureConfig | None = None,
) -> DataFrame:
    """Apply event-time watermarks and checkpoint-backed customer state."""

    config = config or StatefulFeatureConfig()
    schema = feature_schema(FEATURE_COLUMNS)
    # Keep duplicate rows in the grouped stream so they can be classified and
    # counted by the state function instead of being silently discarded by a
    # separate Spark deduplication operator.
    deduplicated = events.withWatermark("event_time", config.watermark_duration)

    def stateful_function(
        key: tuple[Any, ...],
        batches: Iterator[pd.DataFrame],
        state: Any,
    ) -> Iterator[pd.DataFrame]:
        return _stateful_customer_function(
            key,
            batches,
            state,
            history_retention=config.history_retention,
            state_timeout=config.state_timeout,
        )

    return deduplicated.groupBy("customer_id").applyInPandasWithState(
        stateful_function,
        outputStructType=schema,
        stateStructType=FEATURE_STATE_SCHEMA,
        outputMode="Append",
        timeoutConf="EventTimeTimeout",
    )
