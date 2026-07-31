from datetime import timedelta
from uuid import uuid4

import numpy as np
import pandas as pd

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.schemas import SimulatedTransaction
from sentinelstream.features.batch import build_batch_features
from sentinelstream.features.registry import FEATURE_DEFINITIONS, FEATURE_VERSION
from sentinelstream.features.streaming import StreamingFeatureCalculator, StreamingFeatureConfig
from sentinelstream.simulation.generator import TransactionGenerator


def events(count: int = 50) -> list[SimulatedTransaction]:
    records = TransactionGenerator(
        SimulationSettings(customer_count=6, merchant_count=5, random_seed=27, fraud_ratio=0.0)
    ).generate(count)
    return [record for record in records if isinstance(record, SimulatedTransaction)]


def test_batch_and_streaming_features_have_parity() -> None:
    source = events()
    frame = pd.DataFrame([record.event.model_dump(mode="json") for record in source])
    batch = build_batch_features(frame)
    calculator = StreamingFeatureCalculator()
    streaming = {
        str(record.event.event_id): calculator.process(record.event).features
        for record in sorted(
            source,
            key=lambda record: (record.event.timestamp, str(record.event.event_id)),
        )
    }
    feature_names = [
        "customer_prior_transaction_count",
        "customer_prior_amount_mean",
        "recent_transaction_count_10m",
        "recent_amount_sum_1h",
        "merchant_novelty",
        "device_novelty",
        "country_novelty",
        "geographic_velocity_kmh",
        "impossible_travel",
    ]
    for _, row in batch.iterrows():
        values = streaming[str(row["event_id"])]
        np.testing.assert_allclose(
            [float(row[name]) for name in feature_names],
            [float(values[name]) for name in feature_names],
        )


def test_streaming_state_deduplicates_and_handles_watermarks() -> None:
    source = events(3)
    latest = max(source, key=lambda record: record.event.timestamp).event
    calculator = StreamingFeatureCalculator(
        StreamingFeatureConfig(allowed_lateness=timedelta(minutes=5))
    )
    accepted = calculator.process(latest)
    duplicate = calculator.process(latest)
    within_lateness = latest.model_copy(
        update={
            "event_id": uuid4(),
            "transaction_id": uuid4(),
            "timestamp": latest.timestamp - timedelta(minutes=2),
        }
    )
    late = latest.model_copy(
        update={
            "event_id": uuid4(),
            "transaction_id": uuid4(),
            "timestamp": latest.timestamp - timedelta(minutes=10),
        }
    )

    assert accepted.accepted
    assert duplicate.reason == "duplicate_event"
    assert calculator.process(within_lateness).accepted
    assert calculator.process(late).reason == "event_beyond_watermark"
    assert len(calculator.late_events) == 1


def test_streaming_features_use_state_before_current_event() -> None:
    first = events(1)[0].event
    second = first.model_copy(
        update={
            "event_id": uuid4(),
            "transaction_id": uuid4(),
            "timestamp": first.timestamp + timedelta(minutes=3),
            "ingestion_timestamp": first.ingestion_timestamp + timedelta(minutes=3),
            "transaction_amount": first.transaction_amount * 2,
        }
    )
    calculator = StreamingFeatureCalculator()

    first_result = calculator.process(first)
    second_result = calculator.process(second)

    assert first_result.features["customer_prior_transaction_count"] == 0
    assert second_result.features["customer_prior_transaction_count"] == 1
    assert second_result.features["recent_transaction_count_10m"] == 1


def test_feature_registry_marks_phase3_contracts_safe_and_online() -> None:
    assert FEATURE_VERSION == "phase3-v1"
    assert FEATURE_DEFINITIONS
    assert all(definition.point_in_time_safe for definition in FEATURE_DEFINITIONS)
    assert all(definition.online_supported for definition in FEATURE_DEFINITIONS)
