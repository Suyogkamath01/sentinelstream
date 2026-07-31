from __future__ import annotations

import pytest

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.features.behavioural import calculate_behavioural_features
from sentinelstream.models.decision import RecommendedAction, ThresholdPolicy
from sentinelstream.models.hybrid import HybridFraudEngine, HybridSignals
from sentinelstream.simulation.generator import TransactionGenerator


@pytest.mark.unit
def test_regression_hybrid_threshold_and_reason_codes() -> None:
    engine = HybridFraudEngine(policy=ThresholdPolicy(review_threshold=0.5, block_threshold=0.8))

    low = engine.score(HybridSignals(supervised_probability=0.1))
    high = engine.score(
        HybridSignals(supervised_probability=0.95, anomaly_score=0.9, rule_score=0.9)
    )

    assert low.decision.action is RecommendedAction.ALLOW
    assert high.decision.action is RecommendedAction.BLOCK
    assert high.decision.risk_tier.value == "critical"


@pytest.mark.unit
def test_regression_feature_order_and_event_time_are_stable() -> None:
    event = (
        TransactionGenerator(
            SimulationSettings(customer_count=1, merchant_count=1, random_seed=77, fraud_ratio=0.0)
        )
        .generate(1)[0]
        .event
    )
    first = calculate_behavioural_features(event, [])
    second = calculate_behavioural_features(event, [])

    assert list(first) == list(second)
    assert first["customer_prior_transaction_count"] == 0
    assert event.timestamp.tzinfo is not None


@pytest.mark.unit
def test_regression_schema_rejects_unknown_model_input_fields() -> None:
    payload = (
        TransactionGenerator(SimulationSettings(customer_count=1, merchant_count=1, random_seed=99))
        .generate(1)[0]
        .event
    )
    with pytest.raises(ValueError):
        TransactionEvent.model_validate({**payload.model_dump(), "unexpected": "value"})
