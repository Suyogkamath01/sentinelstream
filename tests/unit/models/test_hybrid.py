from __future__ import annotations

import numpy as np
import pytest

from sentinelstream.models.decision import RecommendedAction, ThresholdPolicy
from sentinelstream.models.hybrid import (
    AblationExperiment,
    HybridFraudEngine,
    HybridSignals,
    HybridWeights,
    ProfileRisk,
    prioritise_alerts,
    validate_calibration,
)


def test_hybrid_engine_fuses_available_sources_and_renormalises_missing_values() -> None:
    engine = HybridFraudEngine(weights=HybridWeights(supervised=0.5, rules=0.5))
    result = engine.score(HybridSignals(supervised_probability=0.8, rule_score=0.2))

    assert result.final_score == pytest.approx(0.5)
    assert result.active_sources == ("supervised", "rules")
    assert result.missing_sources == (
        "anomaly",
        "customer_profile",
        "merchant_profile",
        "device_profile",
    )
    assert result.decision.action is RecommendedAction.REVIEW

    only_model = engine.score(HybridSignals(supervised_probability=0.8))
    assert only_model.final_score == pytest.approx(0.8)


def test_profile_risk_and_alert_priority_are_part_of_the_decision() -> None:
    engine = HybridFraudEngine(
        weights=HybridWeights(
            supervised=0.2,
            anomaly=0.2,
            rules=0.1,
            customer_profile=0.5,
        ),
        policy=ThresholdPolicy(review_threshold=0.5, block_threshold=0.8),
    )
    result = engine.score(
        HybridSignals(
            supervised_probability=0.7,
            anomaly_score=0.7,
            rule_score=0.4,
            customer_profile=ProfileRisk(
                entity_id="customer-001",
                risk_score=0.95,
                confidence=0.9,
                reason_codes=("customer_history_risk",),
            ),
        )
    )

    assert result.is_alert
    assert result.alert_priority > 60
    assert "customer_history_risk" in result.reason_codes


def test_ablation_and_calibration_framework_report_observed_values() -> None:
    engine = HybridFraudEngine()
    signals = HybridSignals(
        supervised_probability=0.8,
        anomaly_score=0.7,
        rule_score=0.6,
        customer_profile=ProfileRisk("customer-001", 0.5),
        merchant_profile=ProfileRisk("merchant-001", 0.4),
        device_profile=ProfileRisk("device-001", 0.3),
    )
    ablations = AblationExperiment(engine).run(signals)
    assert len(ablations) == 6
    assert any(result.score_delta != 0.0 for result in ablations)

    calibration = validate_calibration(
        np.array([0, 0, 1, 1]),
        np.array([0.05, 0.05, 0.95, 0.95]),
    )
    assert calibration.metrics["sample_count"] == 4
    assert calibration.passed


def test_alert_prioritisation_is_deterministic() -> None:
    engine = HybridFraudEngine()
    results = [
        engine.score(HybridSignals(supervised_probability=0.9)),
        engine.score(HybridSignals(supervised_probability=0.6)),
    ]
    ordered = prioritise_alerts(results)
    assert ordered[0].final_score >= ordered[1].final_score
