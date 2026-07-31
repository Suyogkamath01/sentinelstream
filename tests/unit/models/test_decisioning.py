import numpy as np
import pytest

from sentinelstream.models.calibration import (
    ProbabilityCalibrator,
    calibration_report,
)
from sentinelstream.models.decision import (
    PredictionSignals,
    RecommendedAction,
    RiskTier,
    ThresholdPolicy,
    make_decision,
)
from sentinelstream.models.thresholding import (
    CostModel,
    expected_financial_cost,
    optimize_threshold,
)


@pytest.mark.parametrize("method", ["platt", "isotonic"])
def test_calibrator_outputs_probabilities_and_report(method: str) -> None:
    scores = np.array([0.05, 0.15, 0.25, 0.65, 0.75, 0.95])
    labels = np.array([0, 0, 0, 1, 1, 1])

    calibrator = ProbabilityCalibrator(method=method).fit(scores, labels)
    calibrated = calibrator.transform(scores)

    assert calibrator.version == f"phase4-{method}-v1"
    assert np.all((calibrated >= 0.0) & (calibrated <= 1.0))
    assert np.all((calibrator.confidence(scores) >= 0.0) & (calibrator.confidence(scores) <= 1.0))
    report = calibration_report(labels, calibrated)
    assert report["sample_count"] == len(scores)
    assert 0.0 <= report["expected_calibration_error"] <= 1.0


def test_calibration_requires_both_classes() -> None:
    with pytest.raises(ValueError, match="both fraud"):
        ProbabilityCalibrator().fit([0.1, 0.2], [0, 0])


def test_threshold_optimizer_respects_cost_and_capacity() -> None:
    labels = np.array([1, 0, 1, 0, 1, 0])
    scores = np.array([0.95, 0.90, 0.80, 0.30, 0.20, 0.10])
    amounts = np.array([100.0, 5.0, 80.0, 10.0, 120.0, 10.0])
    costs = CostModel(
        false_positive_review_cost=20.0,
        customer_friction_cost=0.0,
        transaction_blocking_cost=0.0,
    )

    unconstrained = optimize_threshold(labels, scores, amounts, cost_model=costs)
    capacity_limited = optimize_threshold(
        labels,
        scores,
        amounts,
        cost_model=costs,
        max_alerts=2,
    )

    baseline_cost = expected_financial_cost(
        labels, scores >= 0.5, amounts, cost_model=costs
    )
    assert unconstrained.expected_cost <= baseline_cost
    assert capacity_limited.alert_count <= 2
    assert capacity_limited.capacity_limited


def test_expected_financial_cost_accounts_for_recovered_funds() -> None:
    labels = np.array([1, 0])
    amounts = np.array([100.0, 5.0])
    costs = CostModel(
        customer_friction_cost=0.0,
        transaction_blocking_cost=0.0,
        recovered_funds_rate=0.5,
    )

    assert expected_financial_cost(labels, [False, False], amounts, cost_model=costs) == 50.0


def test_decision_policy_returns_allow_review_and_block() -> None:
    policy = ThresholdPolicy(min_confidence=0.1)

    low = make_decision(PredictionSignals(0.1), policy)
    medium = make_decision(PredictionSignals(0.6), policy)
    critical = make_decision(PredictionSignals(0.9), policy)

    assert (low.action, low.risk_tier) == (RecommendedAction.ALLOW, RiskTier.LOW)
    assert (medium.action, medium.risk_tier) == (RecommendedAction.REVIEW, RiskTier.HIGH)
    assert (critical.action, critical.risk_tier) == (
        RecommendedAction.BLOCK,
        RiskTier.CRITICAL,
    )


def test_decision_abstains_on_low_confidence_and_disagreement() -> None:
    policy = ThresholdPolicy(min_confidence=0.2)

    uncertain = make_decision(PredictionSignals(0.5), policy)
    disagreement = make_decision(
        PredictionSignals(0.2, rule_score=0.95, confidence=0.9),
        policy,
    )

    assert uncertain.action == RecommendedAction.REVIEW
    assert uncertain.abstained
    assert "low_confidence" in uncertain.reason_codes
    assert disagreement.action == RecommendedAction.REVIEW
    assert disagreement.abstained
    assert "rule_model_disagreement" in disagreement.reason_codes


def test_segment_thresholds_use_global_fallback_until_mature() -> None:
    policy = ThresholdPolicy(
        review_threshold=0.5,
        segment_review_thresholds={"mobile": 0.7},
        min_segment_observations=50,
    )

    assert policy.threshold_for("mobile", 49) == 0.5
    assert policy.threshold_for("mobile", 50) == 0.7
    assert policy.threshold_for("unknown", 100) == 0.5
