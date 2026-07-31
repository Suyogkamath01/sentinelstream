"""Versioned risk-policy decisions for fraud-serving consumers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from math import isfinite


class RiskTier(StrEnum):
    """Human-readable risk bands used by alerting and dashboards."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RecommendedAction(StrEnum):
    """Action suggested to the transaction workflow."""

    ALLOW = "allow"
    REVIEW = "review"
    BLOCK = "block"


@dataclass(frozen=True, slots=True)
class ThresholdPolicy:
    """Review and block thresholds with conservative segment fallback rules."""

    version: str = "phase4-policy-v1"
    review_threshold: float = 0.50
    block_threshold: float = 0.85
    min_confidence: float = 0.20
    segment_review_thresholds: Mapping[str, float] = field(default_factory=dict)
    min_segment_observations: int = 100

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("policy version must not be empty")
        if not 0.0 <= self.review_threshold < self.block_threshold <= 1.0:
            raise ValueError("review_threshold must be below block_threshold in [0, 1]")
        if not 0.0 <= self.min_confidence <= 1.0:
            raise ValueError("min_confidence must be between 0 and 1")
        if self.min_segment_observations < 0:
            raise ValueError("min_segment_observations must be non-negative")
        if any(
            not 0.0 <= threshold < self.block_threshold
            for threshold in self.segment_review_thresholds.values()
        ):
            raise ValueError("segment thresholds must be below block_threshold")

    def threshold_for(self, segment_key: str, segment_observations: int) -> float:
        if segment_observations < self.min_segment_observations:
            return self.review_threshold
        return self.segment_review_thresholds.get(segment_key, self.review_threshold)


@dataclass(frozen=True, slots=True)
class PredictionSignals:
    """Model outputs consumed by the deterministic decision policy."""

    calibrated_probability: float
    anomaly_score: float | None = None
    rule_score: float | None = None
    confidence: float | None = None
    segment_key: str = "global"
    segment_observations: int = 0
    model_version: str = "unknown"
    calibration_version: str = "unknown"

    def __post_init__(self) -> None:
        values = (
            self.calibrated_probability,
            self.anomaly_score,
            self.rule_score,
            self.confidence,
        )
        for value in values:
            if value is not None and (not isfinite(value) or not 0.0 <= value <= 1.0):
                raise ValueError("prediction signals must be finite values between 0 and 1")
        if self.segment_observations < 0:
            raise ValueError("segment_observations must be non-negative")
        if not self.segment_key.strip():
            raise ValueError("segment_key must not be empty")


@dataclass(frozen=True, slots=True)
class RiskDecision:
    """Auditable output of one policy evaluation."""

    calibrated_probability: float
    confidence: float
    abstained: bool
    risk_tier: RiskTier
    action: RecommendedAction
    review_threshold: float
    block_threshold: float
    policy_version: str
    model_version: str
    calibration_version: str
    reason_codes: tuple[str, ...]


def _disagreement_reasons(signals: PredictionSignals, review_threshold: float) -> list[str]:
    reasons: list[str] = []
    if signals.rule_score is not None:
        if signals.rule_score >= 0.80 and signals.calibrated_probability < review_threshold:
            reasons.append("rule_model_disagreement")
        if signals.rule_score <= 0.20 and signals.calibrated_probability >= review_threshold:
            reasons.append("rule_model_disagreement")
    if (
        signals.anomaly_score is not None
        and signals.anomaly_score >= 0.80
        and signals.calibrated_probability < review_threshold
    ):
        reasons.append("anomaly_model_disagreement")
    return reasons


def make_decision(signals: PredictionSignals, policy: ThresholdPolicy) -> RiskDecision:
    """Map calibrated risk signals to an allow, review, or block action."""

    probability = signals.calibrated_probability
    review_threshold = policy.threshold_for(
        signals.segment_key, signals.segment_observations
    )
    confidence = (
        signals.confidence
        if signals.confidence is not None
        else 2.0 * abs(probability - 0.5)
    )
    reasons = _disagreement_reasons(signals, review_threshold)
    if confidence < policy.min_confidence:
        reasons.append("low_confidence")
    abstained = bool(reasons)

    if abstained:
        action = RecommendedAction.REVIEW
        tier = RiskTier.HIGH if probability >= review_threshold else RiskTier.MEDIUM
    elif probability >= policy.block_threshold:
        action = RecommendedAction.BLOCK
        tier = RiskTier.CRITICAL
    elif probability >= review_threshold:
        action = RecommendedAction.REVIEW
        tier = RiskTier.HIGH
    elif probability >= 0.25:
        action = RecommendedAction.ALLOW
        tier = RiskTier.MEDIUM
    else:
        action = RecommendedAction.ALLOW
        tier = RiskTier.LOW

    return RiskDecision(
        calibrated_probability=probability,
        confidence=confidence,
        abstained=abstained,
        risk_tier=tier,
        action=action,
        review_threshold=review_threshold,
        block_threshold=policy.block_threshold,
        policy_version=policy.version,
        model_version=signals.model_version,
        calibration_version=signals.calibration_version,
        reason_codes=tuple(dict.fromkeys(reasons)),
    )
