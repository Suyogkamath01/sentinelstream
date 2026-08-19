"""Configurable fusion of model, rules, anomaly, and profile intelligence."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from math import isfinite
from typing import Any

import numpy as np

from sentinelstream.models.calibration import calibration_report
from sentinelstream.models.decision import (
    PredictionSignals,
    RecommendedAction,
    RiskDecision,
    ThresholdPolicy,
    make_decision,
)


class HybridSource(StrEnum):
    """Named intelligence sources that participate in score fusion."""

    SUPERVISED = "supervised"
    ANOMALY = "anomaly"
    RULES = "rules"
    CUSTOMER_PROFILE = "customer_profile"
    MERCHANT_PROFILE = "merchant_profile"
    DEVICE_PROFILE = "device_profile"


@dataclass(frozen=True, slots=True)
class ProfileRisk:
    """Risk state for one customer, merchant, or device entity."""

    entity_id: str
    risk_score: float
    confidence: float = 0.5
    observations: int = 0
    version: str = "unknown"
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.entity_id.strip():
            raise ValueError("profile entity_id must not be empty")
        for value_name, value in (
            ("risk_score", self.risk_score),
            ("confidence", self.confidence),
        ):
            if not isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"profile {value_name} must be between 0 and 1")
        if self.observations < 0:
            raise ValueError("profile observations must be non-negative")


@dataclass(frozen=True, slots=True)
class HybridWeights:
    """Non-negative source weights; missing sources are renormalised."""

    supervised: float = 0.40
    anomaly: float = 0.15
    rules: float = 0.15
    customer_profile: float = 0.10
    merchant_profile: float = 0.10
    device_profile: float = 0.10

    def __post_init__(self) -> None:
        values = (
            self.supervised,
            self.anomaly,
            self.rules,
            self.customer_profile,
            self.merchant_profile,
            self.device_profile,
        )
        if any(not isfinite(value) or value < 0.0 for value in values):
            raise ValueError("hybrid weights must be finite and non-negative")
        if sum(values) <= 0.0:
            raise ValueError("at least one hybrid weight must be positive")

    def for_source(self, source: HybridSource) -> float:
        return {
            HybridSource.SUPERVISED: self.supervised,
            HybridSource.ANOMALY: self.anomaly,
            HybridSource.RULES: self.rules,
            HybridSource.CUSTOMER_PROFILE: self.customer_profile,
            HybridSource.MERCHANT_PROFILE: self.merchant_profile,
            HybridSource.DEVICE_PROFILE: self.device_profile,
        }[source]


@dataclass(frozen=True, slots=True)
class HybridSignals:
    """One transaction's available model and profile evidence."""

    supervised_probability: float | None = None
    anomaly_score: float | None = None
    rule_score: float | None = None
    customer_profile: ProfileRisk | None = None
    merchant_profile: ProfileRisk | None = None
    device_profile: ProfileRisk | None = None
    model_version: str = "unknown"
    calibration_version: str = "unknown"

    def __post_init__(self) -> None:
        for name, value in (
            ("supervised_probability", self.supervised_probability),
            ("anomaly_score", self.anomaly_score),
            ("rule_score", self.rule_score),
        ):
            if value is not None and (not isfinite(value) or not 0.0 <= value <= 1.0):
                raise ValueError(f"{name} must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class HybridResult:
    """Fused score, deterministic decision, and source-level evidence."""

    final_score: float
    confidence: float
    decision: RiskDecision
    source_scores: dict[str, float]
    active_sources: tuple[str, ...]
    missing_sources: tuple[str, ...]
    alert_priority: int
    reason_codes: tuple[str, ...]

    @property
    def is_alert(self) -> bool:
        return self.decision.action in {
            RecommendedAction.REVIEW,
            RecommendedAction.BLOCK,
        }


@dataclass(slots=True)
class HybridFraudEngine:
    """Fuse all available evidence before applying the shared decision policy."""

    weights: HybridWeights = HybridWeights()
    policy: ThresholdPolicy = ThresholdPolicy()

    def score(self, signals: HybridSignals) -> HybridResult:
        evidence = {
            HybridSource.SUPERVISED: signals.supervised_probability,
            HybridSource.ANOMALY: signals.anomaly_score,
            HybridSource.RULES: signals.rule_score,
            HybridSource.CUSTOMER_PROFILE: (
                signals.customer_profile.risk_score if signals.customer_profile else None
            ),
            HybridSource.MERCHANT_PROFILE: (
                signals.merchant_profile.risk_score if signals.merchant_profile else None
            ),
            HybridSource.DEVICE_PROFILE: (
                signals.device_profile.risk_score if signals.device_profile else None
            ),
        }
        active = {
            source: value
            for source, value in evidence.items()
            if value is not None and self.weights.for_source(source) > 0.0
        }
        if not active:
            raise ValueError("hybrid scoring requires at least one active source")
        total_weight = sum(self.weights.for_source(source) for source in active)
        final_score = (
            sum(float(value) * self.weights.for_source(source) for source, value in active.items())
            / total_weight
        )
        confidence_values = {
            source: self._source_confidence(source, float(value), signals)
            for source, value in active.items()
        }
        confidence = self._confidence(
            final_score,
            active,
            confidence_values,
            {source: self.weights.for_source(source) for source in active},
        )
        decision = make_decision(
            PredictionSignals(
                calibrated_probability=final_score,
                anomaly_score=signals.anomaly_score,
                rule_score=signals.rule_score,
                confidence=confidence,
                model_version=signals.model_version,
                calibration_version=signals.calibration_version,
            ),
            self.policy,
        )
        missing = tuple(source.value for source in evidence if source not in active)
        profile_reasons = tuple(
            reason
            for profile in (
                signals.customer_profile,
                signals.merchant_profile,
                signals.device_profile,
            )
            if profile is not None
            for reason in profile.reason_codes
        )
        reasons = tuple(dict.fromkeys((*decision.reason_codes, *profile_reasons)))
        return HybridResult(
            final_score=final_score,
            confidence=confidence,
            decision=decision,
            source_scores={source.value: float(value) for source, value in active.items()},
            active_sources=tuple(source.value for source in active),
            missing_sources=missing,
            alert_priority=self._alert_priority(decision, final_score, confidence),
            reason_codes=reasons,
        )

    def _source_confidence(
        self,
        source: HybridSource,
        value: float,
        signals: HybridSignals,
    ) -> float:
        if source is HybridSource.CUSTOMER_PROFILE and signals.customer_profile:
            return signals.customer_profile.confidence
        if source is HybridSource.MERCHANT_PROFILE and signals.merchant_profile:
            return signals.merchant_profile.confidence
        if source is HybridSource.DEVICE_PROFILE and signals.device_profile:
            return signals.device_profile.confidence
        return 2.0 * abs(value - 0.5)

    @staticmethod
    def _confidence(
        final_score: float,
        active: Mapping[HybridSource, float | None],
        source_confidences: dict[HybridSource, float],
        source_weights: Mapping[HybridSource, float],
    ) -> float:
        weighted_confidence = sum(
            source_confidences[source] * source_weights[source] for source in active
        ) / sum(source_weights.values())
        disagreement = sum(
            abs(float(value) - final_score) for value in active.values() if value is not None
        ) / len(active)
        return float(np.clip(weighted_confidence * (1.0 - disagreement), 0.0, 1.0))

    @staticmethod
    def _alert_priority(decision: RiskDecision, score: float, confidence: float) -> int:
        if decision.action is RecommendedAction.BLOCK:
            base = 100
        elif decision.action is RecommendedAction.REVIEW:
            base = 60
        else:
            return 0
        return int(round(base + score * 20.0 + confidence * 10.0))


@dataclass(frozen=True, slots=True)
class CalibrationValidation:
    """Measured calibration report and explicit acceptance criteria."""

    metrics: dict[str, float | int]
    passed: bool
    max_brier_score: float
    max_expected_calibration_error: float


def validate_calibration(
    labels: Any,
    probabilities: Any,
    *,
    max_brier_score: float = 0.25,
    max_expected_calibration_error: float = 0.10,
    bin_count: int = 10,
) -> CalibrationValidation:
    """Evaluate held-out calibration without inventing or storing results."""

    if max_brier_score < 0.0 or max_expected_calibration_error < 0.0:
        raise ValueError("calibration limits must be non-negative")
    metrics = calibration_report(labels, probabilities, bin_count=bin_count)
    passed = bool(
        metrics["brier_score"] <= max_brier_score
        and metrics["expected_calibration_error"] <= max_expected_calibration_error
    )
    return CalibrationValidation(
        metrics=metrics,
        passed=passed,
        max_brier_score=max_brier_score,
        max_expected_calibration_error=max_expected_calibration_error,
    )


@dataclass(frozen=True, slots=True)
class AblationResult:
    """Observed effect of removing one evidence source from a score."""

    source: str
    baseline_score: float
    ablated_score: float
    score_delta: float
    baseline_action: str
    ablated_action: str


class AblationExperiment:
    """Run reproducible source-removal experiments over supplied signals."""

    def __init__(self, engine: HybridFraudEngine) -> None:
        self.engine = engine

    def run(self, signals: HybridSignals) -> tuple[AblationResult, ...]:
        baseline = self.engine.score(signals)
        results: list[AblationResult] = []
        sources = (
            HybridSource.SUPERVISED,
            HybridSource.ANOMALY,
            HybridSource.RULES,
            HybridSource.CUSTOMER_PROFILE,
            HybridSource.MERCHANT_PROFILE,
            HybridSource.DEVICE_PROFILE,
        )
        for source in sources:
            if self._source_value(signals, source) is None:
                continue
            remaining = [
                other
                for other in sources
                if other is not source and self._source_value(signals, other) is not None
            ]
            if not remaining:
                continue
            ablated = self.engine.score(self._remove_source(signals, source))
            results.append(
                AblationResult(
                    source=source.value,
                    baseline_score=baseline.final_score,
                    ablated_score=ablated.final_score,
                    score_delta=baseline.final_score - ablated.final_score,
                    baseline_action=baseline.decision.action.value,
                    ablated_action=ablated.decision.action.value,
                )
            )
        return tuple(results)

    @staticmethod
    def _source_value(signals: HybridSignals, source: HybridSource) -> float | ProfileRisk | None:
        return {
            HybridSource.SUPERVISED: signals.supervised_probability,
            HybridSource.ANOMALY: signals.anomaly_score,
            HybridSource.RULES: signals.rule_score,
            HybridSource.CUSTOMER_PROFILE: signals.customer_profile,
            HybridSource.MERCHANT_PROFILE: signals.merchant_profile,
            HybridSource.DEVICE_PROFILE: signals.device_profile,
        }[source]

    @staticmethod
    def _remove_source(signals: HybridSignals, source: HybridSource) -> HybridSignals:
        if source is HybridSource.SUPERVISED:
            return replace(signals, supervised_probability=None)
        if source is HybridSource.ANOMALY:
            return replace(signals, anomaly_score=None)
        if source is HybridSource.RULES:
            return replace(signals, rule_score=None)
        if source is HybridSource.CUSTOMER_PROFILE:
            return replace(signals, customer_profile=None)
        if source is HybridSource.MERCHANT_PROFILE:
            return replace(signals, merchant_profile=None)
        return replace(signals, device_profile=None)


def prioritise_alerts(results: Iterable[HybridResult]) -> list[HybridResult]:
    """Return alerts in deterministic priority order for analyst queues."""

    return sorted(
        (result for result in results if result.is_alert),
        key=lambda result: (-result.alert_priority, -result.final_score),
    )
