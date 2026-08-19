"""Evidence-based model promotion decisions with explicit rollback context."""

from __future__ import annotations

from dataclasses import dataclass

from sentinelstream.mlops.registry import ModelRegistry, RegisteredModelVersion


@dataclass(frozen=True, slots=True)
class PromotionCriteria:
    minimum_recall: float | None = None
    minimum_precision: float | None = None
    maximum_fraud_cost: float | None = None
    maximum_calibration_error: float | None = None


@dataclass(frozen=True, slots=True)
class EvaluationSnapshot:
    model_version: str
    metrics: dict[str, float]
    dataset_version: str | None = None
    feature_set_version: str | None = None


@dataclass(frozen=True, slots=True)
class PromotionCheck:
    criterion: str
    passed: bool
    observed: float | None
    required: float | None
    detail: str


@dataclass(frozen=True, slots=True)
class PromotionReport:
    candidate_version: str
    approved: bool
    checks: tuple[PromotionCheck, ...]
    differences: dict[str, float]
    current_production_version: str | None


class PromotionWorkflow:
    """Evaluate a candidate and require an explicit approved promotion call."""

    def __init__(self, criteria: PromotionCriteria) -> None:
        self.criteria = criteria

    def evaluate(
        self,
        candidate: EvaluationSnapshot,
        current: EvaluationSnapshot | None = None,
    ) -> PromotionReport:
        candidate_checks = (
            self._minimum(candidate, "recall", self.criteria.minimum_recall),
            self._minimum(candidate, "precision", self.criteria.minimum_precision),
            self._maximum(candidate, "expected_financial_cost", self.criteria.maximum_fraud_cost),
            self._maximum(
                candidate, "expected_calibration_error", self.criteria.maximum_calibration_error
            ),
        )
        checks = [check for check in candidate_checks if check is not None]
        differences = {}
        if current:
            for key in set(candidate.metrics).intersection(current.metrics):
                differences[key] = candidate.metrics[key] - current.metrics[key]
        return PromotionReport(
            candidate_version=candidate.model_version,
            approved=all(check.passed for check in checks),
            checks=tuple(checks),
            differences=differences,
            current_production_version=current.model_version if current else None,
        )

    def promote(
        self,
        registry: ModelRegistry,
        *,
        name: str,
        candidate_version: str,
        report: PromotionReport,
        alias: str = "production",
    ) -> RegisteredModelVersion:
        if report.candidate_version != candidate_version:
            raise ValueError("promotion report does not match candidate version")
        if not report.approved:
            raise ValueError("candidate does not satisfy promotion criteria")
        return registry.set_alias(name, candidate_version, alias)

    @staticmethod
    def _minimum(
        snapshot: EvaluationSnapshot, metric: str, required: float | None
    ) -> PromotionCheck | None:
        if required is None:
            return None
        observed = snapshot.metrics.get(metric)
        return PromotionCheck(
            metric,
            observed is not None and observed >= required,
            observed,
            required,
            "minimum requirement",
        )

    @staticmethod
    def _maximum(
        snapshot: EvaluationSnapshot, metric: str, required: float | None
    ) -> PromotionCheck | None:
        if required is None:
            return None
        observed = snapshot.metrics.get(metric)
        return PromotionCheck(
            metric,
            observed is not None and observed <= required,
            observed,
            required,
            "maximum requirement",
        )
