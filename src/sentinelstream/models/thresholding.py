"""Financially explicit threshold optimisation for fraud alerts."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True, slots=True)
class CostModel:
    """Costs used to compare intervention thresholds on a labelled partition."""

    false_positive_review_cost: float = 3.0
    false_negative_cost_per_amount: float = 1.0
    customer_friction_cost: float = 0.25
    transaction_blocking_cost: float = 0.50
    recovered_funds_rate: float = 0.0

    def __post_init__(self) -> None:
        values = (
            self.false_positive_review_cost,
            self.false_negative_cost_per_amount,
            self.customer_friction_cost,
            self.transaction_blocking_cost,
        )
        if any(value < 0 for value in values):
            raise ValueError("cost values must be non-negative")
        if not 0.0 <= self.recovered_funds_rate <= 1.0:
            raise ValueError("recovered_funds_rate must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class ThresholdOptimizationResult:
    """Best threshold and its measured operating point."""

    threshold: float
    expected_cost: float
    alert_count: int
    fraud_detected_amount: float
    fraud_missed_amount: float
    evaluated_thresholds: int
    capacity_limited: bool


def _validated_inputs(labels: Any, scores: Any, amounts: Any) -> tuple[np.ndarray, ...]:
    y_true = np.asarray(labels, dtype=int)
    probabilities = np.asarray(scores, dtype=float)
    transaction_amounts = np.asarray(amounts, dtype=float)
    if any(values.ndim != 1 for values in (y_true, probabilities, transaction_amounts)):
        raise ValueError("labels, scores, and amounts must be one-dimensional")
    if not (len(y_true) == len(probabilities) == len(transaction_amounts)) or not len(y_true):
        raise ValueError("labels, scores, and amounts must be non-empty and aligned")
    if not np.isin(y_true, [0, 1]).all():
        raise ValueError("labels must contain only 0 and 1")
    if not np.isfinite(probabilities).all() or not np.isfinite(transaction_amounts).all():
        raise ValueError("scores and amounts must be finite")
    if (transaction_amounts < 0).any():
        raise ValueError("transaction amounts must be non-negative")
    return y_true, np.clip(probabilities, 0.0, 1.0), transaction_amounts


def expected_financial_cost(
    labels: Any,
    intervention: Any,
    amounts: Any,
    *,
    cost_model: CostModel | None = None,
) -> float:
    """Estimate cost for a binary intervention decision on each transaction."""

    intervention_values = np.asarray(intervention, dtype=bool)
    if intervention_values.ndim != 1:
        raise ValueError("intervention must be one-dimensional")
    y_true, _, transaction_amounts = _validated_inputs(
        labels, np.zeros(len(intervention_values)), amounts
    )
    if len(intervention_values) != len(y_true):
        raise ValueError("intervention must match labels")
    costs = cost_model or CostModel()
    false_positives = ((intervention_values == 1) & (y_true == 0)).sum()
    missed_amount = float(
        transaction_amounts[(intervention_values == 0) & (y_true == 1)].sum()
    )
    effective_missed_cost = missed_amount * costs.false_negative_cost_per_amount * (
        1.0 - costs.recovered_funds_rate
    )
    intervention_cost = intervention_values.sum() * (
        costs.customer_friction_cost + costs.transaction_blocking_cost
    )
    return float(
        false_positives * costs.false_positive_review_cost
        + effective_missed_cost
        + intervention_cost
    )


def _candidate_thresholds(scores: np.ndarray, candidates: Iterable[float] | None) -> np.ndarray:
    values = np.asarray(list(candidates), dtype=float) if candidates is not None else np.array([])
    if values.ndim != 1 or (len(values) and not np.isfinite(values).all()):
        raise ValueError("candidate thresholds must be finite and one-dimensional")
    if len(values) and ((values < 0).any() or (values > 1).any()):
        raise ValueError("candidate thresholds must be between 0 and 1")
    defaults = np.linspace(0.0, 1.0, 201)
    return np.unique(np.concatenate((defaults, np.clip(scores, 0.0, 1.0), values)))


def optimize_threshold(
    labels: Any,
    scores: Any,
    amounts: Any,
    *,
    cost_model: CostModel | None = None,
    candidate_thresholds: Iterable[float] | None = None,
    max_alerts: int | None = None,
) -> ThresholdOptimizationResult:
    """Select the lowest-cost threshold, optionally subject to review capacity."""

    y_true, probabilities, transaction_amounts = _validated_inputs(labels, scores, amounts)
    if max_alerts is not None and max_alerts < 0:
        raise ValueError("max_alerts must be non-negative")
    costs = cost_model or CostModel()
    candidates = _candidate_thresholds(probabilities, candidate_thresholds)
    best: ThresholdOptimizationResult | None = None
    capacity_limited = max_alerts is not None
    for threshold in candidates:
        intervention = probabilities >= threshold
        alert_count = int(intervention.sum())
        if max_alerts is not None and alert_count > max_alerts:
            continue
        missed = (y_true == 1) & ~intervention
        result = ThresholdOptimizationResult(
            threshold=float(threshold),
            expected_cost=expected_financial_cost(
                y_true, intervention, transaction_amounts, cost_model=costs
            ),
            alert_count=alert_count,
            fraud_detected_amount=float(transaction_amounts[(y_true == 1) & intervention].sum()),
            fraud_missed_amount=float(transaction_amounts[missed].sum()),
            evaluated_thresholds=len(candidates),
            capacity_limited=capacity_limited,
        )
        if best is None or (result.expected_cost, result.alert_count) < (
            best.expected_cost,
            best.alert_count,
        ):
            best = result
    if best is None:
        raise ValueError("no threshold satisfies the review capacity")
    return best
