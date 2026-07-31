"""Fraud-focused evaluation metrics and financial utility measures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    matthews_corrcoef,
    precision_recall_curve,
    precision_score,
    recall_score,
)


@dataclass(frozen=True, slots=True)
class CostConfig:
    """Configurable costs used to make threshold results financially explicit."""

    false_positive_review_cost: float = 3.0
    false_negative_cost_per_amount: float = 1.0
    customer_friction_cost: float = 0.25
    transaction_blocking_cost: float = 0.50


def _recall_at_fixed_precision(y_true: np.ndarray, scores: np.ndarray, target: float) -> float:
    precision, recall, _ = precision_recall_curve(y_true, scores)
    eligible = recall[precision >= target]
    return float(eligible.max()) if eligible.size else 0.0


def _top_k_metrics(y_true: np.ndarray, scores: np.ndarray, k: int) -> tuple[float, float]:
    k = min(max(1, k), len(scores))
    selected = np.argsort(-scores, kind="stable")[:k]
    selected_labels = y_true[selected]
    precision = float(selected_labels.mean())
    total_fraud = int(y_true.sum())
    recall = float(selected_labels.sum() / total_fraud) if total_fraud else 0.0
    return precision, recall


def evaluate_scores(
    frame: pd.DataFrame,
    scores: np.ndarray,
    *,
    model_name: str,
    threshold: float = 0.5,
    top_k: int | None = None,
    cost_config: CostConfig | None = None,
) -> dict[str, Any]:
    """Calculate classification, ranking, calibration, and financial metrics."""

    if "fraud_label" not in frame:
        raise KeyError("fraud_label is required for evaluation")
    y_true = frame["fraud_label"].astype(int).to_numpy()
    probabilities = np.asarray(scores, dtype=float)
    if len(y_true) != len(probabilities):
        raise ValueError("score and label lengths differ")
    if not np.isfinite(probabilities).all():
        raise ValueError("scores contain non-finite values")
    probabilities = np.clip(probabilities, 0.0, 1.0)
    predictions = (probabilities >= threshold).astype(int)
    amounts = frame["transaction_amount"].astype(float).to_numpy()
    fraud_mask = y_true == 1
    detected_amount = float(amounts[fraud_mask & (predictions == 1)].sum())
    missed_amount = float(amounts[fraud_mask & (predictions == 0)].sum())
    costs = cost_config or CostConfig()
    false_positives = int(((predictions == 1) & (y_true == 0)).sum())
    expected_cost = (
        false_positives * costs.false_positive_review_cost
        + missed_amount * costs.false_negative_cost_per_amount
        + int(predictions.sum()) * (costs.customer_friction_cost + costs.transaction_blocking_cost)
    )
    ranking_k = top_k or max(1, round(len(y_true) * 0.01))
    precision_at_k, recall_at_k = _top_k_metrics(y_true, probabilities, ranking_k)
    return {
        "model_name": model_name,
        "threshold": threshold,
        "pr_auc": float(average_precision_score(y_true, probabilities)),
        "average_precision": float(average_precision_score(y_true, probabilities)),
        "precision": float(precision_score(y_true, predictions, zero_division=0)),
        "recall": float(recall_score(y_true, predictions, zero_division=0)),
        "f1": float(f1_score(y_true, predictions, zero_division=0)),
        "f_beta_2": _fbeta(y_true, predictions, beta=2.0),
        "recall_at_fixed_precision_80": _recall_at_fixed_precision(y_true, probabilities, 0.80),
        "precision_at_top_k": precision_at_k,
        "recall_at_top_k": recall_at_k,
        "top_k": ranking_k,
        "matthews_correlation": float(matthews_corrcoef(y_true, predictions)),
        "brier_score": float(brier_score_loss(y_true, probabilities)),
        "fraud_amount_detected": detected_amount,
        "fraud_amount_missed": missed_amount,
        "expected_financial_cost": float(expected_cost),
        "sample_count": len(y_true),
        "fraud_count": int(y_true.sum()),
    }


def _fbeta(y_true: np.ndarray, predictions: np.ndarray, *, beta: float) -> float:
    from sklearn.metrics import fbeta_score

    return float(fbeta_score(y_true, predictions, beta=beta, zero_division=0))
