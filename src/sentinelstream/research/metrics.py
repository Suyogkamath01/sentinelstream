"""Fraud-focused metrics and statistical helpers for research runs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix, roc_auc_score

from sentinelstream.models.calibration import calibration_report
from sentinelstream.models.evaluation import CostConfig, evaluate_scores
from sentinelstream.security.pii import mask_identifier


def _labels_and_scores(frame: pd.DataFrame, scores: Any) -> tuple[np.ndarray, np.ndarray]:
    if "fraud_label" not in frame:
        raise KeyError("fraud_label is required for research evaluation")
    labels = frame["fraud_label"].astype(int).to_numpy()
    probabilities = np.clip(np.asarray(scores, dtype=float), 0.0, 1.0)
    if probabilities.ndim != 1 or len(labels) != len(probabilities):
        raise ValueError("scores must be one-dimensional and match the frame")
    if not np.isfinite(probabilities).all():
        raise ValueError("scores contain non-finite values")
    return labels, probabilities


def evaluate_research_scores(
    frame: pd.DataFrame,
    scores: Any,
    *,
    model_name: str,
    threshold: float = 0.5,
    cost_config: CostConfig | None = None,
) -> dict[str, Any]:
    """Extend the platform's standard metrics with operational error rates."""

    labels, probabilities = _labels_and_scores(frame, scores)
    result = evaluate_scores(
        frame,
        probabilities,
        model_name=model_name,
        threshold=threshold,
        cost_config=cost_config,
    )
    predictions = (probabilities >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(labels, predictions, labels=[0, 1]).ravel()
    result.update(
        {
            "roc_auc": float(roc_auc_score(labels, probabilities))
            if np.unique(labels).size == 2
            else None,
            "specificity": float(tn / (tn + fp)) if tn + fp else 0.0,
            "false_positive_rate": float(fp / (tn + fp)) if tn + fp else 0.0,
            "false_negative_rate": float(fn / (fn + tp)) if fn + tp else 0.0,
            "true_negative": int(tn),
            "false_positive": int(fp),
            "false_negative": int(fn),
            "true_positive": int(tp),
            "alert_volume": int(predictions.sum()),
            "alert_rate": float(predictions.mean()) if len(predictions) else 0.0,
        }
    )
    if np.unique(labels).size == 2:
        result["expected_calibration_error"] = calibration_report(labels, probabilities)[
            "expected_calibration_error"
        ]
    else:
        result["expected_calibration_error"] = None
    return result


def threshold_analysis(
    frame: pd.DataFrame,
    scores: Any,
    thresholds: Sequence[float],
    *,
    model_name: str,
    cost_config: CostConfig | None = None,
) -> pd.DataFrame:
    """Evaluate one score vector under several decision thresholds."""

    rows = [
        evaluate_research_scores(
            frame,
            scores,
            model_name=model_name,
            threshold=float(threshold),
            cost_config=cost_config,
        )
        for threshold in thresholds
    ]
    return pd.DataFrame(rows)


def reliability_curve(frame: pd.DataFrame, scores: Any, *, bin_count: int = 10) -> pd.DataFrame:
    """Return measured probability and outcome rates for calibration plots."""

    labels, probabilities = _labels_and_scores(frame, scores)
    edges = np.linspace(0.0, 1.0, bin_count + 1)
    rows: list[dict[str, Any]] = []
    for index, (lower, upper) in enumerate(zip(edges[:-1], edges[1:], strict=True)):
        in_bin = (
            (probabilities >= lower) & (probabilities <= upper)
            if index == bin_count - 1
            else (probabilities >= lower) & (probabilities < upper)
        )
        if not in_bin.any():
            continue
        rows.append(
            {
                "bin": index,
                "lower": float(lower),
                "upper": float(upper),
                "count": int(in_bin.sum()),
                "mean_probability": float(probabilities[in_bin].mean()),
                "observed_rate": float(labels[in_bin].mean()),
            }
        )
    return pd.DataFrame(rows)


def bootstrap_interval(
    values: Sequence[float],
    statistic: Callable[[np.ndarray], float] = np.mean,
    *,
    resamples: int = 200,
    confidence_level: float = 0.95,
    seed: int = 42,
) -> tuple[float, float]:
    """Compute a reproducible percentile bootstrap interval for a summary statistic."""

    data = np.asarray(values, dtype=float)
    if data.ndim != 1 or not len(data):
        raise ValueError("values must be a non-empty one-dimensional sequence")
    if resamples < 1:
        estimate = float(statistic(data))
        return estimate, estimate
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be between zero and one")
    rng = np.random.default_rng(seed)
    samples = np.empty(resamples, dtype=float)
    for index in range(resamples):
        samples[index] = float(statistic(rng.choice(data, size=len(data), replace=True)))
    alpha = (1.0 - confidence_level) / 2.0
    return float(np.quantile(samples, alpha)), float(np.quantile(samples, 1.0 - alpha))


def bootstrap_metric_interval(
    frame: pd.DataFrame,
    scores: Any,
    *,
    metric: str = "recall",
    threshold: float = 0.5,
    resamples: int = 200,
    confidence_level: float = 0.95,
    seed: int = 42,
) -> tuple[float, float]:
    """Bootstrap a classification metric without leaking rows across partitions."""

    labels, probabilities = _labels_and_scores(frame, scores)
    rng = np.random.default_rng(seed)
    observed: list[float] = []
    for _ in range(max(1, resamples)):
        indices = rng.integers(0, len(labels), size=len(labels))
        sample = frame.reset_index(drop=True).iloc[indices]
        values = evaluate_research_scores(
            sample,
            probabilities[indices],
            model_name="bootstrap",
            threshold=threshold,
        )
        value = values.get(metric)
        if not isinstance(value, (int, float)):
            raise KeyError(f"metric is not numeric: {metric}")
        observed.append(float(value))
    return (
        bootstrap_interval(
            observed,
            resamples=0,
            confidence_level=confidence_level,
            seed=seed,
        )
        if resamples == 0
        else (
            float(np.quantile(observed, (1.0 - confidence_level) / 2.0)),
            float(np.quantile(observed, 1.0 - (1.0 - confidence_level) / 2.0)),
        )
    )


def segment_metrics(
    frame: pd.DataFrame,
    scores: Any,
    *,
    segment_column: str,
    model_name: str,
    threshold: float = 0.5,
    minimum_segment_size: int = 30,
    cost_config: CostConfig | None = None,
) -> pd.DataFrame:
    """Evaluate sufficiently large segments while masking entity identifiers."""

    if segment_column not in frame:
        raise KeyError(f"segment column does not exist: {segment_column}")
    _, probabilities = _labels_and_scores(frame, scores)
    working = frame.reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for value, indices in working.groupby(segment_column, dropna=False).groups.items():
        if len(indices) < minimum_segment_size:
            continue
        positions = np.asarray(list(indices), dtype=int)
        segment = working.iloc[positions]
        metrics = evaluate_research_scores(
            segment,
            probabilities[positions],
            model_name=model_name,
            threshold=threshold,
            cost_config=cost_config,
        )
        metrics["segment_column"] = segment_column
        metrics["segment"] = (
            mask_identifier(value)
            if segment_column.endswith("_id") or segment_column in {"account_id", "card_id"}
            else str(value)
        )
        rows.append(metrics)
    return pd.DataFrame(rows)


def error_analysis(
    frame: pd.DataFrame,
    scores: Any,
    *,
    threshold: float = 0.5,
    top_n: int = 25,
) -> pd.DataFrame:
    """Return a bounded, PII-safe table of the highest-confidence errors."""

    labels, probabilities = _labels_and_scores(frame, scores)
    predictions = (probabilities >= threshold).astype(int)
    errors = np.flatnonzero(predictions != labels)
    ordered = errors[np.argsort(-np.abs(probabilities[errors] - 0.5), kind="stable")[:top_n]]
    rows: list[dict[str, Any]] = []
    for index in ordered:
        row: dict[str, Any] = {
            "row_index": int(index),
            "error_type": "false_positive" if predictions[index] else "false_negative",
            "probability": float(probabilities[index]),
            "fraud_label": int(labels[index]),
            "transaction_amount": float(frame.iloc[index]["transaction_amount"]),
        }
        for column in ("transaction_id", "customer_id", "merchant_id", "device_id"):
            if column in frame:
                row[column] = mask_identifier(frame.iloc[index][column])
        rows.append(row)
    return pd.DataFrame(rows)
