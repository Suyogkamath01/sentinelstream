"""Probability calibration utilities for fraud-risk scores."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss

CalibrationMethod = Literal["platt", "isotonic"]


def _validated_scores(scores: Any) -> np.ndarray:
    values = np.asarray(scores, dtype=float)
    if values.ndim != 1:
        raise ValueError("scores must be one-dimensional")
    if len(values) == 0 or not np.isfinite(values).all():
        raise ValueError("scores must be non-empty and finite")
    return np.clip(values, 0.0, 1.0)


def _validated_labels(labels: Any, expected_length: int) -> np.ndarray:
    values = np.asarray(labels, dtype=int)
    if values.ndim != 1 or len(values) != expected_length:
        raise ValueError("labels must be one-dimensional and match scores")
    if not np.isin(values, [0, 1]).all():
        raise ValueError("labels must contain only 0 and 1")
    if np.unique(values).size < 2:
        raise ValueError("calibration requires both fraud and non-fraud labels")
    return values


@dataclass(slots=True)
class ProbabilityCalibrator:
    """Fit a calibration mapping on a time-separated calibration partition."""

    method: CalibrationMethod = "isotonic"
    _model: Any = None

    @property
    def version(self) -> str:
        return f"phase4-{self.method}-v1"

    def fit(self, scores: Any, labels: Any) -> ProbabilityCalibrator:
        raw_scores = _validated_scores(scores)
        y_true = _validated_labels(labels, len(raw_scores))
        if self.method == "platt":
            model = LogisticRegression(solver="lbfgs", random_state=42)
            model.fit(raw_scores.reshape(-1, 1), y_true)
        elif self.method == "isotonic":
            model = IsotonicRegression(out_of_bounds="clip")
            model.fit(raw_scores, y_true)
        else:
            raise ValueError(f"unsupported calibration method: {self.method}")
        self._model = model
        return self

    def transform(self, scores: Any) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("calibrator must be fitted before transform")
        raw_scores = _validated_scores(scores)
        if self.method == "platt":
            calibrated = self._model.predict_proba(raw_scores.reshape(-1, 1))[:, 1]
        else:
            calibrated = self._model.predict(raw_scores)
        return np.clip(np.asarray(calibrated, dtype=float), 0.0, 1.0)

    def confidence(self, scores: Any) -> np.ndarray:
        """Return distance from an even-risk decision boundary in [0, 1]."""

        probabilities = self.transform(scores)
        return 2.0 * np.abs(probabilities - 0.5)


def expected_calibration_error(
    labels: Any,
    probabilities: Any,
    *,
    bin_count: int = 10,
) -> float:
    """Calculate the equal-width expected calibration error."""

    if bin_count < 1:
        raise ValueError("bin_count must be positive")
    scores = _validated_scores(probabilities)
    y_true = _validated_labels(labels, len(scores))
    edges = np.linspace(0.0, 1.0, bin_count + 1)
    error = 0.0
    for index, (lower, upper) in enumerate(zip(edges[:-1], edges[1:], strict=True)):
        if index == bin_count - 1:
            in_bin = (scores >= lower) & (scores <= upper)
        else:
            in_bin = (scores >= lower) & (scores < upper)
        if in_bin.any():
            error += float(in_bin.mean()) * abs(
                float(scores[in_bin].mean()) - float(y_true[in_bin].mean())
            )
    return error


def calibration_report(
    labels: Any,
    probabilities: Any,
    *,
    bin_count: int = 10,
) -> dict[str, float | int]:
    """Return compact calibration metrics for a held-out partition."""

    scores = _validated_scores(probabilities)
    y_true = _validated_labels(labels, len(scores))
    return {
        "sample_count": len(scores),
        "brier_score": float(brier_score_loss(y_true, scores)),
        "expected_calibration_error": expected_calibration_error(
            y_true, scores, bin_count=bin_count
        ),
        "mean_predicted_probability": float(scores.mean()),
        "observed_fraud_rate": float(y_true.mean()),
    }
