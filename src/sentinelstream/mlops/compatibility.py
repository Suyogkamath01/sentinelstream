"""Serving-contract validation for registered estimators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CompatibilityReport:
    compatible: bool
    has_predict: bool
    has_probability: bool
    expected_features: tuple[str, ...]
    model_features: tuple[str, ...] | None
    messages: tuple[str, ...]


def validate_model_compatibility(
    model: Any, expected_features: list[str] | tuple[str, ...]
) -> CompatibilityReport:
    expected = tuple(expected_features)
    has_predict = callable(getattr(model, "predict", None))
    has_probability = callable(getattr(model, "predict_proba", None))
    model_features = getattr(model, "feature_names_in_", None)
    if model_features is None and hasattr(model, "named_steps"):
        for estimator in reversed(list(model.named_steps.values())):
            model_features = getattr(estimator, "feature_names_in_", None)
            if model_features is not None:
                break
    actual = tuple(str(value) for value in model_features) if model_features is not None else None
    messages: list[str] = []
    if not has_predict:
        messages.append("model does not expose predict")
    if not has_probability:
        messages.append("model does not expose predict_proba")
    if actual is not None and actual != expected:
        messages.append("model feature names do not match the serving contract")
    return CompatibilityReport(
        not messages, has_predict, has_probability, expected, actual, tuple(messages)
    )


def assert_model_compatible(
    model: Any, expected_features: list[str] | tuple[str, ...]
) -> CompatibilityReport:
    report = validate_model_compatibility(model, expected_features)
    if not report.compatible:
        raise ValueError("; ".join(report.messages))
    return report
