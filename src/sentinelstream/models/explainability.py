"""Versioned model explanations and fraud reason codes."""

from __future__ import annotations

import importlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance

EXPLANATION_VERSION = "phase5-v1"


class ExplainerUnavailableError(RuntimeError):
    """Raised when an explicitly requested optional explainer cannot run."""


@dataclass(frozen=True, slots=True)
class FeatureContribution:
    """One feature's local contribution to the risk score."""

    feature: str
    value: float
    contribution: float
    method: str

    @property
    def direction(self) -> str:
        return "risk_increasing" if self.contribution >= 0 else "risk_decreasing"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"direction": self.direction}


@dataclass(frozen=True, slots=True)
class ReasonCode:
    """A concise, human-readable fraud indicator."""

    code: str
    message: str
    feature: str
    contribution: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Counterfactual:
    """A prototype feature change and its measured score effect."""

    feature: str
    current_value: float
    suggested_value: float
    resulting_probability: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Explanation:
    """A persisted local explanation for one scored transaction."""

    version: str
    method: str
    predicted_probability: float
    base_value: float
    contributions: tuple[FeatureContribution, ...]
    reason_codes: tuple[ReasonCode, ...]
    summary: str
    counterfactuals: tuple[Counterfactual, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "method": self.method,
            "predicted_probability": self.predicted_probability,
            "base_value": self.base_value,
            "contributions": [item.to_dict() for item in self.contributions],
            "reason_codes": [item.to_dict() for item in self.reason_codes],
            "summary": self.summary,
            "counterfactuals": [item.to_dict() for item in self.counterfactuals],
        }


@dataclass(frozen=True, slots=True)
class FeatureImportance:
    """A global feature-importance estimate."""

    feature: str
    importance: float
    uncertainty: float
    rank: int
    method: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class GlobalExplanation:
    """A versioned global explanation for a model and evaluation sample."""

    version: str
    method: str
    importances: tuple[FeatureImportance, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "method": self.method,
            "importances": [item.to_dict() for item in self.importances],
        }


def _as_frame(
    values: pd.Series | dict[str, Any],
    feature_order: list[str] | None = None,
) -> pd.DataFrame:
    frame = values.to_frame().T if isinstance(values, pd.Series) else pd.DataFrame([values])
    if feature_order is not None:
        frame = frame.reindex(columns=feature_order)
    return frame.reset_index(drop=True)


def _predict_probability(estimator: Any, frame: pd.DataFrame) -> np.ndarray:
    if hasattr(estimator, "predict_proba"):
        probabilities = np.asarray(estimator.predict_proba(frame)[:, 1], dtype=float)
    elif hasattr(estimator, "decision_function"):
        decisions = np.asarray(estimator.decision_function(frame), dtype=float)
        probabilities = 1.0 / (1.0 + np.exp(-decisions))
    else:
        probabilities = np.asarray(estimator.predict(frame), dtype=float)
    if probabilities.ndim != 1 or not np.isfinite(probabilities).all():
        raise ValueError("estimator returned invalid risk scores")
    return np.clip(probabilities, 0.0, 1.0)


def _normalise_shap_values(values: Any) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    if array.ndim == 3:
        array = array[:, :, 1]
    if array.ndim != 2:
        raise ValueError("SHAP values must have shape (rows, features)")
    return array


def _shap_explainer(estimator: Any, background: pd.DataFrame) -> Any:
    try:
        shap = importlib.import_module("shap")
        return shap.Explainer(estimator, background)
    except Exception as exc:
        raise ExplainerUnavailableError(
            "SHAP is unavailable or incompatible with this estimator"
        ) from exc


def _local_shap(
    estimator: Any,
    current: pd.DataFrame,
    background: pd.DataFrame,
) -> tuple[np.ndarray, float]:
    explainer = _shap_explainer(estimator, background)
    result = explainer(current)
    values = _normalise_shap_values(result.values)[0]
    base_values = np.asarray(result.base_values, dtype=float)
    if base_values.ndim == 2:
        base_value = float(base_values[0, 1])
    else:
        base_value = float(base_values.reshape(-1)[0])
    return values, base_value


def _numeric_baseline(current: pd.DataFrame, background: pd.DataFrame) -> pd.DataFrame:
    baseline = background.reindex(columns=current.columns).copy()
    for column in current.columns:
        if not pd.api.types.is_numeric_dtype(baseline[column]):
            baseline[column] = current[column]
        else:
            baseline[column] = baseline[column].mean()
    return baseline.fillna(current)


def _ablation_contributions(
    estimator: Any,
    current: pd.DataFrame,
    background: pd.DataFrame,
) -> tuple[np.ndarray, float]:
    baseline = _numeric_baseline(current, background)
    current_probability = float(_predict_probability(estimator, current)[0])
    base_value = float(_predict_probability(estimator, baseline)[0])
    contributions: list[float] = []
    for column in current.columns:
        masked = current.copy()
        masked[column] = baseline[column]
        masked_probability = float(_predict_probability(estimator, masked)[0])
        contributions.append(current_probability - masked_probability)
    return np.asarray(contributions, dtype=float), base_value


def _reason_for(contribution: FeatureContribution) -> tuple[str, str] | None:
    feature = contribution.feature.lower()
    value = contribution.value
    if contribution.contribution <= 0:
        return None
    if feature in {"transaction_amount", "amount_z_score", "amount_log1p"}:
        if feature == "amount_z_score" and value < 3.0:
            return None
        if feature == "transaction_amount" and value < 2_000.0:
            return None
        if feature == "amount_log1p" and value < np.log1p(2_000.0):
            return None
        return "unusually_high_transaction_amount", "Unusually high transaction amount"
    if feature == "device_novelty" and value >= 1:
        return "first_transaction_from_device", "First transaction from this device"
    if feature in {"impossible_travel", "geographic_velocity_kmh"}:
        if feature == "geographic_velocity_kmh" and value < 900.0:
            return None
        return "geographically_impossible_travel", "Geographically impossible travel"
    if feature.startswith("recent_transaction_count_") and value >= 5:
        return "excessive_recent_transaction_velocity", "Excessive recent transaction velocity"
    if feature == "merchant_novelty" and value >= 1:
        return "new_merchant_for_customer", "New merchant for this customer"
    if feature == "country_novelty" and value >= 1:
        return "new_country_for_customer", "New country for this customer"
    if feature == "merchant_category_novelty" and value >= 1:
        return "new_merchant_category", "New merchant category for this customer"
    if feature in {"decline_rate_prior", "failed_transaction_rate_prior"} and value >= 0.5:
        return "prior_payment_failures", "High prior payment-failure rate"
    if feature == "is_cash_withdrawal" and value >= 1:
        return "cash_withdrawal_risk", "Cash withdrawal requires additional review"
    if feature == "customer_seconds_since_previous" and 0 <= value <= 60:
        return "rapid_repeat_transaction", "Rapid repeat transaction"
    return None


def build_reason_codes(
    contributions: tuple[FeatureContribution, ...],
    *,
    max_reasons: int = 3,
) -> tuple[ReasonCode, ...]:
    """Map risk-increasing feature contributions to stable reason codes."""

    if max_reasons < 1:
        raise ValueError("max_reasons must be positive")
    reasons: list[ReasonCode] = []
    seen: set[str] = set()
    for contribution in sorted(contributions, key=lambda item: item.contribution, reverse=True):
        reason = _reason_for(contribution)
        if reason is None or reason[0] in seen:
            continue
        seen.add(reason[0])
        reasons.append(
            ReasonCode(
                code=reason[0],
                message=reason[1],
                feature=contribution.feature,
                contribution=contribution.contribution,
            )
        )
        if len(reasons) == max_reasons:
            break
    return tuple(reasons)


def generate_counterfactuals(
    estimator: Any,
    values: pd.Series | dict[str, Any],
    background: pd.DataFrame,
    *,
    threshold: float = 0.5,
    max_count: int = 3,
) -> tuple[Counterfactual, ...]:
    """Suggest baseline-value changes that reduce a transaction's score."""

    if not 0.0 <= threshold <= 1.0:
        raise ValueError("threshold must be between 0 and 1")
    if max_count < 1:
        raise ValueError("max_count must be positive")
    current = _as_frame(values)
    baseline = _numeric_baseline(current, background)
    current_probability = float(_predict_probability(estimator, current)[0])
    candidates: list[Counterfactual] = []
    for column in current.columns:
        if not pd.api.types.is_numeric_dtype(current[column]):
            continue
        candidate = current.copy()
        candidate[column] = baseline[column]
        resulting_probability = float(_predict_probability(estimator, candidate)[0])
        if current_probability >= threshold and resulting_probability < threshold:
            candidates.append(
                Counterfactual(
                    feature=column,
                    current_value=float(current.at[0, column]),
                    suggested_value=float(baseline.at[0, column]),
                    resulting_probability=resulting_probability,
                )
            )
    candidates.sort(key=lambda item: item.resulting_probability)
    return tuple(candidates[:max_count])


def explain_local(
    estimator: Any,
    values: pd.Series | dict[str, Any],
    *,
    background: pd.DataFrame | None = None,
    feature_order: list[str] | None = None,
    method: str = "auto",
    top_k: int = 8,
    threshold: float = 0.5,
) -> Explanation:
    """Create a local SHAP explanation with a deterministic ablation fallback."""

    if method not in {"auto", "shap", "ablation"}:
        raise ValueError("method must be auto, shap, or ablation")
    if top_k < 1:
        raise ValueError("top_k must be positive")
    current = _as_frame(values, feature_order)
    reference = background if background is not None else current
    reference = reference.reindex(columns=current.columns).fillna(0.0)
    if method == "shap":
        raw_contributions, base_value = _local_shap(estimator, current, reference)
        selected_method = "shap"
    elif method == "ablation":
        raw_contributions, base_value = _ablation_contributions(estimator, current, reference)
        selected_method = "ablation"
    else:
        try:
            raw_contributions, base_value = _local_shap(estimator, current, reference)
            selected_method = "shap"
        except ExplainerUnavailableError:
            raw_contributions, base_value = _ablation_contributions(
                estimator, current, reference
            )
            selected_method = "ablation"
    probability = float(_predict_probability(estimator, current)[0])
    contributions = tuple(
        FeatureContribution(
            feature=column,
            value=float(current.at[0, column]),
            contribution=float(contribution),
            method=selected_method,
        )
        for column, contribution in zip(current.columns, raw_contributions, strict=True)
    )
    contributions = tuple(
        sorted(contributions, key=lambda item: abs(item.contribution), reverse=True)[:top_k]
    )
    reasons = build_reason_codes(contributions)
    reason_text = ", ".join(reason.message.lower() for reason in reasons)
    summary = (
        f"Risk probability {probability:.3f}. "
        + (f"Key indicators: {reason_text}." if reason_text else "No dominant coded indicator.")
    )
    counterfactuals = generate_counterfactuals(
        estimator,
        current.iloc[0],
        reference,
        threshold=threshold,
        max_count=min(3, top_k),
    )
    return Explanation(
        version=EXPLANATION_VERSION,
        method=selected_method,
        predicted_probability=probability,
        base_value=base_value,
        contributions=contributions,
        reason_codes=reasons,
        summary=summary,
        counterfactuals=counterfactuals,
    )


def explain_global(
    estimator: Any,
    features: pd.DataFrame,
    labels: pd.Series | np.ndarray,
    *,
    method: str = "auto",
    n_repeats: int = 5,
    max_samples: int = 500,
) -> GlobalExplanation:
    """Create global SHAP importance with permutation fallback."""

    if method not in {"auto", "shap", "permutation"}:
        raise ValueError("method must be auto, shap, or permutation")
    if n_repeats < 1 or max_samples < 1:
        raise ValueError("n_repeats and max_samples must be positive")
    sample = features.head(max_samples)
    if method in {"auto", "shap"}:
        try:
            explainer = _shap_explainer(estimator, sample)
            values = _normalise_shap_values(explainer(sample).values)
            means = np.abs(values).mean(axis=0)
            deviations = np.abs(values).std(axis=0)
            selected_method = "shap"
        except ExplainerUnavailableError:
            if method == "shap":
                raise
            selected_method = "permutation"
    else:
        selected_method = "permutation"
    if selected_method == "permutation":
        result = permutation_importance(
            estimator,
            sample,
            labels[: len(sample)],
            scoring="average_precision",
            n_repeats=n_repeats,
            random_state=42,
        )
        means = result.importances_mean
        deviations = result.importances_std
    order = np.argsort(-np.abs(means), kind="stable")
    importances = tuple(
        FeatureImportance(
            feature=str(features.columns[index]),
            importance=float(means[index]),
            uncertainty=float(deviations[index]),
            rank=rank,
            method=selected_method,
        )
        for rank, index in enumerate(order, start=1)
    )
    return GlobalExplanation(
        version=EXPLANATION_VERSION,
        method=selected_method,
        importances=importances,
    )


def explanation_stability(first: Explanation, second: Explanation) -> float:
    """Compare the overlap and direction of two local explanations."""

    first_map = {item.feature: np.sign(item.contribution) for item in first.contributions}
    second_map = {item.feature: np.sign(item.contribution) for item in second.contributions}
    features = set(first_map) | set(second_map)
    if not features:
        return 1.0
    agreements = sum(
        feature in second_map and first_map[feature] == second_map[feature]
        for feature in first_map
    )
    return float(agreements / len(features))


class ExplanationStore:
    """Append and read versioned explanations from JSON Lines."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, explanation: Explanation) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(explanation.to_dict(), sort_keys=True) + "\n")

    def read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]
