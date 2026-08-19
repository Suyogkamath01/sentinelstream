"""Configurable score-source ablations for fair research comparisons."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

DEFAULT_ABLATION_COMBINATIONS = (
    "supervised_only",
    "anomaly_only",
    "rules_only",
    "supervised_anomaly",
    "supervised_rules",
    "full_hybrid",
    "without_customer_risk",
    "without_merchant_risk",
    "without_device_risk",
    "without_behavioural_window_features",
)


def _required_sources(name: str, available: tuple[str, ...]) -> tuple[str, ...]:
    fixed = {
        "supervised_only": ("supervised",),
        "anomaly_only": ("anomaly",),
        "rules_only": ("rules",),
        "supervised_anomaly": ("supervised", "anomaly"),
        "supervised_rules": ("supervised", "rules"),
    }
    if name in fixed:
        return fixed[name]
    if name == "full_hybrid":
        return available
    if name == "without_customer_risk":
        return tuple(source for source in available if source != "customer_profile")
    if name == "without_merchant_risk":
        return tuple(source for source in available if source != "merchant_profile")
    if name == "without_device_risk":
        return tuple(source for source in available if source != "device_profile")
    if name == "without_behavioural_window_features":
        return tuple(source for source in available if source != "behavioural_window")
    raise ValueError(f"unsupported ablation combination: {name}")


def build_ablation_scores(
    source_scores: Mapping[str, np.ndarray],
    combinations: Sequence[str] = DEFAULT_ABLATION_COMBINATIONS,
) -> tuple[dict[str, np.ndarray], dict[str, str]]:
    """Build equal-weight ablation scores and explain unavailable combinations."""

    if not source_scores:
        raise ValueError("at least one score source is required")
    names = tuple(source_scores)
    lengths = {len(np.asarray(values)) for values in source_scores.values()}
    if len(lengths) != 1:
        raise ValueError("all score sources must have the same length")
    outputs: dict[str, np.ndarray] = {}
    skipped: dict[str, str] = {}
    for combination in combinations:
        excluded_source = {
            "without_customer_risk": "customer_profile",
            "without_merchant_risk": "merchant_profile",
            "without_device_risk": "device_profile",
            "without_behavioural_window_features": "behavioural_window",
        }.get(combination)
        if excluded_source and excluded_source not in source_scores:
            skipped[combination] = f"source unavailable: {excluded_source}"
            continue
        required = _required_sources(combination, names)
        missing = tuple(source for source in required if source not in source_scores)
        if missing or not required:
            skipped[combination] = f"missing score sources: {', '.join(missing) or 'none'}"
            continue
        values = np.vstack([np.asarray(source_scores[source], dtype=float) for source in required])
        outputs[combination] = np.clip(values.mean(axis=0), 0.0, 1.0)
    return outputs, skipped
