"""Small, non-interactive plots used in research reports."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _pyplot() -> Any:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required to generate research plots") from exc
    return plt


def plot_threshold_analysis(metrics: pd.DataFrame, path: Path) -> Path:
    """Save precision, recall, and expected cost across thresholds."""

    plt = _pyplot()
    path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8, 4.5))
    axis.plot(metrics["threshold"], metrics["precision"], marker="o", label="precision")
    axis.plot(metrics["threshold"], metrics["recall"], marker="o", label="recall")
    axis.plot(
        metrics["threshold"],
        metrics["expected_financial_cost"],
        marker="o",
        label="expected financial cost",
    )
    axis.set(xlabel="Decision threshold", ylabel="Measured value", title="Threshold analysis")
    axis.legend()
    figure.tight_layout()
    figure.savefig(path, dpi=140)
    plt.close(figure)
    return path


def plot_reliability_curve(reliability: pd.DataFrame, path: Path) -> Path:
    """Save a measured reliability curve for one score vector."""

    plt = _pyplot()
    path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(6, 5))
    axis.plot([0, 1], [0, 1], linestyle="--", color="grey", label="perfect calibration")
    if not reliability.empty:
        axis.plot(
            reliability["mean_probability"],
            reliability["observed_rate"],
            marker="o",
            label="observed",
        )
    axis.set(xlabel="Mean predicted probability", ylabel="Observed fraud rate", title="Reliability")
    axis.legend()
    figure.tight_layout()
    figure.savefig(path, dpi=140)
    plt.close(figure)
    return path
