"""Reproducible temporal baseline training and comparison."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import pandas as pd

from sentinelstream.features.batch import build_batch_features
from sentinelstream.models.baselines import (
    DEFAULT_MODEL_NAMES,
    build_estimator,
    encode_feature_frames,
    rule_scores,
)
from sentinelstream.models.evaluation import CostConfig, evaluate_scores
from sentinelstream.models.tracking import ExperimentTracker
from sentinelstream.data.splitting import TemporalSplit, temporal_split


@dataclass(slots=True)
class BaselineRun:
    """Models, features, and metrics produced by one temporal experiment."""

    split: TemporalSplit
    models: dict[str, Any]
    metrics: pd.DataFrame
    feature_columns: tuple[str, ...]
    skipped_models: dict[str, str] = field(default_factory=dict)


def _labels(frame: pd.DataFrame) -> np.ndarray:
    if "fraud_label" not in frame or frame["fraud_label"].isna().any():
        raise ValueError("training data must contain non-null fraud_label values")
    return frame["fraud_label"].astype(int).to_numpy()


def _probability(estimator: Any, matrix: pd.DataFrame) -> np.ndarray:
    if hasattr(estimator, "predict_proba"):
        return np.asarray(estimator.predict_proba(matrix)[:, 1], dtype=float)
    decision = np.asarray(estimator.decision_function(matrix), dtype=float)
    low, high = float(decision.min()), float(decision.max())
    if high == low:
        return np.full(len(decision), 0.5)
    return np.clip((decision - low) / (high - low), 0.0, 1.0)


def _anomaly_probability(estimator: Any, matrix: pd.DataFrame, reference: np.ndarray) -> np.ndarray:
    scores = -np.asarray(estimator.decision_function(matrix), dtype=float)
    low, high = float(reference.min()), float(reference.max())
    if high == low:
        return np.full(len(scores), 0.5)
    return np.clip((scores - low) / (high - low), 0.0, 1.0)


def train_baselines(
    frame: pd.DataFrame,
    *,
    model_names: Sequence[str] = DEFAULT_MODEL_NAMES,
    seed: int = 42,
    threshold: float = 0.5,
    top_k: int | None = None,
    cost_config: CostConfig | None = None,
    tracker: ExperimentTracker | None = None,
) -> BaselineRun:
    """Train baseline models on a temporal split and evaluate without accuracy optimisation."""

    feature_frame = build_batch_features(frame)
    split = temporal_split(feature_frame)
    train_matrix, validation_matrix, test_matrix = encode_feature_frames(
        split.train, split.validation, split.test
    )
    y_train = _labels(split.train)
    fraud_ratio = float(y_train.mean())
    models: dict[str, Any] = {}
    metric_rows: list[dict[str, Any]] = []
    skipped: dict[str, str] = {}
    for model_name in model_names:
        try:
            if model_name == "rules":
                validation_scores = rule_scores(split.validation)
                test_scores = rule_scores(split.test)
                models[model_name] = "deterministic"
            else:
                estimator = build_estimator(model_name, seed=seed, fraud_ratio=fraud_ratio)
                estimator.fit(train_matrix, y_train)
                models[model_name] = estimator
                if model_name == "isolation_forest":
                    reference = -np.asarray(estimator.decision_function(train_matrix), dtype=float)
                    validation_scores = _anomaly_probability(estimator, validation_matrix, reference)
                    test_scores = _anomaly_probability(estimator, test_matrix, reference)
                else:
                    validation_scores = _probability(estimator, validation_matrix)
                    test_scores = _probability(estimator, test_matrix)
        except RuntimeError as exc:
            skipped[model_name] = str(exc)
            continue
        validation_metrics = evaluate_scores(
            split.validation,
            validation_scores,
            model_name=model_name,
            threshold=threshold,
            top_k=top_k,
            cost_config=cost_config,
        )
        validation_metrics["partition"] = "validation"
        test_metrics = evaluate_scores(
            split.test,
            test_scores,
            model_name=model_name,
            threshold=threshold,
            top_k=top_k,
            cost_config=cost_config,
        )
        test_metrics["partition"] = "test"
        metric_rows.extend([validation_metrics, test_metrics])
        if tracker is not None:
            tracker.log(
                model_name,
                test_metrics,
                {
                    "seed": seed,
                    "threshold": threshold,
                    "train_rows": len(split.train),
                    "feature_count": len(train_matrix.columns),
                },
            )
    return BaselineRun(
        split=split,
        models=models,
        metrics=pd.DataFrame(metric_rows),
        feature_columns=tuple(train_matrix.columns),
        skipped_models=skipped,
    )
