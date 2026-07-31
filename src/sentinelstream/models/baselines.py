"""Baseline estimators and deterministic rule scores."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

DEFAULT_MODEL_NAMES = (
    "dummy",
    "rules",
    "logistic_regression",
    "random_forest",
    "xgboost",
    "isolation_forest",
)

DROP_COLUMNS = {
    "fraud_label",
    "fraud_type",
    "timestamp",
    "transaction_id",
    "event_id",
    "customer_id",
    "account_id",
    "card_id",
    "merchant_id",
    "device_id",
    "ip_address",
    "city",
    "latitude",
    "longitude",
}


def encode_feature_frames(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fit a simple train-only one-hot vocabulary and align future partitions."""

    def select(frame: pd.DataFrame) -> pd.DataFrame:
        columns = [column for column in frame.columns if column not in DROP_COLUMNS]
        return frame[columns].copy()

    train_encoded = pd.get_dummies(select(train), dtype=float)
    validation_encoded = pd.get_dummies(select(validation), dtype=float)
    test_encoded = pd.get_dummies(select(test), dtype=float)
    validation_encoded = validation_encoded.reindex(columns=train_encoded.columns, fill_value=0.0)
    test_encoded = test_encoded.reindex(columns=train_encoded.columns, fill_value=0.0)
    return (
        train_encoded.fillna(0.0),
        validation_encoded.fillna(0.0),
        test_encoded.fillna(0.0),
    )


def rule_scores(frame: pd.DataFrame) -> np.ndarray:
    """Combine transparent deterministic signals into a bounded rule score."""

    score = np.zeros(len(frame), dtype=float)
    score += np.where(frame.get("amount_z_score", 0) >= 3.0, 0.35, 0.0)
    score += np.where(frame.get("customer_seconds_since_previous", -1).between(0, 60), 0.25, 0.0)
    score += np.where(frame.get("device_novelty", 0) == 1, 0.20, 0.0)
    score += np.where(frame.get("country_novelty", 0) == 1, 0.15, 0.0)
    score += np.where(frame.get("is_cash_withdrawal", 0) == 1, 0.10, 0.0)
    score += np.where(frame["transaction_amount"] >= 2_000.0, 0.15, 0.0)
    return np.clip(score, 0.0, 1.0)


def build_estimator(name: str, *, seed: int, fraud_ratio: float) -> Any:
    """Construct a reproducible estimator for a named baseline."""

    if name == "dummy":
        return DummyClassifier(strategy="prior")
    if name == "logistic_regression":
        return make_pipeline(
            StandardScaler(),
            LogisticRegression(class_weight="balanced", max_iter=1_000, random_state=seed),
        )
    if name == "random_forest":
        return RandomForestClassifier(
            n_estimators=150,
            class_weight="balanced_subsample",
            random_state=seed,
            n_jobs=1,
        )
    if name == "isolation_forest":
        contamination = min(max(fraud_ratio, 0.001), 0.5)
        return IsolationForest(
            n_estimators=150,
            contamination=contamination,
            random_state=seed,
            n_jobs=1,
        )
    if name == "xgboost":
        try:
            from xgboost import XGBClassifier
        except ImportError as exc:
            raise RuntimeError("xgboost is required for the xgboost baseline") from exc
        return XGBClassifier(
            n_estimators=150,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.85,
            colsample_bytree=0.85,
            scale_pos_weight=max(1.0, (1.0 - fraud_ratio) / max(fraud_ratio, 1e-6)),
            eval_metric="logloss",
            random_state=seed,
            n_jobs=1,
        )
    raise ValueError(f"unknown model baseline: {name}")
