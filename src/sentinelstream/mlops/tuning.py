"""Bounded Optuna tuning for the existing sklearn baselines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split


@dataclass(frozen=True, slots=True)
class TuningResult:
    study_name: str
    best_value: float
    best_params: dict[str, Any]
    trial_count: int


class OptunaTuner:
    """Run a small reproducible study suitable for local development and CI."""

    def __init__(self, *, seed: int = 42, storage: str | None = None) -> None:
        self.seed = seed
        self.storage = storage

    def tune_classifier(
        self,
        features: pd.DataFrame,
        labels: pd.Series | np.ndarray,
        *,
        model_name: str = "logistic_regression",
        study_name: str = "sentinelstream-fraud-tuning",
        n_trials: int = 10,
        timeout_seconds: int | None = 300,
    ) -> TuningResult:
        try:
            import optuna
        except ImportError as exc:
            raise RuntimeError("optuna is required for model tuning") from exc
        if n_trials < 1:
            raise ValueError("n_trials must be positive")
        x_train, x_valid, y_train, y_valid = train_test_split(
            features,
            np.asarray(labels, dtype=int),
            test_size=0.25,
            random_state=self.seed,
            stratify=labels,
        )

        def objective(trial: Any) -> float:
            if model_name == "logistic_regression":
                estimator = LogisticRegression(
                    C=trial.suggest_float("C", 1e-3, 10.0, log=True),
                    max_iter=500,
                    class_weight="balanced",
                    random_state=self.seed,
                )
            elif model_name == "random_forest":
                estimator = RandomForestClassifier(
                    n_estimators=trial.suggest_int("n_estimators", 25, 100),
                    max_depth=trial.suggest_int("max_depth", 2, 10),
                    min_samples_leaf=trial.suggest_int("min_samples_leaf", 1, 5),
                    class_weight="balanced_subsample",
                    random_state=self.seed,
                    n_jobs=1,
                )
            else:
                raise ValueError(f"unsupported tuning model: {model_name}")
            estimator.fit(x_train, y_train)
            probabilities = estimator.predict_proba(x_valid)[:, 1]
            return float(average_precision_score(y_valid, probabilities))

        sampler = optuna.samplers.TPESampler(seed=self.seed)
        pruner = optuna.pruners.MedianPruner(n_startup_trials=2)
        study = optuna.create_study(
            study_name=study_name,
            direction="maximize",
            sampler=sampler,
            pruner=pruner,
            storage=self.storage,
            load_if_exists=True,
        )
        study.optimize(objective, n_trials=n_trials, timeout=timeout_seconds)
        if study.best_trial is None:
            raise RuntimeError("Optuna study did not complete a trial")
        return TuningResult(
            study.study_name, float(study.best_value), dict(study.best_params), len(study.trials)
        )
