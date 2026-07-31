"""Explicit MLflow tracking adapter for reproducible model runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


class MLflowUnavailableError(RuntimeError):
    """Raised when an MLflow operation cannot be performed."""


@dataclass(frozen=True, slots=True)
class LoggedRun:
    run_id: str
    experiment_id: str
    model_uri: str | None


class MLflowTracker:
    """Log genuine parameters, metrics, artifacts, signatures, and models to MLflow."""

    def __init__(
        self, tracking_uri: str, experiment_name: str, *, registry_uri: str | None = None
    ) -> None:
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        self.registry_uri = registry_uri

    def _mlflow(self) -> Any:
        try:
            import mlflow
        except ImportError as exc:
            raise MLflowUnavailableError(
                "mlflow-skinny is required for experiment tracking"
            ) from exc
        mlflow.set_tracking_uri(self.tracking_uri)
        if self.registry_uri:
            mlflow.set_registry_uri(self.registry_uri)
        mlflow.set_experiment(self.experiment_name)
        return mlflow

    def log_run(
        self,
        *,
        run_name: str,
        params: dict[str, Any],
        metrics: dict[str, float],
        estimator: Any | None = None,
        input_example: pd.DataFrame | dict[str, Any] | None = None,
        artifacts: dict[str, Path] | None = None,
        tags: dict[str, str] | None = None,
        artifact_path: str = "model",
    ) -> LoggedRun:
        mlflow = self._mlflow()
        try:
            with mlflow.start_run(run_name=run_name) as run:
                mlflow.log_params({key: str(value) for key, value in params.items()})
                mlflow.log_metrics({key: float(value) for key, value in metrics.items()})
                if tags:
                    mlflow.set_tags(tags)
                for name, path in (artifacts or {}).items():
                    mlflow.log_artifact(str(path), artifact_path=name)
                model_uri = None
                if estimator is not None:
                    signature = None
                    if input_example is not None:
                        signature = mlflow.models.infer_signature(
                            input_example, estimator.predict(input_example)
                        )
                    logged = mlflow.sklearn.log_model(
                        estimator,
                        artifact_path=artifact_path,
                        signature=signature,
                        input_example=input_example,
                        serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE,
                    )
                    model_uri = logged.model_uri
                return LoggedRun(run.info.run_id, run.info.experiment_id, model_uri)
        except Exception as exc:
            raise MLflowUnavailableError(f"MLflow run logging failed: {exc}") from exc

    def log_tuning_summary(
        self, study_name: str, best_value: float, best_params: dict[str, Any]
    ) -> None:
        mlflow = self._mlflow()
        try:
            with mlflow.start_run(run_name=f"optuna-{study_name}"):
                mlflow.log_params({key: str(value) for key, value in best_params.items()})
                mlflow.log_metric("best_objective", float(best_value))
                mlflow.set_tag("study_name", study_name)
        except Exception as exc:
            raise MLflowUnavailableError(f"MLflow tuning logging failed: {exc}") from exc
