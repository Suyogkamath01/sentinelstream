"""Small experiment-tracking adapter with optional MLflow integration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4


class ExperimentTracker:
    """Persist metrics locally and mirror them to MLflow when installed."""

    def __init__(self, path: Path | None = None, *, use_mlflow: bool = True) -> None:
        self.path = path
        self.use_mlflow = use_mlflow

    def log(self, model_name: str, metrics: dict[str, Any], params: dict[str, Any]) -> None:
        record = {
            "run_id": str(uuid4()),
            "model_name": model_name,
            "params": params,
            "metrics": metrics,
        }
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        if not self.use_mlflow:
            return
        try:
            import mlflow

            with mlflow.start_run(run_name=model_name, nested=True):
                mlflow.log_params({key: str(value) for key, value in params.items()})
                mlflow.log_metrics(
                    {
                        key: float(value)
                        for key, value in metrics.items()
                        if isinstance(value, (int, float))
                    }
                )
        except Exception:
            # Tracking must not make a completed model-training run fail.
            return
