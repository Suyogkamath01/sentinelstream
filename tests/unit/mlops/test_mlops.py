from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression

from sentinelstream.mlops.compatibility import validate_model_compatibility
from sentinelstream.mlops.datasets import DatasetVersionStore
from sentinelstream.mlops.promotion import (
    EvaluationSnapshot,
    PromotionCriteria,
    PromotionWorkflow,
)
from sentinelstream.mlops.registry import ModelRegistry
from sentinelstream.mlops.tracking import MLflowTracker
from sentinelstream.mlops.tuning import OptunaTuner


def test_dataset_version_store_and_promotion_criteria(tmp_path) -> None:
    frame = pd.DataFrame({"amount": [1.0, 2.0], "country": ["US", "IN"]})
    store = DatasetVersionStore(tmp_path / "metadata")
    metadata = store.create(
        frame,
        version="v1",
        source="fixture",
        feature_set_version="features-v1",
        split_metadata={"train": 1, "test": 1},
    )
    assert store.load("v1").checksum == metadata.checksum
    workflow = PromotionWorkflow(PromotionCriteria(minimum_recall=0.8, maximum_fraud_cost=10.0))
    report = workflow.evaluate(
        EvaluationSnapshot("candidate", {"recall": 0.9, "expected_financial_cost": 4.0}),
        EvaluationSnapshot("production", {"recall": 0.8, "expected_financial_cost": 6.0}),
    )
    assert report.approved
    assert report.differences["recall"] == pytest.approx(0.1)


def test_model_compatibility_and_optuna_objective() -> None:
    features = pd.DataFrame(
        {"amount": [0.1, 0.2, 0.8, 0.9, 0.3, 0.7], "velocity": [0, 1, 2, 3, 0, 2]}
    )
    labels = np.array([0, 0, 1, 1, 0, 1])
    model = LogisticRegression(max_iter=200).fit(features, labels)
    report = validate_model_compatibility(model, ["amount", "velocity"])
    assert report.compatible
    result = OptunaTuner(seed=7).tune_classifier(
        features,
        labels,
        n_trials=2,
        timeout_seconds=30,
        study_name="unit-test-study",
    )
    assert result.trial_count == 2
    assert 0.0 <= result.best_value <= 1.0


def test_mlflow_tracking_and_registry_workflow(tmp_path) -> None:
    tracking_uri = f"sqlite:///{tmp_path / 'mlflow.db'}"
    tracker = MLflowTracker(tracking_uri, "unit-test-experiment", registry_uri=tracking_uri)
    features = pd.DataFrame({"amount": [0.1, 0.2, 0.8, 0.9], "velocity": [0, 1, 2, 3]})
    labels = np.array([0, 0, 1, 1])
    model = LogisticRegression(max_iter=200).fit(features, labels)
    run = tracker.log_run(
        run_name="unit-test-run",
        params={"seed": 7},
        metrics={"recall": 1.0},
        estimator=model,
        input_example=features.head(1),
    )
    assert run.run_id
    assert run.model_uri
    registered = ModelRegistry(tracking_uri=tracking_uri, registry_uri=tracking_uri).register(
        run.model_uri,
        name="unit-test-model",
        tags={"feature_set_version": "features-v1"},
    )
    promoted = ModelRegistry(tracking_uri=tracking_uri, registry_uri=tracking_uri).set_alias(
        "unit-test-model", registered.version, "production"
    )
    assert "production" in promoted.aliases
