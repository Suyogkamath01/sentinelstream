from pathlib import Path

import pandas as pd

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.validation import validate_records
from sentinelstream.models.evaluation import evaluate_scores
from sentinelstream.models.training import train_baselines
from sentinelstream.models.tracking import ExperimentTracker
from sentinelstream.simulation.generator import TransactionGenerator


def training_frame(count: int = 240) -> pd.DataFrame:
    records = TransactionGenerator(
        SimulationSettings(customer_count=30, merchant_count=12, random_seed=18, fraud_ratio=0.10)
    ).generate(count)
    return validate_records(records, require_labels=True).to_frame()


def test_evaluation_reports_fraud_metrics_without_accuracy() -> None:
    frame = training_frame(60)
    scores = frame["transaction_amount"].rank(pct=True).to_numpy()

    metrics = evaluate_scores(frame, scores, model_name="amount-baseline")

    assert "accuracy" not in metrics
    assert 0.0 <= metrics["pr_auc"] <= 1.0
    assert metrics["fraud_amount_detected"] + metrics["fraud_amount_missed"] > 0
    assert metrics["expected_financial_cost"] >= 0


def test_temporal_baselines_train_and_track(tmp_path: Path) -> None:
    run = train_baselines(
        training_frame(),
        model_names=("dummy", "rules", "logistic_regression", "random_forest", "isolation_forest"),
        tracker=ExperimentTracker(tmp_path / "runs.jsonl", use_mlflow=False),
    )

    assert set(run.models) == {"dummy", "rules", "logistic_regression", "random_forest", "isolation_forest"}
    assert set(run.metrics["partition"]) == {"validation", "test"}
    assert {"pr_auc", "brier_score", "expected_financial_cost"} <= set(run.metrics.columns)
    assert (tmp_path / "runs.jsonl").is_file()

