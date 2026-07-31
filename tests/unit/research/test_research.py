from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.research.ablation import build_ablation_scores
from sentinelstream.research.config import ResearchConfig
from sentinelstream.research.metadata import collect_reproducibility_metadata
from sentinelstream.research.metrics import (
    bootstrap_interval,
    error_analysis,
    evaluate_research_scores,
    reliability_curve,
    segment_metrics,
    threshold_analysis,
)
from sentinelstream.research.runner import ResearchExperimentRunner
from sentinelstream.security.pii import mask_identifier
from sentinelstream.simulation.generator import TransactionGenerator


def _frame(rows: int = 80) -> pd.DataFrame:
    settings = SimulationSettings(
        random_seed=17,
        customer_count=8,
        merchant_count=5,
        fraud_ratio=0.20,
        duration_days=4,
    )
    records = [
        record
        for record in TransactionGenerator(settings).generate(rows)
        if hasattr(record, "event")
    ]
    return pd.DataFrame(
        [
            record.event.model_dump(mode="json") | {"fraud_label": record.ground_truth.fraud_label}
            for record in records
        ]
    )


@pytest.mark.unit
@pytest.mark.research
def test_research_metrics_are_measured_and_thresholded() -> None:
    frame = pd.DataFrame(
        {
            "transaction_amount": [10.0, 20.0, 30.0, 40.0],
            "fraud_label": [0, 1, 0, 1],
            "country": ["US", "US", "GB", "GB"],
            "customer_id": ["customer-a", "customer-b", "customer-c", "customer-d"],
        }
    )
    scores = np.array([0.1, 0.8, 0.7, 0.9])
    metrics = evaluate_research_scores(frame, scores, model_name="test", threshold=0.75)
    assert metrics["model_name"] == "test"
    assert metrics["true_positive"] == 2
    assert metrics["false_positive"] == 0
    assert metrics["roc_auc"] == pytest.approx(1.0)

    table = threshold_analysis(frame, scores, [0.5, 0.75], model_name="test")
    assert list(table["threshold"]) == [0.5, 0.75]
    assert not reliability_curve(frame, scores).empty


@pytest.mark.unit
@pytest.mark.research
def test_research_statistics_and_reports_mask_entity_identifiers() -> None:
    values = [1.0, 2.0, 3.0, 4.0]
    first = bootstrap_interval(values, resamples=50, seed=4)
    second = bootstrap_interval(values, resamples=50, seed=4)
    assert first == second

    frame = pd.DataFrame(
        {
            "transaction_amount": [10.0, 20.0, 30.0, 40.0],
            "fraud_label": [0, 1, 0, 1],
            "customer_id": ["customer-a", "customer-b", "customer-c", "customer-d"],
        }
    )
    errors = error_analysis(frame, np.array([0.9, 0.1, 0.1, 0.9]), top_n=10)
    assert errors.empty or "customer-a" not in errors.to_string()

    segmented = segment_metrics(
        frame,
        np.array([0.1, 0.8, 0.2, 0.9]),
        segment_column="customer_id",
        model_name="test",
        minimum_segment_size=1,
    )
    assert mask_identifier("customer-a") in set(segmented["segment"])


@pytest.mark.unit
@pytest.mark.research
def test_research_runner_writes_reproducible_outputs(tmp_path: Path) -> None:
    frame = _frame()
    config = ResearchConfig(
        experiment_id="smoke",
        dataset_version="dataset-test",
        source="unit-test",
        output_dir=tmp_path,
        model_names=("logistic_regression", "rules"),
        bootstrap_resamples=4,
        minimum_segment_size=1,
    )
    metadata = collect_reproducibility_metadata(config)
    assert metadata["random_seed"] == 42

    result = ResearchExperimentRunner(config).run(frame)
    output_dir = result.write()
    assert set(result.metrics["model_name"]) == {"logistic_regression", "rules"}
    assert (output_dir / "metrics.csv").is_file()
    assert (output_dir / "report.md").is_file()
    if pytest.importorskip("matplotlib"):
        assert (output_dir / "thresholds-rules.png").is_file()


@pytest.mark.unit
@pytest.mark.research
def test_research_config_rejects_invalid_split() -> None:
    with pytest.raises(ValueError, match="leave test data"):
        ResearchConfig(experiment_id="invalid", train_fraction=0.9, validation_fraction=0.2)


@pytest.mark.unit
@pytest.mark.research
def test_ablation_scores_are_equal_weighted_and_report_missing_sources() -> None:
    source_scores = {
        "supervised": np.array([0.2, 0.8]),
        "rules": np.array([0.4, 0.6]),
    }
    outputs, skipped = build_ablation_scores(
        source_scores, ["supervised_rules", "supervised_anomaly"]
    )
    np.testing.assert_allclose(outputs["supervised_rules"], [0.3, 0.7])
    assert "supervised_anomaly" not in outputs
    assert "anomaly" in skipped["supervised_anomaly"]
