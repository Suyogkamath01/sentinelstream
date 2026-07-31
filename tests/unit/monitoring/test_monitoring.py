from __future__ import annotations

import json

import pandas as pd
from prometheus_client import CollectorRegistry

from sentinelstream.monitoring.drift import DriftMonitor, build_dataset_metadata
from sentinelstream.monitoring.health import HealthChecker
from sentinelstream.monitoring.metrics import PrometheusMetrics


def test_prometheus_metrics_are_low_cardinality_and_renderable() -> None:
    metrics = PrometheusMetrics(CollectorRegistry(), namespace="test")
    metrics.record_prediction(
        risk_tier="critical", action="block", model_version="v1", alert=True, priority=1
    )
    metrics.record_streaming_event("spark", "duplicate", 2)
    metrics.set_drift("feature", "amount", 0.3)
    rendered = metrics.render().decode()
    assert "test_predictions_generated_total" in rendered
    assert "transaction_id" not in rendered
    assert "test_drift_score" in rendered


def test_health_checker_preserves_dependency_failure() -> None:
    checker = HealthChecker(
        {
            "database": lambda: ("healthy", "ok"),
            "redis": lambda: (_ for _ in ()).throw(RuntimeError("offline")),
        }
    )
    summary = checker.check()
    assert summary.status == "unavailable"
    assert summary.components[1].status == "unavailable"


def test_drift_report_and_dataset_metadata_are_persisted(tmp_path) -> None:
    reference = pd.DataFrame({"amount": [1.0, 2.0, 3.0], "country": ["US", "US", "IN"]})
    current = pd.DataFrame({"amount": [10.0, 20.0, 30.0], "country": ["IN", "IN", "GB"]})
    metadata = build_dataset_metadata(
        reference,
        version="dataset-v1",
        source="unit-test",
        feature_set_version="features-v1",
    )
    assert len(metadata.checksum) == 64
    report = DriftMonitor(tmp_path, feature_threshold=0.1).generate(
        reference,
        current,
        reference_version="dataset-v1",
        current_version="dataset-v2",
        model_version="model-v1",
    )
    assert report.drifted_features >= 1
    assert (tmp_path / f"{report.report_id}.json").is_file()
    assert (tmp_path / f"{report.report_id}.html").is_file()
    payload = json.loads((tmp_path / f"{report.report_id}.json").read_text())
    assert payload["model_version"] == "model-v1"
