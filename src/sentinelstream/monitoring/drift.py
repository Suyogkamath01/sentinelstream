"""Dataset metadata and lightweight drift reports with optional Evidently output."""

from __future__ import annotations

import hashlib
import html
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class DatasetMetadata:
    """Traceability metadata for a reference or current dataset."""

    version: str
    row_count: int
    columns: tuple[str, ...]
    dtypes: dict[str, str]
    checksum: str
    generated_at: str
    source: str
    feature_set_version: str
    split_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"columns": list(self.columns)}


def build_dataset_metadata(
    frame: pd.DataFrame,
    *,
    version: str,
    source: str,
    feature_set_version: str,
    split_metadata: dict[str, Any] | None = None,
) -> DatasetMetadata:
    """Create a stable metadata record without copying the dataset."""

    ordered = frame.sort_index(axis=1)
    digest = hashlib.sha256()
    digest.update(json.dumps(list(ordered.columns), sort_keys=True).encode())
    digest.update(json.dumps([str(dtype) for dtype in ordered.dtypes], sort_keys=True).encode())
    digest.update(pd.util.hash_pandas_object(ordered, index=True).to_numpy().tobytes())
    return DatasetMetadata(
        version=version,
        row_count=len(frame),
        columns=tuple(str(column) for column in frame.columns),
        dtypes={str(column): str(dtype) for column, dtype in frame.dtypes.items()},
        checksum=digest.hexdigest(),
        generated_at=datetime.now(UTC).isoformat(),
        source=source,
        feature_set_version=feature_set_version,
        split_metadata=split_metadata or {},
    )


@dataclass(frozen=True, slots=True)
class DriftMetric:
    feature: str
    kind: str
    score: float
    threshold: float
    drifted: bool
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DriftReport:
    report_id: str
    generated_at: str
    reference_version: str
    current_version: str
    model_version: str | None
    dataset_version: str | None
    feature_metrics: tuple[DriftMetric, ...]
    prediction_metric: DriftMetric | None
    data_quality: dict[str, Any]
    evidently_report_path: str | None

    @property
    def drifted_features(self) -> int:
        return sum(metric.drifted for metric in self.feature_metrics)

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "generated_at": self.generated_at,
            "reference_version": self.reference_version,
            "current_version": self.current_version,
            "model_version": self.model_version,
            "dataset_version": self.dataset_version,
            "feature_metrics": [metric.to_dict() for metric in self.feature_metrics],
            "prediction_metric": self.prediction_metric.to_dict()
            if self.prediction_metric
            else None,
            "data_quality": self.data_quality,
            "evidently_report_path": self.evidently_report_path,
            "drifted_features": self.drifted_features,
        }


def _numeric_distance(reference: pd.Series, current: pd.Series) -> float:
    reference_values = reference.dropna().astype(float).to_numpy()
    current_values = current.dropna().astype(float).to_numpy()
    if not len(reference_values) or not len(current_values):
        return 1.0 if len(reference_values) != len(current_values) else 0.0
    edges = np.unique(np.quantile(reference_values, np.linspace(0.0, 1.0, 11)))
    if len(edges) < 2:
        return min(abs(float(reference_values.mean()) - float(current_values.mean())), 1.0)
    reference_hist, _ = np.histogram(reference_values, bins=edges)
    current_hist, _ = np.histogram(current_values, bins=edges)
    reference_share = reference_hist / max(reference_hist.sum(), 1)
    current_share = current_hist / max(current_hist.sum(), 1)
    return float(0.5 * np.abs(reference_share - current_share).sum())


def _categorical_distance(reference: pd.Series, current: pd.Series) -> float:
    reference_share = reference.fillna("<missing>").astype(str).value_counts(normalize=True)
    current_share = current.fillna("<missing>").astype(str).value_counts(normalize=True)
    categories = reference_share.index.union(current_share.index)
    return float(
        0.5
        * np.abs(
            reference_share.reindex(categories, fill_value=0)
            - current_share.reindex(categories, fill_value=0)
        ).sum()
    )


def _quality(frame: pd.DataFrame) -> dict[str, Any]:
    missing = frame.isna().mean().sort_values(ascending=False)
    return {
        "row_count": len(frame),
        "missing_rate": {str(key): float(value) for key, value in missing.items()},
        "duplicate_rows": int(frame.duplicated().sum()),
        "column_count": len(frame.columns),
    }


class DriftMonitor:
    """Compare reference/current windows and persist inspectable reports."""

    def __init__(
        self,
        output_dir: Path,
        *,
        feature_threshold: float = 0.20,
        prediction_threshold: float = 0.20,
    ) -> None:
        self.output_dir = output_dir
        self.feature_threshold = feature_threshold
        self.prediction_threshold = prediction_threshold

    def compare(
        self,
        reference: pd.DataFrame,
        current: pd.DataFrame,
        *,
        reference_version: str = "reference",
        current_version: str = "current",
        prediction_column: str = "final_risk_score",
        model_version: str | None = None,
        dataset_version: str | None = None,
    ) -> DriftReport:
        feature_metrics: list[DriftMetric] = []
        for feature in sorted(set(reference.columns).intersection(current.columns)):
            reference_series = reference[feature]
            current_series = current[feature]
            if pd.api.types.is_numeric_dtype(reference_series) and pd.api.types.is_numeric_dtype(
                current_series
            ):
                kind, score = "numeric", _numeric_distance(reference_series, current_series)
            else:
                kind, score = "categorical", _categorical_distance(reference_series, current_series)
            missing_delta = abs(
                float(reference_series.isna().mean()) - float(current_series.isna().mean())
            )
            feature_metrics.append(
                DriftMetric(
                    feature=str(feature),
                    kind=kind,
                    score=score,
                    threshold=self.feature_threshold,
                    drifted=score >= self.feature_threshold,
                    details={"missing_rate_delta": missing_delta},
                )
            )
        prediction_metric = None
        if prediction_column in reference and prediction_column in current:
            score = _numeric_distance(reference[prediction_column], current[prediction_column])
            prediction_metric = DriftMetric(
                feature=prediction_column,
                kind="prediction",
                score=score,
                threshold=self.prediction_threshold,
                drifted=score >= self.prediction_threshold,
                details={},
            )
        return DriftReport(
            report_id=str(uuid4()),
            generated_at=datetime.now(UTC).isoformat(),
            reference_version=reference_version,
            current_version=current_version,
            model_version=model_version,
            dataset_version=dataset_version,
            feature_metrics=tuple(feature_metrics),
            prediction_metric=prediction_metric,
            data_quality={"reference": _quality(reference), "current": _quality(current)},
            evidently_report_path=None,
        )

    def persist(
        self, report: DriftReport, reference: pd.DataFrame, current: pd.DataFrame
    ) -> DriftReport:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        json_path = self.output_dir / f"{report.report_id}.json"
        html_path = self.output_dir / f"{report.report_id}.html"
        json_path.write_text(
            json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
        )
        html_path.write_text(self._html(report), encoding="utf-8")
        evidently_path = self._evidently_report(report, reference, current)
        if evidently_path:
            report = DriftReport(
                report_id=report.report_id,
                generated_at=report.generated_at,
                reference_version=report.reference_version,
                current_version=report.current_version,
                model_version=report.model_version,
                dataset_version=report.dataset_version,
                feature_metrics=report.feature_metrics,
                prediction_metric=report.prediction_metric,
                data_quality=report.data_quality,
                evidently_report_path=str(evidently_path),
            )
            json_path.write_text(
                json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
            )
        return report

    def generate(
        self, reference: pd.DataFrame, current: pd.DataFrame, **kwargs: Any
    ) -> DriftReport:
        report = self.compare(reference, current, **kwargs)
        return self.persist(report, reference, current)

    @staticmethod
    def _html(report: DriftReport) -> str:
        rows = "".join(
            f"<tr><td>{html.escape(metric.feature)}</td><td>{metric.kind}</td>"
            f"<td>{metric.score:.4f}</td><td>{metric.threshold:.4f}</td>"
            f"<td>{'yes' if metric.drifted else 'no'}</td></tr>"
            for metric in report.feature_metrics
        )
        return (
            "<html><head><title>SentinelStream drift report</title></head><body>"
            f"<h1>Drift report {html.escape(report.report_id)}</h1>"
            f"<p>Reference: {html.escape(report.reference_version)}; "
            f"Current: {html.escape(report.current_version)}</p>"
            "<table><tr><th>Feature</th><th>Kind</th><th>Score</th><th>Threshold</th><th>Drifted</th></tr>"
            f"{rows}</table></body></html>"
        )

    def _evidently_report(
        self, report: DriftReport, reference: pd.DataFrame, current: pd.DataFrame
    ) -> Path | None:
        try:
            from evidently import Report
            from evidently.presets import DataDriftPreset

            path = self.output_dir / f"{report.report_id}.evidently.html"
            snapshot = Report(metrics=[DataDriftPreset()]).run(
                current_data=current,
                reference_data=reference,
            )
            snapshot.save_html(str(path))
            return path
        except Exception:
            return None
