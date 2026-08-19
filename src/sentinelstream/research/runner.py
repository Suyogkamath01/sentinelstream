"""Run fair temporal model comparisons and persist inspectable outputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from sentinelstream.data.splitting import TemporalSplit, temporal_split
from sentinelstream.features.batch import build_batch_features
from sentinelstream.mlops.datasets import DatasetVersionStore
from sentinelstream.mlops.tracking import MLflowTracker
from sentinelstream.models.baselines import build_estimator, encode_feature_frames, rule_scores
from sentinelstream.models.evaluation import CostConfig
from sentinelstream.research.ablation import build_ablation_scores
from sentinelstream.research.config import ResearchConfig
from sentinelstream.research.metadata import collect_reproducibility_metadata
from sentinelstream.research.metrics import (
    bootstrap_metric_interval,
    error_analysis,
    evaluate_research_scores,
    reliability_curve,
    segment_metrics,
    threshold_analysis,
)
from sentinelstream.research.plots import plot_reliability_curve, plot_threshold_analysis


@dataclass(slots=True)
class ResearchResult:
    """Measured outputs and metadata from one research run."""

    config: ResearchConfig
    metadata: dict[str, Any]
    metrics: pd.DataFrame
    thresholds: dict[str, pd.DataFrame]
    reliability: dict[str, pd.DataFrame]
    errors: dict[str, pd.DataFrame]
    segments: dict[str, pd.DataFrame]
    skipped_models: dict[str, str]
    ablation_metrics: pd.DataFrame
    skipped_ablations: dict[str, str]

    def write(self) -> Path:
        output_dir = self.config.output_dir / self.config.experiment_id
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "metadata.json").write_text(
            json.dumps(self.metadata, indent=2, sort_keys=True, default=str), encoding="utf-8"
        )
        (output_dir / "config.json").write_text(
            self.config.model_dump_json(indent=2), encoding="utf-8"
        )
        (output_dir / "skipped.json").write_text(
            json.dumps(
                {
                    "models": self.skipped_models,
                    "ablations": self.skipped_ablations,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.metrics.to_csv(output_dir / "metrics.csv", index=False)
        self.ablation_metrics.to_csv(output_dir / "ablation-metrics.csv", index=False)
        for name, table in self.thresholds.items():
            table.to_csv(output_dir / f"thresholds-{name}.csv", index=False)
        for name, table in self.reliability.items():
            table.to_csv(output_dir / f"reliability-{name}.csv", index=False)
        for name, table in self.errors.items():
            table.to_csv(output_dir / f"errors-{name}.csv", index=False)
        for name, table in self.segments.items():
            table.to_csv(output_dir / f"segments-{name}.csv", index=False)
        self._write_report(output_dir)
        return output_dir

    def _write_report(self, output_dir: Path) -> None:
        lines = [
            f"# Research run: `{self.config.experiment_id}`",
            "",
            f"**Question:** {self.config.research_question}",
            f"**Hypothesis:** {self.config.hypothesis}",
            "",
            (
                "Results below are measured outputs from this run. "
                "A skipped model is not treated as a result."
            ),
            "",
            "## Metrics",
            "",
            self.metrics.to_string(index=False)
            if not self.metrics.empty
            else "No metrics were produced.",
            "",
            "## Limitations",
            "",
            "## Ablation metrics",
            "",
            self.ablation_metrics.to_string(index=False)
            if not self.ablation_metrics.empty
            else "No ablation combinations had all required score sources.",
            "",
            "- Synthetic labels, if used, do not establish performance on production fraud.",
            "- Segment rows are omitted when their sample size is below the configured minimum.",
            (
                "- Financial metrics are only meaningful when cost assumptions reflect "
                "the deployment context."
            ),
        ]
        (output_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


class ResearchExperimentRunner:
    """Train comparable baselines on one temporal split and analyse their outputs."""

    def __init__(self, config: ResearchConfig, *, cost_config: CostConfig | None = None) -> None:
        self.config = config
        self.cost_config = cost_config

    def run(self, frame: pd.DataFrame) -> ResearchResult:
        self._validate_frame(frame)
        feature_frame = build_batch_features(frame)
        split = temporal_split(
            feature_frame,
            train_fraction=self.config.train_fraction,
            validation_fraction=self.config.validation_fraction,
        )
        dataset_store = DatasetVersionStore(
            self.config.output_dir / self.config.experiment_id / "datasets"
        )
        dataset_metadata = dataset_store.create(
            feature_frame,
            version=self.config.dataset_version,
            source=self.config.source,
            feature_set_version=self.config.feature_set_version,
            split_metadata={
                "train_rows": len(split.train),
                "validation_rows": len(split.validation),
                "test_rows": len(split.test),
                "train_fraction": self.config.train_fraction,
                "validation_fraction": self.config.validation_fraction,
            },
        )
        metadata = collect_reproducibility_metadata(
            self.config, dataset_metadata=dataset_metadata.to_dict()
        )
        scores, skipped = self._fit_score_models(split)
        return self._analyse(split.test, scores, skipped, metadata)

    def _fit_score_models(
        self, split: TemporalSplit
    ) -> tuple[dict[str, np.ndarray], dict[str, str]]:
        train_matrix, _, test_matrix = encode_feature_frames(
            split.train, split.validation, split.test
        )
        labels = split.train["fraud_label"].astype(int).to_numpy()
        fraud_ratio = float(labels.mean())
        outputs: dict[str, np.ndarray] = {}
        skipped: dict[str, str] = {}
        for name in self.config.model_names:
            try:
                if name == "rules":
                    outputs[name] = rule_scores(split.test)
                    continue
                estimator = build_estimator(
                    name, seed=self.config.random_seed, fraud_ratio=fraud_ratio
                )
                estimator.fit(train_matrix, labels)
                if name == "isolation_forest":
                    train_anomaly = -np.asarray(
                        estimator.decision_function(train_matrix), dtype=float
                    )
                    test_anomaly = -np.asarray(
                        estimator.decision_function(test_matrix), dtype=float
                    )
                    low, high = float(train_anomaly.min()), float(train_anomaly.max())
                    outputs[name] = (
                        np.full(len(test_anomaly), 0.5)
                        if high == low
                        else np.clip((test_anomaly - low) / (high - low), 0.0, 1.0)
                    )
                elif hasattr(estimator, "predict_proba"):
                    outputs[name] = np.asarray(
                        estimator.predict_proba(test_matrix)[:, 1], dtype=float
                    )
                else:
                    decision = np.asarray(estimator.decision_function(test_matrix), dtype=float)
                    low, high = float(decision.min()), float(decision.max())
                    outputs[name] = (
                        np.full(len(decision), 0.5)
                        if high == low
                        else np.clip((decision - low) / (high - low), 0.0, 1.0)
                    )
            except (RuntimeError, ValueError) as exc:
                skipped[name] = str(exc)
        return outputs, skipped

    def _analyse(
        self,
        test_frame: pd.DataFrame,
        scores: dict[str, np.ndarray],
        skipped: dict[str, str],
        metadata: dict[str, Any],
    ) -> ResearchResult:
        metrics: list[dict[str, Any]] = []
        threshold_tables: dict[str, pd.DataFrame] = {}
        reliability_tables: dict[str, pd.DataFrame] = {}
        error_tables: dict[str, pd.DataFrame] = {}
        segment_tables: dict[str, pd.DataFrame] = {}
        for name, values in scores.items():
            row = evaluate_research_scores(
                test_frame,
                values,
                model_name=name,
                threshold=self.config.thresholds[len(self.config.thresholds) // 2],
                cost_config=self.cost_config,
            )
            row["dataset_version"] = self.config.dataset_version
            row["feature_set_version"] = self.config.feature_set_version
            recall_low, recall_high = bootstrap_metric_interval(
                test_frame,
                values,
                metric="recall",
                threshold=self.config.thresholds[len(self.config.thresholds) // 2],
                resamples=self.config.bootstrap_resamples,
                confidence_level=self.config.confidence_level,
                seed=self.config.random_seed,
            )
            row["recall_ci_low"] = recall_low
            row["recall_ci_high"] = recall_high
            metrics.append(row)
            threshold_tables[name] = threshold_analysis(
                test_frame,
                values,
                self.config.thresholds,
                model_name=name,
                cost_config=self.cost_config,
            )
            reliability_tables[name] = reliability_curve(test_frame, values)
            error_tables[name] = error_analysis(test_frame, values)
            if "country" in test_frame:
                segment_tables[name] = segment_metrics(
                    test_frame,
                    values,
                    segment_column="country",
                    model_name=name,
                    threshold=self.config.thresholds[len(self.config.thresholds) // 2],
                    minimum_segment_size=self.config.minimum_segment_size,
                    cost_config=self.cost_config,
                )
        source_scores: dict[str, np.ndarray] = {}
        if "supervised" in scores:
            source_scores["supervised"] = scores["supervised"]
        if "supervised" not in source_scores and "logistic_regression" in scores:
            source_scores["supervised"] = scores["logistic_regression"]
        if "anomaly" in scores:
            source_scores["anomaly"] = scores["anomaly"]
        elif "isolation_forest" in scores:
            source_scores["anomaly"] = scores["isolation_forest"]
        if "rules" in scores:
            source_scores["rules"] = scores["rules"]
        for source in (
            "customer_profile",
            "merchant_profile",
            "device_profile",
            "behavioural_window",
        ):
            if source in scores:
                source_scores[source] = scores[source]
        ablation_scores, skipped_ablations = build_ablation_scores(
            source_scores, self.config.ablation_combinations
        )
        ablation_rows = [
            evaluate_research_scores(
                test_frame,
                values,
                model_name=name,
                threshold=self.config.thresholds[len(self.config.thresholds) // 2],
                cost_config=self.cost_config,
            )
            | {"evaluation_kind": "ablation"}
            for name, values in ablation_scores.items()
        ]
        result = ResearchResult(
            config=self.config,
            metadata=metadata,
            metrics=pd.DataFrame(metrics),
            thresholds=threshold_tables,
            reliability=reliability_tables,
            errors=error_tables,
            segments=segment_tables,
            skipped_models=skipped,
            ablation_metrics=pd.DataFrame(ablation_rows),
            skipped_ablations=skipped_ablations,
        )
        output_dir = self.config.output_dir / self.config.experiment_id
        try:
            for name, table in threshold_tables.items():
                plot_threshold_analysis(table, output_dir / f"thresholds-{name}.png")
            for name, table in reliability_tables.items():
                plot_reliability_curve(table, output_dir / f"reliability-{name}.png")
        except RuntimeError:
            # The research dependency group adds plots; tabular results remain useful without it.
            pass
        if self.config.log_mlflow and self.config.mlflow_tracking_uri and not result.metrics.empty:
            tracker = MLflowTracker(
                self.config.mlflow_tracking_uri, self.config.mlflow_experiment_name
            )
            for row in result.metrics.to_dict(orient="records"):
                numeric = {
                    key: float(value)
                    for key, value in row.items()
                    if isinstance(value, (float, int)) and not isinstance(value, bool)
                }
                tracker.log_run(
                    run_name=f"{self.config.experiment_id}-{row['model_name']}",
                    params={
                        "dataset_version": self.config.dataset_version,
                        "feature_set_version": self.config.feature_set_version,
                        "random_seed": self.config.random_seed,
                    },
                    metrics=numeric,
                    artifacts={
                        "thresholds": output_dir / f"thresholds-{row['model_name']}.png",
                        "reliability": output_dir / f"reliability-{row['model_name']}.png",
                    },
                    tags={"experiment_id": self.config.experiment_id},
                )
        return result

    @staticmethod
    def _validate_frame(frame: pd.DataFrame) -> None:
        required = {
            "timestamp",
            "transaction_amount",
            "fraud_label",
            "customer_id",
            "merchant_id",
            "device_id",
            "country",
        }
        missing = required.difference(frame.columns)
        if missing:
            raise KeyError(f"research frame is missing columns: {sorted(missing)}")
        if frame.empty:
            raise ValueError("research frame must not be empty")
