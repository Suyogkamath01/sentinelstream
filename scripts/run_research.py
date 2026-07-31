"""Run a reproducible SentinelStream research experiment on a labelled dataset."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from sentinelstream.data.ingestion import ingest_path
from sentinelstream.research.config import ResearchConfig
from sentinelstream.research.runner import ResearchExperimentRunner


def _load_frame(path: Path) -> pd.DataFrame:
    result = ingest_path(path, require_labels=True)
    return result.to_frame()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SentinelStream research experiments")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("reports/research"))
    parser.add_argument("--experiment-id", default="local-experiment")
    parser.add_argument("--dataset-version", default="local-dataset")
    parser.add_argument("--feature-version", default="phase3-v1")
    parser.add_argument("--models", default="logistic_regression,rules,isolation_forest")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bootstrap-resamples", type=int, default=200)
    parser.add_argument("--tracking-uri")
    parser.add_argument("--mlflow-experiment", default="sentinelstream-research")
    parser.add_argument("--log-mlflow", action="store_true")
    args = parser.parse_args()
    frame = _load_frame(args.input)
    config = ResearchConfig(
        experiment_id=args.experiment_id,
        dataset_version=args.dataset_version,
        source=str(args.input),
        feature_set_version=args.feature_version,
        output_dir=args.output,
        model_names=tuple(args.models.split(",")),
        random_seed=args.seed,
        bootstrap_resamples=args.bootstrap_resamples,
        mlflow_tracking_uri=args.tracking_uri,
        mlflow_experiment_name=args.mlflow_experiment,
        log_mlflow=args.log_mlflow,
    )
    result = ResearchExperimentRunner(config).run(frame)
    output_dir = result.write()
    model_names = ",".join(result.metrics["model_name"]) if not result.metrics.empty else "none"
    print(
        f"experiment={config.experiment_id} rows={len(frame)} models={model_names} "
        f"skipped={','.join(sorted(result.skipped_models)) or 'none'} output={output_dir}"
    )


if __name__ == "__main__":
    main()
