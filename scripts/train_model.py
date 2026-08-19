"""Train and compare Phase 2 baselines on a validated dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sentinelstream.data.ingestion import ingest_path, write_quality_report, write_quarantine
from sentinelstream.models.baselines import DEFAULT_MODEL_NAMES
from sentinelstream.models.tracking import ExperimentTracker
from sentinelstream.models.training import train_baselines


def main() -> None:
    parser = argparse.ArgumentParser(description="Train SentinelStream temporal baseline models")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/experiments/baseline_metrics.json"),
    )
    parser.add_argument("--models", nargs="+", default=list(DEFAULT_MODEL_NAMES))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    result = ingest_path(args.input, require_labels=True)
    write_quarantine(result, Path("data/interim/quarantine.jsonl"))
    write_quality_report(result, Path("reports/experiments/data_quality.json"))
    run = train_baselines(
        result.to_frame(),
        model_names=args.models,
        seed=args.seed,
        tracker=ExperimentTracker(Path("reports/experiments/mlflow_metrics.jsonl")),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "metrics": run.metrics.to_dict(orient="records"),
                "feature_columns": list(run.feature_columns),
                "skipped_models": run.skipped_models,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(
        f"valid={len(result.records)} quarantined={len(result.quarantined)} "
        f"models={','.join(sorted(run.models))} output={args.output}"
    )


if __name__ == "__main__":
    main()
