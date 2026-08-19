"""Run the deterministic offline SentinelStream demonstration."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.ingestion import ingest_path
from sentinelstream.data.serialization import write_jsonl, write_parquet
from sentinelstream.research.config import ResearchConfig
from sentinelstream.research.runner import ResearchExperimentRunner
from sentinelstream.simulation.generator import TransactionGenerator


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SentinelStream's offline demo")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("reports/demo"))
    args = parser.parse_args()
    settings = SimulationSettings(random_seed=args.seed, fraud_ratio=0.10)
    records = TransactionGenerator(settings).generate(args.count)
    data_dir = args.output / "data"
    jsonl_path = data_dir / "transactions.jsonl"
    parquet_path = data_dir / "transactions.parquet"
    write_jsonl(records, jsonl_path)
    write_parquet(records, parquet_path)
    validation = ingest_path(parquet_path, require_labels=True)
    frame = validation.to_frame()
    config = ResearchConfig(
        experiment_id="offline-demo",
        dataset_version=f"demo-seed-{args.seed}",
        source=str(parquet_path),
        output_dir=args.output / "research",
        model_names=("logistic_regression", "rules"),
        bootstrap_resamples=20,
        minimum_segment_size=5,
    )
    result = ResearchExperimentRunner(config).run(frame)
    report_dir = result.write()
    summary = pd.DataFrame(
        [
            {
                "generated_records": len(records),
                "valid_records": len(validation.records),
                "quarantined_records": len(validation.quarantined),
                "duplicate_records": validation.duplicate_count,
                "research_report": str(report_dir),
            }
        ]
    )
    args.output.mkdir(parents=True, exist_ok=True)
    summary.to_json(args.output / "summary.json", orient="records", indent=2)
    print(
        f"generated={len(records)} valid={len(validation.records)} "
        f"quarantined={len(validation.quarantined)} "
        f"duplicates={validation.duplicate_count} report={report_dir}"
    )


if __name__ == "__main__":
    main()
