"""Command-line entry point for Phase 1 sample generation."""

from __future__ import annotations

import argparse
from pathlib import Path

from sentinelstream.config.settings import load_settings
from sentinelstream.data.schemas import SimulatedTransaction
from sentinelstream.data.serialization import write_jsonl, write_parquet
from sentinelstream.simulation.generator import TransactionGenerator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate reproducible SentinelStream transaction events"
    )
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--fraud-ratio", type=float, default=None)
    parser.add_argument("--duplicate-rate", type=float, default=None)
    parser.add_argument("--late-event-rate", type=float, default=None)
    parser.add_argument("--malformed-rate", type=float, default=None)
    parser.add_argument("--out-of-order-rate", type=float, default=None)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--jsonl", type=Path, default=Path("data/samples/transactions.jsonl"))
    parser.add_argument("--parquet", type=Path, default=Path("data/samples/transactions.parquet"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = load_settings(args.config)
    simulation_updates = {
        key: value
        for key, value in {
            "random_seed": args.seed,
            "fraud_ratio": args.fraud_ratio,
            "duplicate_rate": args.duplicate_rate,
            "late_event_rate": args.late_event_rate,
            "malformed_rate": args.malformed_rate,
            "out_of_order_rate": args.out_of_order_rate,
            "replay": True if args.replay else None,
        }.items()
        if value is not None
    }
    simulation = settings.simulation.model_copy(update=simulation_updates)
    settings = settings.model_copy(update={"simulation": simulation})
    records = TransactionGenerator(settings.simulation).generate(args.count)
    write_jsonl(records, args.jsonl)
    write_parquet(records, args.parquet)

    valid = [record for record in records if isinstance(record, SimulatedTransaction)]
    fraud_count = sum(record.ground_truth.fraud_label for record in valid)
    duplicate_count = len(valid) - len({record.event.event_id for record in valid})
    malformed_count = len(records) - len(valid)
    delayed_count = sum(record.event.timestamp < settings.simulation.start_time for record in valid)
    print(
        f"generated={len(records)} valid={len(valid)} fraud={fraud_count} "
        f"duplicates={duplicate_count} delayed={delayed_count} malformed={malformed_count} "
        f"jsonl={args.jsonl} parquet={args.parquet}"
    )
