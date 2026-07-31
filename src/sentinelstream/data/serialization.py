"""Writers for generated JSON Lines and Parquet datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from sentinelstream.data.schemas import SimulatedTransaction

type GeneratedRecord = SimulatedTransaction | dict[str, Any]


def record_to_json(record: GeneratedRecord) -> dict[str, Any]:
    if isinstance(record, SimulatedTransaction):
        return record.to_json_record()
    return record


def record_to_parquet_row(record: GeneratedRecord) -> dict[str, Any]:
    if not isinstance(record, SimulatedTransaction):
        return {
            "record_kind": "malformed",
            "malformed_reason": record.get("malformed_reason", "unknown"),
            "raw_record": json.dumps(record, sort_keys=True),
        }

    event = record.event.model_dump(mode="json")
    ground_truth = record.ground_truth.model_dump(mode="json")
    return {
        "record_kind": "valid",
        **event,
        "simulation_fraud_label": ground_truth["fraud_label"],
        "simulation_fraud_type": ground_truth["fraud_type"],
    }


def write_jsonl(records: list[GeneratedRecord], path: Path) -> None:
    """Write nested event records, including malformed records, to JSON Lines."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record_to_json(record), sort_keys=True) + "\n")


def write_parquet(records: list[GeneratedRecord], path: Path) -> None:
    """Write a tabular representation with explicit simulation label prefixes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([record_to_parquet_row(record) for record in records])
    frame.to_parquet(path, index=False)
