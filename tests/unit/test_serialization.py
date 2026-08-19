from pathlib import Path

import pandas as pd

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.schemas import SimulatedTransaction
from sentinelstream.data.serialization import write_jsonl, write_parquet
from sentinelstream.simulation.generator import TransactionGenerator


def test_jsonl_and_parquet_outputs(tmp_path: Path) -> None:
    records = TransactionGenerator(SimulationSettings(random_seed=4)).generate(12)
    jsonl_path = tmp_path / "events.jsonl"
    parquet_path = tmp_path / "events.parquet"

    write_jsonl(records, jsonl_path)
    write_parquet(records, parquet_path)

    assert jsonl_path.read_text(encoding="utf-8").count("\n") == 12
    frame = pd.read_parquet(parquet_path)
    assert len(frame) == 12
    assert "simulation_fraud_label" in frame.columns
    assert all(isinstance(record, SimulatedTransaction) for record in records)
