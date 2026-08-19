from pathlib import Path

from sentinelstream.config.settings import SimulationSettings
from sentinelstream.data.ingestion import ingest_path, read_jsonl
from sentinelstream.data.serialization import write_jsonl, write_parquet
from sentinelstream.data.validation import validate_records
from sentinelstream.simulation.generator import TransactionGenerator


def generated_records(count: int = 30) -> list[object]:
    return TransactionGenerator(
        SimulationSettings(customer_count=8, merchant_count=5, random_seed=31, fraud_ratio=0.2)
    ).generate(count)


def test_validation_quarantines_duplicates_and_malformed_records() -> None:
    records = TransactionGenerator(
        SimulationSettings(
            customer_count=8,
            merchant_count=5,
            random_seed=31,
            fraud_ratio=0.2,
            duplicate_rate=1.0,
            malformed_rate=1.0,
        )
    ).generate(30)

    result = validate_records(records, require_labels=True)

    assert len(result.records) == 30
    assert result.duplicate_count == 30
    assert result.quality_report()["issue_counts"]["schema_error"] == 30


def test_jsonl_parser_retains_invalid_lines_for_quarantine(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    valid = generated_records(1)[0]
    write_jsonl([valid], path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("{not-json}\n")

    records = read_jsonl(path)
    result = validate_records(records, require_labels=True)

    assert len(result.records) == 1
    assert result.quarantined[0].code == "schema_error"


def test_parquet_ingestion_preserves_simulation_labels(tmp_path: Path) -> None:
    path = tmp_path / "events.parquet"
    write_parquet(generated_records(), path)

    result = ingest_path(path, require_labels=True)

    assert len(result.records) == 30
    assert {record.fraud_label for record in result.records} == {False, True}


def test_validation_rejects_unknown_categories() -> None:
    record = generated_records(1)[0]
    raw = record.to_json_record()
    raw["event"]["merchant_category"] = "not-a-category"

    result = validate_records([raw], allowed_categories={"grocery"})

    assert result.quarantined[0].code == "invalid_category"
