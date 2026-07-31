"""Dataset readers and durable quarantine/report writers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from sentinelstream.data.validation import ValidationResult, validate_records


def _json_safe_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    cleaned = frame.astype(object).where(pd.notna(frame), None)
    return cleaned.to_dict(orient="records")


def read_jsonl(path: Path) -> list[Any]:
    """Read JSON Lines while retaining malformed lines for quarantine."""

    records: list[Any] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                records.append(
                    {
                        "_parse_error": str(exc),
                        "_line_number": line_number,
                        "_raw_line": line.rstrip("\n"),
                    }
                )
    return records


def read_parquet(path: Path) -> list[dict[str, Any]]:
    """Read a Parquet file into Python records without silently dropping nulls."""

    return _json_safe_records(pd.read_parquet(path))


def ingest_path(
    path: Path,
    *,
    require_labels: bool = False,
    allowed_categories: set[str] | None = None,
) -> ValidationResult:
    """Read and validate a JSONL or Parquet dataset."""

    if not path.is_file():
        raise FileNotFoundError(f"dataset does not exist: {path}")
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        records = read_jsonl(path)
    elif path.suffix.lower() == ".parquet":
        records = read_parquet(path)
    else:
        raise ValueError("supported dataset formats are .jsonl, .ndjson, and .parquet")
    return validate_records(
        records,
        source=str(path),
        require_labels=require_labels,
        allowed_categories=allowed_categories,
    )


def write_quarantine(result: ValidationResult, path: Path) -> None:
    """Write rejected records with reason codes for later inspection."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for issue in result.quarantined:
            handle.write(json.dumps(issue.to_dict(), default=str, sort_keys=True) + "\n")


def write_quality_report(result: ValidationResult, path: Path) -> None:
    """Persist a compact data-quality summary."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.quality_report(), indent=2, sort_keys=True), encoding="utf-8")


def sha256_file(path: Path) -> str:
    """Return a content hash for dataset version tracking."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
