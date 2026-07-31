"""Validation, deduplication, and quarantine for transaction datasets."""

from __future__ import annotations

import copy
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pydantic import ValidationError

from sentinelstream.data.schemas import (
    FraudScenario,
    SimulatedTransaction,
    SimulationGroundTruth,
    TransactionEvent,
)


@dataclass(frozen=True, slots=True)
class CanonicalTransaction:
    """A validated event with optional simulation metadata for offline training."""

    event: TransactionEvent
    fraud_label: bool | None = None
    fraud_type: FraudScenario | None = None

    def to_row(self) -> dict[str, Any]:
        row = self.event.model_dump(mode="json")
        row["fraud_label"] = self.fraud_label
        row["fraud_type"] = self.fraud_type.value if self.fraud_type else None
        return row


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """A quarantined record and the reason it was rejected."""

    record_index: int
    code: str
    message: str
    raw_record: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_index": self.record_index,
            "code": self.code,
            "message": self.message,
            "raw_record": self.raw_record,
        }


@dataclass(slots=True)
class ValidationResult:
    """Validated records and quality outcomes for one ingestion operation."""

    records: list[CanonicalTransaction]
    quarantined: list[ValidationIssue]
    duplicate_count: int
    source: str

    def to_rows(self) -> list[dict[str, Any]]:
        return [record.to_row() for record in self.records]

    def to_frame(self) -> pd.DataFrame:
        """Return validated records as a training-ready tabular frame."""

        return pd.DataFrame(self.to_rows())

    def quality_report(self) -> dict[str, Any]:
        issue_counts: dict[str, int] = {}
        for issue in self.quarantined:
            issue_counts[issue.code] = issue_counts.get(issue.code, 0) + 1
        return {
            "source": self.source,
            "input_records": len(self.records) + len(self.quarantined),
            "valid_records": len(self.records),
            "quarantined_records": len(self.quarantined),
            "duplicate_records": self.duplicate_count,
            "issue_counts": issue_counts,
        }


def _raw_event_payload(raw_record: Mapping[str, Any]) -> Mapping[str, Any]:
    nested_event = raw_record.get("event")
    if isinstance(nested_event, Mapping):
        return nested_event
    simulation_fields = {
        "record_kind",
        "simulation_fraud_label",
        "simulation_fraud_type",
        "raw_record",
        "malformed_reason",
    }
    return {key: value for key, value in raw_record.items() if key not in simulation_fields}


def _ground_truth(raw_record: Mapping[str, Any]) -> SimulationGroundTruth | None:
    nested_truth = raw_record.get("ground_truth")
    if isinstance(nested_truth, Mapping):
        return SimulationGroundTruth.model_validate(nested_truth)
    if "simulation_fraud_label" in raw_record:
        return SimulationGroundTruth(
            fraud_label=bool(raw_record["simulation_fraud_label"]),
            fraud_type=raw_record.get("simulation_fraud_type"),
        )
    return None


def validate_records(
    records: list[Any],
    *,
    source: str = "unknown",
    require_labels: bool = False,
    allowed_categories: Collection[str] | None = None,
) -> ValidationResult:
    """Validate records and quarantine schema, category, label, and duplicate failures."""

    valid: list[CanonicalTransaction] = []
    quarantined: list[ValidationIssue] = []
    seen_event_ids: set[str] = set()
    seen_transaction_ids: set[str] = set()
    allowed = set(allowed_categories or ())

    for record_index, raw_record in enumerate(records):
        if isinstance(raw_record, SimulatedTransaction):
            raw_record = raw_record.to_json_record()
        if not isinstance(raw_record, Mapping):
            quarantined.append(
                ValidationIssue(
                    record_index,
                    "invalid_record",
                    "record must be a mapping",
                    raw_record,
                )
            )
            continue
        try:
            event = TransactionEvent.model_validate(_raw_event_payload(raw_record))
            truth = _ground_truth(raw_record)
        except (ValidationError, ValueError, TypeError) as exc:
            quarantined.append(
                ValidationIssue(
                    record_index,
                    "schema_error",
                    str(exc),
                    copy.deepcopy(dict(raw_record)),
                )
            )
            continue
        if require_labels and truth is None:
            quarantined.append(
                ValidationIssue(
                    record_index,
                    "missing_label",
                    "fraud label is required",
                    copy.deepcopy(dict(raw_record)),
                )
            )
            continue
        if allowed and event.merchant_category not in allowed:
            quarantined.append(
                ValidationIssue(
                    record_index,
                    "invalid_category",
                    f"unsupported merchant category: {event.merchant_category}",
                    copy.deepcopy(dict(raw_record)),
                )
            )
            continue
        event_id = str(event.event_id)
        transaction_id = str(event.transaction_id)
        if event_id in seen_event_ids or transaction_id in seen_transaction_ids:
            quarantined.append(
                ValidationIssue(
                    record_index,
                    "duplicate_event",
                    "event_id or transaction_id was already ingested",
                    copy.deepcopy(dict(raw_record)),
                )
            )
            continue
        seen_event_ids.add(event_id)
        seen_transaction_ids.add(transaction_id)
        valid.append(
            CanonicalTransaction(
                event=event,
                fraud_label=truth.fraud_label if truth else None,
                fraud_type=truth.fraud_type if truth else None,
            )
        )

    duplicate_count = sum(issue.code == "duplicate_event" for issue in quarantined)
    return ValidationResult(valid, quarantined, duplicate_count, source)
