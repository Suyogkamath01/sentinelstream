"""Offline feature generation using the streaming feature definitions."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd

from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.features.behavioural import calculate_behavioural_features

REQUIRED_COLUMNS = {
    "customer_id",
    "merchant_id",
    "device_id",
    "country",
    "transaction_amount",
    "timestamp",
}


def _event_from_row(row: pd.Series) -> TransactionEvent:
    payload = row.to_dict()
    payload.pop("fraud_label", None)
    payload.pop("fraud_type", None)
    return TransactionEvent.model_validate(payload)


def build_batch_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Create offline features through the same function used by streaming state."""

    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise KeyError(f"missing feature columns: {sorted(missing)}")
    working = frame.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True, errors="raise")
    sort_columns = ["timestamp"]
    if "event_id" in working.columns:
        sort_columns.append("event_id")
    working = working.sort_values(sort_columns, kind="stable").reset_index(drop=True)
    histories: defaultdict[str, list[TransactionEvent]] = defaultdict(list)
    rows: list[dict[str, Any]] = []
    for _, row in working.iterrows():
        event = _event_from_row(row)
        output_row = row.to_dict()
        output_row["timestamp"] = event.timestamp
        output_row.update(calculate_behavioural_features(event, histories[event.customer_id]))
        histories[event.customer_id].append(event)
        rows.append(output_row)
    return pd.DataFrame(rows)
