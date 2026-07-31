"""Temporal train, validation, and test splitting."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class TemporalSplit:
    """Chronologically ordered partitions with no row overlap."""

    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame


def temporal_split(
    frame: pd.DataFrame,
    *,
    timestamp_column: str = "timestamp",
    train_fraction: float = 0.70,
    validation_fraction: float = 0.15,
) -> TemporalSplit:
    """Split by event time without shuffling or using future rows for training."""

    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between zero and one")
    if not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be between zero and one")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train and validation fractions must leave test data")
    if timestamp_column not in frame:
        raise KeyError(f"missing timestamp column: {timestamp_column}")
    if len(frame) < 3:
        raise ValueError("at least three rows are required for a temporal split")

    ordered = frame.copy()
    ordered[timestamp_column] = pd.to_datetime(ordered[timestamp_column], utc=True, errors="coerce")
    if ordered[timestamp_column].isna().any():
        raise ValueError("timestamp column contains invalid or missing values")
    ordered = ordered.sort_values(timestamp_column, kind="stable").reset_index(drop=True)
    train_end = max(1, int(len(ordered) * train_fraction))
    validation_end = max(train_end + 1, int(len(ordered) * (train_fraction + validation_fraction)))
    validation_end = min(validation_end, len(ordered) - 1)
    return TemporalSplit(
        train=ordered.iloc[:train_end].copy(),
        validation=ordered.iloc[train_end:validation_end].copy(),
        test=ordered.iloc[validation_end:].copy(),
    )
