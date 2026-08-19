"""Event-time transformations shared by batch and streaming feature paths."""

from __future__ import annotations

from datetime import datetime


def temporal_features(timestamp: datetime) -> dict[str, int]:
    """Return calendar features derived only from an event timestamp."""

    hour = timestamp.hour
    day_of_week = timestamp.weekday()
    return {
        "event_hour": hour,
        "event_day_of_week": day_of_week,
        "is_weekend": int(day_of_week in {5, 6}),
        "is_night": int(hour in {0, 1, 2, 3, 4, 5}),
    }
