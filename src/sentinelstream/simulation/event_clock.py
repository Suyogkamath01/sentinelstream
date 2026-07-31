"""Event-time utilities used by the transaction generator."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from random import Random


class EventClock:
    """Generate deterministic event and ingestion timestamps."""

    def __init__(self, start_time: datetime, duration_days: int, rng: Random) -> None:
        if start_time.tzinfo is None or start_time.utcoffset() is None:
            raise ValueError("start_time must be timezone-aware")
        self.start_time = start_time.astimezone(UTC)
        self.duration_seconds = duration_days * 24 * 60 * 60
        self.rng = rng

    def event_time(self, index: int, *, burst: bool = False) -> datetime:
        """Return a monotonically advancing baseline time."""

        average_step = max(1, self.duration_seconds // 1_000)
        step = (
            self.rng.randint(1, max(2, average_step // 2))
            if burst
            else self.rng.randint(1, average_step * 2)
        )
        offset = min(self.duration_seconds, index * average_step + step)
        return self.start_time + timedelta(seconds=offset)

    def ingestion_time(self, event_time: datetime, index: int) -> datetime:
        """Return an ingestion timestamp after event time."""

        transport_delay = self.rng.randint(1, 120)
        index_delay = timedelta(seconds=index % 11)
        return max(event_time, self.start_time) + timedelta(seconds=transport_delay) + index_delay
