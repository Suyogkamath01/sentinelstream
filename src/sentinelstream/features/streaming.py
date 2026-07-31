"""In-memory event-time feature state for streaming parity tests and demos."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import UUID

from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.features.behavioural import calculate_behavioural_features


@dataclass(frozen=True, slots=True)
class StreamingFeatureConfig:
    """State-retention and event-time policy for the feature calculator."""

    allowed_lateness: timedelta = timedelta(minutes=15)
    history_retention: timedelta = timedelta(days=30)


@dataclass(frozen=True, slots=True)
class StreamingFeatureResult:
    """Outcome of processing one event."""

    event_id: UUID
    accepted: bool
    reason: str | None
    watermark: datetime | None
    features: dict[str, float | int]


@dataclass(slots=True)
class StreamingFeatureCalculator:
    """Maintain bounded customer history with deduplication and watermarks."""

    config: StreamingFeatureConfig = field(default_factory=StreamingFeatureConfig)
    _history: dict[str, list[TransactionEvent]] = field(default_factory=dict, init=False)
    _seen_event_ids: set[UUID] = field(default_factory=set, init=False)
    _seen_transaction_ids: set[UUID] = field(default_factory=set, init=False)
    _max_event_time: datetime | None = field(default=None, init=False)
    _late_events: list[TransactionEvent] = field(default_factory=list, init=False)

    @property
    def watermark(self) -> datetime | None:
        """Return the current event-time watermark."""

        if self._max_event_time is None:
            return None
        return self._max_event_time - self.config.allowed_lateness

    @property
    def late_events(self) -> tuple[TransactionEvent, ...]:
        return tuple(self._late_events)

    def process(self, event: TransactionEvent) -> StreamingFeatureResult:
        """Score one event using state strictly earlier than its event time."""

        if (
            event.event_id in self._seen_event_ids
            or event.transaction_id in self._seen_transaction_ids
        ):
            return StreamingFeatureResult(
                event.event_id,
                False,
                "duplicate_event",
                self.watermark,
                {},
            )
        if self._max_event_time is None or event.timestamp > self._max_event_time:
            self._max_event_time = event.timestamp
        current_watermark = self.watermark
        if current_watermark is not None and event.timestamp < current_watermark:
            self._late_events.append(event)
            return StreamingFeatureResult(
                event.event_id,
                False,
                "event_beyond_watermark",
                current_watermark,
                {},
            )
        history = self._history.setdefault(event.customer_id, [])
        features = calculate_behavioural_features(event, history)
        history.append(event)
        history[:] = [
            candidate
            for candidate in history
            if candidate.timestamp >= event.timestamp - self.config.history_retention
        ]
        self._seen_event_ids.add(event.event_id)
        self._seen_transaction_ids.add(event.transaction_id)
        return StreamingFeatureResult(event.event_id, True, None, current_watermark, features)
