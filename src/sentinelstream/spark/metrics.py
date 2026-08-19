"""Thread-safe metrics for Spark Structured Streaming micro-batches."""

from __future__ import annotations

import logging
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from time import monotonic, time
from typing import TYPE_CHECKING

from sentinelstream.streaming.observability import log_event

if TYPE_CHECKING:
    from sentinelstream.monitoring.metrics import PrometheusMetrics


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    """Point-in-time stream counters suitable for monitoring or audit output."""

    processed_events: int
    dropped_events: int
    duplicate_events: int
    malformed_events: int
    late_events: int
    batches: int
    total_batch_duration_ms: float
    max_batch_duration_ms: float
    total_processing_latency_ms: float
    max_processing_latency_ms: float
    last_batch_id: int | None
    last_updated_at: str | None

    def to_dict(self) -> dict[str, int | float | str | None]:
        return asdict(self)


class SparkStreamingMetrics:
    """Collect explicit application metrics without coupling to Spark listeners."""

    def __init__(
        self,
        logger: logging.Logger | None = None,
        prometheus: PrometheusMetrics | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._logger = logger or logging.getLogger(__name__)
        self._prometheus = prometheus
        self._snapshot = MetricsSnapshot(
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0.0,
            0.0,
            0.0,
            None,
            None,
        )

    def record_batch(
        self,
        *,
        batch_id: int,
        processed_events: int,
        dropped_events: int = 0,
        duplicate_events: int = 0,
        malformed_events: int = 0,
        late_events: int = 0,
        duration_ms: float,
        processing_latency_ms: float = 0.0,
    ) -> MetricsSnapshot:
        """Record one completed batch and emit a structured summary."""

        if (
            min(
                processed_events,
                dropped_events,
                duplicate_events,
                malformed_events,
                late_events,
                duration_ms,
                processing_latency_ms,
            )
            < 0
        ):
            raise ValueError("stream metrics cannot be negative")
        with self._lock:
            current = self._snapshot
            now = datetime.now(UTC).isoformat()
            self._snapshot = MetricsSnapshot(
                processed_events=current.processed_events + processed_events,
                dropped_events=current.dropped_events + dropped_events,
                duplicate_events=current.duplicate_events + duplicate_events,
                malformed_events=current.malformed_events + malformed_events,
                late_events=current.late_events + late_events,
                batches=current.batches + 1,
                total_batch_duration_ms=current.total_batch_duration_ms + duration_ms,
                max_batch_duration_ms=max(current.max_batch_duration_ms, duration_ms),
                total_processing_latency_ms=(
                    current.total_processing_latency_ms + processing_latency_ms
                ),
                max_processing_latency_ms=max(
                    current.max_processing_latency_ms,
                    processing_latency_ms,
                ),
                last_batch_id=batch_id,
                last_updated_at=now,
            )
            snapshot = self._snapshot
        log_event(
            self._logger,
            logging.INFO,
            "spark_stream_batch_completed",
            batch_id=batch_id,
            processed_events=processed_events,
            dropped_events=dropped_events,
            duplicate_events=duplicate_events,
            malformed_events=malformed_events,
            late_events=late_events,
            duration_ms=duration_ms,
            processing_latency_ms=processing_latency_ms,
        )
        if self._prometheus is not None:
            self._prometheus.record_streaming_event("spark", "processed", processed_events)
            self._prometheus.record_streaming_event("spark", "dropped", dropped_events)
            self._prometheus.record_streaming_event("spark", "duplicate", duplicate_events)
            self._prometheus.record_streaming_event("spark", "malformed", malformed_events)
            self._prometheus.record_streaming_event("spark", "late", late_events)
            self._prometheus.record_streaming_batch("spark", duration_ms / 1_000)
            self._prometheus.last_batch_timestamp.labels("spark").set(time())
        return snapshot

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return self._snapshot

    @property
    def logger(self) -> logging.Logger:
        return self._logger


def timed_batch() -> float:
    """Return a monotonic start point for a micro-batch timer."""

    return monotonic()
