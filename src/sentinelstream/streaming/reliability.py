"""Retry policy shared by Kafka producers and consumers."""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded exponential backoff for transient broker or handler failures."""

    max_attempts: int = 3
    initial_backoff_seconds: float = 0.25
    max_backoff_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if self.initial_backoff_seconds < 0.0 or self.max_backoff_seconds < 0.0:
            raise ValueError("retry backoff values must be non-negative")
        if self.initial_backoff_seconds > self.max_backoff_seconds:
            raise ValueError("initial backoff must not exceed maximum backoff")

    def delay_for(self, failed_attempt: int) -> float:
        """Return the delay after a one-based failed attempt."""

        if failed_attempt < 1:
            raise ValueError("failed_attempt must be positive")
        return min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (2 ** (failed_attempt - 1)),
        )

    def sleep_before_retry(self, failed_attempt: int) -> None:
        delay = self.delay_for(failed_attempt)
        if delay:
            time.sleep(delay)


def is_retriable_error(error: BaseException) -> bool:
    """Identify common broker failures without retrying schema or programming errors."""

    try:
        from confluent_kafka import KafkaError, KafkaException
    except ImportError:
        return isinstance(error, (TimeoutError, ConnectionError, OSError, BufferError))
    if isinstance(error, (KafkaError, KafkaException)):
        kafka_error = error if isinstance(error, KafkaError) else error.args[0]
        return bool(kafka_error.retriable() or kafka_error.code() == KafkaError._TIMED_OUT)
    return isinstance(error, (TimeoutError, ConnectionError, OSError, BufferError))


def safe_error_message(error: BaseException, *, limit: int = 2_000) -> str:
    """Bound exception text before it is sent to a DLQ or structured log."""

    message = str(error).strip() or type(error).__name__
    return message[:limit]


def as_retry_policy(
    max_attempts: int,
    backoff_seconds: float,
    *,
    max_backoff_seconds: float = 5.0,
) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=max_attempts,
        initial_backoff_seconds=backoff_seconds,
        max_backoff_seconds=max(max_backoff_seconds, backoff_seconds),
    )
