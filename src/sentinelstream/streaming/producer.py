"""Typed, idempotent Kafka publishing for SentinelStream messages."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from threading import Event
from time import monotonic
from typing import Any, Protocol

from sentinelstream.config.settings import KafkaSettings
from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.streaming.codec import encode_message, topic_for_message
from sentinelstream.streaming.contracts import (
    DeadLetterMessage,
    FeedbackMessage,
    Message,
    TransactionMessage,
)
from sentinelstream.streaming.kafka_config import kafka_client_config
from sentinelstream.streaming.observability import log_event
from sentinelstream.streaming.reliability import RetryPolicy, is_retriable_error, safe_error_message
from sentinelstream.streaming.topics import TopicName, message_key, normalise_topic


class ProducerFuture(Protocol):
    def get(self, timeout: float | None = None) -> Any: ...


class ProducerTransport(Protocol):
    def send(self, topic: str, *, key: bytes, value: bytes) -> ProducerFuture: ...

    def flush(self, timeout: float | None = None) -> Any: ...

    def close(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Broker metadata returned after a message is durably acknowledged."""

    message_id: str
    topic: str
    partition: int | None
    offset: int | None
    attempts: int


def _build_kafka_producer(settings: KafkaSettings, retry_policy: RetryPolicy) -> ProducerTransport:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError("Kafka support requires the confluent-kafka dependency") from exc

    return _ConfluentProducerTransport(
        Producer(
            kafka_client_config(
                settings,
                {
                    "client.id": settings.client_id,
                    "acks": "all",
                    "enable.idempotence": True,
                    "retries": max(0, retry_policy.max_attempts - 1),
                    "max.in.flight.requests.per.connection": 5,
                    "message.timeout.ms": settings.request_timeout_ms,
                },
            )
        )
    )


class _ConfluentFuture:
    def __init__(self, producer: object) -> None:
        self._producer = producer
        self._complete = Event()
        self._error: object | None = None
        self._message: object | None = None

    def callback(self, error: object | None, message: object) -> None:
        self._error = error
        self._message = message
        self._complete.set()

    def get(self, timeout: float | None = None) -> object:
        deadline = None if timeout is None else monotonic() + timeout
        while not self._complete.is_set():
            remaining = None if deadline is None else max(0.0, deadline - monotonic())
            if remaining == 0.0:
                raise TimeoutError("timed out waiting for Kafka delivery")
            self._producer.poll(min(0.1, remaining) if remaining is not None else 0.1)  # type: ignore[attr-defined]
            self._complete.wait(min(0.01, remaining) if remaining is not None else 0.01)
        if self._error is not None:
            if isinstance(self._error, BaseException):
                raise self._error
            from confluent_kafka import KafkaException

            raise KafkaException(self._error)
        return self._message


class _ConfluentProducerTransport:
    def __init__(self, producer: object) -> None:
        self._producer = producer

    def send(self, topic: str, *, key: bytes, value: bytes) -> ProducerFuture:
        future = _ConfluentFuture(self._producer)
        self._producer.produce(  # type: ignore[attr-defined]
            topic,
            key=key,
            value=value,
            on_delivery=future.callback,
        )
        self._producer.poll(0)  # type: ignore[attr-defined]
        return future

    def flush(self, timeout: float | None = None) -> int:
        if timeout is None:
            return self._producer.flush()  # type: ignore[attr-defined]
        return self._producer.flush(timeout)  # type: ignore[attr-defined]

    def close(self) -> int:
        return self._producer.flush()  # type: ignore[attr-defined]


class KafkaMessageProducer:
    """Publish validated messages with broker and application-level retries."""

    def __init__(
        self,
        settings: KafkaSettings | None = None,
        *,
        transport: ProducerTransport | None = None,
        retry_policy: RetryPolicy | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.settings = settings or KafkaSettings()
        self.retry_policy = retry_policy or RetryPolicy(
            max_attempts=self.settings.retry_attempts,
            initial_backoff_seconds=self.settings.retry_backoff_seconds,
        )
        self.logger = logger or logging.getLogger(__name__)
        self._producer = transport or _build_kafka_producer(self.settings, self.retry_policy)

    def publish(
        self,
        message: Message,
        *,
        topic: TopicName | str | None = None,
        key: str | None = None,
    ) -> PublishResult:
        """Validate, encode, and publish a typed message."""

        selected_topic = topic_for_message(message) if topic is None else normalise_topic(topic)
        payload = encode_message(message, topic=selected_topic)
        selected_key = key or message_key(selected_topic, message)
        return self._publish_bytes(
            selected_topic,
            selected_key,
            payload,
            message_id=str(message.message_id),
        )

    def publish_raw(
        self,
        topic: TopicName | str,
        payload: bytes | str,
        *,
        key: str,
        message_id: str = "raw",
    ) -> PublishResult:
        """Publish an unvalidated ingress payload for quarantine or replay tests."""

        if not key:
            raise ValueError("raw messages require a non-empty partition key")
        raw_payload = payload.encode("utf-8") if isinstance(payload, str) else payload
        return self._publish_bytes(normalise_topic(topic), key, raw_payload, message_id=message_id)

    def publish_transaction(
        self,
        event: TransactionEvent,
        *,
        key: str | None = None,
    ) -> PublishResult:
        """Build and publish a serving transaction without exposing simulation labels."""

        message = TransactionMessage(event=event)
        return self.publish(message, key=key)

    def publish_feedback(self, feedback: FeedbackMessage) -> PublishResult:
        return self.publish(feedback, topic=TopicName.FEEDBACK)

    def publish_dead_letter(self, dead_letter: DeadLetterMessage) -> PublishResult:
        return self.publish(dead_letter, topic=TopicName.DEAD_LETTER)

    def flush(self, timeout: float | None = None) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self._producer.close()

    def _publish_bytes(
        self,
        topic: TopicName,
        key: str,
        payload: bytes,
        *,
        message_id: str,
    ) -> PublishResult:
        attempts = 0
        while attempts < self.retry_policy.max_attempts:
            attempts += 1
            try:
                future = self._producer.send(topic.value, key=key.encode("utf-8"), value=payload)
                metadata = future.get(timeout=self.settings.request_timeout_ms / 1_000)
                partition = getattr(metadata, "partition", None)
                offset = getattr(metadata, "offset", None)
                partition = partition() if callable(partition) else partition
                offset = offset() if callable(offset) else offset
                result = PublishResult(
                    message_id=message_id,
                    topic=topic.value,
                    partition=partition,
                    offset=offset,
                    attempts=attempts,
                )
                log_event(
                    self.logger,
                    logging.INFO,
                    "kafka_message_published",
                    message_id=message_id,
                    topic=topic.value,
                    partition=result.partition,
                    offset=result.offset,
                    attempts=attempts,
                )
                return result
            except Exception as exc:
                if attempts >= self.retry_policy.max_attempts or not is_retriable_error(exc):
                    log_event(
                        self.logger,
                        logging.ERROR,
                        "kafka_publish_failed",
                        message_id=message_id,
                        topic=topic.value,
                        attempts=attempts,
                        error_type=type(exc).__name__,
                        error=safe_error_message(exc),
                    )
                    raise
                log_event(
                    self.logger,
                    logging.WARNING,
                    "kafka_publish_retry",
                    message_id=message_id,
                    topic=topic.value,
                    attempt=attempts,
                    error_type=type(exc).__name__,
                )
                self.retry_policy.sleep_before_retry(attempts)
        raise RuntimeError("unreachable producer retry state")
