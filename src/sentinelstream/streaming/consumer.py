"""Manual-commit Kafka consumption with validation, retries, and DLQ handling."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from sentinelstream.config.settings import KafkaSettings
from sentinelstream.streaming.codec import MessageValidationError, decode_message
from sentinelstream.streaming.contracts import DeadLetterMessage, Message
from sentinelstream.streaming.kafka_config import kafka_client_config
from sentinelstream.streaming.observability import log_event
from sentinelstream.streaming.producer import KafkaMessageProducer
from sentinelstream.streaming.reliability import RetryPolicy, safe_error_message
from sentinelstream.streaming.topics import TopicName, normalise_topic


@dataclass(frozen=True, slots=True)
class ConsumerRecord:
    """Transport-neutral Kafka record used by the real and in-memory consumers."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


class ConsumerTransport(Protocol):
    def poll(self, timeout_ms: int, max_records: int) -> list[ConsumerRecord]: ...

    def commit(self, record: ConsumerRecord) -> None: ...

    def rewind(self, record: ConsumerRecord) -> None: ...

    def close(self) -> None: ...


def _build_kafka_consumer(
    settings: KafkaSettings,
    topic: TopicName,
    *,
    group_id: str,
    replay: bool,
) -> ConsumerTransport:
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:
        raise RuntimeError("Kafka support requires the confluent-kafka dependency") from exc

    consumer = Consumer(
        kafka_client_config(
            settings,
            {
                "client.id": f"{settings.client_id}-consumer",
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest" if replay else settings.auto_offset_reset,
                "enable.partition.eof": False,
            },
        )
    )
    consumer.subscribe([topic.value])
    return _KafkaConsumerTransport(consumer)


class _KafkaConsumerTransport:
    def __init__(self, consumer: object) -> None:
        self._consumer = consumer

    def poll(self, timeout_ms: int, max_records: int) -> list[ConsumerRecord]:
        from confluent_kafka import KafkaException

        records: list[ConsumerRecord] = []
        for _ in range(max_records):
            record = self._consumer.poll(timeout_ms / 1_000)  # type: ignore[attr-defined]
            if record is None:
                break
            if record.error():
                raise KafkaException(record.error())
            records.append(
                ConsumerRecord(
                    topic=record.topic(),
                    partition=record.partition(),
                    offset=record.offset(),
                    key=record.key(),
                    value=record.value() or b"",
                )
            )
        return records

    def commit(self, record: ConsumerRecord) -> None:
        from confluent_kafka import TopicPartition

        self._consumer.commit(  # type: ignore[attr-defined]
            offsets=[TopicPartition(record.topic, record.partition, record.offset + 1)],
            asynchronous=False,
        )

    def rewind(self, record: ConsumerRecord) -> None:
        from confluent_kafka import TopicPartition

        self._consumer.seek(  # type: ignore[attr-defined]
            TopicPartition(record.topic, record.partition, record.offset)
        )

    def close(self) -> None:
        self._consumer.close()  # type: ignore[attr-defined]


@dataclass(frozen=True, slots=True)
class ConsumeResult:
    """Outcome of one record, including whether its offset was committed."""

    record: ConsumerRecord
    status: str
    attempts: int
    committed: bool


class KafkaMessageConsumer:
    """Consume one typed topic with at-least-once processing semantics."""

    def __init__(
        self,
        topic: TopicName | str,
        handler: Callable[[Message], None],
        *,
        settings: KafkaSettings | None = None,
        transport: ConsumerTransport | None = None,
        dead_letter_producer: KafkaMessageProducer,
        retry_policy: RetryPolicy | None = None,
        group_id: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.topic = normalise_topic(topic)
        self.handler = handler
        self.settings = settings or KafkaSettings()
        self._transport = transport
        self.dead_letter_producer = dead_letter_producer
        self.retry_policy = retry_policy or RetryPolicy(
            max_attempts=self.settings.retry_attempts,
            initial_backoff_seconds=self.settings.retry_backoff_seconds,
        )
        self.group_id = group_id or self.settings.consumer_group
        self.logger = logger or logging.getLogger(__name__)

    def process_record(self, record: ConsumerRecord) -> ConsumeResult:
        """Process one record and commit only after success or durable DLQ publication."""

        if record.topic != self.topic.value:
            raise ValueError(f"record belongs to {record.topic}, not {self.topic.value}")
        try:
            message = decode_message(record.topic, record.value)
        except MessageValidationError as exc:
            self._dead_letter_and_commit(record, exc, attempts=1)
            return ConsumeResult(record, "dead_letter", 1, True)

        attempts = 0
        while attempts < self.retry_policy.max_attempts:
            attempts += 1
            try:
                self.handler(message)
            except Exception as exc:
                if attempts >= self.retry_policy.max_attempts:
                    self._dead_letter_and_commit(record, exc, attempts=attempts)
                    return ConsumeResult(record, "dead_letter", attempts, True)
                log_event(
                    self.logger,
                    logging.WARNING,
                    "kafka_handler_retry",
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    attempt=attempts,
                    error_type=type(exc).__name__,
                )
                self.retry_policy.sleep_before_retry(attempts)
                continue
            self._commit(record)
            log_event(
                self.logger,
                logging.INFO,
                "kafka_message_processed",
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                message_id=str(message.message_id),
                attempts=attempts,
            )
            return ConsumeResult(record, "processed", attempts, True)
        raise RuntimeError("unreachable consumer retry state")

    def poll_once(self) -> list[ConsumeResult]:
        transport = self._ensure_transport(replay=False)
        return [
            self.process_record(record)
            for record in transport.poll(
                timeout_ms=self.settings.request_timeout_ms,
                max_records=self.settings.max_poll_records,
            )
        ]

    def run(
        self,
        *,
        max_records: int | None = None,
        max_idle_polls: int | None = None,
        replay: bool = False,
    ) -> list[ConsumeResult]:
        """Run until a record limit or idle limit; replay uses a fresh earliest group."""

        transport = self._ensure_transport(replay=replay)
        results: list[ConsumeResult] = []
        idle_polls = 0
        while max_records is None or len(results) < max_records:
            records = transport.poll(
                timeout_ms=self.settings.request_timeout_ms,
                max_records=min(
                    self.settings.max_poll_records,
                    max_records - len(results)
                    if max_records is not None
                    else self.settings.max_poll_records,
                ),
            )
            if not records:
                idle_polls += 1
                if max_idle_polls is not None and idle_polls >= max_idle_polls:
                    break
                continue
            idle_polls = 0
            for record in records:
                results.append(self.process_record(record))
                if max_records is not None and len(results) >= max_records:
                    break
        return results

    def replay(
        self,
        *,
        max_records: int | None = None,
        max_idle_polls: int = 1,
        transport: ConsumerTransport | None = None,
    ) -> list[ConsumeResult]:
        """Read retained records from the beginning using an isolated consumer group."""

        if transport is None:
            self._transport = None
        else:
            self._transport = transport
        self.group_id = f"{self.group_id}.replay.{uuid4().hex}"
        return self.run(
            max_records=max_records,
            max_idle_polls=max_idle_polls,
            replay=True,
        )

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()

    def _ensure_transport(self, *, replay: bool) -> ConsumerTransport:
        if self._transport is None:
            self._transport = _build_kafka_consumer(
                self.settings,
                self.topic,
                group_id=self.group_id,
                replay=replay,
            )
        return self._transport

    def _commit(self, record: ConsumerRecord) -> None:
        self._ensure_transport(replay=False).commit(record)

    def _rewind(self, record: ConsumerRecord) -> None:
        self._ensure_transport(replay=False).rewind(record)

    def _dead_letter_and_commit(
        self,
        record: ConsumerRecord,
        error: BaseException,
        *,
        attempts: int,
    ) -> None:
        try:
            self._dead_letter(record, error, attempts=attempts)
            self._commit(record)
        except Exception:
            self._rewind(record)
            raise

    def _dead_letter(self, record: ConsumerRecord, error: BaseException, *, attempts: int) -> None:
        raw_payload = record.value.decode("utf-8", errors="replace")[:1_000_000]
        dead_letter = DeadLetterMessage(
            original_topic=record.topic,
            original_partition=record.partition,
            original_offset=record.offset,
            original_key=record.key.decode("utf-8", errors="replace") if record.key else None,
            raw_payload=raw_payload,
            error_type=type(error).__name__,
            error_message=safe_error_message(error),
            attempts=attempts,
        )
        self.dead_letter_producer.publish_dead_letter(dead_letter)
        log_event(
            self.logger,
            logging.ERROR,
            "kafka_message_dead_lettered",
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            attempts=attempts,
            error_type=dead_letter.error_type,
        )
