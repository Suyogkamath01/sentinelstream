"""Deterministic Kafka-like transport used by integration tests and local demos."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from sentinelstream.streaming.consumer import ConsumerRecord, ConsumerTransport
from sentinelstream.streaming.producer import ProducerFuture, ProducerTransport
from sentinelstream.streaming.topics import (
    TopicDefinition,
    TopicName,
    normalise_topic,
    partition_for,
    topic_definitions,
)


@dataclass(frozen=True, slots=True)
class MemoryMetadata:
    partition: int
    offset: int


class _MemoryFuture:
    def __init__(self, metadata: MemoryMetadata) -> None:
        self._metadata = metadata

    def get(self, timeout: float | None = None) -> MemoryMetadata:
        return self._metadata


class InMemoryKafkaBroker:
    """A partitioned append-only broker with explicit consumer-group offsets."""

    def __init__(self, definitions: tuple[TopicDefinition, ...] | None = None) -> None:
        self.definitions = definitions or topic_definitions()
        self._records: dict[str, list[ConsumerRecord]] = defaultdict(list)
        self._committed: dict[str, dict[tuple[str, int], int]] = defaultdict(dict)

    def ensure_topics(self) -> tuple[TopicName, ...]:
        return tuple(definition.name for definition in self.definitions)

    def producer(self) -> ProducerTransport:
        return _MemoryProducer(self)

    def consumer(
        self,
        topic: TopicName | str,
        *,
        group_id: str,
        auto_offset_reset: str = "latest",
    ) -> ConsumerTransport:
        if auto_offset_reset not in {"earliest", "latest"}:
            raise ValueError("auto_offset_reset must be earliest or latest")
        return _MemoryConsumer(self, normalise_topic(topic).value, group_id, auto_offset_reset)

    def append(self, topic: str, key: bytes, value: bytes) -> MemoryMetadata:
        definition = self._definition(topic)
        partition = partition_for(key.decode("utf-8"), definition.partitions)
        existing = [
            record.offset
            for record in self._records[definition.name.value]
            if record.partition == partition
        ]
        offset = max(existing, default=-1) + 1
        self._records[definition.name.value].append(
            ConsumerRecord(
                topic=definition.name.value,
                partition=partition,
                offset=offset,
                key=key,
                value=value,
            )
        )
        return MemoryMetadata(partition=partition, offset=offset)

    def records(self, topic: TopicName | str) -> tuple[ConsumerRecord, ...]:
        topic_name = normalise_topic(topic).value
        return tuple(
            sorted(
                self._records[topic_name],
                key=lambda record: (record.partition, record.offset),
            )
        )

    def _definition(self, topic: TopicName | str) -> TopicDefinition:
        topic_name = normalise_topic(topic)
        for definition in self.definitions:
            if definition.name is topic_name:
                return definition
        raise ValueError(f"no definition found for topic: {topic_name.value}")


class _MemoryProducer(ProducerTransport):
    def __init__(self, broker: InMemoryKafkaBroker) -> None:
        self._broker = broker

    def send(self, topic: str, *, key: bytes, value: bytes) -> ProducerFuture:
        return _MemoryFuture(self._broker.append(topic, key, value))

    def flush(self, timeout: float | None = None) -> None:
        return None

    def close(self) -> None:
        return None


class _MemoryConsumer(ConsumerTransport):
    def __init__(
        self,
        broker: InMemoryKafkaBroker,
        topic: str,
        group_id: str,
        auto_offset_reset: str,
    ) -> None:
        self._broker = broker
        self._topic = topic
        self._group_id = group_id
        self._positions: dict[tuple[str, int], int] = {}
        definition = broker._definition(topic)
        for partition in range(definition.partitions):
            committed = broker._committed[group_id].get((topic, partition))
            if committed is not None:
                position = committed
            elif auto_offset_reset == "earliest":
                position = 0
            else:
                records = [
                    record.offset
                    for record in broker.records(topic)
                    if record.partition == partition
                ]
                position = max(records, default=-1) + 1
            self._positions[(topic, partition)] = position

    def poll(self, timeout_ms: int, max_records: int) -> list[ConsumerRecord]:
        candidates = [
            record
            for record in self._broker.records(self._topic)
            if record.offset >= self._positions[(record.topic, record.partition)]
        ]
        candidates.sort(key=lambda record: (record.partition, record.offset))
        selected = candidates[:max_records]
        for record in selected:
            self._positions[(record.topic, record.partition)] = record.offset + 1
        return selected

    def commit(self, record: ConsumerRecord) -> None:
        position = record.offset + 1
        key = (record.topic, record.partition)
        self._broker._committed[self._group_id][key] = max(
            self._broker._committed[self._group_id].get(key, 0), position
        )
        self._positions[key] = max(self._positions.get(key, 0), position)

    def rewind(self, record: ConsumerRecord) -> None:
        key = (record.topic, record.partition)
        self._positions[key] = min(self._positions.get(key, record.offset), record.offset)

    def close(self) -> None:
        return None
