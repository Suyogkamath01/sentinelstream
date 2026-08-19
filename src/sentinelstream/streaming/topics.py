"""Kafka topic contracts and stable partitioning rules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from zlib import crc32

from sentinelstream.config.settings import KafkaSettings
from sentinelstream.streaming.kafka_config import kafka_client_config


class TopicName(StrEnum):
    """Versioned topics used by the SentinelStream event flow."""

    TRANSACTIONS = "sentinelstream.transactions.v1"
    VALIDATED_TRANSACTIONS = "sentinelstream.validated-transactions.v1"
    PREDICTIONS = "sentinelstream.predictions.v1"
    ALERTS = "sentinelstream.alerts.v1"
    FEEDBACK = "sentinelstream.feedback.v1"
    AUDIT = "sentinelstream.audit.v1"
    DEAD_LETTER = "sentinelstream.dead-letter.v1"


@dataclass(frozen=True, slots=True)
class TopicDefinition:
    """Provisioning and partition-key metadata for one topic."""

    name: TopicName
    partitions: int
    replication_factor: int
    key_strategy: str


def topic_definitions(settings: KafkaSettings | None = None) -> tuple[TopicDefinition, ...]:
    """Return the complete topic catalog using the configured partition count."""

    settings = settings or KafkaSettings()
    partitions = settings.topic_partitions
    replication_factor = settings.replication_factor
    return tuple(
        TopicDefinition(name, partitions, replication_factor, key_strategy)
        for name, key_strategy in (
            (TopicName.TRANSACTIONS, "customer_id"),
            (TopicName.VALIDATED_TRANSACTIONS, "customer_id"),
            (TopicName.PREDICTIONS, "customer_id"),
            (TopicName.ALERTS, "customer_id"),
            (TopicName.FEEDBACK, "transaction_id"),
            (TopicName.AUDIT, "event_id_or_message_id"),
            (TopicName.DEAD_LETTER, "original_key_or_message_id"),
        )
    )


def normalise_topic(topic: TopicName | str) -> TopicName:
    try:
        return topic if isinstance(topic, TopicName) else TopicName(topic)
    except ValueError as exc:
        raise ValueError(f"unsupported SentinelStream topic: {topic}") from exc


def topic_definition(
    topic: TopicName | str,
    settings: KafkaSettings | None = None,
) -> TopicDefinition:
    """Look up a topic definition or raise for an undeclared topic."""

    normalised = normalise_topic(topic)
    for definition in topic_definitions(settings):
        if definition.name is normalised:
            return definition
    raise ValueError(f"no definition found for topic: {normalised}")


def message_key(topic: TopicName | str, message: Any) -> str:
    """Extract the stable business key used to preserve per-entity ordering."""

    normalised = normalise_topic(topic)
    if normalised in {
        TopicName.TRANSACTIONS,
        TopicName.VALIDATED_TRANSACTIONS,
        TopicName.PREDICTIONS,
        TopicName.ALERTS,
    }:
        event = getattr(message, "event", None)
        customer_id = getattr(event, "customer_id", None)
        if customer_id:
            return str(customer_id)
        customer_id = getattr(message, "customer_id", None)
        if customer_id:
            return str(customer_id)
    if normalised is TopicName.FEEDBACK:
        transaction_id = getattr(message, "transaction_id", None)
        if transaction_id:
            return str(transaction_id)
    if normalised is TopicName.AUDIT:
        event_id = getattr(message, "event_id", None)
        if event_id:
            return str(event_id)
    if normalised is TopicName.DEAD_LETTER:
        original_key = getattr(message, "original_key", None)
        if original_key:
            return str(original_key)
    message_id = getattr(message, "message_id", None)
    if message_id:
        return str(message_id)
    raise ValueError(f"message has no usable partition key for {normalised}")


def partition_for(key: str, partition_count: int) -> int:
    """Return a deterministic partition index without relying on Python hash randomisation."""

    if not key:
        raise ValueError("partition key must not be empty")
    if partition_count < 1:
        raise ValueError("partition_count must be positive")
    return crc32(key.encode("utf-8")) % partition_count


class KafkaTopicManager:
    """Create the declared topics through Kafka's admin API."""

    def __init__(self, settings: KafkaSettings | None = None) -> None:
        self.settings = settings or KafkaSettings()

    def ensure_topics(self) -> tuple[TopicName, ...]:
        """Create missing topics and tolerate topics already provisioned by an operator."""

        try:
            from confluent_kafka import KafkaError, KafkaException
            from confluent_kafka.admin import AdminClient, NewTopic
        except ImportError as exc:
            raise RuntimeError("Kafka support requires the confluent-kafka dependency") from exc

        admin = AdminClient(
            kafka_client_config(
                self.settings,
                {"client.id": f"{self.settings.client_id}-admin"},
            )
        )
        topics = [
            NewTopic(
                definition.name.value,
                num_partitions=definition.partitions,
                replication_factor=definition.replication_factor,
            )
            for definition in topic_definitions(self.settings)
        ]
        futures = admin.create_topics(topics)
        for future in futures.values():
            try:
                future.result()
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                if error is None or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
        return tuple(definition.name for definition in topic_definitions(self.settings))
