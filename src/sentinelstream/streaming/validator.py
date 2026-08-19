"""Kafka transaction validation handler used by the local streaming worker."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from sentinelstream.streaming.contracts import (
    AuditEventType,
    AuditMessage,
    TransactionMessage,
    ValidatedTransactionMessage,
)
from sentinelstream.streaming.producer import KafkaMessageProducer
from sentinelstream.streaming.topics import TopicName


@dataclass(slots=True)
class TransactionValidator:
    """Publish validated envelopes while preserving the ingress message identity."""

    producer: KafkaMessageProducer

    def __call__(self, message: TransactionMessage) -> None:
        validated = ValidatedTransactionMessage(
            message_id=message.message_id,
            correlation_id=message.message_id,
            event=message.event,
        )
        self.producer.publish(validated, topic=TopicName.VALIDATED_TRANSACTIONS)
        audit = AuditMessage(
            message_id=uuid5(
                NAMESPACE_URL,
                f"sentinelstream:validated-audit:{message.message_id}",
            ),
            correlation_id=message.message_id,
            event_type=AuditEventType.VALIDATED,
            status="validated",
            source_topic=TopicName.TRANSACTIONS.value,
            event_id=message.event.event_id,
            transaction_id=message.event.transaction_id,
            details={"validation_version": validated.validation_version},
        )
        self.producer.publish(audit, topic=TopicName.AUDIT)
