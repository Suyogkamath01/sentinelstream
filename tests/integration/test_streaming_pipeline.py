from __future__ import annotations

from typing import Any

from sentinelstream.config.settings import KafkaSettings, SimulationSettings
from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.models.decision import PredictionSignals
from sentinelstream.simulation.generator import TransactionGenerator
from sentinelstream.streaming.codec import decode_message
from sentinelstream.streaming.consumer import KafkaMessageConsumer
from sentinelstream.streaming.contracts import (
    AlertMessage,
    DeadLetterMessage,
    FeedbackMessage,
    FeedbackOutcome,
    PredictionMessage,
    TransactionMessage,
    ValidatedTransactionMessage,
)
from sentinelstream.streaming.memory import InMemoryKafkaBroker
from sentinelstream.streaming.pipeline import StreamingPipeline
from sentinelstream.streaming.producer import KafkaMessageProducer
from sentinelstream.streaming.reliability import RetryPolicy
from sentinelstream.streaming.topics import TopicName, message_key, partition_for
from sentinelstream.streaming.validator import TransactionValidator


def settings() -> KafkaSettings:
    return KafkaSettings(
        topic_partitions=4,
        request_timeout_ms=1_000,
        max_poll_records=10,
        retry_attempts=3,
        retry_backoff_seconds=0.0,
        auto_offset_reset="earliest",
    )


def event() -> TransactionEvent:
    record = TransactionGenerator(
        SimulationSettings(
            customer_count=2,
            merchant_count=2,
            random_seed=8,
            fraud_ratio=0.0,
        )
    ).generate(1)[0]
    return record.event


def producer(broker: InMemoryKafkaBroker, kafka_settings: KafkaSettings) -> KafkaMessageProducer:
    return KafkaMessageProducer(
        kafka_settings,
        transport=broker.producer(),
        retry_policy=RetryPolicy(max_attempts=1),
    )


def test_transaction_pipeline_publishes_validated_prediction_alert_and_audit() -> None:
    kafka_settings = settings()
    broker = InMemoryKafkaBroker()
    publishing = producer(broker, kafka_settings)

    def high_risk_predictor(
        transaction: TransactionEvent,
        features: dict[str, float | int],
    ) -> PredictionSignals:
        return PredictionSignals(
            calibrated_probability=0.9,
            rule_score=0.9,
            confidence=0.9,
            model_version="test-model",
            calibration_version="test-calibration",
        )

    pipeline = StreamingPipeline(producer=publishing, predictor=high_risk_predictor)
    transactions = KafkaMessageConsumer(
        TopicName.TRANSACTIONS,
        pipeline.process_transaction,
        transport=broker.consumer(
            TopicName.TRANSACTIONS,
            group_id="pipeline",
            auto_offset_reset="earliest",
        ),
        dead_letter_producer=publishing,
        settings=kafka_settings,
        retry_policy=RetryPolicy(max_attempts=1),
    )

    publishing.publish_transaction(event())
    results = transactions.run(max_records=1)

    assert results[0].status == "processed"
    assert len(broker.records(TopicName.VALIDATED_TRANSACTIONS)) == 1
    assert len(broker.records(TopicName.PREDICTIONS)) == 1
    assert len(broker.records(TopicName.ALERTS)) == 1
    assert len(broker.records(TopicName.AUDIT)) == 1

    prediction = decode_message(
        TopicName.PREDICTIONS,
        broker.records(TopicName.PREDICTIONS)[0].value,
    )
    alert = decode_message(TopicName.ALERTS, broker.records(TopicName.ALERTS)[0].value)
    assert isinstance(prediction, PredictionMessage)
    assert isinstance(alert, AlertMessage)
    assert prediction.calibrated_probability == 0.9
    assert alert.decision.action.value == "block"


def test_duplicate_delivery_is_audited_without_a_second_prediction() -> None:
    kafka_settings = settings()
    broker = InMemoryKafkaBroker()
    publishing = producer(broker, kafka_settings)
    transaction = event()
    pipeline = StreamingPipeline(producer=publishing)
    transactions = KafkaMessageConsumer(
        TopicName.TRANSACTIONS,
        pipeline.process_transaction,
        transport=broker.consumer(
            TopicName.TRANSACTIONS,
            group_id="dedupe",
            auto_offset_reset="earliest",
        ),
        dead_letter_producer=publishing,
        settings=kafka_settings,
        retry_policy=RetryPolicy(max_attempts=1),
    )

    publishing.publish(TransactionMessage(event=transaction))
    publishing.publish(TransactionMessage(event=transaction))
    transactions.run(max_records=2)

    assert len(broker.records(TopicName.PREDICTIONS)) == 1
    assert len(broker.records(TopicName.AUDIT)) == 2


def test_schema_poison_message_is_sent_to_dlq_and_offset_is_committed() -> None:
    kafka_settings = settings()
    broker = InMemoryKafkaBroker()
    publishing = producer(broker, kafka_settings)
    transactions = KafkaMessageConsumer(
        TopicName.TRANSACTIONS,
        lambda message: None,
        transport=broker.consumer(
            TopicName.TRANSACTIONS,
            group_id="poison",
            auto_offset_reset="earliest",
        ),
        dead_letter_producer=publishing,
        settings=kafka_settings,
        retry_policy=RetryPolicy(max_attempts=3),
    )
    publishing.publish_raw(
        TopicName.TRANSACTIONS,
        '{"event": {"not": "a transaction"}}',
        key="poison-customer",
    )

    result = transactions.run(max_records=1)
    assert result[0].status == "dead_letter"
    assert result[0].attempts == 1
    assert len(broker.records(TopicName.DEAD_LETTER)) == 1

    dead_letter = decode_message(
        TopicName.DEAD_LETTER,
        broker.records(TopicName.DEAD_LETTER)[0].value,
    )
    assert isinstance(dead_letter, DeadLetterMessage)
    assert dead_letter.original_topic == TopicName.TRANSACTIONS.value
    assert dead_letter.error_type == "MessageValidationError"
    assert transactions.run(max_records=1, max_idle_polls=1) == []


def test_handler_retries_before_commit_and_replay_uses_earliest_offsets() -> None:
    kafka_settings = settings()
    broker = InMemoryKafkaBroker()
    publishing = producer(broker, kafka_settings)
    attempts = 0

    def flaky_handler(message: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("temporary downstream outage")

    transaction = event()
    publishing.publish_transaction(transaction)
    consumer = KafkaMessageConsumer(
        TopicName.TRANSACTIONS,
        flaky_handler,
        transport=broker.consumer(
            TopicName.TRANSACTIONS,
            group_id="retry",
            auto_offset_reset="earliest",
        ),
        dead_letter_producer=publishing,
        settings=kafka_settings,
        retry_policy=RetryPolicy(max_attempts=3),
    )

    result = consumer.run(max_records=1)
    assert result[0].attempts == 3
    assert result[0].committed
    assert len(broker.records(TopicName.DEAD_LETTER)) == 0

    replayed: list[str] = []
    replay_consumer = KafkaMessageConsumer(
        TopicName.TRANSACTIONS,
        lambda message: replayed.append(str(message.event.event_id)),
        transport=broker.consumer(
            TopicName.TRANSACTIONS,
            group_id="replay-run-unique",
            auto_offset_reset="earliest",
        ),
        dead_letter_producer=publishing,
        settings=kafka_settings,
        retry_policy=RetryPolicy(max_attempts=1),
    )
    replay_consumer.replay(
        max_records=1,
        transport=broker.consumer(
            TopicName.TRANSACTIONS,
            group_id="replay-run-unique-transport",
            auto_offset_reset="earliest",
        ),
    )
    assert replayed == [str(transaction.event_id)]


def test_feedback_topic_is_typed_and_audited() -> None:
    kafka_settings = settings()
    broker = InMemoryKafkaBroker()
    publishing = producer(broker, kafka_settings)
    transaction = event()
    pipeline = StreamingPipeline(producer=publishing)
    feedback = FeedbackMessage(
        event_id=transaction.event_id,
        transaction_id=transaction.transaction_id,
        analyst_id="analyst-001",
        outcome=FeedbackOutcome.CONFIRMED_FRAUD,
    )
    feedback_consumer = KafkaMessageConsumer(
        TopicName.FEEDBACK,
        pipeline.process_feedback,
        transport=broker.consumer(
            TopicName.FEEDBACK,
            group_id="feedback",
            auto_offset_reset="earliest",
        ),
        dead_letter_producer=publishing,
        settings=kafka_settings,
        retry_policy=RetryPolicy(max_attempts=1),
    )

    publishing.publish_feedback(feedback)
    result = feedback_consumer.run(max_records=1)

    assert result[0].status == "processed"
    assert len(broker.records(TopicName.AUDIT)) == 1


def test_partition_key_is_stable_for_customer_state() -> None:
    message = TransactionMessage(event=event())
    key = message_key(TopicName.TRANSACTIONS, message)

    assert key == message.event.customer_id
    assert partition_for(key, 4) == partition_for(key, 4)


def test_transaction_validator_preserves_identity_and_emits_validation_audit() -> None:
    kafka_settings = settings()
    broker = InMemoryKafkaBroker()
    publishing = producer(broker, kafka_settings)
    transactions = KafkaMessageConsumer(
        TopicName.TRANSACTIONS,
        TransactionValidator(publishing),
        transport=broker.consumer(
            TopicName.TRANSACTIONS,
            group_id="validator",
            auto_offset_reset="earliest",
        ),
        dead_letter_producer=publishing,
        settings=kafka_settings,
        retry_policy=RetryPolicy(max_attempts=1),
    )
    message = TransactionMessage(event=event())
    publishing.publish(message)

    result = transactions.run(max_records=1)

    assert result[0].status == "processed"
    validated = decode_message(
        TopicName.VALIDATED_TRANSACTIONS,
        broker.records(TopicName.VALIDATED_TRANSACTIONS)[0].value,
    )
    assert isinstance(validated, ValidatedTransactionMessage)
    assert validated.message_id == message.message_id
    audit = decode_message(TopicName.AUDIT, broker.records(TopicName.AUDIT)[0].value)
    assert audit.event_type.value == "validated"
