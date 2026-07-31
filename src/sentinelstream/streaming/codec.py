"""Strict JSON encoding and topic-aware schema validation."""

from __future__ import annotations

import json
from typing import cast

from pydantic import ValidationError

from sentinelstream.streaming.contracts import (
    AlertMessage,
    AuditMessage,
    DeadLetterMessage,
    FeedbackMessage,
    Message,
    PredictionMessage,
    StreamMessage,
    TransactionMessage,
    ValidatedTransactionMessage,
)
from sentinelstream.streaming.topics import TopicName, normalise_topic

MESSAGE_MODELS: dict[TopicName, type[StreamMessage]] = {
    TopicName.TRANSACTIONS: TransactionMessage,
    TopicName.VALIDATED_TRANSACTIONS: ValidatedTransactionMessage,
    TopicName.PREDICTIONS: PredictionMessage,
    TopicName.ALERTS: AlertMessage,
    TopicName.FEEDBACK: FeedbackMessage,
    TopicName.AUDIT: AuditMessage,
    TopicName.DEAD_LETTER: DeadLetterMessage,
}


class MessageEncodingError(ValueError):
    """Raised when a message cannot be encoded for the selected topic."""


class MessageValidationError(ValueError):
    """Raised when a Kafka payload violates the topic's message contract."""


def model_for_topic(topic: TopicName | str) -> type[StreamMessage]:
    normalised = normalise_topic(topic)
    return MESSAGE_MODELS[normalised]


def topic_for_message(message: Message) -> TopicName:
    """Return the only topic contract compatible with a typed message."""

    for topic, model in MESSAGE_MODELS.items():
        if isinstance(message, model):
            return topic
    raise MessageEncodingError(f"unsupported message type: {type(message).__name__}")


def encode_message(message: Message, *, topic: TopicName | str | None = None) -> bytes:
    """Encode a Pydantic message with deterministic compact JSON."""

    selected_topic = topic_for_message(message) if topic is None else normalise_topic(topic)
    expected_model = model_for_topic(selected_topic)
    if not isinstance(message, expected_model):
        raise MessageEncodingError(
            f"{type(message).__name__} is not valid for topic {selected_topic.value}"
        )
    try:
        return json.dumps(
            message.model_dump(mode="json"),
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise MessageEncodingError(f"message could not be JSON encoded: {exc}") from exc


def decode_message(topic: TopicName | str, payload: bytes | str) -> Message:
    """Decode and validate a payload using the contract assigned to its topic."""

    selected_topic = normalise_topic(topic)
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        raw = json.loads(text)
        if not isinstance(raw, dict):
            raise TypeError("message JSON root must be an object")
        model = model_for_topic(selected_topic)
        return cast(Message, model.model_validate(raw))
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValidationError,
        ValueError,
    ) as exc:
        raise MessageValidationError(
            f"invalid payload for topic {selected_topic.value}: {exc}"
        ) from exc
