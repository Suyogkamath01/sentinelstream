"""Versioned message contracts carried by SentinelStream Kafka topics."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinelstream.data.schemas import TransactionEvent
from sentinelstream.models.decision import RecommendedAction, RiskTier

STREAM_SCHEMA_VERSION = "phase6-v1"


def _utc_now() -> datetime:
    return datetime.now(UTC)


class StreamMessage(BaseModel):
    """Common metadata used for tracing, replay, and audit correlation."""

    model_config = ConfigDict(extra="forbid")

    message_id: UUID = Field(default_factory=uuid4)
    schema_version: str = STREAM_SCHEMA_VERSION
    produced_at: datetime = Field(default_factory=_utc_now)
    correlation_id: UUID | None = None

    @field_validator("produced_at")
    @classmethod
    def normalise_produced_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("produced_at must include a timezone")
        return value.astimezone(UTC)

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != STREAM_SCHEMA_VERSION:
            raise ValueError(f"unsupported stream schema version: {value}")
        return value


class TransactionMessage(StreamMessage):
    """A serving-safe transaction delivered to the transaction topic."""

    event: TransactionEvent


class ValidatedTransactionMessage(StreamMessage):
    """A schema-valid transaction ready for online feature calculation."""

    event: TransactionEvent
    validation_version: str = "phase2-validation-v1"


class PredictionMessage(StreamMessage):
    """Model and feature outputs for one accepted transaction."""

    event_id: UUID
    transaction_id: UUID
    customer_id: str = Field(min_length=3, max_length=64)
    features: dict[str, float | int]
    model_probability: float | None = Field(default=None, ge=0.0, le=1.0)
    calibrated_probability: float = Field(ge=0.0, le=1.0)
    anomaly_score: float | None = Field(default=None, ge=0.0, le=1.0)
    rule_score: float | None = Field(default=None, ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    model_version: str = Field(min_length=1, max_length=128)
    calibration_version: str = Field(min_length=1, max_length=128)
    feature_version: str = Field(min_length=1, max_length=128)
    explanation: dict[str, Any] | None = None


class DecisionMessage(BaseModel):
    """Serializable form of the Phase 4 risk decision dataclass."""

    model_config = ConfigDict(extra="forbid")

    calibrated_probability: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    abstained: bool
    risk_tier: RiskTier
    action: RecommendedAction
    review_threshold: float = Field(ge=0.0, le=1.0)
    block_threshold: float = Field(ge=0.0, le=1.0)
    policy_version: str = Field(min_length=1, max_length=128)
    model_version: str = Field(min_length=1, max_length=128)
    calibration_version: str = Field(min_length=1, max_length=128)
    reason_codes: tuple[str, ...]


class AlertMessage(StreamMessage):
    """Analyst-facing intervention generated for review or block decisions."""

    event_id: UUID
    transaction_id: UUID
    customer_id: str = Field(min_length=3, max_length=64)
    decision: DecisionMessage
    prediction_message_id: UUID
    reason_codes: tuple[str, ...]
    explanation: dict[str, Any] | None = None


class FeedbackOutcome(StrEnum):
    """Analyst outcomes accepted on the feedback topic."""

    CONFIRMED_FRAUD = "confirmed_fraud"
    NOT_FRAUD = "not_fraud"
    ESCALATED = "escalated"
    NO_ACTION = "no_action"


class FeedbackMessage(StreamMessage):
    """Analyst feedback correlated to a scored transaction."""

    event_id: UUID
    transaction_id: UUID
    analyst_id: str = Field(min_length=1, max_length=128)
    outcome: FeedbackOutcome
    note: str | None = Field(default=None, max_length=2_000)


class AuditEventType(StrEnum):
    """Lifecycle events emitted to the append-only audit topic."""

    RECEIVED = "received"
    VALIDATED = "validated"
    PREDICTED = "predicted"
    ALERTED = "alerted"
    FEEDBACK_RECEIVED = "feedback_received"
    IGNORED = "ignored"
    QUARANTINED = "quarantined"
    REPLAYED = "replayed"


class AuditMessage(StreamMessage):
    """Structured processing evidence for one stream operation."""

    event_type: AuditEventType
    status: str = Field(min_length=1, max_length=64)
    source_topic: str = Field(min_length=1, max_length=256)
    event_id: UUID | None = None
    transaction_id: UUID | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class DeadLetterMessage(StreamMessage):
    """A bounded, replayable record for a poison message or failed handler."""

    original_topic: str = Field(min_length=1, max_length=256)
    original_partition: int = Field(ge=0)
    original_offset: int = Field(ge=0)
    original_key: str | None = Field(default=None, max_length=512)
    raw_payload: str = Field(max_length=1_000_000)
    error_type: str = Field(min_length=1, max_length=256)
    error_message: str = Field(min_length=1, max_length=2_000)
    attempts: int = Field(ge=1)
    failed_at: datetime = Field(default_factory=_utc_now)

    @field_validator("failed_at")
    @classmethod
    def normalise_failed_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("failed_at must include a timezone")
        return value.astimezone(UTC)


type Message = (
    TransactionMessage
    | ValidatedTransactionMessage
    | PredictionMessage
    | AlertMessage
    | FeedbackMessage
    | AuditMessage
    | DeadLetterMessage
)
