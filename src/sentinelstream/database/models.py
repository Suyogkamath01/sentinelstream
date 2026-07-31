"""Relational models for transaction history, risk outputs, and profiles."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from sentinelstream.database.base import Base, TimestampMixin, utc_now


class TransactionRecord(TimestampMixin, Base):
    """Persisted serving transaction and its original event payload."""

    __tablename__ = "transactions"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_transactions_event_id"),
        UniqueConstraint("transaction_id", name="uq_transactions_transaction_id"),
        Index("ix_transactions_customer_event_time", "customer_id", "event_time"),
        Index("ix_transactions_merchant_event_time", "merchant_id", "event_time"),
        Index("ix_transactions_transaction_id", "transaction_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transaction_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    merchant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    device_id: Mapped[str] = mapped_column(String(128), nullable=False)
    country: Mapped[str] = mapped_column(String(2), nullable=False)
    transaction_amount: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    transaction_status: Mapped[str] = mapped_column(String(32), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    predictions: Mapped[list[PredictionRecord]] = relationship(back_populates="transaction")


class PredictionRecord(TimestampMixin, Base):
    """Persisted hybrid fraud score and evidence."""

    __tablename__ = "predictions"
    __table_args__ = (
        UniqueConstraint("prediction_message_id", name="uq_predictions_message_id"),
        Index("ix_predictions_customer_created", "customer_id", "created_at"),
        Index("ix_predictions_transaction_id", "transaction_id"),
        CheckConstraint(
            "final_risk_score >= 0 AND final_risk_score <= 1", name="ck_prediction_score"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prediction_message_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    transaction_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("transactions.transaction_id"), nullable=False
    )
    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    final_risk_score: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    risk_tier: Mapped[str] = mapped_column(String(16), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    model_probability: Mapped[float | None] = mapped_column(Float)
    anomaly_score: Mapped[float | None] = mapped_column(Float)
    rule_score: Mapped[float | None] = mapped_column(Float)
    model_version: Mapped[str] = mapped_column(String(128), nullable=False)
    calibration_version: Mapped[str] = mapped_column(String(128), nullable=False)
    feature_version: Mapped[str] = mapped_column(String(128), nullable=False)
    reason_codes: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    source_scores: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    transaction: Mapped[TransactionRecord] = relationship(back_populates="predictions")
    alerts: Mapped[list[AlertRecord]] = relationship(back_populates="prediction")


class AlertRecord(TimestampMixin, Base):
    """Analyst-facing alert lifecycle record."""

    __tablename__ = "alerts"
    __table_args__ = (
        UniqueConstraint("alert_id", name="uq_alerts_alert_id"),
        Index("ix_alerts_status_priority", "status", "priority"),
        Index("ix_alerts_customer_created", "customer_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    alert_id: Mapped[str] = mapped_column(String(36), nullable=False)
    prediction_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("predictions.id"), nullable=False
    )
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    transaction_id: Mapped[str] = mapped_column(String(36), nullable=False)
    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    risk_tier: Mapped[str] = mapped_column(String(16), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reason_codes: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    acknowledged_by: Mapped[str | None] = mapped_column(String(128))
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_by: Mapped[str | None] = mapped_column(String(128))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    prediction: Mapped[PredictionRecord] = relationship(back_populates="alerts")


class FeedbackRecord(TimestampMixin, Base):
    """Analyst feedback associated with a scored transaction."""

    __tablename__ = "feedback"
    __table_args__ = (
        Index("ix_feedback_transaction_created", "transaction_id", "created_at"),
        Index("ix_feedback_analyst_created", "analyst_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    feedback_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    transaction_id: Mapped[str] = mapped_column(String(36), nullable=False)
    analyst_id: Mapped[str] = mapped_column(String(128), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    note: Mapped[str | None] = mapped_column(Text)


class CustomerProfile(TimestampMixin, Base):
    """Materialized customer risk profile used by the hybrid engine."""

    __tablename__ = "customer_profiles"

    customer_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    observations: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    version: Mapped[str] = mapped_column(String(128), nullable=False, default="unknown")
    reason_codes: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class MerchantProfile(TimestampMixin, Base):
    """Materialized merchant risk profile used by the hybrid engine."""

    __tablename__ = "merchant_profiles"

    merchant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    observations: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    version: Mapped[str] = mapped_column(String(128), nullable=False, default="unknown")
    reason_codes: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class DeviceProfile(TimestampMixin, Base):
    """Materialized device risk profile used by the hybrid engine."""

    __tablename__ = "device_profiles"

    device_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    observations: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    version: Mapped[str] = mapped_column(String(128), nullable=False, default="unknown")
    reason_codes: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class AuditRecord(TimestampMixin, Base):
    """Append-only audit evidence for API and stream operations."""

    __tablename__ = "audit_records"
    __table_args__ = (
        UniqueConstraint("message_id", name="uq_audit_message_id"),
        Index("ix_audit_event_created", "event_type", "created_at"),
        Index("ix_audit_transaction_created", "transaction_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    message_id: Mapped[str] = mapped_column(String(36), nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(36))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False)
    source_topic: Mapped[str] = mapped_column(String(256), nullable=False)
    event_id: Mapped[str | None] = mapped_column(String(36))
    transaction_id: Mapped[str | None] = mapped_column(String(36))
    details: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
