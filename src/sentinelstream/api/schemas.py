"""Validated request and response schemas exposed by the REST API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from sentinelstream.data.schemas import TransactionEvent


class TokenRequest(BaseModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=512)


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int


class PredictionRequest(BaseModel):
    event: TransactionEvent
    supervised_probability: float | None = Field(default=None, ge=0.0, le=1.0)
    anomaly_score: float | None = Field(default=None, ge=0.0, le=1.0)
    rule_score: float | None = Field(default=None, ge=0.0, le=1.0)


class PredictionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    prediction_message_id: str
    event_id: str
    transaction_id: str
    customer_id: str
    final_risk_score: float
    confidence: float
    risk_tier: str
    action: str
    model_probability: float | None
    anomaly_score: float | None
    rule_score: float | None
    model_version: str
    calibration_version: str
    feature_version: str
    reason_codes: list[str]
    source_scores: dict[str, float]
    created_at: datetime


class ExplanationResponse(BaseModel):
    prediction_message_id: str
    summary: str | None = None
    reason_codes: list[str]
    source_scores: dict[str, float]
    feature_version: str


class AlertResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    alert_id: str
    prediction_id: int
    event_id: str
    transaction_id: str
    customer_id: str
    risk_tier: str
    action: str
    status: str
    priority: int
    reason_codes: list[str]
    acknowledged_by: str | None
    resolved_by: str | None
    created_at: datetime


class AlertDetailResponse(AlertResponse):
    """Alert plus persisted prediction evidence for investigation."""

    final_risk_score: float | None = None
    confidence: float | None = None
    model_probability: float | None = None
    anomaly_score: float | None = None
    rule_score: float | None = None
    source_scores: dict[str, float] = Field(default_factory=dict)
    explanation_summary: str | None = None


class DashboardTransactionResponse(BaseModel):
    """Joined transaction and prediction row for operational dashboards."""

    transaction_id: str
    event_id: str
    customer_id: str
    merchant_id: str
    device_id: str
    country: str
    transaction_amount: float
    currency: str
    transaction_status: str
    event_time: datetime
    prediction_message_id: str | None = None
    fraud_probability: float | None = None
    final_risk_score: float | None = None
    confidence: float | None = None
    risk_tier: str | None = None
    decision: str | None = None
    alert_status: str | None = None


class ProfileResponse(BaseModel):
    """Persisted customer or merchant risk profile."""

    entity_id: str
    risk_score: float
    confidence: float
    observations: int
    version: str
    reason_codes: list[str]
    updated_at: datetime


class FeedbackRequest(BaseModel):
    event_id: str = Field(min_length=1, max_length=36)
    transaction_id: str = Field(min_length=1, max_length=36)
    outcome: Literal["confirmed_fraud", "not_fraud", "escalated", "no_action"]
    note: str | None = Field(default=None, max_length=2_000)


class FeedbackResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    feedback_id: str
    event_id: str
    transaction_id: str
    analyst_id: str
    outcome: str
    note: str | None
    created_at: datetime


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    event_id: str
    customer_id: str
    merchant_id: str
    device_id: str
    country: str
    transaction_amount: float
    currency: str
    transaction_status: str
    event_time: datetime


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "not_ready"]
    database: bool | None = None
    redis: bool | None = None


class MetricsResponse(BaseModel):
    predictions_created: int
    alerts_created: int
    feedback_created: int
    transactions_created: int = 0
    unresolved_alerts: int = 0
    reviewed_alerts: int = 0
    blocked_transactions: int = 0
    average_fraud_probability: float | None = None
    risk_tier_distribution: dict[str, int] = Field(default_factory=dict)
