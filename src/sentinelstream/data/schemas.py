"""Canonical transaction and simulation-only ground-truth contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from ipaddress import IPv4Address, IPv6Address
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CURRENCY_CODES = frozenset({"AUD", "BRL", "CAD", "EUR", "GBP", "INR", "JPY", "SGD", "USD"})


class TransactionType(StrEnum):
    """Transaction types used by the simulator."""

    PURCHASE = "purchase"
    CASH_WITHDRAWAL = "cash_withdrawal"
    TRANSFER = "transfer"
    REFUND = "refund"


class Channel(StrEnum):
    """Customer-facing transaction channels."""

    ECOMMERCE = "ecommerce"
    POS = "pos"
    ATM = "atm"
    MOBILE = "mobile"
    BANK_TRANSFER = "bank_transfer"
    QR = "qr"


class AuthenticationMethod(StrEnum):
    """Authentication methods represented in transaction events."""

    THREE_DS = "3ds"
    PIN = "pin"
    BIOMETRIC = "biometric"
    OTP = "otp"
    NONE = "none"


class TransactionStatus(StrEnum):
    """Processing status of a transaction."""

    APPROVED = "approved"
    DECLINED = "declined"
    REVERSED = "reversed"


class FraudScenario(StrEnum):
    """Fraud patterns available in the synthetic data generator."""

    ACCOUNT_TAKEOVER = "account_takeover"
    CARD_TESTING = "card_testing"
    STOLEN_CARD_USAGE = "stolen_card_usage"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    HIGH_VALUE_ANOMALY = "high_value_anomaly"
    RAPID_TRANSACTION_VELOCITY = "rapid_transaction_velocity"
    NEW_DEVICE_FRAUD = "new_device_fraud"
    UNUSUAL_MERCHANT_CATEGORY = "unusual_merchant_category"
    CASH_OUT = "cash_out"
    COORDINATED_FRAUD_CAMPAIGN = "coordinated_fraud_campaign"
    MERCHANT_COLLUSION = "merchant_collusion"
    REFUND_ABUSE = "refund_abuse"
    ATM_FRAUD = "atm_fraud"
    CROSS_BORDER_FRAUD = "cross_border_fraud"


class TransactionEvent(BaseModel):
    """Serving-safe transaction event; it intentionally excludes fraud labels."""

    model_config = ConfigDict(extra="forbid")

    transaction_id: UUID
    event_id: UUID
    customer_id: str = Field(min_length=3, max_length=64)
    account_id: str = Field(min_length=3, max_length=64)
    card_id: str = Field(min_length=3, max_length=64)
    merchant_id: str = Field(min_length=3, max_length=64)
    merchant_category: str = Field(min_length=2, max_length=64)
    transaction_amount: float = Field(gt=0.0, le=10_000_000.0)
    currency: str = Field(min_length=3, max_length=3)
    transaction_type: TransactionType
    channel: Channel
    timestamp: datetime
    country: str = Field(min_length=2, max_length=2)
    city: str = Field(min_length=2, max_length=80)
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    device_id: str = Field(min_length=3, max_length=128)
    ip_address: IPv4Address | IPv6Address
    card_present: bool
    authentication_method: AuthenticationMethod
    transaction_status: TransactionStatus
    event_version: int = Field(default=1, ge=1)
    ingestion_timestamp: datetime

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        value = value.upper()
        if value not in CURRENCY_CODES:
            raise ValueError(f"unsupported currency: {value}")
        return value

    @field_validator("country")
    @classmethod
    def validate_country(cls, value: str) -> str:
        return value.upper()

    @field_validator("timestamp", "ingestion_timestamp")
    @classmethod
    def normalise_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_event_order(self) -> TransactionEvent:
        if self.ingestion_timestamp < self.timestamp:
            raise ValueError("ingestion_timestamp cannot precede event timestamp")
        return self


class SimulationGroundTruth(BaseModel):
    """Synthetic evaluation metadata kept outside the serving event contract."""

    model_config = ConfigDict(extra="forbid")

    fraud_label: bool
    fraud_type: FraudScenario | None = None

    @model_validator(mode="after")
    def validate_label_and_type(self) -> SimulationGroundTruth:
        if self.fraud_label != (self.fraud_type is not None):
            raise ValueError("fraud_label and fraud_type must agree")
        return self


class SimulatedTransaction(BaseModel):
    """A generated event paired with simulation-only evaluation metadata."""

    model_config = ConfigDict(extra="forbid")

    event: TransactionEvent
    ground_truth: SimulationGroundTruth

    def to_json_record(self) -> dict[str, Any]:
        """Return a JSON-serialisable nested record."""

        return self.model_dump(mode="json")
