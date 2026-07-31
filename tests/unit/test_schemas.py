from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from sentinelstream.data.schemas import TransactionEvent


def valid_event() -> dict[str, object]:
    event_time = datetime(2025, 1, 1, 12, tzinfo=UTC)
    return {
        "transaction_id": uuid4(),
        "event_id": uuid4(),
        "customer_id": "customer-001",
        "account_id": "account-001",
        "card_id": "card-001",
        "merchant_id": "merchant-001",
        "merchant_category": "grocery",
        "transaction_amount": 24.50,
        "currency": "USD",
        "transaction_type": "purchase",
        "channel": "pos",
        "timestamp": event_time,
        "country": "US",
        "city": "New York",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "device_id": "device-001",
        "ip_address": "192.0.2.1",
        "card_present": True,
        "authentication_method": "pin",
        "transaction_status": "approved",
        "event_version": 1,
        "ingestion_timestamp": event_time,
    }


def test_transaction_schema_normalises_utc_values() -> None:
    event = TransactionEvent(**valid_event())

    assert event.currency == "USD"
    assert event.timestamp.tzinfo == UTC
    assert event.ingestion_timestamp == event.timestamp


@pytest.mark.parametrize(
    ("field", "value"),
    [("currency", "ZZZ"), ("transaction_amount", 0), ("latitude", 91), ("country", "USA")],
)
def test_transaction_schema_rejects_invalid_values(field: str, value: object) -> None:
    payload = valid_event()
    payload[field] = value

    with pytest.raises(ValidationError):
        TransactionEvent(**payload)


def test_transaction_schema_rejects_naive_timestamp() -> None:
    payload = valid_event()
    payload["timestamp"] = datetime(2025, 1, 1, 12)

    with pytest.raises(ValidationError):
        TransactionEvent(**payload)


def test_transaction_schema_rejects_ingestion_before_event() -> None:
    payload = valid_event()
    payload["ingestion_timestamp"] = datetime(2025, 1, 1, 11, tzinfo=UTC)

    with pytest.raises(ValidationError):
        TransactionEvent(**payload)
