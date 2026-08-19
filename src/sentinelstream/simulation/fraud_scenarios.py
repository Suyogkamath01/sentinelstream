"""Fraud scenario mutations for synthetic events."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid5

from sentinelstream.data.schemas import (
    AuthenticationMethod,
    Channel,
    FraudScenario,
    TransactionEvent,
    TransactionStatus,
    TransactionType,
)

SCENARIOS = tuple(FraudScenario)


def _bounded_amount(value: float) -> float:
    return round(min(max(value, 0.50), 10_000_000.0), 2)


def apply_fraud_scenario(
    event: TransactionEvent, scenario: FraudScenario, *, seed: str
) -> TransactionEvent:
    """Apply one explainable fraud pattern to an otherwise valid event."""

    namespace = UUID("2b9c7a1c-c9d0-4e38-a3e9-8f2eea4b8143")
    updates: dict[str, object] = {}
    if scenario is FraudScenario.ACCOUNT_TAKEOVER:
        updates = {
            "device_id": f"compromised-{uuid5(namespace, seed).hex[:16]}",
            "authentication_method": AuthenticationMethod.NONE,
            "channel": Channel.ECOMMERCE,
            "amount": None,
            "transaction_amount": _bounded_amount(event.transaction_amount * 2.5),
        }
    elif scenario is FraudScenario.CARD_TESTING:
        updates = {
            "transaction_amount": _bounded_amount(min(event.transaction_amount, 15.0)),
            "merchant_category": "digital_goods",
            "channel": Channel.ECOMMERCE,
            "authentication_method": AuthenticationMethod.NONE,
        }
    elif scenario is FraudScenario.STOLEN_CARD_USAGE:
        updates = {
            "card_present": False,
            "authentication_method": AuthenticationMethod.NONE,
            "transaction_amount": _bounded_amount(event.transaction_amount * 1.8),
            "country": "GB",
            "city": "London",
            "latitude": 51.5074,
            "longitude": -0.1278,
        }
    elif scenario is FraudScenario.IMPOSSIBLE_TRAVEL:
        updates = {
            "country": "JP",
            "city": "Tokyo",
            "latitude": 35.6762,
            "longitude": 139.6503,
            "channel": Channel.POS,
        }
    elif scenario is FraudScenario.HIGH_VALUE_ANOMALY:
        updates = {
            "transaction_amount": _bounded_amount(max(event.transaction_amount * 12, 8_000.0))
        }
    elif scenario is FraudScenario.RAPID_TRANSACTION_VELOCITY:
        updates = {
            "timestamp": event.timestamp + timedelta(seconds=1),
            "transaction_amount": _bounded_amount(min(event.transaction_amount, 75.0)),
        }
    elif scenario is FraudScenario.NEW_DEVICE_FRAUD:
        updates = {
            "device_id": f"new-device-{uuid5(namespace, seed).hex[:16]}",
            "authentication_method": AuthenticationMethod.OTP,
        }
    elif scenario is FraudScenario.UNUSUAL_MERCHANT_CATEGORY:
        updates = {
            "merchant_category": "gambling",
            "transaction_amount": _bounded_amount(event.transaction_amount * 2),
        }
    elif scenario is FraudScenario.CASH_OUT:
        updates = {
            "transaction_type": TransactionType.CASH_WITHDRAWAL,
            "channel": Channel.ATM,
            "merchant_category": "cash_advance",
            "transaction_amount": _bounded_amount(max(event.transaction_amount * 4, 1_000.0)),
        }
    elif scenario is FraudScenario.COORDINATED_FRAUD_CAMPAIGN:
        updates = {
            "merchant_id": "merchant-campaign-000001",
            "device_id": "campaign-device-000001",
            "transaction_amount": _bounded_amount(event.transaction_amount * 1.5),
        }
    elif scenario is FraudScenario.MERCHANT_COLLUSION:
        updates = {
            "merchant_id": "merchant-colluding-000001",
            "merchant_category": "cash_advance",
            "transaction_amount": _bounded_amount(event.transaction_amount * 2.2),
        }
    elif scenario is FraudScenario.REFUND_ABUSE:
        updates = {
            "transaction_type": TransactionType.REFUND,
            "transaction_status": TransactionStatus.APPROVED,
            "transaction_amount": _bounded_amount(event.transaction_amount),
        }
    elif scenario is FraudScenario.ATM_FRAUD:
        updates = {
            "transaction_type": TransactionType.CASH_WITHDRAWAL,
            "channel": Channel.ATM,
            "merchant_category": "cash_advance",
            "card_present": False,
            "transaction_amount": _bounded_amount(max(event.transaction_amount * 3, 500.0)),
        }
    elif scenario is FraudScenario.CROSS_BORDER_FRAUD:
        updates = {
            "country": "BR",
            "city": "Sao Paulo",
            "latitude": -23.5505,
            "longitude": -46.6333,
            "currency": "BRL",
        }
    else:
        raise ValueError(f"unsupported fraud scenario: {scenario}")

    updates.pop("amount", None)
    return event.model_copy(update=updates)
