from __future__ import annotations

import httpx

from sentinelstream.dashboard.client import DashboardAPIError, DashboardClient
from sentinelstream.dashboard.transforms import (
    filter_transactions,
    format_amount,
    format_probability,
    format_timestamp,
    mask_records_for_display,
    sort_frame,
)


def test_dashboard_client_authenticates_and_sends_bearer_token() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/v1/auth/token":
            return httpx.Response(200, json={"access_token": "token", "expires_in": 60})
        return httpx.Response(200, json={"transactions_created": 2})

    client = DashboardClient("https://api.example", transport=httpx.MockTransport(handler))
    assert client.authenticate("analyst", "password") == 60
    assert client.metrics()["transactions_created"] == 2
    assert seen[-1].headers["authorization"] == "Bearer token"


def test_dashboard_client_surfaces_api_errors() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(503, text="service unavailable"))
    client = DashboardClient("https://api.example", transport=transport)
    try:
        client.health()
    except DashboardAPIError as exc:
        assert exc.status_code == 503
        assert "service unavailable" in str(exc)
    else:
        raise AssertionError("expected DashboardAPIError")


def test_transaction_filtering_and_formatting() -> None:
    records = [
        {
            "transaction_id": "t1",
            "country": "US",
            "risk_tier": "high",
            "alert_status": "open",
            "transaction_amount": 100.0,
            "event_time": "2025-01-01T00:00:00Z",
        },
        {
            "transaction_id": "t2",
            "country": "IN",
            "risk_tier": "low",
            "alert_status": None,
            "transaction_amount": 10.0,
            "event_time": "2025-01-02T00:00:00Z",
        },
    ]
    filtered = filter_transactions(records, risk_tier="high", country="US", minimum_amount=50)
    assert filtered["transaction_id"].tolist() == ["t1"]
    assert sort_frame(filtered, "transaction_amount").iloc[0]["transaction_id"] == "t1"
    assert format_probability(0.125) == "12.5%"
    assert format_amount(100, "USD") == "USD 100.00"
    assert format_timestamp("2025-01-01T00:00:00Z").endswith("UTC")


def test_dashboard_masks_sensitive_identifiers_for_display() -> None:
    records = [{"customer_id": "customer-123", "transaction_id": "transaction-123"}]

    masked = mask_records_for_display(records)

    assert masked[0]["customer_id"] != records[0]["customer_id"]
    assert masked[0]["transaction_id"] == records[0]["transaction_id"]
