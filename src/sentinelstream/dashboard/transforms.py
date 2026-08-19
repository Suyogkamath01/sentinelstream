"""Pure dashboard data transformations and formatting helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd

from sentinelstream.security.pii import mask_identifier

_DISPLAY_MASKED_FIELDS = frozenset(
    {
        "account_id",
        "card_id",
        "city",
        "customer_id",
        "device_id",
        "email",
        "entity_id",
        "ip_address",
        "latitude",
        "longitude",
        "merchant_id",
        "phone",
        "phone_number",
    }
)


def mask_records_for_display(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mask sensitive fields for operator tables while preserving API values for actions."""

    masked: list[dict[str, Any]] = []
    for record in records:
        copy = dict(record)
        for field in _DISPLAY_MASKED_FIELDS & copy.keys():
            copy[field] = mask_identifier(copy[field])
        masked.append(copy)
    return masked


def records_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize API records for tables without inventing missing fields."""

    frame = pd.DataFrame(records)
    if "event_time" in frame:
        frame["event_time"] = pd.to_datetime(frame["event_time"], errors="coerce", utc=True)
    if "created_at" in frame:
        frame["created_at"] = pd.to_datetime(frame["created_at"], errors="coerce", utc=True)
    return frame


def filter_transactions(
    records: list[dict[str, Any]],
    *,
    query: str = "",
    risk_tier: str = "All",
    alert_status: str = "All",
    country: str = "All",
    minimum_amount: float | None = None,
    maximum_amount: float | None = None,
) -> pd.DataFrame:
    frame = records_frame(records)
    if frame.empty:
        return frame
    if query.strip():
        needle = query.strip().lower()
        searchable = frame.astype(str).apply(
            lambda column: column.str.lower().str.contains(needle, regex=False)
        )
        frame = frame[searchable.any(axis=1)]
    if risk_tier != "All" and "risk_tier" in frame:
        frame = frame[frame["risk_tier"].fillna("Unknown").str.lower() == risk_tier.lower()]
    if alert_status != "All" and "alert_status" in frame:
        frame = frame[frame["alert_status"].fillna("none").str.lower() == alert_status.lower()]
    if country != "All" and "country" in frame:
        frame = frame[frame["country"] == country]
    if minimum_amount is not None and "transaction_amount" in frame:
        frame = frame[frame["transaction_amount"] >= minimum_amount]
    if maximum_amount is not None and "transaction_amount" in frame:
        frame = frame[frame["transaction_amount"] <= maximum_amount]
    return frame.reset_index(drop=True)


def sort_frame(frame: pd.DataFrame, column: str, *, descending: bool = True) -> pd.DataFrame:
    if frame.empty or column not in frame:
        return frame
    return frame.sort_values(column, ascending=not descending, kind="stable").reset_index(drop=True)


def format_probability(value: Any) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):.1%}"


def format_amount(value: Any, currency: str = "") -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{currency} {float(value):,.2f}".strip()


def format_timestamp(value: Any) -> str:
    if value is None or pd.isna(value):
        return "—"
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return "—" if pd.isna(parsed) else parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def metric_value(metrics: dict[str, Any], key: str, fallback: Any = "—") -> Any:
    value = metrics.get(key)
    return fallback if value is None else value


def health_label(status: str | None) -> str:
    return {
        "healthy": "Healthy",
        "ok": "Healthy",
        "degraded": "Degraded",
        "unavailable": "Unavailable",
        "not_ready": "Not ready",
    }.get((status or "unknown").lower(), "Unknown")
