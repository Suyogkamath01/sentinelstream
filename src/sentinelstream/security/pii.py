"""Boundary-level masking and redaction for logs and operator interfaces."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

_REDACTED = "[REDACTED]"
_PII_KEYS = {
    "account_id",
    "card_id",
    "city",
    "customer_id",
    "device_id",
    "email",
    "ip_address",
    "latitude",
    "longitude",
    "merchant_id",
    "phone",
    "phone_number",
    "postal_code",
}
_SECRET_KEY_PARTS = {
    "api_key",
    "authorization",
    "password",
    "private_key",
    "secret",
    "token",
}
_BEARER_PATTERN = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+")
_URL_CREDENTIAL_PATTERN = re.compile(r"(://)([^:/\s]+):([^@\s]+)@")
_KEY_VALUE_SECRET_PATTERN = re.compile(
    r"(?i)(\b(?:password|token|secret|api[_-]?key)\s*[=:]\s*)([^\s,;]+)"
)


def mask_identifier(value: Any, *, namespace: str = "sentinelstream") -> Any:
    """Return a stable, non-reversible display identifier for a sensitive value."""

    if value is None:
        return None
    text = str(value)
    digest = hashlib.sha256(f"{namespace}:{text}".encode()).hexdigest()[:12]
    return f"id_{digest}"


def _is_secret_key(key: str) -> bool:
    lowered = key.lower().replace("-", "_")
    return any(part in lowered for part in _SECRET_KEY_PARTS)


def redact_value(key: str | None, value: Any) -> Any:
    """Redact secrets and pseudonymise identifiers in a structured value."""

    normalised_key = (key or "").lower()
    if _is_secret_key(normalised_key) or normalised_key in {"raw_payload", "request_body"}:
        return _REDACTED
    if normalised_key in _PII_KEYS:
        return mask_identifier(value)
    if isinstance(value, Mapping):
        return {
            str(item_key): redact_value(str(item_key), item) for item_key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [redact_value(None, item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    if hasattr(value, "value"):
        return redact_value(key, value.value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


def redact_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    """Return a recursively safe copy of structured log or error fields."""

    return {str(key): redact_value(str(key), value) for key, value in values.items()}


def redact_text(value: str) -> str:
    """Remove common credential forms from free-form messages."""

    redacted = _BEARER_PATTERN.sub(r"\1[REDACTED]", value)
    redacted = _URL_CREDENTIAL_PATTERN.sub(r"\1[REDACTED]:[REDACTED]@", redacted)
    return _KEY_VALUE_SECRET_PATTERN.sub(r"\1[REDACTED]", redacted)
