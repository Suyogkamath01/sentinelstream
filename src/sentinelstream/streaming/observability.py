"""Small structured-logging helpers for stream operations."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from sentinelstream.security.pii import redact_mapping, redact_text

_FIELDS = "sentinelstream_fields"


def _safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return redact_mapping(value)
    if isinstance(value, (list, tuple)):
        return [_safe_value(item) for item in value]
    if hasattr(value, "value"):
        return _safe_value(value.value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return redact_text(value) if isinstance(value, str) else value
    return str(value)


class JsonLogFormatter(logging.Formatter):
    """Render one log record as a compact JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, _FIELDS, {})
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": redact_text(record.getMessage()),
            **_safe_value(fields),
        }
        if record.exc_info:
            payload["exception"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    """Emit a structured event without placing payload data in the message text."""

    logger.log(level, event, extra={_FIELDS: fields})


def configure_structured_logging(level: str = "INFO") -> None:
    """Install a JSON stream handler once for local and service runtimes."""

    root = logging.getLogger()
    root.setLevel(level.upper())
    if any(isinstance(handler, logging.StreamHandler) for handler in root.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root.addHandler(handler)
