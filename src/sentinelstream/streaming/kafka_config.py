"""Common Kafka client security configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sentinelstream.config.settings import KafkaSettings


def kafka_client_config(
    settings: KafkaSettings,
    values: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Add configured transport security without exposing credentials to callers."""

    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
        **(values or {}),
    }
    if settings.sasl_mechanisms:
        config["sasl.mechanisms"] = settings.sasl_mechanisms
    if settings.sasl_username is not None:
        config["sasl.username"] = settings.sasl_username.get_secret_value()
    if settings.sasl_password is not None:
        config["sasl.password"] = settings.sasl_password.get_secret_value()
    return config
