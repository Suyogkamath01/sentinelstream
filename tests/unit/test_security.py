from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from pydantic import SecretStr, ValidationError

from sentinelstream.api.security import (
    Permission,
    Role,
    TokenUser,
    create_access_token,
    decode_access_token,
)
from sentinelstream.config import (
    AppSettings,
    KafkaSettings,
    RateLimitSettings,
    RedisSettings,
)
from sentinelstream.config.settings import Environment
from sentinelstream.database.redis_cache import RedisCache
from sentinelstream.security.pii import mask_identifier, redact_mapping, redact_text
from sentinelstream.security.rate_limit import RateLimiter
from sentinelstream.streaming.kafka_config import kafka_client_config
from sentinelstream.streaming.observability import JsonLogFormatter


@pytest.mark.security
def test_jwt_validates_signature_issuer_and_audience() -> None:
    secret = "test-secret-with-at-least-32-characters"
    token = create_access_token(
        "analyst-1",
        [Role.ANALYST],
        secret=secret,
        issuer="sentinelstream",
        audience="sentinelstream-api",
    )

    user = decode_access_token(
        token,
        secret=secret,
        issuer="sentinelstream",
        audience="sentinelstream-api",
    )

    assert user.subject == "analyst-1"
    assert user.jti
    assert user.has_permission(Permission.RESOLVE_ALERT)
    with pytest.raises(jwt.InvalidIssuerError):
        decode_access_token(token, secret=secret, issuer="other")
    with pytest.raises(jwt.InvalidAudienceError):
        decode_access_token(token, secret=secret, audience="other")
    with pytest.raises(jwt.InvalidSignatureError):
        decode_access_token(token, secret="different-secret-with-at-least-32-characters")


@pytest.mark.security
def test_expired_jwt_is_rejected() -> None:
    token = create_access_token(
        "analyst-1",
        [Role.ANALYST],
        secret="test-secret-with-at-least-32-characters",
        now=datetime.now(UTC) - timedelta(minutes=2),
        expires_minutes=1,
    )

    with pytest.raises(jwt.ExpiredSignatureError):
        decode_access_token(token, secret="test-secret-with-at-least-32-characters")


@pytest.mark.security
def test_role_permissions_are_explicit() -> None:
    readonly = TokenUser("viewer", frozenset({Role.READONLY}))
    investigator = TokenUser("investigator", frozenset({Role.INVESTIGATOR}))

    assert readonly.has_permission(Permission.VIEW_ALERTS)
    assert not readonly.has_permission(Permission.RESOLVE_ALERT)
    assert investigator.has_permission(Permission.ACK_ALERT)
    assert not investigator.has_permission(Permission.RESOLVE_ALERT)


@pytest.mark.security
def test_pii_masking_and_log_redaction() -> None:
    customer = mask_identifier("customer-123")
    assert customer == mask_identifier("customer-123")
    assert customer != "customer-123"

    safe = redact_mapping(
        {
            "customer_id": "customer-123",
            "password": "not-for-logs",
            "message": "Authorization: Bearer abc.def.ghi",
        }
    )
    assert safe["customer_id"] == customer
    assert safe["password"] == "[REDACTED]"
    assert "abc.def.ghi" not in safe["message"]
    assert redact_text("postgres://user:password@example/db") == (
        "postgres://[REDACTED]:[REDACTED]@example/db"
    )

    record = logging.LogRecord(
        "security",
        logging.WARNING,
        __file__,
        1,
        "token=raw-token",
        (),
        None,
    )
    formatted = JsonLogFormatter().format(record)
    assert "raw-token" not in formatted

    try:
        raise RuntimeError("password=exception-secret")
    except RuntimeError:
        exception_record = logging.LogRecord(
            "security",
            logging.ERROR,
            __file__,
            1,
            "request failed",
            (),
            None,
        )
        exception_record.exc_info = sys.exc_info()
    assert "exception-secret" not in JsonLogFormatter().format(exception_record)


@pytest.mark.security
def test_rate_limiter_uses_bounded_fallback() -> None:
    cache = RedisCache(RedisSettings(enabled=False, fallback_max_entries=100))
    limiter = RateLimiter(
        cache,
        RateLimitSettings(requests=2, window_seconds=60),
    )

    assert limiter.check("client-a").allowed
    assert limiter.check("client-a").allowed
    rejected = limiter.check("client-a")
    assert not rejected.allowed
    assert rejected.remaining == 0


@pytest.mark.security
def test_secret_values_are_not_serialised_in_settings() -> None:
    settings = RedisSettings(url=SecretStr("redis://user:password@example/0").get_secret_value())
    assert "password" in settings.url
    assert json.dumps(settings.model_dump(), default=str)


@pytest.mark.security
def test_production_secret_validation_and_kafka_credentials() -> None:
    with pytest.raises(ValidationError):
        AppSettings(environment=Environment.PRODUCTION, secret_key=SecretStr("short"))
    with pytest.raises(ValidationError):
        AppSettings(
            environment=Environment.PRODUCTION,
            secret_key=SecretStr("test-secret-with-at-least-32-characters"),
        )

    kafka = kafka_client_config(
        KafkaSettings(
            security_protocol="SASL_SSL",
            sasl_mechanisms="SCRAM-SHA-256",
            sasl_username=SecretStr("client"),
            sasl_password=SecretStr("password"),
        ),
        {"client.id": "test-client"},
    )
    assert kafka["security.protocol"] == "SASL_SSL"
    assert kafka["sasl.username"] == "client"
    assert kafka["sasl.password"] == "password"
