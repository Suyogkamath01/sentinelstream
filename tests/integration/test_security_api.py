from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import SecretStr

from sentinelstream.api import Role, create_app
from sentinelstream.config import AppSettings, RateLimitSettings, RedisSettings
from sentinelstream.database import RedisCache, create_database


def _client(tmp_path: Path) -> TestClient:
    settings = AppSettings(
        secret_key=SecretStr("test-api-secret-with-at-least-32-bytes"),
        redis=RedisSettings(enabled=False),
        rate_limit=RateLimitSettings(
            requests=100,
            window_seconds=60,
            authentication_requests=2,
            authentication_window_seconds=60,
        ),
        api={"max_request_bytes": 1_024},
    )
    database = create_database(settings, url=f"sqlite:///{tmp_path / 'security.db'}")
    database.create_all()
    users = {
        "readonly": ("readonly-password", [Role.READONLY]),
        "investigator": ("investigator-password", [Role.INVESTIGATOR]),
    }

    def authenticate(username: str, password: str) -> list[Role] | None:
        configured = users.get(username)
        return configured[1] if configured and configured[0] == password else None

    return TestClient(
        create_app(
            settings,
            database=database,
            cache=RedisCache(settings.redis),
            authenticator=authenticate,
            initialize_database=False,
        )
    )


def _token(client: TestClient) -> str:
    response = client.post(
        "/v1/auth/token",
        data={"username": "readonly", "password": "readonly-password"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def test_invalid_token_and_revocation_are_rejected(tmp_path: Path) -> None:
    client = _client(tmp_path)

    invalid = client.get(
        "/v1/alerts",
        headers={
            "Authorization": "Bearer malformed-token",
            "X-Correlation-ID": "security-test-1",
        },
    )
    assert invalid.status_code == 401
    assert invalid.headers["X-Correlation-ID"] == "security-test-1"
    assert "malformed-token" not in invalid.text
    token = _token(client)
    headers = {"Authorization": f"Bearer {token}"}
    assert client.get("/v1/alerts", headers=headers).status_code == 200
    assert client.post("/v1/auth/revoke", headers=headers).json() == {"revoked": True}
    assert client.get("/v1/alerts", headers=headers).status_code == 401


def test_authentication_rate_limit_and_payload_policy(tmp_path: Path) -> None:
    client = _client(tmp_path)
    first = client.post(
        "/v1/auth/token",
        data={"username": "readonly", "password": "wrong"},
    )
    second = client.post(
        "/v1/auth/token",
        data={"username": "readonly", "password": "wrong"},
    )
    third = client.post(
        "/v1/auth/token",
        data={"username": "readonly", "password": "wrong"},
    )
    assert first.status_code == 401
    assert second.status_code == 401
    assert third.status_code == 429
    assert third.headers["X-RateLimit-Remaining"] == "0"

    oversized = client.post(
        "/v1/auth/revoke",
        content=b"x" * 1_025,
        headers={"Authorization": "Bearer malformed-token", "Content-Type": "application/json"},
    )
    assert oversized.status_code == 413

    unsupported = client.post(
        "/v1/auth/revoke",
        content=b"{}",
        headers={"Authorization": "Bearer malformed-token", "Content-Type": "text/plain"},
    )
    assert unsupported.status_code == 415


def test_readiness_returns_service_unavailable_for_failed_dependency(tmp_path: Path) -> None:
    settings = AppSettings(
        secret_key=SecretStr("test-api-secret-with-at-least-32-bytes"),
        redis=RedisSettings(url="redis://127.0.0.1:1/0", socket_timeout_seconds=0.01),
    )
    database = create_database(settings, url=f"sqlite:///{tmp_path / 'readiness.db'}")
    database.create_all()
    app = create_app(
        settings,
        database=database,
        cache=RedisCache(settings.redis),
        initialize_database=False,
    )

    response = TestClient(app).get("/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    database.close()
