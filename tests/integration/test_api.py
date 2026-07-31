from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import SecretStr

from sentinelstream.api import Role, create_app
from sentinelstream.config import AppSettings, RedisSettings
from sentinelstream.config.settings import SimulationSettings
from sentinelstream.database import RedisCache, create_database
from sentinelstream.simulation.generator import TransactionGenerator


def _client(tmp_path: Path) -> tuple[TestClient, object]:
    settings = AppSettings(
        secret_key=SecretStr("test-api-secret-with-at-least-32-bytes"),
        redis=RedisSettings(enabled=False),
    )
    database = create_database(settings, url=f"sqlite:///{tmp_path / 'api.db'}")
    database.create_all()

    users = {
        "admin": {"password": "admin-password", "roles": [Role.ADMIN]},
        "analyst": {"password": "analyst-password", "roles": [Role.ANALYST]},
        "readonly": {"password": "readonly-password", "roles": [Role.READONLY]},
    }

    def authenticate(username: str, password: str) -> list[Role] | None:
        user = users.get(username)
        return user["roles"] if user and user["password"] == password else None

    app = create_app(
        settings,
        database=database,
        cache=RedisCache(settings.redis),
        authenticator=authenticate,
        initialize_database=False,
    )
    return TestClient(app), database


def _token(client: TestClient, username: str, password: str) -> str:
    response = client.post(
        "/v1/auth/token",
        data={"username": username, "password": password},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def test_prediction_alert_feedback_history_and_rbac(tmp_path: Path) -> None:
    client, database = _client(tmp_path)
    admin_token = _token(client, "admin", "admin-password")
    readonly_token = _token(client, "readonly", "readonly-password")
    headers = {"Authorization": f"Bearer {admin_token}"}
    event = (
        TransactionGenerator(
            SimulationSettings(customer_count=1, merchant_count=1, random_seed=23, fraud_ratio=0.0)
        )
        .generate(1)[0]
        .event
    )
    payload = {
        "event": event.model_dump(mode="json"),
        "supervised_probability": 0.99,
        "anomaly_score": 0.95,
        "rule_score": 0.90,
    }

    denied = client.post(
        "/v1/predictions",
        json=payload,
        headers={"Authorization": f"Bearer {readonly_token}"},
    )
    assert denied.status_code == 403

    created = client.post("/v1/predictions", json=payload, headers=headers)
    assert created.status_code == 200, created.text
    prediction = created.json()
    assert prediction["action"] == "block"
    prediction_id = prediction["prediction_message_id"]

    retrieved = client.get(f"/v1/predictions/{prediction_id}", headers=headers)
    assert retrieved.status_code == 200
    explanation = client.get(f"/v1/predictions/{prediction_id}/explanation", headers=headers)
    assert explanation.status_code == 200

    alerts = client.get("/v1/alerts", headers=headers)
    assert alerts.status_code == 200
    alert_id = alerts.json()[0]["alert_id"]
    assert client.post(f"/v1/alerts/{alert_id}/acknowledge", headers=headers).status_code == 200
    assert client.post(f"/v1/alerts/{alert_id}/resolve", headers=headers).status_code == 200

    feedback = client.post(
        "/v1/feedback",
        json={
            "event_id": str(event.event_id),
            "transaction_id": str(event.transaction_id),
            "outcome": "confirmed_fraud",
            "note": "confirmed during review",
        },
        headers=headers,
    )
    assert feedback.status_code == 200
    history = client.get(f"/v1/history/customers/{event.customer_id}", headers=headers)
    assert history.status_code == 200
    assert len(history.json()) == 1
    masked_history = client.get(
        f"/v1/history/customers/{event.customer_id}",
        headers={"Authorization": f"Bearer {readonly_token}"},
    )
    assert masked_history.status_code == 200
    assert masked_history.json()[0]["customer_id"] != event.customer_id
    assert masked_history.json()[0]["customer_id"].startswith("id_")
    metrics = client.get("/metrics", headers=headers)
    assert metrics.json()["predictions_created"] == 1
    dashboard_rows = client.get("/v1/dashboard/transactions", headers=headers)
    assert dashboard_rows.status_code == 200
    assert dashboard_rows.json()[0]["transaction_id"] == str(event.transaction_id)
    prometheus = client.get("/prometheus/metrics")
    assert prometheus.status_code == 200
    assert "sentinelstream_api_requests_total" in prometheus.text
    dependencies = client.get("/v1/system/dependencies", headers=headers)
    assert dependencies.status_code == 200
    assert {item["name"] for item in dependencies.json()["components"]} >= {
        "postgresql",
        "redis",
    }
    assert client.get("/live").json()["status"] == "ok"
    database.close()


def test_api_authentication_and_request_validation(tmp_path: Path) -> None:
    client, database = _client(tmp_path)
    assert client.get("/v1/alerts").status_code == 401
    invalid = client.post(
        "/v1/auth/token",
        data={"username": "admin", "password": "incorrect"},
    )
    assert invalid.status_code == 401
    admin_token = _token(client, "admin", "admin-password")
    response = client.post(
        "/v1/predictions",
        json={"event": {"not": "a transaction"}},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 422
    database.close()
